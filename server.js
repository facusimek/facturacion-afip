
require('dotenv').config();
const express = require('express');
const bodyParser = require('body-parser');
const { google } = require('googleapis');
const TelegramBot = require('node-telegram-bot-api');
const Afip = require('@afipsdk/afip.js');

// ====== CONFIG PRINCIPAL ======
const PORT = process.env.PORT || 3000;
const SHEET_ID = process.env.SHEET_ID;             // ID de tu planilla
const SHEET_NAME = process.env.SHEET_NAME || 'Hoja 1'; // nombre de pestaña
const TELEGRAM_TOKEN = process.env.TELEGRAM_TOKEN; 
const WEBHOOK_URL = process.env.WEBHOOK_URL;       // https://tuapp.onrender.com/telegram

// AFIP: por defecto usamos homologación con CUIT de prueba (no requiere cert/key)
const AFIP_CUIT = Number(process.env.AFIP_CUIT || '20409378472');
const AFIP_PROD = String(process.env.AFIP_PROD || 'false') === 'true';
const AFIP_PTO_VTA = Number(process.env.AFIP_PTO_VTA || '1');
const AFIP_CBTE_TIPO = Number(process.env.AFIP_CBTE_TIPO || '11'); // 11 = Factura C

// Si querés producción, cargá cert y key completos (multilínea) en variables de entorno
const AFIP_CERT = process.env.AFIP_CERT ? process.env.AFIP_CERT.replace(/\\n/g, '\\n') : undefined;
const AFIP_KEY  = process.env.AFIP_KEY  ? process.env.AFIP_KEY.replace(/\\n/g, '\\n') : undefined;

// ====== CLIENTE GOOGLE SHEETS ======
const auth = new google.auth.GoogleAuth({
  credentials: JSON.parse(process.env.GOOGLE_SA_JSON),
  scopes: ['https://www.googleapis.com/auth/spreadsheets']
});
const sheets = google.sheets({ version: 'v4', auth });

// ====== AFIP SDK ======
const afip = new Afip({
  CUIT: AFIP_CUIT,
  production: AFIP_PROD,
  cert: AFIP_CERT,
  key: AFIP_KEY
});

// ====== TELEGRAM (webhook) ======
const bot = new TelegramBot(TELEGRAM_TOKEN, { webHook: true });
bot.setWebHook(`${WEBHOOK_URL}`);

// ====== APP WEB ======
const app = express();
app.use(bodyParser.json());

// Endpoint que Telegram llama
app.post('/telegram', (req, res) => {
  bot.processUpdate(req.body);
  res.sendStatus(200);
});

// Formato simple del mensaje para facturar:
//  Nombre | DNI o CUIT | Detalle | Total
//  Ej: Juan Perez | DNI 12345678 | Servicio de diseño | 5000

function parseMessage(text) {
  const parts = text.split('|').map(s => s.trim());
  if (parts.length < 4) return null;
  const [nombre, docCampo, detalle, totalStr] = parts;

  let doc_tipo = 'DNI';
  let doc_nro = '';
  const m1 = docCampo.match(/(DNI|CUIT)\\s*(\\d+)/i);
  if (m1) {
    doc_tipo = m1[1].toUpperCase();
    doc_nro = m1[2];
  } else {
    // si no puso prefijo, asumimos DNI
    doc_nro = docCampo.replace(/\\D/g, '');
  }

  const total = Number(String(totalStr).replace(/[^\\d.]/g, ''));
  return {
    fecha: new Date().toISOString().slice(0,10),
    cliente_nombre: nombre,
    doc_tipo,
    doc_nro,
    concepto: 2, // Servicios
    detalle,
    total,
    pto_vta: AFIP_PTO_VTA,
    cbte_tipo: AFIP_CBTE_TIPO
  };
}

async function appendRow(row) {
  const values = [[
    row.fecha,
    row.cliente_nombre,
    row.doc_tipo,
    row.doc_nro,
    row.concepto,
    row.detalle,
    row.total,
    row.pto_vta,
    row.cbte_tipo,
    'PENDIENTE', '', '', '', ''
  ]];
  await sheets.spreadsheets.values.append({
    spreadsheetId: SHEET_ID,
    range: `${SHEET_NAME}!A:Z`,
    valueInputOption: 'RAW',
    requestBody: { values }
  });
}

function docTipoCode(t) {
  return String(t).toUpperCase() === 'CUIT' ? 80 : 96; // 80=CUIT, 96=DNI
}

async function emitirFactura(row) {
  // row: objeto con los campos arriba
  const ahora = new Date();
  const yyyymmdd = `${ahora.getFullYear()}${String(ahora.getMonth()+1).padStart(2,'0')}${String(ahora.getDate()).padStart(2,'0')}`;

  const data = {
    CantReg: 1,
    PtoVta: Number(row.pto_vta),
    CbteTipo: Number(row.cbte_tipo),      // 11 = Factura C
    Concepto: Number(row.concepto),       // 1 Prod, 2 Serv, 3 Ambos
    DocTipo: docTipoCode(row.doc_tipo),
    DocNro: Number(row.doc_nro),
    CbteFch: yyyymmdd,
    ImpTotal: Number(row.total),
    ImpTotConc: 0,
    ImpNeto: Number(row.total),
    ImpIVA: 0,
    ImpTrib: 0,
    MonId: 'PES',
    MonCotiz: 1,
    Iva: []
  };

  // crea el siguiente comprobante disponible
  const res = await afip.ElectronicBilling.createNextVoucher(data);
  return {
    CAE: res.CAE,
    CAEFchVto: res.CAEFchVto,
    voucher_number: res.voucher_number
  };
}

async function updateLastRowWithResult(result) {
  // Busca la última fila con estado PENDIENTE y escribe el resultado
  const get = await sheets.spreadsheets.values.get({
    spreadsheetId: SHEET_ID,
    range: `${SHEET_NAME}!A:Z`
  });
  const rows = get.data.values || [];
  for (let i = rows.length - 1; i >= 1; i--) {
    if (rows[i][9] === 'PENDIENTE') { // col J = estado
      const rowIndex = i + 1; // 1-based
      const updates = [[
        'EMITIDO',
        result.CAE,
        result.CAEFchVto,
        result.voucher_number,
        ''
      ]];
      await sheets.spreadsheets.values.update({
        spreadsheetId: SHEET_ID,
        range: `${SHEET_NAME}!J${rowIndex}:N${rowIndex}`,
        valueInputOption: 'RAW',
        requestBody: { values: updates }
      });
      return rowIndex;
    }
  }
}

// Cuando recibimos un mensaje: cargamos fila -> emitimos -> respondemos con CAE
const welcome = 'Hola! Enviame: Nombre | DNI o CUIT | Detalle | Total\\nEjemplo:\\nJuan Perez | DNI 12345678 | Servicio de diseño | 5000';

const appWelcome = 'OK';
const app = express();
app.use(bodyParser.json());

// Telegram webhook endpoint
app.post('/telegram', (req, res) => {
  bot.processUpdate(req.body);
  res.sendStatus(200);
});

// Healthcheck
app.get('/', (_, res) => res.send(appWelcome));

// Telegram message handler
bot.on('message', async (msg) => {
  const chatId = msg.chat.id;
  const text = (msg.text || '').trim();

  // Comando /start de bienvenida
  if (text === '/start') {
    return bot.sendMessage(chatId, welcome);
  }

  const parsed = parseMessage(text);
  if (!parsed) {
    return bot.sendMessage(chatId, 'Formato incorrecto. Usá: Nombre | DNI o CUIT | Detalle | Total');
  }

  try {
    await appendRow(parsed);
    const result = await emitirFactura(parsed);
    await updateLastRowWithResult(result);

    return bot.sendMessage(chatId,
      `✅ Factura emitida\\nCAE: ${result.CAE}\\nVence: ${result.CAEFchVto}\\nNro: ${result.voucher_number}`);
  } catch (e) {
    console.error(e);
    return bot.sendMessage(chatId, '❌ No se pudo emitir. Revisá datos y probá de nuevo.');
  }
});

app.listen(PORT, () => console.log('Server on', PORT));
