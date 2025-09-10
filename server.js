// server.js
'use strict';

require('dotenv').config();
const express = require('express');
const bodyParser = require('body-parser');
const { google } = require('googleapis');
const TelegramBot = require('node-telegram-bot-api');
const Afip = require('@afipsdk/afip.js');

// ====== CONFIG ======
const PORT = process.env.PORT || 3000;
const SHEET_ID = process.env.SHEET_ID;
const SHEET_NAME = process.env.SHEET_NAME || 'Hoja 1';
const TELEGRAM_TOKEN = process.env.TELEGRAM_TOKEN;
const WEBHOOK_URL = process.env.WEBHOOK_URL;

const AFIP_CUIT = Number(process.env.AFIP_CUIT || '20409378472'); // CUIT test por defecto (homologación)
const AFIP_PROD = String(process.env.AFIP_PROD || 'false') === 'true';
const AFIP_PTO_VTA = Number(process.env.AFIP_PTO_VTA || '1');
const AFIP_CBTE_TIPO = Number(process.env.AFIP_CBTE_TIPO || '11'); // 11 = Factura C

// Para producción real (si AFIP_PROD=true)
const AFIP_CERT = process.env.AFIP_CERT ? process.env.AFIP_CERT.replace(/\\n/g, '\n') : undefined;
const AFIP_KEY  = process.env.AFIP_KEY  ? process.env.AFIP_KEY.replace(/\\n/g, '\n') : undefined;

// ====== HELPERS (errores legibles) ======
function humanError(e) {
  try {
    if (!e) return 'Error desconocido';
    const data = e.response?.data;
    if (typeof data === 'string') return data.slice(0, 500);
    if (data?.faultstring) return data.faultstring;
    if (data?.error?.message) return data.error.message;
    if (e.message) return e.message;
    return JSON.stringify(e).slice(0, 500);
  } catch (_) {
    return 'Error desconocido';
  }
}

function logError(prefix, e) {
  const msg = humanError(e);
  console.error(`[${prefix}]`, msg, e?.stack ? `\nSTACK:\n${e.stack}` : '');
  return msg;
}

function toYYYYMMDD(dateStr) {
  // dateStr esperado: YYYY-MM-DD (columna 'fecha' de la planilla)
  const d = dateStr ? new Date(dateStr) : new Date();
  const isValid = !isNaN(d.getTime());
  const dd = isValid ? d : new Date();
  const y = dd.getFullYear();
  const m = String(dd.getMonth() + 1).padStart(2, '0');
  const day = String(dd.getDate()).padStart(2, '0');
  return `${y}${m}${day}`;
}

// ====== GOOGLE SHEETS CLIENT ======
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

// ====== TELEGRAM WEBHOOK ======
const bot = new TelegramBot(TELEGRAM_TOKEN, { webHook: true });
bot.setWebHook(WEBHOOK_URL);

// ====== EXPRESS (una sola instancia) ======
const app = express();
app.use(bodyParser.json());

// Endpoint que Telegram llama (no abre en el navegador)
app.post('/telegram', (req, res) => {
  bot.processUpdate(req.body);
  res.sendStatus(200);
});

// Healthcheck simple (sí abre en el navegador)
app.get('/', (_, res) => res.send('OK'));

// (Opcional) Endpoints de diagnóstico rápido
app.get('/diag/sheets', async (_, res) => {
  try {
    const dummy = {
      fecha: new Date().toISOString().slice(0,10),
      cliente_nombre: 'TEST',
      doc_tipo: 'DNI',
      doc_nro: '12345678',
      concepto: 2, // Servicios
      detalle: 'ping',
      total: 1,
      pto_vta: AFIP_PTO_VTA,
      cbte_tipo: AFIP_CBTE_TIPO
    };
    await appendRow(dummy);
    res.send('SHEETS OK');
  } catch (e) {
    res.status(500).send('SHEETS ERROR: ' + humanError(e));
  }
});

app.get('/diag/afip', async (_, res) => {
  try {
    const st = await afip.ElectronicBilling.getServerStatus();
    res.send('AFIP OK: ' + JSON.stringify(st));
  } catch (e) {
    res.status(500).send('AFIP ERROR: ' + humanError(e));
  }
});

// ====== LÓGICA ======
function parseMessage(text) {
  const parts = text.split('|').map(s => s.trim());
  if (parts.length < 4) return null;
  const [nombre, docCampo, detalle, totalStr] = parts;

  let doc_tipo = 'DNI';
  let doc_nro = '';
  const m1 = docCampo.match(/(DNI|CUIT)\s*(\d+)/i);
  if (m1) {
    doc_tipo = m1[1].toUpperCase();
    doc_nro = m1[2];
  } else {
    // si no puso prefijo, asumimos DNI
    doc_nro = docCampo.replace(/\D/g, '');
  }

  const total = Number(String(totalStr).replace(/[^\d.]/g, ''));
  return {
    fecha: new Date().toISOString().slice(0,10),
    cliente_nombre: nombre,
    doc_tipo,
    doc_nro,
    concepto: 2, // *** Solo servicios por defecto ***
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
  return sheets.spreadsheets.values.append({
    spreadsheetId: SHEET_ID,
    range: `${SHEET_NAME}!A:Z`,
    valueInputOption: 'RAW',
    requestBody: { values }
  });
}

function docTipoCode(t) { return String(t).toUpperCase() === 'CUIT' ? 80 : 96; } // 80=CUIT, 96=DNI

// ====== Condición IVA del receptor ======
// IDs comunes: 1=RI, 6=Monotributo, 4=Exento, 5=Consumidor Final, 7=No Categorizado, 15=No Alcanzado
function getCondicionIVAReceptorId(parsed) {
  if (parsed.doc_tipo === 'DNI') return 5; // Consumidor Final para DNI
  // Para CUIT, usamos un default configurable por variable de entorno (6=Monotributo, 1=RI, etc.)
  const def = Number(process.env.IVA_COND_RECEPTOR_ID_DEFAULT || '6');
  return def;
}

async function emitirFactura(row) {
  // Fecha del comprobante y fechas de servicio (obligatorias para Concepto 2 o 3)
  const cbteFch = toYYYYMMDD(row.fecha);

  const data = {
    CantReg: 1,
    PtoVta: Number(row.pto_vta),
    CbteTipo: Number(row.cbte_tipo),   // 11 = Factura C
    Concepto: Number(row.concepto),    // 2 = Servicios (por defecto)
    DocTipo: docTipoCode(row.doc_tipo),
    DocNro: Number(row.doc_nro),
    CbteFch: cbteFch,
    ImpTotal: Number(row.total),
    ImpTotConc: 0,
    ImpNeto: Number(row.total),
    ImpIVA: 0,
    ImpTrib: 0,
    MonId: 'PES',
    MonCotiz: 1,
    Iva: [],
    CondicionIVAReceptorId: getCondicionIVAReceptorId(row)
  };

  // Obligatorio cuando Concepto es 2 (Servicios) o 3 (Ambos)
  if (data.Concepto === 2 || data.Concepto === 3) {
    data.FchServDesde = cbteFch;
    data.FchServHasta = cbteFch;
    data.FchVtoPago   = cbteFch;
  }

  console.log(
    'Usando CondicionIVAReceptorId=',
    data.CondicionIVAReceptorId,
    'Concepto=',
    data.Concepto,
    'Fechas Serv=',
    data.FchServDesde,
    data.FchServHasta,
    data.FchVtoPago
  );

  // Usa el siguiente número de comprobante disponible
  const res = await afip.ElectronicBilling.createNextVoucher(data);
  return { CAE: res.CAE, CAEFchVto: res.CAEFchVto, voucher_number: res.voucher_number };
}

async function updateLastRowWithResult(result) {
  const get = await sheets.spreadsheets.values.get({
    spreadsheetId: SHEET_ID,
    range: `${SHEET_NAME}!A:Z`
  });
  const rows = get.data.values || [];
  for (let i = rows.length - 1; i >= 1; i--) {
    if (rows[i][9] === 'PENDIENTE') { // col J = estado
      const rowIndex = i + 1; // 1-based
      const updates = [[ 'EMITIDO', result.CAE, result.CAEFchVto, result.voucher_number, '' ]];
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

// ====== HANDLER TELEGRAM (con errores detallados) ======
bot.on('message', async (msg) => {
  const chatId = msg.chat.id;
  const text = (msg.text || '').trim();

  if (text === '/start') {
    return bot.sendMessage(
      chatId,
      'Hola! Enviame: Nombre | DNI o CUIT | Detalle | Total\nEjemplo:\nJuan Perez | DNI 12345678 | Servicio de diseño | 5000'
    );
  }

  const parsed = parseMessage(text);
  if (!parsed) {
    return bot.sendMessage(chatId, 'Formato incorrecto. Usá: Nombre | DNI o CUIT | Detalle | Total');
  }

  // Paso 1: escribir en Sheets
  try {
    await appendRow(parsed);
  } catch (e) {
    const msg = logError('SHEETS_APPEND', e);
    return bot.sendMessage(chatId, '❌ Error en Google Sheets: ' + msg);
  }

  // Paso 2: emitir en AFIP
  let result;
  try {
    result = await emitirFactura(parsed);
  } catch (e) {
    const msg = logError('AFIP_EMITIR', e);
    return bot.sendMessage(chatId, '❌ Error en AFIP: ' + msg);
  }

  // Paso 3: actualizar fila con CAE
  try {
    await updateLastRowWithResult(result);
  } catch (e) {
    const msg = logError('SHEETS_UPDATE', e);
    await bot.sendMessage(chatId, `✅ Factura emitida\nCAE: ${result.CAE}\nVence: ${result.CAEFchVto}\nNro: ${result.voucher_number}\n⚠️ Pero no pude escribir el resultado en tu planilla: ${msg}`);
    return;
  }

  // OK final
  return bot.sendMessage(
    chatId,
    `✅ Factura emitida\nCAE: ${result.CAE}\nVence: ${result.CAEFchVto}\nNro: ${result.voucher_number}`
  );
});

// ====== START ======
app.listen(PORT, () => {
  console.log('Server on', PORT, '| PROD=', AFIP_PROD, '| PtoVta=', AFIP_PTO_VTA, '| Tipo=', AFIP_CBTE_TIPO);
});
