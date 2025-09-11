// server.js
'use strict';

require('dotenv').config();
const express = require('express');
const bodyParser = require('body-parser');
const { google } = require('googleapis');
const TelegramBot = require('node-telegram-bot-api');
const Afip = require('@afipsdk/afip.js');

// === NUEVO: librerías para PDF/QR y utilidades de archivos ===
const fs = require('fs');
const path = require('path');
const PDFDocument = require('pdfkit');
const QRCode = require('qrcode');

// ====== CONFIG ======
const PORT = process.env.PORT || 3000;
const SHEET_ID = process.env.SHEET_ID;
const SHEET_NAME = process.env.SHEET_NAME || 'Hoja 1';
const TELEGRAM_TOKEN = process.env.TELEGRAM_TOKEN;
const WEBHOOK_URL = process.env.WEBHOOK_URL;

const AFIP_CUIT = Number(process.env.AFIP_CUIT || '20409378472'); // CUIT test (homologación)
const AFIP_PROD = String(process.env.AFIP_PROD || 'false') === 'true';
const AFIP_PTO_VTA = Number(process.env.AFIP_PTO_VTA || '1');
const AFIP_CBTE_TIPO = Number(process.env.AFIP_CBTE_TIPO || '11'); // 11 = Factura C

// Para producción real (si AFIP_PROD=true)
const AFIP_CERT = process.env.AFIP_CERT ? process.env.AFIP_CERT.replace(/\\n/g, '\n') : undefined;
const AFIP_KEY  = process.env.AFIP_KEY  ? process.env.AFIP_KEY.replace(/\\n/g, '\n') : undefined;

// === NUEVO: carpeta de Google Drive (opcional)
const DRIVE_FOLDER_ID = process.env.DRIVE_FOLDER_ID || '';

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
  } catch (_) { return 'Error desconocido'; }
}
function logError(prefix, e) {
  const msg = humanError(e);
  console.error(`[${prefix}]`, msg, e?.stack ? `\nSTACK:\n${e.stack}` : '');
  return msg;
}
function toYYYYMMDD(dateStr) {
  const d = dateStr ? new Date(dateStr) : new Date();
  const ok = !isNaN(d.getTime());
  const dd = ok ? d : new Date();
  const y = dd.getFullYear();
  const m = String(dd.getMonth() + 1).padStart(2, '0');
  const day = String(dd.getDate()).padStart(2, '0');
  return `${y}${m}${day}`;
}

// ====== GOOGLE CLIENTS ======
const auth = new google.auth.GoogleAuth({
  credentials: JSON.parse(process.env.GOOGLE_SA_JSON),
  scopes: [
    'https://www.googleapis.com/auth/spreadsheets',
    // === NUEVO: permisos de Drive, necesarios si subís PDFs
    'https://www.googleapis.com/auth/drive.file'
  ]
});
const sheets = google.sheets({ version: 'v4', auth });
// === NUEVO: cliente de Google Drive
const drive = google.drive({ version: 'v3', auth });

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

// ====== EXPRESS ======
const app = express();
app.use(bodyParser.json());

// Endpoint que Telegram llama (no abre en el navegador)
app.post('/telegram', (req, res) => {
  bot.processUpdate(req.body);
  res.sendStatus(200);
});

// Healthcheck
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
  } catch (e) { res.status(500).send('SHEETS ERROR: ' + humanError(e)); }
});

app.get('/diag/afip', async (_, res) => {
  try {
    const st = await afip.ElectronicBilling.getServerStatus();
    res.send('AFIP OK: ' + JSON.stringify(st));
  } catch (e) { res.status(500).send('AFIP ERROR: ' + humanError(e)); }
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

// Condición IVA del receptor (RG 5616)
function getCondicionIVAReceptorId(parsed) {
  if (parsed.doc_tipo === 'DNI') return 5; // Consumidor Final
  const def = Number(process.env.IVA_COND_RECEPTOR_ID_DEFAULT || '6'); // 6=Monotributo por defecto
  return def;
}

// === NUEVO: URL del QR AFIP según RG 4892
function afipQrUrl({ fechaISO, ptoVta, tipoCmp, nroCmp, importe, tipoDocRec, nroDocRec, cae }) {
  const payload = {
    ver: 1,
    fecha: fechaISO,               // "YYYY-MM-DD"
    cuit: AFIP_CUIT,               // CUIT emisor
    ptoVta: Number(ptoVta),
    tipoCmp: Number(tipoCmp),
    nroCmp: Number(nroCmp),
    importe: Number(importe),
    moneda: 'PES',
    ctz: 1,
    tipoDocRec: Number(tipoDocRec || 99), // 96 DNI / 80 CUIT / 99 - No Informado
    nroDocRec: Number(nroDocRec || 0),
    tipoCodAut: 'E',               // electrónico
    codAut: Number(cae)
  };
  const base64url = Buffer.from(JSON.stringify(payload))
    .toString('base64')
    .replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
  return 'https://www.afip.gob.ar/fe/qr/?p=' + base64url;
}

// === NUEVO: Generación de PDF con QR y datos básicos
async function generarPDF({ row, result }) {
  const fileName = `Factura_C_${String(row.pto_vta).padStart(4,'0')}-${String(result.voucher_number).padStart(8,'0')}.pdf`;
  const filePath = path.join('/tmp', fileName); // Render permite escribir en /tmp

  const doc = new PDFDocument({ size: 'A4', margin: 36 });
  doc.pipe(fs.createWriteStream(filePath));

  // Encabezado
  doc.fontSize(18).text('FACTURA C', { align: 'center' });
  doc.moveDown(0.5);
  doc.fontSize(10).text(`Punto de Venta: ${row.pto_vta}  -  Número: ${result.voucher_number}`, { align: 'center' });
  doc.text(`Fecha: ${row.fecha}`, { align: 'center' });
  doc.moveDown();

  // Emisor / Receptor
  doc.fontSize(11).text(`Emisor CUIT: ${AFIP_CUIT}`);
  doc.text(`Receptor: ${row.cliente_nombre}`);
  doc.text(`Doc: ${row.doc_tipo} ${row.doc_nro || '-'}`);
  doc.moveDown();

  // Detalle y total
  doc.text(`Detalle: ${row.detalle}`);
  doc.moveDown(0.5);
  doc.fontSize(14).text(`TOTAL: $ ${Number(row.total).toFixed(2)}`, { align: 'right' });
  doc.moveDown();

  // CAE
  doc.fontSize(11).text(`CAE: ${result.CAE}`);
  doc.text(`Vencimiento CAE: ${result.CAEFchVto}`);
  doc.moveDown();

  // QR AFIP
  const qrUrl = afipQrUrl({
    fechaISO: row.fecha,
    ptoVta: row.pto_vta,
    tipoCmp: row.cbte_tipo,
    nroCmp: result.voucher_number,
    importe: row.total,
    tipoDocRec: (row.doc_tipo || '').toUpperCase() === 'CUIT' ? 80 : 96,
    nroDocRec: row.doc_nro,
    cae: result.CAE
  });
  const qrDataURL = await QRCode.toDataURL(qrUrl, { margin: 1, scale: 6 });
  const qrBase64 = qrDataURL.split(',')[1];
  const qrBuffer = Buffer.from(qrBase64, 'base64');

  doc.text('Código QR AFIP:');
  doc.image(qrBuffer, { fit: [120, 120] });
  doc.moveDown();
  doc.fontSize(8).fillColor('#555').text(qrUrl);

  doc.end();
  await new Promise((res) => doc.on('finish', res));
  return { filePath, fileName };
}

// === NUEVO: Subir PDF a Google Drive (opcional)
async function subirPDFaDrive({ filePath, fileName }) {
  if (!DRIVE_FOLDER_ID) return null;
  const fileMeta = { name: fileName, parents: [DRIVE_FOLDER_ID] };
  const media = { mimeType: 'application/pdf', body: fs.createReadStream(filePath) };
  const res = await drive.files.create({
    requestBody: fileMeta,
    media,
    fields: 'id, webViewLink, webContentLink'
  });
  return res.data; // { id, webViewLink, webContentLink }
}

function docTipoCode(t) { return String(t).toUpperCase() === 'CUIT' ? 80 : 96; } // 80=CUIT, 96=DNI

async function emitirFactura(row) {
  // Fechas (obligatorias para servicios)
  const cbteFch = toYYYYMMDD(row.fecha);

  const data = {
    CantReg: 1,
    PtoVta: Number(row.pto_vta),
    CbteTipo: Number(row.cbte_tipo),   // 11 = Factura C
    Concepto: Number(row.concepto),    // 2 = Servicios
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
    // NO incluir "Iva" para Factura C (evita error 10071)
    CondicionIVAReceptorId: getCondicionIVAReceptorId(row)
  };

  // Fechas obligatorias para Concepto 2 o 3
  if (data.Concepto === 2 || data.Concepto === 3) {
    data.FchServDesde = cbteFch;
    data.FchServHasta = cbteFch;
    data.FchVtoPago   = cbteFch;
  }

  console.log(
    'CbteTipo=', data.CbteTipo,
    '| CondIVARec=', data.CondicionIVAReceptorId,
    '| Concepto=', data.Concepto,
    '| FchServ=', data.FchServDesde, data.FchServHasta, data.FchVtoPago
  );

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

// ====== HANDLER TELEGRAM ======
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

  // Paso 1: Google Sheets
  try {
    await appendRow(parsed);
  } catch (e) {
    const msgErr = logError('SHEETS_APPEND', e);
    return bot.sendMessage(chatId, '❌ Error en Google Sheets: ' + msgErr);
  }

  // Paso 2: AFIP
  let result;
  try {
    result = await emitirFactura(parsed);
  } catch (e) {
    const msgErr = logError('AFIP_EMITIR', e);
    return bot.sendMessage(chatId, '❌ Error en AFIP: ' + msgErr);
  }

  // === NUEVO: generar PDF y enviarlo por Telegram
  let pdfInfo;
  try {
    pdfInfo = await generarPDF({ row: parsed, result });
    await bot.sendDocument(chatId, fs.createReadStream(pdfInfo.filePath), {
      caption: `Factura C ${String(parsed.pto_vta).padStart(4,'0')}-${String(result.voucher_number).padStart(8,'0')} | CAE ${result.CAE}`
    });
  } catch (e) {
    console.error('[PDF]', humanError(e));
  }

  // === NUEVO: subir a Google Drive (si DRIVE_FOLDER_ID está configurado)
  try {
    const driveFile = await subirPDFaDrive(pdfInfo || {});
    if (driveFile?.webViewLink) {
      await bot.sendMessage(chatId, `📄 Guardé una copia en Drive: ${driveFile.webViewLink}`);
    }
  } catch (e) {
    console.error('[DRIVE]', humanError(e));
  }

  // Paso 3: actualizar fila (CAE)
  try {
    await updateLastRowWithResult(result);
  } catch (e) {
    const msgErr = logError('SHEETS_UPDATE', e);
    await bot.sendMessage(chatId, `✅ Factura emitida\nCAE: ${result.CAE}\nVence: ${result.CAEFchVto}\nNro: ${result.voucher_number}\n⚠️ No pude escribir el resultado en tu planilla: ${msgErr}`);
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
