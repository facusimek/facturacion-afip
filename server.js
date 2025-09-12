// server.js
'use strict';

require('dotenv').config();
const express = require('express');
const bodyParser = require('body-parser');
const { google } = require('googleapis');
const TelegramBot = require('node-telegram-bot-api');
const Afip = require('@afipsdk/afip.js');

// PDF/QR y utilidades
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

const AFIP_CUIT = Number(process.env.AFIP_CUIT || '20409378472'); // homologación
const AFIP_PROD = String(process.env.AFIP_PROD || 'false') === 'true';
const AFIP_PTO_VTA = Number(process.env.AFIP_PTO_VTA || '1');
const AFIP_CBTE_TIPO = Number(process.env.AFIP_CBTE_TIPO || '11'); // 11 = Factura C

// Producción (si AFIP_PROD=true)
const AFIP_CERT = process.env.AFIP_CERT ? process.env.AFIP_CERT.replace(/\\n/g, '\n') : undefined;
const AFIP_KEY  = process.env.AFIP_KEY  ? process.env.AFIP_KEY.replace(/\\n/g, '\n') : undefined;

// Drive opcional
const DRIVE_FOLDER_ID = process.env.DRIVE_FOLDER_ID || '';

// Datos de emisor para el PDF (opcionales)
const EM_NOMBRE  = process.env.EMISOR_NOMBRE  || `CUIT ${AFIP_CUIT}`;
const EM_DOM     = process.env.EMISOR_DOMICILIO || '';
const EM_RESPIVA = process.env.EMISOR_RESP_IVA || 'Monotributista';
const EM_IIBB    = process.env.EMISOR_IIBB || '';
const EM_INI     = process.env.EMISOR_INICIO_ACT || '';

// Timeouts (ms)
const TG_TIMEOUT_MS = 12000;
const AFIP_TIMEOUT_MS = 20000;
const DRIVE_TIMEOUT_MS = 15000;
const PDF_TIMEOUT_MS = 12000;

// ====== HELPERS ======
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
function withTimeout(promise, ms, label='OP') {
  return Promise.race([
    promise,
    new Promise((_, rej) => setTimeout(() => rej(new Error(`${label} timeout a ${ms}ms`)), ms))
  ]);
}

// ---- Montos AR: "5.000,50" | "5000.50" | "5,000.50" → número ----
function parseMonto(str) {
  if (typeof str !== 'string') str = String(str ?? '');
  const s = str.trim();
  if (s.includes('.') && s.includes(',')) {
    return Number(s.replace(/\./g, '').replace(',', '.').replace(/[^0-9.]/g, ''));
  }
  if (s.includes(',') && !s.includes('.')) {
    return Number(s.replace(/\./g, '').replace(',', '.').replace(/[^0-9.]/g, ''));
  }
  return Number(s.replace(/[^0-9.]/g, ''));
}

// ---- CUIT válido (módulo 11) ----
function esCUITValido(cuit) {
  const s = String(cuit || '').replace(/\D/g, '');
  if (s.length !== 11) return false;
  const mult = [5,4,3,2,7,6,5,4,3,2];
  let sum = 0;
  for (let i=0;i<10;i++) sum += parseInt(s[i],10)*mult[i];
  let dv = 11 - (sum % 11);
  if (dv === 11) dv = 0;
  if (dv === 10) dv = 9;
  return dv === parseInt(s[10],10);
}

// ====== GOOGLE CLIENTS ======
const auth = new google.auth.GoogleAuth({
  credentials: JSON.parse(process.env.GOOGLE_SA_JSON),
  scopes: [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive.file' // subir PDFs si querés
  ]
});
const sheets = google.sheets({ version: 'v4', auth });
const drive = google.drive({ version: 'v3', auth });

// === DRIVE con OAuth del usuario (usa tu cuota) ===
const oauth2Client = new google.auth.OAuth2(
  process.env.GOOGLE_OAUTH_CLIENT_ID,
  process.env.GOOGLE_OAUTH_CLIENT_SECRET
);
if (process.env.GOOGLE_OAUTH_REFRESH_TOKEN) {
  oauth2Client.setCredentials({ refresh_token: process.env.GOOGLE_OAUTH_REFRESH_TOKEN });
}
const driveUser = process.env.GOOGLE_OAUTH_REFRESH_TOKEN
  ? google.drive({ version: 'v3', auth: oauth2Client })
  : null;
const DRIVE_MODE = driveUser ? 'OAUTH_USER' : 'SERVICE_ACCOUNT';
console.log('[DRIVE] Mode:', DRIVE_MODE);

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

// Helpers Telegram con timeout
async function sendTgMessage(chatId, text, opts) {
  try { return await withTimeout(bot.sendMessage(chatId, text, opts), TG_TIMEOUT_MS, 'Telegram sendMessage'); }
  catch (e) { logError('TG_SEND_MSG', e); }
}
async function sendTgDocument(chatId, filePath, opts) {
  try { return await withTimeout(bot.sendDocument(chatId, fs.createReadStream(filePath), opts), TG_TIMEOUT_MS, 'Telegram sendDocument'); }
  catch (e) { logError('TG_SEND_DOC', e); }
}

// ====== EXPRESS ======
const app = express();
app.use(bodyParser.json());

// Telegram webhook endpoint
app.post('/telegram', (req, res) => {
  bot.processUpdate(req.body);
  res.sendStatus(200);
});

// Healthcheck
app.get('/', (_, res) => res.send('OK'));

// Diagnósticos
app.get('/diag/sheets', async (_, res) => {
  try {
    const dummy = {
      fecha: new Date().toISOString().slice(0,10),
      cliente_nombre: 'TEST',
      doc_tipo: 'DNI',
      doc_nro: '12345678',
      concepto: 2,
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
    const st = await withTimeout(afip.ElectronicBilling.getServerStatus(), 8000, 'AFIP status');
    res.send('AFIP OK: ' + JSON.stringify(st));
  } catch (e) { res.status(500).send('AFIP ERROR: ' + humanError(e)); }
});
app.get('/diag/drive', async (_, res) => {
  try {
    if (!DRIVE_FOLDER_ID) return res.status(400).send('Falta DRIVE_FOLDER_ID');
    const drv = driveUser || drive;
    const tmp = '/tmp/drive-test.txt';
    fs.writeFileSync(tmp, 'hello drive');
    const up = await drv.files.create({
      requestBody: { name: 'drive-test.txt', parents: [DRIVE_FOLDER_ID] },
      media: { mimeType: 'text/plain', body: fs.createReadStream(tmp) },
      fields: 'id, webViewLink, parents',
      supportsAllDrives: true
    });
    res.send('DRIVE OK: ' + JSON.stringify(up.data));
  } catch (e) {
    res.status(500).send('DRIVE ERROR: ' + humanError(e));
  }
});
app.get('/diag/whoami', async (_, res) => {
  try {
    if (driveUser) {
      const about = await driveUser.about.get({ fields: 'user(emailAddress,displayName)' });
      return res.send(`Drive auth: OAUTH como ${about.data.user?.emailAddress}`);
    }
    const saEmail = (() => {
      try { return JSON.parse(process.env.GOOGLE_SA_JSON)?.client_email; } catch { return ''; }
    })();
    return res.send(`Drive auth: SERVICE_ACCOUNT ${saEmail}`);
  } catch (e) {
    return res.status(500).send('whoami error: ' + humanError(e));
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
    doc_nro = docCampo.replace(/\D/g, '');
  }

  const total = parseMonto(totalStr);
  return {
    fecha: new Date().toISOString().slice(0,10),
    cliente_nombre: nombre,
    doc_tipo,
    doc_nro,
    concepto: 2, // solo servicios
    detalle,
    total,
    pto_vta: AFIP_PTO_VTA,
    cbte_tipo: AFIP_CBTE_TIPO
  };
}

// Normaliza receptor para evitar rechazos típicos
function normalizarReceptor(row) {
  const r = { ...row };
  const dt = (r.doc_tipo || '').toUpperCase();

  if (dt === 'CUIT') {
    const n = String(r.doc_nro || '').replace(/\D/g, '');
    if (!esCUITValido(n)) { r.doc_tipo = 'CF'; r.doc_nro = '0'; }
    else { r.doc_nro = n; }
  } else if (dt === 'DNI') {
    const n = String(r.doc_nro || '').replace(/\D/g, '');
    if (n.length < 7 || n.length > 8) { r.doc_tipo = 'CF'; r.doc_nro = '0'; }
    else { r.doc_nro = n; }
  } else {
    r.doc_tipo = 'CF'; r.doc_nro = '0';
  }
  return r;
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
function docTipoCode(t) {
  const u = String(t || '').toUpperCase();
  if (u === 'CUIT') return 80;
  if (u === 'DNI') return 96;
  return 99; // CF / desconocido
}

// Condición IVA del receptor (RG 5616)
function getCondicionIVAReceptorId(parsed) {
  const u = (parsed.doc_tipo || '').toUpperCase();
  if (u === 'DNI' || u === 'CF') return 5; // Consumidor Final
  const def = Number(process.env.IVA_COND_RECEPTOR_ID_DEFAULT || '6'); // 6=Monotributo
  return def;
}

// QR AFIP (RG 4892)
function afipQrUrl({ fechaISO, ptoVta, tipoCmp, nroCmp, importe, tipoDocRec, nroDocRec, cae }) {
  const payload = {
    ver: 1,
    fecha: fechaISO,
    cuit: AFIP_CUIT,
    ptoVta: Number(ptoVta),
    tipoCmp: Number(tipoCmp),
    nroCmp: Number(nroCmp),
    importe: Number(importe),
    moneda: 'PES',
    ctz: 1,
    tipoDocRec: Number(tipoDocRec || 99),
    nroDocRec: Number(nroDocRec || 0),
    tipoCodAut: 'E',
    codAut: Number(cae)
  };
  const base64url = Buffer.from(JSON.stringify(payload))
    .toString('base64').replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
  return 'https://www.afip.gob.ar/fe/qr/?p=' + base64url;
}

function formatARS(n) {
  const v = Number(n || 0);
  return '$ ' + v.toFixed(2)
    .replace('.', ',')
    .replace(/\B(?=(\d{3})+(?!\d))/g, '.');
}

// ====== PDF legible + FIX de finalización ======
async function generarPDF({ row, result }) {
  const fileName = `Factura_C_${String(row.pto_vta).padStart(4,'0')}-${String(result.voucher_number).padStart(8,'0')}.pdf`;
  const filePath = path.join('/tmp', fileName);

  const doc = new PDFDocument({ size: 'A4', margin: 36 });
  const out = fs.createWriteStream(filePath);
  doc.pipe(out);

  const usableW = doc.page.width - 72;      // ancho útil (margen 36)
  const startY = 36;

  // Header: letra C
  doc.rect(36, startY, 40, 40).stroke();
  doc.fontSize(24).text('C', 36, startY + 7, { width: 40, align: 'center' });

  // Datos emisor
  const emX = 36 + 50;
  doc.fontSize(12).text(EM_NOMBRE, emX, startY);
  doc.fontSize(9).fillColor('#333');
  if (EM_DOM)       doc.text(EM_DOM, emX, doc.y);
  doc.text(`CUIT: ${AFIP_CUIT}   |   Resp. IVA: ${EM_RESPIVA}`, emX, doc.y);
  if (EM_IIBB)      doc.text(`Ing. Brutos: ${EM_IIBB}`, emX, doc.y);
  if (EM_INI)       doc.text(`Inicio de actividades: ${EM_INI}`, emX, doc.y);
  doc.fillColor('black');

  // Datos comprobante (derecha)
  const compBoxW = 200, compX = 36 + usableW - compBoxW;
  const compY = startY;
  doc.rect(compX, compY, compBoxW, 40).stroke();
  doc.fontSize(10).text(`Factura C`, compX + 8, compY + 6);
  doc.text(`Pto Vta: ${String(row.pto_vta).padStart(4, '0')}   N°: ${String(result.voucher_number).padStart(8, '0')}`, compX + 8, doc.y);
  doc.text(`Fecha: ${row.fecha}`, compX + 8, doc.y);

  doc.moveDown(0.5);
  // Receptor box
  const recY = compY + 50;
  doc.rect(36, recY, usableW, 60).stroke();
  doc.fontSize(10).text(`Receptor: ${row.cliente_nombre}`, 42, recY + 6);
  doc.text(`Documento: ${(row.doc_tipo || '-').toUpperCase()} ${row.doc_nro || '-'}`, 42, doc.y);
  doc.text(`Cond. IVA: ${(row.doc_tipo || '').toUpperCase() === 'CUIT' ? (process.env.IVA_COND_RECEPTOR_ID_DEFAULT ? 'Resp. Inscripto/Monotributo' : 'CUIT') : 'Consumidor Final'}`, 42, doc.y);

  // Ítems (simple: 1 renglón con el detalle)
  const tableY = recY + 70;
  const cols = [
    { title: 'Descripción', x: 36, w: usableW - 200 },
    { title: 'Cant.', x: 36 + (usableW - 200), w: 50, align: 'right' },
    { title: 'P. Unit.', x: 36 + (usableW - 150), w: 75, align: 'right' },
    { title: 'Importe', x: 36 + (usableW - 75), w: 75, align: 'right' }
  ];

  // header tabla
  doc.rect(36, tableY, usableW, 20).fillAndStroke('#f2f2f2', '#000');
  doc.fillColor('#000').fontSize(9);
  cols.forEach(c => doc.text(c.title, c.x + 6, tableY + 6, { width: c.w - 12, align: c.align || 'left' }));

  // fila item
  const itemY = tableY + 20;
  doc.rect(36, itemY, usableW, 22).stroke();
  const cant = 1;
  const unit = Number(row.total);
  const imp = unit * cant;
  doc.text(row.detalle || 'Servicio', cols[0].x + 6, itemY + 6, { width: cols[0].w - 12 });
  doc.text(String(cant), cols[1].x + 6, itemY + 6, { width: cols[1].w - 12, align: 'right' });
  doc.text(formatARS(unit), cols[2].x + 6, itemY + 6, { width: cols[2].w - 12, align: 'right' });
  doc.text(formatARS(imp), cols[3].x + 6, itemY + 6, { width: cols[3].w - 12, align: 'right' });

  // Totales
  const totY = itemY + 30;
  doc.rect(36 + usableW - 200, totY, 200, 45).stroke();
  doc.fontSize(10).text('Subtotal:', 36 + usableW - 190, totY + 8, { width: 120, align: 'left' });
  doc.text(formatARS(imp), 36 + usableW - 90, totY + 8, { width: 80, align: 'right' });
  doc.text('TOTAL:', 36 + usableW - 190, totY + 25, { width: 120, align: 'left' });
  doc.fontSize(12).text(formatARS(imp), 36 + usableW - 90, totY + 22, { width: 80, align: 'right' });

  // CAE y QR
  const caeY = totY + 60;
  const qrSize = 120;
  // Box CAE
  doc.rect(36, caeY, usableW - qrSize - 12, qrSize).stroke();
  doc.fontSize(10)
     .text(`CAE: ${result.CAE}`, 42, caeY + 12)
     .text(`Vto CAE: ${result.CAEFchVto}`, 42, doc.y)
     .text(`Fecha comp.: ${row.fecha}`, 42, doc.y);

  // QR
  const qrUrl = afipQrUrl({
    fechaISO: row.fecha,
    ptoVta: row.pto_vta,
    tipoCmp: row.cbte_tipo,
    nroCmp: result.voucher_number,
    importe: row.total,
    tipoDocRec: (row.doc_tipo || '').toUpperCase() === 'CUIT' ? 80 : (row.doc_tipo || '').toUpperCase() === 'DNI' ? 96 : 99,
    nroDocRec: row.doc_nro,
    cae: result.CAE
  });
  const qrDataURL = await QRCode.toDataURL(qrUrl, { margin: 1, scale: 6 });
  const qrBase64 = qrDataURL.split(',')[1];
  const qrBuffer = Buffer.from(qrBase64, 'base64');
  doc.image(qrBuffer, 36 + usableW - qrSize, caeY, { width: qrSize, height: qrSize });

  // Pie
  doc.fontSize(8).fillColor('#666').text(qrUrl, 36, caeY + qrSize + 6);
  doc.fillColor('#000');

  doc.end();

  await new Promise((resolve, reject) => {
    out.on('finish', resolve);
    out.on('error', reject);
    doc.on('error', reject);
  });

  return { filePath, fileName };
}

// Subir PDF a Google Drive (usa OAuth si está disponible)
async function subirPDFaDrive({ filePath, fileName }) {
  if (!DRIVE_FOLDER_ID) return null;
  const drv = driveUser || drive; // <— usa OAuth del usuario si existe

  const fileMeta = { name: fileName, parents: [DRIVE_FOLDER_ID] };
  const media = { mimeType: 'application/pdf', body: fs.createReadStream(filePath) };
  try {
    const res = await withTimeout(
      drv.files.create({
        requestBody: fileMeta,
        media,
        fields: 'id, webViewLink, webContentLink, parents',
        supportsAllDrives: true
      }),
      DRIVE_TIMEOUT_MS,
      'Drive upload'
    );
    return res.data;
  } catch (e) {
    const msg = 'Drive: ' + humanError(e);
    console.error('[DRIVE_UPLOAD]', msg);
    throw new Error(msg);
  }
}

async function emitirFactura(row) {
  const norm = normalizarReceptor(row);
  const cbteFch = toYYYYMMDD(norm.fecha);

  const data = {
    CantReg: 1,
    PtoVta: Number(norm.pto_vta),
    CbteTipo: Number(norm.cbte_tipo),   // 11 = Factura C
    Concepto: Number(norm.concepto),    // 2 = Servicios
    DocTipo: docTipoCode(norm.doc_tipo),
    DocNro: Number(norm.doc_nro),
    CbteFch: cbteFch,
    ImpTotal: Number(norm.total),
    ImpTotConc: 0,
    ImpNeto: Number(norm.total),
    ImpIVA: 0,
    ImpTrib: 0,
    MonId: 'PES',
    MonCotiz: 1,
    CondicionIVAReceptorId: getCondicionIVAReceptorId(norm)
    // NO enviar "Iva" en Factura C
  };

  if (data.Concepto === 2 || data.Concepto === 3) {
    data.FchServDesde = cbteFch;
    data.FchServHasta = cbteFch;
    data.FchVtoPago   = cbteFch;
  }

  console.log('AFIP createNextVoucher START', { DocTipo: data.DocTipo, DocNro: data.DocNro, Total: data.ImpTotal });
  const res = await withTimeout(
    afip.ElectronicBilling.createNextVoucher(data),
    AFIP_TIMEOUT_MS,
    'AFIP createNextVoucher'
  );
  console.log('AFIP createNextVoucher DONE');

  return { CAE: res.CAE, CAEFchVto: res.CAEFchVto, voucher_number: res.voucher_number, norm };
}

async function updateLastRowWithResult(result) {
  const get = await sheets.spreadsheets.values.get({
    spreadsheetId: SHEET_ID,
    range: `${SHEET_NAME}!A:Z`
  });
  const rows = get.data.values || [];
  for (let i = rows.length - 1; i >= 1; i--) {
    if (rows[i][9] === 'PENDIENTE') {
      const rowIndex = i + 1;
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

// Marca la última fila PENDIENTE como ERROR y escribe el motivo en col N
async function markLastRowError(errMsg) {
  const get = await sheets.spreadsheets.values.get({
    spreadsheetId: SHEET_ID,
    range: `${SHEET_NAME}!A:Z`
  });
  const rows = get.data.values || [];
  for (let i = rows.length - 1; i >= 1; i--) {
    if (rows[i][9] === 'PENDIENTE') {
      const rowIndex = i + 1;
      const updates = [[ 'ERROR', '', '', '', String(errMsg).slice(0, 500) ]];
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

// ====== TELEGRAM HANDLER ======
bot.on('message', async (msg) => {
  const chatId = msg.chat.id;
  const text = (msg.text || '').trim();

  // Watchdog: si en 35s no se resolvió, avisamos
  let finished = false;
  const watchdog = setTimeout(async () => {
    if (!finished) {
      await sendTgMessage(chatId, '⚠️ Se está demorando más de lo normal. Podés reintentar en unos minutos. Si vuelve a pasar, mirá Logs en Render.');
    }
  }, 35000);

  try {
    if (text === '/start') {
      await sendTgMessage(chatId, 'Hola! Enviame: Nombre | DNI o CUIT | Detalle | Total\nEjemplo:\nJuan Perez | DNI 12345678 | Servicio de diseño | 5000');
      finished = true; clearTimeout(watchdog);
      return;
    }

    const parsed = parseMessage(text);
    if (!parsed) {
      await sendTgMessage(chatId, 'Formato incorrecto. Usá: Nombre | DNI o CUIT | Detalle | Total');
      finished = true; clearTimeout(watchdog);
      return;
    }

    // 1) Google Sheets
    try { await appendRow(parsed); }
    catch (e) {
      const msgErr = logError('SHEETS_APPEND', e);
      await sendTgMessage(chatId, '❌ Error en Google Sheets: ' + msgErr);
      finished = true; clearTimeout(watchdog);
      return;
    }

    // Avances a Telegram
    await sendTgMessage(chatId, '⏳ Recibí los datos. Estoy emitiendo la factura…');
    await sendTgMessage(chatId, '➡️ Enviando solicitud a AFIP…');

    // 2) AFIP (con timeout)
    let result;
    try { result = await emitirFactura(parsed); }
    catch (e) {
      const msgErr = logError('AFIP_EMITIR', e);
      await markLastRowError('AFIP: ' + msgErr);
      await sendTgMessage(chatId, '❌ Error en AFIP: ' + msgErr);
      finished = true; clearTimeout(watchdog);
      return;
    }

    await sendTgMessage(chatId, `🧾 AFIP respondió. Generando PDF… (CAE ${result.CAE})`);

    // 3) PDF y envío (con timeout global)
    let pdfInfo;
    try {
      pdfInfo = await withTimeout(generarPDF({ row: result.norm || parsed, result }), PDF_TIMEOUT_MS, 'PDF build');
      await sendTgDocument(
        chatId,
        pdfInfo.filePath,
        { caption: `Factura C ${String(parsed.pto_vta).padStart(4,'0')}-${String(result.voucher_number).padStart(8,'0')} | CAE ${result.CAE}` }
      );
    } catch (e) {
      logError('PDF', e);
      await sendTgMessage(chatId, '⚠️ La factura salió pero no pude adjuntar el PDF.');
    }

    // 4) Drive (opcional)
    if (DRIVE_FOLDER_ID) {
      await sendTgMessage(chatId, '☁️ Subiendo copia a Drive…');
      try {
        const driveFile = await withTimeout(subirPDFaDrive(pdfInfo || {}), DRIVE_TIMEOUT_MS, 'Drive upload wrapper');
        if (driveFile?.webViewLink) {
          await sendTgMessage(chatId, `📄 Guardé una copia en Drive: ${driveFile.webViewLink}`);
        } else {
          await sendTgMessage(chatId, '⚠️ Subí el archivo pero no recibí link (revisá permisos de la carpeta).');
        }
      } catch (e) {
        await sendTgMessage(chatId, '⚠️ No pude subir a Drive: ' + humanError(e));
      }
    }

    // 5) Actualizar planilla
    try { await updateLastRowWithResult(result); }
    catch (e) {
      const msgErr = logError('SHEETS_UPDATE', e);
      await sendTgMessage(chatId, `✅ Factura emitida\nCAE: ${result.CAE}\nVence: ${result.CAEFchVto}\nNro: ${result.voucher_number}\n⚠️ No pude escribir el resultado en tu planilla: ${msgErr}`);
      finished = true; clearTimeout(watchdog);
      return;
    }

    // 6) Mensaje final OK
    await sendTgMessage(chatId, `✅ Factura emitida\nCAE: ${result.CAE}\nVence: ${result.CAEFchVto}\nNro: ${result.voucher_number}`);
    finished = true; clearTimeout(watchdog);
  } catch (e) {
    const msgErr = logError('HANDLER_FATAL', e);
    await markLastRowError('FATAL: ' + msgErr);
    await sendTgMessage(chatId, '❌ Error inesperado: ' + msgErr);
    finished = true; clearTimeout(watchdog);
  }
});

// ====== START ======
app.listen(PORT, () => {
  console.log('Server on', PORT, '| PROD=', AFIP_PROD, '| PtoVta=', AFIP_PTO_VTA, '| Tipo=', AFIP_CBTE_TIPO);
});
process.on('unhandledRejection', (e) => console.error('[unhandledRejection]', e));
process.on('uncaughtException', (e) => console.error('[uncaughtException]', e));
