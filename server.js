// server.js
//
// Requisitos en package.json (asegurate que estén instalados):
//   "express", "body-parser", "node-telegram-bot-api", "googleapis", "pdfkit", "@afipsdk/afip.js"
//   (y opcionalmente "dotenv" si querés cargar .env local)
//
// ============================
//  Variables de entorno (.env)
// ============================
//
// # Telegram / Render
// TELEGRAM_BOT_TOKEN=123456:ABC...
// WEBHOOK_URL=https://tuapp.onrender.com
//
// # Google OAuth (Drive + Sheets) usando tu cuenta (NO Service Account)
// GOOGLE_CLIENT_ID=xxxxxxxx.apps.googleusercontent.com
// GOOGLE_CLIENT_SECRET=xxxxxxxx
// GOOGLE_REDIRECT_URI=urn:ietf:wg:oauth:2.0:oob   # o el de tu consola si usaste otro
// GOOGLE_REFRESH_TOKEN=1//0g....
//
// # Google Sheets
// GOOGLE_SHEETS_SPREADSHEET_ID=xxxxxxxxxxxxxxxxxxxxxxxxxxxxx
// SHEETS_TAB_FACTURAS=Facturas          # hoja donde se registran pedidos/facturas
// SHEETS_TAB_PACIENTES=Pacientes        # base de datos de pacientes
//
// # Columnas esperadas en SHEETS_TAB_FACTURAS (en orden):
// # Ejemplo: ["Estado","Fecha","DNI/CUIT","Nombre","Domicilio","Descripción","Cantidad","Unidad","Precio unitario (ARS)","Total (ARS)","Tipo","PtoVta","CbteNro","CAE","VtoCAE","Mensaje","PDF Link"]
// # Si tu orden es distinto, ajustá los índices en FACT_COLS más abajo.
//
// # Google Drive
// GOOGLE_DRIVE_FOLDER_ID=xxxxxxxxxxxxxx   # opcional (carpeta destino). Si no está, sube a Mi unidad.
//
// # AFIP
// AFIP_CUIT=20123456789
// AFIP_CERT_BASE64=...   # tu .crt en Base64 (sin saltos) o deja vacío si usás ruta
// AFIP_KEY_BASE64=...    # tu .key en Base64 (sin saltos) o deja vacío si usás ruta
// AFIP_PROD=false        # "true" para producción, "false" homologación
// AFIP_PTO_VTA=1         # tu punto de venta habilitado para FE
// AFIP_TIPO_CBTE=11      # 11 = Factura C, 13 = Nota de Crédito C, etc.
//
// # PDF
// EMIRO_LOGO_BASE64=/9j/4AAQ...           # PNG/JPG en Base64 (opcional)
//
// =======================================

const express = require('express');
const bodyParser = require('body-parser');
const TelegramBot = require('node-telegram-bot-api');
const { google } = require('googleapis');
const PDFDocument = require('pdfkit');
const fs = require('fs');
const path = require('path');
const Afip = require('@afipsdk/afip.js');

// -------------------- Utiles --------------------
const PORT = process.env.PORT || process.env.RENDER_PORT || 10000;
const BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN;
const WEBHOOK_URL = process.env.WEBHOOK_URL;

if (!BOT_TOKEN) {
  console.error('Falta TELEGRAM_BOT_TOKEN');
  process.exit(1);
}

// Google
const GOOGLE_CLIENT_ID = process.env.GOOGLE_CLIENT_ID;
const GOOGLE_CLIENT_SECRET = process.env.GOOGLE_CLIENT_SECRET;
const GOOGLE_REDIRECT_URI = process.env.GOOGLE_REDIRECT_URI || 'urn:ietf:wg:oauth:2.0:oob';
const GOOGLE_REFRESH_TOKEN = process.env.GOOGLE_REFRESH_TOKEN;
const SPREADSHEET_ID = process.env.GOOGLE_SHEETS_SPREADSHEET_ID;
const TAB_FACTURAS = process.env.SHEETS_TAB_FACTURAS || 'Facturas';
const TAB_PACIENTES = process.env.SHEETS_TAB_PACIENTES || 'Pacientes';
const DRIVE_FOLDER_ID = process.env.GOOGLE_DRIVE_FOLDER_ID || null;

// AFIP
const AFIP_CUIT = Number(process.env.AFIP_CUIT);
const AFIP_PROD = String(process.env.AFIP_PROD || 'false').toLowerCase() === 'true';
const AFIP_PTO_VTA = Number(process.env.AFIP_PTO_VTA || 1);
const AFIP_TIPO_CBTE = Number(process.env.AFIP_TIPO_CBTE || 11); // 11 = Factura C
const AFIP_CERT_BASE64 = process.env.AFIP_CERT_BASE64 || '';
const AFIP_KEY_BASE64 = process.env.AFIP_KEY_BASE64 || '';

// PDF
const LOGO_BASE64 = process.env.EMIRO_LOGO_BASE64 || '';

// Columna fija: si tu hoja cambia, ajustá aquí los índices
// (0-based)
const FACT_COLS = {
  ESTADO: 0,
  FECHA: 1,
  DOC: 2,
  NOMBRE: 3,
  DOMICILIO: 4,
  DESCRIPCION: 5,
  CANTIDAD: 6,
  UNIDAD: 7,
  PRECIO_UNIT: 8,
  TOTAL: 9,
  TIPO: 10,      // "C"
  PTO_VTA: 11,
  CbteNro: 12,
  CAE: 13,
  VTO_CAE: 14,
  MENSAJE: 15,
  PDF_LINK: 16,
};

// -------------------- Google: OAuth2 --------------------
const oAuth2Client = new google.auth.OAuth2(
  GOOGLE_CLIENT_ID,
  GOOGLE_CLIENT_SECRET,
  GOOGLE_REDIRECT_URI
);
if (GOOGLE_REFRESH_TOKEN) {
  oAuth2Client.setCredentials({ refresh_token: GOOGLE_REFRESH_TOKEN });
}
const sheets = google.sheets({ version: 'v4', auth: oAuth2Client });
const drive = google.drive({ version: 'v3', auth: oAuth2Client });

// -------------------- AFIP SDK --------------------
let afipInstance = null;
function getAfip() {
  if (afipInstance) return afipInstance;

  let cert = undefined;
  let key = undefined;

  if (AFIP_CERT_BASE64) {
    cert = Buffer.from(AFIP_CERT_BASE64, 'base64').toString('utf8');
    fs.writeFileSync(path.join(__dirname, 'afip.crt'), cert, { encoding: 'utf8' });
    cert = path.join(__dirname, 'afip.crt');
  }
  if (AFIP_KEY_BASE64) {
    key = Buffer.from(AFIP_KEY_BASE64, 'base64').toString('utf8');
    fs.writeFileSync(path.join(__dirname, 'afip.key'), key, { encoding: 'utf8' });
    key = path.join(__dirname, 'afip.key');
  }

  afipInstance = new Afip({
    CUIT: AFIP_CUIT,
    production: AFIP_PROD,
    cert, // path (si definimos arriba)
    key,  // path (si definimos arriba)
    res_folder: path.join(__dirname, 'afip'), // opcional
  });
  return afipInstance;
}

// -------------------- Express + Telegram --------------------
const app = express();
app.use(bodyParser.json());

const bot = new TelegramBot(BOT_TOKEN);
// Webhook
if (!WEBHOOK_URL) {
  console.error('Falta WEBHOOK_URL');
  process.exit(1);
}
bot.setWebHook(`${WEBHOOK_URL}/telegram`);

app.post('/telegram', (req, res) => {
  bot.processUpdate(req.body);
  res.sendStatus(200);
});

app.get('/', (_req, res) => {
  res.send('AFIP OK');
});

// -------------------- Helpers --------------------
const toARNumber = (n) =>
  Number(n).toLocaleString('es-AR', { minimumFractionDigits: 2, maximumFractionDigits: 2 });

const normalizarNumero = (s = '') => String(s).replace(/\./g, '').replace(',', '.').trim();

const redondear2 = (n) =>
  Math.round((Number(n) + Number.EPSILON) * 100) / 100;

function hoy() {
  const d = new Date();
  d.setHours(d.getHours() - 3); // AR -0300 simple
  return d;
}
function yyyymmdd(d = new Date()) {
  const yyyy = d.getFullYear();
  const mm = String(d.getMonth() + 1).padStart(2, '0');
  const dd = String(d.getDate()).padStart(2, '0');
  return `${yyyy}${mm}${dd}`;
}

// buscar paciente por DNI/CUIT
async function buscarPacientePorDoc(docValor) {
  if (!SPREADSHEET_ID) return null;
  const resp = await sheets.spreadsheets.values.get({
    spreadsheetId: SPREADSHEET_ID,
    range: `${TAB_PACIENTES}!A:Z`,
  });
  const rows = resp.data.values || [];
  // Buscamos columnas: DNI/CUIT, Nombre, Domicilio
  // Supongamos encabezados en fila 1:
  if (!rows.length) return null;
  const headers = rows[0].map((h) => (h || '').toString().trim().toLowerCase());
  const idxDoc = headers.findIndex((h) => ['dni', 'cuit', 'dni/cuit'].includes(h));
  const idxNombre = headers.findIndex((h) => h.includes('nombre'));
  const idxDom = headers.findIndex((h) => h.includes('domic'));
  if (idxDoc < 0) return null;

  for (let i = 1; i < rows.length; i++) {
    const r = rows[i];
    const v = (r[idxDoc] || '').toString().replace(/\D/g, '');
    const busc = (docValor || '').toString().replace(/\D/g, '');
    if (v && busc && v === busc) {
      return {
        nombre: (r[idxNombre] || '').toString().trim(),
        domicilio: (r[idxDom] || '').toString().trim(),
      };
    }
  }
  return null;
}

function parsearMensaje(text) {
  // Extrae: dni|cuit, cantidad/cant, precio/valor, unidad, desc|descripcion, fecha|desde|hasta
  const out = {};
  const s = (text || '').toLowerCase();

  // DNI o CUIT (tomamos números)
  const dniMatch = s.match(/dni\s+([\d\.]+)/);
  const cuitMatch = s.match(/cuit\s+([\d\-\.\s]+)/);
  if (dniMatch) out.docTipo = 96, out.doc = dniMatch[1].replace(/\D/g, ''); // 96=DN
  if (cuitMatch) out.docTipo = 80, out.doc = cuitMatch[1].replace(/\D/g, ''); // 80=CUIT

  // cantidad
  const cantMatch = s.match(/(?:cantidad|cant)\s+([\d\.,]+)/);
  if (cantMatch) out.cantidad = Number(normalizarNumero(cantMatch[1]));

  // precio
  const precioMatch = s.match(/(?:precio|valor)\s+([\d\.,]+)/);
  if (precioMatch) out.precio = Number(normalizarNumero(precioMatch[1]));

  // unidad
  const unidadMatch = s.match(/unidad\s+([a-záéíóú]+)/);
  if (unidadMatch) out.unidad = unidadMatch[1];

  // desc
  const descMatch = s.match(/(?:desc|descripción|descripcion)\s+(.+?)(?=$|fecha|desde|hasta)/);
  if (descMatch) out.desc = descMatch[1].trim();

  // fechas
  const fechaMatch = s.match(/fecha\s+(\d{4}-\d{2}-\d{2}|\d{8})/);
  if (fechaMatch) out.fecha = fechaMatch[1];

  const desdeMatch = s.match(/desde\s+(\d{4}-\d{2}-\d{2}|\d{8})/);
  const hastaMatch = s.match(/hasta\s+(\d{4}-\d{2}-\d{2}|\d{8})/);
  if (desdeMatch) out.desde = desdeMatch[1];
  if (hastaMatch) out.hasta = hastaMatch[1];

  return out;
}

function parseFechaAAAAMMDD(s) {
  if (!s) return null;
  if (/^\d{8}$/.test(s)) return s;
  if (/^\d{4}-\d{2}-\d{2}$/.test(s)) {
    return s.replace(/-/g, '');
  }
  return null;
}

function escapeHtml(x='') {
  return x.replace(/[&<>"']/g, (m)=>({ '&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;' }[m]));
}

// -------------------- PDF --------------------
async function generarPDF({ emisor, receptor, detalle, caeInfo }) {
  // archivo temporal
  const fileName = `factura_${detalle.CbteFch}_${detalle.CbteDesde}.pdf`;
  const filePath = path.join('/tmp', fileName);

  return new Promise((resolve, reject) => {
    const doc = new PDFDocument({ size: 'A4', margin: 36 });
    const stream = fs.createWriteStream(filePath);
    doc.pipe(stream);

    // Encabezado con logo
    if (LOGO_BASE64) {
      try {
        const buf = Buffer.from(LOGO_BASE64, 'base64');
        doc.image(buf, 36, 36, { width: 120 });
      } catch (_) {}
    }
    doc.fontSize(16).text(`Factura C`, { align: 'right' });
    doc.moveDown(0.3);
    doc.fontSize(10).text(`Punto de venta: ${AFIP_PTO_VTA} - Comp. N°: ${detalle.CbteDesde}`, { align: 'right' });
    if (caeInfo && caeInfo.CAE) {
      doc.text(`CAE: ${caeInfo.CAE}`, { align: 'right' });
      doc.text(`Vto CAE: ${caeInfo.CAEFchVto || ''}`, { align: 'right' });
    }
    doc.moveDown();

    // Emisor
    doc.fontSize(11).text(`Emisor`, { underline: true });
    doc.fontSize(10).text(`CUIT: ${AFIP_CUIT}`);
    doc.text(`Fecha: ${detalle.CbteFch.slice(6,8)}/${detalle.CbteFch.slice(4,6)}/${detalle.CbteFch.slice(0,4)}`);
    doc.moveDown(0.5);

    // Receptor
    doc.fontSize(11).text(`Receptor`, { underline: true });
    doc.fontSize(10).text(`Nombre: ${receptor.Nombre || '-'}`);
    doc.text(`${receptor.DocTipo===80 ? 'CUIT' : 'DNI'}: ${receptor.DocNro || '-'}`);
    doc.text(`Domicilio: ${receptor.Domicilio || '-'}`);
    doc.moveDown(0.5);

    // Periodo servicio (Concepto=2)
    doc.fontSize(11).text(`Servicios`, { underline: true });
    doc.fontSize(10).text(`Desde: ${detalle.FchServDesde.slice(6,8)}/${detalle.FchServDesde.slice(4,6)}/${detalle.FchServDesde.slice(0,4)}  -  Hasta: ${detalle.FchServHasta.slice(6,8)}/${detalle.FchServHasta.slice(4,6)}/${detalle.FchServHasta.slice(0,4)}  -  Vto pago: ${detalle.FchVtoPago.slice(6,8)}/${detalle.FchVtoPago.slice(4,6)}/${detalle.FchVtoPago.slice(0,4)}`);
    doc.moveDown(0.5);

    // Tabla de ítems
    const startY = doc.y + 6;
    const colX = [36, 270, 330, 420, 510]; // Descripción, Cantidad, Unidad, Precio unit, Importe
    doc.fontSize(10).text('Descripción', colX[0], startY);
    doc.text('Cant.', colX[1], startY, { width: 50, align: 'right' });
    doc.text('Unidad', colX[2], startY, { width: 80, align: 'right' });
    doc.text('Precio unit.', colX[3], startY, { width: 80, align: 'right' });
    doc.text('Importe', colX[4], startY, { width: 80, align: 'right' });
    doc.moveTo(36, startY + 14).lineTo(559, startY + 14).stroke();

    let y = startY + 20;
    (detalle.Items || []).forEach(it => {
      doc.text(it.Descripcion || '-', colX[0], y, { width: 220 });
      doc.text(String(it.Cantidad ?? ''), colX[1], y, { width: 50, align: 'right' });
      doc.text(String(it.Unidad ?? ''), colX[2], y, { width: 80, align: 'right' });
      doc.text(`$ ${toARNumber(it.PrecioUnitario || 0)}`, colX[3], y, { width: 80, align: 'right' });
      doc.text(`$ ${toARNumber(it.Importe || 0)}`, colX[4], y, { width: 80, align: 'right' });
      y += 16;
    });

    doc.moveTo(36, y + 4).lineTo(559, y + 4).stroke();
    y += 10;
    doc.fontSize(11).text('TOTAL', 420, y, { width: 80, align: 'right' });
    doc.fontSize(12).text(`$ ${toARNumber(detalle.ImpTotal)}`, 510, y, { width: 80, align: 'right' });

    doc.end();
    stream.on('finish', () => resolve({ filePath, fileName }));
    stream.on('error', reject);
  });
}

// -------------------- Google Drive upload --------------------
async function subirADrive({ filePath, fileName }) {
  const fileMetadata = {
    name: fileName,
    parents: DRIVE_FOLDER_ID ? [DRIVE_FOLDER_ID] : undefined,
  };
  const media = {
    mimeType: 'application/pdf',
    body: fs.createReadStream(filePath),
  };
  const res = await drive.files.create({
    requestBody: fileMetadata,
    media,
    fields: 'id, webViewLink, webContentLink',
  });
  return res.data;
}

// -------------------- Google Sheets helpers --------------------
async function agregarFilaFacturas(datos = []) {
  const resp = await sheets.spreadsheets.values.append({
    spreadsheetId: SPREADSHEET_ID,
    range: `${TAB_FACTURAS}!A:Z`,
    valueInputOption: 'USER_ENTERED',
    insertDataOption: 'INSERT_ROWS',
    requestBody: { values: [datos] },
  });
  // Ubicación fila
  const updates = resp.data.updates;
  // No siempre vuelve la fila directa; hacemos una búsqueda para actualizar luego si hace falta.
  return true;
}

async function leerTodasFacturas() {
  const resp = await sheets.spreadsheets.values.get({
    spreadsheetId: SPREADSHEET_ID,
    range: `${TAB_FACTURAS}!A:Z`,
  });
  return resp.data.values || [];
}

async function actualizarCelda(rowIndex0, colIndex0, valor) {
  await sheets.spreadsheets.values.update({
    spreadsheetId: SPREADSHEET_ID,
    range: `${TAB_FACTURAS}!${colToLetter(colIndex0 + 1)}${rowIndex0 + 1}`,
    valueInputOption: 'USER_ENTERED',
    requestBody: { values: [[valor]] },
  });
}
async function actualizarRango(rowIndex0, desdeCol0, valoresArray) {
  await sheets.spreadsheets.values.update({
    spreadsheetId: SPREADSHEET_ID,
    range: `${TAB_FACTURAS}!${colToLetter(desdeCol0 + 1)}${rowIndex0 + 1}:${colToLetter(desdeCol0 + valoresArray.length)}${rowIndex0 + 1}`,
    valueInputOption: 'USER_ENTERED',
    requestBody: { values: [valoresArray] },
  });
}
function colToLetter(col) {
  let temp = '';
  let letter = '';
  while (col > 0) {
    temp = (col - 1) % 26;
    letter = String.fromCharCode(temp + 65) + letter;
    col = (col - temp - 1) / 26;
  }
  return letter;
}

// -------------------- Telegram: bienvenida --------------------
const BIENVENIDA_HTML =
`<b>¡Hola! Soy tu bot de facturación AFIP</b> 👋

<b>¿Qué hago?</b>
• Tomo tus datos por Telegram, los registro en tu Google Sheets, emito la <b>Factura C</b> (Servicios) en AFIP y genero el <b>PDF</b>. Luego lo subo a tu Google Drive.

<b>Datos mínimos para facturar servicios</b>
• <u>DNI o CUIT del paciente</u> (si ya está en la hoja <i>Pacientes</i>, se completa Nombre y Domicilio automático)
• <u>Cantidad</u> (por ejemplo: 3)
• <u>Precio unitario</u> (por ejemplo: 15000)
• (Opcional) <u>Unidad</u> (por defecto: “sesión”; podés usar “hora”)
• (Opcional) <u>desc</u> (descripción que aparece en la factura)
• (Opcional) <u>fecha</u> o <u>desde</u> y <u>hasta</u> (si no los enviás, uso la fecha de hoy para las tres: Desde/Hasta/Vto Pago)

<b>Formato del mensaje</b>
• <code>facturar dni 12345678 cantidad 3 unidad sesión precio 15000 desc terapia cognitiva fecha 2025-09-12</code>
• <code>facturar cuit 20-12345678-3 cant 5 unidad hora precio 12000 desc sesión online desde 2025-09-10 hasta 2025-09-10</code>

<b>Qué registro en la planilla (pestaña ${TAB_FACTURAS})</b>
• Estado (Pendiente/Emitido/Error), Fecha, DNI/CUIT, Nombre, Domicilio, Descripción, <b>Cantidad</b>, <b>Unidad</b>, <b>Precio unitario</b>, <b>Total</b>, Tipo, PtoVta, N° Comprobante, CAE, Vto CAE, Mensaje y link al PDF.

<b>Notas importantes</b>
• Emite <b>Factura C</b> con <b>Concepto=2 (Servicios)</b> y <b>sin IVA</b>.
• En AFIP envío obligatoriamente: <i>FchServDesde</i>, <i>FchServHasta</i> y <i>FchVtoPago</i>.
• El <b>Total</b> lo calculo como <i>Cantidad × Precio unitario</i> (redondeado a 2 decimales).

<b>Comandos</b>
• /start – muestra esta ayuda.
• Mandá un mensaje con el formato anterior para facturar.

Listo, cuando quieras empezamos 😄`;

bot.onText(/^\/start/i, (msg) => {
  bot.sendMessage(msg.chat.id, BIENVENIDA_HTML, { parse_mode: 'HTML' });
});

// -------------------- Telegram: entrada libre --------------------
bot.on('message', async (msg) => {
  const chatId = msg.chat.id;
  const text = (msg.text || '').trim();

  if (/^\/start/i.test(text)) return;

  if (!/facturar/i.test(text)) {
    // Ignoramos mensajes que no pidan facturar; igual mostramos mini-tip
    return bot.sendMessage(chatId, '👋 Mandame algo como: <code>facturar dni 12345678 cantidad 3 unidad sesión precio 15000 desc terapia</code>', { parse_mode: 'HTML' });
  }

  try {
    await bot.sendMessage(chatId, '✅ Recibí los datos. Estoy emitiendo la factura…');

    // Parseo
    const p = parsearMensaje(text);

    if (!p.doc) {
      throw new Error('Falta DNI/CUIT del receptor (ej: "dni 12345678" o "cuit 20-....").');
    }
    const DocTipo = p.docTipo || 96; // DNI por defecto
    const DocNro = p.doc;
    const cantidad = p.cantidad ? Number(p.cantidad) : 1;
    const unidad = (p.unidad || 'sesión').trim();
    const precioUnitario = p.precio ? Number(p.precio) : NaN;
    if (!precioUnitario || isNaN(precioUnitario)) {
      throw new Error('Falta el precio unitario (ej: "precio 15000").');
    }
    const totalCalculado = redondear2(cantidad * precioUnitario);
    const descripcion = (p.desc || 'Servicio profesional').trim();

    // Fechas AFIP (Concepto=2 Servicios)
    const hoyD = hoy();
    const FchServDesde = parseFechaAAAAMMDD(p.desde || p.fecha) || yyyymmdd(hoyD);
    const FchServHasta = parseFechaAAAAMMDD(p.hasta || p.fecha) || yyyymmdd(hoyD);
    const FchVtoPago  = yyyymmdd(hoyD);

    // Buscar datos de paciente en hoja Pacientes
    let datosPac = await buscarPacientePorDoc(DocNro);
    const Nombre = datosPac?.nombre || '';
    const Domicilio = datosPac?.domicilio || '';

    // Registrar como PENDIENTE en Sheets
    if (!SPREADSHEET_ID) {
      throw new Error('Falta GOOGLE_SHEETS_SPREADSHEET_ID en variables de entorno.');
    }

    const ahoraLocal = new Date();
    const fechaStr = `${String(ahoraLocal.getDate()).padStart(2,'0')}/${String(ahoraLocal.getMonth()+1).padStart(2,'0')}/${ahoraLocal.getFullYear()}`;

    const fila = new Array(FACT_COLS.PDF_LINK + 1).fill('');
    fila[FACT_COLS.ESTADO] = 'Pendiente';
    fila[FACT_COLS.FECHA] = fechaStr;
    fila[FACT_COLS.DOC] = DocNro;
    fila[FACT_COLS.NOMBRE] = Nombre;
    fila[FACT_COLS.DOMICILIO] = Domicilio;
    fila[FACT_COLS.DESCRIPCION] = descripcion;
    fila[FACT_COLS.CANTIDAD] = cantidad;
    fila[FACT_COLS.UNIDAD] = unidad;
    fila[FACT_COLS.PRECIO_UNIT] = precioUnitario;
    fila[FACT_COLS.TOTAL] = totalCalculado;
    fila[FACT_COLS.TIPO] = 'C';
    fila[FACT_COLS.PTO_VTA] = AFIP_PTO_VTA;

    await agregarFilaFacturas(fila);

    await bot.sendMessage(chatId, '📡 Enviando solicitud a AFIP…');

    // AFIP: próximo número
    const afip = getAfip();
    const last = await afip.ElectronicBilling.getLastVoucher(AFIP_PTO_VTA, AFIP_TIPO_CBTE);
    const proxNumero = last + 1;

    // Crear voucher (Factura C, Concepto=2, sin IVA)
    const CbteFch = yyyymmdd(hoyD);
    const data = {
      CantReg: 1,
      PtoVta: AFIP_PTO_VTA,
      CbteTipo: AFIP_TIPO_CBTE, // 11 Factura C
      Concepto: 2, // Servicios
      DocTipo,
      DocNro: Number(DocNro),
      CbteDesde: proxNumero,
      CbteHasta: proxNumero,
      CbteFch,
      ImpTotal: totalCalculado,
      ImpTotConc: 0,
      ImpNeto: totalCalculado,
      ImpOpEx: 0,
      ImpIVA: 0,
      ImpTrib: 0,
      MonId: 'PES',
      MonCotiz: 1,
      // Fechas obligatorias por Concepto=2
      FchServDesde,
      FchServHasta,
      FchVtoPago,
      // NO incluir Iva en Factura C
    };

    const resp = await afip.ElectronicBilling.createVoucher(data);
    const CAE = resp.CAE;
    const CAEFchVto = resp.CAEFchVto;

    await bot.sendMessage(chatId, '🧾 AFIP respondió. Generando PDF…');

    // Generar PDF
    const receptor = { Nombre, DocTipo, DocNro, Domicilio };
    const detalle = {
      ...data,
      Items: [{
        Descripcion: descripcion,
        Cantidad: cantidad,
        Unidad: unidad,
        PrecioUnitario: precioUnitario,
        Importe: totalCalculado,
      }],
    };

    const pdf = await generarPDF({
      emisor: { CUIT: AFIP_CUIT },
      receptor,
      detalle,
      caeInfo: { CAE, CAEFchVto },
    });

    // Subir a Drive
    let driveInfo = null;
    try {
      driveInfo = await subirADrive(pdf);
    } catch (e) {
      console.warn('No pude subir a Drive:', e?.message || e);
    }

    // Buscar fila para actualizar (última que coincida con DNI/CUIT y "Pendiente")
    const rows = await leerTodasFacturas();
    let filaIndex = -1;
    for (let i = rows.length - 1; i >= 1; i--) {
      const r = rows[i];
      if ((r[FACT_COLS.ESTADO] || '').toLowerCase() === 'pendiente' &&
          (r[FACT_COLS.DOC] || '').replace(/\D/g,'') === String(DocNro).replace(/\D/g,'')) {
        filaIndex = i;
        break;
      }
    }

    if (filaIndex >= 0) {
      // completar datos
      await actualizarCelda(filaIndex, FACT_COLS.ESTADO, 'Emitido');
      await actualizarRango(filaIndex, FACT_COLS.CbteNro, [
        proxNumero, CAE, CAEFchVto || '', ''
      ]);
      await actualizarCelda(filaIndex, FACT_COLS.TOTAL, totalCalculado);
      if (driveInfo?.webViewLink) {
        await actualizarCelda(filaIndex, FACT_COLS.PDF_LINK, driveInfo.webViewLink);
      }
    }

    // Enviar PDF al chat (y link de Drive si existe)
    try {
      await bot.sendDocument(chatId, fs.createReadStream(pdf.filePath), {
        caption: driveInfo?.webViewLink ? `✅ Factura emitida (CAE: ${CAE}). PDF subido a Drive: ${driveInfo.webViewLink}` : `✅ Factura emitida (CAE: ${CAE}).`,
      });
    } catch {
      await bot.sendMessage(chatId, `✅ Factura emitida (CAE: ${CAE}).${driveInfo?.webViewLink ? ` PDF en Drive: ${driveInfo.webViewLink}` : ''}`);
    }

    // Limpieza archivo temporal
    try { fs.unlinkSync(pdf.filePath); } catch {}

  } catch (err) {
    console.error('ERROR:', err);
    const msgErr = (err && err.message) ? err.message : String(err);
    try { await bot.sendMessage(chatId, `❌ Error: ${msgErr}`); } catch {}
    // Intentar marcar error en la última fila "Pendiente"
    try {
      const rows = await leerTodasFacturas();
      let filaIndex = -1;
      for (let i = rows.length - 1; i >= 1; i--) {
        const r = rows[i];
        if ((r[FACT_COLS.ESTADO] || '').toLowerCase() === 'pendiente') {
          filaIndex = i; break;
        }
      }
      if (filaIndex >= 0) {
        await actualizarCelda(filaIndex, FACT_COLS.ESTADO, 'Error');
        await actualizarCelda(filaIndex, FACT_COLS.MENSAJE, msgErr);
      }
    } catch {}
  }
});

// -------------------- Start --------------------
app.listen(PORT, () => {
  console.log(`Servidor escuchando en ${PORT}`);
});
