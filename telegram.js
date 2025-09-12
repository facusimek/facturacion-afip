// telegram.js
import express from 'express';
import axios from 'axios';
import fs from 'fs';

export function registerTelegram(app) {
  app.use(express.json());

  const BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN || process.env.TELEGRAM_TOKEN;
  if (!BOT_TOKEN) {
    console.warn('[TELEGRAM] Falta TELEGRAM_BOT_TOKEN (o TELEGRAM_TOKEN)');
  }
  const API = `https://api.telegram.org/bot${BOT_TOKEN}`;

  // --- util: mandar mensaje ---
  async function sendMessage(chatId, text, extra = {}) {
    try {
      const { data } = await axios.post(`${API}/sendMessage`, {
        chat_id: chatId,
        text,
        parse_mode: 'HTML',
        ...extra,
      });
      return data;
    } catch (err) {
      console.error('[TELEGRAM] sendMessage error:', err.response?.data || err.message);
    }
  }

  // --- util: extraer chat_id de cualquier update ---
  function extractChatId(update) {
    return (
      update?.message?.chat?.id ??
      update?.edited_message?.chat?.id ??
      update?.callback_query?.message?.chat?.id ??
      update?.channel_post?.chat?.id ??
      update?.my_chat_member?.chat?.id ??
      update?.chat_member?.chat?.id ??
      null
    );
  }

  // --- opcional: persistir chat_ids en un json (útil para copiarlo a env) ---
  function rememberChatId(id) {
    try {
      const p = './chat_ids.json';
      let ids = [];
      if (fs.existsSync(p)) ids = JSON.parse(fs.readFileSync(p, 'utf8'));
      if (!ids.includes(id)) {
        ids.push(id);
        fs.writeFileSync(p, JSON.stringify(ids, null, 2));
      }
    } catch (e) {
      console.warn('[TELEGRAM] No pude guardar chat_id localmente:', e.message);
    }
  }

  // 1) webhook que Telegram va a llamar
  app.post('/telegram/webhook', async (req, res) => {
    const update = req.body;
    const chatId = extractChatId(update);

    if (chatId) {
      console.log('[TELEGRAM_CHAT_ID]', chatId);
      rememberChatId(chatId);

      // responde algo básico para confirmar
      if (update.message?.text === '/start') {
        await sendMessage(
          chatId,
          `¡Bot conectado! ✅\nTu <b>chat_id</b> es <code>${chatId}</code>\nYa no deberías ver “Requested entity was not found”.`
        );
      } else {
        await sendMessage(chatId, 'Recibido ✅');
      }
    } else {
      // Esto evita 400 de Telegram: siempre devolver 200 aunque no sepamos el chat
      console.warn('[TELEGRAM] No encontré chat_id en update:', JSON.stringify(update).slice(0, 400));
    }

    // Telegram necesita 200 rápido
    res.sendStatus(200);
  });

  // 2) endpoint para setear webhook (más cómodo que usar curl)
  app.get('/telegram/set-webhook', async (req, res) => {
    try {
      const base = (process.env.WEBHOOK_URL || 'https://facturacion-afip.onrender.com').replace(/\/$/, '');
      const url = `${base}/telegram/webhook`;
      const { data } = await axios.get(`${API}/setWebhook`, { params: { url } });
      res.json({ setWebhook: data, url });
    } catch (e) {
      res.status(500).json(e.response?.data || { error: e.message });
    }
  });

  // 3) endpoint de prueba manual (para cuando ya tengas el chat_id)
  app.get('/telegram/test/:chatId', async (req, res) => {
    await sendMessage(req.params.chatId, 'Prueba desde el servidor ✅');
    res.send('ok');
  });
}
