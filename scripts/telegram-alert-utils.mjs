function resolveTelegramConfig() {
  const botToken = process.env.TIEREDGE_TELEGRAM_BOT_TOKEN || process.env.TELEGRAM_BOT_TOKEN || '';
  const chatId = process.env.TIEREDGE_TELEGRAM_CHAT_ID || process.env.TELEGRAM_CHAT_ID || '';
  return {
    botToken: String(botToken).trim(),
    chatId: String(chatId).trim(),
  };
}

async function telegramRequest(method, payload) {
  const { botToken } = resolveTelegramConfig();
  if (!botToken) {
    return {
      ok: false,
      error: 'telegram_config_missing',
    };
  }
  try {
    const response = await fetch(`https://api.telegram.org/bot${botToken}/${method}`, {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
      },
      body: JSON.stringify(payload),
    });
    if (!response.ok) {
      return {
        ok: false,
        error: `telegram_http_${response.status}`,
      };
    }
    const body = await response.json();
    if (!body?.ok) {
      return {
        ok: false,
        error: body?.description || 'telegram_api_rejected',
      };
    }
    return { ok: true, data: body.result ?? null };
  } catch (error) {
    return {
      ok: false,
      error: error instanceof Error ? error.message : 'telegram_request_failed',
    };
  }
}

export async function sendTelegramMessage(text, options = {}) {
  const { botToken, chatId } = resolveTelegramConfig();
  if (!botToken || !chatId) {
    return {
      ok: false,
      error: 'telegram_config_missing',
    };
  }
  const keyboard = Array.isArray(options.keyboard) && options.keyboard.length
    ? {
        keyboard: options.keyboard.map((row) => row.map((label) => ({ text: label }))),
        resize_keyboard: true,
        one_time_keyboard: false,
      }
    : null;
  return telegramRequest('sendMessage', {
    chat_id: options.chatId || chatId,
    text,
    disable_web_page_preview: true,
    reply_markup: keyboard || undefined,
  });
}

export async function fetchTelegramUpdates(offset = null) {
  const payload = {
    timeout: 0,
    allowed_updates: ['message'],
  };
  if (Number.isFinite(offset)) payload.offset = offset;
  return telegramRequest('getUpdates', payload);
}

export function telegramConfiguredChatId() {
  return resolveTelegramConfig().chatId;
}
