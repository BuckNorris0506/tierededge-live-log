function resolveTelegramConfig() {
  const botToken = process.env.TIEREDGE_TELEGRAM_BOT_TOKEN || process.env.TELEGRAM_BOT_TOKEN || '';
  const chatId = process.env.TIEREDGE_TELEGRAM_CHAT_ID || process.env.TELEGRAM_CHAT_ID || '';
  return {
    botToken: String(botToken).trim(),
    chatId: String(chatId).trim(),
  };
}

export async function sendTelegramMessage(text) {
  const { botToken, chatId } = resolveTelegramConfig();
  if (!botToken || !chatId) {
    return {
      ok: false,
      error: 'telegram_config_missing',
    };
  }

  try {
    const response = await fetch(`https://api.telegram.org/bot${botToken}/sendMessage`, {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
      },
      body: JSON.stringify({
        chat_id: chatId,
        text,
        disable_web_page_preview: true,
      }),
    });
    if (!response.ok) {
      return {
        ok: false,
        error: `telegram_http_${response.status}`,
      };
    }
    const payload = await response.json();
    if (!payload?.ok) {
      return {
        ok: false,
        error: payload?.description || 'telegram_api_rejected',
      };
    }
    return { ok: true };
  } catch (error) {
    return {
      ok: false,
      error: error instanceof Error ? error.message : 'telegram_request_failed',
    };
  }
}
