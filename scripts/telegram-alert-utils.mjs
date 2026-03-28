import fs from 'node:fs';
import path from 'node:path';
import { buildTransportDiagnostics, buildTransportFailureError } from './fetch-diagnostics-utils.mjs';

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function resolveTelegramConfig() {
  const botToken = process.env.TIEREDGE_TELEGRAM_BOT_TOKEN || process.env.TELEGRAM_BOT_TOKEN || '';
  const chatId = process.env.TIEREDGE_TELEGRAM_CHAT_ID || process.env.TELEGRAM_CHAT_ID || '';
  return {
    botToken: String(botToken).trim(),
    chatId: String(chatId).trim(),
  };
}

async function telegramRequest(method, payload, options = {}) {
  const { botToken } = resolveTelegramConfig();
  if (!botToken) {
    return {
      ok: false,
      error: 'telegram_config_missing',
    };
  }
  const targetUrl = `https://api.telegram.org/bot${botToken}/${method}`;
  const attempt = Number(options.attempt || 0);
  try {
    const response = await fetch(targetUrl, {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
      },
      body: JSON.stringify(payload),
    });
    const rawText = await response.text();
    let body = null;
    try {
      body = rawText ? JSON.parse(rawText) : null;
    } catch {
      body = null;
    }
    if (!response.ok) {
      const retryAfterSeconds = Number(body?.parameters?.retry_after);
      if (response.status === 429 && attempt === 0 && Number.isFinite(retryAfterSeconds) && retryAfterSeconds > 0 && retryAfterSeconds <= 5) {
        await sleep((retryAfterSeconds + 1) * 1000);
        return telegramRequest(method, payload, { attempt: attempt + 1 });
      }
      return {
        ok: false,
        error: `telegram_http_${response.status}`,
        diagnostics: {
          service: 'telegram',
          request_method: 'POST',
          request_target_domain: 'api.telegram.org',
          request_path: new URL(targetUrl).pathname,
          response_status: response.status,
          retry_after_seconds: Number.isFinite(retryAfterSeconds) ? retryAfterSeconds : null,
          response_description: body?.description || null,
          primary_cause: response.status === 429 ? 'telegram_rate_limited' : 'telegram_http_error',
          attempt,
        },
      };
    }
    if (!body?.ok) {
      return {
        ok: false,
        error: body?.description || 'telegram_api_rejected',
        diagnostics: {
          service: 'telegram',
          request_method: 'POST',
          request_target_domain: 'api.telegram.org',
          request_path: new URL(targetUrl).pathname,
          response_status: 200,
          retry_after_seconds: Number.isFinite(Number(body?.parameters?.retry_after)) ? Number(body.parameters.retry_after) : null,
          response_description: body?.description || null,
          primary_cause: 'telegram_api_rejected',
          attempt,
        },
      };
    }
    return { ok: true, data: body.result ?? null };
  } catch (error) {
    const wrapped = buildTransportFailureError({
      prefix: `telegram_transport_failed:${method}`,
      service: 'telegram',
      targetUrl,
      method: 'POST',
      timeoutMs: null,
      authEnvVars: ['TIEREDGE_TELEGRAM_BOT_TOKEN', 'TELEGRAM_BOT_TOKEN'],
      error,
    });
    return {
      ok: false,
      error: wrapped.message,
      diagnostics: buildTransportDiagnostics({
        service: 'telegram',
        targetUrl,
        method: 'POST',
        timeoutMs: null,
        authEnvVars: ['TIEREDGE_TELEGRAM_BOT_TOKEN', 'TELEGRAM_BOT_TOKEN'],
        error,
      }),
    };
  }
}

async function telegramGetFile(filePath) {
  const { botToken } = resolveTelegramConfig();
  if (!botToken) {
    return {
      ok: false,
      error: 'telegram_config_missing',
    };
  }
  const targetUrl = `https://api.telegram.org/file/bot${botToken}/${filePath}`;
  try {
    const response = await fetch(targetUrl, { method: 'GET' });
    if (!response.ok) {
      return {
        ok: false,
        error: `telegram_file_http_${response.status}`,
      };
    }
    const arrayBuffer = await response.arrayBuffer();
    return {
      ok: true,
      data: Buffer.from(arrayBuffer),
    };
  } catch (error) {
    const wrapped = buildTransportFailureError({
      prefix: 'telegram_file_download_failed',
      service: 'telegram',
      targetUrl,
      method: 'GET',
      timeoutMs: null,
      authEnvVars: ['TIEREDGE_TELEGRAM_BOT_TOKEN', 'TELEGRAM_BOT_TOKEN'],
      error,
    });
    return {
      ok: false,
      error: wrapped.message,
      diagnostics: buildTransportDiagnostics({
        service: 'telegram',
        targetUrl,
        method: 'GET',
        timeoutMs: null,
        authEnvVars: ['TIEREDGE_TELEGRAM_BOT_TOKEN', 'TELEGRAM_BOT_TOKEN'],
        error,
      }),
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

export async function fetchTelegramUpdates(offset = null, timeoutSeconds = 20) {
  const payload = {
    timeout: Number.isFinite(timeoutSeconds) ? Math.max(0, Math.floor(timeoutSeconds)) : 20,
    allowed_updates: ['message'],
  };
  if (Number.isFinite(offset)) payload.offset = offset;
  return telegramRequest('getUpdates', payload);
}

export function telegramConfiguredChatId() {
  return resolveTelegramConfig().chatId;
}

export async function fetchTelegramFileInfo(fileId) {
  const result = await telegramRequest('getFile', { file_id: fileId });
  if (!result.ok) return result;
  return {
    ok: true,
    data: result.data,
  };
}

export async function downloadTelegramFile(fileId, destinationPath) {
  const fileInfo = await fetchTelegramFileInfo(fileId);
  if (!fileInfo.ok) return fileInfo;
  const filePath = String(fileInfo.data?.file_path || '').trim();
  if (!filePath) {
    return {
      ok: false,
      error: 'telegram_file_path_missing',
    };
  }
  const fileResult = await telegramGetFile(filePath);
  if (!fileResult.ok) return fileResult;
  fs.mkdirSync(path.dirname(destinationPath), { recursive: true });
  fs.writeFileSync(destinationPath, fileResult.data);
  return {
    ok: true,
    data: {
      destination_path: destinationPath,
      file_path: filePath,
      file_size: fileInfo.data?.file_size ?? null,
    },
  };
}
