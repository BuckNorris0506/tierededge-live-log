export const WHATSAPP_DISABLED_MESSAGE = 'WhatsApp is disabled for TieredEdge operations. Use Telegram for operator commands and execution follow-up.';

export function printWhatsappDisabled(channel = 'whatsapp') {
  const payload = {
    ok: false,
    channel,
    status: 'disabled',
    reason: 'whatsapp_removed_from_operations',
    message: WHATSAPP_DISABLED_MESSAGE,
  };
  console.log(JSON.stringify(payload, null, 2));
}
