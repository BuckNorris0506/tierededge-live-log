function truncate(value, max = 240) {
  const text = String(value || '').trim();
  if (!text) return null;
  return text.length > max ? `${text.slice(0, max - 1)}...` : text;
}

function safeUrlParts(targetUrl) {
  try {
    const url = new URL(String(targetUrl || ''));
    return {
      request_target_domain: url.hostname || null,
      request_path: url.pathname || null,
    };
  } catch {
    return {
      request_target_domain: null,
      request_path: null,
    };
  }
}

function authEnvPresence(authEnvVars = []) {
  const present = {};
  for (const key of authEnvVars) {
    present[key] = Boolean(String(process.env[key] || '').trim());
  }
  return present;
}

export function classifyTransportFailure(error) {
  const cause = error?.cause;
  const code = String(error?.code || cause?.code || '').trim() || null;
  const causeMessage = truncate(cause?.message || null);
  const message = truncate(error?.message || null);

  if (code === 'ERR_TLS_CERT_ALTNAME_INVALID') {
    return {
      error_class: 'tls_certificate_problem',
      error_code: code,
      primary_cause: 'TLS/certificate hostname mismatch',
      error_message: message,
      cause_message: causeMessage,
    };
  }

  if ([
    'DEPTH_ZERO_SELF_SIGNED_CERT',
    'SELF_SIGNED_CERT_IN_CHAIN',
    'UNABLE_TO_VERIFY_LEAF_SIGNATURE',
    'ERR_TLS_CERT_SIGNATURE_ALGORITHM_UNSUPPORTED',
  ].includes(code || '')) {
    return {
      error_class: 'tls_certificate_problem',
      error_code: code,
      primary_cause: 'TLS/certificate trust failure',
      error_message: message,
      cause_message: causeMessage,
    };
  }

  if (['ENOTFOUND', 'EAI_AGAIN'].includes(code || '')) {
    return {
      error_class: 'dns_resolution_failure',
      error_code: code,
      primary_cause: 'DNS resolution failure',
      error_message: message,
      cause_message: causeMessage,
    };
  }

  if (['ETIMEDOUT', 'UND_ERR_CONNECT_TIMEOUT', 'ABORT_ERR'].includes(code || '')) {
    return {
      error_class: 'timeout',
      error_code: code,
      primary_cause: 'Request timed out',
      error_message: message,
      cause_message: causeMessage,
    };
  }

  if (['ECONNREFUSED', 'ECONNRESET', 'EHOSTUNREACH', 'ENETUNREACH'].includes(code || '')) {
    return {
      error_class: 'network_reachability_failure',
      error_code: code,
      primary_cause: 'Network connection failure',
      error_message: message,
      cause_message: causeMessage,
    };
  }

  return {
    error_class: 'unknown_transport_failure',
    error_code: code,
    primary_cause: 'Unknown transport failure',
    error_message: message,
    cause_message: causeMessage,
  };
}

export function buildTransportDiagnostics({ service, targetUrl, method = 'GET', timeoutMs = null, authEnvVars = [], error }) {
  return {
    service: String(service || '').trim() || null,
    request_method: String(method || 'GET').toUpperCase(),
    timeout_ms: Number.isFinite(timeoutMs) ? Number(timeoutMs) : null,
    auth_env_presence: authEnvPresence(authEnvVars),
    ...safeUrlParts(targetUrl),
    ...classifyTransportFailure(error),
  };
}

export function buildTransportFailureError({ prefix, service, targetUrl, method = 'GET', timeoutMs = null, authEnvVars = [], error }) {
  const diagnostics = buildTransportDiagnostics({
    service,
    targetUrl,
    method,
    timeoutMs,
    authEnvVars,
    error,
  });
  const err = new Error([
    String(prefix || 'transport_failed').trim(),
    diagnostics.error_class || 'unknown_transport_failure',
    diagnostics.request_target_domain || 'unknown_host',
    diagnostics.error_code || 'no_code',
    diagnostics.cause_message || diagnostics.error_message || 'request_failed',
  ].join(':'));
  err.cause = error;
  err.transport_diagnostics = diagnostics;
  return err;
}
