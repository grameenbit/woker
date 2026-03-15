// ─── cookie.js ────────────────────────────────────────────────────────────────
// Google Ads-style persistent cross-site UID cookie system.
// UID = 32-char hex, stored in __adnx_uid cookie (2 years).
// Profile cached in Cloudflare KV for fast access.

const COOKIE_NAME = '__adnx_uid';
const COOKIE_TTL  = 60 * 60 * 24 * 365 * 2; // 2 years in seconds

// ── UID from request cookie or query param ────────────────────────────────────
export function getUID(req) {
  const cookie = req.headers.get('Cookie') || '';
  const match  = cookie.match(new RegExp(`(?:^|;\\s*)${COOKIE_NAME}=([^;]+)`));
  if (match?.[1]?.length === 32) return match[1];
  const url = new URL(req.url);
  const qp  = url.searchParams.get('uid');
  if (qp?.length === 32) return qp;
  return null;
}

export function newUID() {
  return crypto.randomUUID().replace(/-/g, '');
}

// Build Set-Cookie header value (SameSite=None; Secure for cross-site)
export function buildCookieHeader(uid, domain) {
  const parts = [
    `${COOKIE_NAME}=${uid}`,
    `Max-Age=${COOKIE_TTL}`,
    `Path=/`,
    `SameSite=None`,
    `Secure`,
    `HttpOnly`,
  ];
  if (domain && !domain.includes('localhost')) parts.push(`Domain=${domain}`);
  return parts.join('; ');
}

// ── KV profile cache ──────────────────────────────────────────────────────────
export async function getProfileFromKV(uid, env) {
  try {
    const raw = await env.USER_PROFILES.get(uid);
    return raw ? JSON.parse(raw) : null;
  } catch { return null; }
}

export async function saveProfileToKV(uid, profile, env) {
  try {
    await env.USER_PROFILES.put(uid, JSON.stringify(profile), { expirationTtl: COOKIE_TTL });
  } catch (e) { console.error('KV save failed:', e); }
}

// ── Merge new page visit into user profile ────────────────────────────────────
export function mergeVisit(existing, visit) {
  const profile = existing || {
    uid:          visit.uid,
    first_seen:   visit.ts,
    page_history: [],
    interests:    [],
    keywords:     [],
    top_category: null,
    page_count:   0,
  };

  // Update latest geo/device (always overwrite with newest)
  profile.last_seen   = visit.ts;
  profile.country     = visit.country    || profile.country;
  profile.city        = visit.city       || profile.city;
  profile.device_type = visit.deviceType || profile.device_type;
  profile.os_type     = visit.os         || profile.os_type;
  profile.browser     = visit.browser    || profile.browser;
  profile.ip_address  = visit.ip         || profile.ip_address;
  profile.isVPN       = visit.isVPN      || profile.isVPN;
  profile.isProxy     = visit.isProxy    || profile.isProxy;
  profile.riskScore   = Math.max(profile.riskScore || 0, visit.riskScore || 0);

  // Prepend to page history, keep last 200
  profile.page_history = [
    { url: visit.url, title: visit.title, referrer: visit.referrer, ts: visit.ts },
    ...(profile.page_history || []),
  ].slice(0, 200);

  profile.page_count = (profile.page_count || 0) + 1;
  return profile;
}
