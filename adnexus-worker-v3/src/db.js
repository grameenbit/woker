// ─── db.js ────────────────────────────────────────────────────────────────────
// Supabase REST API client — fetch-based, no npm packages needed.
// Uses service role key (set via GitHub Actions secret: SUPABASE_SERVICE_KEY).

export function createDB(env) {
  const BASE = env.SUPABASE_URL;
  const KEY  = env.SUPABASE_SERVICE_KEY;

  const HEADERS = {
    'apikey':        KEY,
    'Authorization': `Bearer ${KEY}`,
    'Content-Type':  'application/json',
    'Prefer':        'return=minimal',
  };
  const HEADERS_REP = { ...HEADERS, 'Prefer': 'return=representation' };

  return {
    // SELECT — returns array or []
    async select(table, query = '') {
      const r = await fetch(`${BASE}/rest/v1/${table}?${query}`, { headers: HEADERS_REP });
      if (!r.ok) { console.error(`SELECT ${table} ${r.status}:`, await r.text()); return []; }
      return r.json();
    },

    // UPSERT — insert or update on conflict
    async upsert(table, body) {
      const r = await fetch(`${BASE}/rest/v1/${table}`, {
        method: 'POST',
        headers: { ...HEADERS, 'Prefer': 'resolution=merge-duplicates,return=minimal' },
        body: JSON.stringify(body),
      });
      if (!r.ok && r.status !== 201) console.error(`UPSERT ${table} ${r.status}:`, await r.text());
    },

    // INSERT — fire and forget
    async insert(table, body) {
      const r = await fetch(`${BASE}/rest/v1/${table}`, {
        method: 'POST',
        headers: HEADERS,
        body: JSON.stringify(body),
      });
      if (!r.ok && r.status !== 201) console.error(`INSERT ${table} ${r.status}:`, await r.text());
    },

    // UPDATE — match is { col: value }
    async update(table, match, body) {
      const q = Object.entries(match).map(([k, v]) => `${k}=eq.${encodeURIComponent(v)}`).join('&');
      const r = await fetch(`${BASE}/rest/v1/${table}?${q}`, {
        method: 'PATCH',
        headers: HEADERS,
        body: JSON.stringify(body),
      });
      if (!r.ok) console.error(`UPDATE ${table} ${r.status}:`, await r.text());
    },
  };
}

// ── Revenue share config (cached in KV 5 minutes) ─────────────────────────────
export async function getRevenueShare(env) {
  const cacheKey = 'cfg:revenue_share';
  try {
    const cached = await env.AD_CACHE.get(cacheKey);
    if (cached) return JSON.parse(cached);
  } catch {}

  try {
    const D = createDB(env);
    const rows = await D.select('platform_config', 'key=in.(revenue_share_publisher,revenue_share_admin)');
    if (Array.isArray(rows) && rows.length) {
      const map = Object.fromEntries(rows.map(r => [r.key, parseFloat(r.value)]));
      const share = {
        publisher: map['revenue_share_publisher'] ?? 0.80,
        admin:     map['revenue_share_admin']     ?? 0.20,
      };
      await env.AD_CACHE.put(cacheKey, JSON.stringify(share), { expirationTtl: 300 });
      return share;
    }
  } catch (e) { console.error('getRevenueShare failed:', e); }

  return { publisher: 0.80, admin: 0.20 }; // safe fallback
}
