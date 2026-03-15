// ─── ai-analyze.js ────────────────────────────────────────────────────────────
// Interest analysis via buildpicoapps free WebSocket AI.
// Runs every 5 page views per user (non-blocking, background task).
// Falls back to heuristic analysis if WebSocket fails.

// Analyze every N page views
export const ANALYZE_EVERY = 5;
export const shouldAnalyze = (pageCount) => pageCount % ANALYZE_EVERY === 0 && pageCount > 0;

const SYSTEM_PROMPT = `You are an ad targeting AI. When given browsing history, respond ONLY with a valid JSON object (no markdown, no explanation) using exactly this structure:
{"interests":["cat1","cat2"],"keywords":["kw1","kw2"],"top_category":"cat1","intent":"browsing"}
Use only these categories: technology, gaming, finance, health, travel, fashion, sports, news, entertainment, ecommerce, food, automotive, education, real_estate, crypto, software.
Keywords must be specific ad-targetable phrases that advertisers would bid on.`;

// ── buildpicoapps WebSocket AI ────────────────────────────────────────────────
export async function analyzeWithAI(pageHistory, env) {
  const pages = pageHistory
    .slice(0, 40)
    .map(p => `URL: ${p.url || ''}${p.title ? ' | Title: ' + p.title : ''}`)
    .join('\n');

  return new Promise((resolve) => {
    const timeout = setTimeout(() => resolve(null), 12000);

    try {
      const ws = new WebSocket('wss://backend.buildpicoapps.com/api/chatbot/chat');
      let responseText = '';

      ws.addEventListener('open', () => {
        ws.send(JSON.stringify({
          chatId:       crypto.randomUUID(),
          appId:        env.AI_APP_ID || 'language-industry',
          systemPrompt: SYSTEM_PROMPT,
          message:      `Analyze this browsing history for ad targeting:\n${pages}`,
        }));
      });

      ws.addEventListener('message', (e) => { responseText += e.data; });

      ws.addEventListener('close', () => {
        clearTimeout(timeout);
        try {
          const jsonMatch = responseText.match(/\{[\s\S]*\}/);
          if (!jsonMatch) { resolve(null); return; }
          const parsed = JSON.parse(jsonMatch[0]);
          resolve({
            interests:    Array.isArray(parsed.interests) ? parsed.interests.slice(0, 10) : [],
            keywords:     Array.isArray(parsed.keywords)  ? parsed.keywords.slice(0, 20)  : [],
            top_category: parsed.top_category || null,
            intent:       parsed.intent || 'browsing',
            analyzed_at:  new Date().toISOString(),
            source:       'buildpicoapps',
          });
        } catch { resolve(null); }
      });

      ws.addEventListener('error', () => { clearTimeout(timeout); resolve(null); });
    } catch { clearTimeout(timeout); resolve(null); }
  });
}

// ── Heuristic fallback (regex-based, no API needed) ───────────────────────────
const CATEGORY_PATTERNS = {
  technology:    /tech|software|code|app|github|android|ios|api|developer|javascript|python|laptop|phone/i,
  gaming:        /game|gaming|steam|xbox|playstation|nintendo|esport|twitch|roblox|minecraft|valorant/i,
  finance:       /bank|invest|stock|crypto|bitcoin|forex|trading|loan|insurance|finance|money|wallet/i,
  health:        /health|fitness|diet|gym|exercise|medicine|doctor|hospital|wellness|nutrition/i,
  travel:        /travel|flight|hotel|booking|airbnb|visa|holiday|vacation|destination|resort/i,
  fashion:       /fashion|clothing|shoes|dress|style|wear|outfit|beauty|makeup|cosmetic/i,
  sports:        /sport|football|cricket|soccer|basketball|tennis|nba|fifa|ipl|match|tournament/i,
  entertainment: /movie|music|celebrity|show|netflix|youtube|series|film|actor|song|streaming/i,
  ecommerce:     /shop|buy|price|deal|sale|discount|amazon|daraz|product|review|cart|checkout/i,
  education:     /learn|course|tutorial|university|college|study|exam|skill|certification/i,
  food:          /food|recipe|restaurant|cook|eat|diet|meal|cuisine|delivery|zomato/i,
  automotive:    /car|bike|auto|vehicle|motor|motorcycle|electric vehicle|ev|drive/i,
  real_estate:   /rent|apartment|house|property|real estate|flat|buy home|land/i,
  crypto:        /bitcoin|ethereum|crypto|defi|nft|blockchain|web3|binance|coinbase/i,
  software:      /saas|software|tool|plugin|extension|framework|library|sdk|devtools/i,
};

export function heuristicAnalyze(pageHistory) {
  const allText = pageHistory.map(p => `${p.url || ''} ${p.title || ''}`).join(' ');
  const scores  = {};

  for (const [cat, pattern] of Object.entries(CATEGORY_PATTERNS)) {
    const matches = (allText.match(new RegExp(pattern.source, 'gi')) || []).length;
    if (matches > 0) scores[cat] = matches;
  }

  const sorted    = Object.entries(scores).sort((a, b) => b[1] - a[1]);
  const interests = sorted.slice(0, 6).map(([k]) => k);

  // Extract search queries as keywords
  const keywords = [];
  for (const p of pageHistory.slice(0, 20)) {
    try {
      const u = new URL(p.url || 'https://x');
      const q = u.searchParams.get('q') || u.searchParams.get('search') || u.searchParams.get('query');
      if (q) keywords.push(q.toLowerCase().trim());
      const segs = u.pathname.split('/').filter(s => s.length > 3 && !/^\d+$/.test(s));
      keywords.push(...segs.map(s => s.replace(/-/g, ' ')));
    } catch {}
  }

  return {
    interests,
    keywords:     [...new Set(keywords)].slice(0, 15),
    top_category: interests[0] || 'general',
    intent:       scores.ecommerce > 2 ? 'buying' : 'browsing',
    analyzed_at:  new Date().toISOString(),
    source:       'heuristic',
  };
}
