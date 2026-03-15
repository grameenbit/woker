// ─── fingerprint.js ───────────────────────────────────────────────────────────
// Extracts geo, device, OS, browser, VPN/proxy data from Cloudflare headers.
// No external API calls — Cloudflare provides all geo data natively.

const HOSTING_ASNS = new Set([
  'AS14061','AS16276','AS24940','AS63949','AS45102','AS8100',
  'AS20473','AS62240','AS7922','AS15169','AS8075','AS16509',
  'AS13335','AS54113','AS36351','AS394711','AS46844',
]);

const VPN_KEYWORDS = [
  'nordvpn','expressvpn','surfshark','cyberghost','purevpn','protonvpn',
  'ipvanish','tunnelbear','windscribe','mullvad','hidemyass','pia',
  'privateinternetaccess','hotspotshield','vyprvpn',
];

const DC_KEYWORDS = [
  'amazon','aws','google cloud','microsoft azure','digitalocean','linode',
  'vultr','ovh','hetzner','contabo','hosting','server','datacenter',
  'cloud','vps','colocation','colo',
];

export function parseUA(ua = '') {
  // Device type
  let deviceType = 'desktop';
  if (/ipad|tablet|kindle/i.test(ua))                                           deviceType = 'tablet';
  else if (/mobile|android|iphone|ipod|blackberry|windows phone|opera mini/i.test(ua)) deviceType = 'mobile';

  // OS
  let os = 'Unknown';
  if      (/windows nt/i.test(ua))  os = 'Windows';
  else if (/android/i.test(ua))     { const v = ua.match(/android\s([\d.]+)/i);  os = `Android${v ? ' '+v[1] : ''}`; }
  else if (/iphone|ipad/i.test(ua)) { const v = ua.match(/os\s([\d_]+)/i);       os = `iOS${v ? ' '+v[1].replace(/_/g,'.') : ''}`; }
  else if (/mac os x/i.test(ua))    os = 'macOS';
  else if (/linux/i.test(ua))       os = 'Linux';
  else if (/cros/i.test(ua))        os = 'ChromeOS';

  // Browser
  let browser = 'Unknown';
  if      (/edg\//i.test(ua))           browser = 'Edge';
  else if (/opr\//i.test(ua))           browser = 'Opera';
  else if (/samsungbrowser/i.test(ua))  browser = 'Samsung';
  else if (/chrome/i.test(ua))          browser = 'Chrome';
  else if (/firefox/i.test(ua))         browser = 'Firefox';
  else if (/safari/i.test(ua))          browser = 'Safari';

  return { deviceType, os, browser };
}

export function detectProxy(asn = '', isp = '') {
  const org = (isp || '').toLowerCase();
  const asnKey = `AS${String(asn).replace('AS','').trim()}`;
  let isVPN = false, isProxy = false, risk = 0;
  const signals = [];

  if (HOSTING_ASNS.has(asnKey)) {
    isProxy = true; risk += 40;
    signals.push({ type: 'datacenter', label: 'Datacenter/Hosting IP', desc: isp });
  }
  if (VPN_KEYWORDS.some(k => org.includes(k))) {
    isVPN = true; risk += 60;
    signals.push({ type: 'vpn', label: 'Known VPN Provider', desc: isp });
  } else if (DC_KEYWORDS.some(k => org.includes(k))) {
    isProxy = true; risk = Math.max(risk, 35);
    if (!signals.length) signals.push({ type: 'proxy', label: 'Hosting/Cloud IP', desc: isp });
  }

  return { isVPN, isProxy, riskScore: Math.min(100, risk), signals };
}

// Main fingerprint function — call on every request
export function fingerprint(req) {
  const cf  = req.cf || {};
  const h   = req.headers;
  const ua  = h.get('User-Agent') || '';
  const ip  = h.get('CF-Connecting-IP') || h.get('X-Forwarded-For')?.split(',')[0]?.trim() || '0.0.0.0';
  const asn = String(cf.asn || '');
  const isp = cf.asOrganization || '';

  const { deviceType, os, browser } = parseUA(ua);
  const proxy = detectProxy(asn, isp);

  return {
    // Geo (from Cloudflare — city/region/country level accuracy)
    country:  cf.country  || 'XX',
    region:   cf.region   || '',
    city:     cf.city     || '',
    timezone: cf.timezone || '',
    latitude: cf.latitude  || null,
    longitude:cf.longitude || null,
    // Network
    ip, asn, isp,
    // Device
    deviceType, os, browser, ua,
    // Proxy/VPN
    ...proxy,
  };
}

// Geo-based CPM/CPC floor prices
export const GEO_TIERS = {
  1: { min_cpm: 5.00, min_cpc: 0.50, countries: ['US','GB','CA','AU','DE','FR','NL','SE','JP','SG','CH','NO','DK','FI','NZ','IE','AT','BE','IL','AE'] },
  2: { min_cpm: 2.00, min_cpc: 0.20, countries: ['IN','BR','MX','ID','PH','TH','MY','VN','TR','ZA','KR','SA','QA','KW','CL','CO','PE','AR','PL','CZ','HU','RO'] },
  3: { min_cpm: 0.50, min_cpc: 0.05, countries: ['BD','PK','NG','EG','KE','GH','TZ','ET','UG','ZW','SD','SN','CM','CI','RW','MZ','MW','ZM','AF','MM','KH'] },
};

export function getGeoTier(country) {
  for (const [tier, data] of Object.entries(GEO_TIERS))
    if (data.countries.includes(country)) return { tier: parseInt(tier), ...data };
  return { tier: 3, ...GEO_TIERS[3] };
}
