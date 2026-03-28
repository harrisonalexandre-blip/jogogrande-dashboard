// Vercel Serverless Function: GA4 Realtime Data
// GA4 quota: 50 realtime tokens/hour → 1 call every 3 min = 20 tokens/hour
const { google } = require('googleapis');

// In-memory cache (persists across warm invocations)
let _cache = null;
let _cacheTime = 0;
const CACHE_TTL = 180000; // 3 minutes

module.exports = async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET');
  // CDN cache: serve stale for up to 5 min while revalidating
  res.setHeader('Cache-Control', 's-maxage=180, stale-while-revalidate=120');

  // Return cache if fresh
  if (_cache && (Date.now() - _cacheTime) < CACHE_TTL) {
    return res.status(200).json(_cache);
  }

  try {
    const keyJson = process.env.GA_SERVICE_ACCOUNT_KEY;
    if (!keyJson) {
      return res.status(200).json({
        ok: false, error: 'GA_SERVICE_ACCOUNT_KEY not configured', fallback: true,
        data: { activeUsers: 0, countries: [], cities: [], perMinute: [], updated: new Date().toISOString() }
      });
    }

    const key = JSON.parse(keyJson);
    const auth = new google.auth.GoogleAuth({
      credentials: key,
      scopes: ['https://www.googleapis.com/auth/analytics.readonly']
    });

    const analyticsData = google.analyticsdata({ version: 'v1beta', auth });
    const propertyId = process.env.GA_PROPERTY_ID || '521853091';

    // === SINGLE API CALL: country+city combined (1 token) ===
    const report = await analyticsData.properties.runRealtimeReport({
      property: `properties/${propertyId}`,
      requestBody: {
        dimensions: [{ name: 'country' }, { name: 'city' }],
        metrics: [{ name: 'activeUsers' }],
        orderBys: [{ metric: { metricName: 'activeUsers' }, desc: true }],
        limit: 100
      }
    });

    let totalUsers = 0;
    const countryMap = {};
    const cityMap = {};

    (report.data.rows || []).forEach(r => {
      const country = r.dimensionValues[0].value;
      const city = r.dimensionValues[1].value;
      const users = parseInt(r.metricValues[0].value);
      totalUsers += users;
      countryMap[country] = (countryMap[country] || 0) + users;
      if (city !== '(not set)') {
        cityMap[city] = (cityMap[city] || 0) + users;
      }
    });

    const countries = Object.entries(countryMap)
      .map(([n, v]) => ({ n, v })).sort((a, b) => b.v - a.v).slice(0, 10);
    const cities = Object.entries(cityMap)
      .map(([n, v]) => ({ n, v })).sort((a, b) => b.v - a.v).slice(0, 10);

    // Build perMinute from history (shift previous cache + add current)
    let perMinute = [];
    if (_cache && _cache.data && _cache.data.perMinute) {
      perMinute = _cache.data.perMinute.slice(1); // shift left
      perMinute.push(totalUsers); // add current
    } else {
      // First call: fill with current value
      perMinute = new Array(30).fill(0);
      perMinute[29] = totalUsers;
    }

    const result = {
      ok: true,
      data: { activeUsers: totalUsers, countries, cities, pages: [], perMinute, updated: new Date().toISOString() }
    };

    _cache = result;
    _cacheTime = Date.now();
    return res.status(200).json(result);

  } catch (err) {
    console.error('GA Realtime error:', err.message);
    // Return stale cache if available
    if (_cache) {
      return res.status(200).json({ ..._cache, cached: true });
    }
    return res.status(200).json({
      ok: false, error: err.message, fallback: true,
      data: { activeUsers: 0, countries: [], cities: [], perMinute: [], updated: new Date().toISOString() }
    });
  }
};
