// Vercel Serverless Function: GA4 Realtime Data
// With in-memory cache to avoid exhausting GA4 daily quota
const { google } = require('googleapis');

// In-memory cache (persists across invocations on same Vercel instance)
let _cache = null;
let _cacheTime = 0;
const CACHE_TTL = 120000; // 2 minutes in ms

module.exports = async (req, res) => {
  // CORS
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET');
  res.setHeader('Cache-Control', 's-maxage=120, stale-while-revalidate=60');

  // Return cache if fresh
  if (_cache && (Date.now() - _cacheTime) < CACHE_TTL) {
    return res.status(200).json(_cache);
  }

  try {
    const keyJson = process.env.GA_SERVICE_ACCOUNT_KEY;
    if (!keyJson) {
      return res.status(200).json({
        ok: false, error: 'GA_SERVICE_ACCOUNT_KEY not configured',
        fallback: true,
        data: { activeUsers: 0, countries: [], cities: [], pages: [], updated: new Date().toISOString() }
      });
    }

    const key = JSON.parse(keyJson);
    const auth = new google.auth.GoogleAuth({
      credentials: key,
      scopes: ['https://www.googleapis.com/auth/analytics.readonly']
    });

    const analyticsData = google.analyticsdata({ version: 'v1beta', auth });
    const propertyId = process.env.GA_PROPERTY_ID || '521853091';

    // Single consolidated call: activeUsers with country+city dimensions
    // Uses only 1 API token instead of 5
    const [mainReport, minuteReport] = await Promise.all([
      analyticsData.properties.runRealtimeReport({
        property: `properties/${propertyId}`,
        requestBody: {
          dimensions: [{ name: 'country' }, { name: 'city' }],
          metrics: [{ name: 'activeUsers' }],
          orderBys: [{ metric: { metricName: 'activeUsers' }, desc: true }],
          limit: 50
        }
      }),
      analyticsData.properties.runRealtimeReport({
        property: `properties/${propertyId}`,
        requestBody: {
          dimensions: [{ name: 'minutesAgo' }],
          metrics: [{ name: 'activeUsers' }],
          minuteRanges: [{ startMinutesAgo: 29, endMinutesAgo: 0 }]
        }
      })
    ]);

    // Aggregate from combined report
    let totalUsers = 0;
    const countryMap = {};
    const cityMap = {};

    (mainReport.data.rows || []).forEach(r => {
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
      .map(([n, v]) => ({ n, v }))
      .sort((a, b) => b.v - a.v)
      .slice(0, 10);

    const cities = Object.entries(cityMap)
      .map(([n, v]) => ({ n, v }))
      .sort((a, b) => b.v - a.v)
      .slice(0, 10);

    // Per-minute bars
    const minuteMap = {};
    (minuteReport.data.rows || []).forEach(r => {
      minuteMap[parseInt(r.dimensionValues[0].value)] = parseInt(r.metricValues[0].value);
    });
    const perMinute = [];
    for (let i = 29; i >= 0; i--) {
      perMinute.push(minuteMap[i] || 0);
    }

    const result = {
      ok: true,
      data: {
        activeUsers: totalUsers,
        countries,
        cities,
        pages: [],
        perMinute,
        updated: new Date().toISOString()
      }
    };

    // Store in cache
    _cache = result;
    _cacheTime = Date.now();

    return res.status(200).json(result);

  } catch (err) {
    console.error('GA Realtime API error:', err.message);

    // If we have stale cache, return it instead of empty fallback
    if (_cache) {
      _cache.data.updated = new Date().toISOString();
      return res.status(200).json({ ..._cache, cached: true });
    }

    return res.status(200).json({
      ok: false, error: err.message, fallback: true,
      data: { activeUsers: 0, countries: [], cities: [], pages: [], updated: new Date().toISOString() }
    });
  }
};
