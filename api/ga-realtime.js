// Vercel Serverless Function: GA4 Realtime Data
// Fetches real-time active users, top countries, top cities from GA4
const { google } = require('googleapis');

module.exports = async (req, res) => {
  // CORS
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET');
  res.setHeader('Cache-Control', 'no-cache, no-store');

  try {
    // Service Account auth from env var
    const keyJson = process.env.GA_SERVICE_ACCOUNT_KEY;
    if (!keyJson) {
      return res.status(200).json({
        ok: false,
        error: 'GA_SERVICE_ACCOUNT_KEY not configured',
        // Return fallback static data
        fallback: true,
        data: {
          activeUsers: 0,
          countries: [],
          cities: [],
          pages: [],
          updated: new Date().toISOString()
        }
      });
    }

    const key = JSON.parse(keyJson);
    const auth = new google.auth.GoogleAuth({
      credentials: key,
      scopes: ['https://www.googleapis.com/auth/analytics.readonly']
    });

    const analyticsData = google.analyticsdata({ version: 'v1beta', auth });
    const propertyId = process.env.GA_PROPERTY_ID || '521853091';

    // Run 3 realtime reports in parallel
    const [usersReport, countriesReport, citiesReport, pagesReport] = await Promise.all([
      // 1. Active users total
      analyticsData.properties.runRealtimeReport({
        property: `properties/${propertyId}`,
        requestBody: {
          metrics: [{ name: 'activeUsers' }]
        }
      }),
      // 2. Active users by country
      analyticsData.properties.runRealtimeReport({
        property: `properties/${propertyId}`,
        requestBody: {
          dimensions: [{ name: 'country' }],
          metrics: [{ name: 'activeUsers' }],
          orderBys: [{ metric: { metricName: 'activeUsers' }, desc: true }],
          limit: 10
        }
      }),
      // 3. Active users by city
      analyticsData.properties.runRealtimeReport({
        property: `properties/${propertyId}`,
        requestBody: {
          dimensions: [{ name: 'city' }],
          metrics: [{ name: 'activeUsers' }],
          orderBys: [{ metric: { metricName: 'activeUsers' }, desc: true }],
          limit: 10
        }
      }),
      // 4. Active users by page title
      analyticsData.properties.runRealtimeReport({
        property: `properties/${propertyId}`,
        requestBody: {
          dimensions: [{ name: 'unifiedScreenName' }],
          metrics: [{ name: 'activeUsers' }],
          orderBys: [{ metric: { metricName: 'activeUsers' }, desc: true }],
          limit: 8
        }
      })
    ]);

    // Parse results
    const activeUsers = usersReport.data.rows?.[0]?.metricValues?.[0]?.value || '0';

    const countries = (countriesReport.data.rows || []).map(r => ({
      n: r.dimensionValues[0].value,
      v: parseInt(r.metricValues[0].value)
    }));

    const cities = (citiesReport.data.rows || []).map(r => ({
      n: r.dimensionValues[0].value,
      v: parseInt(r.metricValues[0].value)
    })).filter(c => c.n !== '(not set)');

    const pages = (pagesReport.data.rows || []).map(r => ({
      n: r.dimensionValues[0].value,
      v: parseInt(r.metricValues[0].value)
    }));

    return res.status(200).json({
      ok: true,
      data: {
        activeUsers: parseInt(activeUsers),
        countries,
        cities,
        pages,
        updated: new Date().toISOString()
      }
    });

  } catch (err) {
    console.error('GA Realtime API error:', err.message);
    return res.status(200).json({
      ok: false,
      error: err.message,
      fallback: true,
      data: {
        activeUsers: 0,
        countries: [],
        cities: [],
        pages: [],
        updated: new Date().toISOString()
      }
    });
  }
};
