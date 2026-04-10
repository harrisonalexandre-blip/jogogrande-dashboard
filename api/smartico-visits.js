// Smartico Visits API — pulls visit_count, registration_count, ftd_count
// aggregated by day from TheAffiliatePlatform reporting API

const API_URL = 'https://boapi3.smartico.ai/api/af2_media_report_op';
const API_KEY = '13d4a8d4-2e2e-11f1-8319-027e66b7665d-12447';

export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  res.setHeader('Content-Type', 'application/json');
  // Cache for 1 hour
  res.setHeader('Cache-Control', 's-maxage=3600, stale-while-revalidate=1800');

  if (req.method === 'OPTIONS') return res.status(200).end();

  try {
    const days = parseInt(req.query.days) || 30;
    // Use BRT (UTC-3) for date calculations to match Phoenix/dashboard timezone
    const now = new Date(Date.now() - 3 * 60 * 60 * 1000);
    const from = new Date(now);
    from.setDate(from.getDate() - days);
    // date_to is exclusive in Smartico API, so add 1 day
    const to = new Date(now);
    to.setDate(to.getDate() + 1);

    const dateFrom = from.toISOString().slice(0, 10);
    const dateTo = to.toISOString().slice(0, 10);

    const url = `${API_URL}?aggregation_period=DAY&date_from=${dateFrom}&date_to=${dateTo}`;
    const resp = await fetch(url, {
      headers: { authorization: API_KEY },
    });

    if (!resp.ok) throw new Error(`Smartico API: ${resp.status}`);
    const data = await resp.json();

    if (data.errCode) {
      return res.status(200).json({ error: 'Smartico API error', errCode: data.errCode, message: data.errMsg });
    }

    const rows = data.data || data.result || [];

    // Aggregate by day
    const dailyMap = {};
    for (const r of rows) {
      const dt = (r.dt || '').slice(0, 10);
      if (!dt) continue;
      if (!dailyMap[dt]) dailyMap[dt] = { date: dt, visits: 0, regs: 0, ftd: 0 };
      dailyMap[dt].visits += r.visit_count || 0;
      dailyMap[dt].regs += r.registration_count || 0;
      dailyMap[dt].ftd += r.ftd_count || 0;
    }

    const daily = Object.values(dailyMap).sort((a, b) => a.date.localeCompare(b.date));

    // Totals
    const totals = daily.reduce((t, d) => ({
      visits: t.visits + d.visits,
      regs: t.regs + d.regs,
      ftd: t.ftd + d.ftd,
    }), { visits: 0, regs: 0, ftd: 0 });

    // Period aggregations (using BRT date)
    const today = now.toISOString().slice(0, 10);
    const yesterday = new Date(now); yesterday.setDate(yesterday.getDate() - 1);
    const yesterdayStr = yesterday.toISOString().slice(0, 10);
    console.log(`[Smartico Visits] BRT today=${today}, dateFrom=${dateFrom}, dateTo=${dateTo}, rows=${rows.length}`);
    const d7 = new Date(now); d7.setDate(d7.getDate() - 7);
    const d7Str = d7.toISOString().slice(0, 10);
    const d30 = new Date(now); d30.setDate(d30.getDate() - 30);
    const d30Str = d30.toISOString().slice(0, 10);

    const agg = (from) => daily.filter(d => d.date >= from).reduce((t, d) => ({
      visits: t.visits + d.visits, regs: t.regs + d.regs, ftd: t.ftd + d.ftd,
    }), { visits: 0, regs: 0, ftd: 0 });

    const todayData = dailyMap[today] || { visits: 0, regs: 0, ftd: 0 };
    const yesterdayData = dailyMap[yesterdayStr] || { visits: 0, regs: 0, ftd: 0 };

    return res.status(200).json({
      status: 'ok',
      daily,
      totals,
      periods: {
        today: todayData,
        yesterday: yesterdayData,
        week: agg(d7Str),
        month: agg(d30Str),
      },
      updated: now.toISOString(),
    });

  } catch (err) {
    console.error('Smartico Visits Error:', err);
    return res.status(500).json({ error: 'Erro ao buscar visits', message: err.message });
  }
}
