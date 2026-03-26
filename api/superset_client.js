/**
 * SUPERSET API CLIENT — Jogo Grande
 *
 * Puxa dados automaticamente do Apache Superset via REST API.
 * Sem necessidade de acesso direto ao ClickHouse.
 *
 * Fluxo: Login → CSRF Token → Chart Data API → JSON
 */

const SUPERSET_CONFIG = {
    baseUrl: 'https://superset.eks-prod.doforce.work',
    username: 'harrisonwash@gmail.com',
    password: 'JogoGrande123!',
};

// Chart IDs do JogoGrande (project_id=176)
const CHART_IDS = {
    // Casino
    casino_daily: 2047,          // Turnover Day (daily summary: uap, spins, turnover, payout, ggr, ggr%)
    casino_total: 2048,          // Casino total (aggregate by currency)
    casino_by_user: 2053,        // Casino by user (per player stats)
    casino_by_game: 2055,        // Casino by game (per game/provider stats)
    casino_user_game: 2054,      // Casino user+game cross
    casino_users_info: 2061,     // Users info (registration, verification, etc)
    casino_ggr_big: 2058,        // GGR total (big number)
    casino_uap_big: 2057,        // UAP total (big number)
    casino_ggr_pct_big: 2059,    // GGR% (big number)
    casino_turnover_area: 2050,  // Turnover trend (area chart)
    casino_ggr_area: 2052,       // GGR trend (area chart)

    // Cohorts
    casino_cohort: 4524,         // Casino cohort by registration month
    sport_cohort: 4525,          // Sport cohort by registration month
    payment_cohort: 4522,        // Payment cohort

    // Payment & Balance
    payment_users: 4523,         // Payment users detail
    balance_users: 4527,         // Client balances (real + bonus)
    balance_monthly: 4528,       // Balance by month
};

class SupersetClient {
    constructor(config = SUPERSET_CONFIG) {
        this.baseUrl = config.baseUrl;
        this.username = config.username;
        this.password = config.password;
        this.accessToken = null;
        this.csrfToken = null;
        this.cookies = '';
    }

    /**
     * Step 1: Login via REST API and get JWT access token
     */
    async login() {
        const resp = await fetch(`${this.baseUrl}/api/v1/security/login`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                username: this.username,
                password: this.password,
                provider: 'db',
                refresh: true,
            }),
        });

        if (!resp.ok) throw new Error(`Login failed: ${resp.status} ${resp.statusText}`);

        const data = await resp.json();
        this.accessToken = data.access_token;

        // Capture cookies for session
        const setCookie = resp.headers.get('set-cookie');
        if (setCookie) this.cookies = setCookie;

        console.log('[Superset] Login OK — token obtained');
        return this.accessToken;
    }

    /**
     * Step 2: Get CSRF token (needed for data requests)
     */
    async getCsrfToken() {
        const resp = await fetch(`${this.baseUrl}/api/v1/security/csrf_token/`, {
            headers: {
                'Authorization': `Bearer ${this.accessToken}`,
            },
        });

        if (!resp.ok) throw new Error(`CSRF failed: ${resp.status}`);

        const data = await resp.json();
        this.csrfToken = data.result;
        console.log('[Superset] CSRF token obtained');
        return this.csrfToken;
    }

    /**
     * Step 3: Pull data from a specific chart
     */
    async getChartData(chartId) {
        if (!this.accessToken) await this.login();
        if (!this.csrfToken) await this.getCsrfToken();

        const resp = await fetch(`${this.baseUrl}/api/v1/chart/${chartId}/data/`, {
            headers: {
                'Authorization': `Bearer ${this.accessToken}`,
                'X-CSRFToken': this.csrfToken,
            },
        });

        if (!resp.ok) {
            if (resp.status === 401) {
                // Token expired, re-login
                console.log('[Superset] Token expired, re-authenticating...');
                await this.login();
                await this.getCsrfToken();
                return this.getChartData(chartId);
            }
            throw new Error(`Chart ${chartId} failed: ${resp.status}`);
        }

        const data = await resp.json();

        if (data.result && data.result[0]) {
            const r = data.result[0];
            return {
                status: r.status,
                rowcount: r.rowcount,
                columns: r.colnames,
                data: r.data,
            };
        }

        throw new Error(`Chart ${chartId}: no data returned`);
    }

    /**
     * Pull ALL JogoGrande charts at once
     */
    async getAllData() {
        await this.login();
        await this.getCsrfToken();

        const results = {};
        const errors = [];

        for (const [name, chartId] of Object.entries(CHART_IDS)) {
            try {
                console.log(`[Superset] Pulling ${name} (chart ${chartId})...`);
                results[name] = await this.getChartData(chartId);
                console.log(`  ✓ ${results[name].rowcount} rows`);
            } catch (e) {
                console.error(`  ✗ ${name}: ${e.message}`);
                errors.push({ chart: name, id: chartId, error: e.message });
            }
        }

        return { results, errors, timestamp: new Date().toISOString() };
    }

    /**
     * Pull only the essential daily metrics (for scheduled task)
     */
    async getDailyMetrics() {
        await this.login();
        await this.getCsrfToken();

        const essentialCharts = {
            casino_daily: CHART_IDS.casino_daily,
            casino_by_game: CHART_IDS.casino_by_game,
            balance_users: CHART_IDS.balance_users,
            payment_cohort: CHART_IDS.payment_cohort,
        };

        const results = {};
        for (const [name, chartId] of Object.entries(essentialCharts)) {
            try {
                results[name] = await this.getChartData(chartId);
            } catch (e) {
                results[name] = { error: e.message };
            }
        }

        return { data: results, timestamp: new Date().toISOString() };
    }
}

// Helper: format chart data for our dashboard
function formatCasinoDaily(chartData) {
    if (!chartData || !chartData.data) return [];
    return chartData.data.map(r => ({
        date: new Date(r.spin_date).toISOString().split('T')[0],
        uap: r.casino_uap,
        spins: r.casino_spin_count,
        avg_spin_eur: Math.round(r.avg_spin_eur * 100) / 100,
        turnover_eur: Math.round(r.casino_turnover_eur * 100) / 100,
        payout_eur: Math.round(r.casino_payout_eur * 100) / 100,
        ggr_eur: Math.round(r.ggr_eur * 100) / 100,
        ggr_pct: Math.round(r.ggr_percent * 100) / 100,
    })).sort((a, b) => a.date.localeCompare(b.date));
}

function formatGameData(chartData) {
    if (!chartData || !chartData.data) return [];
    return chartData.data.map(r => ({
        integrator: r.intergrator_name,
        provider: r.provider_name,
        game: r.game_name,
        game_id: r.game_id,
        uap: r.casino_uap,
        turnover_eur: Math.round(r.casino_turnover_eur * 100) / 100,
        payout_eur: Math.round(r.casino_payout_eur * 100) / 100,
        ggr_eur: Math.round(r.ggr_eur * 100) / 100,
        ggr_pct: Math.round(r.ggr_percent * 100) / 100,
        avg_spin_eur: Math.round(r.avg_spin_eur * 100) / 100,
    })).sort((a, b) => b.turnover_eur - a.turnover_eur);
}

module.exports = { SupersetClient, CHART_IDS, formatCasinoDaily, formatGameData, SUPERSET_CONFIG };
