#!/usr/bin/env python3
"""
phoenix_sync.py — Sync dashboard com Phoenix DB (ClickHouse) direto.
Substitui Smartico API + DTP CSV como fonte de dados principal.
Todas as queries usam timezone BRT (America/Sao_Paulo).

Uso: python3 scripts/phoenix_sync.py [--days N] [--no-deploy]
"""

import clickhouse_connect
import json, re, os, sys, subprocess
from datetime import datetime, timedelta
from collections import defaultdict

# ── Config ──
CH_HOST = 'analytics.phoenix365-prod.com'
CH_PORT = 443
CH_USER = 'project_176_ro'
CH_PASS = 'uChiThoocahtiek9'
CH_DB   = 'project_176'
TZ      = 'America/Sao_Paulo'

REPO_DIR = os.environ.get('REPO_DIR', '/Users/harrison/Documents/Jogo Grande/JOGO GRANDE')
INDEX    = os.path.join(REPO_DIR, 'index.html')

# ── Args ──
import argparse
parser = argparse.ArgumentParser()
parser.add_argument('--days', type=int, default=1, help='Quantos dias atualizar (1=hoje, 7=semana, 0=todos)')
parser.add_argument('--no-deploy', action='store_true', help='Não fazer deploy após sync')
args = parser.parse_args()

# ── Connect ──
print(f"🔌 Conectando Phoenix DB ({CH_HOST})...")
client = clickhouse_connect.get_client(
    host=CH_HOST, port=CH_PORT,
    username=CH_USER, password=CH_PASS,
    secure=True
)
print("✅ Conectado!")

# ── Determine date range ──
now_brt = datetime.now()  # assume máquina em BRT
TODAY = now_brt.strftime('%Y-%m-%d')

if args.days == 0:
    DATE_FROM = '2025-11-01'  # início dos dados
else:
    DATE_FROM = (now_brt - timedelta(days=max(args.days - 1, 0))).strftime('%Y-%m-%d')

DATE_TO = TODAY
YESTERDAY = (now_brt - timedelta(days=1)).strftime('%Y-%m-%d')
WEEK_START = (now_brt - timedelta(days=now_brt.weekday())).strftime('%Y-%m-%d')
print(f"📅 Período: {DATE_FROM} → {DATE_TO}")

# ── Query 1: Daily KPIs ──
print("\n1️⃣  Buscando KPIs diários...")

daily_data = client.query(f"""
WITH
    regs AS (
        SELECT
            toDate(toTimezone(registration_time, '{TZ}')) as dt,
            count() as nr
        FROM {CH_DB}.client
        WHERE toDate(toTimezone(registration_time, '{TZ}')) >= '{DATE_FROM}'
          AND toDate(toTimezone(registration_time, '{TZ}')) <= '{DATE_TO}'
          AND is_test = false
        GROUP BY dt
    ),
    deps AS (
        SELECT
            toDate(toTimezone(create_time, '{TZ}')) as dt,
            count() as dep_n,
            sum(amount)/100.0 as dep_total,
            count(DISTINCT client_id) as dep_unique
        FROM {CH_DB}.deposits
        WHERE toDate(toTimezone(create_time, '{TZ}')) >= '{DATE_FROM}'
          AND toDate(toTimezone(create_time, '{TZ}')) <= '{DATE_TO}'
          AND status = 'PAID'
        GROUP BY dt
    ),
    first_deps AS (
        SELECT client_id, min(toDate(toTimezone(create_time, '{TZ}'))) as first_dt
        FROM {CH_DB}.deposits WHERE status = 'PAID'
        GROUP BY client_id
    ),
    ftds AS (
        SELECT first_dt as dt, count() as ftd_count
        FROM first_deps
        WHERE first_dt >= '{DATE_FROM}' AND first_dt <= '{DATE_TO}'
        GROUP BY dt
    ),
    fdas AS (
        SELECT
            toDate(toTimezone(d.create_time, '{TZ}')) as dt,
            sum(d.amount)/100.0 as fda_total
        FROM {CH_DB}.deposits d
        INNER JOIN first_deps f ON d.client_id = f.client_id
            AND f.first_dt = toDate(toTimezone(d.create_time, '{TZ}'))
        WHERE toDate(toTimezone(d.create_time, '{TZ}')) >= '{DATE_FROM}'
          AND toDate(toTimezone(d.create_time, '{TZ}')) <= '{DATE_TO}'
          AND d.status = 'PAID'
        GROUP BY dt
    ),
    wds AS (
        SELECT
            toDate(toTimezone(create_time, '{TZ}')) as dt,
            count() as wd_n,
            sum(amount)/100.0 as wd_total
        FROM {CH_DB}.withdrawals
        WHERE toDate(toTimezone(create_time, '{TZ}')) >= '{DATE_FROM}'
          AND toDate(toTimezone(create_time, '{TZ}')) <= '{DATE_TO}'
          AND status = 'PAID'
        GROUP BY dt
    ),
    casino AS (
        SELECT
            toDate(toTimezone(spin_date, '{TZ}')) as dt,
            sum(CASE WHEN status='BET' THEN amount ELSE 0 END)/100.0 as c_to,
            (sum(CASE WHEN status='BET' THEN amount ELSE 0 END) -
             sum(CASE WHEN status='WIN' THEN amount ELSE 0 END))/100.0 as c_ggr,
            count(DISTINCT client_id) as c_uap
        FROM {CH_DB}.casino
        WHERE toDate(toTimezone(spin_date, '{TZ}')) >= '{DATE_FROM}'
          AND toDate(toTimezone(spin_date, '{TZ}')) <= '{DATE_TO}'
          AND is_free_spin = false
        GROUP BY dt
    ),
    sport AS (
        SELECT
            toDate(toTimezone(create_time, '{TZ}')) as dt,
            sum(bet_amount)/100.0 as sb_to,
            (sum(bet_amount) - sum(CASE WHEN bet_status='WIN' THEN payout ELSE 0 END))/100.0 as sb_ggr,
            count(DISTINCT client_id) as sb_uap
        FROM {CH_DB}.bet_change
        WHERE toDate(toTimezone(create_time, '{TZ}')) >= '{DATE_FROM}'
          AND toDate(toTimezone(create_time, '{TZ}')) <= '{DATE_TO}'
          AND test_mode = false
        GROUP BY dt
    )
SELECT
    r.dt,
    r.nr,
    COALESCE(dp.dep_n, 0) as dep_n,
    COALESCE(dp.dep_total, 0) as dep_total,
    COALESCE(dp.dep_unique, 0) as dep_unique,
    COALESCE(f.ftd_count, 0) as ftd,
    COALESCE(fa.fda_total, 0) as fda,
    COALESCE(w.wd_n, 0) as wd_n,
    COALESCE(w.wd_total, 0) as wd_total,
    COALESCE(c.c_to, 0) as c_to,
    COALESCE(c.c_ggr, 0) as c_ggr,
    COALESCE(c.c_uap, 0) as c_uap,
    COALESCE(s.sb_to, 0) as sb_to,
    COALESCE(s.sb_ggr, 0) as sb_ggr,
    COALESCE(s.sb_uap, 0) as sb_uap
FROM regs r
LEFT JOIN deps dp ON r.dt = dp.dt
LEFT JOIN ftds f ON r.dt = f.dt
LEFT JOIN fdas fa ON r.dt = fa.dt
LEFT JOIN wds w ON r.dt = w.dt
LEFT JOIN casino c ON r.dt = c.dt
LEFT JOIN sport s ON r.dt = s.dt
ORDER BY r.dt
""")

rows = daily_data.result_rows
cols = daily_data.column_names
print(f"   → {len(rows)} dias retornados")

# ── Query 2: Hourly data for today ──
print("\n2️⃣  Buscando dados horários (hoje BRT)...")

hourly_regs = client.query(f"""
    SELECT toHour(toTimezone(registration_time, '{TZ}')) as h, count()
    FROM {CH_DB}.client
    WHERE toDate(toTimezone(registration_time, '{TZ}')) = '{TODAY}' AND is_test = false
    GROUP BY h ORDER BY h
""")

hourly_ftd_q = client.query(f"""
    WITH fd AS (
        SELECT client_id, min(toDate(toTimezone(create_time, '{TZ}'))) as first_dt
        FROM {CH_DB}.deposits WHERE status='PAID' GROUP BY client_id
    ),
    -- Assign each FTD to the hour of their FIRST deposit only (no double-counting)
    ftd_first_hour AS (
        SELECT d.client_id,
               toHour(toTimezone(min(d.create_time), '{TZ}')) as h
        FROM {CH_DB}.deposits d
        JOIN fd ON d.client_id=fd.client_id AND fd.first_dt='{TODAY}'
        WHERE toDate(toTimezone(d.create_time, '{TZ}'))='{TODAY}' AND d.status='PAID'
        GROUP BY d.client_id
    )
    SELECT h, count() FROM ftd_first_hour GROUP BY h ORDER BY h
""")

hourly_vol = client.query(f"""
    SELECT toHour(toTimezone(spin_date, '{TZ}')) as h,
        sum(CASE WHEN status='BET' THEN amount ELSE 0 END)/100.0
    FROM {CH_DB}.casino
    WHERE toDate(toTimezone(spin_date, '{TZ}'))='{TODAY}' AND is_free_spin=false
    GROUP BY h ORDER BY h
""")

# Build 24h arrays
hr_regs = [0]*24
hr_ftd = [0]*24
hr_vol = [0.0]*24

for row in hourly_regs.result_rows:
    hr_regs[row[0]] = int(row[1])
for row in hourly_ftd_q.result_rows:
    hr_ftd[row[0]] = int(row[1])
for row in hourly_vol.result_rows:
    hr_vol[row[0]] = round(float(row[1]), 2)

total_hr_regs = sum(hr_regs)
total_hr_ftd = sum(hr_ftd)
total_hr_vol = sum(hr_vol)
print(f"   → Regs={total_hr_regs}, FTD={total_hr_ftd}, Vol=R${total_hr_vol:,.0f}")

# ── Query 3: RECON data (deposits/withdrawals/MI per day) for DTP overlay ──
print("\n3️⃣  Buscando RECON (depósitos/saques diários)...")

recon_data = client.query(f"""
    WITH
    deps AS (
        SELECT
            toDate(toTimezone(create_time, '{TZ}')) as dt,
            count() as dep_n,
            sum(amount)/100.0 as dep_total
        FROM {CH_DB}.deposits
        WHERE toDate(toTimezone(create_time, '{TZ}')) >= '2025-11-01'
          AND status = 'PAID'
        GROUP BY dt
    ),
    wds AS (
        SELECT
            toDate(toTimezone(create_time, '{TZ}')) as dt,
            count() as wd_n,
            sum(amount)/100.0 as wd_total
        FROM {CH_DB}.withdrawals
        WHERE toDate(toTimezone(create_time, '{TZ}')) >= '2025-11-01'
          AND status = 'PAID'
        GROUP BY dt
    ),
    -- MI (Movimentação Interna) não existe no ClickHouse.
    -- CANCELLED/CANCELLED_BY_CLIENT são saques cancelados, NÃO mov interna.
    -- MI real vem da DTP (dtp_mi) e deve ser inserido manualmente ou via DTP API.
    mi_placeholder AS (SELECT 1 as x)
    SELECT
        d.dt,
        d.dep_n, d.dep_total,
        COALESCE(w.wd_n, 0), COALESCE(w.wd_total, 0),
        0 as mi_n, 0.0 as mi_total
    FROM deps d
    LEFT JOIN wds w ON d.dt = w.dt
    ORDER BY d.dt
""")

recon_rows = recon_data.result_rows
print(f"   → {len(recon_rows)} dias com dados financeiros")

# ── Read index.html ──
print("\n4️⃣  Lendo index.html...")
with open(INDEX, 'r', encoding='utf-8') as f:
    html = f.read()

# ── Update D[] array ──
print("\n5️⃣  Atualizando D[] array...")

# Parse existing D array
d_match = re.search(r'const D=(\[.*?\]);', html)
if not d_match:
    print("❌ Não encontrou D[] no HTML!")
    sys.exit(1)

D = json.loads(d_match.group(1))
d_map = {d['date']: i for i, d in enumerate(D)}

updated = 0
created = 0

for row in rows:
    dt_str = str(row[0])
    nr, dep_n, dep_total, dep_unique, ftd, fda = row[1], row[2], row[3], row[4], row[5], row[6]
    wd_n, wd_total = row[7], row[8]
    c_to, c_ggr, c_uap = row[9], row[10], row[11]
    sb_to, sb_ggr, sb_uap = row[12], row[13], row[14]

    dt_obj = datetime.strptime(dt_str, '%Y-%m-%d')
    nc = dep_total - wd_total
    ngr = c_ggr + sb_ggr

    entry = {
        'date': dt_str,
        'df': dt_obj.strftime('%d/%m'),
        'month': dt_obj.strftime('%Y-%m'),
        'ml': dt_obj.strftime('%b/%y').upper(),
        'wk': dt_obj.isocalendar()[1],
        'yr': dt_obj.year,
        'nr': int(nr),
        'ftd': int(ftd),
        'uap': int(c_uap + sb_uap),
        'sb_uap': int(sb_uap),
        'sb_to': round(float(sb_to), 2),
        'sb_po': 0.0,  # not available directly
        'sb_ggr': round(float(sb_ggr), 2),
        'sb_bc': 0,
        'sb_bon': 0.0,
        'sb_ngr': round(float(sb_ggr), 2),
        'c_uap': int(c_uap),
        'c_to': round(float(c_to), 2),
        'c_po': round(float(c_to) - float(c_ggr), 2),
        'cg': round(float(c_ggr), 2),
        'c_sc': 0,
        'c_bon': 0.0,
        'nd': int(dep_n),
        'ds': round(float(dep_total), 2),
        'nw': int(wd_n),
        'ws': round(float(wd_total), 2),
        'ngr': round(float(ngr), 2),
        'nc': round(float(nc), 2),
        'sg': round(float(sb_ggr), 2),
        'tk': round(float(dep_total / dep_n), 2) if dep_n > 0 else 0,
        'mi': 0,
        'fda': round(float(fda), 2)
    }

    if dt_str in d_map:
        idx = d_map[dt_str]
        existing = D[idx]

        # If Phoenix has NO casino data for this day but existing does,
        # preserve the original casino fields (Smartico data for NOV-JAN)
        CASINO_KEYS = {'c_to', 'c_po', 'cg', 'c_uap', 'c_sc', 'c_bon'}
        phoenix_has_casino = float(c_to) > 0 or float(c_ggr) != 0
        existing_has_casino = existing.get('cg', 0) != 0 or existing.get('c_to', 0) > 0

        if not phoenix_has_casino and existing_has_casino:
            # Keep existing casino + ngr, only update non-casino fields
            for k, v in entry.items():
                if k not in CASINO_KEYS and k != 'ngr' and k != 'uap':
                    existing[k] = v
            # Recalc NGR = existing casino GGR + new sports GGR
            existing['ngr'] = round(existing.get('cg', 0) + float(sb_ggr), 2)
            existing['uap'] = existing.get('c_uap', 0) + int(sb_uap)
        else:
            for k, v in entry.items():
                existing[k] = v

        updated += 1
    else:
        D.append(entry)
        d_map[dt_str] = len(D) - 1
        created += 1

# Sort by date
D.sort(key=lambda x: x['date'])
print(f"   → {updated} atualizados, {created} criados. Total: {len(D)} dias")

# ── Update RECON[] ──
print("\n6️⃣  Atualizando RECON[]...")

recon_list = []
for row in recon_rows:
    dt_str = str(row[0])
    recon_list.append({
        'd': dt_str,
        'dtp_dep': round(float(row[2]), 2),
        'dtp_dep_n': int(row[1]),
        'dtp_saq': round(float(row[4]), 2),
        'dtp_saq_n': int(row[3]),
        'dtp_mi': round(float(row[6]), 2),
        'dtp_mi_n': int(row[5])
    })

print(f"   → {len(recon_list)} dias RECON")

# ── Query 4: CLI_M — Top Players per period ──
print("\n4️⃣  Buscando rankings de jogadores (CLI_M)...")

def build_player_rankings(period_expr, period_alias, date_from, date_to):
    """Query top players by turnover and GGR for a given period grouping."""
    q = f"""
    WITH
        casino_pl AS (
            SELECT
                client_id,
                {period_expr} as period,
                sum(CASE WHEN status='BET' THEN amount ELSE 0 END)/100.0 as turnover,
                (sum(CASE WHEN status='BET' THEN amount ELSE 0 END) -
                 sum(CASE WHEN status='WIN' THEN amount ELSE 0 END))/100.0 as ggr
            FROM {CH_DB}.casino
            WHERE toDate(toTimezone(spin_date, '{TZ}')) >= '{date_from}'
              AND toDate(toTimezone(spin_date, '{TZ}')) <= '{date_to}'
              AND is_free_spin = false
            GROUP BY client_id, period
        ),
        sport_pl AS (
            SELECT
                client_id,
                {period_expr.replace('spin_date','create_time')} as period,
                sum(bet_amount)/100.0 as turnover,
                (sum(bet_amount) - sum(CASE WHEN bet_status='WIN' THEN payout ELSE 0 END))/100.0 as ggr
            FROM {CH_DB}.bet_change
            WHERE toDate(toTimezone(create_time, '{TZ}')) >= '{date_from}'
              AND toDate(toTimezone(create_time, '{TZ}')) <= '{date_to}'
              AND test_mode = false
            GROUP BY client_id, period
        ),
        combined AS (
            SELECT
                COALESCE(c.client_id, s.client_id) as client_id,
                COALESCE(c.period, s.period) as period,
                COALESCE(c.turnover, 0) + COALESCE(s.turnover, 0) as total_to,
                COALESCE(c.ggr, 0) + COALESCE(s.ggr, 0) as total_ggr
            FROM casino_pl c
            FULL OUTER JOIN sport_pl s ON c.client_id = s.client_id AND c.period = s.period
        )
    SELECT
        cb.period,
        cb.client_id,
        cb.total_to,
        cb.total_ggr
    FROM combined cb
    ORDER BY cb.period, cb.total_to DESC
    """
    return client.query(q)

# Build client name lookup
print("   Carregando nomes dos clientes...")
client_names = {}
try:
    # Try first_name + last_name
    name_q = client.query(f"""
        SELECT client_id, first_name, last_name
        FROM {CH_DB}.client
        WHERE is_test = false
    """)
    for row in name_q.result_rows:
        cid = row[0]
        fn = (row[1] or '').strip()
        ln = (row[2] or '').strip()
        # Avoid duplicated names when first_name already contains last_name
        if ln and fn.lower().endswith(ln.lower()):
            name = fn
        else:
            name = f"{fn} {ln}".strip()
        client_names[cid] = name if name else f"Player {cid}"
    print(f"   → {len(client_names)} nomes carregados")
except Exception as e:
    print(f"   ⚠️ Erro ao carregar nomes (tentando campo 'name'): {e}")
    try:
        name_q = client.query(f"SELECT client_id, user_name FROM {CH_DB}.client WHERE is_test = false")
        for row in name_q.result_rows:
            client_names[row[0]] = (row[1] or '').strip() or f"Player {row[0]}"
        print(f"   → {len(client_names)} nomes carregados (campo 'name')")
    except Exception as e2:
        print(f"   ⚠️ Sem nomes disponíveis: {e2}")

def get_name(cid):
    return client_names.get(cid, f"Player {cid}")

# Build CLI_M: monthly keys + daily (today + yesterday) + weekly
cli_m = {}

try:
    # Monthly rankings
    monthly_result = build_player_rankings(
        f"formatDateTime(toStartOfMonth(toTimezone(spin_date, '{TZ}')), '%Y-%m')",
        'month',
        '2025-11-01', TODAY
    )
    # Group by period
    monthly_groups = defaultdict(list)
    for row in monthly_result.result_rows:
        period, cid, turnover, ggr = row
        if not period:
            continue
        monthly_groups[period].append({'n': get_name(cid), 'to': round(float(turnover), 2), 'ggr': round(float(ggr), 2)})

    for period, players in monthly_groups.items():
        top_to = sorted(players, key=lambda x: -x['to'])[:10]
        sorted_ggr = sorted(players, key=lambda x: x['ggr'])
        winners = sorted_ggr[:10]  # most negative GGR = winners (bad for house)
        losers = sorted(players, key=lambda x: -x['ggr'])[:10]  # most positive GGR = losers (good for house)
        cli_m[period] = {
            'to': [{'n': p['n'], 'v': p['to']} for p in top_to],
            'w': [{'n': p['n'], 'v': p['ggr']} for p in winners],
            'l': [{'n': p['n'], 'v': p['ggr']} for p in losers]
        }
    print(f"   → CLI_M mensal: {len(monthly_groups)} meses")

    # Daily rankings (today + yesterday)
    daily_result = build_player_rankings(
        f"formatDateTime(toDate(toTimezone(spin_date, '{TZ}')), '%Y-%m-%d')",
        'day',
        YESTERDAY, TODAY
    )
    daily_groups = defaultdict(list)
    for row in daily_result.result_rows:
        period, cid, turnover, ggr = row
        daily_groups[period].append({'n': get_name(cid), 'to': round(float(turnover), 2), 'ggr': round(float(ggr), 2)})

    for period, players in daily_groups.items():
        top_to = sorted(players, key=lambda x: -x['to'])[:10]
        sorted_ggr = sorted(players, key=lambda x: x['ggr'])
        winners = sorted_ggr[:10]
        losers = sorted(players, key=lambda x: -x['ggr'])[:10]
        cli_m[period] = {
            'to': [{'n': p['n'], 'v': p['to']} for p in top_to],
            'w': [{'n': p['n'], 'v': p['ggr']} for p in winners],
            'l': [{'n': p['n'], 'v': p['ggr']} for p in losers]
        }
    print(f"   → CLI_M diário: {len(daily_groups)} dias (hoje+ontem)")

    # Weekly rankings (current week: Monday → today)
    weekly_result = build_player_rankings(
        f"'W'",
        'week',
        WEEK_START, TODAY
    )
    week_players = []
    for row in weekly_result.result_rows:
        period, cid, turnover, ggr = row
        week_players.append({'n': get_name(cid), 'to': round(float(turnover), 2), 'ggr': round(float(ggr), 2)})

    if week_players:
        top_to = sorted(week_players, key=lambda x: -x['to'])[:10]
        winners = sorted(week_players, key=lambda x: x['ggr'])[:10]
        losers = sorted(week_players, key=lambda x: -x['ggr'])[:10]
        cli_m['W'] = {
            'to': [{'n': p['n'], 'v': p['to']} for p in top_to],
            'w': [{'n': p['n'], 'v': p['ggr']} for p in winners],
            'l': [{'n': p['n'], 'v': p['ggr']} for p in losers]
        }
    print(f"   → CLI_M total: {len(cli_m)} chaves")

except Exception as e:
    print(f"   ⚠️ Erro CLI_M: {e}. Preservando dados atuais.")
    cli_m = None

# ── Query 5: Per-affiliate daily data from Phoenix (BRT timezone) ──
print("\n5️⃣  Buscando dados per-afiliado do Phoenix (timezone BRT)...")

# Get aff_id → name mapping from Smartico API
aff_id_map = {}
try:
    import requests as req
    SMARTICO_HOST = "https://boapi3.smartico.ai"
    SMARTICO_KEY = "13d4a8d4-2e2e-11f1-8319-027e66b7665d-12447"
    sm_resp = req.get(f'{SMARTICO_HOST}/api/af2_media_report_op',
        headers={'authorization': SMARTICO_KEY},
        params={'aggregation_period': 'MONTH', 'date_from': '2025-11-01',
                'date_to': TODAY, 'group_by': 'affiliate_id'}, timeout=30)
    sm_data = sm_resp.json()
    sm_rows = sm_data.get('data', sm_data) if isinstance(sm_data, dict) else sm_data
    for r in sm_rows:
        aid = str(r.get('affiliate_id', ''))
        name = r.get('affiliate_name', '')
        if aid and name and aid not in aff_id_map:
            aff_id_map[aid] = name
    print(f"   → {len(aff_id_map)} afiliados mapeados (Smartico)")
except Exception as e:
    print(f"   ⚠️ Erro mapeamento Smartico: {e}")

def get_aff_name(aid):
    return aff_id_map.get(str(aid), f"Afiliado {aid}")

# Query Phoenix: per-aff_id daily data using SEPARATE queries (no FULL OUTER JOIN)
# Each metric is queried independently and merged in Python to avoid row duplication
phoenix_aff_days = {}
try:
    # Common CTE: aff_ids mapping (GROUP BY client_id to avoid duplicates)
    AFF_IDS_CTE = f"""
        SELECT client_id,
               any(extractAllGroupsVertical(btag, 'aff_id=([0-9]+)')[1][1]) as aff_id
        FROM {CH_DB}.client
        WHERE btag != '' AND is_test = false
        GROUP BY client_id
    """

    # Helper: {(aff_id, date_str): value}
    def query_metric(sql, label):
        res = client.query(sql)
        data = {}
        for row in res.result_rows:
            aid, dt = str(row[0]), str(row[1])
            if not aid:
                continue
            vals = row[2:]  # remaining columns
            data[(aid, dt)] = vals[0] if len(vals) == 1 else vals
        print(f"      {label}: {len(data)} registros")
        return data

    # 1) Registrations
    reg_data = query_metric(f"""
        WITH aff_ids AS ({AFF_IDS_CTE})
        SELECT a.aff_id,
               toDate(toTimezone(c.registration_time, '{TZ}')) as dt,
               count() as cnt
        FROM {CH_DB}.client c
        JOIN aff_ids a ON c.client_id = a.client_id
        WHERE toDate(toTimezone(c.registration_time, '{TZ}')) >= '{DATE_FROM}'
          AND c.is_test = false AND a.aff_id != ''
        GROUP BY a.aff_id, dt
    """, "Registros")

    # 2) Deposits
    dep_data = query_metric(f"""
        WITH aff_ids AS ({AFF_IDS_CTE})
        SELECT a.aff_id,
               toDate(toTimezone(d.create_time, '{TZ}')) as dt,
               sum(d.amount)/100.0 as total
        FROM {CH_DB}.deposits d
        JOIN aff_ids a ON d.client_id = a.client_id
        WHERE d.status = 'PAID'
          AND toDate(toTimezone(d.create_time, '{TZ}')) >= '{DATE_FROM}'
          AND a.aff_id != ''
        GROUP BY a.aff_id, dt
    """, "Depósitos")

    # 3) FTDs
    ftd_data = query_metric(f"""
        WITH fd AS (
            SELECT client_id, min(toDate(toTimezone(create_time, '{TZ}'))) as first_dt
            FROM {CH_DB}.deposits WHERE status='PAID' GROUP BY client_id
        ),
        aff_ids AS ({AFF_IDS_CTE})
        SELECT a.aff_id,
               fd.first_dt as dt,
               count() as cnt
        FROM fd
        JOIN aff_ids a ON fd.client_id = a.client_id
        WHERE fd.first_dt >= '{DATE_FROM}' AND a.aff_id != ''
        GROUP BY a.aff_id, dt
    """, "FTDs")

    # 4) Withdrawals
    wd_data = query_metric(f"""
        WITH aff_ids AS ({AFF_IDS_CTE})
        SELECT a.aff_id,
               toDate(toTimezone(w.create_time, '{TZ}')) as dt,
               sum(w.amount)/100.0 as total
        FROM {CH_DB}.withdrawals w
        JOIN aff_ids a ON w.client_id = a.client_id
        WHERE w.status = 'PAID'
          AND toDate(toTimezone(w.create_time, '{TZ}')) >= '{DATE_FROM}'
          AND a.aff_id != ''
        GROUP BY a.aff_id, dt
    """, "Saques")

    # 5) Casino (turnover + GGR as tuple)
    casino_data = query_metric(f"""
        WITH aff_ids AS ({AFF_IDS_CTE})
        SELECT a.aff_id,
               toDate(toTimezone(cs.spin_date, '{TZ}')) as dt,
               sum(CASE WHEN cs.status='BET' THEN cs.amount ELSE 0 END)/100.0 as vol,
               (sum(CASE WHEN cs.status='BET' THEN cs.amount ELSE 0 END) -
                sum(CASE WHEN cs.status='WIN' THEN cs.amount ELSE 0 END))/100.0 as ggr
        FROM {CH_DB}.casino cs
        JOIN aff_ids a ON cs.client_id = a.client_id
        WHERE toDate(toTimezone(cs.spin_date, '{TZ}')) >= '{DATE_FROM}'
          AND cs.is_free_spin = false AND a.aff_id != ''
        GROUP BY a.aff_id, dt
    """, "Casino")

    # Merge all metrics by (aff_id, date) — no duplication possible
    all_keys = set()
    all_keys.update(reg_data.keys())
    all_keys.update(dep_data.keys())
    all_keys.update(ftd_data.keys())
    all_keys.update(wd_data.keys())
    all_keys.update(casino_data.keys())

    for (aid, dt_str) in all_keys:
        name = get_aff_name(aid)
        rg = reg_data.get((aid, dt_str), 0)
        da = dep_data.get((aid, dt_str), 0)
        ftd = ftd_data.get((aid, dt_str), 0)
        wa = wd_data.get((aid, dt_str), 0)
        casino = casino_data.get((aid, dt_str), (0, 0))
        vol, ggr = (casino if isinstance(casino, tuple) else (casino, 0))

        if name not in phoenix_aff_days:
            phoenix_aff_days[name] = []
        phoenix_aff_days[name].append({
            'd': dt_str,
            'ftd': int(ftd),
            'da': round(float(da), 2),
            'rg': int(rg),
            'wa': round(float(wa), 2),
            'np': round(float(ggr), 2),
            'vol': round(float(vol), 2)
        })

    # Sort each affiliate's entries by date
    for name in phoenix_aff_days:
        phoenix_aff_days[name].sort(key=lambda e: e['d'])

    print(f"   → {len(phoenix_aff_days)} afiliados com dados diários do Phoenix (BRT)")
    # Show sample for today
    today_ftd = sum(
        sum(e['ftd'] for e in entries if e['d'] == TODAY)
        for entries in phoenix_aff_days.values()
    )
    today_da = sum(
        sum(e['da'] for e in entries if e['d'] == TODAY)
        for entries in phoenix_aff_days.values()
    )
    print(f"   → Hoje: {today_ftd} FTDs, R${today_da:,.0f} depósitos (per-afiliado Phoenix BRT)")

except Exception as e:
    print(f"   ⚠️ Erro dados per-afiliado Phoenix: {e}")
    import traceback; traceback.print_exc()

# ── Query 6: AFF_M — Affiliate rankings per period ──
print("\n6️⃣  Gerando AFF_M (rankings de afiliados por período)...")

# Load existing AFF_M to preserve historical months
existing_aff_m = {}
aff_m_match = re.search(r'const AFF_M=(\{.+?\});', html)
if aff_m_match:
    try:
        existing_aff_m = json.loads(aff_m_match.group(1))
        print(f"   → AFF_M existente preservado: {len(existing_aff_m)} chaves")
    except:
        pass

aff_m = dict(existing_aff_m)

# Use Phoenix affDays data for AFF_M generation
try:
    aff_source = phoenix_aff_days if phoenix_aff_days else {}

    if aff_source:
        # Monthly aggregation
        month_agg = defaultdict(lambda: defaultdict(lambda: {'vol': 0, 'np': 0}))
        for name, entries in aff_source.items():
            for e in entries:
                m = e['d'][:7]
                month_agg[m][name]['vol'] += e.get('vol', 0)
                month_agg[m][name]['np'] += e.get('np', 0)
        for m, affiliates in month_agg.items():
            aff_list = [{'n': n, 'vol': round(v['vol'], 2), 'np': round(v['np'], 2)} for n, v in affiliates.items()]
            aff_m[m] = {
                'v': [{'n': a['n'], 'v': a['vol']} for a in sorted(aff_list, key=lambda x: -x['vol'])[:10]],
                'b': [{'n': a['n'], 'v': a['np']} for a in sorted(aff_list, key=lambda x: -x['np'])[:10]],
                'w': [{'n': a['n'], 'v': a['np']} for a in sorted(aff_list, key=lambda x: x['np'])[:10]]
            }
        print(f"   → AFF_M mensal: {len(month_agg)} meses")

        # Daily aggregation (today + yesterday)
        keep_days = {TODAY, YESTERDAY}
        day_agg = defaultdict(lambda: defaultdict(lambda: {'vol': 0, 'np': 0}))
        for name, entries in aff_source.items():
            for e in entries:
                d = e['d']
                if d in keep_days:
                    day_agg[d][name]['vol'] += e.get('vol', 0)
                    day_agg[d][name]['np'] += e.get('np', 0)
        for d, affiliates in day_agg.items():
            aff_list = [{'n': n, 'vol': round(v['vol'], 2), 'np': round(v['np'], 2)} for n, v in affiliates.items()]
            aff_m[d] = {
                'v': [{'n': a['n'], 'v': a['vol']} for a in sorted(aff_list, key=lambda x: -x['vol'])[:10]],
                'b': [{'n': a['n'], 'v': a['np']} for a in sorted(aff_list, key=lambda x: -x['np'])[:10]],
                'w': [{'n': a['n'], 'v': a['np']} for a in sorted(aff_list, key=lambda x: x['np'])[:10]]
            }
        print(f"   → AFF_M diário: {len(day_agg)} dias")

        # Weekly AFF_M
        week_aff = defaultdict(lambda: {'vol': 0, 'np': 0})
        for name, entries in aff_source.items():
            for e in entries:
                if e['d'] >= WEEK_START and e['d'] <= TODAY:
                    week_aff[name]['vol'] += e.get('vol', 0)
                    week_aff[name]['np'] += e.get('np', 0)
        if week_aff:
            waff_list = [{'n': n, 'vol': round(v['vol'], 2), 'np': round(v['np'], 2)} for n, v in week_aff.items()]
            aff_m['W'] = {
                'v': [{'n': a['n'], 'v': a['vol']} for a in sorted(waff_list, key=lambda x: -x['vol'])[:10]],
                'b': [{'n': a['n'], 'v': a['np']} for a in sorted(waff_list, key=lambda x: -x['np'])[:10]],
                'w': [{'n': a['n'], 'v': a['np']} for a in sorted(waff_list, key=lambda x: x['np'])[:10]]
            }
    print(f"   → AFF_M total: {len(aff_m)} chaves")

except Exception as e:
    print(f"   ⚠️ Erro AFF_M: {e}. Preservando dados atuais.")
    if not aff_m:
        aff_m = None

# ── Update HOURLY ──
print("\n7️⃣  Atualizando HOURLY...")
hourly_obj = {
    'date': TODAY,
    'regs': hr_regs,
    'ftd': hr_ftd,
    'vol': [round(v, 2) for v in hr_vol]
}
print(f"   → {sum(hr_regs)} regs, {sum(hr_ftd)} FTD, R${sum(hr_vol):,.0f} vol")

# ── Write changes to HTML ──
print("\n8️⃣  Escrevendo alterações no HTML...")

# Replace D[]
d_json = json.dumps(D, separators=(',', ':'))
html = re.sub(r'const D=\[.*?\];', f'const D={d_json};', html, count=1)

# Replace RECON[]
recon_json = json.dumps(recon_list, separators=(',', ':'))
html = re.sub(r'(const|var) RECON=\[.*\];', f'const RECON={recon_json};', html, count=1)

# Replace HOURLY
hourly_json = json.dumps(hourly_obj, separators=(',', ':'))
html = re.sub(r'(const|let) HOURLY=\{.*?\};', f'let HOURLY={hourly_json};', html, count=1)

# Update AFF.affDays with Phoenix-sourced data (BRT timezone)
# NOTE: AFF.days (tracked totals for orgânico/tracked split) is kept from Smartico
# because Smartico properly distinguishes tracked vs organic registrations.
# Only AFF.affDays (affiliate rankings) uses Phoenix data for BRT consistency.
if phoenix_aff_days:
    try:
        aff_match = re.search(r'(?:const |var |let )?AFF=(\{.+\});', html)
        if aff_match:
            aff_obj = json.loads(aff_match.group(1))
            aff_obj['affDays'] = phoenix_aff_days
            aff_obj['syncAt'] = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
            # Preserve AFF.days from Smartico (orgânico/tracked split)
            aff_json = json.dumps(aff_obj, separators=(',', ':'), ensure_ascii=False)
            html = re.sub(r'AFF=\{.+\};', f'AFF={aff_json};', html, count=1)
            print(f"   → AFF.affDays atualizado: {len(phoenix_aff_days)} afiliados (fonte: Phoenix BRT)")
            print(f"   → AFF.days preservado do Smartico (split orgânico/tracked)")
    except Exception as e:
        print(f"   ⚠️ Erro ao atualizar AFF: {e}")

# Replace CLI_M (player rankings per period)
if cli_m is not None:
    cli_m_json = json.dumps(cli_m, separators=(',', ':'), ensure_ascii=False)
    html = re.sub(r'const CLI_M=\{.+?\};', f'const CLI_M={cli_m_json};', html, count=1)
    print(f"   → CLI_M atualizado: {len(cli_m)} chaves, {len(cli_m_json)//1024}KB")
else:
    print("   → CLI_M preservado (erro na query)")

# Replace AFF_M (affiliate rankings per period)
if aff_m is not None:
    aff_m_json = json.dumps(aff_m, separators=(',', ':'), ensure_ascii=False)
    html = re.sub(r'const AFF_M=\{.+?\};', f'const AFF_M={aff_m_json};', html, count=1)
    print(f"   → AFF_M atualizado: {len(aff_m)} chaves, {len(aff_m_json)//1024}KB")
else:
    print("   → AFF_M preservado (erro na geração)")

with open(INDEX, 'w', encoding='utf-8') as f:
    f.write(html)

print("✅ index.html atualizado!")

# ── Deploy ──
if not args.no_deploy:
    print("\n9️⃣  Deploy Vercel...")
    os.chdir(REPO_DIR)
    result = subprocess.run(
        ['npx', 'vercel', '--prod', '--yes'],
        capture_output=True, text=True, timeout=120
    )
    if 'Aliased' in result.stdout or 'Production' in result.stdout:
        print("✅ Deploy concluído!")
        # Extract URL
        for line in result.stdout.split('\n'):
            if 'jogogrande-dashboard.vercel.app' in line:
                print(f"   → {line.strip()}")
    else:
        print(f"⚠️  Deploy output: {result.stdout[-200:]}")
        if result.stderr:
            print(f"   stderr: {result.stderr[-200:]}")
else:
    print("\n⏭️  Deploy pulado (--no-deploy)")

# ── Summary ──
print(f"""
{'='*60}
  ✅ SYNC CONCLUÍDO — Phoenix DB → Dashboard
{'='*60}
  Período:    {DATE_FROM} → {DATE_TO}
  Dias:       {updated} atualizados, {created} criados
  RECON:      {len(recon_list)} dias
  HOURLY:     {sum(hr_regs)} regs, {sum(hr_ftd)} FTD
  Fonte:      Phoenix DB (ClickHouse) — timezone BRT
{'='*60}
""")
