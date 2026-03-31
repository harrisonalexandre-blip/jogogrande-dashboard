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
    )
    SELECT toHour(toTimezone(d.create_time, '{TZ}')) as h, count(DISTINCT d.client_id)
    FROM {CH_DB}.deposits d
    JOIN fd ON d.client_id=fd.client_id AND fd.first_dt='{TODAY}'
    WHERE toDate(toTimezone(d.create_time, '{TZ}'))='{TODAY}' AND d.status='PAID'
    GROUP BY h ORDER BY h
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
    mi AS (
        SELECT
            toDate(toTimezone(create_time, '{TZ}')) as dt,
            count() as mi_n,
            sum(amount)/100.0 as mi_total
        FROM {CH_DB}.payment
        WHERE toDate(toTimezone(create_time, '{TZ}')) >= '2025-11-01'
          AND type = 'WITHDRAWAL' AND status IN ('CANCELLED','CANCELLED_BY_CLIENT')
        GROUP BY dt
    )
    SELECT
        d.dt,
        d.dep_n, d.dep_total,
        COALESCE(w.wd_n, 0), COALESCE(w.wd_total, 0),
        COALESCE(m.mi_n, 0), COALESCE(m.mi_total, 0)
    FROM deps d
    LEFT JOIN wds w ON d.dt = w.dt
    LEFT JOIN mi m ON d.dt = m.dt
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
d_match = re.search(r'const D=(\[.*?\]);', html, re.DOTALL)
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
        # Preserve any fields not in our update
        for k, v in entry.items():
            D[idx][k] = v
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
html = re.sub(r'const D=\[.*?\];', f'const D={d_json};', html, count=1, flags=re.DOTALL)

# Replace RECON[]
recon_json = json.dumps(recon_list, separators=(',', ':'))
html = re.sub(r'var RECON=\[.*?\];', f'var RECON={recon_json};', html, count=1, flags=re.DOTALL)

# Replace HOURLY
hourly_json = json.dumps(hourly_obj, separators=(',', ':'))
html = re.sub(r'const HOURLY=\{.*?\};', f'const HOURLY={hourly_json};', html, count=1)

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
