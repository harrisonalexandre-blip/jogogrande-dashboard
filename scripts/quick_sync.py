#!/usr/bin/env python3
"""Quick incremental Smartico sync - only today + yesterday."""
import re, json, requests, sys
from datetime import datetime, timedelta
from collections import defaultdict

API = "https://boapi3.smartico.ai"
KEY = "ed91f910-2897-11f1-8250-027e66b7665d-12447"
HDR = {"authorization": KEY}
TODAY = datetime.now().strftime('%Y-%m-%d')
YESTERDAY = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
TMR = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d')

FS = ['visit_count','registration_count','ftd_count','deposit_count','deposit_total',
      'withdrawal_total','net_pl','commissions_total','volume','operations',
      'net_pl_casino','net_pl_sport','commissions_cpa','commissions_rev_share',
      'bonus_amount','chargback_total','ftd_total','balance','net_deposits']

def api(p):
    r = requests.get(f"{API}/api/af2_media_report_op", headers=HDR, params=p, timeout=30)
    r.raise_for_status()
    d = r.json()
    return d.get('data', d) if isinstance(d, dict) else d

def short(r):
    return {'rg':int(r.get('registration_count',0)),'ftd':int(r.get('ftd_count',0)),
        'dc':int(r.get('deposit_count',0)),'da':r.get('deposit_total',0),
        'wa':r.get('withdrawal_total',0),'np':r.get('net_pl',0),
        'cm':r.get('commissions_total',0),'vol':r.get('volume',0),
        'ops':int(r.get('operations',0)),'vc':int(r.get('visit_count',0))}

def full(r):
    return {'rg':int(r.get('registration_count',0)),'ftd':int(r.get('ftd_count',0)),
        'dc':int(r.get('deposit_count',0)),'da':r.get('deposit_total',0),
        'wa':r.get('withdrawal_total',0),'np':r.get('net_pl',0),
        'npc':r.get('net_pl_casino',0),'nps':r.get('net_pl_sport',0),
        'cm':r.get('commissions_total',0),'cpa':r.get('commissions_cpa',0),
        'rs':r.get('commissions_rev_share',0),'bn':r.get('bonus_amount',0),
        'vol':r.get('volume',0),'ops':int(r.get('operations',0)),
        'cb':r.get('chargback_total',0),'fda':r.get('ftd_total',0),
        'bal':r.get('balance',0),'vc':int(r.get('visit_count',0)),
        'nd':r.get('net_deposits',0),'fr':0}

def clean_name(n):
    return n.replace('DEFAULT_AFFILIATE:','') if n.startswith('DEFAULT_AFFILIATE:') else n

def agg(data, kfn):
    g = defaultdict(lambda: defaultdict(float))
    for row in data:
        k = kfn(row)
        if not k: continue
        for f in FS: g[k][f] += row.get(f,0) or 0
    return g

# ---- MAIN ----
print(f"=== QUICK SYNC (incremental: {YESTERDAY} → {TODAY}) ===")

# Read existing AFF from index.html
with open('/tmp/jogogrande-dashboard/index.html','r') as f:
    html = f.read()

aff_match = re.search(r'AFF=(\{.*?\});\s*\n', html, re.DOTALL)
if not aff_match:
    aff_match = re.search(r'AFF=(\{.*?\});', html, re.DOTALL)
if not aff_match:
    print("❌ Could not find AFF in index.html")
    sys.exit(1)

AFF = json.loads(aff_match.group(1))
print(f"   Existing: {len(AFF.get('days',[]))} days, {len(AFF.get('affs',[]))} affs")

# 1. UPDATE DAYS (only today + yesterday)
print("1. days (today+yesterday)...")
raw = api({'aggregation_period':'DAY','date_from':YESTERDAY,'date_to':TMR})
da = agg(raw, lambda r: str(r.get('dt',''))[:10])
days_map = {d['d']: d for d in AFF.get('days',[])}
for d in sorted(da.keys()):
    e = short(da[d]); e['d'] = d
    days_map[d] = e
AFF['days'] = sorted(days_map.values(), key=lambda x: x['d'])
print(f"   → {len(AFF['days'])} days total")

# 2. UPDATE AFFDAYS (only today + yesterday)
print("2. affDays (today+yesterday)...")
# Get name mapping from existing affs
nm = {a.get('n',''): a.get('n','') for a in AFF.get('affs',[])}
raw = api({'aggregation_period':'DAY','group_by':'affiliate_id','date_from':YESTERDAY,'date_to':TMR})
tmp = defaultdict(lambda: defaultdict(lambda: defaultdict(float)))
for r in raw:
    n = clean_name(r.get('affiliate_name','') or str(r.get('affiliate_id','')))
    d = str(r.get('dt',''))[:10]
    if n and d:
        for f in FS: tmp[n][d][f] += r.get(f,0) or 0

# Merge with existing affDays (keep last 7 days)
cutoff = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
existing_ad = AFF.get('affDays', {})
for n in tmp:
    old = {e['d']: e for e in existing_ad.get(n, []) if e.get('d','') >= cutoff}
    for d in tmp[n]:
        e = short(tmp[n][d]); e['d'] = d
        old[d] = e
    existing_ad[n] = sorted(old.values(), key=lambda x: x['d'])
# Also trim affiliates not in last 7 days
for n in list(existing_ad.keys()):
    existing_ad[n] = [e for e in existing_ad[n] if e.get('d','') >= cutoff]
    if not existing_ad[n]: del existing_ad[n]
AFF['affDays'] = existing_ad
print(f"   → {len(AFF['affDays'])} affiliates with daily data")

# 3. UPDATE CURRENT MONTH in months[]
print("3. current month...")
cur_month = datetime.now().strftime('%Y-%m')
month_start = f"{cur_month}-01"
raw = api({'aggregation_period':'MONTH','date_from':month_start,'date_to':TMR})
ma = agg(raw, lambda r: str(r.get('dt',''))[:7])
months_map = {m['m']: m for m in AFF.get('months',[])}
for m in ma:
    e = full(ma[m]); e['m'] = m
    months_map[m] = e
AFF['months'] = sorted(months_map.values(), key=lambda x: x['m'])
print(f"   → {len(AFF['months'])} months total")

# 4. UPDATE CURRENT WEEK in weeks[]
print("4. current week...")
from datetime import date as dt_date
iso = dt_date.today().isocalendar()
cur_week = f"{iso[0]}-W{iso[1]:02d}"
# Get days of current week
week_start = dt_date.today() - timedelta(days=dt_date.today().weekday())
raw = api({'aggregation_period':'DAY','date_from':week_start.isoformat(),'date_to':TMR})
wa = defaultdict(lambda: defaultdict(float))
for r in raw:
    d = str(r.get('dt',''))[:10]
    if d:
        for f in FS: wa[cur_week][f] += r.get(f,0) or 0
weeks_map = {w['w']: w for w in AFF.get('weeks',[])}
if cur_week in wa:
    e = full(wa[cur_week]); e['w'] = cur_week
    weeks_map[cur_week] = e
AFF['weeks'] = sorted(weeks_map.values(), key=lambda x: x['w'])[-5:]
print(f"   → {len(AFF['weeks'])} weeks")

# 5. DTP CSV (same as full_sync)
print("5. DTP CSV update...")
import csv
DTP_PATH = '/Users/harrison/Documents/Jogo Grande/JOGO GRANDE/processados/ELEVEX GROUP LTD_LATEST.csv'
new_recon = None
try:
    with open(DTP_PATH, 'r', encoding='utf-8-sig') as f:
        lines = f.readlines()
    reader = csv.DictReader(lines[1:], delimiter=';')
    dtp_by_date = defaultdict(lambda: {'dep':0,'saq':0,'dep_n':0,'saq_n':0,'mi':0,'mi_n':0})
    for r in reader:
        raw_date = r.get('Data','').strip()[:10]
        if not raw_date: continue
        parts = raw_date.split('/')
        if len(parts)==3:
            d = f"{parts[2]}-{parts[1]}-{parts[0]}"
        else:
            d = raw_date
        status = r.get('Status','').strip().upper()
        if status != 'REALIZADO': continue
        tipo = r.get('Tipo','').strip()
        val = float(r.get('ValorMovimentado','0').replace(',','.'))
        app = r.get('Aplicacao','').strip().upper()
        if tipo == 'Venda':
            dtp_by_date[d]['dep'] += val; dtp_by_date[d]['dep_n'] += 1
        elif tipo == 'Saque':
            dtp_by_date[d]['saq'] += val; dtp_by_date[d]['saq_n'] += 1
            if app == 'WEB':
                dtp_by_date[d]['mi'] += val; dtp_by_date[d]['mi_n'] += 1

    recon_match = re.search(r'RECON=(\[.*?\]);\s*\n', html, re.DOTALL)
    if not recon_match:
        recon_match = re.search(r'RECON=(\[.*?\]);', html)
    old_recon = json.loads(recon_match.group(1)) if recon_match else []
    recon_map = {r['d']: r for r in old_recon}
    for d, v in dtp_by_date.items():
        recon_map[d] = {'d':d,'dtp_dep':round(v['dep'],2),'dtp_saq':round(v['saq'],2),
            'dtp_dep_n':v['dep_n'],'dtp_saq_n':v['saq_n'],
            'dtp_mi':round(v['mi'],2),'dtp_mi_n':v['mi_n']}
    new_recon = sorted(recon_map.values(), key=lambda x: x['d'])
    print(f"   RECON: {len(new_recon)} entries")
except Exception as e:
    print(f"   DTP skip: {e}")

# === 6. UPDATE D[] TODAY with Smartico data (NR, FTD, Volume) ===
# Smartico provides real-time NR/FTD/Volume for today before Phoenix (D+1)
print("6. D[] today (Smartico → NR, FTD, Volume)...")
try:
    sm_today = api({'aggregation_period':'DAY','date_from':TODAY,'date_to':TMR})
    sm_totals = defaultdict(float)
    for r in sm_today:
        for f in FS: sm_totals[f] += r.get(f,0) or 0

    sm_nr = int(sm_totals['registration_count'])
    sm_ftd = int(sm_totals['ftd_count'])
    sm_vol = round(sm_totals['volume'], 2)

    # Find today's entry in D[] and update nr, ftd, c_to (volume as proxy)
    # Only update if Phoenix hasn't already provided data (cg=0 means no Phoenix yet)
    d_match = re.search(r'\{[^}]*"date":\s*"' + re.escape(TODAY) + r'"[^}]*\}', html)
    if d_match:
        d_str = d_match.group()
        d_obj = json.loads(d_str)

        # Only fill if Phoenix hasn't arrived (cg=0 means no consolidated data yet)
        if abs(d_obj.get('cg', 0)) < 1:
            d_obj['nr'] = sm_nr
            d_obj['ftd'] = sm_ftd
            d_obj['c_to'] = sm_vol  # Smartico volume as casino turnover proxy
            d_obj['fda'] = round(sm_totals.get('ftd_total', 0), 2)  # FTD Amount
            new_d = json.dumps(d_obj, separators=(',',': '))
            html = html.replace(d_str, new_d, 1)
            sm_fda = round(sm_totals.get('ftd_total', 0), 2)
            print(f"   → D[{TODAY}]: nr={sm_nr}, ftd={sm_ftd}, vol=R${sm_vol:,.2f}, fda=R${sm_fda:,.2f}")
        else:
            print(f"   → D[{TODAY}]: Phoenix data present (cg={d_obj.get('cg',0)}), skipping Smartico fill")
    else:
        print(f"   → D[{TODAY}]: entry not found in D[]")
except Exception as e:
    print(f"   D[] today skip: {e}")

# === SAVE ===
AFF['syncAt'] = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')

aj = json.dumps(AFF, separators=(',',':'))
html = re.sub(r'AFF=\{.*?\};', f'AFF={aj};', html, count=1, flags=re.DOTALL)
if new_recon:
    rj = json.dumps(new_recon, separators=(',',':'))
    html = re.sub(r'RECON=\[.*?\];', f'RECON={rj};', html, count=1, flags=re.DOTALL)

with open('/tmp/jogogrande-dashboard/index.html','w') as f:
    f.write(html)

td = AFF['days'][-1] if AFF.get('days') else {}
print(f"\n✅ QUICK SYNC DONE")
print(f"   Hoje ({td.get('d','?')}): rg={td.get('rg',0)}, ftd={td.get('ftd',0)}, dep=R${td.get('da',0):,.2f}")
print(f"   API calls: 5 (vs 6 full) | Period: {YESTERDAY}→{TODAY}")
