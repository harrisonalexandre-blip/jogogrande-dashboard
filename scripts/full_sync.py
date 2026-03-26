#!/usr/bin/env python3
"""Full Smartico sync - all fields with correct format."""
import re, json, requests, sys, csv
from datetime import datetime, timedelta, date
from collections import defaultdict

API = "https://boapi3.smartico.ai"
KEY = "ed91f910-2897-11f1-8250-027e66b7665d-12447"
HDR = {"authorization": KEY}
TMR = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d')
D7 = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
W5 = (datetime.now() - timedelta(weeks=5)).strftime('%Y-%m-%d')

FS = ['visit_count','registration_count','ftd_count','deposit_count','deposit_total',
      'withdrawal_total','net_pl','commissions_total','volume','operations',
      'net_pl_casino','net_pl_sport','commissions_cpa','commissions_rev_share',
      'bonus_amount','chargback_total','ftd_total','balance','net_deposits']

def api(p):
    r = requests.get(f"{API}/api/af2_media_report_op", headers=HDR, params=p, timeout=30)
    r.raise_for_status()
    d = r.json()
    return d.get('data', d) if isinstance(d, dict) else d

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

def short(r):
    return {'rg':int(r.get('registration_count',0)),'ftd':int(r.get('ftd_count',0)),
        'dc':int(r.get('deposit_count',0)),'da':r.get('deposit_total',0),
        'wa':r.get('withdrawal_total',0),'np':r.get('net_pl',0),
        'cm':r.get('commissions_total',0),'vol':r.get('volume',0),
        'ops':int(r.get('operations',0)),'vc':int(r.get('visit_count',0))}

def agg(data, kfn):
    g = defaultdict(lambda: defaultdict(float))
    for row in data:
        k = kfn(row)
        if not k: continue
        for f in FS: g[k][f] += row.get(f,0) or 0
    return g

def clean_name(n):
    return n.replace('DEFAULT_AFFILIATE:','') if n.startswith('DEFAULT_AFFILIATE:') else n

# ---- MAIN ----
print("=== FULL SMARTICO SYNC ===")

# 1. AFFS
print("1. affs...")
raw = api({'group_by':'affiliate_id'})
nm = {}
ag = defaultdict(lambda: defaultdict(float))
for r in raw:
    aid = str(r.get('affiliate_id',''))
    n = clean_name(r.get('affiliate_name','') or aid)
    nm[aid] = n
    for f in FS: ag[n][f] += r.get(f,0) or 0
affs = []
for n in sorted(ag.keys()):
    e = full(ag[n]); e['n'] = n; affs.append(e)
print(f"   {len(affs)} affiliates")

# 2. DAYS
print("2. days...")
raw = api({'aggregation_period':'DAY','date_from':'2025-11-20','date_to':TMR})
da = agg(raw, lambda r: str(r.get('dt',''))[:10])
days = []
for d in sorted(da.keys()):
    e = short(da[d]); e['d'] = d; days.append(e)
print(f"   {len(days)} days ({days[0]['d']}→{days[-1]['d']})")

# 3. MONTHS
print("3. months...")
raw = api({'aggregation_period':'MONTH','date_from':'2025-11-01','date_to':TMR})
ma = agg(raw, lambda r: str(r.get('dt',''))[:7])
months = []
for m in sorted(ma.keys()):
    e = full(ma[m]); e['m'] = m; months.append(e)
print(f"   {len(months)} months")

# 4. AFFMONTHS
print("4. affMonths...")
raw = api({'aggregation_period':'MONTH','group_by':'affiliate_id','date_from':'2025-11-01','date_to':TMR})
tmp = defaultdict(lambda: defaultdict(lambda: defaultdict(float)))
for r in raw:
    n = clean_name(nm.get(str(r.get('affiliate_id','')), r.get('affiliate_name','')))
    m = str(r.get('dt',''))[:7]
    if n and m:
        for f in FS: tmp[n][m][f] += r.get(f,0) or 0
affMonths = {}
for n in tmp:
    ml = []
    for m in sorted(tmp[n].keys()):
        e = full(tmp[n][m]); e['m'] = m; ml.append(e)
    affMonths[n] = ml
print(f"   {len(affMonths)} affiliates")

# 5. AFFDAYS
print("5. affDays (7d)...")
raw = api({'aggregation_period':'DAY','group_by':'affiliate_id','date_from':D7,'date_to':TMR})
tmp2 = defaultdict(lambda: defaultdict(lambda: defaultdict(float)))
for r in raw:
    n = clean_name(nm.get(str(r.get('affiliate_id','')), r.get('affiliate_name','')))
    d = str(r.get('dt',''))[:10]
    if n and d:
        for f in FS: tmp2[n][d][f] += r.get(f,0) or 0
affDays = {}
for n in tmp2:
    dl = []
    for d in sorted(tmp2[n].keys()):
        e = short(tmp2[n][d]); e['d'] = d; dl.append(e)
    affDays[n] = dl
print(f"   {len(affDays)} affiliates")

# 6. WEEKS
print("6. weeks...")
raw = api({'aggregation_period':'DAY','date_from':W5,'date_to':TMR})
wa = defaultdict(lambda: defaultdict(float))
for r in raw:
    d = str(r.get('dt',''))[:10]
    if d:
        dt = date.fromisoformat(d)
        wk = f"{dt.year}-W{dt.isocalendar()[1]:02d}"
        for f in FS: wa[wk][f] += r.get(f,0) or 0
weeks = []
for w in sorted(wa.keys()):
    e = full(wa[w]); e['w'] = w; weeks.append(e)
weeks = weeks[-5:]
print(f"   {len(weeks)} weeks")

# === DTP UPDATE ===
print("\n7. DTP CSV update...")
DTP_PATH = '/Users/harrison/Documents/Jogo Grande/JOGO GRANDE/processados/ELEVEX GROUP LTD_LATEST.csv'
try:
    with open(DTP_PATH, 'r', encoding='utf-8-sig') as f:
        lines = f.readlines()
    start = 1  # skip sep= line
    reader = csv.DictReader(lines[start:], delimiter=';')
    rows = list(reader)
    
    # Group by date
    from collections import Counter
    dtp_by_date = defaultdict(lambda: {'dep':0,'saq':0,'dep_n':0,'saq_n':0,'mi':0,'mi_n':0})
    for r in rows:
        raw_date = r.get('Data','').strip()[:10]
        if not raw_date: continue
        # Convert DD/MM/YYYY to YYYY-MM-DD
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
            dtp_by_date[d]['dep'] += val
            dtp_by_date[d]['dep_n'] += 1
        elif tipo == 'Saque':
            dtp_by_date[d]['saq'] += val
            dtp_by_date[d]['saq_n'] += 1
            if app == 'WEB':
                dtp_by_date[d]['mi'] += val
                dtp_by_date[d]['mi_n'] += 1
    
    print(f"   {len(dtp_by_date)} dates in DTP CSV")
    
    # Read current RECON from index.html
    with open('/tmp/jogogrande-dashboard/index.html', 'r') as f:
        html = f.read()
    
    recon_match = re.search(r'RECON=(\[.*?\]);\s*\n', html, re.DOTALL)
    if not recon_match:
        recon_match = re.search(r'RECON=(\[.*?\]);', html)
    old_recon = json.loads(recon_match.group(1)) if recon_match else []
    
    # Update RECON entries
    recon_map = {r['d']: r for r in old_recon}
    for d, v in dtp_by_date.items():
        recon_map[d] = {
            'd': d,
            'dtp_dep': round(v['dep'],2),
            'dtp_saq': round(v['saq'],2),
            'dtp_dep_n': v['dep_n'],
            'dtp_saq_n': v['saq_n'],
            'dtp_mi': round(v['mi'],2),
            'dtp_mi_n': v['mi_n']
        }
    
    new_recon = sorted(recon_map.values(), key=lambda x: x['d'])
    print(f"   RECON: {len(new_recon)} entries, last={new_recon[-1]['d']}")
    today_r = recon_map.get(datetime.now().strftime('%Y-%m-%d'), {})
    print(f"   Hoje DTP: dep=R${today_r.get('dtp_dep',0):,.2f}, saq=R${today_r.get('dtp_saq',0):,.2f}")
    
except Exception as e:
    print(f"   DTP error: {e}")
    new_recon = None

# === BUILD & WRITE ===
AFF = {'affs':affs,'months':months,'weeks':weeks,'days':days,
       'affMonths':affMonths,'affDays':affDays,
       'syncAt':datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')}

# Validate
err = []
if len(affs)<50: err.append(f"affs={len(affs)}")
if not affs[0].get('n'): err.append("no names")
if len(days)<100: err.append(f"days={len(days)}")
if len(affDays)<20: err.append(f"affDays={len(affDays)}")
if err:
    print(f"\n❌ ABORT: {err}")
    sys.exit(1)

with open('/tmp/jogogrande-dashboard/index.html','r') as f:
    html = f.read()

aj = json.dumps(AFF, separators=(',',':'))
html = re.sub(r'AFF=\{.*?\};', f'AFF={aj};', html, count=1, flags=re.DOTALL)

if new_recon:
    rj = json.dumps(new_recon, separators=(',',':'))
    html = re.sub(r'RECON=\[.*?\];', f'RECON={rj};', html, count=1, flags=re.DOTALL)

with open('/tmp/jogogrande-dashboard/index.html','w') as f:
    f.write(html)

td = days[-1]
print(f"\n✅ DONE")
print(f"   Smartico hoje ({td['d']}): rg={td['rg']}, ftd={td['ftd']}, dep=R${td['da']:,.2f}")
print(f"   AFF: {len(affs)} affs, {len(days)} days, {len(affDays)} affDays")
print(f"   RECON: {len(new_recon) if new_recon else 'unchanged'}")
