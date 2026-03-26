#!/usr/bin/env python3
"""
Smartico Affiliate Sync — Safe update script for Jogo Grande Dashboard.

This script:
1. Calls Smartico API for aggregate data (days, months)
2. Preserves affiliate-level data (affs, affDays, affMonths) with names
3. Validates EVERYTHING before writing
4. NEVER produces broken AFF data

Usage: python3 scripts/smartico_sync.py
"""
import re, json, sys, os
from datetime import datetime, timedelta
from collections import defaultdict

try:
    import requests
except ImportError:
    print("ERROR: requests not installed. Run: pip3 install requests")
    sys.exit(1)

# ============================================================
# CONFIG
# ============================================================
REPO_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INDEX_HTML = os.path.join(REPO_DIR, 'index.html')
API_HOST = "https://boapi3.smartico.ai"
API_KEY = "ed91f910-2897-11f1-8250-027e66b7665d-12447"
HEADERS = {"authorization": API_KEY}

NUMERIC_FIELDS = [
    'visit_count', 'registration_count', 'ftd_count', 'deposit_count',
    'deposit_total', 'withdrawal_total', 'net_pl', 'commissions_total',
    'volume', 'operations', 'net_pl_casino', 'net_pl_sport',
    'commissions_cpa', 'commissions_rev_share', 'bonus_amount',
    'chargback_total', 'ftd_total', 'balance', 'net_deposits'
]

# ============================================================
# HELPERS
# ============================================================
def api_get(params):
    """Call Smartico API, return data array."""
    resp = requests.get(f"{API_HOST}/api/af2_media_report_op",
                       headers=HEADERS, params=params, timeout=30)
    resp.raise_for_status()
    body = resp.json()
    return body.get('data', body) if isinstance(body, dict) else body

def aggregate_by(data, key_fn):
    """Aggregate numeric fields by a key function."""
    groups = defaultdict(lambda: defaultdict(float))
    for row in data:
        k = key_fn(row)
        if not k:
            continue
        for f in NUMERIC_FIELDS:
            groups[k][f] += row.get(f, 0) or 0
    return groups

def load_current_aff():
    """Load current AFF from index.html."""
    with open(INDEX_HTML, 'r') as f:
        html = f.read()
    match = re.search(r'AFF=(\{.*?\});\s*\n', html, re.DOTALL)
    if not match:
        match = re.search(r'AFF=(\{.*?\});', html)
    if not match:
        print("ERROR: Could not find AFF in index.html")
        sys.exit(1)
    return json.loads(match.group(1)), html

def validate_aff(aff):
    """Strict validation. Returns list of errors (empty = OK)."""
    errors = []
    if not isinstance(aff.get('affs'), list):
        errors.append("affs is not an array")
    elif len(aff['affs']) < 50:
        errors.append(f"affs too few: {len(aff['affs'])}")
    elif not aff['affs'][0].get('n'):
        errors.append("affs[0] missing 'n' (name field)")

    if not isinstance(aff.get('days'), list):
        errors.append("days is not an array")
    elif len(aff['days']) < 100:
        errors.append(f"days too few: {len(aff['days'])}")
    elif not aff['days'][0].get('d'):
        errors.append("days[0] missing 'd' (date field)")

    if not isinstance(aff.get('affDays'), dict):
        errors.append("affDays is not a dict")
    elif len(aff['affDays']) < 20:
        errors.append(f"affDays too few affiliates: {len(aff['affDays'])}")

    if not isinstance(aff.get('affMonths'), dict):
        errors.append("affMonths is not a dict")

    if not isinstance(aff.get('months'), list):
        errors.append("months is not an array")

    return errors

# ============================================================
# MAIN SYNC
# ============================================================
def main():
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Smartico Sync starting...")

    # Load current AFF
    current_aff, html = load_current_aff()
    print(f"  Current: {len(current_aff.get('affs',[]))} affs, {len(current_aff.get('days',[]))} days")

    # Validate current AFF has good structure (affs with names)
    current_ok = not validate_aff(current_aff)
    if not current_ok:
        print(f"  WARNING: Current AFF has issues, will try to fix from API")

    tomorrow = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d')

    # ----- FETCH DAILY DATA -----
    print("  Fetching daily data...")
    try:
        daily_raw = api_get({'aggregation_period': 'DAY', 'date_from': '2025-11-20', 'date_to': tomorrow})
        daily_agg = aggregate_by(daily_raw, lambda r: r.get('dt', '')[:10])
        new_days = []
        for d in sorted(daily_agg.keys()):
            r = daily_agg[d]
            new_days.append({
                'rg': int(r['registration_count']), 'ftd': int(r['ftd_count']),
                'dc': int(r['deposit_count']), 'da': r['deposit_total'],
                'wa': r['withdrawal_total'], 'np': r['net_pl'],
                'cm': r['commissions_total'], 'vol': r['volume'],
                'ops': int(r['operations']), 'vc': int(r['visit_count']), 'd': d
            })
        print(f"    {len(new_days)} days ({new_days[0]['d']} → {new_days[-1]['d']})")
    except Exception as e:
        print(f"    ERROR fetching days: {e}. Keeping current.")
        new_days = current_aff.get('days', [])

    # ----- FETCH MONTHLY DATA -----
    print("  Fetching monthly data...")
    try:
        monthly_raw = api_get({'aggregation_period': 'MONTH', 'date_from': '2025-11-01', 'date_to': tomorrow})
        monthly_agg = aggregate_by(monthly_raw, lambda r: r.get('dt', '')[:7])
        new_months = []
        for m in sorted(monthly_agg.keys()):
            r = monthly_agg[m]
            new_months.append({
                'rg': int(r['registration_count']), 'ftd': int(r['ftd_count']),
                'dc': int(r['deposit_count']), 'da': r['deposit_total'],
                'wa': r['withdrawal_total'], 'np': r['net_pl'],
                'npc': r['net_pl_casino'], 'nps': r['net_pl_sport'],
                'cm': r['commissions_total'], 'cpa': r['commissions_cpa'],
                'rs': r['commissions_rev_share'], 'bn': r['bonus_amount'],
                'vol': r['volume'], 'ops': int(r['operations']),
                'cb': r['chargback_total'], 'fda': r['ftd_total'],
                'bal': r['balance'], 'vc': int(r['visit_count']),
                'nd': r['net_deposits'], 'fr': 0, 'm': m
            })
        print(f"    {len(new_months)} months")
    except Exception as e:
        print(f"    ERROR fetching months: {e}. Keeping current.")
        new_months = current_aff.get('months', [])

    # ----- BUILD FINAL AFF -----
    # CRITICAL: affs, affDays, affMonths are PRESERVED (they have affiliate names)
    # Only days, months, weeks are updated from API (aggregate data, no names needed)
    final_aff = {
        'affs': current_aff.get('affs', []),         # PRESERVE (has names)
        'months': new_months,                          # UPDATE
        'weeks': current_aff.get('weeks', []),         # PRESERVE (complex)
        'days': new_days if len(new_days) >= 100 else current_aff.get('days', []),  # UPDATE with guard
        'affMonths': current_aff.get('affMonths', {}), # PRESERVE (keyed by name)
        'affDays': current_aff.get('affDays', {}),     # PRESERVE (keyed by name)
        'syncAt': datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
    }

    # ----- VALIDATE -----
    errors = validate_aff(final_aff)
    if errors:
        print(f"\n  ❌ VALIDATION FAILED: {errors}")
        print("  ABORTING — index.html NOT modified")
        sys.exit(1)

    # ----- WRITE -----
    aff_json = json.dumps(final_aff, separators=(',', ':'))
    html_new = re.sub(r'AFF=\{.*?\};', f'AFF={aff_json};', html, count=1, flags=re.DOTALL)

    with open(INDEX_HTML, 'w') as f:
        f.write(html_new)

    print(f"\n  ✅ SUCCESS: {len(aff_json)//1024}KB AFF written")
    print(f"     affs={len(final_aff['affs'])}, days={len(final_aff['days'])}, months={len(final_aff['months'])}")
    print(f"     affDays={len(final_aff['affDays'])}, affMonths={len(final_aff['affMonths'])}")

if __name__ == '__main__':
    main()
