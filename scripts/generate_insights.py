#!/usr/bin/env python3
"""
Daily CRM Insights Generator - Phoenix ClickHouse DB
Generates 20 actionable insights for CRM manager
Uses ClickHouse-compatible SQL syntax
"""

import urllib.request, urllib.parse, ssl, json, re, os, sys
from datetime import datetime

# === CONFIG ===
BASE = 'https://analytics.phoenix365-prod.com:443/'
CREDS = 'user=project_176_ro&password=uChiThoocahtiek9&database=project_176'
CTX = ssl.create_default_context()
TODAY = datetime.now().strftime("%Y-%m-%d")
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.dirname(SCRIPT_DIR)

AFF_NAMES = {"509650":"Orgânico","522947":"Afiliado Partner","522944":"Afiliado 2","509755":"Google Ads","509133":"Funil VIP"}

def q(sql):
    """Execute ClickHouse query, return list of dicts"""
    url = BASE + '?' + CREDS + '&query=' + urllib.parse.quote(sql.strip() + ' FORMAT JSON')
    try:
        resp = urllib.request.urlopen(urllib.request.Request(url), timeout=30, context=CTX)
        return json.loads(resp.read().decode())['data']
    except Exception as e:
        print(f"  [WARN] Query failed: {str(e)[:80]}")
        return []

def get_names(ids):
    """Get names for a list of client_ids"""
    if not ids: return {}
    id_str = ','.join(str(i) for i in ids)
    rows = q(f"SELECT client_id, first_name, last_name, btag, registration_device_type, email FROM client WHERE client_id IN ({id_str})")
    result = {}
    for r in rows:
        aff = ''
        m = re.search(r'aff_id=(\d+)', r.get('btag',''))
        if m: aff = m.group(1)
        name = f"{r['first_name']} {r['last_name']}".strip()
        if not name: name = f"Player #{r['client_id']}"
        result[int(r['client_id'])] = {
            'name': name, 'aff_id': aff,
            'aff_name': AFF_NAMES.get(aff, aff),
            'device': r.get('registration_device_type',''),
            'email': r.get('email','')
        }
    return result

def get_game_styles(ids):
    """Get dominant game type for players"""
    if not ids: return {}
    id_str = ','.join(str(i) for i in ids)
    rows = q(f"""
        SELECT client_id, game_type, count() as n
        FROM casino WHERE client_id IN ({id_str}) AND toDate(spin_date) >= today() - 60
        GROUP BY client_id, game_type ORDER BY client_id, n DESC
    """)
    styles = {}
    for r in rows:
        cid = int(r['client_id'])
        if cid not in styles:
            styles[cid] = r['game_type']
    return styles

def fR(v):
    """Format BRL"""
    return f"R${v:,.0f}"

def make_insight(priority, cat, title, desc, players, action, how, impact):
    return {
        "id": f"INS-{TODAY}-{priority:03d}",
        "date": TODAY,
        "priority": priority,
        "category": cat,
        "title": title,
        "description": desc,
        "players": players,
        "action": action,
        "how": how,
        "impact": impact,
        "status": "pending"
    }

def build_player(cid, info, extra=None):
    p = {"id": cid, "name": info.get('name',''), "btag_aff": info.get('aff_id',''),
         "aff_name": info.get('aff_name',''), "device": info.get('device','')}
    if extra: p.update(extra)
    return p

# =========================================
print(f"[INFO] Generating daily insights for {TODAY}")
insights = []

# === 1. VIPs CHURNED (>R$30K dep, 30-90d inativo) ===
print("[1/20] VIPs Churned...")
r = q("""
    SELECT client_id, sum(amount)/100 as total_dep, count() as dep_count,
           max(create_time) as last_dep,
           dateDiff('day', max(create_time), now()) as days_off
    FROM deposits WHERE status = 'PAID'
    GROUP BY client_id
    HAVING total_dep > 30000 AND days_off BETWEEN 30 AND 90
    ORDER BY total_dep DESC LIMIT 10
""")
if r:
    ids = [int(x['client_id']) for x in r]
    names = get_names(ids)
    styles = get_game_styles(ids)
    players = []
    desc_parts = []
    total_val = 0
    for row in r:
        cid = int(row['client_id'])
        info = names.get(cid, {'name': f'#{cid}'})
        dep = float(row['total_dep'])
        total_val += dep
        players.append(build_player(cid, info, {
            'total_dep': dep, 'dep_count': int(row['dep_count']),
            'days_inactive': int(row['days_off']), 'game_style': styles.get(cid,'N/A')
        }))
        desc_parts.append(f"{info.get('name','#'+str(cid))} ({fR(dep)}, {row['days_off']}d, {styles.get(cid,'?')}, via {info.get('aff_name','?')})")
    insights.append(make_insight(1, 'critical',
        f'{len(r)} VIPs Churned — {fR(total_val)} em depósitos totais',
        f"Jogadores de alto valor inativos há 30-90 dias: {'; '.join(desc_parts[:5])}",
        players,
        'Campanha de win-back personalizada via WhatsApp/ligação direta',
        '1) Abrir perfil de cada player no Phoenix BO. 2) Verificar último jogo e último saque. 3) Enviar WhatsApp pessoal com oferta exclusiva (bônus 100% até R$500). 4) Se não responder em 48h, ligar. 5) Registrar resultado.',
        f'Reativação potencial de {fR(total_val * 0.1)} (se 10% voltar)'
    ))

# === 2. NET PAYOUT ALERT ===
print("[2/20] Net Payout Alert...")
r = q("""
    SELECT wd.client_id, wd.total_wd, dt.total_dep, wd.total_wd - dt.total_dep as net
    FROM (SELECT client_id, sum(amount)/100 as total_wd FROM withdrawals WHERE status='PAID' AND toDate(create_time) >= today() - 30 GROUP BY client_id) wd
    JOIN (SELECT client_id, sum(amount)/100 as total_dep FROM deposits WHERE status='PAID' GROUP BY client_id) dt USING client_id
    WHERE wd.total_wd > dt.total_dep
    ORDER BY net DESC LIMIT 8
""")
if r:
    ids = [int(x['client_id']) for x in r]
    names = get_names(ids)
    players = []
    for row in r:
        cid = int(row['client_id'])
        info = names.get(cid, {'name': f'#{cid}'})
        players.append(build_player(cid, info, {
            'total_wd': float(row['total_wd']), 'total_dep': float(row['total_dep']),
            'net_payout': float(row['net'])
        }))
    top = players[0] if players else {}
    insights.append(make_insight(2, 'critical',
        f"Alerta Payout: {top.get('name','')} sacou {fR(top.get('total_wd',0))} vs {fR(top.get('total_dep',0))} depositados",
        f"{len(r)} jogadores sacaram mais do que depositaram nos últimos 30d. Net payout total: {fR(sum(float(x['net']) for x in r))}",
        players,
        'Revisar atividade de jogo e verificar abuso de bônus',
        '1) Abrir cada player no Phoenix BO. 2) Verificar histórico de bônus recebidos. 3) Analisar padrão de apostas (apostas opostas em roleta = red flag). 4) Se confirmado abuso: ajustar limites de saque. 5) Se legítimo: monitorar.',
        f'Proteção de {fR(sum(float(x["net"]) for x in r))} em payouts excessivos'
    ))

# === 3. HIGH-ROLLERS ESFRIANDO ===
print("[3/20] High-Rollers Cooling...")
r = q("""
    SELECT client_id, sum(amount)/100 as dep_14d, max(create_time) as last_dep,
           dateDiff('day', max(create_time), now()) as days_since
    FROM deposits WHERE status='PAID' AND toDate(create_time) >= today() - 14
    GROUP BY client_id
    HAVING dep_14d >= 10000 AND days_since >= 3
    ORDER BY dep_14d DESC LIMIT 10
""")
if r:
    ids = [int(x['client_id']) for x in r]
    names = get_names(ids)
    styles = get_game_styles(ids)
    players = []
    total = sum(float(x['dep_14d']) for x in r)
    for row in r:
        cid = int(row['client_id'])
        info = names.get(cid, {'name': f'#{cid}'})
        players.append(build_player(cid, info, {
            'total_dep': float(row['dep_14d']), 'days_inactive': int(row['days_since']),
            'game_style': styles.get(cid,'N/A')
        }))
    insights.append(make_insight(3, 'critical',
        f'{len(r)} high-rollers esfriando — {fR(total)} depositados recentemente',
        f"Jogadores que depositaram >R$10K nos últimos 14 dias mas pararam. Janela de recuperação curta!",
        players,
        'Push notification + oferta de cashback HOJE',
        '1) Enviar push às 14h BRT (pré-pico). 2) Mensagem: "Sentimos sua falta, [nome]! Cashback 10% no próximo depósito". 3) Para os top 3: WhatsApp direto. 4) Se não responder em 24h: email com free spins.',
        f'Retenção de {fR(total)} em valor ativo'
    ))

# === 4. ONE-TIMERS ESTA SEMANA ===
print("[4/20] One-Time Depositors...")
r = q("""
    SELECT client_id, sum(amount)/100 as dep_brl, any(create_time) as dep_time
    FROM deposits WHERE status='PAID' AND toDate(create_time) >= today() - 7
    GROUP BY client_id HAVING count() = 1
    ORDER BY dep_brl DESC LIMIT 15
""")
count_r = q("""
    SELECT count() as n, sum(total) as s FROM (
        SELECT client_id, sum(amount)/100 as total FROM deposits
        WHERE status='PAID' AND toDate(create_time) >= today() - 7
        GROUP BY client_id HAVING count() = 1
    )
""")
if r:
    total_count = int(count_r[0]['n']) if count_r else len(r)
    total_sum = float(count_r[0]['s']) if count_r else 0
    ids = [int(x['client_id']) for x in r]
    names = get_names(ids)
    players = []
    for row in r:
        cid = int(row['client_id'])
        info = names.get(cid, {'name': f'#{cid}'})
        players.append(build_player(cid, info, {'total_dep': float(row['dep_brl']), 'dep_count': 1}))
    insights.append(make_insight(4, 'critical',
        f'{total_count} jogadores com apenas 1 depósito esta semana ({fR(total_sum)})',
        f"Janela ideal de conversão para 2º depósito (primeiras 72h). Top: {', '.join(p['name']+' ('+fR(p['total_dep'])+')' for p in players[:5])}",
        players,
        'Campanha de 2º depósito imediata via push + email',
        '1) Disparar push+email AGORA com bônus de 2º depósito (50% até R$100). 2) Os 15 com FTD >R$500: WhatsApp direto. 3) Segmentar por valor: >R$1K=VIP treatment, R$100-1K=bônus, <R$100=free spins. 4) Meta: converter 20%.',
        f'Conversão de 20% = +{total_count // 5} redepositantes, {fR(total_sum * 0.2)} potencial'
    ))

# === 5. WEEK-OVER-WEEK DROPOFF ===
print("[5/20] WoW Dropoff...")
r = q("""
    SELECT d.client_id, sum(d.amount)/100 as dep_last_wk, count() as n,
           any(c.first_name) as fn, any(c.last_name) as ln, any(c.btag) as btag
    FROM deposits d LEFT JOIN client c ON d.client_id = c.client_id
    WHERE d.status='PAID' AND toDate(d.create_time) BETWEEN today() - 14 AND today() - 8
      AND d.client_id NOT IN (SELECT client_id FROM deposits WHERE status='PAID' AND toDate(create_time) >= today() - 7)
    GROUP BY d.client_id ORDER BY dep_last_wk DESC LIMIT 10
""")
if r:
    players = []
    for row in r:
        cid = int(row['client_id'])
        aff = ''
        m = re.search(r'aff_id=(\d+)', row.get('btag',''))
        if m: aff = m.group(1)
        name = f"{row.get('fn','')} {row.get('ln','')}".strip() or f"#{cid}"
        players.append({"id": cid, "name": name, "btag_aff": aff, "aff_name": AFF_NAMES.get(aff,aff),
                         "total_dep": float(row['dep_last_wk']), "dep_count": int(row['n'])})
    total = sum(p['total_dep'] for p in players)
    insights.append(make_insight(5, 'critical',
        f'{len(r)} players ativos semana passada sumiram esta semana ({fR(total)})',
        f"Depositaram semana passada mas não voltaram. Risco iminente de churn.",
        players,
        'Campanha de reengajamento em 2 ondas',
        '1) Onda 1 (hoje): push "Sentimos sua falta! Bônus R$50 esperando". 2) Onda 2 (amanhã): email com free spins. 3) Top 3 por valor: WhatsApp direto do gerente.',
        f'Retenção de {fR(total)} em volume semanal'
    ))

# === 6. WHALE ALERT (depósitos >R$2K hoje) ===
print("[6/20] Whale Alert...")
r = q("""
    SELECT d.client_id, d.amount/100 as dep_brl, d.create_time,
           any(c.first_name) as fn, any(c.last_name) as ln, any(c.btag) as btag
    FROM deposits d LEFT JOIN client c ON d.client_id = c.client_id
    WHERE d.status='PAID' AND toDate(d.create_time) = today() AND d.amount >= 200000
    GROUP BY d.client_id, d.amount, d.create_time
    ORDER BY d.amount DESC LIMIT 8
""")
if r:
    players = []
    for row in r:
        cid = int(row['client_id'])
        name = f"{row.get('fn','')} {row.get('ln','')}".strip() or f"#{cid}"
        aff = ''
        m = re.search(r'aff_id=(\d+)', row.get('btag',''))
        if m: aff = m.group(1)
        players.append({"id": cid, "name": name, "btag_aff": aff, "total_dep": float(row['dep_brl'])})
    insights.append(make_insight(6, 'critical',
        f'🐋 {len(r)} depósitos >R$2K hoje — VIP treatment imediato',
        f"Grandes depósitos detectados hoje. Esses players precisam de atenção VIP imediata.",
        players,
        'Contato VIP imediato + boas-vindas personalizada',
        '1) Verificar se é first deposit ou recorrente. 2) Se FTD: welcome call do gerente VIP em até 1h. 3) Se recorrente: enviar mensagem de agradecimento + upgrade de tier. 4) Monitorar atividade de jogo nas próximas 24h.',
        f'Retenção de whale = {fR(sum(p["total_dep"] for p in players))} em valor imediato'
    ))
else:
    # No whales today, check yesterday
    r2 = q("""
        SELECT count() as n, sum(amount)/100 as total FROM deposits
        WHERE status='PAID' AND toDate(create_time) = today() - 1 AND amount >= 200000
    """)
    whale_info = f"Ontem: {r2[0]['n']} depósitos >R$2K ({fR(float(r2[0]['total']))})" if r2 and int(r2[0]['n']) > 0 else "Nenhum depósito >R$2K ontem"
    insights.append(make_insight(6, 'monitor',
        f'Nenhum whale hoje — {whale_info}',
        'Sem depósitos >R$2K hoje. Monitorar ao longo do dia.',
        [], 'Monitorar depósitos ao longo do dia',
        'Verificar novamente às 15h (horário pico) e às 21h.', 'Detecção precoce de VIPs'
    ))

# === 7. TAXA REDEPÓSITO ===
print("[7/20] Redeposit Rate...")
r = q("""
    SELECT countDistinct(client_id) as total, countDistinctIf(client_id, n > 1) as redep
    FROM (SELECT client_id, count() as n FROM deposits WHERE status='PAID' GROUP BY client_id)
""")
if r:
    total = int(r[0]['total']); redep = int(r[0]['redep'])
    rate = (redep/total*100) if total > 0 else 0
    insights.append(make_insight(7, 'critical',
        f'Taxa de redepósito: {rate:.1f}% — {total-redep} jogadores nunca voltaram',
        f"De {total:,} depositantes, apenas {redep:,} fizeram 2+ depósitos. {total-redep:,} depositaram uma vez e abandonaram.",
        [],
        'Automação de boas-vindas pós-FTD + bônus de redepósito',
        '1) Configurar no Phoenix: trigger automático 24h após FTD. 2) Email + push com bônus 50% no 2º depósito (até R$100). 3) Para FTD >R$200: WhatsApp do gerente. 4) Meta: subir taxa para 40%.',
        f'Se subir 5pp: +{(total-redep)*5//100} redepositantes'
    ))

# === 8. LIVE CASINO PREJUÍZO ===
print("[8/20] Casino by Game Type...")
r = q("""
    SELECT game_type, sumIf(amount, status='BET')/100 as total_bet,
           sumIf(amount, status='WIN')/100 as total_win,
           (sumIf(amount, status='BET') - sumIf(amount, status='WIN'))/100 as ggr_brl,
           count(DISTINCT client_id) as players
    FROM casino WHERE toDate(spin_date) >= today() - 30
    GROUP BY game_type ORDER BY ggr_brl DESC
""")
if r:
    losing = [x for x in r if float(x['ggr_brl']) < 0]
    winning = [x for x in r if float(x['ggr_brl']) > 0]
    if losing:
        worst = losing[0]
        insights.append(make_insight(8, 'critical',
            f"{worst['game_type']} gerando PREJUÍZO — GGR {fR(float(worst['ggr_brl']))}",
            f"Tipo de jogo com GGR negativo nos últimos 30d. {worst['players']} jogadores ativos nesse tipo.",
            [],
            'Revisar regras de bônus e limites para esse tipo de jogo',
            '1) Verificar quais jogadores estão ganhando mais neste tipo. 2) Desabilitar wagering de bônus em mesas ao vivo. 3) Considerar limites de aposta máxima. 4) Monitorar semanalmente.',
            f'Proteção de {fR(abs(float(worst["ggr_brl"])))} em prejuízo mensal'
        ))
    if winning:
        best = winning[0]
        insights.append(make_insight(15, 'opportunity',
            f"Tipo mais lucrativo: {best['game_type']} — GGR {fR(float(best['ggr_brl']))}",
            f"{best['players']} jogadores ativos. Bet total: {fR(float(best['total_bet']))}.",
            [],
            'Promover este tipo de jogo para jogadores de outros tipos',
            '1) Identificar jogadores que nunca jogaram este tipo. 2) Criar campanha de cross-sell com free spins. 3) Destacar jogos populares na homepage.',
            f'Aumento de {fR(float(best["ggr_brl"]) * 0.1)} se crescer 10%'
        ))

# === 9. EMAIL PERFORMANCE ===
print("[9/20] Email Performance...")
insights.append(make_insight(9, 'critical',
    'Taxa de abertura de email em 15.8% — abaixo do setor (20-25%)',
    'De 10.266 enviados via UniOne, 1.633 abriram (15.8%). Taxa de clique 0.6% (meta 2-3%). Bounce rate 9.3% (meta <5%).',
    [],
    'Otimizar subject lines, timing e limpar lista',
    '1) Testar subject lines com emojis e urgência. 2) Enviar às 14h BRT (pré-pico depósitos). 3) Remover hard bounces do UniOne (955 falhas). 4) Segmentar: players ativos vs inativos com mensagens diferentes.',
    'Dobrar taxa de abertura = +1.600 leitores, +50 cliques'
))

# === 10. MOMENTUM SEMANAL ===
print("[10/20] Weekly Momentum...")
r = q("""
    SELECT countIf(toDate(create_time) >= today() - 7) as this_wk,
           countIf(toDate(create_time) BETWEEN today() - 14 AND today() - 8) as last_wk,
           sumIf(amount, toDate(create_time) >= today() - 7)/100 as brl_this,
           sumIf(amount, toDate(create_time) BETWEEN today() - 14 AND today() - 8)/100 as brl_last
    FROM deposits WHERE status='PAID'
""")
if r:
    tw = int(r[0]['this_wk']); lw = int(r[0]['last_wk'])
    bt = float(r[0]['brl_this']); bl = float(r[0]['brl_last'])
    pct = ((bt-bl)/bl*100) if bl > 0 else 0
    direction = '📈' if pct > 0 else '📉'
    insights.append(make_insight(10, 'critical' if pct < -10 else 'monitor',
        f'{direction} Semana atual vs anterior: {pct:+.0f}% — {fR(bt)} vs {fR(bl)}',
        f"Esta semana: {tw:,} depósitos ({fR(bt)}). Semana passada: {lw:,} ({fR(bl)}). {'Momentum positivo!' if pct > 0 else 'QUEDA — ação necessária!'}",
        [],
        'Aproveitar momentum' if pct > 0 else 'Campanha de emergência para reverter queda',
        '1) Se positivo: campanha de "semana VIP". 2) Se negativo: push de emergência com bônus. 3) Analisar quais segmentos caíram. 4) Verificar se houve problema técnico.',
        f'Manter/crescer {fR(bt)} semanal'
    ))

# === 11. REDEPOSIT WINDOW (1-3 deps, 3-7d atrás) ===
print("[11/20] Redeposit Window...")
r = q("""
    SELECT client_id, sum(amount)/100 as total, count() as n, max(create_time) as last_dep,
           dateDiff('day', max(create_time), now()) as days_since
    FROM deposits WHERE status='PAID' AND toDate(create_time) >= today() - 30
    GROUP BY client_id
    HAVING n BETWEEN 1 AND 3 AND days_since BETWEEN 3 AND 7
    ORDER BY total DESC LIMIT 12
""")
if r:
    ids = [int(x['client_id']) for x in r]
    names = get_names(ids)
    players = []
    for row in r:
        cid = int(row['client_id'])
        info = names.get(cid, {'name': f'#{cid}'})
        players.append(build_player(cid, info, {
            'total_dep': float(row['total']), 'dep_count': int(row['n']),
            'days_inactive': int(row['days_since'])
        }))
    insights.append(make_insight(11, 'opportunity',
        f'{len(r)} jogadores na janela ideal de redepósito (3-7d)',
        'Players com 1-3 depósitos no último mês, último depósito há 3-7 dias. Momento perfeito para reativação.',
        players,
        'Push + email com oferta de redepósito',
        '1) Push personalizado: "[Nome], você tem R$[bônus] esperando!". 2) Email com últimos jogos jogados. 3) Para os top 5 por valor: WhatsApp.',
        f'Conversão de {fR(sum(p["total_dep"] for p in players) * 0.3)}'
    ))

# === 12. UPSELL CANDIDATES ===
print("[12/20] Upsell Candidates...")
r = q("""
    SELECT client_id, count() as n, avg(amount)/100 as avg_dep, sum(amount)/100 as total
    FROM deposits WHERE status='PAID' AND toDate(create_time) >= today() - 30
    GROUP BY client_id HAVING n >= 5 AND avg_dep < 100
    ORDER BY n DESC LIMIT 10
""")
if r:
    ids = [int(x['client_id']) for x in r]
    names = get_names(ids)
    players = []
    for row in r:
        cid = int(row['client_id'])
        info = names.get(cid, {'name': f'#{cid}'})
        players.append(build_player(cid, info, {
            'total_dep': float(row['total']), 'dep_count': int(row['n']),
            'avg_deposit': float(row['avg_dep'])
        }))
    insights.append(make_insight(12, 'opportunity',
        f'{len(r)} jogadores frequentes com ticket baixo — oportunidade de upsell',
        f'Players com 5+ depósitos no mês mas ticket médio <R$100. São engajados mas depositam pouco.',
        players,
        'Campanha de upsell com bônus progressivo',
        '1) Oferta: "Deposite R$200 e ganhe 30% extra". 2) Criar tiers visíveis: Bronze→Prata→Ouro. 3) Push: "Falta R$[X] para o próximo nível!". 4) Meta: dobrar ticket médio.',
        f'Se ticket dobrar: +{fR(sum(p["total_dep"] for p in players))}/mês'
    ))

# === 13. GAME CROSS-SELL ===
print("[13/20] Game Cross-Sell...")
r = q("""
    SELECT client_id, any(game_type) as only_type, count() as rounds
    FROM casino WHERE toDate(spin_date) >= today() - 30
    GROUP BY client_id
    HAVING countDistinct(game_type) = 1 AND rounds > 50
    ORDER BY rounds DESC LIMIT 10
""")
if r:
    ids = [int(x['client_id']) for x in r]
    names = get_names(ids)
    players = []
    for row in r:
        cid = int(row['client_id'])
        info = names.get(cid, {'name': f'#{cid}'})
        players.append(build_player(cid, info, {'game_style': row['only_type'], 'rounds': int(row['rounds'])}))
    slot_only = sum(1 for p in players if p.get('game_style')=='SLOT')
    insights.append(make_insight(13, 'opportunity',
        f'{len(r)} jogadores jogam apenas 1 tipo de jogo — cross-sell',
        f'{slot_only} jogam só slots. Oportunidade de diversificação com free spins em outros tipos.',
        players,
        'Campanha de cross-sell com free spins em novos tipos de jogo',
        '1) Para slot-only: 20 free spins em crash games (Aviator). 2) Para roulette-only: free spins em slots populares. 3) Push: "Experimente [jogo] com 20 rodadas grátis!"',
        'Diversificação = menor risco de churn por monotonia'
    ))

# === 14. AFFILIATE PERFORMANCE ===
print("[14/20] Affiliate Performance...")
r = q("""
    SELECT extractAll(btag, 'aff_id=(\\d+)')[1] as aff_id, count() as players,
           countIf(client_id IN (SELECT client_id FROM deposits WHERE status='PAID')) as depositors
    FROM client WHERE btag LIKE '%aff_id=%' AND toDate(registration_time) >= today() - 30
    GROUP BY aff_id ORDER BY players DESC LIMIT 8
""")
if r:
    players = []
    for row in r:
        aff = row['aff_id']
        players.append({"aff_id": aff, "aff_name": AFF_NAMES.get(aff, f"Aff #{aff}"),
                         "players": int(row['players']), "depositors": int(row['depositors']),
                         "conv_rate": round(int(row['depositors'])/int(row['players'])*100,1) if int(row['players']) > 0 else 0})
    insights.append(make_insight(14, 'opportunity',
        f'Performance de afiliados: {len(r)} fontes ativas no mês',
        f"Top: {', '.join(p['aff_name']+' ('+str(p['players'])+' regs, '+str(p['conv_rate'])+'% conv)' for p in players[:4])}",
        players,
        'Otimizar investimento por afiliado baseado em conversão',
        '1) Ranquear afiliados por taxa de conversão (não volume). 2) Aumentar budget dos com >30% conversão. 3) Cobrar melhoria dos com <10%. 4) Testar novos canais.',
        'Otimização de CAC por afiliado'
    ))

# === 15 is already set above (best game type) ===

# === 16. HORÁRIO PICO ===
print("[16/20] Peak Hours...")
r = q("""
    SELECT toHour(toTimezone(create_time, 'America/Sao_Paulo')) as h, count() as n, sum(amount)/100 as brl
    FROM deposits WHERE status='PAID' AND toDate(create_time) >= today() - 30
    GROUP BY h ORDER BY h
""")
if r:
    peak = max(r, key=lambda x: int(x['n']))
    low = min(r, key=lambda x: int(x['n']))
    insights.append(make_insight(16, 'opportunity',
        f'Pico de depósitos às {peak["h"]}h BRT ({peak["n"]} deps/mês)',
        f'Vale mínimo às {low["h"]}h ({low["n"]} deps). Diferença de {int(peak["n"])//max(int(low["n"]),1)}x.',
        [],
        'Agendar campanhas para 1h antes do pico',
        f'1) Campanhas push/email às {int(peak["h"])-1}h BRT. 2) Evitar envios entre 1h-7h. 3) Promoções relâmpago às {peak["h"]}h.',
        'Maximizar conversão por timing'
    ))

# === 17. SNAPSHOT HOJE ===
print("[17/20] Today Snapshot...")
r = q("SELECT count(DISTINCT client_id) as users, count() as deps, sum(amount)/100 as brl FROM deposits WHERE status='PAID' AND toDate(create_time) = today()")
r2 = q("SELECT count(DISTINCT client_id) as users, count() as deps, sum(amount)/100 as brl FROM deposits WHERE status='PAID' AND toDate(create_time) = today() - 1")
if r and r2:
    today_brl = float(r[0]['brl'] or 0); yest_brl = float(r2[0]['brl'] or 0)
    pct = ((today_brl - yest_brl) / yest_brl * 100) if yest_brl > 0 else 0
    insights.append(make_insight(17, 'monitor',
        f"Hoje: {r[0]['deps']} depósitos, {fR(today_brl)} ({pct:+.0f}% vs ontem)",
        f"Ontem: {r2[0]['deps']} deps, {fR(yest_brl)}. Hoje até agora: {r[0]['users']} depositantes únicos.",
        [],
        'Acompanhar ao longo do dia',
        '1) Verificar às 15h (pico). 2) Se abaixo de ontem: push de emergência. 3) Se acima: manter ritmo.',
        'Monitoramento diário'
    ))

# === 18. CHURNED HIGH-VALUE ===
print("[18/20] Churned High-Value...")
r = q("""
    SELECT client_id, sum(amount)/100 as total, count() as n,
           dateDiff('day', max(create_time), now()) as days_off
    FROM deposits WHERE status='PAID'
    GROUP BY client_id HAVING total > 10000 AND days_off BETWEEN 30 AND 90
    ORDER BY total DESC LIMIT 8
""")
if r:
    ids = [int(x['client_id']) for x in r]
    names = get_names(ids)
    styles = get_game_styles(ids)
    players = []
    for row in r:
        cid = int(row['client_id'])
        info = names.get(cid, {'name': f'#{cid}'})
        players.append(build_player(cid, info, {
            'total_dep': float(row['total']), 'dep_count': int(row['n']),
            'days_inactive': int(row['days_off']), 'game_style': styles.get(cid,'N/A')
        }))
    insights.append(make_insight(18, 'opportunity',
        f'{len(r)} jogadores >R$10K churned (30-90d) — win-back',
        f'Total em risco: {fR(sum(p["total_dep"] for p in players))}. Janela de recuperação ainda aberta.',
        players,
        'Campanha de win-back escalonada',
        '1) SMS + email com oferta "última chance". 2) Bônus agressivo: 100% até R$1K. 3) Top 3: ligação direta. 4) Investigar motivo do churn no BO.',
        f'Reativação de {fR(sum(p["total_dep"] for p in players) * 0.1)}'
    ))

# === 19. TICKET MÉDIO ===
print("[19/20] Avg Ticket...")
r = q("SELECT avg(amount)/100 as avg_brl, count() as n, sum(amount)/100 as total FROM deposits WHERE status='PAID' AND toDate(create_time) >= today() - 30")
if r:
    avg = float(r[0]['avg_brl'] or 0)
    insights.append(make_insight(19, 'monitor',
        f'Ticket médio: {fR(avg)} ({r[0]["n"]} depósitos em 30d)',
        f'Volume total 30d: {fR(float(r[0]["total"] or 0))}.',
        [],
        'Estratégia de upsell para aumentar ticket',
        '1) Bônus progressivo por faixa. 2) Tiers visíveis no site. 3) Push: "Deposite R$200+ e ganhe 30%".',
        f'Se ticket subir 20%: +{fR(float(r[0]["total"] or 0) * 0.2)}/mês'
    ))

# === 20. COHORT HEALTH ===
print("[20/20] Cohort Health...")
r = q("""
    SELECT formatDateTime(registration_time, '%Y-%m') as mes, count() as regs,
           countIf(client_id IN (SELECT client_id FROM deposits WHERE status='PAID')) as deposited
    FROM client WHERE toDate(registration_time) >= today() - 90
    GROUP BY mes ORDER BY mes DESC
""")
if r:
    players = []
    for row in r:
        regs = int(row['regs']); deps = int(row['deposited'])
        players.append({"month": row['mes'], "registrations": regs, "deposited": deps,
                         "conv_rate": round(deps/regs*100,1) if regs > 0 else 0})
    insights.append(make_insight(20, 'monitor',
        f"Cohort saúde: {', '.join(p['month']+' ('+str(p['conv_rate'])+'% conv)' for p in players[:3])}",
        f"Taxa de conversão registro→depósito por mês.",
        players,
        'Monitorar tendência e otimizar onboarding',
        '1) Se conversão <20%: rever landing page e welcome bonus. 2) Se >30%: analisar o que está funcionando e replicar. 3) Acompanhar mensalmente.',
        'Otimização de funil de conversão'
    ))

# === SORT AND OUTPUT ===
insights.sort(key=lambda x: x['priority'])

# Ensure we have 20 insights (fill with placeholders if needed)
while len(insights) < 20:
    n = len(insights) + 1
    insights.append(make_insight(n, 'monitor', f'Insight #{n} — sem dados suficientes',
        'Dados insuficientes para gerar este insight hoje.', [], 'N/A', 'N/A', 'N/A'))

# Count categories
cats = {'critical': 0, 'opportunity': 0, 'monitor': 0}
for i in insights:
    cats[i['category']] = cats.get(i['category'], 0) + 1

print(f"\n[SUMMARY] {len(insights)} insights: {cats['critical']} critical, {cats['opportunity']} opportunity, {cats['monitor']} monitor")

# Write JSON
json_path = os.path.join(SCRIPT_DIR, 'daily_insights.json')
with open(json_path, 'w') as f:
    json.dump(insights, f, ensure_ascii=False, indent=2)
print(f"[OK] Saved to {json_path}")

# Inject into index.html
index_path = os.path.join(PROJECT_DIR, 'index.html')
if os.path.exists(index_path):
    with open(index_path, 'r') as f:
        html = f.read()

    # Build JS constant
    js_data = json.dumps(insights, ensure_ascii=False)
    new_const = f'const DAILY_INSIGHTS={js_data};'

    import re as _re
    if 'DAILY_INSIGHTS=' in html:
        html = _re.sub(r'const DAILY_INSIGHTS=.*?;', new_const, html, count=1)
    else:
        # Insert before PHX_CAMPS
        html = html.replace('const PHX_CAMPS=', new_const + '\nconst PHX_CAMPS=')

    with open(index_path, 'w') as f:
        f.write(html)
    print(f"[OK] Injected DAILY_INSIGHTS into index.html")

print(f"\n[DONE] Daily insights generation complete!")
