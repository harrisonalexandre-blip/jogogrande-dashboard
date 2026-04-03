// Vercel Serverless Function — AI Chat with Claude
// POST /api/ai-chat
// Body: { messages: [{role,content}], dataContext: {...} }

export default async function handler(req, res) {
  // CORS
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') return res.status(200).end();
  if (req.method !== 'POST') return res.status(405).json({ error: 'Method not allowed' });

  const ANTHROPIC_KEY = process.env.ANTHROPIC_API_KEY;
  if (!ANTHROPIC_KEY) {
    return res.status(500).json({ error: 'ANTHROPIC_API_KEY não configurada. Adicione no Vercel Environment Variables.' });
  }

  try {
    const { messages, dataContext } = req.body;
    if (!messages || !messages.length) {
      return res.status(400).json({ error: 'Nenhuma mensagem enviada' });
    }

    // Build system prompt with all dashboard data
    const systemPrompt = buildSystemPrompt(dataContext);

    // Call Claude API
    const response = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-api-key': ANTHROPIC_KEY,
        'anthropic-version': '2023-06-01'
      },
      body: JSON.stringify({
        model: 'claude-sonnet-4-6',
        max_tokens: 1024,
        system: systemPrompt,
        messages: messages.slice(-10) // Keep last 10 messages for context
      })
    });

    if (!response.ok) {
      const err = await response.text();
      console.error('Claude API error:', response.status, err);
      let detail = '';
      try { detail = JSON.parse(err)?.error?.message || err.slice(0, 200); } catch(_) { detail = err.slice(0, 200); }
      return res.status(response.status).json({ error: `API Claude ${response.status}: ${detail}` });
    }

    const data = await response.json();
    const reply = data.content?.[0]?.text || 'Sem resposta';

    return res.status(200).json({ response: reply });
  } catch (e) {
    console.error('AI Chat error:', e);
    return res.status(500).json({ error: e.message });
  }
}

function buildSystemPrompt(ctx) {
  if (!ctx) return 'Você é um analista de dados. Responda de forma concisa em português.';

  let prompt = `Você é um analista de dados sênior especializado em iGaming/apostas esportivas e cassino online. Você trabalha para a AFFBR, uma empresa de marketing de afiliados que opera a marca "Jogo Grande".

REGRAS DE RESPOSTA:
- Responda SEMPRE em português do Brasil
- Seja conciso e direto — máximo 4-5 parágrafos
- Use números reais dos dados fornecidos, sempre com R$ para valores monetários
- Destaque métricas importantes com **negrito**
- Compare com período anterior quando relevante
- Dê insights e sugestões acionáveis quando possível
- Se não tiver dados suficientes para responder, diga claramente

CONTEXTO ATUAL DO DASHBOARD:
- Período selecionado: ${ctx.periodLabel || ctx.currentPeriod || 'N/A'}
- Dias no período: ${ctx.kpis?.dias || 'N/A'}
`;

  // KPIs
  if (ctx.kpis) {
    const k = ctx.kpis;
    prompt += `
MÉTRICAS PRINCIPAIS (${ctx.periodLabel}):
- Cadastros: ${k.cadastros} | FTD: ${k.ftd} | Conversão: ${k.conversionPct?.toFixed(1)}%
- Vol. Depósitos: R$ ${k.volDepositos?.toLocaleString('pt-BR')} (${k.numDepositos} transações) | Ticket Médio Dep: R$ ${k.ticketMedioDep?.toFixed(2)}
- Vol. Saques: R$ ${k.volSaques?.toLocaleString('pt-BR')} (${k.numSaques} transações) | Ticket Médio Saq: R$ ${k.ticketMedioSaq?.toFixed(2)}
- % Dep/Saq: ${k.depSaqPct?.toFixed(1)}%
- NGR: R$ ${k.ngr?.toLocaleString('pt-BR')} | NetCash: R$ ${k.netCash?.toLocaleString('pt-BR')}
- Casino GGR: R$ ${k.casinoGGR?.toLocaleString('pt-BR')} | Sportsbook GGR: R$ ${k.sportsbookGGR?.toLocaleString('pt-BR')}
- Total Wagering: R$ ${k.totalWagering?.toLocaleString('pt-BR')} | Hold %: ${k.holdPct?.toFixed(2)}%
- Jogadores Ativos (UAP): ${k.uap}
- Volume FTD: R$ ${k.ftdVolume?.toLocaleString('pt-BR')}
`;
  }

  // Previous period
  if (ctx.prevPeriod) {
    const p = ctx.prevPeriod;
    prompt += `
PERÍODO ANTERIOR (${p.label}):
- Cadastros: ${p.cadastros} | FTD: ${p.ftd}
- Vol. Depósitos: R$ ${p.volDepositos?.toLocaleString('pt-BR')}
- Vol. Saques: R$ ${p.volSaques?.toLocaleString('pt-BR')}
- NGR: R$ ${p.ngr?.toLocaleString('pt-BR')} | NetCash: R$ ${p.netCash?.toLocaleString('pt-BR')}
- Wagering: R$ ${p.totalWagering?.toLocaleString('pt-BR')}
- UAP: ${p.uap} | Dias: ${p.dias}
`;
  }

  // Churn
  if (ctx.churn) {
    prompt += `
CHURN:
- 7 dias: ${ctx.churn.rate7d?.toFixed(1)}% (${ctx.churn.count7d} players)
- 30 dias: ${ctx.churn.rate30d?.toFixed(1)}% (${ctx.churn.count30d} inativos)
- 90 dias: ${ctx.churn.rate90d?.toFixed(1)}% (${ctx.churn.count90d} inativos)
- Total de players: ${ctx.churn.totalPlayers}
`;
  }

  // Top players
  if (ctx.topPlayers) {
    prompt += `
TOP 10 PLAYERS POR TURNOVER:
${ctx.topPlayers.byTurnover?.map((p, i) => `${i + 1}. ${p.name}: TO R$ ${p.turnover?.toLocaleString('pt-BR')} | NGR R$ ${p.ngr?.toLocaleString('pt-BR')} | Dep R$ ${p.deposits?.toLocaleString('pt-BR')}`).join('\n')}

MELHORES PLAYERS (mais lucro para nós, NGR mais negativo):
${ctx.topPlayers.bestForUs?.map((p, i) => `${i + 1}. ${p.name}: NGR R$ ${p.ngr?.toLocaleString('pt-BR')}`).join('\n') || 'N/A'}

PIORES PLAYERS (maior prejuízo, NGR mais positivo):
${ctx.topPlayers.worstForUs?.map((p, i) => `${i + 1}. ${p.name}: NGR R$ ${p.ngr?.toLocaleString('pt-BR')}`).join('\n') || 'N/A'}

Total de players no sistema: ${ctx.totalPlayers}
`;
  }

  // Affiliates
  if (ctx.topAffiliates) {
    prompt += `
TOP 10 AFILIADOS POR DEPÓSITOS:
${ctx.topAffiliates.map((a, i) => `${i + 1}. ${a.btag}: Dep R$ ${a.deposits?.toLocaleString('pt-BR')} | ${a.players} players | ${a.ftd} FTDs | NGR R$ ${a.ngr?.toLocaleString('pt-BR')}`).join('\n')}
`;
  }

  // Daily trend
  if (ctx.dailyTrend?.length) {
    prompt += `
TENDÊNCIA DIÁRIA (últimos ${ctx.dailyTrend.length} dias):
${ctx.dailyTrend.map(d => `${d.date}: Dep R$${d.deposits} | Saq R$${d.withdrawals} | NGR R$${d.ngr} | Reg ${d.registrations} | FTD ${d.ftd} | UAP ${d.uap}`).join('\n')}
`;
  }

  // EUR/BRL
  if (ctx.eurBrlRate) {
    prompt += `\nCâmbio EUR/BRL: ${ctx.eurBrlRate}\n`;
  }

  // GA4
  if (ctx.ga4Realtime) {
    prompt += `\nGA4 Tempo Real: ${ctx.ga4Realtime.usersNow} usuários online agora\n`;
  }

  return prompt;
}
