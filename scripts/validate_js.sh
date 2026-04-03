#!/bin/bash
# validate_js.sh — Valida sintaxe JS do index.html ANTES de fazer deploy
# Uso: bash scripts/validate_js.sh
# Se falhar, NÃO faça git push nem deploy no Vercel.

set -e

echo "🔍 Extraindo JavaScript do index.html..."
python3 -c "
import re, sys
with open('index.html', 'r') as f:
    html = f.read()
scripts = re.findall(r'<script[^>]*>(.*?)</script>', html, re.DOTALL)
combined = '\n'.join(scripts)
with open('/tmp/_jg_validate.js', 'w') as f:
    f.write(combined)
print(f'   {len(scripts)} blocos de script, {len(combined):,} chars')
"

echo "🔍 Verificando sintaxe com Node.js..."
if node --check /tmp/_jg_validate.js 2>&1; then
  echo ""
  echo "✅ JavaScript OK — seguro para deploy!"
  echo ""
else
  echo ""
  echo "❌ ERRO DE SINTAXE — NÃO FAÇA DEPLOY!"
  echo "   Corrija o erro acima antes de rodar: npx vercel --prod"
  echo ""
  exit 1
fi

# Verificar arquivos críticos de auth
echo "🔍 Verificando arquivos críticos..."
for f in api/login.js api/users.js package.json; do
  if [ -f "$f" ]; then
    echo "   ✅ $f existe"
  else
    echo "   ❌ FALTANDO: $f — DEPLOY BLOQUEADO!"
    exit 1
  fi
done

# Verificar ioredis no package.json
if grep -q "ioredis" package.json; then
  echo "   ✅ ioredis presente no package.json"
else
  echo "   ❌ ioredis FALTANDO no package.json — login vai quebrar!"
  exit 1
fi

echo ""
echo "🚀 Tudo OK! Pode rodar: npx vercel --prod --yes"
echo ""
