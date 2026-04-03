#!/bin/bash
# DigiToPay CSV Sync & Deploy
# Uso: ./dtp_sync.sh <caminho_do_csv_exportado>
# Exemplo: ./dtp_sync.sh ~/Downloads/ELEVEX\ GROUP\ LTD.csv

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
DEST="$PROJECT_DIR/processados/ELEVEX GROUP LTD_LATEST.csv"

# ─── Verificar argumento ───
if [ -z "$1" ]; then
  # Sem argumento: procura o CSV mais recente em ~/Downloads
  CSV=$(ls -t ~/Downloads/ELEVEX*.csv ~/Downloads/ELEVEX*.CSV 2>/dev/null | head -1)
  if [ -z "$CSV" ]; then
    echo "❌ Uso: $0 <caminho_do_csv>"
    echo "   Ou coloque o CSV exportado em ~/Downloads com nome começando em ELEVEX"
    exit 1
  fi
  echo "📂 CSV encontrado automaticamente: $CSV"
else
  CSV="$1"
fi

# ─── Validar arquivo ───
if [ ! -f "$CSV" ]; then
  echo "❌ Arquivo não encontrado: $CSV"
  exit 1
fi

LINES=$(wc -l < "$CSV")
if [ "$LINES" -lt 10 ]; then
  echo "❌ CSV parece vazio ou inválido ($LINES linhas)"
  exit 1
fi

echo "✅ CSV válido: $LINES linhas"
echo "📋 Últimas 3 transações:"
tail -3 "$CSV" | cut -d';' -f1,3,4,8,10

# ─── Copiar CSV ───
echo ""
echo "📁 Copiando para processados/..."
cp "$CSV" "$DEST"
echo "   → $DEST"

# ─── Rodar sync ───
echo ""
echo "🔄 Rodando full_sync.py..."
cd "$PROJECT_DIR"
python3 "$SCRIPT_DIR/full_sync.py"

# ─── Deploy ───
echo ""
echo "🚀 Deploy Vercel..."
vercel deploy --prod

echo ""
echo "✅ Tudo pronto! Dashboard atualizado."
