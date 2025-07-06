#!/bin/bash

# =============================================================================
# VERIFICACION DE SEGURIDAD ANTES DE COMMIT
# =============================================================================

echo "Verificando seguridad antes de Git commit..."

# Colores
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

ERRORS=0

check_item() {
    if [ $1 -eq 0 ]; then
        echo -e "  OK: $2"
    else
        echo -e "  ERROR: $2"
        ERRORS=$((ERRORS + 1))
    fi
}

echo ""
echo "Verificando archivos sensibles..."

# Verificar .env no este en git
git ls-files | grep -q "^\.env$"
if [ $? -eq 0 ]; then
    echo -e "  ERROR: .env esta en Git (PELIGROSO)"
    ERRORS=$((ERRORS + 1))
else
    echo -e "  OK: .env no esta en Git"
fi

# Verificar archivos JSON sensibles
git ls-files | grep -E ".*credentials.*\.json$|.*-[a-f0-9]{12}\.json$" > /dev/null
if [ $? -eq 0 ]; then
    echo -e "  ERROR: Archivos JSON sensibles en Git:"
    git ls-files | grep -E ".*credentials.*\.json$|.*-[a-f0-9]{12}\.json$"
    ERRORS=$((ERRORS + 1))
else
    echo -e "  OK: No hay archivos JSON sensibles en Git"
fi

# Verificar credentials/ no este en git
git ls-files | grep -q "^credentials/"
if [ $? -eq 0 ]; then
    echo -e "  ERROR: Directorio credentials/ esta en Git"
    ERRORS=$((ERRORS + 1))
else
    echo -e "  OK: Directorio credentials/ no esta en Git"
fi

echo ""
echo "Buscando credenciales hardcodeadas..."

# Buscar credenciales hardcodeadas (excluyendo falsos positivos)
HARDCODED=$(grep -r -i "password\s*=\s*[\"'][^\"']*[\"']\|secret\s*=\s*[\"'][^\"']*[\"']\|api_key\s*=\s*[\"'][^\"']*[\"']\|access_token\s*=\s*[\"'][^\"']*[\"']" \
    --include="*.py" --include="*.yml" --include="*.yaml" --exclude-dir=".git" . | \
    grep -v "tu_password\|tu_secret\|your_\|xcom_push\|xcom_pull\|key='" | head -5)

if [ -n "$HARDCODED" ]; then
    echo -e "  ERROR: Posibles credenciales hardcodeadas:"
    echo "$HARDCODED"
    ERRORS=$((ERRORS + 1))
else
    echo -e "  OK: No se encontraron credenciales hardcodeadas"
fi

echo ""
echo "Verificando estructura..."

[ -f ".env.example" ]
check_item $? ".env.example existe"

grep -q "\.env" .gitignore
check_item $? ".gitignore incluye .env"

grep -q "credentials/" .gitignore
check_item $? ".gitignore incluye credentials/"

echo ""
echo "Resultado:"

if [ $ERRORS -eq 0 ]; then
    echo -e "${GREEN}EXITO: Proyecto seguro para commit!${NC}"
    echo ""
    echo "Comandos sugeridos:"
    echo "  git add ."
    echo "  git commit -m 'Tu mensaje'"
    echo "  git push origin main"
    exit 0
else
    echo -e "${RED}ERROR: $ERRORS problemas de seguridad encontrados${NC}"
    echo ""
    echo -e "${YELLOW}Correcciones necesarias:${NC}"
    echo "  • Mueve archivos sensibles a credentials/"
    echo "  • Usa variables de entorno"
    echo "  • Verifica .gitignore"
    exit 1
fi
