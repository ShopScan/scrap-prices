# Guía de Seguridad

## ANTES DE SUBIR A GITHUB

### Verificación Automática
```bash
./check_security.sh
```

### Archivos que NUNCA deben estar en Git
- `.env` - Variables de entorno
- `credentials/` - Archivos de credenciales
- `*.json` (archivos de service accounts)
- `logs/` - Logs que pueden contener información sensible

## Configuración Segura

### Generar Credenciales
```bash
./generate_secrets.sh  # Genera .env con passwords seguros
```

### Variables de Entorno Requeridas
- `POSTGRES_PASSWORD` - Password de base de datos
- `AIRFLOW__WEBSERVER__SECRET_KEY` - Clave secreta de Airflow
- `MINIO_ROOT_PASSWORD` - Password de MinIO
- `GCP_PROJECT_ID` - Project ID de Google Cloud

## Lista de Verificación

- [ ] `.env` no está en Git
- [ ] Credenciales GCP están en `credentials/` (no en Git)
- [ ] Passwords son únicos y seguros
- [ ] `.gitignore` está actualizado
- [ ] `./check_security.sh` pasa sin errores

## Si Commiteaste Credenciales

```bash
# Eliminar archivo del historial
git filter-branch --force --index-filter \
'git rm --cached --ignore-unmatch archivo_sensible.json' \
--prune-empty --tag-name-filter cat -- --all

# Forzar push
git push origin --force --all
```
