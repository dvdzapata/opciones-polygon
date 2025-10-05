# opciones-polygon

Script para descargar contratos de opciones desde la API de Polygon y guardarlos en PostgreSQL.

## Configuración

1. Crear un archivo `.env` en el directorio raíz con las siguientes variables:
   ```env
   POLYGON_API_KEY=tu_api_key
   ASSETS=AVGO:11,AAPL:12
   DB_HOST=localhost
   DB_PORT=5432
   DB_NAME=opciones
   DB_USER=usuario
   DB_PASSWORD=secreto
   ```

2. Instalar dependencias:
   ```bash
   pip install -r requirements.txt
   ```

## Ejecución

```bash
python -m opciones_polygon.downloader
```

El script respeta el límite de 5 peticiones por minuto, registra el progreso en `options_progress.json` y conserva los logs en `opciones_download.log`.
