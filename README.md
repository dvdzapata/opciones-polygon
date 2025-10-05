# opciones-polygon

Script para descargar contratos de opciones desde la API de Polygon y guardarlos en PostgreSQL.
Además sincroniza los datos OHLC diarios de cada contrato mediante el endpoint `/v1/open-close/{optionsTicker}/{date}` y los persiste en la tabla `opciones_ohlc`.
La descarga aplica una ventana rodante de 2 años hacia atrás desde la fecha de ejecución para evitar recuperar contratos fuera del histórico requerido y vuelve a intentar la sincronización OHLC aunque el catálogo de contratos ya esté completo.

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

El script respeta el límite de 5 peticiones por minuto, registra el progreso de contratos en `options_progress.json`, el avance de OHLC en `options_ohlc_progress.json` y conserva los logs en `opciones_download.log`.
