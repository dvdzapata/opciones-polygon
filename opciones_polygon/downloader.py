"""Download Polygon options contracts and persist them into PostgreSQL."""
from __future__ import annotations

import datetime as _dt
import json
import logging
import os
import sys
import time
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import psycopg
import requests
from dotenv import load_dotenv

API_URL = "https://api.polygon.io/v3/reference/options/contracts"
OPEN_CLOSE_URL = "https://api.polygon.io/v1/open-close"
PROGRESS_FILE = Path("options_progress.json")
LOG_FILE = Path("opciones_download.log")
RATE_LIMIT_SECONDS = 12.5
MAX_UNDERLYINGS = 3
FUENTE = "polygon"


@dataclass
class OptionRecord:
    cfi: str
    tipo_contrato: str
    estilo_opcion: Optional[str]
    fecha_expiracion: _dt.datetime
    mercado: Optional[str]
    acciones_por_contrato: int
    precio_strike: Decimal
    id_opcion: str
    symbol: str
    asset_id: str
    cantidad_adicional: List[Optional[Decimal]]
    tipo_adicional: List[Optional[str]]
    tipo_subyacente_adicional: List[Optional[str]]


@dataclass
class OptionOHLCRecord:
    fecha_inicio: _dt.datetime
    id_opcion: str
    open: Optional[Decimal]
    high: Optional[Decimal]
    low: Optional[Decimal]
    close: Optional[Decimal]
    precio_premercado: Optional[Decimal]
    precio_afterhours: Optional[Decimal]
    volume: Optional[int]
    status: Optional[str]
    symbol: str
    asset_id: str


class ProgressTracker:
    """Track per-asset pagination progress to resume interrupted downloads."""

    def __init__(self, path: Path) -> None:
        self._path = path
        self._data: Dict[str, Dict[str, Any]] = {}
        if path.exists():
            try:
                self._data = json.loads(path.read_text())
            except (json.JSONDecodeError, OSError) as exc:
                logging.getLogger(__name__).warning(
                    "No se pudo leer progreso existente (%s): %s", path, exc
                )
                self._data = {}

    def get_cursor(self, symbol: str) -> Optional[str]:
        record = self._data.get(symbol.upper())
        if not record:
            return None
        if record.get("completed"):
            return "__COMPLETED__"
        return record.get("cursor")

    def update(self, symbol: str, cursor: Optional[str], completed: bool) -> None:
        self._data[symbol.upper()] = {
            "cursor": cursor,
            "completed": completed,
            "timestamp": _dt.datetime.now(tz=_dt.timezone.utc).isoformat(),
        }
        self._persist()

    def mark_in_progress(self, symbol: str) -> None:
        record = self._data.get(symbol.upper())
        if record and record.get("completed"):
            return
        cursor_value = record.get("cursor") if record else None
        self._data[symbol.upper()] = {
            "cursor": cursor_value,
            "completed": False,
            "timestamp": _dt.datetime.now(tz=_dt.timezone.utc).isoformat(),
        }
        self._persist()

    def _persist(self) -> None:
        try:
            self._path.write_text(json.dumps(self._data, indent=2, sort_keys=True))
        except OSError as exc:
            logging.getLogger(__name__).error(
                "Error al guardar el progreso en %s: %s", self._path, exc
            )


class PolygonOptionsDownloader:
    def __init__(
        self,
        api_key: str,
        assets: Dict[str, str],
        db_settings: Dict[str, str],
        logger: logging.Logger,
        progress_tracker: ProgressTracker,
    ) -> None:
        self.api_key = api_key
        self.assets = {symbol.upper(): asset_id for symbol, asset_id in assets.items()}
        self.db_settings = db_settings
        self.logger = logger
        self.progress = progress_tracker
        self.session = requests.Session()
        self.session.headers.update({"Authorization": f"Bearer {self.api_key}"})
        self.last_request_timestamp: Optional[float] = None

    def run(self) -> None:
        with psycopg.connect(**self.db_settings) as conn:
            conn.autocommit = False
            for symbol, asset_id in self.assets.items():
                cursor = self.progress.get_cursor(symbol)
                if cursor == "__COMPLETED__":
                    self.logger.info(
                        "Descarga ya completada anteriormente para %s. Omitiendo.",
                        symbol,
                    )
                    continue
                self.progress.mark_in_progress(symbol)
                self._download_symbol(conn, symbol, asset_id, cursor)

    def _download_symbol(
        self,
        conn: psycopg.Connection,
        symbol: str,
        asset_id: str,
        start_cursor: Optional[str],
    ) -> None:
        cursor = None if start_cursor in (None, "__COMPLETED__") else start_cursor
        total_inserted = 0
        while True:
            try:
                results, next_cursor = self._fetch_contracts(symbol, cursor)
            except Exception as exc:  # pylint: disable=broad-except
                self.logger.error(
                    "Error al obtener contratos para %s cursor=%s: %s",
                    symbol,
                    cursor,
                    exc,
                    exc_info=True,
                )
                break

            if not results:
                self.logger.info(
                    "Sin resultados para %s con cursor=%s. Marcando como completado.",
                    symbol,
                    cursor,
                )
                self.progress.update(symbol, None, True)
                break

            inserted = self._persist_contracts(conn, symbol, asset_id, results)
            total_inserted += inserted
            cursor = next_cursor
            self.progress.update(symbol, cursor, False if cursor else True)

            if not cursor:
                self.logger.info(
                    "Descarga completada para %s. Total insertado: %d",
                    symbol,
                    total_inserted,
                )
                break

    def _fetch_contracts(
        self, symbol: str, cursor: Optional[str]
    ) -> Tuple[List[Dict[str, Any]], Optional[str]]:
        self._respect_rate_limit()
        params: Dict[str, Any] = {
            "underlying_ticker": symbol,
            "limit": 100,
        }
        if cursor:
            params["cursor"] = cursor
        response = self.session.get(API_URL, params=params, timeout=30)
        if response.status_code != requests.codes.ok:
            raise RuntimeError(
                f"Respuesta inesperada {response.status_code}: {response.text[:200]}"
            )
        data = response.json()
        results = data.get("results", []) or []
        next_url = data.get("next_url")
        next_cursor = self._extract_cursor(next_url) if next_url else None
        return results, next_cursor

    def _respect_rate_limit(self) -> None:
        now = time.monotonic()
        if self.last_request_timestamp is not None:
            elapsed = now - self.last_request_timestamp
            if elapsed < RATE_LIMIT_SECONDS:
                time.sleep(RATE_LIMIT_SECONDS - elapsed)
        self.last_request_timestamp = time.monotonic()

    @staticmethod
    def _extract_cursor(next_url: str) -> Optional[str]:
        if not next_url:
            return None
        if "cursor=" not in next_url:
            return None
        return next_url.split("cursor=")[-1].split("&")[0] or None

    def _insert_contract(
        self,
        conn: psycopg.Connection,
        cur: psycopg.Cursor,
        record: OptionRecord,
    ) -> bool:
        params = (
            record.cfi,
            record.tipo_contrato,
            record.estilo_opcion,
            record.fecha_expiracion,
            record.mercado,
            record.acciones_por_contrato,
            record.precio_strike,
            record.id_opcion,
            record.symbol,
            record.asset_id,
            FUENTE,
            _dt.datetime.now(tz=_dt.timezone.utc),
            *record.cantidad_adicional,
            *record.tipo_adicional,
            *record.tipo_subyacente_adicional,
        )
        try:
            cur.execute(
                """
                INSERT INTO opciones (
                    cfi,
                    tipo_contrato,
                    estilo_opcion,
                    fecha_expiracion,
                    mercado,
                    acciones_por_contrato,
                    precio_strike,
                    id_opcion,
                    symbol,
                    asset_id,
                    fuente,
                    fecha_insercion,
                    cantidad_adicional_1,
                    cantidad_adicional_2,
                    cantidad_adicional_3,
                    tipo_adicional_1,
                    tipo_adicional_2,
                    tipo_adicional_3,
                    tipo_subyacente_adicional_1,
                    tipo_subyacente_adicional_2,
                    tipo_subyacente_adicional_3
                ) VALUES (
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s
                )
                ON CONFLICT (id_opcion) DO NOTHING
                """,
                params,
            )
            conn.commit()
            return bool(cur.rowcount)
        except psycopg.DatabaseError as exc:
            conn.rollback()
            self.logger.error(
                "Error al insertar contrato %s: %s",
                record.id_opcion,
                exc,
                exc_info=True,
            )
        return False

    def _ensure_open_close(
        self,
        conn: psycopg.Connection,
        cur: psycopg.Cursor,
        record: OptionRecord,
    ) -> None:
        if self._open_close_exists(cur, record):
            return
        date_str = record.fecha_expiracion.date().isoformat()
        try:
            raw = self._fetch_open_close(record.id_opcion, date_str)
        except FileNotFoundError:
            self.logger.warning(
                "OHLC no disponible para %s en fecha %s", record.id_opcion, date_str
            )
            return
        except Exception as exc:  # pylint: disable=broad-except
            raise RuntimeError(
                f"Fallo al descargar OHLC para {record.id_opcion} {date_str}"
            ) from exc
        try:
            ohlc_record = self._validate_ohlc(raw, record)
        except ValueError as exc:
            self.logger.error(
                "Datos OHLC descartados para %s: %s | Datos: %s",
                record.id_opcion,
                exc,
                raw,
            )
            return
        self._persist_ohlc(conn, cur, ohlc_record)

    @staticmethod
    def _open_close_exists(cur: psycopg.Cursor, record: OptionRecord) -> bool:
        cur.execute(
            """
            SELECT 1
            FROM opciones_ohlc
            WHERE symbol = %s AND asset_id = %s AND id_opcion = %s
            LIMIT 1
            """,
            (record.symbol, record.asset_id, record.id_opcion),
        )
        return cur.fetchone() is not None

    def _fetch_open_close(self, option_ticker: str, date_str: str) -> Dict[str, Any]:
        self._respect_rate_limit()
        url = f"{OPEN_CLOSE_URL}/{option_ticker}/{date_str}"
        response = self.session.get(url, timeout=30)
        if response.status_code == requests.codes.not_found:
            raise FileNotFoundError(response.text)
        if response.status_code != requests.codes.ok:
            raise RuntimeError(
                f"Respuesta inesperada {response.status_code}: {response.text[:200]}"
            )
        return response.json()

    def _validate_ohlc(
        self, payload: Dict[str, Any], contract: OptionRecord
    ) -> OptionOHLCRecord:
        option_symbol = payload.get("symbol")
        if option_symbol != contract.id_opcion:
            raise ValueError("symbol del OHLC no coincide con id_opcion")

        from_value = payload.get("from")
        if not isinstance(from_value, str) or not from_value:
            raise ValueError("Campo 'from' inválido")
        try:
            from_date = _dt.date.fromisoformat(from_value)
        except ValueError as exc:
            raise ValueError("Formato de fecha_inicio inválido") from exc
        fecha_inicio = _dt.datetime.combine(
            from_date, _dt.time.min, tzinfo=_dt.timezone.utc
        )

        volume_value = payload.get("volume")
        if volume_value is not None:
            if not isinstance(volume_value, int) or volume_value < 0:
                raise ValueError("volume inválido")

        status_value = payload.get("status")
        if status_value is not None and not isinstance(status_value, str):
            raise ValueError("status inválido")

        return OptionOHLCRecord(
            fecha_inicio=fecha_inicio,
            id_opcion=contract.id_opcion,
            open=self._optional_decimal(payload.get("open"), "open"),
            high=self._optional_decimal(payload.get("high"), "high"),
            low=self._optional_decimal(payload.get("low"), "low"),
            close=self._optional_decimal(payload.get("close"), "close"),
            precio_premercado=self._optional_decimal(
                payload.get("preMarket"), "preMarket"
            ),
            precio_afterhours=self._optional_decimal(
                payload.get("afterHours"), "afterHours"
            ),
            volume=volume_value,
            status=status_value,
            symbol=contract.symbol,
            asset_id=contract.asset_id,
        )

    def _persist_ohlc(
        self, conn: psycopg.Connection, cur: psycopg.Cursor, record: OptionOHLCRecord
    ) -> None:
        params = (
            record.fecha_inicio,
            record.id_opcion,
            record.open,
            record.high,
            record.low,
            record.close,
            record.precio_premercado,
            record.precio_afterhours,
            record.volume,
            record.status,
            record.symbol,
            record.asset_id,
            _dt.datetime.now(tz=_dt.timezone.utc),
        )
        try:
            cur.execute(
                """
                INSERT INTO opciones_ohlc (
                    fecha_inicio,
                    id_opcion,
                    open,
                    high,
                    low,
                    close,
                    precio_premercado,
                    precio_afterhours,
                    volume,
                    status,
                    symbol,
                    asset_id,
                    ts_ingesta
                ) VALUES (
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s
                )
                ON CONFLICT (symbol, asset_id, id_opcion) DO UPDATE SET
                    fecha_inicio = EXCLUDED.fecha_inicio,
                    open = EXCLUDED.open,
                    high = EXCLUDED.high,
                    low = EXCLUDED.low,
                    close = EXCLUDED.close,
                    precio_premercado = EXCLUDED.precio_premercado,
                    precio_afterhours = EXCLUDED.precio_afterhours,
                    volume = EXCLUDED.volume,
                    status = EXCLUDED.status,
                    ts_ingesta = EXCLUDED.ts_ingesta
                """,
                params,
            )
            conn.commit()
            self.logger.info(
                "OHLC almacenado | symbol=%s id_opcion=%s fecha=%s",
                record.symbol,
                record.id_opcion,
                record.fecha_inicio.date().isoformat(),
            )
        except psycopg.DatabaseError as exc:
            conn.rollback()
            raise RuntimeError(f"Error al insertar OHLC {record.id_opcion}") from exc

    @staticmethod
    def _optional_decimal(value: Any, field_name: str) -> Optional[Decimal]:
        if value is None:
            return None
        try:
            decimal_value = Decimal(str(value))
        except (InvalidOperation, TypeError) as exc:
            raise ValueError(f"{field_name} inválido") from exc
        return decimal_value.quantize(Decimal("0.0001"))

    def _persist_contracts(
        self,
        conn: psycopg.Connection,
        symbol: str,
        asset_id: str,
        contracts: Iterable[Dict[str, Any]],
    ) -> int:
        inserted = 0
        with conn.cursor() as cur:
            for contract in contracts:
                try:
                    record = self._validate_and_transform(contract, symbol, asset_id)
                except ValueError as exc:
                    self.logger.error(
                        "Contrato descartado para %s: %s | Datos: %s",
                        symbol,
                        exc,
                        contract,
                    )
                    continue

                new_inserted = self._insert_contract(conn, cur, record)
                if new_inserted:
                    inserted += 1
                    self.logger.info(
                        "Progreso | fecha_expiracion=%s symbol=%s id_opcion=%s total=%d",
                        record.fecha_expiracion.isoformat(),
                        record.symbol,
                        record.id_opcion,
                        inserted,
                    )

                try:
                    self._ensure_open_close(conn, cur, record)
                except Exception as exc:  # pylint: disable=broad-except
                    conn.rollback()
                    self.logger.error(
                        "Error al procesar OHLC para %s: %s",
                        record.id_opcion,
                        exc,
                        exc_info=True,
                    )
        return inserted

    def _validate_and_transform(
        self, contract: Dict[str, Any], symbol: str, asset_id: str
    ) -> OptionRecord:
        required_str_fields = ["cfi", "contract_type", "ticker", "underlying_ticker"]
        for field in required_str_fields:
            value = contract.get(field)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"Campo obligatorio inválido: {field}")

        contract_type = contract["contract_type"].lower()
        if contract_type not in {"call", "put"}:
            raise ValueError(f"Tipo de contrato no soportado: {contract_type}")

        exercise_style = contract.get("exercise_style")
        if exercise_style is not None:
            if not isinstance(exercise_style, str):
                raise ValueError("exercise_style inválido")
            exercise_style = exercise_style.lower()
            if exercise_style not in {"american", "european"}:
                raise ValueError(f"exercise_style no soportado: {exercise_style}")

        expiration_date_raw = contract.get("expiration_date")
        if not isinstance(expiration_date_raw, str) or not expiration_date_raw:
            raise ValueError("expiration_date inválido")
        try:
            expiration_date = _dt.date.fromisoformat(expiration_date_raw)
        except ValueError as exc:
            raise ValueError("Formato de fecha_expiracion inválido") from exc
        expiration_dt = _dt.datetime.combine(
            expiration_date, _dt.time.min, tzinfo=_dt.timezone.utc
        )

        shares = contract.get("shares_per_contract")
        if not isinstance(shares, int) or shares <= 0:
            raise ValueError("shares_per_contract inválido")

        strike_raw = contract.get("strike_price")
        if strike_raw is None:
            raise ValueError("strike_price faltante")
        try:
            strike_decimal = Decimal(str(strike_raw))
        except (InvalidOperation, TypeError) as exc:
            raise ValueError("strike_price inválido") from exc
        if strike_decimal < 0:
            raise ValueError("strike_price debe ser >= 0")

        primary_exchange = contract.get("primary_exchange")
        if primary_exchange is not None and not isinstance(primary_exchange, str):
            raise ValueError("primary_exchange inválido")

        additional = contract.get("additional_underlyings") or []
        if not isinstance(additional, list):
            raise ValueError("additional_underlyings debe ser lista")
        cantidad_adicional: List[Optional[Decimal]] = [None] * MAX_UNDERLYINGS
        tipo_adicional: List[Optional[str]] = [None] * MAX_UNDERLYINGS
        tipo_subyacente_adicional: List[Optional[str]] = [None] * MAX_UNDERLYINGS
        for idx, item in enumerate(additional[:MAX_UNDERLYINGS]):
            if not isinstance(item, dict):
                raise ValueError("Elemento additional_underlyings inválido")
            amount = item.get("amount")
            type_ = item.get("type")
            underlying = item.get("underlying")
            if amount is not None:
                try:
                    amount_decimal = Decimal(str(amount))
                except (InvalidOperation, TypeError) as exc:
                    raise ValueError("Cantidad adicional inválida") from exc
                cantidad_adicional[idx] = amount_decimal
            if type_ is not None:
                if not isinstance(type_, str):
                    raise ValueError("Tipo adicional inválido")
                tipo_adicional[idx] = type_.lower()
            if underlying is not None:
                if not isinstance(underlying, str):
                    raise ValueError("Subyacente adicional inválido")
                tipo_subyacente_adicional[idx] = underlying

        if contract["underlying_ticker"].upper() != symbol.upper():
            raise ValueError(
                "El underlying_ticker del contrato no coincide con el símbolo solicitado"
            )

        return OptionRecord(
            cfi=contract["cfi"],
            tipo_contrato=contract_type,
            estilo_opcion=exercise_style,
            fecha_expiracion=expiration_dt,
            mercado=primary_exchange,
            acciones_por_contrato=shares,
            precio_strike=strike_decimal.quantize(Decimal("0.01")),
            id_opcion=contract["ticker"],
            symbol=symbol.upper(),
            asset_id=str(asset_id),
            cantidad_adicional=cantidad_adicional,
            tipo_adicional=tipo_adicional,
            tipo_subyacente_adicional=tipo_subyacente_adicional,
        )


def configure_logging() -> logging.Logger:
    logger = logging.getLogger("opciones_downloader")
    logger.setLevel(logging.INFO)

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(message)s", "%Y-%m-%d %H:%M:%S"
    )

    file_handler = logging.FileHandler(LOG_FILE, encoding="utf-8")
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    return logger


def parse_assets(value: str) -> Dict[str, str]:
    assets: Dict[str, str] = {}
    for part in value.split(","):
        part = part.strip()
        if not part:
            continue
        if ":" not in part:
            raise ValueError(f"Formato inválido en ASSETS: {part}")
        symbol, asset_id = part.split(":", 1)
        symbol = symbol.strip().upper()
        asset_id = asset_id.strip()
        if not symbol or not asset_id:
            raise ValueError(f"Formato inválido en ASSETS: {part}")
        assets[symbol] = asset_id
    if not assets:
        raise ValueError("No se especificaron assets en ASSETS")
    return assets


def load_configuration() -> Tuple[str, Dict[str, str], Dict[str, str]]:
    load_dotenv()
    api_key = os.getenv("POLYGON_API_KEY") or os.getenv("API_KEY")
    if not api_key:
        raise RuntimeError("POLYGON_API_KEY/API_KEY no definido en .env")
    assets_env = os.getenv("ASSETS")
    if not assets_env:
        raise RuntimeError("ASSETS no definido en .env")
    assets = parse_assets(assets_env)
    db_settings = {
        "host": os.getenv("DB_HOST"),
        "port": os.getenv("DB_PORT"),
        "dbname": os.getenv("DB_NAME"),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "connect_timeout": 10,
    }
    if not all(db_settings.values()):
        missing = [k for k, v in db_settings.items() if not v]
        raise RuntimeError(f"Variables de base de datos faltantes: {', '.join(missing)}")
    return api_key, assets, db_settings


def main() -> None:
    logger = configure_logging()
    try:
        api_key, assets, db_settings = load_configuration()
    except Exception as exc:  # pylint: disable=broad-except
        logger.error("Error de configuración: %s", exc)
        sys.exit(1)

    progress_tracker = ProgressTracker(PROGRESS_FILE)
    downloader = PolygonOptionsDownloader(
        api_key=api_key,
        assets=assets,
        db_settings=db_settings,
        logger=logger,
        progress_tracker=progress_tracker,
    )
    try:
        downloader.run()
    except KeyboardInterrupt:
        logger.warning("Descarga interrumpida por el usuario. Se conserva el progreso.")
    except Exception as exc:  # pylint: disable=broad-except
        logger.error("Fallo inesperado: %s", exc, exc_info=True)


if __name__ == "__main__":
    main()
