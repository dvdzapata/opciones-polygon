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
OHLC_PROGRESS_FILE = Path("options_ohlc_progress.json")
LOG_FILE = Path("opciones_download.log")
RATE_LIMIT_SECONDS = 12.5
MAX_UNDERLYINGS = 3
FUENTE = "polygon"
HISTORY_LOOKBACK_DAYS = 365 * 2


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


class OHLCProgressTracker:
    """Track per-option OHLC synchronization progress."""

    def __init__(self, path: Path) -> None:
        self._path = path
        self._data: Dict[str, Dict[str, Any]] = {}
        if path.exists():
            try:
                self._data = json.loads(path.read_text())
            except (json.JSONDecodeError, OSError) as exc:
                logging.getLogger(__name__).warning(
                    "No se pudo leer progreso OHLC existente (%s): %s", path, exc
                )
                self._data = {}

    def get_last_date(self, option_id: str) -> Optional[_dt.date]:
        entry = self._data.get(option_id)
        if not entry:
            return None
        last_date = entry.get("last_date")
        if not last_date:
            return None
        try:
            return _dt.date.fromisoformat(last_date)
        except ValueError:
            return None

    def update(self, option_id: str, date_value: _dt.date, status: str) -> None:
        self._data[option_id] = {
            "last_date": date_value.isoformat(),
            "status": status,
            "timestamp": _dt.datetime.now(tz=_dt.timezone.utc).isoformat(),
        }
        self._persist()

    def _persist(self) -> None:
        try:
            self._path.write_text(json.dumps(self._data, indent=2, sort_keys=True))
        except OSError as exc:
            logging.getLogger(__name__).error(
                "Error al guardar el progreso OHLC en %s: %s", self._path, exc
            )


class PolygonOptionsDownloader:
    def __init__(
        self,
        api_key: str,
        assets: Dict[str, str],
        db_settings: Dict[str, str],
        logger: logging.Logger,
        progress_tracker: ProgressTracker,
        ohlc_progress_tracker: OHLCProgressTracker,
    ) -> None:
        self.api_key = api_key
        self.assets = {symbol.upper(): asset_id for symbol, asset_id in assets.items()}
        self.db_settings = db_settings
        self.logger = logger
        self.progress = progress_tracker
        self.ohlc_progress = ohlc_progress_tracker
        self.session = requests.Session()
        self.session.headers.update({"Authorization": f"Bearer {self.api_key}"})
        self.last_request_timestamp: Optional[float] = None
        today = _dt.datetime.now(tz=_dt.timezone.utc).date()
        self.history_start_date = today - _dt.timedelta(days=HISTORY_LOOKBACK_DAYS)

    def run(self) -> None:
        with psycopg.connect(**self.db_settings) as conn:
            conn.autocommit = False
            for symbol, asset_id in self.assets.items():
                cursor = self.progress.get_cursor(symbol)
                if cursor == "__COMPLETED__":
                    self.logger.info(
                        "Descarga de contratos ya completada para %s. Reintentando OHLC.",
                        symbol,
                    )
                else:
                    self.progress.mark_in_progress(symbol)
                    self._download_symbol(conn, symbol, asset_id, cursor)
                self._sync_open_close_for_asset(conn, symbol, asset_id)

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

    def _sync_open_close_for_asset(
        self, conn: psycopg.Connection, symbol: str, asset_id: str
    ) -> None:
        symbol = symbol.upper()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id_opcion, fecha_expiracion
                FROM opciones
                WHERE symbol = %s AND asset_id = %s
                ORDER BY fecha_expiracion
                """,
                (symbol, asset_id),
            )
            contracts = cur.fetchall()

        if not contracts:
            self.logger.info(
                "Sin contratos almacenados para %s. No hay OHLC pendientes.", symbol
            )
            return

        with conn.cursor() as cur:
            for option_id, expiration_dt in contracts:
                if not option_id or expiration_dt is None:
                    self.logger.error(
                        "Registro de contrato inválido para %s: %s", symbol, option_id
                    )
                    continue
                if not isinstance(expiration_dt, _dt.datetime):
                    self.logger.error(
                        "fecha_expiracion inválida para %s: %s",
                        option_id,
                        expiration_dt,
                    )
                    continue
                expiration_date = expiration_dt.date()
                try:
                    self._sync_option_ohlc(
                        conn,
                        cur,
                        option_id=option_id,
                        symbol=symbol,
                        asset_id=asset_id,
                        expiration_date=expiration_date,
                    )
                except Exception as exc:  # pylint: disable=broad-except
                    conn.rollback()
                    self.logger.error(
                        "Error al sincronizar OHLC para %s: %s",
                        option_id,
                        exc,
                        exc_info=True,
                    )

    def _fetch_contracts(
        self, symbol: str, cursor: Optional[str]
    ) -> Tuple[List[Dict[str, Any]], Optional[str]]:
        self._respect_rate_limit()
        params: Dict[str, Any] = {
            "underlying_ticker": symbol,
            "limit": 100,
            "expiration_date.gte": self.history_start_date.isoformat(),
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
        self._sync_option_ohlc(
            conn,
            cur,
            option_id=record.id_opcion,
            symbol=record.symbol,
            asset_id=record.asset_id,
            expiration_date=record.fecha_expiracion.date(),
        )

    def _sync_option_ohlc(
        self,
        conn: psycopg.Connection,
        cur: psycopg.Cursor,
        option_id: str,
        symbol: str,
        asset_id: str,
        expiration_date: _dt.date,
    ) -> None:
        today = _dt.datetime.now(tz=_dt.timezone.utc).date()
        target_date = min(today, expiration_date)
        if target_date < self.history_start_date:
            return

        last_date = self.ohlc_progress.get_last_date(option_id)
        if last_date is not None and last_date >= target_date:
            return

        if self._open_close_up_to_date(cur, symbol, asset_id, option_id, target_date):
            self.ohlc_progress.update(option_id, target_date, "ok")
            return

        date_str = target_date.isoformat()
        self.logger.info(
            "Descargando OHLC %s fecha=%s", option_id, date_str
        )
        try:
            raw = self._fetch_open_close(option_id, date_str)
        except FileNotFoundError:
            self.logger.warning(
                "OHLC no disponible para %s en fecha %s", option_id, date_str
            )
            self.ohlc_progress.update(option_id, target_date, "missing")
            return
        except Exception as exc:  # pylint: disable=broad-except
            raise RuntimeError(
                f"Fallo al descargar OHLC para {option_id} {date_str}"
            ) from exc

        try:
            ohlc_record = self._validate_ohlc(raw, option_id, symbol, asset_id)
        except ValueError as exc:
            self.logger.error(
                "Datos OHLC descartados para %s: %s | Datos: %s",
                option_id,
                exc,
                raw,
            )
            self.ohlc_progress.update(option_id, target_date, "invalid")
            return

        if ohlc_record.fecha_inicio.date() != target_date:
            self.logger.error(
                "Fecha OHLC inesperada para %s: esperado %s recibido %s",
                option_id,
                target_date,
                ohlc_record.fecha_inicio.date(),
            )
            self.ohlc_progress.update(option_id, target_date, "mismatch")
            return

        self._persist_ohlc(conn, cur, ohlc_record)
        conn.commit()
        self.ohlc_progress.update(option_id, target_date, "ok")

    @staticmethod
    def _open_close_up_to_date(
        cur: psycopg.Cursor,
        symbol: str,
        asset_id: str,
        option_id: str,
        target_date: _dt.date,
    ) -> bool:
        cur.execute(
            """
            SELECT fecha_inicio
            FROM opciones_ohlc
            WHERE symbol = %s AND asset_id = %s AND id_opcion = %s
            """,
            (symbol, asset_id, option_id),
        )
        row = cur.fetchone()
        if not row:
            return False
        stored_date = row[0]
        if isinstance(stored_date, _dt.datetime):
            stored_date = stored_date.date()
        elif isinstance(stored_date, str):
            try:
                stored_date = _dt.date.fromisoformat(stored_date)
            except ValueError:
                return False
        else:
            return False
        return stored_date >= target_date

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
        self, payload: Dict[str, Any], option_id: str, symbol: str, asset_id: str
    ) -> OptionOHLCRecord:
        option_symbol = payload.get("symbol")
        if option_symbol != option_id:
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
            id_opcion=option_id,
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
            symbol=symbol,
            asset_id=asset_id,
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
    ohlc_progress_tracker = OHLCProgressTracker(OHLC_PROGRESS_FILE)
    downloader = PolygonOptionsDownloader(
        api_key=api_key,
        assets=assets,
        db_settings=db_settings,
        logger=logger,
        progress_tracker=progress_tracker,
        ohlc_progress_tracker=ohlc_progress_tracker,
    )
    try:
        downloader.run()
    except KeyboardInterrupt:
        logger.warning("Descarga interrumpida por el usuario. Se conserva el progreso.")
    except Exception as exc:  # pylint: disable=broad-except
        logger.error("Fallo inesperado: %s", exc, exc_info=True)


if __name__ == "__main__":
    main()
