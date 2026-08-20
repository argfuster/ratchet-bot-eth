"""
Bot de trading: Estrategia Ratchet EMA50 (4h) con reentrada por toque
======================================================================

Activo: ETHUSDT (Binance Futures, USDT-M)
Timeframe de señal: 4 horas
Régimen: EMA50 sobre cierres de 4h (posición del cierre del período anterior)
SL: 50% del rango (high-low) del período de 4h anterior, fijo desde el open del
    período actual, chequeado por el exchange via orden STOP_MARKET real.
Reentrada: orden STOP_MARKET pendiente en el nivel exacto del SL que se tocó,
    misma dirección, mientras el régimen se mantenga.

IMPORTANTE — LEER ANTES DE CORRER EN CUENTA REAL:
- Esta estrategia está validada por backtest (histórico 2020-2026, ambos activos,
  precisión de ejecución de 1 minuto), pero NUNCA fue operada en papel ni en vivo.
- Recomendación: arrancar con DRY_RUN=true y/o tamaño de posición mínimo antes de
  confiar capital significativo, incluso si tu intención es ir a cuenta real ya.
- Apalancamiento recomendado: máximo 5x (ver documento de la estrategia). Para ETH
  específicamente, considerar 3-4x dado que mostró más variabilidad entre años.
- El bot NUNCA debería correr con las API keys en el código. Usa variables de entorno.
"""

import os
import time
import json
import logging
import math
from datetime import datetime, timezone, timedelta
from decimal import Decimal, ROUND_DOWN

import requests
from binance.client import Client
from binance.enums import *
from binance.exceptions import BinanceAPIException

try:
    from dotenv import load_dotenv
    load_dotenv()  # carga .env si existe (util para correr local; en Railway se ignora, usa las env vars del panel)
except ImportError:
    pass

# ============================================================
# CONFIGURACIÓN (todo por variable de entorno, nada hardcodeado)
# ============================================================

API_KEY = os.environ.get("BINANCE_API_KEY")
API_SECRET = os.environ.get("BINANCE_API_SECRET")
USE_TESTNET = os.environ.get("BINANCE_TESTNET", "false").lower() == "true"
DRY_RUN = os.environ.get("DRY_RUN", "false").lower() == "true"

SYMBOL = os.environ.get("SYMBOL", "ETHUSDT")
LEVERAGE = int(os.environ.get("LEVERAGE", "3"))          # techo recomendado: 5x (ETH: 3-4x)
POSITION_PCT_CAPITAL = float(os.environ.get("POSITION_PCT_CAPITAL", "0.95"))  # % del capital disponible a usar
EMA_SPAN = int(os.environ.get("EMA_SPAN", "50"))
SL_FRACTION = float(os.environ.get("SL_FRACTION", "0.5"))  # 50% del rango del período anterior
KLINE_INTERVAL = "4h"
POLL_SECONDS = int(os.environ.get("POLL_SECONDS", "30"))

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")

STATE_FILE = os.environ.get("STATE_FILE", "ratchet_bot_state.json")
LOG_FILE = os.environ.get("LOG_FILE", "ratchet_bot.log")

# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler()]
)
log = logging.getLogger("ratchet_bot")

def notify(msg: str, level: str = "info"):
    """Loguea y manda a Telegram si esta configurado."""
    getattr(log, level, log.info)(msg)
    if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
        try:
            url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
            requests.post(url, data={"chat_id": TELEGRAM_CHAT_ID, "text": f"[RatchetBot {SYMBOL}] {msg}"}, timeout=10)
        except Exception as e:
            log.warning(f"No se pudo enviar notificacion a Telegram: {e}")

# ============================================================
# CLIENTE BINANCE
# ============================================================

if not API_KEY or not API_SECRET:
    raise SystemExit("Faltan BINANCE_API_KEY / BINANCE_API_SECRET como variables de entorno. Abortando.")

client = Client(API_KEY, API_SECRET, testnet=USE_TESTNET)

if USE_TESTNET:
    log.info("Conectado a TESTNET de Binance Futures.")
else:
    log.warning("Conectado a CUENTA REAL de Binance Futures. Operando con dinero real.")

if DRY_RUN:
    log.warning("DRY_RUN activo: no se van a enviar ordenes reales, solo se simula la logica.")

# ============================================================
# ESTADO PERSISTENTE
# ============================================================

def estado_default():
    return {
        "position": None,          # None | "LONG" | "SHORT"
        "entry_price": None,
        "sl_order_id": None,
        "sl_price": None,
        "current_period_open_time": None,   # timestamp (ms) del inicio del periodo de 4h actual
        "waiting_reentry": False,
        "reentry_order_id": None,
        "reentry_price": None,
        "reentry_dirn": None,       # "LONG" | "SHORT"
    }

def cargar_estado():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE) as f:
            return json.load(f)
    return estado_default()

def guardar_estado(estado):
    with open(STATE_FILE, "w") as f:
        json.dump(estado, f, indent=2)

estado = cargar_estado()

# ============================================================
# INFO DEL SIMBOLO (precision de precio/cantidad)
# ============================================================

def con_reintentos(func, max_intentos=6, espera_inicial=5, *args, **kwargs):
    """Ejecuta func con reintentos y espera progresiva (5s, 10s, 20s, 40s...).
    Evita que un fallo transitorio de la API (rate limit, red, etc.) crashee
    el contenedor y dispare un loop de reinicios de Railway, que a su vez
    puede agravar un ban temporal de IP en Binance."""
    espera = espera_inicial
    for intento in range(1, max_intentos + 1):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            if intento == max_intentos:
                log.error(f"Fallaron los {max_intentos} intentos de {func.__name__}: {e}")
                raise
            log.warning(f"Intento {intento}/{max_intentos} de {func.__name__} fallo ({e}). Reintentando en {espera}s...")
            time.sleep(espera)
            espera = min(espera * 2, 300)  # tope de 5 minutos entre reintentos

def obtener_filtros_simbolo(symbol):
    info = client.futures_exchange_info()
    for s in info["symbols"]:
        if s["symbol"] == symbol:
            price_filter = next(f for f in s["filters"] if f["filterType"] == "PRICE_FILTER")
            lot_filter = next(f for f in s["filters"] if f["filterType"] == "LOT_SIZE")
            return {
                "tick_size": float(price_filter["tickSize"]),
                "step_size": float(lot_filter["stepSize"]),
                "min_qty": float(lot_filter["minQty"]),
            }
    raise ValueError(f"Simbolo {symbol} no encontrado")

FILTROS = con_reintentos(obtener_filtros_simbolo, 6, 5, SYMBOL)

def redondear_precio(price):
    tick = FILTROS["tick_size"]
    return float(Decimal(str(price)).quantize(Decimal(str(tick)), rounding=ROUND_DOWN))

def redondear_cantidad(qty):
    step = FILTROS["step_size"]
    return float(Decimal(str(qty)).quantize(Decimal(str(step)), rounding=ROUND_DOWN))

# ============================================================
# DATOS DE MERCADO Y CALCULO DE LA SEÑAL (misma logica que el backtest)
# ============================================================

def obtener_klines_4h(symbol, limit=300):
    """Trae velas de 4h. limit=300 da ~50 dias de historia, mas que suficiente para
    que la EMA50 converja (en la practica converge razonablemente en ~3-5x el span)."""
    raw = client.futures_klines(symbol=symbol, interval=KLINE_INTERVAL, limit=limit)
    velas = []
    for k in raw:
        velas.append({
            "open_time": k[0], "open": float(k[1]), "high": float(k[2]),
            "low": float(k[3]), "close": float(k[4]), "close_time": k[6],
        })
    return velas

def calcular_ema(closes, span):
    alpha = 2 / (span + 1)
    ema = [closes[0]]
    for c in closes[1:]:
        ema.append(alpha * c + (1 - alpha) * ema[-1])
    return ema

def calcular_regimen_y_sl(velas):
    """
    Replica exacto la logica del backtest:
    - regimen = 1 (largo) si cierre del periodo anterior > EMA50 anterior; -1 si <
    - sl_price del periodo ACTUAL (el ultimo de la lista, aun en curso) = open actual
      -+ SL_FRACTION * rango del periodo anterior (el que ya cerro).
    Devuelve: (regimen, sl_price_para_periodo_actual, vela_actual, vela_anterior)
    """
    closes = [v["close"] for v in velas]
    ema = calcular_ema(closes, EMA_SPAN)

    vela_actual = velas[-1]      # el periodo de 4h en curso (aun no cerro)
    vela_anterior = velas[-2]    # el ultimo periodo YA CERRADO

    close_anterior = vela_anterior["close"]
    ema_anterior = ema[-2]
    regimen = 1 if close_anterior > ema_anterior else (-1 if close_anterior < ema_anterior else 0)

    rango_anterior = vela_anterior["high"] - vela_anterior["low"]
    open_actual = vela_actual["open"]
    if regimen == 1:
        sl_price = open_actual - SL_FRACTION * rango_anterior
    elif regimen == -1:
        sl_price = open_actual + SL_FRACTION * rango_anterior
    else:
        sl_price = None

    return regimen, sl_price, vela_actual, vela_anterior

# ============================================================
# EJECUCION DE ORDENES
# ============================================================

def obtener_precio_actual(symbol):
    return float(client.futures_mark_price(symbol=symbol)["markPrice"])

def obtener_capital_disponible():
    balances = client.futures_account_balance()
    usdt = next(b for b in balances if b["asset"] == "USDT")
    return float(usdt["availableBalance"])

def calcular_cantidad(symbol, precio, capital_pct, leverage):
    capital = obtener_capital_disponible()
    notional = capital * capital_pct * leverage
    qty = notional / precio
    qty = redondear_cantidad(qty)
    if qty < FILTROS["min_qty"]:
        raise ValueError(f"Cantidad calculada ({qty}) menor al minimo permitido ({FILTROS['min_qty']})")
    return qty

def configurar_leverage(symbol, leverage):
    if DRY_RUN:
        log.info(f"[DRY_RUN] Configuraria leverage {leverage}x para {symbol}")
        return
    client.futures_change_leverage(symbol=symbol, leverage=leverage)

def abrir_posicion(symbol, direccion, precio_referencia):
    """direccion: 'LONG' o 'SHORT'. Abre a mercado."""
    side = SIDE_BUY if direccion == "LONG" else SIDE_SELL
    qty = calcular_cantidad(symbol, precio_referencia, POSITION_PCT_CAPITAL, LEVERAGE)

    if DRY_RUN:
        log.info(f"[DRY_RUN] Abriria {direccion} {qty} {symbol} a mercado (precio ref {precio_referencia})")
        return {"avgPrice": precio_referencia, "executedQty": qty}

    orden = client.futures_create_order(
        symbol=symbol, side=side, type=ORDER_TYPE_MARKET, quantity=qty
    )
    notify(f"Posicion {direccion} abierta: {qty} {symbol} (orden {orden['orderId']})")
    return orden

def colocar_sl(symbol, direccion, sl_price, qty):
    """Coloca un STOP_MARKET real en el exchange, en reduceOnly (cierra la posicion)."""
    side = SIDE_SELL if direccion == "LONG" else SIDE_BUY
    sl_price = redondear_precio(sl_price)

    if DRY_RUN:
        log.info(f"[DRY_RUN] Colocaria SL {direccion} a {sl_price} ({qty} {symbol})")
        return {"orderId": f"dryrun_sl_{int(time.time())}"}

    orden = client.futures_create_order(
        symbol=symbol, side=side, type=FUTURE_ORDER_TYPE_STOP_MARKET,
        stopPrice=sl_price, closePosition=True,
    )
    notify(f"SL colocado ({direccion}) a {sl_price}")
    return orden

def colocar_orden_reentrada(symbol, direccion, nivel_precio):
    """Coloca una orden STOP_MARKET pendiente que abre posicion nueva al tocar el nivel
    donde se toco el SL anterior — replica la reentrada por toque del backtest."""
    side = SIDE_BUY if direccion == "LONG" else SIDE_SELL
    nivel_precio = redondear_precio(nivel_precio)
    precio_actual = obtener_precio_actual(symbol)
    qty = calcular_cantidad(symbol, precio_actual, POSITION_PCT_CAPITAL, LEVERAGE)

    if DRY_RUN:
        log.info(f"[DRY_RUN] Colocaria orden de reentrada {direccion} a {nivel_precio}")
        return {"orderId": f"dryrun_reentry_{int(time.time())}"}

    orden = client.futures_create_order(
        symbol=symbol, side=side, type=FUTURE_ORDER_TYPE_STOP_MARKET,
        stopPrice=nivel_precio, quantity=qty,
    )
    notify(f"Orden de reentrada colocada ({direccion}) a {nivel_precio}")
    return orden

def cancelar_orden(symbol, order_id):
    if order_id is None or (isinstance(order_id, str) and order_id.startswith("dryrun")):
        return
    try:
        client.futures_cancel_order(symbol=symbol, orderId=order_id)
    except BinanceAPIException as e:
        if e.code != -2011:  # -2011 = la orden ya no existe (ya se ejecuto o ya se cancelo)
            raise

def orden_ejecutada(symbol, order_id):
    """True si la orden ya se ejecuto (filled)."""
    if order_id is None:
        return False
    if isinstance(order_id, str) and order_id.startswith("dryrun"):
        return False  # en dry run nunca se auto-ejecuta, se simula aparte
    try:
        orden = client.futures_get_order(symbol=symbol, orderId=order_id)
        return orden["status"] == "FILLED"
    except BinanceAPIException:
        return False

def buscar_stop_existente(symbol):
    """Busca si ya hay una orden STOP_MARKET abierta para el symbol (por ejemplo,
    colocada manualmente). Devuelve el orderId y stopPrice si existe, o None."""
    try:
        ordenes = client.futures_get_open_orders(symbol=symbol)
        for o in ordenes:
            if o["type"] == "STOP_MARKET":
                return {"orderId": o["orderId"], "stopPrice": float(o["stopPrice"])}
    except BinanceAPIException as e:
        log.warning(f"No se pudo consultar ordenes abiertas: {e}")
    return None

def posicion_abierta(symbol):
    """Devuelve la cantidad neta de la posicion actual en el exchange (0 si esta plana)."""
    posiciones = client.futures_position_information(symbol=symbol)
    for p in posiciones:
        if p["symbol"] == symbol:
            return float(p["positionAmt"])
    return 0.0

# ============================================================
# LOOP PRINCIPAL
# ============================================================

def periodo_actual_ms():
    """Timestamp de inicio del periodo de 4h en curso (UTC), redondeado hacia abajo."""
    now = datetime.now(timezone.utc)
    hora_redondeada = (now.hour // 4) * 4
    inicio = now.replace(hour=hora_redondeada, minute=0, second=0, microsecond=0)
    return int(inicio.timestamp() * 1000)

def ciclo():
    global estado
    velas = obtener_klines_4h(SYMBOL, limit=300)
    regimen, sl_price_calc, vela_actual, vela_anterior = calcular_regimen_y_sl(velas)
    periodo_ms = periodo_actual_ms()
    nuevo_periodo = estado["current_period_open_time"] != periodo_ms

    direccion_regimen = "LONG" if regimen == 1 else ("SHORT" if regimen == -1 else None)

    # --- Caso 1: en posicion ---
    if estado["position"] is not None:
        # PRIORIDAD MAXIMA: si hay posicion pero no hay SL colocado (por ejemplo, porque
        # la colocacion del SL fallo despues de abrir la posicion), reintentar el SL
        # antes de cualquier otra cosa -- una posicion sin SL es el estado mas peligroso.
        if estado["sl_order_id"] is None and sl_price_calc is not None:
            # primero chequear si ya existe un SL puesto manualmente en el exchange,
            # antes de crear uno nuevo y duplicarlo
            existente = buscar_stop_existente(SYMBOL)
            if existente is not None:
                notify(f"Se encontro un SL ya existente en el exchange (colocado manualmente u otro origen) a {existente['stopPrice']}. Adoptando ese, sin crear uno nuevo.")
                estado["sl_order_id"] = existente["orderId"]
                estado["sl_price"] = existente["stopPrice"]
                estado["current_period_open_time"] = periodo_ms
                guardar_estado(estado)
                return

            notify(f"ALERTA: posicion {estado['position']} sin SL colocado. Reintentando colocar SL de emergencia...", level="warning")
            try:
                qty = abs(posicion_abierta(SYMBOL))
                if qty > 0:
                    orden_sl = con_reintentos(colocar_sl, 8, 3, SYMBOL, estado["position"], sl_price_calc, qty)
                    estado["sl_order_id"] = orden_sl["orderId"]
                    estado["sl_price"] = sl_price_calc
                    estado["current_period_open_time"] = periodo_ms
                    guardar_estado(estado)
                    notify(f"SL de emergencia colocado correctamente a {sl_price_calc}")
                else:
                    notify("La posicion ya no existe en el exchange (se debe haber cerrado manualmente). Reseteando estado.", level="warning")
                    estado = estado_default()
                    guardar_estado(estado)
            except Exception as e:
                notify(f"CRITICO: no se pudo colocar el SL de emergencia tras varios reintentos: {e}. Requiere intervencion manual inmediata.", level="error")
            return

        # chequear si el SL se ejecuto
        if orden_ejecutada(SYMBOL, estado["sl_order_id"]) or (DRY_RUN and False):
            notify(f"SL tocado en {estado['position']} @ {estado['sl_price']}. Pasando a espera de reentrada.")
            estado["waiting_reentry"] = True
            estado["reentry_dirn"] = estado["position"]
            estado["reentry_price"] = estado["sl_price"]
            estado["position"] = None
            estado["entry_price"] = None
            estado["sl_order_id"] = None
            guardar_estado(estado)
            # colocar orden de reentrada inmediatamente si el regimen sigue favoreciendo esa direccion
            if direccion_regimen == estado["reentry_dirn"]:
                orden = colocar_orden_reentrada(SYMBOL, estado["reentry_dirn"], estado["reentry_price"])
                estado["reentry_order_id"] = orden["orderId"]
                guardar_estado(estado)
            return

        # si arranca un nuevo periodo de 4h, recalcular y reemplazar el SL
        if nuevo_periodo and sl_price_calc is not None and direccion_regimen == estado["position"]:
            cancelar_orden(SYMBOL, estado["sl_order_id"])
            qty = abs(posicion_abierta(SYMBOL))
            orden = colocar_sl(SYMBOL, estado["position"], sl_price_calc, qty)
            estado["sl_order_id"] = orden["orderId"]
            estado["sl_price"] = sl_price_calc
            estado["current_period_open_time"] = periodo_ms
            guardar_estado(estado)
            notify(f"SL actualizado para nuevo periodo de 4h: {sl_price_calc}")
        return

    # --- Caso 2: esperando reentrada ---
    if estado["waiting_reentry"]:
        if orden_ejecutada(SYMBOL, estado["reentry_order_id"]):
            notify(f"Reentrada ejecutada: {estado['reentry_dirn']} @ {estado['reentry_price']}")
            estado["position"] = estado["reentry_dirn"]
            estado["entry_price"] = estado["reentry_price"]
            estado["waiting_reentry"] = False
            estado["reentry_order_id"] = None
            # colocar el SL del periodo actual
            if sl_price_calc is not None:
                qty = abs(posicion_abierta(SYMBOL))
                orden = colocar_sl(SYMBOL, estado["position"], sl_price_calc, qty)
                estado["sl_order_id"] = orden["orderId"]
                estado["sl_price"] = sl_price_calc
                estado["current_period_open_time"] = periodo_ms
            guardar_estado(estado)
            return

        # si el regimen cambio de direccion mientras esperabamos, cancelar la espera
        # y abrir directamente en la nueva direccion
        if direccion_regimen is not None and direccion_regimen != estado["reentry_dirn"]:
            notify(f"Regimen cambio de {estado['reentry_dirn']} a {direccion_regimen} mientras se esperaba reentrada. Cancelando espera y abriendo en la nueva direccion.")
            cancelar_orden(SYMBOL, estado["reentry_order_id"])
            estado["waiting_reentry"] = False
            estado["reentry_order_id"] = None
            guardar_estado(estado)
            # cae al caso 3 en el siguiente ciclo
        return

    # --- Caso 3: plano, sin espera activa -> evaluar entrada nueva ---
    if direccion_regimen is not None:
        precio_actual = obtener_precio_actual(SYMBOL)
        orden_entrada = con_reintentos(abrir_posicion, 4, 3, SYMBOL, direccion_regimen, precio_actual)
        estado["position"] = direccion_regimen
        estado["entry_price"] = float(orden_entrada.get("avgPrice", precio_actual)) or precio_actual
        estado["sl_order_id"] = None  # todavia no colocado -- se guarda YA asi el proximo ciclo lo detecta si algo falla abajo
        estado["current_period_open_time"] = periodo_ms
        guardar_estado(estado)
        notify(f"Posicion abierta, guardando estado antes de intentar el SL...")

        if sl_price_calc is not None:
            try:
                qty = abs(posicion_abierta(SYMBOL)) if not DRY_RUN else float(orden_entrada.get("executedQty", 0))
                orden_sl = con_reintentos(colocar_sl, 8, 3, SYMBOL, direccion_regimen, sl_price_calc, qty)
                estado["sl_order_id"] = orden_sl["orderId"]
                estado["sl_price"] = sl_price_calc
                guardar_estado(estado)
            except Exception as e:
                notify(f"ALERTA: posicion abierta pero el SL fallo tras varios reintentos: {e}. Se reintentara en el proximo ciclo.", level="error")
                # no re-lanzamos: el proximo ciclo va a entrar por el chequeo de "sl_order_id is None" de arriba

def reconciliar_estado_inicial():
    """Al arrancar, si hay una posicion abierta en el exchange que el estado local
    no conoce (por ejemplo, por un crash antes de guardar estado), la adopta en vez
    de intentar abrir una posicion nueva y duplicar exposicion."""
    global estado
    qty_exchange = posicion_abierta(SYMBOL)
    if qty_exchange != 0 and estado["position"] is None:
        direccion = "LONG" if qty_exchange > 0 else "SHORT"
        notify(f"Se detecto una posicion {direccion} existente en el exchange ({qty_exchange}) que el estado local no conocia. Adoptando.", level="warning")
        estado["position"] = direccion
        try:
            precio_actual = obtener_precio_actual(SYMBOL)
            estado["entry_price"] = precio_actual  # aproximado; no tenemos el entry real si no lo guardamos antes
        except Exception:
            pass
        existente = buscar_stop_existente(SYMBOL)
        if existente is not None:
            estado["sl_order_id"] = existente["orderId"]
            estado["sl_price"] = existente["stopPrice"]
            notify(f"SL existente detectado y adoptado: {existente['stopPrice']}")
        else:
            estado["sl_order_id"] = None
            notify("ALERTA: no se encontro SL para la posicion adoptada. Se intentara colocar uno en el proximo ciclo.", level="warning")
        guardar_estado(estado)

def main():
    notify(f"Bot iniciado. Symbol={SYMBOL} Leverage={LEVERAGE}x DryRun={DRY_RUN} Testnet={USE_TESTNET}")
    con_reintentos(configurar_leverage, 6, 5, SYMBOL, LEVERAGE)
    con_reintentos(reconciliar_estado_inicial, 6, 5)
    while True:
        try:
            ciclo()
        except Exception as e:
            log.exception(f"Error en el ciclo principal: {e}")
            notify(f"ERROR en el bot: {e}", level="error")
            time.sleep(POLL_SECONDS * 4)  # ante un error, esperar mas antes de reintentar
            continue
        time.sleep(POLL_SECONDS)

if __name__ == "__main__":
    main()
