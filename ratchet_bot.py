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
import threading
import asyncio
from datetime import datetime, timezone, timedelta
from decimal import Decimal, ROUND_DOWN

import requests
from binance.client import Client
from binance.enums import *
from binance.exceptions import BinanceAPIException
from telegram.ext import Application, CommandHandler, ContextTypes
from telegram import Update

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
SL_MIN_PCT = float(os.environ.get("SL_MIN_PCT", "0.25"))  # piso minimo de distancia del SL respecto al precio de entrada
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
# FORMATO DE MENSAJES (estilo visual, con separadores y PnL con/sin leverage)
# ============================================================

SEP = "─" * 28

def fmt_open(direccion, entry_price, sl_price, sl_es_natural, capital, regimen_str):
    env = "🧪 TESTNET" if USE_TESTNET else "💰 REAL"
    dist_pct = abs(sl_price - entry_price) / entry_price * 100
    tipo_sl = "natural" if sl_es_natural else "piso aplicado"
    dir_str = "🟢 LONG" if direccion == "LONG" else "🔴 SHORT"
    return (
        f"{SEP}\n"
        f"⚡ ENTRADA RATCHET {env}\n"
        f"{SEP}\n"
        f"Par:      {SYMBOL}\n"
        f"Dir:      {dir_str}\n"
        f"Entry:    {entry_price:,.2f}\n"
        f"SL:       {sl_price:,.2f} (-{dist_pct:.2f}%, {tipo_sl})\n"
        f"Régimen:  {regimen_str}\n"
        f"Capital:  ${capital:,.2f} × {LEVERAGE}x\n"
        f"{SEP}"
    )

def fmt_close(motivo, direccion, entry_price, exit_price, estado_extra=""):
    """motivo: 'sl' | 'regimen' | 'manual'"""
    pnl_pct = (exit_price - entry_price) * (1 if direccion == "LONG" else -1) / entry_price * 100
    pnl_lev = pnl_pct * LEVERAGE
    titulos = {"sl": "🛑 SL tocado", "regimen": "🔀 Cambio de régimen", "manual": "✋ Cierre manual"}
    dir_str = "🟢 LONG" if direccion == "LONG" else "🔴 SHORT"
    msg = (
        f"{SEP}\n"
        f"{titulos.get(motivo, '📤 Cierre')}\n"
        f"{SEP}\n"
        f"Dir:      {dir_str}\n"
        f"Entry:    {entry_price:,.2f}\n"
        f"Exit:     {exit_price:,.2f}\n"
        f"PnL:      {pnl_pct:>+.3f}% (precio)\n"
        f"PnL:      {pnl_lev:>+.3f}% ({LEVERAGE}x capital)\n"
    )
    if estado_extra:
        msg += f"Estado:   {estado_extra}\n"
    msg += SEP
    return msg

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
        "sl_es_natural": True,      # True si el SL vigente es la formula pura, False si tiene piso/ratchet aplicado
        "current_period_open_time": None,   # timestamp (ms) del inicio del periodo de 4h actual
        "waiting_reentry": False,
        "reentry_order_id": None,
        "reentry_price": None,
        "reentry_dirn": None,       # "LONG" | "SHORT"
        "paused": False,            # si True, el bot no abre posiciones nuevas ni reentra (pero sigue protegiendo una posicion ya abierta si la hubiera)
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

def con_reintentos(func, max_intentos=6, espera_inicial=5, *args, no_reintentar=(), **kwargs):
    """Ejecuta func con reintentos y espera progresiva (5s, 10s, 20s, 40s...).
    Evita que un fallo transitorio de la API (rate limit, red, etc.) crashee
    el contenedor y dispare un loop de reinicios de Railway, que a su vez
    puede agravar un ban temporal de IP en Binance.
    Si max_intentos es None, reintenta INDEFINIDAMENTE (con techo de 5min entre
    intentos) en vez de terminar en excepcion -- usar solo para llamadas de arranque
    donde crashear el proceso seria peor que esperar mas tiempo.
    no_reintentar: tupla de tipos de excepcion que se relanzan de inmediato, SIN
    reintentar -- para errores donde reintentar la misma accion no tiene sentido
    (por ejemplo, colocar un SL que ya sabemos invalido porque la posicion se cerro)."""
    espera = espera_inicial
    intento = 0
    while True:
        intento += 1
        try:
            return func(*args, **kwargs)
        except no_reintentar:
            raise
        except Exception as e:
            if max_intentos is not None and intento >= max_intentos:
                log.error(f"Fallaron los {max_intentos} intentos de {func.__name__}: {e}")
                raise
            etiqueta = f"{intento}/{max_intentos}" if max_intentos is not None else f"{intento} (indefinido)"
            mensaje = f"Intento {etiqueta} de {func.__name__} fallo ({e}). Reintentando en {espera}s..."
            if espera >= 300:
                # ya llegamos al techo de espera -- avisar por Telegram cada tanto,
                # sin spamear en los reintentos rapidos del principio
                notify(mensaje, level="warning")
            else:
                log.warning(mensaje)
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

FILTROS = con_reintentos(obtener_filtros_simbolo, None, 5, SYMBOL)

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

def aplicar_piso_sl(sl_price, precio_referencia, direccion):
    """Si la distancia entre el SL natural y el precio de referencia es menor al
    piso minimo (SL_MIN_PCT), la reemplaza por esa distancia minima. Protege contra
    el caso donde una reentrada ocurre muy cerca del cierre de un periodo de 4h,
    dejando el SL natural practicamente pegado al precio de entrada (visto en vivo
    el 22-ago-2026: genero una cascada de ~22 aperturas/cierres en 3 horas).

    Tambien cubre el caso mas degenerado: si el precio se movio bastante DENTRO del
    mismo periodo de 4h desde que se fijo el open de referencia, el SL 'natural'
    (siempre anclado a ese open fijo, sin importar donde este el precio ahora) puede
    terminar directamente del LADO INCORRECTO del precio -- arriba en un largo, abajo
    en un corto (visto en vivo el 24-ago-2026).

    IMPORTANTE: precio_referencia debe ser el precio ACTUAL de mercado, NO el precio
    de entrada original de la posicion. Usar el entry_price original rompe el ratchet
    en posiciones que vienen ganando hace varios periodos: una vez que el SL legitimo
    supera la entrada original (exactamente el objetivo de un trailing stop que
    protege ganancias), comparar contra esa entrada vieja marca el avance real como
    'del lado incorrecto' y lo fuerza hacia abajo, deshaciendo ganancias ya aseguradas
    (bug real visto en vivo el 25-ago-2026: dos actualizaciones seguidas revirtieron
    un SL que habia subido a 2466.73 de vuelta a 2450.79, perdiendo ~16 puntos de
    proteccion ya ganada, porque 2466.73 superaba la entrada original aunque seguia
    perfectamente por debajo del precio de mercado vigente)."""
    if precio_referencia is None or precio_referencia == 0:
        return sl_price
    del_lado_incorrecto = (direccion == "LONG" and sl_price >= precio_referencia) or \
                           (direccion == "SHORT" and sl_price <= precio_referencia)
    dist_pct = abs(sl_price - precio_referencia) / precio_referencia * 100
    if del_lado_incorrecto or dist_pct < SL_MIN_PCT:
        if direccion == "LONG":
            return precio_referencia * (1 - SL_MIN_PCT/100)
        else:
            return precio_referencia * (1 + SL_MIN_PCT/100)
    return sl_price

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
    log.info(f"{direccion} abierto a mercado: {qty} {symbol} (precio ref {precio_referencia})")
    return orden

class PosicionCerradaAMercado(Exception):
    """Señal de que, al intentar colocar el SL, el precio ya habia cruzado el nivel
    (Binance rechazo con -2021) y se cerro la posicion a mercado en su lugar."""
    pass

class ReentradaYaDisparada(Exception):
    """Señal de que, al intentar colocar la orden PENDIENTE de reentrada, el precio ya
    habia superado ese nivel (Binance rechazo con -2021 la orden condicional) -- en ese
    caso, se abrio la posicion a MERCADO directamente en su lugar (el toque, en terminos
    de precio, ya ocurrio; solo faltaba que el bot lo reconociera)."""
    def __init__(self, msg, orden_mercado):
        super().__init__(msg)
        self.orden_mercado = orden_mercado

def colocar_sl(symbol, direccion, sl_price, qty):
    """Coloca un stop de cierre via la Algo Order API (obligatoria desde dic-2025 de
    Binance para todas las ordenes condicionales: STOP_MARKET, TAKE_PROFIT_MARKET, etc.
    El endpoint viejo /fapi/v1/order ya NO acepta estos tipos de orden -- da error -4120.

    Caso especial: si el precio ya cruzo el nivel de sl_price para el momento en que
    la orden llega al exchange (por ejemplo, un movimiento muy rapido justo al arrancar
    un periodo nuevo, antes de que el bot llegue a colocar el SL actualizado), Binance
    puede rechazar la orden con -2021 "Order would immediately trigger" en vez de
    ejecutarla. En ese caso, cerramos la posicion a mercado directamente y avisamos
    con una excepcion especifica, para que el llamador actualice el estado como
    corresponde (posicion cerrada), en vez de asumir que quedo un SL activo."""
    side = SIDE_SELL if direccion == "LONG" else SIDE_BUY
    sl_price = redondear_precio(sl_price)

    if DRY_RUN:
        log.info(f"[DRY_RUN] Colocaria SL algo-order {direccion} a {sl_price} ({qty} {symbol})")
        return {"algoId": f"dryrun_sl_{int(time.time())}"}

    try:
        orden = client.futures_create_algo_order(
            algoType="CONDITIONAL", symbol=symbol, side=side, type="STOP_MARKET",
            triggerPrice=sl_price, closePosition="true",
        )
        log.info(f"SL colocado ({direccion}) a {sl_price} [algoId={orden.get('algoId')}]")
        return orden
    except BinanceAPIException as e:
        if e.code == -2021:
            notify(f"⚠️ El nivel de SL ({sl_price:.2f}) ya fue superado por el precio antes de poder colocarlo. Cerrando la posicion a mercado.", level="warning")
            client.futures_create_order(symbol=symbol, side=side, type=ORDER_TYPE_MARKET, quantity=qty)
            raise PosicionCerradaAMercado(f"Posicion cerrada a mercado, el nivel {sl_price} ya estaba superado")
        raise

def colocar_orden_reentrada(symbol, direccion, nivel_precio):
    """Coloca un stop de apertura (algo order) que dispara una posicion nueva al tocar
    el nivel donde se toco el SL anterior — replica la reentrada por toque del backtest.

    Caso especial: si el precio ya supero ese nivel para el momento en que la orden
    llega al exchange (rebote muy rapido), Binance rechaza con -2021. En ese caso, el
    toque ya ocurrio en terminos de precio -- abrimos la posicion a MERCADO directamente
    y avisamos con una excepcion especifica, en vez de insistir con una orden pendiente
    que ya no tiene sentido (visto en vivo el 22-ago-2026: reintentar esto a ciegas
    crasheo el ciclo tras 6 intentos fallidos)."""
    side = SIDE_BUY if direccion == "LONG" else SIDE_SELL
    nivel_precio = redondear_precio(nivel_precio)
    precio_actual = obtener_precio_actual(symbol)
    qty = calcular_cantidad(symbol, precio_actual, POSITION_PCT_CAPITAL, LEVERAGE)

    if DRY_RUN:
        log.info(f"[DRY_RUN] Colocaria orden de reentrada algo-order {direccion} a {nivel_precio}")
        return {"algoId": f"dryrun_reentry_{int(time.time())}"}

    try:
        orden = client.futures_create_algo_order(
            algoType="CONDITIONAL", symbol=symbol, side=side, type="STOP_MARKET",
            triggerPrice=nivel_precio, quantity=qty,
        )
        notify(f"⏳ Orden de reentrada colocada ({direccion})\nNivel: {nivel_precio:.2f}")
        return orden
    except BinanceAPIException as e:
        if e.code == -2021:
            notify(f"⚡ El nivel de reentrada ({nivel_precio:.2f}) ya fue superado por el precio. Abriendo a mercado directamente.", level="warning")
            orden_mercado = client.futures_create_order(symbol=symbol, side=side, type=ORDER_TYPE_MARKET, quantity=qty)
            raise ReentradaYaDisparada(f"Reentrada abierta a mercado, el nivel {nivel_precio} ya estaba superado", orden_mercado)
        raise

def cancelar_orden(symbol, algo_id):
    if algo_id is None or (isinstance(algo_id, str) and algo_id.startswith("dryrun")):
        return
    try:
        client.futures_cancel_algo_order(symbol=symbol, algoId=algo_id)
    except BinanceAPIException as e:
        if e.code != -2011:  # -2011 = la orden ya no existe (ya se ejecuto/disparo o ya se cancelo)
            raise

def buscar_stop_existente(symbol):
    """Busca si ya hay una orden STOP_MARKET (algo order) abierta para el symbol
    (por ejemplo, colocada manualmente). Devuelve algoId y triggerPrice si existe."""
    try:
        ordenes = client.futures_get_open_algo_orders(symbol=symbol)
        lista = ordenes.get("orders", ordenes) if isinstance(ordenes, dict) else ordenes
        for o in lista:
            tipo = o.get("orderType") or o.get("type")
            if tipo == "STOP_MARKET":
                return {"algoId": o["algoId"], "triggerPrice": float(o.get("triggerPrice", o.get("price", 0)))}
    except BinanceAPIException as e:
        log.warning(f"No se pudo consultar ordenes algo abiertas: {e}")
    return None

def posicion_abierta(symbol):
    """Devuelve la cantidad neta de la posicion actual en el exchange (0 si esta plana)."""
    posiciones = client.futures_position_information(symbol=symbol)
    for p in posiciones:
        if p["symbol"] == symbol:
            return float(p["positionAmt"])
    return 0.0

def entry_price_real(symbol):
    """Devuelve el precio de entrada REAL (promedio) de la posicion segun el exchange
    -- Binance lo expone directamente en el mismo endpoint de posicion (entryPrice),
    no hace falta aproximarlo con el precio de mercado actual."""
    posiciones = client.futures_position_information(symbol=symbol)
    for p in posiciones:
        if p["symbol"] == symbol:
            ep = float(p.get("entryPrice", 0))
            return ep if ep > 0 else None
    return None

def sl_fue_tocado(symbol, direccion_esperada):
    """Chequeo robusto: en vez de parsear el status de la algo order (cuyo string
    exacto de 'disparada' no esta 100% documentado), consultamos directamente si la
    posicion en el exchange sigue existiendo en la direccion esperada. Si esta plana,
    el SL (que es la unica forma de cerrar una posicion en este bot) se ejecuto."""
    qty = posicion_abierta(symbol)
    if direccion_esperada == "LONG":
        return qty <= 0
    else:  # SHORT
        return qty >= 0

def reentrada_fue_tocada(symbol, direccion_esperada):
    """Igual que sl_fue_tocado pero para el caso inverso: estabamos planos esperando
    reentrada, y chequeamos si ya existe una posicion nueva en la direccion esperada."""
    qty = posicion_abierta(symbol)
    if direccion_esperada == "LONG":
        return qty > 0
    else:  # SHORT
        return qty < 0

# ============================================================
# LOOP PRINCIPAL
# ============================================================


def periodo_actual_ms():
    """Timestamp de inicio del periodo de 4h en curso (UTC), redondeado hacia abajo."""
    now = datetime.now(timezone.utc)
    hora_redondeada = (now.hour // 4) * 4
    inicio = now.replace(hour=hora_redondeada, minute=0, second=0, microsecond=0)
    return int(inicio.timestamp() * 1000)

def colocar_sl_seguro(symbol, direccion, sl_price, qty):
    """Wrapper robusto: antes de colocar el SL, chequea en el exchange (no solo en el
    estado local) si ya existe uno activo, y lo cancela primero si es asi. Evita el
    error -4130 (orden duplicada) cuando una cancelacion anterior fallo silenciosamente
    o hubo un delay de propagacion entre cancelar y crear."""
    existente = buscar_stop_existente(symbol)
    if existente is not None:
        con_reintentos(cancelar_orden, 5, 2, symbol, existente["algoId"])
        time.sleep(1)  # pequeño margen para que el exchange propague la cancelacion
    return colocar_sl(symbol, direccion, sl_price, qty)

def colocar_sl_o_pasar_a_espera(direccion, sl_price_calc, periodo_ms):
    """Intenta colocar el SL con reintentos normales. Si el nivel calculado ya estaba
    superado por el precio (PosicionCerradaAMercado -- la posicion se cerro de
    emergencia), NO reintenta colocar ese mismo SL invalido: en cambio, transiciona
    el estado a 'esperando reentrada', exactamente igual que un SL tocado normal.
    Esto corta el loop de apertura/cierre repetido que puede darse cuando una
    reentrada ocurre muy cerca del cierre de un periodo de 4h (el SL recalculado
    para el periodo actual puede terminar practicamente pegado al precio de entrada).
    Devuelve True si coloco el SL con exito, False si transiciono a espera de reentrada."""
    global estado
    qty = abs(posicion_abierta(SYMBOL))
    if qty == 0:
        notify("La posicion ya no existe en el exchange. Reseteando estado.", level="warning")
        estado = estado_default()
        guardar_estado(estado)
        return False

    # Verificar el SL previo REAL (exchange, no solo memoria del bot) ANTES de decidir
    # si corresponde aplicar el piso -- el orden importa: el backtest validado solo
    # aplica el piso la PRIMERA vez que se coloca el SL de una posicion (equivalente a
    # sl_actual=None), nunca en las actualizaciones posteriores vela a vela. Aplicarlo
    # tambien ahi (como hacia el bot antes de este fix) es una desviacion real de lo
    # validado, y fue la causa de fondo del bug del 25-ago-2026 (revirtio un avance
    # legitimo del ratchet).
    #
    # El "nivel ya vigente" se verifica contra el exchange, no solo contra la memoria
    # del bot -- si alguien cambio el SL manualmente en Binance (visto en vivo el
    # 25-ago-2026), estado["sl_price"] queda desincronizado de la realidad, y confiar
    # ciegamente en el podria dejar que el ratchet "mejore" hacia un nivel que en
    # realidad es un retroceso respecto al SL real ya colocado.
    sl_price_original = sl_price_calc
    sl_previo_estado = estado.get("sl_price")
    sl_previo_real = None
    existente = buscar_stop_existente(SYMBOL)
    if existente is not None:
        sl_previo_real = existente["triggerPrice"]
        if sl_previo_estado is not None and abs(sl_previo_real - sl_previo_estado) > 0.01:
            log.warning(f"Desincronizacion detectada: el bot creia SL={sl_previo_estado:.2f} pero el exchange tiene {sl_previo_real:.2f} (posible cambio manual). Usando el valor real del exchange.")
    # usar el mas protector entre lo que el bot cree y lo que el exchange realmente tiene
    if sl_previo_estado is not None and sl_previo_real is not None:
        sl_previo = max(sl_previo_estado, sl_previo_real) if direccion == "LONG" else min(sl_previo_estado, sl_previo_real)
    else:
        sl_previo = sl_previo_real if sl_previo_real is not None else sl_previo_estado

    if sl_previo is None:
        # primera vez que se coloca el SL de esta posicion (equivalente a sl_actual=None
        # en el backtest) -- aca si corresponde el piso, igual que en la reentrada validada.
        # Usa entry_price (no precio de mercado) para el calculo del piso: como esta
        # funcion ahora SOLO corre en la primera colocacion (nunca en actualizaciones
        # sobre una posicion ya ratcheteada), entry_price ya no puede estar "viejo" como
        # pasaba en el bug del 25-ago -- y usarlo da una distancia de piso EXACTA de
        # SL_MIN_PCT%, sin el ruido de los segundos que pasan entre que se ejecuta la
        # reentrada y se coloca el SL (con precio de mercado, ese pequeño desfasaje hacia
        # que el piso real terminara dando 0.13%-0.25% en vez de 0.25% fijo).
        entry_ref = estado.get("entry_price")
        sl_price_calc = aplicar_piso_sl(sl_price_calc, entry_ref, direccion)
        if sl_price_calc != sl_price_original:
            log.info(f"Piso de SL aplicado: {sl_price_original:.2f} -> {sl_price_calc:.2f} (distancia natural menor a {SL_MIN_PCT}% respecto al precio de entrada)")

    if sl_previo is not None:
        if direccion == "LONG" and sl_previo > sl_price_calc:
            log.info(f"Ratchet: manteniendo SL previo ({sl_previo:.2f}) en vez de retroceder a {sl_price_calc:.2f}")
            sl_price_calc = sl_previo
        elif direccion == "SHORT" and sl_previo < sl_price_calc:
            log.info(f"Ratchet: manteniendo SL previo ({sl_previo:.2f}) en vez de retroceder a {sl_price_calc:.2f}")
            sl_price_calc = sl_previo

    try:
        orden = con_reintentos(colocar_sl_seguro, 8, 3, SYMBOL, direccion, sl_price_calc, qty,
                                no_reintentar=(PosicionCerradaAMercado,))
        estado["sl_order_id"] = orden["algoId"]
        estado["sl_price"] = sl_price_calc
        estado["sl_es_natural"] = (sl_price_calc == sl_price_original)
        estado["current_period_open_time"] = periodo_ms
        guardar_estado(estado)
        return True
    except PosicionCerradaAMercado:
        notify(f"🔴 Posicion cerrada de emergencia (SL calculado invalido para este punto del periodo)\nPasando a espera de reentrada en {sl_price_original:.2f}", level="warning")
        estado["waiting_reentry"] = True
        estado["reentry_dirn"] = direccion
        estado["reentry_price"] = sl_price_original  # nivel NATURAL, no el ajustado por piso/ratchet
        estado["position"] = None
        estado["entry_price"] = None
        estado["sl_order_id"] = None
        guardar_estado(estado)
        return False

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
                notify(f"🛡️ SL existente detectado en el exchange (manual u otro origen)\nNivel: {existente['triggerPrice']:.2f}\nAdoptado, sin crear uno nuevo.")
                estado["sl_order_id"] = existente["algoId"]
                estado["sl_price"] = existente["triggerPrice"]
                estado["current_period_open_time"] = periodo_ms
                guardar_estado(estado)
                return

            notify(f"⚠️ ALERTA: posicion {estado['position']} sin SL colocado. Reintentando colocar SL de emergencia...", level="warning")
            try:
                if colocar_sl_o_pasar_a_espera(estado["position"], sl_price_calc, periodo_ms):
                    notify(f"✅ SL de emergencia colocado correctamente\nNivel: {estado['sl_price']:.2f}")
            except Exception as e:
                notify(f"🚨 CRITICO: no se pudo colocar el SL de emergencia tras varios reintentos: {e}. Requiere intervencion manual inmediata.", level="error")
            return

        # chequear si el SL se ejecuto (via el tamaño real de la posicion, no el status de la orden)
        if sl_fue_tocado(SYMBOL, estado["position"]):
            direccion_cerrada = estado["position"]
            entry_previo = estado["entry_price"]
            sl_previo = estado["sl_price"]
            nivel_reentrada = sl_price_calc  # nivel NATURAL (sin piso/ratchet) -- validado que da mejor resultado que reentrar en el nivel real donde cerro
            notify(fmt_close("sl", direccion_cerrada, entry_previo, sl_previo,
                              estado_extra=f"⏳ Esperando reentrada en {nivel_reentrada:.2f}"))
            estado["waiting_reentry"] = True
            estado["reentry_dirn"] = direccion_cerrada
            estado["reentry_price"] = nivel_reentrada
            estado["position"] = None
            estado["entry_price"] = None
            estado["sl_order_id"] = None
            guardar_estado(estado)
            # colocar orden de reentrada inmediatamente si el regimen sigue favoreciendo esa direccion
            if direccion_regimen == estado["reentry_dirn"]:
                try:
                    orden = con_reintentos(colocar_orden_reentrada, 6, 3, SYMBOL, estado["reentry_dirn"], estado["reentry_price"],
                                            no_reintentar=(ReentradaYaDisparada,))
                    estado["reentry_order_id"] = orden["algoId"]
                    guardar_estado(estado)
                except ReentradaYaDisparada as e:
                    # el toque ya ocurrio en precio -- la posicion se abrio a mercado dentro de colocar_orden_reentrada
                    notify(f"🟢 Reentrada ejecutada (a mercado, nivel ya superado)\n{estado['reentry_dirn']} @ ~{estado['reentry_price']:.2f}")
                    estado["position"] = estado["reentry_dirn"]
                    estado["entry_price"] = float(e.orden_mercado.get("avgPrice") or 0) or obtener_precio_actual(SYMBOL)
                    estado["waiting_reentry"] = False
                    estado["reentry_order_id"] = None
                    estado["sl_order_id"] = None
                    estado["sl_price"] = None  # nueva posicion -- no arrastrar el ratchet de la anterior
                    guardar_estado(estado)
                    if sl_price_calc is not None:
                        try:
                            colocar_sl_o_pasar_a_espera(estado["position"], sl_price_calc, periodo_ms)
                        except Exception as e2:
                            notify(f"⚠️ ALERTA: reentrada a mercado ejecutada pero el SL fallo: {e2}. Se reintentara el proximo ciclo.", level="error")
            return

        # si arranca un nuevo periodo de 4h, recalcular y reemplazar el SL
        if nuevo_periodo and sl_price_calc is not None and direccion_regimen == estado["position"]:
            sl_anterior = estado["sl_price"]
            if colocar_sl_o_pasar_a_espera(estado["position"], sl_price_calc, periodo_ms):
                notify(f"🔄 SL actualizado (nuevo periodo 4h)\n{sl_anterior:.2f} → {estado['sl_price']:.2f}")
        return

    # --- Caso 2: esperando reentrada ---
    if estado["waiting_reentry"]:
        # PRIORIDAD: si estamos esperando reentrada pero no hay ninguna orden pendiente
        # activa (por ejemplo, porque la colocacion fallo y crasheo el ciclo antes de
        # guardar el order_id -- visto en vivo el 22-ago-2026), reintentar colocarla
        # antes de cualquier otra cosa. Sin esto, el bot queda esperando en silencio
        # para siempre, sin ninguna orden que pueda dispararse.
        if estado["reentry_order_id"] is None and direccion_regimen == estado["reentry_dirn"] and not estado.get("paused", False):
            try:
                orden = con_reintentos(colocar_orden_reentrada, 6, 3, SYMBOL, estado["reentry_dirn"], estado["reentry_price"],
                                        no_reintentar=(ReentradaYaDisparada,))
                estado["reentry_order_id"] = orden["algoId"]
                guardar_estado(estado)
            except ReentradaYaDisparada as e:
                notify(f"🟢 Reentrada ejecutada (a mercado, nivel ya superado)\n{estado['reentry_dirn']} @ ~{estado['reentry_price']:.2f}")
                estado["position"] = estado["reentry_dirn"]
                estado["entry_price"] = float(e.orden_mercado.get("avgPrice") or 0) or obtener_precio_actual(SYMBOL)
                estado["waiting_reentry"] = False
                estado["reentry_order_id"] = None
                estado["sl_order_id"] = None
                estado["sl_price"] = None  # nueva posicion -- no arrastrar el ratchet de la anterior
                guardar_estado(estado)
                if sl_price_calc is not None:
                    try:
                        colocar_sl_o_pasar_a_espera(estado["position"], sl_price_calc, periodo_ms)
                    except Exception as e2:
                        notify(f"⚠️ ALERTA: reentrada a mercado ejecutada pero el SL fallo: {e2}. Se reintentara el proximo ciclo.", level="error")
            except Exception as e:
                notify(f"⚠️ ALERTA: no se pudo colocar la orden de reentrada tras varios intentos: {e}. Se reintentara el proximo ciclo.", level="error")
            return

        if reentrada_fue_tocada(SYMBOL, estado["reentry_dirn"]):
            log.info(f"Reentrada ejecutada {estado['reentry_dirn']} @ {estado['reentry_price']:.2f}")
            estado["position"] = estado["reentry_dirn"]
            estado["entry_price"] = estado["reentry_price"]
            estado["waiting_reentry"] = False
            estado["reentry_order_id"] = None
            estado["sl_order_id"] = None
            estado["sl_price"] = None  # nueva posicion -- no arrastrar el ratchet de la anterior
            guardar_estado(estado)  # guardar YA, antes de intentar el SL, por la misma razon que en Caso 3
            # colocar el SL del periodo actual
            if sl_price_calc is not None:
                try:
                    if colocar_sl_o_pasar_a_espera(estado["position"], sl_price_calc, periodo_ms):
                        try:
                            capital = obtener_capital_disponible()
                        except Exception:
                            capital = 0
                        regimen_str = f"{estado['position']} (EMA{EMA_SPAN})"
                        notify(fmt_open(estado["position"], estado["entry_price"], estado["sl_price"],
                                         estado["sl_es_natural"], capital, regimen_str))
                except Exception as e:
                    notify(f"⚠️ ALERTA: reentrada ejecutada pero el SL fallo: {e}. Se reintentara el proximo ciclo.", level="error")
            return

        # si el regimen cambio de direccion mientras esperabamos, cancelar la espera
        # y abrir directamente en la nueva direccion
        if direccion_regimen is not None and direccion_regimen != estado["reentry_dirn"]:
            notify(f"🔀 Regimen cambio: {estado['reentry_dirn']} → {direccion_regimen}\nCancelando espera de reentrada, abriendo en la nueva direccion.")
            con_reintentos(cancelar_orden, 5, 2, SYMBOL, estado["reentry_order_id"])
            estado["waiting_reentry"] = False
            estado["reentry_order_id"] = None
            guardar_estado(estado)
            # cae al caso 3 en el siguiente ciclo
        return

    # --- Caso 3: plano, sin espera activa -> evaluar entrada nueva ---
    if direccion_regimen is not None and not estado.get("paused", False):
        precio_actual = obtener_precio_actual(SYMBOL)
        orden_entrada = con_reintentos(abrir_posicion, 4, 3, SYMBOL, direccion_regimen, precio_actual)
        estado["position"] = direccion_regimen
        estado["entry_price"] = float(orden_entrada.get("avgPrice", precio_actual)) or precio_actual
        estado["sl_order_id"] = None  # todavia no colocado -- se guarda YA asi el proximo ciclo lo detecta si algo falla abajo
        estado["sl_price"] = None  # nueva posicion -- no arrastrar el ratchet de la anterior
        estado["current_period_open_time"] = periodo_ms
        guardar_estado(estado)
        log.info(f"Posicion abierta, guardando estado antes de intentar el SL...")

        if sl_price_calc is not None:
            try:
                if colocar_sl_o_pasar_a_espera(direccion_regimen, sl_price_calc, periodo_ms):
                    try:
                        capital = obtener_capital_disponible()
                    except Exception:
                        capital = 0
                    regimen_str = f"{direccion_regimen} (EMA{EMA_SPAN})"
                    notify(fmt_open(direccion_regimen, estado["entry_price"], estado["sl_price"],
                                     estado["sl_es_natural"], capital, regimen_str))
            except Exception as e:
                notify(f"⚠️ ALERTA: posicion abierta pero el SL fallo tras varios reintentos: {e}. Se reintentara en el proximo ciclo.", level="error")
                # no re-lanzamos: el proximo ciclo va a entrar por el chequeo de "sl_order_id is None" de arriba

def reconciliar_estado_inicial():
    """Al arrancar, reconcilia el estado local contra la realidad del exchange:
    1. Si hay una posicion abierta que el estado local no conoce, la adopta.
    2. Si NO hay posicion pero hay una orden condicional pendiente (de una reentrada
       previa) que el estado local no conoce, tambien la adopta como "esperando
       reentrada" -- evita que el bot abra una posicion nueva a mercado mientras esa
       orden vieja sigue viva y podria dispararse mas tarde, duplicando exposicion."""
    global estado
    qty_exchange = posicion_abierta(SYMBOL)

    if qty_exchange != 0 and estado["position"] is None:
        direccion = "LONG" if qty_exchange > 0 else "SHORT"
        notify(f"🔍 Posicion {direccion} existente detectada en el exchange ({qty_exchange})\nEl estado local no la conocia -- adoptando.", level="warning")
        estado["position"] = direccion
        try:
            entry_real = entry_price_real(SYMBOL)
            estado["entry_price"] = entry_real if entry_real is not None else obtener_precio_actual(SYMBOL)
        except Exception:
            pass
        existente = buscar_stop_existente(SYMBOL)
        if existente is not None:
            estado["sl_order_id"] = existente["algoId"]
            estado["sl_price"] = existente["triggerPrice"]
            notify(f"🛡️ SL existente detectado y adoptado\nNivel: {existente['triggerPrice']:.2f}")
        else:
            estado["sl_order_id"] = None
            notify("ALERTA: no se encontro SL para la posicion adoptada. Se intentara colocar uno en el proximo ciclo.", level="warning")
        estado["current_period_open_time"] = periodo_actual_ms()  # evita que el proximo ciclo crea que "cambio de periodo" y recalcule el SL sin necesidad
        guardar_estado(estado)
        return

    if qty_exchange == 0 and estado["position"] is None and not estado["waiting_reentry"]:
        # plano segun el exchange Y segun el estado local -- pero puede haber una orden
        # de reentrada vieja todavia pendiente que el estado local no recuerda
        pendiente = buscar_stop_existente(SYMBOL)
        if pendiente is not None:
            notify(f"🔍 Orden pendiente detectada en el exchange (nivel {pendiente['triggerPrice']:.2f}) sin posicion asociada.\nAdoptando como espera de reentrada, en vez de abrir una posicion nueva a mercado.", level="warning")
            estado["waiting_reentry"] = True
            estado["reentry_order_id"] = pendiente["algoId"]
            estado["reentry_price"] = pendiente["triggerPrice"]
            # no sabemos con certeza la direccion original de esa orden vieja sin mas
            # contexto -- la inferimos por el lado (BUY=LONG, SELL=SHORT) si esta disponible
            estado["reentry_dirn"] = None  # se completa abajo si es posible
            try:
                ordenes = client.futures_get_open_algo_orders(symbol=SYMBOL)
                lista = ordenes.get("orders", ordenes) if isinstance(ordenes, dict) else ordenes
                for o in lista:
                    if o.get("algoId") == pendiente["algoId"]:
                        estado["reentry_dirn"] = "LONG" if o.get("side") == "BUY" else "SHORT"
            except Exception:
                pass
            guardar_estado(estado)

# ============================================================
# COMANDOS INTERACTIVOS DE TELEGRAM
# ============================================================

estado_lock = threading.Lock()  # protege 'estado' entre el loop principal y los comandos de Telegram

async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    env = "🧪 TESTNET" if USE_TESTNET else "💰 REAL"
    await update.message.reply_text(
        f"🤖 Ratchet Bot EMA{EMA_SPAN} (4h) {env}\n\n"
        f"Par: {SYMBOL} · Lev: {LEVERAGE}x · SL={int(SL_FRACTION*100)}% del rango previo · Piso={SL_MIN_PCT}%\n\n"
        f"/status /close /reset /pause /resume /balance /help"
    )

async def cmd_help(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        "/start   — info del bot\n"
        "/status  — posición o espera activa\n"
        "/close   — cerrar posición manualmente\n"
        "/reset   — resetear estado (tras un cierre manual, evita reentradas fantasma)\n"
        "/pause   — pausar (no abre nuevas posiciones ni reentra)\n"
        "/resume  — reanudar tras /pause\n"
        "/balance — balance USDT\n"
        "/help    — esta ayuda"
    )

async def cmd_status(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    with estado_lock:
        snap = dict(estado)
    try:
        velas = obtener_klines_4h(SYMBOL, limit=300)
        regimen, sl_price_calc, _, _ = calcular_regimen_y_sl(velas)
        regimen_str = "LONG" if regimen == 1 else ("SHORT" if regimen == -1 else "sin definir")
    except Exception:
        regimen_str = "no disponible"

    if snap["position"] is None and not snap["waiting_reentry"]:
        pausa_str = "\n⏸️ PAUSADO" if snap.get("paused") else ""
        await update.message.reply_text(f"📭 Sin posición activa\nRégimen actual: {regimen_str} (EMA{EMA_SPAN}){pausa_str}")
        return

    if snap["waiting_reentry"]:
        await update.message.reply_text(
            f"⏳ Esperando reentrada\n"
            f"Dirección: {snap['reentry_dirn']}\n"
            f"Nivel: {snap['reentry_price']:.2f}\n"
            f"Régimen actual: {regimen_str} (EMA{EMA_SPAN})"
        )
        return

    try:
        mark = obtener_precio_actual(SYMBOL)
        dirn_num = 1 if snap["position"] == "LONG" else -1
        pnl = (mark - snap["entry_price"]) * dirn_num / snap["entry_price"] * 100
    except Exception:
        mark = pnl = 0

    # verificar el SL REAL contra el exchange, no solo confiar en la memoria del bot --
    # si alguien cambio el SL manualmente en Binance, snap["sl_price"] queda desincronizado
    sl_mostrado = snap["sl_price"]
    aviso_desync = ""
    try:
        existente = buscar_stop_existente(SYMBOL)
        if existente is not None:
            sl_real = existente["triggerPrice"]
            if snap["sl_price"] is None or abs(sl_real - snap["sl_price"]) > 0.01:
                aviso_desync = f"\n⚠️ El bot creía SL={snap['sl_price']:.2f} pero el exchange tiene {sl_real:.2f} -- mostrando el real."
                sl_mostrado = sl_real
        elif snap["sl_price"] is not None:
            aviso_desync = f"\n🚨 El bot cree que hay un SL en {snap['sl_price']:.2f} pero NO se encontró ninguna orden activa en el exchange."
    except Exception as e:
        aviso_desync = f"\n⚠️ No se pudo verificar el SL contra el exchange: {e}"

    dist_sl = abs(sl_mostrado - snap["entry_price"]) / snap["entry_price"] * 100 if sl_mostrado else 0
    coincide = "✅ coincide" if snap["position"] == regimen_str else "⚠️ no coincide"
    await update.message.reply_text(
        f"📊 Posición activa\n"
        f"Dir:    {'🟢' if snap['position']=='LONG' else '🔴'} {snap['position']}\n"
        f"Entry:  {snap['entry_price']:,.2f}\n"
        f"Mark:   {mark:,.2f}\n"
        f"PnL:    {pnl:+.2f}%\n"
        f"SL:     {sl_mostrado:,.2f} (-{dist_sl:.2f}%)\n"
        f"Régimen: {regimen_str} (EMA{EMA_SPAN}) {coincide}"
        f"{aviso_desync}"
    )

async def cmd_close(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    global estado
    with estado_lock:
        if estado["position"] is None:
            await update.message.reply_text("📭 Sin posición activa.")
            return
        direccion = estado["position"]
        entry_price = estado["entry_price"]
        sl_order_id = estado["sl_order_id"]
        try:
            cancelar_orden(SYMBOL, sl_order_id)
            side = SIDE_SELL if direccion == "LONG" else SIDE_BUY
            qty = abs(posicion_abierta(SYMBOL))
            if DRY_RUN:
                exit_price = obtener_precio_actual(SYMBOL)
            else:
                orden = client.futures_create_order(symbol=SYMBOL, side=side, type=ORDER_TYPE_MARKET, quantity=qty)
                exit_price = float(orden.get("avgPrice") or 0) or obtener_precio_actual(SYMBOL)
            estado = estado_default()
            guardar_estado(estado)
            await update.message.reply_text(fmt_close("manual", direccion, entry_price, exit_price))
        except Exception as e:
            await update.message.reply_text(f"❌ Error cerrando posición: {e}")

async def cmd_balance(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    try:
        bal = obtener_capital_disponible()
        await update.message.reply_text(f"💰 Balance: ${bal:,.2f} USDT")
    except Exception as e:
        await update.message.reply_text(f"❌ Error consultando balance: {e}")

async def cmd_reset(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """Resetea el estado del bot a 'plano', cancelando cualquier orden pendiente
    (SL o reentrada) que pudiera quedar viva en el exchange. Rechaza el reset si
    todavia hay una posicion REAL abierta -- resetear el estado sin cerrarla dejaria
    esa posicion sin ningun SL gestionado por el bot."""
    global estado
    with estado_lock:
        try:
            qty = posicion_abierta(SYMBOL)
        except Exception as e:
            await update.message.reply_text(f"❌ Error consultando el exchange: {e}")
            return
        if qty != 0:
            await update.message.reply_text(
                f"⚠️ Todavia hay una posicion real abierta ({qty} {SYMBOL}) en el exchange.\n"
                f"Usa /close primero para cerrarla -- resetear el estado ahora la dejaria sin SL gestionado."
            )
            return

        canceladas = []
        for campo in ("sl_order_id", "reentry_order_id"):
            oid = estado.get(campo)
            if oid:
                try:
                    con_reintentos(cancelar_orden, 3, 2, SYMBOL, oid)
                    canceladas.append(oid)
                except Exception as e:
                    log.warning(f"No se pudo cancelar orden {oid} durante /reset: {e}")
        # tambien barrer cualquier orden condicional que haya quedado suelta sin registrar en el estado
        try:
            suelta = buscar_stop_existente(SYMBOL)
            if suelta is not None:
                con_reintentos(cancelar_orden, 3, 2, SYMBOL, suelta["algoId"])
                canceladas.append(suelta["algoId"])
        except Exception as e:
            log.warning(f"No se pudo verificar/cancelar ordenes sueltas durante /reset: {e}")

        estado = estado_default()
        guardar_estado(estado)
        detalle = f" ({len(canceladas)} orden(es) pendiente(s) cancelada(s))" if canceladas else ""
        await update.message.reply_text(
            f"🔄 Estado reseteado -- el bot vuelve a evaluar el régimen desde cero en el próximo ciclo.{detalle}"
        )

async def cmd_pause(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """Pausa el bot: no va a abrir posiciones nuevas ni reentrar mientras este pausado.
    Si habia una orden de reentrada pendiente esperando, la cancela (si no, quedaria
    viva en el exchange y podria ejecutarse igual, sin que el bot lo gestione). Si hay
    una posicion REAL abierta, la deja intacta y sigue protegiendola con su SL normal
    -- pausar no la cierra, solo evita que se abra una posicion NUEVA despues."""
    global estado
    with estado_lock:
        if estado.get("paused"):
            await update.message.reply_text("⏸️ Ya estaba pausado.")
            return
        estado["paused"] = True
        aviso_extra = ""
        if estado["waiting_reentry"]:
            oid = estado.get("reentry_order_id")
            if oid:
                try:
                    con_reintentos(cancelar_orden, 3, 2, SYMBOL, oid)
                    aviso_extra = " (orden de reentrada pendiente cancelada)"
                except Exception as e:
                    log.warning(f"No se pudo cancelar orden de reentrada al pausar: {e}")
            estado["waiting_reentry"] = False
            estado["reentry_order_id"] = None
            estado["reentry_price"] = None
            estado["reentry_dirn"] = None
        guardar_estado(estado)
        nota_posicion = ""
        if estado["position"] is not None:
            nota_posicion = f"\n⚠️ Sigue habiendo una posición {estado['position']} abierta -- el bot la va a seguir protegiendo con su SL normal, pausar solo evita abrir una posición NUEVA después."
        await update.message.reply_text(f"⏸️ Bot pausado{aviso_extra} -- no va a abrir ni reentrar hasta /resume.{nota_posicion}")

async def cmd_resume(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    global estado
    with estado_lock:
        if not estado.get("paused"):
            await update.message.reply_text("▶️ No estaba pausado.")
            return
        estado["paused"] = False
        guardar_estado(estado)
        await update.message.reply_text("▶️ Bot reanudado -- vuelve a evaluar entradas en el próximo ciclo.")

def iniciar_telegram_listener():
    """Corre el listener de comandos de Telegram en su propio hilo y su propio event
    loop, en paralelo al loop de trading principal (que sigue siendo sincronico)."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        log.info("Telegram no configurado -- comandos interactivos deshabilitados.")
        return

    def _run():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
        app.add_handler(CommandHandler("start", cmd_start))
        app.add_handler(CommandHandler("help", cmd_help))
        app.add_handler(CommandHandler("status", cmd_status))
        app.add_handler(CommandHandler("close", cmd_close))
        app.add_handler(CommandHandler("reset", cmd_reset))
        app.add_handler(CommandHandler("pause", cmd_pause))
        app.add_handler(CommandHandler("resume", cmd_resume))
        app.add_handler(CommandHandler("balance", cmd_balance))
        log.info("Listener de comandos de Telegram iniciado.")
        app.run_polling(drop_pending_updates=True, close_loop=False, stop_signals=None)

    hilo = threading.Thread(target=_run, daemon=True)
    hilo.start()

def main():
    modo = "🧪 DRY RUN (simulacion)" if DRY_RUN else "💰 CUENTA REAL"
    notify(f"🤖 Bot iniciado\nSymbol: {SYMBOL} | Leverage: {LEVERAGE}x\nModo: {modo}")
    con_reintentos(configurar_leverage, None, 5, SYMBOL, LEVERAGE)
    con_reintentos(reconciliar_estado_inicial, None, 5)
    iniciar_telegram_listener()
    while True:
        try:
            with estado_lock:
                ciclo()
        except Exception as e:
            log.exception(f"Error en el ciclo principal: {e}")
            notify(f"🚨 ERROR en el bot: {e}", level="error")
            time.sleep(POLL_SECONDS * 4)  # ante un error, esperar mas antes de reintentar
            continue
        time.sleep(POLL_SECONDS)

if __name__ == "__main__":
    main()
