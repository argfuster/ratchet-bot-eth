# Bot Ratchet EMA50 (4h) — ETHUSDT, Binance Futures

Implementa la estrategia validada por backtest: régimen por EMA50 en velas de 4h,
SL = 50% del rango del período anterior (como orden STOP_MARKET real en el exchange),
reentrada por toque del nivel de SL (como orden STOP_MARKET pendiente).

## Antes de arrancar — leer esto

- **Nunca operada en papel ni en vivo antes de hoy.** El backtest es riguroso (7 años,
  precisión de 1 minuto, sin depender de crashes ni de pocas operaciones), pero no
  reemplaza la experiencia real. Considerá arrancar con `DRY_RUN=true` unos días para
  ver que la lógica haga lo esperado sin arriesgar capital, incluso si tu plan es ir
  a cuenta real pronto.
- **Apalancamiento**: el bot viene configurado en 3x por defecto (`LEVERAGE=3` en
  `.env.example`). El techo validado es 5x — para ETH específicamente, la evidencia
  del backtest mostró más variabilidad entre años a 5x (ver documento de la estrategia),
  así que 3-4x es más conservador.
- **API Key**: creá una key en Binance con permisos de **Futures** habilitados. No le
  des permiso de retiro (withdrawal) — el bot no lo necesita.
- **Nunca subas el archivo `.env`** con tus claves reales a git ni lo compartas.

## Instalación

```bash
pip install -r requirements.txt
cp .env.example .env
# editar .env con tus valores
```

## Correr localmente

```bash
python3 ratchet_bot.py
```

## Deploy en Railway (mismo patrón que usaste con bot_ema15m.py)

1. Subir este directorio a un repo de GitHub (sin el `.env`, agregalo a `.gitignore`).
2. Crear un nuevo proyecto en Railway, conectado a ese repo.
3. En el panel de Railway, cargar las variables de entorno (las mismas de `.env.example`,
   con tus valores reales) — Railway no usa el archivo `.env`, usa su propio panel.
4. Railway detecta `requirements.txt` y corre `python3 ratchet_bot.py` automáticamente
   (o configurar el Start Command manualmente si hace falta).

## Qué hace el bot en cada ciclo (cada `POLL_SECONDS`, default 30s)

1. Trae las últimas velas de 4h y calcula EMA50 + régimen (usando el cierre y la EMA
   del período **ya cerrado**, nunca el actual — mismo criterio sin look-ahead que el backtest).
2. Si hay posición abierta: chequea si el SL se ejecutó (pasa a esperar reentrada) o si
   arrancó un nuevo período de 4h (recalcula y reemplaza el SL en el exchange).
3. Si está esperando reentrada: chequea si la orden de reentrada se ejecutó, o si el
   régimen cambió de dirección (en ese caso cancela la espera y abre directo en la nueva dirección).
4. Si está plano: si el régimen indica una dirección, abre posición a mercado y coloca
   el SL inicial.

## Estado persistente

El archivo `ratchet_bot_state.json` guarda el estado actual (posición, SL, si está
esperando reentrada) para que el bot pueda recuperarse si se reinicia. Si necesitás
resetear el estado del bot desde cero, borrá ese archivo (pero primero asegurate de
que no haya posiciones/órdenes abiertas en el exchange que el bot fuera a perder de vista).

## Notificaciones

Si completás `TELEGRAM_BOT_TOKEN` y `TELEGRAM_CHAT_ID`, el bot manda un mensaje en
cada evento importante (apertura, cierre por SL, reentrada, cambios de régimen, errores).

## Limitaciones conocidas de esta primera versión

- Un solo símbolo por instancia del bot (correr dos instancias separadas para BTC+ETH
  si en algún momento querés ambos en paralelo).
- El polling es cada 30s por default — para esta estrategia (señal de 4h, SL manejado
  por el exchange) es más que suficiente; no hace falta un websocket de menor latencia.
- No incluye gestión de funding rate ni alertas de margen — revisar manualmente el
  panel de Binance con regularidad, sobre todo al principio.
