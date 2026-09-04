"""External runtime instrumentation for bot_v18_32_ULTIMATE_OPTIMIZED_FINAL.py.
Does not modify the source file. Run with the same environment as the bot.

Modes:
  AUDITOR_MODE=on  -> use embedded Auditor as source defines it
  AUDITOR_MODE=off -> disable Auditor after import, before bot start

The wrapper records handler/callback/webhook durations and emits JSONL.
"""
import os, sys, json, time, asyncio, inspect, importlib.util, logging, threading
from datetime import datetime, timezone

SOURCE = os.environ.get("V1832_SOURCE", "/mnt/data/files/bot_v18_32_ULTIMATE_OPTIMIZED_FINAL.py")
MODE = os.environ.get("AUDITOR_MODE", "on").lower()
LOG = os.environ.get("RUNTIME_DIAG_LOG", "runtime_v18_32_diagnostic.jsonl")

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | RUNTIME_DIAG | %(message)s")
logger = logging.getLogger("runtime_diag")

spec = importlib.util.spec_from_file_location("bot_v18_32_runtime", SOURCE)
if spec is None or spec.loader is None:
    raise RuntimeError(f"Cannot load source: {SOURCE}")
bot = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = bot
spec.loader.exec_module(bot)

if MODE == "off":
    try:
        if bot.PERFORMANCE_AUDITOR is not None:
            bot.PERFORMANCE_AUDITOR.stop()
    except Exception:
        logger.exception("failed stopping auditor")
    bot.PERFORMANCE_AUDITOR = None

LOCK = threading.Lock()

def emit(kind, **fields):
    rec = {"ts": datetime.now(timezone.utc).isoformat(), "kind": kind, "auditor": MODE}
    rec.update(fields)
    line = json.dumps(rec, ensure_ascii=False, default=str)
    with LOCK:
        with open(LOG, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    logger.info(line)


def wrap_async(name, fn):
    if not inspect.iscoroutinefunction(fn):
        return fn
    async def wrapped(*args, **kwargs):
        t0 = time.perf_counter()
        emit("handler_start", name=name)
        try:
            result = await fn(*args, **kwargs)
            return result
        except Exception as exc:
            emit("handler_error", name=name, elapsed_ms=round((time.perf_counter()-t0)*1000,2), error=repr(exc))
            raise
        finally:
            emit("handler_end", name=name, elapsed_ms=round((time.perf_counter()-t0)*1000,2), over_5s=(time.perf_counter()-t0)>5)
    wrapped.__name__ = getattr(fn, "__name__", name)
    wrapped.__qualname__ = getattr(fn, "__qualname__", name)
    return wrapped

HANDLERS = [
    "start","full_analysis","quick_analysis","trade_now","show_levels","trade_history",
    "gold_price","daily_analysis","weekly_analysis","weekly_report","daily_report",
    "news_status","markets","audit_report","status","subscribe","unsubscribe","plans",
    "plan_callback","subscription_request_callback","my_subscription","referral","admin_command",
    "home_menu","analyses_menu","trades_menu","market_news_menu","institutional_menu","liquidity_menu",
    "help_menu","router","callback_router",
]

for name in HANDLERS:
    fn = getattr(bot, name, None)
    if fn is not None:
        setattr(bot, name, wrap_async(name, fn))

# Re-wrap Flask endpoint from the actual registered view so Flask requests are measured.
orig_webhook = bot.app.view_functions.get("webhook")
if orig_webhook is not None:
    def webhook_probe(*args, **kwargs):
        t0=time.perf_counter(); emit("webhook_start")
        try:
            rv=orig_webhook(*args, **kwargs)
            return rv
        finally:
            emit("webhook_end", elapsed_ms=round((time.perf_counter()-t0)*1000,2), over_5s=(time.perf_counter()-t0)>5)
    bot.app.view_functions["webhook"] = webhook_probe

# Important: start_bot() resolves global handler names when registering, so wrapping above
# is sufficient for CommandHandler/CallbackQueryHandler/MessageHandler registration.

emit("diagnostic_boot", source=SOURCE, mode=MODE, pid=os.getpid())

try:
    bot.main()
finally:
    emit("diagnostic_exit")
