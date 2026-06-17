"""
RAZALI — Local launcher
Reads creds.json + local_config.py, sets env vars, starts the server.
Run with:  python run_local.py
"""
import os, json, sys, subprocess

# ── Load Google creds from file ───────────────────────────────
creds_path = os.path.join(os.path.dirname(__file__), "creds.json")
if not os.path.exists(creds_path):
    print("ERROR: creds.json not found next to this script.")
    sys.exit(1)
with open(creds_path, encoding="utf-8") as f:
    os.environ["GOOGLE_CREDS"] = f.read()

# ── Load local_config.py ──────────────────────────────────────
try:
    import local_config as cfg
except ImportError:
    print("ERROR: local_config.py not found. Create it first.")
    sys.exit(1)

os.environ["REDIS_URL"]               = cfg.REDIS_URL
os.environ["TELEGRAM_BOT_TOKEN"]      = cfg.TELEGRAM_BOT_TOKEN
os.environ["TELEGRAM_CHAT_ID"]        = cfg.TELEGRAM_CHAT_ID
os.environ["TWILIO_ACCOUNT_SID"]      = cfg.TWILIO_ACCOUNT_SID
os.environ["TWILIO_AUTH_TOKEN"]       = cfg.TWILIO_AUTH_TOKEN
os.environ["TWILIO_WHATSAPP_NUMBER"]  = cfg.TWILIO_WHATSAPP_NUMBER
os.environ["SALON_PHONE"]             = cfg.SALON_PHONE
os.environ["BOOKING_URL"]             = cfg.BOOKING_URL

# ── Start server ──────────────────────────────────────────────
print("\n🚀  Starting RAZALI locally at http://localhost:8000")
print("    Booking page → http://localhost:8000/book")
print("    Press Ctrl+C to stop\n")

subprocess.run([
    sys.executable, "-m", "uvicorn",
    "razali_main:app",
    "--host", "0.0.0.0",
    "--port", "8000",
    "--reload"   # auto-restarts when you save razali_main.py
])
