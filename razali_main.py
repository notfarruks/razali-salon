"""
RAZALI — Complete Backend
Serves:
  POST /whatsapp        — Twilio WhatsApp webhook (greeter only)
  GET  /api/data        — services, masters, categories for the website
  GET  /api/slots       — available time slots for a master+date
  POST /api/book        — create booking from website
  GET  /book            — serves the booking HTML page
"""

import logging, sys, httpx, json, os, uuid, asyncio
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from pathlib import Path

from fastapi import FastAPI, Request, HTTPException, Response
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.trustedhost import TrustedHostMiddleware
from pydantic import BaseModel

from twilio.twiml.messaging_response import MessagingResponse
from twilio.request_validator import RequestValidator

import gspread, gspread.exceptions
from google.oauth2.service_account import Credentials
import redis

# ─── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("razali")

def log(level, msg, **ctx):
    extra = " ".join(f"{k}={v}" for k, v in ctx.items())
    getattr(logger, level)(f"{msg} | {extra}" if extra else msg)

# ─── App ──────────────────────────────────────────────────────────────────────
app = FastAPI(title="RAZALI")
app.add_middleware(TrustedHostMiddleware, allowed_hosts=["*"])
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])
executor = ThreadPoolExecutor(max_workers=4)

# ─── Env ──────────────────────────────────────────────────────────────────────
BOOKING_URL      = os.environ.get("BOOKING_URL", "https://razali-salon-production.up.railway.app/book")
SALON_PHONE      = os.environ.get("SALON_PHONE", "+994XXXXXXXXX")
CANCEL_CUTOFF_HOURS = int(os.environ.get("CANCEL_CUTOFF_HOURS", "2"))  # min hours before appt to allow cancel
ADMIN_PASSWORD = os.environ.get("ADMIN_PASSWORD", "razali2026")
TWILIO_SID       = os.environ.get("TWILIO_ACCOUNT_SID", "")
TWILIO_TOKEN     = os.environ.get("TWILIO_AUTH_TOKEN", "")
TWILIO_WA_NUMBER = os.environ.get("TWILIO_WHATSAPP_NUMBER", "whatsapp:+994557192949")
TELEGRAM_TOKEN   = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT    = os.environ.get("TELEGRAM_CHAT_ID", "")

# ─── Redis ────────────────────────────────────────────────────────────────────
redis_client = redis.from_url(
    os.environ.get("REDIS_URL", "redis://default:BaEbVkRTeNZheGeHSZyWVmRafYjhPncI@redis.railway.internal:6379"),
    decode_responses=True
)

def is_rate_limited(phone: str) -> bool:
    key = f"razali:rate:{phone}"
    count = redis_client.get(key)
    if count and int(count) >= 15:
        return True
    pipe = redis_client.pipeline()
    pipe.incr(key)
    pipe.expire(key, 60)
    pipe.execute()
    return False

# ─── Google Sheets ────────────────────────────────────────────────────────────
_sheets_client = None

def get_sheets():
    global _sheets_client
    if _sheets_client is None:
        creds_json = json.loads(os.environ.get("GOOGLE_CREDS", "{}"))
        scopes = ["https://spreadsheets.google.com/feeds",
                  "https://www.googleapis.com/auth/drive"]
        creds = Credentials.from_service_account_info(creds_json, scopes=scopes)
        _sheets_client = gspread.authorize(creds)
    return _sheets_client

# ─── In-memory cache ──────────────────────────────────────────────────────────
MASTERS       = {}   # {id: {name, mon..sun, start_time, end_time, specialty, experience}}
SERVICES      = {}   # {id: {category, name, price}}
DURATIONS     = {}   # {(master_id, service_id): minutes}
CATEGORIES    = []
BLOCKED_DATES = set()

def load_data():
    global MASTERS, SERVICES, DURATIONS, CATEGORIES, BLOCKED_DATES
    try:
        client = get_sheets()
        db = client.open("RAZALI_DB")

        new_masters = {}
        for row in db.worksheet("Masters").get_all_records():
            mid = str(row["id"])
            new_masters[mid] = {
                "name":       row["name"],
                "specialty":  row.get("specialty", ""),
                "experience": row.get("experience", ""),
                "mon": str(row.get("mon","")).upper()=="TRUE",
                "tue": str(row.get("tue","")).upper()=="TRUE",
                "wed": str(row.get("wed","")).upper()=="TRUE",
                "thu": str(row.get("thu","")).upper()=="TRUE",
                "fri": str(row.get("fri","")).upper()=="TRUE",
                "sat": str(row.get("sat","")).upper()=="TRUE",
                "sun": str(row.get("sun","")).upper()=="TRUE",
                "start_time": str(row.get("start_time","09:00")),
                "end_time":   str(row.get("end_time","19:00")),
            }

        new_services, new_cats = {}, []
        for row in db.worksheet("Services").get_all_records():
            sid = str(row["id"])
            cat = row["category"]
            new_services[sid] = {
                "category": cat,
                "name":     row["name"],
                "price":    float(row["price"]),
            }
            if cat not in new_cats:
                new_cats.append(cat)

        new_durations = {}
        for row in db.worksheet("Service_Duration").get_all_records():
            new_durations[(str(row["master_id"]), str(row["service_id"]))] = int(row["duration_mins"])

        new_blocked = set()
        try:
            for row in db.worksheet("Blocked_Dates").get_all_records():
                mid = str(row.get("master_id","")).strip()
                d   = str(row.get("date","")).strip()
                if mid and d:
                    new_blocked.add((mid, d))
        except gspread.exceptions.WorksheetNotFound:
            pass

        MASTERS, SERVICES, DURATIONS = new_masters, new_services, new_durations
        CATEGORIES, BLOCKED_DATES    = new_cats, new_blocked
        log("info", "Data loaded", masters=len(MASTERS), services=len(SERVICES))
    except Exception as e:
        log("error", "load_data failed", error=str(e))

def is_blocked(master_id, date_str):
    return (master_id, date_str) in BLOCKED_DATES or ("ALL", date_str) in BLOCKED_DATES

# ─── Slot logic ───────────────────────────────────────────────────────────────
def get_slots(master_id: str, service_id: str, date_str: str) -> list[str]:
    master = MASTERS.get(master_id)
    if not master or is_blocked(master_id, date_str):
        return []
    duration = DURATIONS.get((master_id, service_id), 60)
    date = datetime.strptime(date_str, "%Y-%m-%d")
    day_name = date.strftime("%a").lower()
    if not master.get(day_name, False):
        return []
    sh, sm = map(int, master["start_time"].split(":"))
    eh, em = map(int, master["end_time"].split(":"))
    start = datetime(date.year, date.month, date.day, sh, sm)
    end   = datetime(date.year, date.month, date.day, eh, em)

    db = get_sheets().open("RAZALI_DB")
    booked = []
    for b in db.worksheet("Bookings").get_all_records():
        if (str(b.get("master_id")) == master_id and
            str(b.get("date")) == date_str and
            str(b.get("status")) not in ("Cancelled","No-Show")):
            btime = str(b.get("time",""))
            bdur  = DURATIONS.get((master_id, str(b.get("service_id"))), 60)
            if btime:
                bh, bm = map(int, btime.split(":"))
                bs = datetime(date.year, date.month, date.day, bh, bm)
                be = bs + timedelta(minutes=bdur)
                booked.append((bs, be))

    slots, cur = [], start
    while cur + timedelta(minutes=duration) <= end:
        se = cur + timedelta(minutes=duration)
        if all(se <= bs or cur >= be for bs, be in booked):
            slots.append(cur.strftime("%H:%M"))
        cur += timedelta(minutes=30)
    return slots

def get_available_dates(master_id: str) -> list[str]:
    master = MASTERS.get(master_id)
    if not master:
        return []
    result, today = [], datetime.now().date()
    check = today + timedelta(days=1)
    while len(result) < 28:
        ds  = check.strftime("%Y-%m-%d")
        day = check.strftime("%a").lower()
        if master.get(day, False) and not is_blocked(master_id, ds):
            result.append(ds)
        check += timedelta(days=1)
    return result

# ─── Sheet writers ────────────────────────────────────────────────────────────
def write_booking(phone, name, master_id, service_id, date, time, note=""):
    try:
        db    = get_sheets().open("RAZALI_DB")
        sheet = db.worksheet("Bookings")
        bid   = str(uuid.uuid4())[:8].upper()
        sheet.append_row([bid, phone, name, master_id, service_id, date, time,
                          "Confirmed", "FALSE", note])
        log("info", "Booking written", id=bid)
        return bid
    except Exception as e:
        log("error", "write_booking failed", error=str(e))
        return None

def cancel_booking(row: int):
    try:
        db = get_sheets().open("RAZALI_DB")
        db.worksheet("Bookings").update_cell(row, 8, "Cancelled")
        return True
    except Exception as e:
        log("error", "cancel_booking failed", error=str(e))
        return False

def reschedule_booking(row: int, new_date: str, new_time: str):
    try:
        sheet = get_sheets().open("RAZALI_DB").worksheet("Bookings")
        sheet.update_cell(row, 6, new_date)
        sheet.update_cell(row, 7, new_time)
        sheet.update_cell(row, 9, "FALSE")
        return True
    except Exception as e:
        log("error", "reschedule failed", error=str(e))
        return False

def fetch_active_booking(phone: str) -> dict:
    try:
        db    = get_sheets().open("RAZALI_DB")
        rows  = db.worksheet("Bookings").get_all_records()
        today = datetime.now().strftime("%Y-%m-%d")
        for i, b in enumerate(reversed(rows), 1):
            cp = str(b.get("phone","")).replace("whatsapp:","").strip()
            up = phone.replace("whatsapp:","").strip()
            if cp == up and str(b.get("status")) == "Confirmed" and str(b.get("date")) >= today:
                return {"found": True, "row": len(rows)-i+2,
                        "booking_id": str(b.get("id")),
                        "master_id":  str(b.get("master_id")),
                        "service_id": str(b.get("service_id")),
                        "date": str(b.get("date")),
                        "time": str(b.get("time")),
                        "customer_name": str(b.get("customer_name"))}
        return {"found": False}
    except Exception as e:
        log("error", "fetch_active_booking failed", error=str(e))
        return {"found": False}

def fetch_all_bookings(phone: str) -> list:
    try:
        db    = get_sheets().open("RAZALI_DB")
        rows  = db.worksheet("Bookings").get_all_records()
        today = datetime.now().strftime("%Y-%m-%d")
        out   = []
        for i, b in enumerate(rows, 2):
            cp = str(b.get("phone","")).replace("whatsapp:","").strip()
            up = phone.replace("whatsapp:","").strip()
            if cp == up and str(b.get("status")) == "Confirmed" and str(b.get("date")) >= today:
                out.append({"row": i,
                            "booking_id":  str(b.get("id")),
                            "master_id":   str(b.get("master_id")),
                            "service_id":  str(b.get("service_id")),
                            "date":        str(b.get("date")),
                            "time":        str(b.get("time")),
                            "customer_name": str(b.get("customer_name"))})
        out.sort(key=lambda x: (x["date"], x["time"]))
        return out
    except Exception as e:
        log("error", "fetch_all_bookings failed", error=str(e))
        return []

def fetch_booking_by_id(phone: str, booking_id: str) -> dict:
    try:
        db    = get_sheets().open("RAZALI_DB")
        rows  = db.worksheet("Bookings").get_all_records()
        today = datetime.now().strftime("%Y-%m-%d")
        for i, b in enumerate(rows, 2):
            cp = str(b.get("phone","")).replace("whatsapp:","").strip()
            up = phone.replace("whatsapp:","").strip()
            if (cp == up and
                str(b.get("id")) == booking_id and
                str(b.get("status")) == "Confirmed" and
                str(b.get("date")) >= today):
                return {"found": True, "row": i,
                        "booking_id": str(b.get("id")),
                        "master_id":  str(b.get("master_id")),
                        "service_id": str(b.get("service_id")),
                        "date": str(b.get("date")),
                        "time": str(b.get("time")),
                        "customer_name": str(b.get("customer_name"))}
        return {"found": False}
    except Exception as e:
        log("error", "fetch_booking_by_id failed", error=str(e))
        return {"found": False}

# ─── Notifications ────────────────────────────────────────────────────────────
async def telegram(msg: str):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT:
        return
    try:
        async with httpx.AsyncClient() as c:
            await c.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                         json={"chat_id": TELEGRAM_CHAT, "text": msg, "parse_mode": "HTML"},
                         timeout=5.0)
    except Exception as e:
        log("error", "telegram failed", error=str(e))

async def whatsapp_send(phone: str, msg: str) -> bool:
    if not TWILIO_SID or not TWILIO_TOKEN:
        return False
    try:
        async with httpx.AsyncClient() as c:
            r = await c.post(
                f"https://api.twilio.com/2010-04-01/Accounts/{TWILIO_SID}/Messages.json",
                data={"From": TWILIO_WA_NUMBER, "To": phone, "Body": msg},
                auth=(TWILIO_SID, TWILIO_TOKEN), timeout=10.0)
            return r.status_code == 201
    except Exception as e:
        log("error", "whatsapp_send failed", error=str(e))
        return False

def booking_alert(bid, name, phone, mid, sid, date, time):
    mn  = MASTERS.get(mid,{}).get("name", mid)
    svc = SERVICES.get(sid,{})
    dur = DURATIONS.get((mid,sid), 60)
    df  = datetime.strptime(date,"%Y-%m-%d").strftime("%d %b %Y")
    return (f"📅 <b>NEW BOOKING</b>\n━━━━━━━━━━━━━━━━━━\n"
            f"🆔 #{bid}\n👤 {name}\n📞 {phone}\n━━━━━━━━━━━━━━━━━━\n"
            f"💅 {svc.get('name',sid)}\n👩 {mn}\n📅 {df}\n🕐 {time}\n"
            f"⏱ {dur} min\n💰 {svc.get('price',0):.0f} AZN\n━━━━━━━━━━━━━━━━━━")

def cancel_alert(bid, name, phone, mid, sid, date, time):
    mn = MASTERS.get(mid,{}).get("name", mid)
    sn = SERVICES.get(sid,{}).get("name", sid)
    df = datetime.strptime(date,"%Y-%m-%d").strftime("%d %b %Y")
    return (f"❌ <b>CANCELLED</b>\n━━━━━━━━━━━━━━━━━━\n"
            f"🆔 #{bid}\n👤 {name}\n📞 {phone}\n━━━━━━━━━━━━━━━━━━\n"
            f"💅 {sn}\n👩 {mn}\n📅 {df}\n🕐 {time}\n━━━━━━━━━━━━━━━━━━")

def reschedule_alert(bid, name, phone, mid, sid, old_d, old_t, new_d, new_t):
    mn = MASTERS.get(mid,{}).get("name", mid)
    sn = SERVICES.get(sid,{}).get("name", sid)
    return (f"🔄 <b>RESCHEDULED</b>\n━━━━━━━━━━━━━━━━━━\n"
            f"🆔 #{bid}\n👤 {name}\n📞 {phone}\n━━━━━━━━━━━━━━━━━━\n"
            f"💅 {sn}\n👩 {mn}\n"
            f"❌ {datetime.strptime(old_d,'%Y-%m-%d').strftime('%d %b')} {old_t}\n"
            f"✅ {datetime.strptime(new_d,'%Y-%m-%d').strftime('%d %b')} {new_t}\n"
            f"━━━━━━━━━━━━━━━━━━")

async def refresh_loop():
    """Reload masters/services/blocked dates from Sheets every 10 minutes."""
    while True:
        await asyncio.sleep(600)
        try:
            await asyncio.get_event_loop().run_in_executor(executor, load_data)
            log("info", "Data auto-refreshed")
        except Exception as e:
            log("error", "refresh_loop error", error=str(e))

# ─── Reminder loop ────────────────────────────────────────────────────────────
async def reminder_loop():
    while True:
        try:
            await asyncio.sleep(1800)
            now    = datetime.now()
            target = now + timedelta(hours=24)
            td, th = target.strftime("%Y-%m-%d"), target.strftime("%H")
            db     = get_sheets().open("RAZALI_DB")
            sheet  = db.worksheet("Bookings")
            for i, b in enumerate(sheet.get_all_records(), 2):
                if (str(b.get("date")) == td and
                    str(b.get("time",""))[:2] == th and
                    str(b.get("status")) == "Confirmed" and
                    str(b.get("reminder_sent","")).upper() == "FALSE"):
                    phone = str(b.get("phone",""))
                    mn    = MASTERS.get(str(b.get("master_id")),{}).get("name","")
                    sn    = SERVICES.get(str(b.get("service_id")),{}).get("name","")
                    df    = datetime.strptime(td,"%Y-%m-%d").strftime("%d %b %Y")
                    msg   = (f"⏰ *Reminder / Xatırlatma*\n\n"
                             f"Tomorrow *{df}* at *{b.get('time')}*\n"
                             f"you have an appointment with *{mn}* for *{sn}*.\n\n"
                             f"📍 RAZALI Nails / Hair / Make Up\n\n"
                             f"To cancel or reschedule reply to this bot.")
                    ok = await whatsapp_send(phone, msg)
                    if ok:
                        sheet.update_cell(i, 9, "TRUE")
                        log("info", "Reminder sent", phone=phone[-6:])
        except Exception as e:
            log("error", "reminder_loop error", error=str(e))

# ─── Startup ──────────────────────────────────────────────────────────────────
@app.on_event("startup")
async def on_startup():
    log("info", "RAZALI starting up")
    load_data()
    asyncio.create_task(reminder_loop())
    asyncio.create_task(refresh_loop())

# ═══════════════════════════════════════════════════════════════════════════════
# API ENDPOINTS
# ═══════════════════════════════════════════════════════════════════════════════

# ── GET /api/data — all services and masters for the website ──────────────────
@app.get("/api/data")
async def api_data():
    masters_out = []
    for mid, m in MASTERS.items():
        masters_out.append({
            "id":         mid,
            "name":       m["name"],
            "specialty":  m.get("specialty",""),
            "experience": m.get("experience",""),
            "initials":   m["name"][:2].upper(),
        })

    services_out = []
    for sid, s in SERVICES.items():
        services_out.append({
            "id":       sid,
            "category": s["category"],
            "name":     s["name"],
            "price":    s["price"],
        })

    # Default durations per service (use first master's duration as representative)
    durations_out = {}
    for (mid, sid), dur in DURATIONS.items():
        if sid not in durations_out:
            durations_out[sid] = dur

    return JSONResponse({
        "categories": CATEGORIES,
        "services":   services_out,
        "masters":    masters_out,
        "durations":  durations_out,
    })

# ── GET /api/slots ─────────────────────────────────────────────────────────────
@app.get("/api/slots")
async def api_slots(master_id: str, service_id: str, date: str):
    try:
        datetime.strptime(date, "%Y-%m-%d")
    except ValueError:
        raise HTTPException(400, "Invalid date format")
    slots = await asyncio.get_event_loop().run_in_executor(
        executor, get_slots, master_id, service_id, date
    )
    return JSONResponse({"slots": slots})

# ── GET /api/dates ─────────────────────────────────────────────────────────────
@app.get("/api/dates")
async def api_dates(master_id: str):
    if master_id not in MASTERS:
        raise HTTPException(404, "Master not found")
    dates = await asyncio.get_event_loop().run_in_executor(
        executor, get_available_dates, master_id
    )
    return JSONResponse({"dates": dates})

# ── POST /api/book ─────────────────────────────────────────────────────────────
class BookingRequest(BaseModel):
    name:       str
    phone:      str
    service_id: str
    master_id:  str
    date:       str
    time:       str
    note:       str = ""

@app.post("/api/book")
async def api_book(req: BookingRequest):
    name  = req.name.strip()[:60]
    phone = req.phone.strip()[:20]

    if not name or not phone:
        raise HTTPException(400, "Name and phone are required")
    if req.service_id not in SERVICES:
        raise HTTPException(400, "Invalid service")
    if req.master_id not in MASTERS:
        raise HTTPException(400, "Invalid master")

    # Double-check slot is still free
    slots = await asyncio.get_event_loop().run_in_executor(
        executor, get_slots, req.master_id, req.service_id, req.date
    )
    if req.time not in slots:
        raise HTTPException(409, "This slot is no longer available. Please choose another time.")

    # Normalise phone for WhatsApp
    wa_phone = phone.replace(" ","").replace("-","")
    if not wa_phone.startswith("+"):
        wa_phone = "+" + wa_phone
    wa_phone = "whatsapp:" + wa_phone

    bid = await asyncio.get_event_loop().run_in_executor(
        executor, write_booking,
        wa_phone, name, req.master_id, req.service_id, req.date, req.time, req.note
    )
    if not bid:
        raise HTTPException(500, "Failed to save booking. Please try again.")

    # Telegram alert
    asyncio.create_task(telegram(
        booking_alert(bid, name, wa_phone, req.master_id, req.service_id, req.date, req.time)
    ))

    # WhatsApp confirmation to customer
    svc = SERVICES.get(req.service_id, {})
    mn  = MASTERS.get(req.master_id, {}).get("name","")
    df  = datetime.strptime(req.date,"%Y-%m-%d").strftime("%d %b %Y")
    wa_msg = (
        f"✅ *Booking confirmed! / Rezervasiya təsdiqləndi!*\n\n"
        f"🆔 *#{bid}*\n"
        f"💅 {svc.get('name','')}\n"
        f"👩 {mn}\n"
        f"📅 {df} • 🕐 {req.time}\n"
        f"💰 {int(svc.get('price',0))} AZN\n\n"
        f"⏰ You'll receive a reminder 24h before.\n"
        f"To cancel or reschedule, reply to this message."
    )
    asyncio.create_task(whatsapp_send(wa_phone, wa_msg))

    log("info", "Booking confirmed via web", id=bid, phone=wa_phone[-6:])
    return JSONResponse({"booking_id": bid})

# ── GET /api/my-bookings ───────────────────────────────────────────────────────
@app.get("/api/my-bookings")
async def api_my_bookings(phone: str):
    clean = phone.strip().replace(" ", "").replace("-", "")
    if not clean.startswith("+"): clean = "+" + clean
    wa_phone = "whatsapp:" + clean
    bookings = await asyncio.get_event_loop().run_in_executor(
        executor, fetch_all_bookings, wa_phone)
    result = []
    for b in bookings:
        result.append({**b,
            "master_name":  MASTERS.get(b["master_id"], {}).get("name", b["master_id"]),
            "service_name": SERVICES.get(b["service_id"], {}).get("name", b["service_id"]),
            "price":        SERVICES.get(b["service_id"], {}).get("price", 0),
        })
    return JSONResponse({"bookings": result})

# ── POST /api/cancel-booking ───────────────────────────────────────────────────
class CancelWebRequest(BaseModel):
    phone:      str
    booking_id: str

@app.post("/api/cancel-booking")
async def api_cancel_booking(req: CancelWebRequest):
    clean = req.phone.strip().replace(" ", "").replace("-", "")
    if not clean.startswith("+"): clean = "+" + clean
    wa_phone = "whatsapp:" + clean
    b = await asyncio.get_event_loop().run_in_executor(
        executor, fetch_booking_by_id, wa_phone, req.booking_id)
    if not b.get("found"):
        raise HTTPException(404, "Booking not found")
    # Enforce cancellation cutoff
    try:
        appt_dt = datetime.strptime(f"{b['date']} {b['time']}", "%Y-%m-%d %H:%M")
        if datetime.now() > appt_dt - timedelta(hours=CANCEL_CUTOFF_HOURS):
            raise HTTPException(400, f"Cancellations must be made at least {CANCEL_CUTOFF_HOURS} hours before the appointment.")
    except HTTPException:
        raise
    except Exception:
        pass
    ok = await asyncio.get_event_loop().run_in_executor(executor, cancel_booking, b["row"])
    if ok:
        asyncio.create_task(telegram(cancel_alert(
            b["booking_id"], b["customer_name"], wa_phone,
            b["master_id"], b["service_id"], b["date"], b["time"]
        )))
    return JSONResponse({"success": ok})

# ── POST /api/reschedule-booking ───────────────────────────────────────────────
class RescheduleWebRequest(BaseModel):
    phone:      str
    booking_id: str
    new_date:   str
    new_time:   str

@app.post("/api/reschedule-booking")
async def api_reschedule_booking(req: RescheduleWebRequest):
    clean = req.phone.strip().replace(" ", "").replace("-", "")
    if not clean.startswith("+"): clean = "+" + clean
    wa_phone = "whatsapp:" + clean
    b = await asyncio.get_event_loop().run_in_executor(
        executor, fetch_booking_by_id, wa_phone, req.booking_id)
    if not b.get("found"):
        raise HTTPException(404, "Booking not found")
    slots = await asyncio.get_event_loop().run_in_executor(
        executor, get_slots, b["master_id"], b["service_id"], req.new_date)
    if req.new_time not in slots:
        raise HTTPException(409, "Slot not available")
    ok = await asyncio.get_event_loop().run_in_executor(
        executor, reschedule_booking, b["row"], req.new_date, req.new_time)
    if ok:
        asyncio.create_task(telegram(reschedule_alert(
            b["booking_id"], b["customer_name"], wa_phone,
            b["master_id"], b["service_id"],
            b["date"], b["time"], req.new_date, req.new_time
        )))
    return JSONResponse({"success": ok})

# ── GET /api/admin/today ───────────────────────────────────────────────────────
@app.get("/api/admin/today")
async def api_admin_today(pw: str, date: str = None):
    if pw != ADMIN_PASSWORD:
        raise HTTPException(403, "Wrong password")
    if not date:
        date = datetime.now().strftime("%Y-%m-%d")
    try:
        datetime.strptime(date, "%Y-%m-%d")
    except ValueError:
        raise HTTPException(400, "Invalid date")
    try:
        db    = get_sheets().open("RAZALI_DB")
        rows  = db.worksheet("Bookings").get_all_records()
        result = []
        for b in rows:
            if str(b.get("date")) == date and str(b.get("status")) == "Confirmed":
                mid = str(b.get("master_id"))
                sid = str(b.get("service_id"))
                dur = DURATIONS.get((mid, sid), 60)
                result.append({
                    "booking_id":    str(b.get("id")),
                    "customer_name": str(b.get("customer_name","")),
                    "phone":         str(b.get("phone","")).replace("whatsapp:",""),
                    "master_id":     mid,
                    "master_name":   MASTERS.get(mid,{}).get("name", mid),
                    "master_initials": MASTERS.get(mid,{}).get("name","??")[:2].upper(),
                    "service_id":    sid,
                    "service_name":  SERVICES.get(sid,{}).get("name", sid),
                    "time":          str(b.get("time","")),
                    "duration":      dur,
                    "price":         SERVICES.get(sid,{}).get("price",0),
                })
        result.sort(key=lambda x: (x["master_id"], x["time"]))
        masters_list = [{"id": mid, "name": m["name"],
                         "initials": m["name"][:2].upper()}
                        for mid, m in MASTERS.items()]
        return JSONResponse({"date": date, "bookings": result, "masters": masters_list})
    except Exception as e:
        log("error", "api_admin_today failed", error=str(e))
        raise HTTPException(500, "Failed to load data")

# ── GET /admin ─────────────────────────────────────────────────────────────────
@app.get("/admin", response_class=HTMLResponse)
async def serve_admin_page():
    html_path = Path(__file__).parent / "razali_admin.html"
    if html_path.exists():
        return HTMLResponse(html_path.read_text(encoding="utf-8"))
    raise HTTPException(404, "Admin page not found")

# ── GET / — landing page ───────────────────────────────────────────────────────
@app.get("/", response_class=HTMLResponse)
async def serve_landing_page():
    html_path = Path(__file__).parent / "razali_landing.html"
    if html_path.exists():
        return HTMLResponse(html_path.read_text(encoding="utf-8"))
    # Fallback redirect to /book
    from fastapi.responses import RedirectResponse
    return RedirectResponse(url="/book")

# ── GET /book — serve the HTML page ───────────────────────────────────────────
@app.get("/book", response_class=HTMLResponse)
async def serve_booking_page():
    html_path = Path(__file__).parent / "razali_booking.html"
    if html_path.exists():
        return HTMLResponse(html_path.read_text(encoding="utf-8"))
    raise HTTPException(404, "Booking page not found")

# ═══════════════════════════════════════════════════════════════════════════════
# WHATSAPP WEBHOOK  (greeter only — sends booking link)
# ═══════════════════════════════════════════════════════════════════════════════

GREETER = {
    "az": (
        "👋 *RAZALI* salonuna xoş gəlmisiniz!\n\n"
        "Rezervasiya etmək üçün linkə keçin:\n"
        "{url}\n\n"
        "Mövcud rezervasiyalarınız üçün:\n"
        "*MƏNİM* — rezervasiyalarıma bax\n"
        "*DƏYİŞ* — vaxtı dəyiş\n"
        "*İPTAL* — ləğv et\n"
        "*KÖMƏK* — {phone}"
    ),
    "en": (
        "👋 Welcome to *RAZALI* salon!\n\n"
        "Book your appointment here:\n"
        "{url}\n\n"
        "For existing bookings:\n"
        "*MY* — view my bookings\n"
        "*CHANGE* — reschedule\n"
        "*CANCEL* — cancel\n"
        "*HELP* — {phone}"
    ),
    "ru": (
        "👋 Добро пожаловать в салон *RAZALI*!\n\n"
        "Запишитесь онлайн:\n"
        "{url}\n\n"
        "Управление записями:\n"
        "*МОИ* — мои записи\n"
        "*ПЕРЕНОС* — перенести\n"
        "*ОТМЕНА* — отменить\n"
        "*ПОМОЩЬ* — {phone}"
    ),
}

KEYWORDS = {
    "cancel":     {"az": "İPTAL",   "en": "CANCEL",  "ru": "ОТМЕНА"},
    "reschedule": {"az": "DƏYİŞ",   "en": "CHANGE",  "ru": "ПЕРЕНОС"},
    "my":         {"az": "MƏNİM",   "en": "MY",      "ru": "МОИ"},
    "help":       {"az": "KÖMƏK",   "en": "HELP",    "ru": "ПОМОЩЬ"},
    "yes":        {"az": "BƏLİ",    "en": "YES",     "ru": "ДА"},
    "no":         {"az": "XEYİR",   "en": "NO",      "ru": "XEYİR"},
    "back":       {"az": "GERİ",    "en": "BACK",    "ru": "НАЗАД"},
}

def reply_wa(text):
    r = MessagingResponse()
    r.message(text)
    return Response(content=str(r), media_type="application/xml")

def detect_lang(text: str) -> str | None:
    t = text.strip().lower()
    if t == "az": return "az"
    if t == "en": return "en"
    if t == "ru": return "ru"
    # Detect from keywords
    for action, langs in KEYWORDS.items():
        for lng, kw in langs.items():
            if t == kw.lower():
                return lng
    return None

def get_wa_session(phone):
    d = redis_client.get(f"razali:wa:{phone}")
    if d: return json.loads(d)
    return {"lang": None, "state": "IDLE", "cancel_booking": None,
            "reschedule_booking": None, "available_dates": [], "available_slots": []}

def save_wa_session(phone, s):
    redis_client.setex(f"razali:wa:{phone}", 86400, json.dumps(s))

def fmt_bookings(bookings, lang):
    lines = []
    for b in bookings:
        sn = SERVICES.get(b["service_id"],{}).get("name", b["service_id"])
        mn = MASTERS.get(b["master_id"],{}).get("name", b["master_id"])
        df = datetime.strptime(b["date"],"%Y-%m-%d").strftime("%d %b %Y")
        lines.append(f"🆔 #{b['booking_id']}\n💅 {sn}\n👩 {mn}\n📅 {df} • 🕐 {b['time']}")
    sep = "\n" + "─"*20 + "\n"
    return sep.join(lines)

def fmt_dates(dates):
    lines = []
    emojis = "1️⃣2️⃣3️⃣4️⃣5️⃣6️⃣7️⃣8️⃣9️⃣🔟".split()
    # Split emoji string properly
    emojis = ["1️⃣","2️⃣","3️⃣","4️⃣","5️⃣","6️⃣","7️⃣"]
    for i, d in enumerate(dates[:7], 0):
        dt  = datetime.strptime(d,"%Y-%m-%d")
        day = dt.strftime("%A")
        fmt = dt.strftime("%d %b")
        lines.append(f"{emojis[i] if i<7 else str(i+1)+'.'} {day}, {fmt}")
    return "\n".join(lines)

def fmt_slots(slots):
    emojis = ["1️⃣","2️⃣","3️⃣","4️⃣","5️⃣","6️⃣","7️⃣","8️⃣","9️⃣","🔟"]
    return "\n".join(f"{emojis[i] if i<10 else str(i+1)+'.'} {s}"
                     for i, s in enumerate(slots))

@app.post("/whatsapp")
async def whatsapp_webhook(request: Request):
    # Validate Twilio signature
    validator = RequestValidator(TWILIO_TOKEN)
    form_data = await request.form()
    params    = dict(form_data)
    fproto    = request.headers.get("x-forwarded-proto","https")
    fhost     = request.headers.get("x-forwarded-host", request.headers.get("host",""))
    url       = f"{fproto}://{fhost}{request.url.path}"
    sig       = request.headers.get("X-Twilio-Signature","")
    if TWILIO_TOKEN and not validator.validate(url, params, sig):
        raise HTTPException(403, "Invalid signature")

    Body  = params.get("Body","").strip()
    From  = params.get("From","")
    Media = params.get("MediaContentType0","")

    # Non-text messages
    if not Body and Media:
        return reply_wa("💬 Please send a text message.")

    if is_rate_limited(From):
        return reply_wa("⚠️ Too many messages. Please wait a minute.")

    user   = Body.strip()[:100]
    ulower = user.lower()
    log("info", "WA message", phone=From[-6:], text=user[:20])

    session = get_wa_session(From)
    lang    = session.get("lang")

    # ── DETECT LANGUAGE ────────────────────────────────────────────────────────
    if lang is None:
        detected = detect_lang(user)
        if detected in ("az","en","ru"):
            lang = detected
            session["lang"] = lang
        elif ulower in ("az","en","ru"):
            lang = ulower
            session["lang"] = lang
        else:
            save_wa_session(From, session)
            return reply_wa("👋 *RAZALI*\n\nSelect language / Dil seçin / Выберите язык:\n\n🇦🇿 *AZ*\n🇬🇧 *EN*\n🇷🇺 *RU*")

    # Resolve keywords for this lang
    kw = {k: v[lang].lower() for k, v in KEYWORDS.items()}

    # ── CONFIRM CANCEL ────────────────────────────────────────────────────────
    if session["state"] == "CONFIRM_CANCEL":
        b = session.get("cancel_booking",{})
        if ulower == kw["yes"]:
            # Enforce cancellation cutoff
            try:
                appt_dt = datetime.strptime(f"{b['date']} {b['time']}", "%Y-%m-%d %H:%M")
                if datetime.now() > appt_dt - timedelta(hours=CANCEL_CUTOFF_HOURS):
                    session.update({"state":"IDLE","cancel_booking":None})
                    save_wa_session(From, session)
                    msgs = {
                        "en": f"⚠️ Sorry, cancellations must be made at least {CANCEL_CUTOFF_HOURS} hours before your appointment. Please contact us directly: {SALON_PHONE}",
                        "ru": f"⚠️ Отменить запись можно не менее чем за {CANCEL_CUTOFF_HOURS} ч до визита. Свяжитесь с нами: {SALON_PHONE}",
                        "az": f"⚠️ Rezervasiyanı görüşdən ən az {CANCEL_CUTOFF_HOURS} saat əvvəl ləğv etmək mümkündür. Bizimlə əlaqə: {SALON_PHONE}",
                    }
                    return reply_wa(msgs[lang])
            except Exception:
                pass
            ok = await asyncio.get_event_loop().run_in_executor(executor, cancel_booking, b["row"])
            if ok:
                asyncio.create_task(telegram(cancel_alert(
                    b["booking_id"], b["customer_name"], From,
                    b["master_id"], b["service_id"], b["date"], b["time"]
                )))
            session.update({"state":"IDLE","cancel_booking":None})
            save_wa_session(From, session)
            msgs = {"en":"✅ Booking cancelled. Book again anytime.",
                    "ru":"✅ Запись отменена.",
                    "az":"✅ Rezervasiya ləğv edildi."}
            return reply_wa(msgs[lang])
        elif ulower == kw["no"]:
            session["state"] = "IDLE"
            save_wa_session(From, session)
            msgs = {"en":"👍 Booking kept.","ru":"👍 Запись сохранена.","az":"👍 Rezervasiya saxlanıldı."}
            return reply_wa(msgs[lang])
        else:
            sn = SERVICES.get(b.get("service_id",""),{}).get("name","")
            mn = MASTERS.get(b.get("master_id",""),{}).get("name","")
            df = datetime.strptime(b["date"],"%Y-%m-%d").strftime("%d %b %Y")
            yes_kw = KEYWORDS["yes"][lang]
            no_kw  = KEYWORDS["no"][lang]
            msgs = {
                "en": f"Cancel this booking?\n🆔 #{b['booking_id']}\n💅 {sn}\n👩 {mn}\n📅 {df} {b['time']}\n\n*{yes_kw}* — yes\n*{no_kw}* — no",
                "ru": f"Отменить запись?\n🆔 #{b['booking_id']}\n💅 {sn}\n👩 {mn}\n📅 {df} {b['time']}\n\n*{yes_kw}* — да\n*{no_kw}* — нет",
                "az": f"Ləğv etmək istəyirsiniz?\n🆔 #{b['booking_id']}\n💅 {sn}\n👩 {mn}\n📅 {df} {b['time']}\n\n*{yes_kw}* — bəli\n*{no_kw}* — xeyr",
            }
            save_wa_session(From, session)
            return reply_wa(msgs[lang])

    # ── RESCHEDULE: PICK DATE ─────────────────────────────────────────────────
    if session["state"] == "RESCHEDULE_DATE":
        dates = session.get("available_dates",[])
        try:
            idx = int(user)
            if 1 <= idx <= len(dates):
                chosen = dates[idx-1]
                rb     = session["reschedule_booking"]
                slots  = await asyncio.get_event_loop().run_in_executor(
                    executor, get_slots, rb["master_id"], rb["service_id"], chosen)
                if not slots:
                    new_dates = get_available_dates(rb["master_id"])
                    session["available_dates"] = new_dates
                    save_wa_session(From, session)
                    no_slot = {"en":"No slots that day. Pick another:","ru":"Нет слотов. Выберите другую дату:","az":"Bu gündə boş saat yoxdur. Başqa tarix seçin:"}
                    return reply_wa(no_slot[lang]+"\n\n"+fmt_dates(new_dates))
                session["date"]             = chosen
                session["available_slots"]  = slots
                session["state"]            = "RESCHEDULE_TIME"
                df = datetime.strptime(chosen,"%Y-%m-%d").strftime("%d %b %Y")
                save_wa_session(From, session)
                pick_time = {"en":f"Available times for {df}:","ru":f"Свободное время {df}:","az":f"{df} tarixi üçün boş saatlar:"}
                return reply_wa(pick_time[lang]+"\n\n"+fmt_slots(slots))
        except ValueError:
            pass
        save_wa_session(From, session)
        err = {"en":"Please enter a number from the list.","ru":"Введите номер из списка.","az":"Siyahıdan nömrə yazın."}
        return reply_wa(err[lang]+"\n\n"+fmt_dates(dates))

    # ── RESCHEDULE: PICK TIME ─────────────────────────────────────────────────
    if session["state"] == "RESCHEDULE_TIME":
        slots = session.get("available_slots",[])
        try:
            idx = int(user)
            if 1 <= idx <= len(slots):
                new_time = slots[idx-1]
                new_date = session["date"]
                rb       = session["reschedule_booking"]
                ok       = await asyncio.get_event_loop().run_in_executor(
                    executor, reschedule_booking, rb["row"], new_date, new_time)
                if ok:
                    asyncio.create_task(telegram(reschedule_alert(
                        rb["booking_id"], rb["customer_name"], From,
                        rb["master_id"], rb["service_id"],
                        rb["date"], rb["time"], new_date, new_time
                    )))
                    mn = MASTERS.get(rb["master_id"],{}).get("name","")
                    df = datetime.strptime(new_date,"%Y-%m-%d").strftime("%d %b %Y")
                    session.update({"state":"IDLE","reschedule_booking":None,
                                    "available_dates":[],"available_slots":[]})
                    save_wa_session(From, session)
                    msgs = {
                        "en": f"✅ Rescheduled!\n👩 {mn}\n📅 {df} • 🕐 {new_time}",
                        "ru": f"✅ Перенесено!\n👩 {mn}\n📅 {df} • 🕐 {new_time}",
                        "az": f"✅ Dəyişdirildi!\n👩 {mn}\n📅 {df} • 🕐 {new_time}",
                    }
                    return reply_wa(msgs[lang])
                else:
                    session["state"] = "IDLE"
                    save_wa_session(From, session)
                    err = {"en":"⚠️ Error. Please try again.","ru":"⚠️ Ошибка.","az":"⚠️ Xəta baş verdi."}
                    return reply_wa(err[lang])
        except ValueError:
            pass
        save_wa_session(From, session)
        err = {"en":"Please enter a number from the list.","ru":"Введите номер из списка.","az":"Siyahıdan nömrə yazın."}
        return reply_wa(err[lang]+"\n\n"+fmt_slots(slots))

    # ── GLOBAL KEYWORDS ───────────────────────────────────────────────────────

    # HELP
    if ulower == kw["help"]:
        msgs = {
            "en": f"🤝 Contact us directly:\n📞 {SALON_PHONE}\n\nOr book online:\n{BOOKING_URL}",
            "ru": f"🤝 Свяжитесь с нами:\n📞 {SALON_PHONE}\n\nЗапись онлайн:\n{BOOKING_URL}",
            "az": f"🤝 Bizimlə əlaqə:\n📞 {SALON_PHONE}\n\nOnline rezervasiya:\n{BOOKING_URL}",
        }
        save_wa_session(From, session)
        return reply_wa(msgs[lang])

    # MY BOOKINGS
    if ulower == kw["my"]:
        bookings = await asyncio.get_event_loop().run_in_executor(executor, fetch_all_bookings, From)
        save_wa_session(From, session)
        if not bookings:
            nb = {"en":"📋 No active bookings.","ru":"📋 Нет активных записей.","az":"📋 Aktiv rezervasiyanız yoxdur."}
            return reply_wa(nb[lang])
        header = {"en":"📋 *Your bookings:*","ru":"📋 *Ваши записи:*","az":"📋 *Rezervasiyalarınız:*"}
        cancel_kw = KEYWORDS["cancel"][lang]
        change_kw = KEYWORDS["reschedule"][lang]
        footer = {"en":f"To cancel: *{cancel_kw}*\nTo reschedule: *{change_kw}*",
                  "ru":f"Отмена: *{cancel_kw}*\nПеренос: *{change_kw}*",
                  "az":f"Ləğv: *{cancel_kw}*\nDəyiş: *{change_kw}*"}
        return reply_wa(header[lang]+"\n\n"+fmt_bookings(bookings,lang)+"\n\n"+footer[lang])

    # CANCEL
    if ulower == kw["cancel"]:
        b = await asyncio.get_event_loop().run_in_executor(executor, fetch_active_booking, From)
        if not b["found"]:
            nb = {"en":"📋 No active bookings.","ru":"📋 Нет активных записей.","az":"📋 Aktiv rezervasiyanız yoxdur."}
            save_wa_session(From, session)
            return reply_wa(nb[lang])
        session["cancel_booking"] = b
        session["state"] = "CONFIRM_CANCEL"
        sn = SERVICES.get(b["service_id"],{}).get("name","")
        mn = MASTERS.get(b["master_id"],{}).get("name","")
        df = datetime.strptime(b["date"],"%Y-%m-%d").strftime("%d %b %Y")
        yes_kw = KEYWORDS["yes"][lang]
        no_kw  = KEYWORDS["no"][lang]
        msgs = {
            "en": f"📋 *Your booking:*\n🆔 #{b['booking_id']}\n💅 {sn}\n👩 {mn}\n📅 {df} • 🕐 {b['time']}\n\nCancel this?\n*{yes_kw}* — yes\n*{no_kw}* — no",
            "ru": f"📋 *Ваша запись:*\n🆔 #{b['booking_id']}\n💅 {sn}\n👩 {mn}\n📅 {df} • 🕐 {b['time']}\n\nОтменить?\n*{yes_kw}* — да\n*{no_kw}* — нет",
            "az": f"📋 *Rezervasiyanız:*\n🆔 #{b['booking_id']}\n💅 {sn}\n👩 {mn}\n📅 {df} • 🕐 {b['time']}\n\nLəğv etmək?\n*{yes_kw}* — bəli\n*{no_kw}* — xeyr",
        }
        save_wa_session(From, session)
        return reply_wa(msgs[lang])

    # RESCHEDULE
    if ulower == kw["reschedule"]:
        b = await asyncio.get_event_loop().run_in_executor(executor, fetch_active_booking, From)
        if not b["found"]:
            nb = {"en":"📋 No active bookings to reschedule.",
                  "ru":"📋 Нет активных записей для переноса.",
                  "az":"📋 Dəyişdiriləcək aktiv rezervasiya yoxdur."}
            save_wa_session(From, session)
            return reply_wa(nb[lang])
        session["reschedule_booking"] = b
        dates = await asyncio.get_event_loop().run_in_executor(
            executor, get_available_dates, b["master_id"])
        session["available_dates"] = dates
        session["state"] = "RESCHEDULE_DATE"
        sn = SERVICES.get(b["service_id"],{}).get("name","")
        mn = MASTERS.get(b["master_id"],{}).get("name","")
        df = datetime.strptime(b["date"],"%Y-%m-%d").strftime("%d %b %Y")
        save_wa_session(From, session)
        hdr = {
            "en": f"📋 *Current booking:*\n🆔 #{b['booking_id']}\n💅 {sn}\n👩 {mn}\n📅 {df} • 🕐 {b['time']}\n\nPick a new date:",
            "ru": f"📋 *Текущая запись:*\n🆔 #{b['booking_id']}\n💅 {sn}\n👩 {mn}\n📅 {df} • 🕐 {b['time']}\n\nВыберите новую дату:",
            "az": f"📋 *Mövcud rezervasiya:*\n🆔 #{b['booking_id']}\n💅 {sn}\n👩 {mn}\n📅 {df} • 🕐 {b['time']}\n\nYeni tarix seçin:",
        }
        return reply_wa(hdr[lang]+"\n\n"+fmt_dates(dates))

    # ── FALLBACK — show greeting/menu ─────────────────────────────────────────
    save_wa_session(From, session)
    greeting = GREETER.get(lang, GREETER["az"])
    return reply_wa(greeting.format(url=BOOKING_URL, phone=SALON_PHONE))
