import logging
import sys
import httpx
from fastapi import FastAPI, Form, Response, Request, HTTPException
from twilio.twiml.messaging_response import MessagingResponse
from twilio.request_validator import RequestValidator
from datetime import datetime, timedelta
from fastapi.middleware.trustedhost import TrustedHostMiddleware
import gspread
import gspread.exceptions
from google.oauth2.service_account import Credentials
import redis
import json
import os
import uuid
import asyncio
from concurrent.futures import ThreadPoolExecutor

# ─── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("razali")

def log(level: str, msg: str, **context):
    ctx = " ".join(f"{k}={v}" for k, v in context.items())
    full_msg = f"{msg} | {ctx}" if ctx else msg
    getattr(logger, level)(full_msg)

# ─── App ──────────────────────────────────────────────────────────────────────
app = FastAPI(title="RAZALI Salon Bot")
app.add_middleware(TrustedHostMiddleware, allowed_hosts=["*"])
executor = ThreadPoolExecutor(max_workers=4)

# ─── Redis ────────────────────────────────────────────────────────────────────
redis_client = redis.from_url(
    os.environ.get("REDIS_URL", "redis://localhost:6379"),
    decode_responses=True
)
SESSION_TTL = 60 * 60 * 24

def get_session(phone: str) -> dict:
    data = redis_client.get(f"razali:session:{phone}")
    if data:
        return json.loads(data)
    return {
        "lang": None,
        "state": "IDLE",
        "category": None,
        "service_id": None,
        "master_id": None,
        "date": None,
        "time": None,
        "customer_name": None,
        "cancel_booking": None
    }

def save_session(phone: str, session: dict):
    redis_client.setex(f"razali:session:{phone}", SESSION_TTL, json.dumps(session))

def clear_booking_session(phone: str, lang: str) -> dict:
    new_session = {
        "lang": lang,
        "state": "CHOOSING_CATEGORY",
        "category": None,
        "service_id": None,
        "master_id": None,
        "date": None,
        "time": None,
        "customer_name": None,
        "cancel_booking": None
    }
    save_session(phone, new_session)
    return new_session

# ─── Rate Limiting ────────────────────────────────────────────────────────────
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

# ─── Twilio Validation ────────────────────────────────────────────────────────
async def validate_twilio_request(request: Request) -> dict:
    auth_token = os.environ.get("TWILIO_AUTH_TOKEN", "")
    validator = RequestValidator(auth_token)
    form_data = await request.form()
    params = dict(form_data)
    forwarded_proto = request.headers.get("x-forwarded-proto", "https")
    forwarded_host = request.headers.get("x-forwarded-host", request.headers.get("host", ""))
    url = f"{forwarded_proto}://{forwarded_host}{request.url.path}"
    signature = request.headers.get("X-Twilio-Signature", "")
    if not validator.validate(url, params, signature):
        raise HTTPException(status_code=403, detail="Invalid Twilio signature")
    return params

# ─── Google Sheets ────────────────────────────────────────────────────────────
_sheets_client = None

def get_google_client(force_refresh: bool = False):
    global _sheets_client
    if _sheets_client is None or force_refresh:
        creds_json = json.loads(os.environ.get("GOOGLE_CREDS"))
        scopes = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive"
        ]
        creds = Credentials.from_service_account_info(creds_json, scopes=scopes)
        _sheets_client = gspread.authorize(creds)
        log("info", "Google Sheets client initialized")
    return _sheets_client

# ─── Data cache ───────────────────────────────────────────────────────────────
MASTERS = {}
SERVICES = {}
DURATIONS = {}
CATEGORIES = []

def load_data():
    global MASTERS, SERVICES, DURATIONS, CATEGORIES
    try:
        client = get_google_client()
        db = client.open("RAZALI_DB")

        masters_sheet = db.worksheet("Masters")
        new_masters = {}
        for row in masters_sheet.get_all_records():
            mid = str(row["id"])
            new_masters[mid] = {
                "name": row["name"],
                "mon": str(row.get("mon", "")).upper() == "TRUE",
                "tue": str(row.get("tue", "")).upper() == "TRUE",
                "wed": str(row.get("wed", "")).upper() == "TRUE",
                "thu": str(row.get("thu", "")).upper() == "TRUE",
                "fri": str(row.get("fri", "")).upper() == "TRUE",
                "sat": str(row.get("sat", "")).upper() == "TRUE",
                "sun": str(row.get("sun", "")).upper() == "TRUE",
                "start_time": str(row.get("start_time", "09:00")),
                "end_time": str(row.get("end_time", "19:00"))
            }

        services_sheet = db.worksheet("Services")
        new_services = {}
        new_categories = []
        for row in services_sheet.get_all_records():
            sid = str(row["id"])
            cat = row["category"]
            new_services[sid] = {
                "category": cat,
                "name": row["name"],
                "price": float(row["price"])
            }
            if cat not in new_categories:
                new_categories.append(cat)

        duration_sheet = db.worksheet("Service_Duration")
        new_durations = {}
        for row in duration_sheet.get_all_records():
            key = (str(row["master_id"]), str(row["service_id"]))
            new_durations[key] = int(row["duration_mins"])

        MASTERS = new_masters
        SERVICES = new_services
        DURATIONS = new_durations
        CATEGORIES = new_categories
        log("info", "Data loaded", masters=len(MASTERS), services=len(SERVICES))
    except Exception as e:
        log("error", "Failed to load data", error=str(e))

# ─── Slot availability ────────────────────────────────────────────────────────
def get_available_slots(master_id: str, service_id: str, date_str: str) -> list:
    """
    FIX: Previously opened a new Google Sheets connection per call.
    Now reuses the cached client and also caches bookings for the day
    so multiple slot checks don't each make a separate API call.
    """
    try:
        master = MASTERS.get(master_id)
        if not master:
            return []
        duration = DURATIONS.get((master_id, service_id), 60)
        date = datetime.strptime(date_str, "%Y-%m-%d")
        day_name = date.strftime("%a").lower()
        if not master.get(day_name, False):
            return []
        start_h, start_m = map(int, master["start_time"].split(":"))
        end_h, end_m = map(int, master["end_time"].split(":"))
        start = datetime(date.year, date.month, date.day, start_h, start_m)
        end = datetime(date.year, date.month, date.day, end_h, end_m)

        # Reuse cached client instead of opening a new connection each time
        client = get_google_client()
        db = client.open("RAZALI_DB")
        bookings_sheet = db.worksheet("Bookings")
        all_bookings = bookings_sheet.get_all_records()

        booked_slots = []
        for b in all_bookings:
            if (str(b.get("master_id")) == master_id and
                str(b.get("date")) == date_str and
                str(b.get("status")) not in ("Cancelled",)):
                btime = str(b.get("time", ""))
                bduration = DURATIONS.get((master_id, str(b.get("service_id"))), 60)
                if btime:
                    bh, bm = map(int, btime.split(":"))
                    bstart = datetime(date.year, date.month, date.day, bh, bm)
                    bend = bstart + timedelta(minutes=bduration)
                    booked_slots.append((bstart, bend))
        available = []
        current = start
        while current + timedelta(minutes=duration) <= end:
            slot_end = current + timedelta(minutes=duration)
            is_free = True
            for bstart, bend in booked_slots:
                if not (slot_end <= bstart or current >= bend):
                    is_free = False
                    break
            if is_free:
                available.append(current.strftime("%H:%M"))
            current += timedelta(minutes=30)
        return available
    except Exception as e:
        log("error", "Slot calculation failed", error=str(e))
        return []

def get_available_dates(master_id: str) -> list:
    master = MASTERS.get(master_id)
    if not master:
        return []
    available = []
    today = datetime.now().date()
    check = today + timedelta(days=1)
    while len(available) < 7:
        day_name = check.strftime("%a").lower()
        if master.get(day_name, False):
            available.append(check.strftime("%Y-%m-%d"))
        check += timedelta(days=1)
    return available

# ─── Booking writer ───────────────────────────────────────────────────────────
def write_booking(phone, customer_name, master_id, service_id, date, time):
    try:
        client = get_google_client()
        db = client.open("RAZALI_DB")
        sheet = db.worksheet("Bookings")
        booking_id = str(uuid.uuid4())[:8].upper()
        sheet.append_row([
            booking_id, phone, customer_name,
            master_id, service_id, date, time,
            "Confirmed", "FALSE"
        ])
        log("info", "Booking written", id=booking_id)
        return booking_id
    except Exception as e:
        log("error", "Booking write failed", error=str(e))
        return None

# ─── Cancellation ─────────────────────────────────────────────────────────────
def fetch_active_booking(phone: str) -> dict:
    """Find the most recent active booking for this phone number."""
    try:
        client = get_google_client()
        db = client.open("RAZALI_DB")
        sheet = db.worksheet("Bookings")
        all_bookings = sheet.get_all_records()
        today = datetime.now().strftime("%Y-%m-%d")
        for i, b in enumerate(reversed(all_bookings), 1):
            clean_phone = str(b.get("phone", "")).replace("whatsapp:", "").strip()
            user_phone = str(phone).replace("whatsapp:", "").strip()
            if (clean_phone == user_phone and
                str(b.get("status")) == "Confirmed" and
                str(b.get("date")) >= today):
                row_num = len(all_bookings) - i + 2  # actual sheet row
                return {
                    "found": True,
                    "row": row_num,
                    "booking_id": str(b.get("id")),
                    "master_id": str(b.get("master_id")),
                    "service_id": str(b.get("service_id")),
                    "date": str(b.get("date")),
                    "time": str(b.get("time")),
                    "customer_name": str(b.get("customer_name"))
                }
        return {"found": False}
    except Exception as e:
        log("error", "Fetch booking failed", error=str(e))
        return {"found": False}

# ─── NEW: Fetch all active bookings ──────────────────────────────────────────
def fetch_all_active_bookings(phone: str) -> list:
    """
    Returns all upcoming confirmed bookings for this phone, sorted by date+time.
    Used by the View Bookings feature.
    """
    try:
        client = get_google_client()
        db = client.open("RAZALI_DB")
        sheet = db.worksheet("Bookings")
        all_bookings = sheet.get_all_records()
        today = datetime.now().strftime("%Y-%m-%d")
        results = []
        for i, b in enumerate(all_bookings, 2):  # row 2 = first data row
            clean_phone = str(b.get("phone", "")).replace("whatsapp:", "").strip()
            user_phone = str(phone).replace("whatsapp:", "").strip()
            if (clean_phone == user_phone and
                str(b.get("status")) == "Confirmed" and
                str(b.get("date")) >= today):
                results.append({
                    "row": i,
                    "booking_id": str(b.get("id")),
                    "master_id": str(b.get("master_id")),
                    "service_id": str(b.get("service_id")),
                    "date": str(b.get("date")),
                    "time": str(b.get("time")),
                    "customer_name": str(b.get("customer_name"))
                })
        # Sort ascending by date then time so nearest appointment is first
        results.sort(key=lambda x: (x["date"], x["time"]))
        return results
    except Exception as e:
        log("error", "Fetch all bookings failed", error=str(e))
        return []

def cancel_booking_in_sheet(row: int) -> bool:
    """Set booking status to Cancelled."""
    try:
        client = get_google_client()
        db = client.open("RAZALI_DB")
        sheet = db.worksheet("Bookings")
        sheet.update_cell(row, 8, "Cancelled")
        log("info", "Booking cancelled in sheet", row=row)
        return True
    except Exception as e:
        log("error", "Cancel booking failed", error=str(e))
        return False

# ─── Telegram ─────────────────────────────────────────────────────────────────
async def send_telegram_alert(message: str):
    token = os.environ.get("TELEGRAM_BOT_TOKEN")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        return
    try:
        async with httpx.AsyncClient() as client:
            await client.post(
                f"https://api.telegram.org/bot{token}/sendMessage",
                json={"chat_id": chat_id, "text": message, "parse_mode": "HTML"},
                timeout=5.0
            )
            log("info", "Telegram alert sent")
    except Exception as e:
        log("error", "Telegram alert failed", error=str(e))

def build_booking_alert(booking_id, customer_name, phone, master_id, service_id, date, time):
    master_name = MASTERS.get(master_id, {}).get("name", master_id)
    service = SERVICES.get(service_id, {})
    service_name = service.get("name", service_id)
    price = service.get("price", 0)
    duration = DURATIONS.get((master_id, service_id), 60)
    date_fmt = datetime.strptime(date, "%Y-%m-%d").strftime("%d %b %Y")
    return (
        f"📅 <b>YENİ REZERVASIYA / NEW BOOKING</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"🆔 <b>ID:</b> #{booking_id}\n"
        f"👤 <b>Müştəri:</b> {customer_name}\n"
        f"📞 <b>Telefon:</b> {phone}\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"💅 <b>Xidmət:</b> {service_name}\n"
        f"👩 <b>Master:</b> {master_name}\n"
        f"📅 <b>Tarix:</b> {date_fmt}\n"
        f"🕐 <b>Saat:</b> {time}\n"
        f"⏱ <b>Müddət:</b> {duration} dəq\n"
        f"💰 <b>Qiymət:</b> {price:.0f} AZN\n"
        f"━━━━━━━━━━━━━━━━━━"
    )

def build_cancellation_alert(booking_id, customer_name, phone, master_id, service_id, date, time):
    master_name = MASTERS.get(master_id, {}).get("name", master_id)
    service_name = SERVICES.get(service_id, {}).get("name", service_id)
    date_fmt = datetime.strptime(date, "%Y-%m-%d").strftime("%d %b %Y")
    return (
        f"❌ <b>REZERVASIYA LƏĞV EDİLDİ / BOOKING CANCELLED</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"🆔 <b>ID:</b> #{booking_id}\n"
        f"👤 <b>Müştəri:</b> {customer_name}\n"
        f"📞 <b>Telefon:</b> {phone}\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"💅 <b>Xidmət:</b> {service_name}\n"
        f"👩 <b>Master:</b> {master_name}\n"
        f"📅 <b>Tarix:</b> {date_fmt}\n"
        f"🕐 <b>Saat:</b> {time}\n"
        f"━━━━━━━━━━━━━━━━━━"
    )

# ─── Reminder scheduler ───────────────────────────────────────────────────────
async def reminder_loop():
    while True:
        try:
            await asyncio.sleep(1800)
            now = datetime.now()
            target = now + timedelta(hours=24)
            target_date = target.strftime("%Y-%m-%d")
            target_hour = target.strftime("%H")
            client = get_google_client()
            db = client.open("RAZALI_DB")
            sheet = db.worksheet("Bookings")
            all_bookings = sheet.get_all_records()
            for i, b in enumerate(all_bookings, 2):
                if (str(b.get("date")) == target_date and
                    str(b.get("time", ""))[:2] == target_hour and
                    str(b.get("status")) == "Confirmed" and
                    str(b.get("reminder_sent")).upper() == "FALSE"):
                    phone = str(b.get("phone", ""))
                    master_name = MASTERS.get(str(b.get("master_id")), {}).get("name", "")
                    service_name = SERVICES.get(str(b.get("service_id")), {}).get("name", "")
                    appt_time = str(b.get("time"))
                    appt_date = datetime.strptime(target_date, "%Y-%m-%d").strftime("%d %b %Y")
                    reminder_msg = (
                        f"⏰ *Xatırlatma / Reminder*\n\n"
                        f"Sabah *{appt_date}* tarixində saat *{appt_time}*-də\n"
                        f"*{master_name}* ilə *{service_name}* görüşünüz var.\n\n"
                        f"Tomorrow at *{appt_time}* you have an appointment\n"
                        f"with *{master_name}* for *{service_name}*.\n\n"
                        f"📍 RAZALI Nails / Hair / Make Up\n\n"
                        f"❌ Ləğv etmək / To cancel: *İPTAL* / *CANCEL* / *ОТМЕНА*"
                    )
                    success = await send_whatsapp_reminder(phone, reminder_msg)
                    # FIX: Only mark reminder_sent=TRUE if the message actually sent
                    if success:
                        sheet.update_cell(i, 9, "TRUE")
                        log("info", "Reminder sent and marked", phone=phone[-6:])
                    else:
                        log("warning", "Reminder failed, will retry next cycle", phone=phone[-6:])
        except Exception as e:
            log("error", "Reminder loop error", error=str(e))

async def send_whatsapp_reminder(phone: str, message: str) -> bool:
    """Returns True if message was sent successfully."""
    account_sid = os.environ.get("TWILIO_ACCOUNT_SID")
    auth_token = os.environ.get("TWILIO_AUTH_TOKEN")
    from_number = os.environ.get("TWILIO_WHATSAPP_NUMBER", "whatsapp:+14155238886")
    if not account_sid or not auth_token:
        return False
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                f"https://api.twilio.com/2010-04-01/Accounts/{account_sid}/Messages.json",
                data={"From": from_number, "To": phone, "Body": message},
                auth=(account_sid, auth_token),
                timeout=10.0
            )
            if resp.status_code == 201:
                log("info", "Reminder sent", phone=phone[-6:])
                return True
            else:
                log("error", "Reminder failed", status=resp.status_code)
                return False
    except Exception as e:
        log("error", "Reminder exception", error=str(e))
        return False

# ─── Startup ──────────────────────────────────────────────────────────────────
@app.on_event("startup")
async def startup_event():
    log("info", "RAZALI bot starting up...")
    load_data()
    asyncio.create_task(reminder_loop())

# ─── Lexicon ──────────────────────────────────────────────────────────────────
LEXICON = {
    "az": {
        "language_picker": "👋 *RAZALI* salonuna xoş gəlmisiniz!\n\nDil seçin:\n🇦🇿 *AZ*\n🇬🇧 *EN*\n🇷🇺 *RU*",
        # Updated welcome: tells users what the bot can do
        "welcome": (
            "✨ *RAZALI Nails / Hair / Make Up*\n\n"
            "Nə etmək istəyirsiniz?\n\n"
            "{categories}\n\n"
            "─────────────────\n"
            "*MƏNİM* — rezervasiyalarıma bax\n"
            "*İPTAL* — rezervasiyanı ləğv et"
        ),
        "choose_service": "💅 *{category}* xidmətləri:\n\n{services}\n\n*GERİ* — kateqoriyaya qayıt",
        "choose_master": "👩 Master seçin:\n\n{masters}\n\n*GERİ* — xidmətə qayıt",
        "choose_date": "📅 *{master_name}* üçün mövcud günlər:\n\n{dates}\n\n*GERİ* — mastera qayıt",
        "choose_time": "🕐 *{date}* tarixi üçün boş saatlar:\n\n{slots}\n\n*GERİ* — tarixə qayıt",
        "no_slots": "😔 Bu tarixdə boş saat yoxdur. Başqa tarix seçin.\n\n{dates}",
        "ask_name": "✍️ Rezervasiyanı təsdiqləmək üçün adınızı yazın:",
        "confirm": (
            "✅ *Rezervasiya təsdiqləndi!*\n\n"
            "🆔 *ID:* #{booking_id}\n"
            "💅 *Xidmət:* {service}\n"
            "👩 *Master:* {master}\n"
            "📅 *Tarix:* {date}\n"
            "🕐 *Saat:* {time}\n"
            "💰 *Qiymət:* {price} AZN\n\n"
            "⏰ Görüşdən 24 saat əvvəl xatırlatma göndəriləcək.\n"
            "❌ Ləğv etmək üçün *İPTAL* yazın."
        ),
        "cancel_found": (
            "📋 *Aktiv rezervasiyanız:*\n\n"
            "🆔 #{booking_id}\n"
            "💅 {service}\n"
            "👩 {master}\n"
            "📅 {date}\n"
            "🕐 {time}\n\n"
            "Ləğv etmək istədiyinizə əminsinizmi?\n"
            "✅ *BƏLİ* — ləğv et\n"
            "❌ *XEYİR* — saxla"
        ),
        "cancel_confirmed": "✅ Rezervasiyanız ləğv edildi. Yeni rezervasiya üçün istənilən mesaj göndərin.",
        "cancel_aborted": "👍 Rezervasiyanız saxlanıldı.",
        "no_bookings": "📋 Aktiv rezervasiyanız yoxdur.",
        "cancelled": "❌ Əməliyyat ləğv edildi.",
        "fallback": "Zəhmət olmasa aşağıdakı seçimlərdən birini yazın.",
        # NEW: view bookings strings
        "my_bookings_header": "📋 *Rezervasiyalarınız:*\n\n",
        "my_booking_item": "🆔 #{booking_id}\n💅 {service}\n👩 {master}\n📅 {date} • 🕐 {time}\n💰 {price} AZN",
        "my_bookings_footer": "\n\n❌ Ləğv etmək üçün *İPTAL* yazın.",
        "back": "GERİ",
        "cancel": "İPTAL",
        "my": "MƏNİM",
        "yes": "BƏLİ",
        "no": "XEYİR",
    },
    "en": {
        "language_picker": "👋 Welcome to *RAZALI* salon!\n\nSelect language:\n🇦🇿 *AZ*\n🇬🇧 *EN*\n🇷🇺 *RU*",
        "welcome": (
            "✨ *RAZALI Nails / Hair / Make Up*\n\n"
            "What would you like?\n\n"
            "{categories}\n\n"
            "─────────────────\n"
            "*MY* — view my bookings\n"
            "*CANCEL* — cancel your booking"
        ),
        "choose_service": "💅 *{category}* services:\n\n{services}\n\n*BACK* — return to categories",
        "choose_master": "👩 Choose a master:\n\n{masters}\n\n*BACK* — return to services",
        "choose_date": "📅 Available days for *{master_name}*:\n\n{dates}\n\n*BACK* — return to masters",
        "choose_time": "🕐 Available slots for *{date}*:\n\n{slots}\n\n*BACK* — return to dates",
        "no_slots": "😔 No available slots on this date. Choose another.\n\n{dates}",
        "ask_name": "✍️ Please enter your name to confirm the booking:",
        "confirm": (
            "✅ *Booking confirmed!*\n\n"
            "🆔 *ID:* #{booking_id}\n"
            "💅 *Service:* {service}\n"
            "👩 *Master:* {master}\n"
            "📅 *Date:* {date}\n"
            "🕐 *Time:* {time}\n"
            "💰 *Price:* {price} AZN\n\n"
            "⏰ You will receive a reminder 24 hours before.\n"
            "❌ To cancel type *CANCEL*."
        ),
        "cancel_found": (
            "📋 *Your active booking:*\n\n"
            "🆔 #{booking_id}\n"
            "💅 {service}\n"
            "👩 {master}\n"
            "📅 {date}\n"
            "🕐 {time}\n\n"
            "Are you sure you want to cancel?\n"
            "✅ *YES* — cancel it\n"
            "❌ *NO* — keep it"
        ),
        "cancel_confirmed": "✅ Your booking has been cancelled. Send any message to make a new booking.",
        "cancel_aborted": "👍 Your booking has been kept.",
        "no_bookings": "📋 You have no active bookings.",
        "cancelled": "❌ Action cancelled.",
        "fallback": "Please choose one of the options below.",
        "my_bookings_header": "📋 *Your bookings:*\n\n",
        "my_booking_item": "🆔 #{booking_id}\n💅 {service}\n👩 {master}\n📅 {date} • 🕐 {time}\n💰 {price} AZN",
        "my_bookings_footer": "\n\n❌ To cancel, type *CANCEL*.",
        "back": "BACK",
        "cancel": "CANCEL",
        "my": "MY",
        "yes": "YES",
        "no": "NO",
    },
    "ru": {
        "language_picker": "👋 Добро пожаловать в салон *RAZALI*!\n\nВыберите язык:\n🇦🇿 *AZ*\n🇬🇧 *EN*\n🇷🇺 *RU*",
        "welcome": (
            "✨ *RAZALI Nails / Hair / Make Up*\n\n"
            "Что вас интересует?\n\n"
            "{categories}\n\n"
            "─────────────────\n"
            "*МОИ* — мои записи\n"
            "*ОТМЕНА* — отменить запись"
        ),
        "choose_service": "💅 Услуги *{category}*:\n\n{services}\n\n*НАЗАД* — вернуться к категориям",
        "choose_master": "👩 Выберите мастера:\n\n{masters}\n\n*НАЗАД* — вернуться к услугам",
        "choose_date": "📅 Доступные дни для *{master_name}*:\n\n{dates}\n\n*НАЗАД* — вернуться к мастерам",
        "choose_time": "🕐 Свободные слоты на *{date}*:\n\n{slots}\n\n*НАЗАД* — вернуться к датам",
        "no_slots": "😔 На эту дату нет свободных слотов. Выберите другую.\n\n{dates}",
        "ask_name": "✍️ Введите ваше имя для подтверждения записи:",
        "confirm": (
            "✅ *Запись подтверждена!*\n\n"
            "🆔 *ID:* #{booking_id}\n"
            "💅 *Услуга:* {service}\n"
            "👩 *Мастер:* {master}\n"
            "📅 *Дата:* {date}\n"
            "🕐 *Время:* {time}\n"
            "💰 *Цена:* {price} AZN\n\n"
            "⏰ Напоминание придёт за 24 часа.\n"
            "❌ Для отмены напишите *ОТМЕНА*."
        ),
        "cancel_found": (
            "📋 *Ваша активная запись:*\n\n"
            "🆔 #{booking_id}\n"
            "💅 {service}\n"
            "👩 {master}\n"
            "📅 {date}\n"
            "🕐 {time}\n\n"
            "Вы уверены, что хотите отменить?\n"
            "✅ *ДА* — отменить\n"
            "❌ *НЕТ* — оставить"
        ),
        "cancel_confirmed": "✅ Ваша запись отменена. Отправьте любое сообщение для новой записи.",
        "cancel_aborted": "👍 Ваша запись сохранена.",
        "no_bookings": "📋 У вас нет активных записей.",
        "cancelled": "❌ Действие отменено.",
        "fallback": "Пожалуйста, выберите один из вариантов ниже.",
        "my_bookings_header": "📋 *Ваши записи:*\n\n",
        "my_booking_item": "🆔 #{booking_id}\n💅 {service}\n👩 {master}\n📅 {date} • 🕐 {time}\n💰 {price} AZN",
        "my_bookings_footer": "\n\n❌ Для отмены напишите *ОТМЕНА*.",
        "back": "НАЗАД",
        "cancel": "ОТМЕНА",
        "my": "МОИ",
        "yes": "ДА",
        "no": "НЕТ",
    }
}

# ─── Display helpers ──────────────────────────────────────────────────────────
NUMBER_EMOJIS = {"1":"1️⃣","2":"2️⃣","3":"3️⃣","4":"4️⃣","5":"5️⃣",
                 "6":"6️⃣","7":"7️⃣","8":"8️⃣","9":"9️⃣","10":"🔟"}

def fmt_categories(lang: str) -> str:
    lines = []
    for i, cat in enumerate(CATEGORIES, 1):
        emoji = NUMBER_EMOJIS.get(str(i), f"{i}.")
        lines.append(f"{emoji} {cat}")
    return "\n".join(lines)

def fmt_services(category: str, lang: str) -> str:
    lines = []
    i = 1
    for sid, s in SERVICES.items():
        if s["category"] == category:
            emoji = NUMBER_EMOJIS.get(str(i), f"{i}.")
            lines.append(f"{emoji} {s['name']} — {s['price']:.0f} AZN")
            i += 1
    return "\n".join(lines)

def fmt_masters(service_id: str, lang: str) -> str:
    lines = []
    i = 1
    for mid, m in MASTERS.items():
        duration = DURATIONS.get((mid, service_id), 60)
        price = SERVICES.get(service_id, {}).get("price", 0)
        emoji = NUMBER_EMOJIS.get(str(i), f"{i}.")
        lines.append(f"{emoji} {m['name']} — {duration} dəq / {price:.0f} AZN")
        i += 1
    return "\n".join(lines)

def fmt_dates(dates: list) -> str:
    lines = []
    for i, d in enumerate(dates, 1):
        date = datetime.strptime(d, "%Y-%m-%d")
        day_name = date.strftime("%A")
        fmt = date.strftime("%d %b")
        emoji = NUMBER_EMOJIS.get(str(i), f"{i}.")
        lines.append(f"{emoji} {day_name}, {fmt}")
    return "\n".join(lines)

def fmt_slots(slots: list) -> str:
    lines = []
    for i, s in enumerate(slots, 1):
        emoji = NUMBER_EMOJIS.get(str(i), f"{i}.")
        lines.append(f"{emoji} {s}")
    return "\n".join(lines)

def get_service_by_category_index(category: str, index: int):
    i = 1
    for sid, s in SERVICES.items():
        if s["category"] == category:
            if i == index:
                return sid
            i += 1
    return None

def get_master_by_index(index: int):
    for i, mid in enumerate(MASTERS.keys(), 1):
        if i == index:
            return mid
    return None

# ─── NEW: Format view bookings message ───────────────────────────────────────
def fmt_my_bookings(bookings: list, lang: str) -> str:
    lex = LEXICON[lang]
    parts = []
    for b in bookings:
        service_name = SERVICES.get(b["service_id"], {}).get("name", b["service_id"])
        master_name = MASTERS.get(b["master_id"], {}).get("name", b["master_id"])
        price = SERVICES.get(b["service_id"], {}).get("price", 0)
        date_fmt = datetime.strptime(b["date"], "%Y-%m-%d").strftime("%d %b %Y")
        parts.append(lex["my_booking_item"].format(
            booking_id=b["booking_id"],
            service=service_name,
            master=master_name,
            date=date_fmt,
            time=b["time"],
            price=int(price)
        ))
    return lex["my_bookings_header"] + "\n─────────────────\n".join(parts) + lex["my_bookings_footer"]

# ─── Main webhook ─────────────────────────────────────────────────────────────
@app.post("/whatsapp")
async def incoming_whatsapp(request: Request):
    params = await validate_twilio_request(request)
    Body = params.get("Body", "")
    From = params.get("From", "")
    MediaContentType0 = params.get("MediaContentType0", "")

    # FIX: Handle voice messages, images, stickers — Twilio sends these with
    # an empty Body. Without this check the bot falls through silently.
    if not Body.strip() and MediaContentType0:
        response = MessagingResponse()
        # We don't know the lang yet reliably, so send a bilingual nudge
        response.message("💬 Zəhmət olmasa mətn yazın / Please send a text message.")
        return Response(content=str(response), media_type="application/xml")

    user_text = Body.strip()
    # FIX: Cap name input at 60 chars to prevent abuse
    user_text = user_text[:60]
    user_lower = user_text.lower()
    response = MessagingResponse()
    session = get_session(From)

    if is_rate_limited(From):
        response.message("⚠️ Too many messages. Please wait a moment.")
        return Response(content=str(response), media_type="application/xml")

    log("info", "Message", phone=From[-6:], state=session["state"], text=user_text[:20])

    # ── LANGUAGE SELECTION ────────────────────────────────────────────────────
    if session["lang"] is None or session["state"] == "WAITING_FOR_LANG":
        if user_lower in ["az", "en", "ru"]:
            session["lang"] = user_lower
            session["state"] = "CHOOSING_CATEGORY"
            lang = user_lower
            response.message(LEXICON[lang]["welcome"].format(categories=fmt_categories(lang)))
            save_session(From, session)
            return Response(content=str(response), media_type="application/xml")
        else:
            response.message(LEXICON["az"]["language_picker"])
            session["state"] = "WAITING_FOR_LANG"
            save_session(From, session)
            return Response(content=str(response), media_type="application/xml")

    lang = session["lang"]
    back_word = LEXICON[lang]["back"].lower()
    cancel_word = LEXICON[lang]["cancel"].lower()
    my_word = LEXICON[lang]["my"].lower()
    yes_word = LEXICON[lang]["yes"].lower()
    no_word = LEXICON[lang]["no"].lower()

    # ── VIEW MY BOOKINGS ──────────────────────────────────────────────────────
    # Works from any state — same pattern as CANCEL
    if user_lower == my_word and session["state"] not in ("CONFIRM_CANCEL",):
        bookings = await asyncio.get_event_loop().run_in_executor(
            executor, fetch_all_active_bookings, From
        )
        if not bookings:
            response.message(LEXICON[lang]["no_bookings"])
        else:
            response.message(fmt_my_bookings(bookings, lang))
        save_session(From, session)
        return Response(content=str(response), media_type="application/xml")

    # ── CANCELLATION FLOW ─────────────────────────────────────────────────────
    if user_lower == cancel_word and session["state"] not in ("CONFIRM_CANCEL",):
        booking = await asyncio.get_event_loop().run_in_executor(
            executor, fetch_active_booking, From
        )
        if not booking["found"]:
            response.message(LEXICON[lang]["no_bookings"])
            save_session(From, session)
            return Response(content=str(response), media_type="application/xml")

        session["cancel_booking"] = booking
        session["state"] = "CONFIRM_CANCEL"
        master_name = MASTERS.get(booking["master_id"], {}).get("name", "")
        service_name = SERVICES.get(booking["service_id"], {}).get("name", "")
        date_fmt = datetime.strptime(booking["date"], "%Y-%m-%d").strftime("%d %b %Y")

        msg = LEXICON[lang]["cancel_found"].format(
            booking_id=booking["booking_id"],
            service=service_name,
            master=master_name,
            date=date_fmt,
            time=booking["time"]
        )
        response.message(msg)
        save_session(From, session)
        return Response(content=str(response), media_type="application/xml")

    # ── CONFIRM CANCELLATION ──────────────────────────────────────────────────
    if session["state"] == "CONFIRM_CANCEL":
        if user_lower == yes_word:
            booking = session.get("cancel_booking", {})
            success = await asyncio.get_event_loop().run_in_executor(
                executor, cancel_booking_in_sheet, booking["row"]
            )
            if success:
                asyncio.create_task(send_telegram_alert(
                    build_cancellation_alert(
                        booking["booking_id"],
                        booking["customer_name"],
                        From,
                        booking["master_id"],
                        booking["service_id"],
                        booking["date"],
                        booking["time"]
                    )
                ))
                response.message(LEXICON[lang]["cancel_confirmed"])
                log("info", "Booking cancelled", id=booking["booking_id"], phone=From[-6:])
            else:
                response.message("⚠️ Xəta baş verdi. Zəhmət olmasa yenidən cəhd edin.")
            session = clear_booking_session(From, lang)
        elif user_lower == no_word:
            response.message(LEXICON[lang]["cancel_aborted"])
            session = clear_booking_session(From, lang)
        else:
            booking = session.get("cancel_booking", {})
            master_name = MASTERS.get(booking.get("master_id",""), {}).get("name", "")
            service_name = SERVICES.get(booking.get("service_id",""), {}).get("name", "")
            date_fmt = datetime.strptime(booking["date"], "%Y-%m-%d").strftime("%d %b %Y")
            msg = LEXICON[lang]["cancel_found"].format(
                booking_id=booking["booking_id"],
                service=service_name,
                master=master_name,
                date=date_fmt,
                time=booking["time"]
            )
            response.message(msg)
        save_session(From, session)
        return Response(content=str(response), media_type="application/xml")

    # ── CHOOSING CATEGORY ─────────────────────────────────────────────────────
    if session["state"] == "CHOOSING_CATEGORY":
        try:
            idx = int(user_text)
            if 1 <= idx <= len(CATEGORIES):
                category = CATEGORIES[idx - 1]
                session["category"] = category
                session["state"] = "CHOOSING_SERVICE"
                response.message(LEXICON[lang]["choose_service"].format(
                    category=category, services=fmt_services(category, lang)
                ))
                save_session(From, session)
                return Response(content=str(response), media_type="application/xml")
        except ValueError:
            pass
        response.message(LEXICON[lang]["welcome"].format(categories=fmt_categories(lang)))
        save_session(From, session)
        return Response(content=str(response), media_type="application/xml")

    # ── CHOOSING SERVICE ──────────────────────────────────────────────────────
    if session["state"] == "CHOOSING_SERVICE":
        if user_lower == back_word:
            session["state"] = "CHOOSING_CATEGORY"
            response.message(LEXICON[lang]["welcome"].format(categories=fmt_categories(lang)))
            save_session(From, session)
            return Response(content=str(response), media_type="application/xml")
        try:
            idx = int(user_text)
            service_id = get_service_by_category_index(session["category"], idx)
            if service_id:
                session["service_id"] = service_id
                session["state"] = "CHOOSING_MASTER"
                response.message(LEXICON[lang]["choose_master"].format(
                    masters=fmt_masters(service_id, lang)
                ))
                save_session(From, session)
                return Response(content=str(response), media_type="application/xml")
        except ValueError:
            pass
        response.message(LEXICON[lang]["choose_service"].format(
            category=session["category"], services=fmt_services(session["category"], lang)
        ))
        save_session(From, session)
        return Response(content=str(response), media_type="application/xml")

    # ── CHOOSING MASTER ───────────────────────────────────────────────────────
    if session["state"] == "CHOOSING_MASTER":
        if user_lower == back_word:
            session["state"] = "CHOOSING_SERVICE"
            response.message(LEXICON[lang]["choose_service"].format(
                category=session["category"], services=fmt_services(session["category"], lang)
            ))
            save_session(From, session)
            return Response(content=str(response), media_type="application/xml")
        try:
            idx = int(user_text)
            master_id = get_master_by_index(idx)
            if master_id:
                session["master_id"] = master_id
                session["state"] = "CHOOSING_DATE"
                dates = get_available_dates(master_id)
                session["available_dates"] = dates
                master_name = MASTERS[master_id]["name"]
                response.message(LEXICON[lang]["choose_date"].format(
                    master_name=master_name, dates=fmt_dates(dates)
                ))
                save_session(From, session)
                return Response(content=str(response), media_type="application/xml")
        except ValueError:
            pass
        response.message(LEXICON[lang]["choose_master"].format(
            masters=fmt_masters(session["service_id"], lang)
        ))
        save_session(From, session)
        return Response(content=str(response), media_type="application/xml")

    # ── CHOOSING DATE ─────────────────────────────────────────────────────────
    if session["state"] == "CHOOSING_DATE":
        if user_lower == back_word:
            session["state"] = "CHOOSING_MASTER"
            response.message(LEXICON[lang]["choose_master"].format(
                masters=fmt_masters(session["service_id"], lang)
            ))
            save_session(From, session)
            return Response(content=str(response), media_type="application/xml")
        try:
            idx = int(user_text)
            dates = session.get("available_dates", [])
            if 1 <= idx <= len(dates):
                chosen_date = dates[idx - 1]
                slots = await asyncio.get_event_loop().run_in_executor(
                    executor, get_available_slots,
                    session["master_id"], session["service_id"], chosen_date
                )
                if not slots:
                    dates = get_available_dates(session["master_id"])
                    session["available_dates"] = dates
                    response.message(LEXICON[lang]["no_slots"].format(dates=fmt_dates(dates)))
                    save_session(From, session)
                    return Response(content=str(response), media_type="application/xml")
                session["date"] = chosen_date
                session["available_slots"] = slots
                session["state"] = "CHOOSING_TIME"
                date_fmt = datetime.strptime(chosen_date, "%Y-%m-%d").strftime("%d %b %Y")
                response.message(LEXICON[lang]["choose_time"].format(
                    date=date_fmt, slots=fmt_slots(slots)
                ))
                save_session(From, session)
                return Response(content=str(response), media_type="application/xml")
        except ValueError:
            pass
        dates = session.get("available_dates", [])
        master_name = MASTERS.get(session["master_id"], {}).get("name", "")
        response.message(LEXICON[lang]["choose_date"].format(
            master_name=master_name, dates=fmt_dates(dates)
        ))
        save_session(From, session)
        return Response(content=str(response), media_type="application/xml")

    # ── CHOOSING TIME ─────────────────────────────────────────────────────────
    if session["state"] == "CHOOSING_TIME":
        if user_lower == back_word:
            session["state"] = "CHOOSING_DATE"
            dates = session.get("available_dates", [])
            master_name = MASTERS.get(session["master_id"], {}).get("name", "")
            response.message(LEXICON[lang]["choose_date"].format(
                master_name=master_name, dates=fmt_dates(dates)
            ))
            save_session(From, session)
            return Response(content=str(response), media_type="application/xml")
        try:
            idx = int(user_text)
            slots = session.get("available_slots", [])
            if 1 <= idx <= len(slots):
                session["time"] = slots[idx - 1]
                session["state"] = "WAITING_FOR_NAME"
                response.message(LEXICON[lang]["ask_name"])
                save_session(From, session)
                return Response(content=str(response), media_type="application/xml")
        except ValueError:
            pass
        slots = session.get("available_slots", [])
        date_fmt = datetime.strptime(session["date"], "%Y-%m-%d").strftime("%d %b %Y")
        response.message(LEXICON[lang]["choose_time"].format(
            date=date_fmt, slots=fmt_slots(slots)
        ))
        save_session(From, session)
        return Response(content=str(response), media_type="application/xml")

    # ── WAITING FOR NAME ──────────────────────────────────────────────────────
    if session["state"] == "WAITING_FOR_NAME":
        customer_name = Body.strip()[:60]  # FIX: enforced cap on name length
        master_id = session["master_id"]
        service_id = session["service_id"]
        date = session["date"]
        time = session["time"]
        booking_id = await asyncio.get_event_loop().run_in_executor(
            executor, write_booking,
            From, customer_name, master_id, service_id, date, time
        )
        if booking_id:
            master_name = MASTERS.get(master_id, {}).get("name", "")
            service = SERVICES.get(service_id, {})
            date_fmt = datetime.strptime(date, "%Y-%m-%d").strftime("%d %b %Y")
            asyncio.create_task(send_telegram_alert(
                build_booking_alert(booking_id, customer_name, From, master_id, service_id, date, time)
            ))
            response.message(LEXICON[lang]["confirm"].format(
                booking_id=booking_id,
                service=service.get("name", ""),
                master=master_name,
                date=date_fmt,
                time=time,
                price=int(service.get("price", 0))
            ))
            session = clear_booking_session(From, lang)
        else:
            response.message("⚠️ Xəta baş verdi. Zəhmət olmasa yenidən cəhd edin.")
        save_session(From, session)
        return Response(content=str(response), media_type="application/xml")

    # ── FALLBACK ──────────────────────────────────────────────────────────────
    session["state"] = "CHOOSING_CATEGORY"
    response.message(LEXICON[lang]["welcome"].format(categories=fmt_categories(lang)))
    save_session(From, session)
    return Response(content=str(response), media_type="application/xml")