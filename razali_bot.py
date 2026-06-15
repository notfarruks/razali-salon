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
SESSION_TTL = 60 * 60 * 24  # 24 hours for booking sessions

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
        "customer_name": None
    }

def save_session(phone: str, session: dict):
    redis_client.setex(f"razali:session:{phone}", SESSION_TTL, json.dumps(session))

def clear_session(phone: str):
    session = get_session(phone)
    lang = session.get("lang")
    new_session = {
        "lang": lang,
        "state": "IDLE",
        "category": None,
        "service_id": None,
        "master_id": None,
        "date": None,
        "time": None,
        "customer_name": None
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

# ─── In-memory data cache ─────────────────────────────────────────────────────
MASTERS = {}       # {id: {name, mon, tue, ..., start_time, end_time}}
SERVICES = {}      # {id: {category, name, price}}
DURATIONS = {}     # {(master_id, service_id): duration_mins}
CATEGORIES = []    # ["Nails", "Hair", "Makeup"]

def load_data():
    global MASTERS, SERVICES, DURATIONS, CATEGORIES
    try:
        client = get_google_client()
        db = client.open("RAZALI_DB")

        # Load masters
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

        # Load services
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

        # Load durations
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

# ─── Slot availability engine ─────────────────────────────────────────────────
def get_available_slots(master_id: str, service_id: str, date_str: str) -> list:
    """Returns list of available time strings e.g. ['09:00', '10:00', ...]"""
    try:
        master = MASTERS.get(master_id)
        if not master:
            return []

        duration = DURATIONS.get((master_id, service_id), 60)
        date = datetime.strptime(date_str, "%Y-%m-%d")
        day_name = date.strftime("%a").lower()  # mon, tue, etc.

        # Check if master works this day
        if not master.get(day_name, False):
            return []

        # Build working hours slots (every 30 min)
        start_h, start_m = map(int, master["start_time"].split(":"))
        end_h, end_m = map(int, master["end_time"].split(":"))
        start = datetime(date.year, date.month, date.day, start_h, start_m)
        end = datetime(date.year, date.month, date.day, end_h, end_m)

        # Get existing bookings for this master on this date
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

        # Find free slots
        available = []
        current = start
        while current + timedelta(minutes=duration) <= end:
            slot_end = current + timedelta(minutes=duration)
            # Check if slot overlaps with any booking
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
    """Returns next 7 available working days for a master."""
    master = MASTERS.get(master_id)
    if not master:
        return []
    available = []
    today = datetime.now().date()
    check = today + timedelta(days=1)  # Start from tomorrow
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
        log("info", "Booking written", id=booking_id, master=master_id, date=date, time=time)
        return booking_id
    except Exception as e:
        log("error", "Booking write failed", error=str(e))
        return None

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

# ─── Lexicon ──────────────────────────────────────────────────────────────────
LEXICON = {
    "az": {
        "language_picker": "👋 *RAZALI* salonuna xoş gəlmisiniz!\n\nDil seçin:\n🇦🇿 *AZ*\n🇬🇧 *EN*\n🇷🇺 *RU*",
        "welcome": "✨ *RAZALI Nails / Hair / Make Up*\n\nNə etmək istəyirsiniz?\n\n{categories}\n\n*İPTAL* — rezervasiyanı ləğv et",
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
        "cancelled": "❌ Rezervasiya ləğv edildi. Yenidən başlamaq üçün istənilən mesaj göndərin.",
        "no_bookings": "📋 Aktiv rezervasiyanız yoxdur.",
        "fallback": "Zəhmət olmasa aşağıdakı seçimlərdən birini yazın.",
        "back": "GERİ",
        "cancel": "İPTAL",
    },
    "en": {
        "language_picker": "👋 Welcome to *RAZALI* salon!\n\nSelect language:\n🇦🇿 *AZ*\n🇬🇧 *EN*\n🇷🇺 *RU*",
        "welcome": "✨ *RAZALI Nails / Hair / Make Up*\n\nWhat would you like?\n\n{categories}\n\n*CANCEL* — cancel booking",
        "choose_service": "💅 *{category}* services:\n\n{services}\n\n*BACK* — return to categories",
        "choose_master": "👩 Choose a master:\n\n{masters}\n\n*BACK* — return to services",
        "choose_date": "📅 Available days for *{master_name}*:\n\n{dates}\n\n*BACK* — return to masters",
        "choose_time": "🕐 Available slots for *{date}*:\n\n{slots}\n\n*BACK* — return to dates",
        "no_slots": "😔 No available slots on this date. Choose another date.\n\n{dates}",
        "ask_name": "✍️ Please enter your name to confirm the booking:",
        "confirm": (
            "✅ *Booking confirmed!*\n\n"
            "🆔 *ID:* #{booking_id}\n"
            "💅 *Service:* {service}\n"
            "👩 *Master:* {master}\n"
            "📅 *Date:* {date}\n"
            "🕐 *Time:* {time}\n"
            "💰 *Price:* {price} AZN\n\n"
            "⏰ You will receive a reminder 24 hours before your appointment.\n"
            "❌ To cancel type *CANCEL*."
        ),
        "cancelled": "❌ Booking cancelled. Send any message to start again.",
        "no_bookings": "📋 You have no active bookings.",
        "fallback": "Please choose one of the options below.",
        "back": "BACK",
        "cancel": "CANCEL",
    },
    "ru": {
        "language_picker": "👋 Добро пожаловать в салон *RAZALI*!\n\nВыберите язык:\n🇦🇿 *AZ*\n🇬🇧 *EN*\n🇷🇺 *RU*",
        "welcome": "✨ *RAZALI Nails / Hair / Make Up*\n\nЧто вас интересует?\n\n{categories}\n\n*ОТМЕНА* — отменить запись",
        "choose_service": "💅 Услуги *{category}*:\n\n{services}\n\n*НАЗАД* — вернуться к категориям",
        "choose_master": "👩 Выберите мастера:\n\n{masters}\n\n*НАЗАД* — вернуться к услугам",
        "choose_date": "📅 Доступные дни для *{master_name}*:\n\n{dates}\n\n*НАЗАД* — вернуться к мастерам",
        "choose_time": "🕐 Свободные слоты на *{date}*:\n\n{slots}\n\n*НАЗАД* — вернуться к датам",
        "no_slots": "😔 На эту дату нет свободных слотов. Выберите другую дату.\n\n{dates}",
        "ask_name": "✍️ Введите ваше имя для подтверждения записи:",
        "confirm": (
            "✅ *Запись подтверждена!*\n\n"
            "🆔 *ID:* #{booking_id}\n"
            "💅 *Услуга:* {service}\n"
            "👩 *Мастер:* {master}\n"
            "📅 *Дата:* {date}\n"
            "🕐 *Время:* {time}\n"
            "💰 *Цена:* {price} AZN\n\n"
            "⏰ Напоминание придёт за 24 часа до записи.\n"
            "❌ Для отмены напишите *ОТМЕНА*."
        ),
        "cancelled": "❌ Запись отменена. Отправьте любое сообщение, чтобы начать снова.",
        "no_bookings": "📋 У вас нет активных записей.",
        "fallback": "Пожалуйста, выберите один из вариантов ниже.",
        "back": "НАЗАД",
        "cancel": "ОТМЕНА",
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
        service = SERVICES.get(service_id, {})
        price = service.get("price", 0)
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
    """Get service id by its position in a category."""
    i = 1
    for sid, s in SERVICES.items():
        if s["category"] == category:
            if i == index:
                return sid
            i += 1
    return None

def get_master_by_index(index: int):
    """Get master id by position."""
    for i, mid in enumerate(MASTERS.keys(), 1):
        if i == index:
            return mid
    return None

# ─── 24hr Reminder Scheduler ──────────────────────────────────────────────────
async def reminder_loop():
    """Runs every 30 minutes, sends reminders for appointments in ~24hrs."""
    while True:
        try:
            await asyncio.sleep(1800)  # every 30 minutes
            now = datetime.now()
            target = now + timedelta(hours=24)
            target_date = target.strftime("%Y-%m-%d")
            target_hour = target.strftime("%H")

            client = get_google_client()
            db = client.open("RAZALI_DB")
            sheet = db.worksheet("Bookings")
            all_bookings = sheet.get_all_records()

            for i, b in enumerate(all_bookings, 2):  # row 2 onwards
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
                        f"📍 RAZALI Nails / Hair / Make Up"
                    )

                    # Send WhatsApp reminder via Twilio
                    await send_whatsapp_reminder(phone, reminder_msg)

                    # Mark reminder as sent
                    sheet.update_cell(i, 9, "TRUE")
                    log("info", "Reminder sent", phone=phone[-6:], date=target_date, time=appt_time)

        except Exception as e:
            log("error", "Reminder loop error", error=str(e))

async def send_whatsapp_reminder(phone: str, message: str):
    """Send WhatsApp message via Twilio REST API."""
    account_sid = os.environ.get("TWILIO_ACCOUNT_SID")
    auth_token = os.environ.get("TWILIO_AUTH_TOKEN")
    from_number = os.environ.get("TWILIO_WHATSAPP_NUMBER", "whatsapp:+14155238886")
    if not account_sid or not auth_token:
        log("warning", "Twilio credentials missing for reminder")
        return
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                f"https://api.twilio.com/2010-04-01/Accounts/{account_sid}/Messages.json",
                data={
                    "From": from_number,
                    "To": phone,
                    "Body": message
                },
                auth=(account_sid, auth_token),
                timeout=10.0
            )
            if resp.status_code == 201:
                log("info", "WhatsApp reminder sent", phone=phone[-6:])
            else:
                log("error", "WhatsApp reminder failed", status=resp.status_code)
    except Exception as e:
        log("error", "WhatsApp reminder exception", error=str(e))

# ─── Startup ──────────────────────────────────────────────────────────────────
@app.on_event("startup")
async def startup_event():
    log("info", "RAZALI bot starting up...")
    load_data()
    asyncio.create_task(reminder_loop())
    log("info", "Reminder scheduler started")

# ─── Main webhook ─────────────────────────────────────────────────────────────
@app.post("/whatsapp")
async def incoming_whatsapp(request: Request):
    params = await validate_twilio_request(request)
    Body = params.get("Body", "")
    From = params.get("From", "")
    user_text = Body.strip()
    user_lower = user_text.lower()
    response = MessagingResponse()
    session = get_session(From)

    if is_rate_limited(From):
        response.message("⚠️ Too many messages. Please wait a moment.")
        return Response(content=str(response), media_type="application/xml")

    log("info", "Message received", phone=From[-6:], state=session["state"], text=user_text[:20])

    # ── LANGUAGE SELECTION ────────────────────────────────────────────────────
    if session["lang"] is None or session["state"] == "WAITING_FOR_LANG":
        if user_lower in ["az", "en", "ru"]:
            session["lang"] = user_lower
            session["state"] = "CHOOSING_CATEGORY"
            lang = user_lower
            msg = LEXICON[lang]["welcome"].format(categories=fmt_categories(lang))
            response.message(msg)
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

    # ── GLOBAL CANCEL ─────────────────────────────────────────────────────────
    if user_lower == cancel_word:
        session = clear_session(From)
        session["lang"] = lang
        session["state"] = "CHOOSING_CATEGORY"
        response.message(LEXICON[lang]["cancelled"])
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
                msg = LEXICON[lang]["choose_service"].format(
                    category=category,
                    services=fmt_services(category, lang)
                )
                response.message(msg)
                save_session(From, session)
                return Response(content=str(response), media_type="application/xml")
        except ValueError:
            pass
        msg = LEXICON[lang]["welcome"].format(categories=fmt_categories(lang))
        response.message(msg)
        save_session(From, session)
        return Response(content=str(response), media_type="application/xml")

    # ── CHOOSING SERVICE ──────────────────────────────────────────────────────
    if session["state"] == "CHOOSING_SERVICE":
        if user_lower == back_word:
            session["state"] = "CHOOSING_CATEGORY"
            msg = LEXICON[lang]["welcome"].format(categories=fmt_categories(lang))
            response.message(msg)
            save_session(From, session)
            return Response(content=str(response), media_type="application/xml")
        try:
            idx = int(user_text)
            service_id = get_service_by_category_index(session["category"], idx)
            if service_id:
                session["service_id"] = service_id
                session["state"] = "CHOOSING_MASTER"
                msg = LEXICON[lang]["choose_master"].format(
                    masters=fmt_masters(service_id, lang)
                )
                response.message(msg)
                save_session(From, session)
                return Response(content=str(response), media_type="application/xml")
        except ValueError:
            pass
        msg = LEXICON[lang]["choose_service"].format(
            category=session["category"],
            services=fmt_services(session["category"], lang)
        )
        response.message(msg)
        save_session(From, session)
        return Response(content=str(response), media_type="application/xml")

    # ── CHOOSING MASTER ───────────────────────────────────────────────────────
    if session["state"] == "CHOOSING_MASTER":
        if user_lower == back_word:
            session["state"] = "CHOOSING_SERVICE"
            msg = LEXICON[lang]["choose_service"].format(
                category=session["category"],
                services=fmt_services(session["category"], lang)
            )
            response.message(msg)
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
                msg = LEXICON[lang]["choose_date"].format(
                    master_name=master_name,
                    dates=fmt_dates(dates)
                )
                response.message(msg)
                save_session(From, session)
                return Response(content=str(response), media_type="application/xml")
        except ValueError:
            pass
        msg = LEXICON[lang]["choose_master"].format(
            masters=fmt_masters(session["service_id"], lang)
        )
        response.message(msg)
        save_session(From, session)
        return Response(content=str(response), media_type="application/xml")

    # ── CHOOSING DATE ─────────────────────────────────────────────────────────
    if session["state"] == "CHOOSING_DATE":
        if user_lower == back_word:
            session["state"] = "CHOOSING_MASTER"
            msg = LEXICON[lang]["choose_master"].format(
                masters=fmt_masters(session["service_id"], lang)
            )
            response.message(msg)
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
                    msg = LEXICON[lang]["no_slots"].format(dates=fmt_dates(dates))
                    response.message(msg)
                    save_session(From, session)
                    return Response(content=str(response), media_type="application/xml")
                session["date"] = chosen_date
                session["available_slots"] = slots
                session["state"] = "CHOOSING_TIME"
                date_fmt = datetime.strptime(chosen_date, "%Y-%m-%d").strftime("%d %b %Y")
                msg = LEXICON[lang]["choose_time"].format(
                    date=date_fmt,
                    slots=fmt_slots(slots)
                )
                response.message(msg)
                save_session(From, session)
                return Response(content=str(response), media_type="application/xml")
        except ValueError:
            pass
        dates = session.get("available_dates", [])
        master_name = MASTERS.get(session["master_id"], {}).get("name", "")
        msg = LEXICON[lang]["choose_date"].format(
            master_name=master_name,
            dates=fmt_dates(dates)
        )
        response.message(msg)
        save_session(From, session)
        return Response(content=str(response), media_type="application/xml")

    # ── CHOOSING TIME ─────────────────────────────────────────────────────────
    if session["state"] == "CHOOSING_TIME":
        if user_lower == back_word:
            session["state"] = "CHOOSING_DATE"
            dates = session.get("available_dates", [])
            master_name = MASTERS.get(session["master_id"], {}).get("name", "")
            msg = LEXICON[lang]["choose_date"].format(
                master_name=master_name,
                dates=fmt_dates(dates)
            )
            response.message(msg)
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
        msg = LEXICON[lang]["choose_time"].format(
            date=date_fmt,
            slots=fmt_slots(slots)
        )
        response.message(msg)
        save_session(From, session)
        return Response(content=str(response), media_type="application/xml")

    # ── WAITING FOR NAME ──────────────────────────────────────────────────────
    if session["state"] == "WAITING_FOR_NAME":
        customer_name = Body.strip()
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

            msg = LEXICON[lang]["confirm"].format(
                booking_id=booking_id,
                service=service.get("name", ""),
                master=master_name,
                date=date_fmt,
                time=time,
                price=int(service.get("price", 0))
            )
            response.message(msg)
            session = clear_session(From)
            session["lang"] = lang
            session["state"] = "CHOOSING_CATEGORY"
        else:
            response.message("⚠️ Xəta baş verdi. Zəhmət olmasa yenidən cəhd edin.")

        save_session(From, session)
        return Response(content=str(response), media_type="application/xml")

    # ── FALLBACK ──────────────────────────────────────────────────────────────
    session["state"] = "CHOOSING_CATEGORY"
    msg = LEXICON[lang]["welcome"].format(categories=fmt_categories(lang))
    response.message(msg)
    save_session(From, session)
    return Response(content=str(response), media_type="application/xml")
