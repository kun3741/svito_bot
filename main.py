import asyncio
import logging
import json
import ssl
import os
from datetime import datetime, timedelta
from pathlib import Path
from dotenv import load_dotenv
from aiogram import Bot, Dispatcher, F
from aiogram.types import (
    Message, CallbackQuery, 
    InlineKeyboardMarkup, InlineKeyboardButton,
    ReplyKeyboardMarkup, KeyboardButton
)
from aiogram.filters import Command
from aiogram.enums import ParseMode
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
import aiohttp
from aiohttp import web
from motor.motor_asyncio import AsyncIOMotorClient

# --- КОНФІГУРАЦІЯ ---
load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DB_NAME = os.getenv("DB_NAME", "lumos_bot")
CHECK_INTERVAL = int(os.getenv("CHECK_INTERVAL", "45"))
PORT = int(os.getenv("PORT", "8080"))
BASE_DIR = Path(__file__).resolve().parent

APQE_PQFRTY = os.getenv("APQE_PQFRTY")
APSRC_PFRTY = os.getenv("APSRC_PFRTY")

# Список черг для моніторингу
QUEUES = [
    "1.1", "1.2",
    "2.1", "2.2",
    "3.1", "3.2",
    "4.1", "4.2",
    "5.1", "5.2",
    "6.1", "6.2"
]

# Тексти кнопок
BTN_CHECK = "🔄 Перевірити графік"
BTN_MY_QUEUE = "📋 Мої підписки"
BTN_SET_QUEUE = "⚡ Обрати черги"
BTN_CHANGE_QUEUE = "✏️ Керувати чергами"
BTN_HELP = "❓ Допомога"
BTN_DONATE = "💛 Підтримати проєкт"

# Посилання на донат
DONATE_URL = "https://send.monobank.ua/jar/5N86nkGZ1R"  # Замінити на справжнє посилання
DONATE_TEXT = "[💛 Підтримай розвиток проєкту]({url})"

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# --- MongoDB ---
mongo_client: AsyncIOMotorClient = None
db = None

async def init_db():
    """Ініціалізація підключення до MongoDB"""
    global mongo_client, db
    try:
        mongo_client = AsyncIOMotorClient(MONGO_URI)
        db = mongo_client[DB_NAME]
        
        # Перевірка з'єднання
        await mongo_client.admin.command('ping')
        
        # Перевірка колекцій
        collections = await db.list_collection_names()
        logging.info(f"✅ Connected to MongoDB. Collections: {collections}")
        
        # Рахуємо документи
        users_count = await db.users.count_documents({})
        states_count = await db.schedule_state.count_documents({})
        logging.info(f"📊 Users: {users_count}, Schedule states: {states_count}")
        
    except Exception as e:
        logging.error(f"❌ MongoDB connection failed: {e}")
        raise

async def close_db():
    """Закриття підключення до MongoDB"""
    global mongo_client
    if mongo_client:
        mongo_client.close()
        logging.info("MongoDB connection closed")

# --- FSM СТАНИ ---
class AddressForm(StatesGroup):
    waiting_for_city = State()
    waiting_for_street = State()

# --- РОБОТА З БАЗОЮ ДАНИХ ---
async def get_user_data(user_id: int) -> dict | None:
    """Отримує дані користувача з MongoDB"""
    user = await db.users.find_one({"user_id": user_id})
    if user:
        # Підтримка старого формату (queue як str) та нового (queues як list)
        queues = user.get("queues", [])
        if not queues and user.get("queue"):
            queues = [user.get("queue")]  # Міграція старого формату
        return {
            "queues": queues, 
            "address": user.get("address"),
            "reminders": user.get("reminders", True),  # За замовчуванням увімкнено
            "reminder_intervals": user.get("reminder_intervals", DEFAULT_REMINDER_INTERVALS)
        }
    return None

async def set_user_data(user_id: int, queues: list[str], address: str = None):
    """Зберігає дані користувача в MongoDB"""
    await db.users.update_one(
        {"user_id": user_id},
        {"$set": {"queues": queues, "address": address, "updated_at": datetime.now()}, "$unset": {"queue": ""}},
        upsert=True
    )

async def add_queue_to_user(user_id: int, queue: str, address: str = None):
    """Додає чергу до підписок користувача"""
    user_data = await get_user_data(user_id)
    if user_data:
        queues = user_data.get("queues", [])
        if queue not in queues:
            queues.append(queue)
        # Зберігаємо адресу тільки якщо передана
        addr = address if address else user_data.get("address")
        await set_user_data(user_id, queues, addr)
    else:
        await set_user_data(user_id, [queue], address)

async def remove_queue_from_user(user_id: int, queue: str):
    """Видаляє конкретну чергу з підписок користувача"""
    user_data = await get_user_data(user_id)
    if user_data:
        queues = user_data.get("queues", [])
        if queue in queues:
            queues.remove(queue)
        await set_user_data(user_id, queues, user_data.get("address"))

async def get_user_queues(user_id: int) -> list[str]:
    """Отримує список черг користувача"""
    data = await get_user_data(user_id)
    if data:
        return data.get("queues", [])
    return []

async def remove_user_queue(user_id: int):
    """Видаляє всі підписки користувача"""
    await db.users.delete_one({"user_id": user_id})

async def get_users_by_queue(queue: str) -> list[int]:
    """Повертає список user_id підписаних на певну чергу"""
    # Пошук в масиві queues або в старому полі queue
    cursor = db.users.find({"$or": [{"queues": queue}, {"queue": queue}]})
    users = await cursor.to_list(length=None)
    return [user["user_id"] for user in users]

async def toggle_user_reminders(user_id: int) -> bool:
    """Перемикає стан нагадувань користувача, повертає новий стан"""
    user = await db.users.find_one({"user_id": user_id})
    current_state = user.get("reminders", True) if user else True
    new_state = not current_state
    
    await db.users.update_one(
        {"user_id": user_id},
        {"$set": {"reminders": new_state}},
        upsert=True
    )
    return new_state

async def get_user_reminders_state(user_id: int) -> bool:
    """Повертає стан нагадувань користувача"""
    user = await db.users.find_one({"user_id": user_id})
    return user.get("reminders", True) if user else True

async def get_user_reminder_intervals(user_id: int) -> list[int]:
    """Повертає обрані інтервали нагадувань користувача"""
    user = await db.users.find_one({"user_id": user_id})
    return user.get("reminder_intervals", DEFAULT_REMINDER_INTERVALS) if user else DEFAULT_REMINDER_INTERVALS

async def toggle_reminder_interval(user_id: int, interval: int) -> list[int]:
    """Перемикає інтервал нагадувань, повертає новий список"""
    user = await db.users.find_one({"user_id": user_id})
    intervals = user.get("reminder_intervals", DEFAULT_REMINDER_INTERVALS.copy()) if user else DEFAULT_REMINDER_INTERVALS.copy()
    
    if interval in intervals:
        intervals.remove(interval)
    else:
        intervals.append(interval)
        intervals.sort(reverse=True)  # Сортуємо від більшого до меншого
    
    await db.users.update_one(
        {"user_id": user_id},
        {"$set": {"reminder_intervals": intervals}},
        upsert=True
    )
    return intervals

async def get_schedule_state(queue_id: str) -> str | None:
    """Отримує збережений стан графіку для черги"""
    try:
        state = await db.schedule_state.find_one({"queue_id": queue_id})
        if state:
            return state.get("data_hash")
        return None
    except Exception as e:
        logging.error(f"Error getting schedule state for {queue_id}: {e}")
        return None

async def save_schedule_state(queue_id: str, data_hash: str):
    """Зберігає стан графіку для черги"""
    try:
        await db.schedule_state.update_one(
            {"queue_id": queue_id},
            {"$set": {"data_hash": data_hash, "updated_at": datetime.now()}},
            upsert=True
        )
    except Exception as e:
        logging.error(f"Error saving schedule state for {queue_id}: {e}")

# --- НАГАДУВАННЯ ---
# Доступні інтервали нагадувань (хвилини)
AVAILABLE_REMINDER_INTERVALS = {
    5: "5 хв",
    10: "10 хв",
    15: "15 хв",
    30: "30 хв",
    60: "1 год",
    120: "2 год"
}
DEFAULT_REMINDER_INTERVALS = [60, 30, 15, 5]  # За замовчуванням

async def get_sent_reminder(user_id: int, queue_id: str, event_time: str, event_type: str, minutes: int) -> bool:
    """Перевіряє чи було відправлено нагадування"""
    reminder = await db.reminders.find_one({
        "user_id": user_id,
        "queue_id": queue_id,
        "event_time": event_time,
        "event_type": event_type,
        "minutes": minutes
    })
    return reminder is not None

async def mark_reminder_sent(user_id: int, queue_id: str, event_time: str, event_type: str, minutes: int):
    """Позначає нагадування як відправлене"""
    await db.reminders.update_one(
        {
            "user_id": user_id,
            "queue_id": queue_id,
            "event_time": event_time,
            "event_type": event_type,
            "minutes": minutes
        },
        {"$set": {"sent_at": datetime.now()}},
        upsert=True
    )

async def cleanup_old_reminders():
    """Видаляє старі нагадування (старші 2 днів)"""
    try:
        cutoff = datetime.now() - timedelta(days=2)
        result = await db.reminders.delete_many({"sent_at": {"$lt": cutoff}})
        if result.deleted_count > 0:
            logging.info(f"Cleaned {result.deleted_count} old reminders")
    except Exception as e:
        logging.error(f"Error cleaning reminders: {e}")

# --- КЛАВІАТУРИ ---
def get_main_keyboard(has_queue: bool = False) -> ReplyKeyboardMarkup:
    """Головна клавіатура (Reply Keyboard)"""
    queue_btn = BTN_CHANGE_QUEUE if has_queue else BTN_SET_QUEUE
    
    buttons = [
        [KeyboardButton(text=BTN_CHECK), KeyboardButton(text=BTN_MY_QUEUE)],
        [KeyboardButton(text=queue_btn), KeyboardButton(text=BTN_HELP)],
        [KeyboardButton(text=BTN_DONATE)],
    ]
    return ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)

def get_queue_choice_keyboard(reminders_on: bool = True) -> InlineKeyboardMarkup:
    """Вибір способу встановлення черги"""
    reminder_text = "🔔 Нагадування: ВКЛ" if reminders_on else "🔕 Нагадування: ВИКЛ"
    buttons = [
        [InlineKeyboardButton(text="🏠 Додати за адресою", callback_data="enter_address")],
        [InlineKeyboardButton(text="🔢 Обрати зі списку", callback_data="select_queue")],
        [InlineKeyboardButton(text=reminder_text, callback_data="toggle_reminders")],
        [InlineKeyboardButton(text="⏰ Налаштувати нагадування", callback_data="reminder_settings")],
        [InlineKeyboardButton(text="🗑 Скасувати всі підписки", callback_data="unsubscribe")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def get_reminder_intervals_keyboard(selected_intervals: list[int]) -> InlineKeyboardMarkup:
    """Клавіатура вибору інтервалів нагадувань"""
    buttons = []
    row = []
    
    for interval, label in AVAILABLE_REMINDER_INTERVALS.items():
        # Позначаємо обрані інтервали галочкою
        text = f"✅ {label}" if interval in selected_intervals else f"⬜ {label}"
        row.append(InlineKeyboardButton(text=text, callback_data=f"reminder_int_{interval}"))
        if len(row) == 3:
            buttons.append(row)
            row = []
    
    if row:
        buttons.append(row)
    
    buttons.append([InlineKeyboardButton(text="◀️ Назад", callback_data="back_choice")])
    
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def get_queue_list_keyboard(subscribed_queues: list[str] = None) -> InlineKeyboardMarkup:
    """Клавіатура вибору черги зі списку (з позначенням підписаних)"""
    if subscribed_queues is None:
        subscribed_queues = []
    
    buttons = []
    row = []
    for queue in QUEUES:
        # Позначаємо підписані черги галочкою
        text = f"✅ {queue}" if queue in subscribed_queues else f"{queue}"
        row.append(InlineKeyboardButton(text=text, callback_data=f"queue_{queue}"))
        if len(row) == 4:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)
    
    buttons.append([InlineKeyboardButton(text="✔️ Готово", callback_data="done_select")])
    buttons.append([InlineKeyboardButton(text="◀️ Назад", callback_data="back_choice")])
    
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def get_cancel_keyboard() -> InlineKeyboardMarkup:
    """Клавіатура скасування"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="❌ Скасувати", callback_data="cancel_input")]
    ])

def get_donate_keyboard() -> InlineKeyboardMarkup:
    """Кнопка підтримки під повідомленнями"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💛 Підтримати проєкт", callback_data="show_donate")]
    ])

# --- ОТРИМАННЯ ДАНИХ ---
def get_ssl_context():
    ssl_context = ssl.create_default_context()
    ssl_context.set_ciphers('DEFAULT@SECLEVEL=1')
    return ssl_context

async def fetch_schedule(session, queue_id):
    if not APQE_PQFRTY:
        logging.error("APQE_PQFRTY not set!")
        return None
    
    params = {'queue': queue_id}
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json",
        "Referer": "https://svitlo.oe.if.ua/"
    }
    
    ssl_context = get_ssl_context()
    connector = aiohttp.TCPConnector(ssl=ssl_context)
    
    try:
        async with aiohttp.ClientSession(connector=connector) as session:
            async with session.get(APQE_PQFRTY, params=params, headers=headers) as response:
                if response.status == 200:
                    return await response.json()
                else:
                    text = await response.text()
                    logging.error(f"API returned {response.status} for queue {queue_id}: {text[:200]}")
                    return None
    except Exception as e:
        logging.error(f"Error fetching {queue_id}: {e}")
        return None

async def fetch_schedule_by_address(city: str, street: str, house: str) -> dict | None:
    if not APSRC_PFRTY:
        logging.error("APSRC_PFRTY not set!")
        return None
    
    address = f"{city},{street},{house}"
    
    payload = {
        'accountNumber': '',
        'userSearchChoice': 'pob',
        'address': address
    }
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Referer": "https://svitlo.oe.if.ua/",
        "Origin": "https://svitlo.oe.if.ua"
    }
    
    ssl_context = get_ssl_context()
    connector = aiohttp.TCPConnector(ssl=ssl_context)
    
    try:
        async with aiohttp.ClientSession(connector=connector) as session:
            async with session.post(APSRC_PFRTY, data=payload, headers=headers) as response:
                if response.status == 200:
                    data = await response.json()
                    logging.info(f"Address search result for '{address}': {data}")
                    return data
                else:
                    text = await response.text()
                    logging.error(f"Address search failed: {response.status}, response: {text[:200]}")
                    return None
    except Exception as e:
        logging.error(f"Error searching by address: {e}")
        return None

def extract_queue_from_response(data) -> tuple[str | None, list | None]:
    if not data or not isinstance(data, dict):
        return None, None
    
    current = data.get("current", {})
    schedule = data.get("schedule", [])
    
    if current.get("hasQueue") != "yes":
        return None, None
    
    queue_num = current.get("queue")
    sub_queue = current.get("subQueue")
    
    if queue_num is not None and sub_queue is not None:
        queue_id = f"{queue_num}.{sub_queue}"
        if queue_id in QUEUES:
            return queue_id, schedule
    
    return None, None

# --- ФОРМАТУВАННЯ ПОВІДОМЛЕННЯ ---
def format_notification(queue_id, data, is_update=True, address=None):
    """Форматує повідомлення з графіками на ВСІ доступні дати"""
    if not data or not isinstance(data, list):
        return f"⚠️ Отримано некоректні дані для черги {queue_id}"

    days_names = ["Понеділок", "Вівторок", "Середа", "Четвер", "П'ятниця", "Субота", "Неділя"]
    
    header = "⚡️ *Оновлення ГПВ!*" if is_update else "📊 *Поточний графік*"
    address_line = f"📍 *Адреса:* {address}\n" if address else ""
    
    text = f"{header}\n\n{address_line}🔢 *Черга:* {queue_id}\n"
    
    # Обробляємо кожну дату
    for record in data:
        event_date = record.get("eventDate", "Невідомо")
        approved_since = record.get("scheduleApprovedSince", "")
        
        # День тижня
        day_name = ""
        try:
            day, month, year = event_date.split('.')
            dt = datetime(int(year), int(month), int(day))
            day_name = days_names[dt.weekday()]
        except:
            pass
        
        queue_data = record.get("queues", {}).get(queue_id, [])
        
        schedule_lines = []
        if queue_data:
            for slot in queue_data:
                start = slot.get('from', '??')
                end = slot.get('to', '??')
                
                # Тривалість
                duration_str = ""
                try:
                    start_h, start_m = map(int, start.split(':'))
                    end_h, end_m = map(int, end.split(':'))
                    start_minutes = start_h * 60 + start_m
                    end_minutes = end_h * 60 + end_m
                    if end_minutes == 0:
                        end_minutes = 24 * 60
                    diff_minutes = end_minutes - start_minutes
                    if diff_minutes > 0:
                        h = diff_minutes // 60
                        m = diff_minutes % 60
                        duration_str = f" ({h} год)" if m == 0 else f" ({h} год {m} хв)"
                except:
                    pass
                
                schedule_lines.append(f"  🔴 {start} - {end}{duration_str}")
            
            schedule_str = "\n".join(schedule_lines)
        else:
            schedule_str = "  ✅ Відключень не заплановано"
        
        text += f"\n📅 *{event_date}* _{day_name}_\n{schedule_str}\n"
    
    # Час затвердження (беремо з останнього запису)
    if data:
        last_approved = data[-1].get("scheduleApprovedSince", "")
        if last_approved:
            text += f"\n🕒 _Затверджено: {last_approved}_"
    
    return text

def format_user_status(user_data) -> str:
    """Форматує статус користувача"""
    if user_data:
        if isinstance(user_data, dict):
            queues = user_data.get("queues", [])
            # Підтримка старого формату
            if not queues and user_data.get("queue"):
                queues = [user_data.get("queue")]
            
            address = user_data.get("address")
            reminders = user_data.get("reminders", True)
            reminder_intervals = user_data.get("reminder_intervals", DEFAULT_REMINDER_INTERVALS)
            
            if not queues:
                return "⚠️ Підписку не налаштовано"
            
            queues_str = ", ".join(sorted(queues))
            queues_count = len(queues)
            queues_label = "Черги" if queues_count > 1 else "Черга"
            
            if reminders and reminder_intervals:
                intervals_labels = [AVAILABLE_REMINDER_INTERVALS.get(i, f"{i} хв") for i in sorted(reminder_intervals, reverse=True)]
                reminders_str = ", ".join(intervals_labels)
            elif reminders:
                reminders_str = "ВКЛ (без інтервалів)"
            else:
                reminders_str = "ВИКЛ"
            
            lines = []
            if address:
                lines.append(f"📍 *Адреса:* {address}")
            lines.append(f"🔢 *{queues_label}:* {queues_str}")
            lines.append(f"⏰ *Нагадування:* {reminders_str}")
            
            return "\n".join(lines)
        else:
            return f"🔢 *Черга:* {user_data}"
    return "⚠️ Підписку не налаштовано"

# --- ХЕНДЛЕРИ КОМАНД ---
@dp.message(Command("start"))
async def cmd_start(message: Message, state: FSMContext):
    await state.clear()
    user_data = await get_user_data(message.from_user.id)
    queues = user_data.get("queues", []) if user_data else []
    has_queue = len(queues) > 0
    
    if has_queue:
        status = format_user_status(user_data)
        text = (
            f"💡 *З поверненням, {message.from_user.first_name}!*\n\n"
            f"{status}\n\n"
            f"Я повідомлю тебе, якщо графік зміниться ⚡"
        )
    else:
        text = (
            f"💡 *Привіт, {message.from_user.first_name}!*\n\n"
            f"Я *Люмос* — допоможу тобі дізнаватись про відключення першим!\n\n"
            f"⚡ Обирай свою чергу і будь готовим до відключень.\n\n"
            f"🤔 Не знаєш чергу? Натискай кнопку «⚡ Обрати чергу» — я допоможу все знайти!"
        )
    
    await message.answer(text, reply_markup=get_main_keyboard(has_queue), parse_mode=ParseMode.MARKDOWN)

@dp.message(Command("help"))
async def cmd_help(message: Message):
    text = (
        "📚 *Як користуватися ботом:*\n\n"
        f"*{BTN_CHECK}* - переглянути графіки ваших черг\n"
        f"*{BTN_MY_QUEUE}* - інформація про ваші підписки\n"
        f"*{BTN_SET_QUEUE}/{BTN_CHANGE_QUEUE}* - керувати чергами\n\n"
        "🔔 *Як це працює:*\n"
        "1. Введіть адресу або оберіть черги зі списку\n"
        "2. Можна відслідковувати кілька черг одночасно\n"
        "3. Бот автоматично перевіряє графіки\n"
        "4. При змінах вам прийде сповіщення\n\n"
        "⏰ *Нагадування:*\n"
        "Обирайте інтервали: 5, 10, 15, 30 хв, 1 або 2 год\n"
        "Налаштувати можна в меню керування чергами"
    )
    await message.answer(text, parse_mode=ParseMode.MARKDOWN)

# --- ХЕНДЛЕРИ КНОПОК КЛАВІАТУРИ ---
@dp.message(F.text == BTN_CHECK)
async def btn_check(message: Message):
    user_queues = await get_user_queues(message.from_user.id)
    
    if not user_queues:
        reminders_on = await get_user_reminders_state(message.from_user.id)
        await message.answer(
            "⚠️ Спочатку оберіть чергу!",
            reply_markup=get_queue_choice_keyboard(reminders_on),
            parse_mode=ParseMode.MARKDOWN
        )
        return
    
    loading_msg = await message.answer("⏳ Завантажую графіки...")
    
    ssl_context = get_ssl_context()
    connector = aiohttp.TCPConnector(ssl=ssl_context)
    
    user_data = await get_user_data(message.from_user.id)
    address = user_data.get("address") if isinstance(user_data, dict) else None
    
    async with aiohttp.ClientSession(connector=connector) as session:
        results = []
        for queue in sorted(user_queues):
            data = await fetch_schedule(session, queue)
            if data:
                msg = format_notification(queue, data, is_update=False, address=address if len(user_queues) == 1 else None)
                results.append(msg)
        
        await loading_msg.delete()
        
        if results:
            for i, msg in enumerate(results):
                # Додаємо кнопку донату до останнього повідомлення
                if i == len(results) - 1:
                    await message.answer(msg, parse_mode=ParseMode.MARKDOWN, reply_markup=get_donate_keyboard())
                else:
                    await message.answer(msg, parse_mode=ParseMode.MARKDOWN)
                await asyncio.sleep(0.3)
        else:
            await message.answer("❌ Не вдалося отримати дані. Спробуйте пізніше.")

@dp.message(F.text == BTN_MY_QUEUE)
async def btn_my_queue(message: Message):
    user_data = await get_user_data(message.from_user.id)
    queues = user_data.get("queues", []) if user_data else []
    status = format_user_status(user_data)
    
    if queues:
        count = len(queues)
        plural = "черг" if count > 1 else "чергу"
        text = f"✅ *Ваші підписки:*\n\n{status}\n\n🔔 Ви відслідковуєте {count} {plural}.\nПри змінах прийде сповіщення."
    else:
        text = f"⚠️ *Підписку не налаштовано*\n\nОберіть чергу, щоб отримувати сповіщення."
    
    await message.answer(text, parse_mode=ParseMode.MARKDOWN)

@dp.message(F.text.in_({BTN_SET_QUEUE, BTN_CHANGE_QUEUE}))
async def btn_set_queue(message: Message, state: FSMContext):
    await state.clear()
    user_data = await get_user_data(message.from_user.id)
    queues = user_data.get("queues", []) if user_data else []
    reminders_on = user_data.get("reminders", True) if user_data else True
    
    if queues:
        status = format_user_status(user_data)
        text = f"✏️ *Керування підписками*\n\n*Поточні підписки:*\n{status}\n\nОберіть спосіб:"
    else:
        text = "⚡ *Оберіть спосіб налаштування:*"
    
    await message.answer(text, reply_markup=get_queue_choice_keyboard(reminders_on), parse_mode=ParseMode.MARKDOWN)

@dp.message(F.text == BTN_HELP)
async def btn_help(message: Message):
    await cmd_help(message)

def get_donate_text() -> str:
    """Повертає текст про донати"""
    return (
        "💛 *Підтримай розвиток проєкту!*\n\n"
        "🆓 *Люмос — повністю безкоштовний* і таким залишиться назавжди.\n\n"
        "Кожен донат — добровільний, але саме ваша підтримка допомагає "
        "робити бота кращим: додавати нові функції, покращувати стабільність "
        "та забезпечувати безперебійну роботу. 🙏\n\n"
        f"🔗 {DONATE_URL}"
    )

@dp.message(F.text == BTN_DONATE)
async def btn_donate(message: Message):
    await message.answer(get_donate_text(), parse_mode=ParseMode.MARKDOWN)

@dp.callback_query(F.data == "show_donate")
async def cb_show_donate(callback: CallbackQuery):
    """Показує повідомлення про донати"""
    await callback.message.answer(get_donate_text(), parse_mode=ParseMode.MARKDOWN)
    await callback.answer()

# --- FSM ХЕНДЛЕРИ ДЛЯ АДРЕСИ ---
@dp.message(AddressForm.waiting_for_city)
async def process_city(message: Message, state: FSMContext):
    city = message.text.strip()
    await state.update_data(city=city)
    await state.set_state(AddressForm.waiting_for_street)
    
    text = (
        f"🏙 Місто: *{city}*\n\n"
        f"🏠 *Тепер введіть вулицю та номер будинку:*\n\n"
        f"Формат: `Вулиця, Номер`\n"
        f"Наприклад: `Паркова, 7    `"
    )
    await message.answer(text, reply_markup=get_cancel_keyboard(), parse_mode=ParseMode.MARKDOWN)

@dp.message(AddressForm.waiting_for_street)
async def process_street(message: Message, state: FSMContext):
    input_text = message.text.strip()
    
    if ',' in input_text:
        parts = input_text.split(',', 1)
        street = parts[0].strip()
        house = parts[1].strip()
    else:
        parts = input_text.rsplit(' ', 1)
        if len(parts) == 2:
            street = parts[0].strip()
            house = parts[1].strip()
        else:
            await message.answer(
                "⚠️ Не вдалося розпізнати формат.\n\n"
                "Введіть у форматі: `Вулиця, Номер`\n"
                "Наприклад: `Бельведерська, 65`",
                reply_markup=get_cancel_keyboard(),
                parse_mode=ParseMode.MARKDOWN
            )
            return
    
    data = await state.get_data()
    city = data.get('city')
    
    full_address = f"{city}, {street}, {house}"
    
    loading_msg = await message.answer(f"⏳ Шукаю чергу для адреси:\n*{full_address}*...", parse_mode=ParseMode.MARKDOWN)
    
    result = await fetch_schedule_by_address(city, street, house)
    
    await loading_msg.delete()
    
    if result:
        queue, schedule = extract_queue_from_response(result)
        
        if queue and schedule:
            await add_queue_to_user(message.from_user.id, queue, full_address)
            await state.clear()
            
            user_queues = await get_user_queues(message.from_user.id)
            queues_str = ", ".join(sorted(user_queues))
            
            text = (
                f"✅ *Адресу знайдено!*\n\n"
                f"📍 *Адреса:* {full_address}\n"
                f"🔢 *Черга:* {queue}\n\n"
                f"📋 *Всі ваші черги:* {queues_str}\n"
                f"🔔 Тепер ви отримуватимете сповіщення про зміни в графіку."
            )
            await message.answer(text, reply_markup=get_main_keyboard(has_queue=True), parse_mode=ParseMode.MARKDOWN)
            
            msg = format_notification(queue, schedule, is_update=False, address=full_address)
            await message.answer(msg, parse_mode=ParseMode.MARKDOWN, reply_markup=get_donate_keyboard())
        else:
            await state.clear()
            reminders_on = await get_user_reminders_state(message.from_user.id)
            await message.answer(
                "⚠️ Не вдалося визначити чергу для цієї адреси.\n\n"
                "Спробуйте ввести адресу ще раз або оберіть чергу вручну.",
                reply_markup=get_queue_choice_keyboard(reminders_on),
                parse_mode=ParseMode.MARKDOWN
            )
    else:
        await state.clear()
        reminders_on = await get_user_reminders_state(message.from_user.id)
        await message.answer(
            "❌ Адресу не знайдено.\n\n"
            "Перевірте правильність написання та спробуйте ще раз.",
            reply_markup=get_queue_choice_keyboard(reminders_on),
            parse_mode=ParseMode.MARKDOWN
        )

# --- CALLBACK ХЕНДЛЕРИ ---
@dp.callback_query(F.data == "enter_address")
async def cb_enter_address(callback: CallbackQuery, state: FSMContext):
    await state.set_state(AddressForm.waiting_for_city)
    text = (
        "🏙 *Введіть назву міста/села:*\n\n"
        "Наприклад: `Івано-Франківськ`"
    )
    await callback.message.edit_text(text, reply_markup=get_cancel_keyboard(), parse_mode=ParseMode.MARKDOWN)
    await callback.answer()

@dp.callback_query(F.data == "cancel_input")
async def cb_cancel_input(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    user_queues = await get_user_queues(callback.from_user.id)
    has_queue = len(user_queues) > 0
    
    await callback.message.edit_text("❌ *Введення скасовано*", parse_mode=ParseMode.MARKDOWN)
    await callback.message.answer("Оберіть дію:", reply_markup=get_main_keyboard(has_queue))
    await callback.answer()

@dp.callback_query(F.data == "select_queue")
async def cb_select_queue(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    user_queues = await get_user_queues(callback.from_user.id)
    text = "🔢 *Оберіть черги для відслідковування:*\n\n✅ — підписані\nНатисніть на чергу щоб додати/видалити"
    await callback.message.edit_text(text, reply_markup=get_queue_list_keyboard(user_queues), parse_mode=ParseMode.MARKDOWN)
    await callback.answer()

@dp.callback_query(F.data == "back_choice")
async def cb_back_choice(callback: CallbackQuery):
    user_data = await get_user_data(callback.from_user.id)
    queues = user_data.get("queues", []) if user_data else []
    reminders_on = user_data.get("reminders", True) if user_data else True
    
    if queues:
        status = format_user_status(user_data)
        text = f"✏️ *Керування підписками*\n\n*Поточні підписки:*\n{status}\n\nОберіть спосіб:"
    else:
        text = "⚡ *Оберіть спосіб налаштування:*"
    
    await callback.message.edit_text(text, reply_markup=get_queue_choice_keyboard(reminders_on), parse_mode=ParseMode.MARKDOWN)
    await callback.answer()

@dp.callback_query(F.data.startswith("queue_"))
async def cb_queue_select(callback: CallbackQuery):
    queue = callback.data.replace("queue_", "")
    
    if queue not in QUEUES:
        await callback.answer("❌ Невідома черга!", show_alert=True)
        return
    
    user_queues = await get_user_queues(callback.from_user.id)
    
    # Тогл - якщо є, видаляємо, якщо немає - додаємо
    if queue in user_queues:
        await remove_queue_from_user(callback.from_user.id, queue)
        await callback.answer(f"➖ Черга {queue} видалена")
    else:
        await add_queue_to_user(callback.from_user.id, queue)
        await callback.answer(f"➕ Черга {queue} додана")
    
    # Оновлюємо клавіатуру
    user_queues = await get_user_queues(callback.from_user.id)
    text = "🔢 *Оберіть черги для відслідковування:*\n\n✅ — підписані\nНатисніть на чергу щоб додати/видалити"
    await callback.message.edit_text(text, reply_markup=get_queue_list_keyboard(user_queues), parse_mode=ParseMode.MARKDOWN)

@dp.callback_query(F.data == "done_select")
async def cb_done_select(callback: CallbackQuery):
    """Завершення вибору черг"""
    user_queues = await get_user_queues(callback.from_user.id)
    has_queue = len(user_queues) > 0
    
    if has_queue:
        queues_str = ", ".join(sorted(user_queues))
        count = len(user_queues)
        plural = "черг" if count > 1 else "чергу"
        
        text = (
            f"✅ *Підписки оновлено!*\n\n"
            f"🔢 *Ви відслідковуєте {count} {plural}:* {queues_str}\n\n"
            f"🔔 Тепер ви отримуватимете сповіщення про зміни в графіках."
        )
        await callback.message.edit_text(text, parse_mode=ParseMode.MARKDOWN)
        await callback.message.answer("Меню оновлено:", reply_markup=get_main_keyboard(has_queue=True))
        
        # Показуємо поточні графіки для обраних черг
        ssl_context = get_ssl_context()
        connector = aiohttp.TCPConnector(ssl=ssl_context)
        
        async with aiohttp.ClientSession(connector=connector) as session:
            sorted_queues = sorted(user_queues)
            for i, queue in enumerate(sorted_queues):
                data = await fetch_schedule(session, queue)
                if data:
                    msg = format_notification(queue, data, is_update=False)
                    # Додаємо кнопку донату до останнього повідомлення
                    if i == len(sorted_queues) - 1:
                        await callback.message.answer(msg, parse_mode=ParseMode.MARKDOWN, reply_markup=get_donate_keyboard())
                    else:
                        await callback.message.answer(msg, parse_mode=ParseMode.MARKDOWN)
                    await asyncio.sleep(0.3)
    else:
        reminders_on = await get_user_reminders_state(callback.from_user.id)
        text = "⚠️ *Ви не обрали жодної черги*\n\nОберіть хоча б одну чергу для відслідковування."
        await callback.message.edit_text(text, reply_markup=get_queue_choice_keyboard(reminders_on), parse_mode=ParseMode.MARKDOWN)
    
    await callback.answer()

@dp.callback_query(F.data == "toggle_reminders")
async def cb_toggle_reminders(callback: CallbackQuery):
    """Перемикає стан нагадувань"""
    new_state = await toggle_user_reminders(callback.from_user.id)
    
    user_data = await get_user_data(callback.from_user.id)
    queues = user_data.get("queues", []) if user_data else []
    
    if queues:
        status = format_user_status(user_data)
        text = f"✏️ *Керування підписками*\n\n*Поточні підписки:*\n{status}\n\nОберіть спосіб:"
    else:
        text = "⚡ *Оберіть спосіб налаштування:*"
    
    await callback.message.edit_text(text, reply_markup=get_queue_choice_keyboard(new_state), parse_mode=ParseMode.MARKDOWN)
    
    state_text = "увімкнено" if new_state else "вимкнено"
    await callback.answer(f"🔔 Нагадування {state_text}!")

@dp.callback_query(F.data == "reminder_settings")
async def cb_reminder_settings(callback: CallbackQuery):
    """Показує налаштування інтервалів нагадувань"""
    intervals = await get_user_reminder_intervals(callback.from_user.id)
    
    # Формуємо текст обраних інтервалів
    if intervals:
        selected = [AVAILABLE_REMINDER_INTERVALS[i] for i in sorted(intervals, reverse=True) if i in AVAILABLE_REMINDER_INTERVALS]
        selected_text = ", ".join(selected)
    else:
        selected_text = "не обрано"
    
    text = (
        "⏰ *Налаштування нагадувань*\n\n"
        f"*Обрані інтервали:* {selected_text}\n\n"
        "Натисніть на інтервал щоб додати/видалити:"
    )
    
    await callback.message.edit_text(text, reply_markup=get_reminder_intervals_keyboard(intervals), parse_mode=ParseMode.MARKDOWN)
    await callback.answer()

@dp.callback_query(F.data.startswith("reminder_int_"))
async def cb_toggle_reminder_interval(callback: CallbackQuery):
    """Перемикає інтервал нагадування"""
    interval = int(callback.data.replace("reminder_int_", ""))
    
    if interval not in AVAILABLE_REMINDER_INTERVALS:
        await callback.answer("❌ Невідомий інтервал!", show_alert=True)
        return
    
    new_intervals = await toggle_reminder_interval(callback.from_user.id, interval)
    
    # Формуємо текст
    if new_intervals:
        selected = [AVAILABLE_REMINDER_INTERVALS[i] for i in sorted(new_intervals, reverse=True) if i in AVAILABLE_REMINDER_INTERVALS]
        selected_text = ", ".join(selected)
    else:
        selected_text = "не обрано"
    
    text = (
        "⏰ *Налаштування нагадувань*\n\n"
        f"*Обрані інтервали:* {selected_text}\n\n"
        "Натисніть на інтервал щоб додати/видалити:"
    )
    
    await callback.message.edit_text(text, reply_markup=get_reminder_intervals_keyboard(new_intervals), parse_mode=ParseMode.MARKDOWN)
    
    label = AVAILABLE_REMINDER_INTERVALS[interval]
    if interval in new_intervals:
        await callback.answer(f"✅ {label} додано")
    else:
        await callback.answer(f"➖ {label} видалено")

@dp.callback_query(F.data == "unsubscribe")
async def cb_unsubscribe(callback: CallbackQuery):
    user_queues = await get_user_queues(callback.from_user.id)
    
    if not user_queues:
        await callback.answer("ℹ️ У вас немає активних підписок", show_alert=True)
        return
    
    await remove_user_queue(callback.from_user.id)
    
    text = "🔕 *Всі підписки скасовано*\n\nВи більше не отримуватимете сповіщення."
    await callback.message.edit_text(text, parse_mode=ParseMode.MARKDOWN)
    await callback.message.answer("Меню оновлено:", reply_markup=get_main_keyboard(has_queue=False))
    await callback.answer("✅ Підписки скасовано")

# --- ОСНОВНИЙ ЦИКЛ ПЕРЕВІРКИ ---
def extract_all_schedules(data, queue_id: str) -> dict:
    """
    Витягує графіки для ВСІХ дат.
    Повертає: {"20.01.2026": [...hours...], "21.01.2026": [...hours...]}
    """
    result = {}
    
    if not data or not isinstance(data, list):
        return result
    
    for record in data:
        event_date = record.get("eventDate")
        if not event_date:
            continue
            
        queue_hours = record.get("queues", {}).get(queue_id, [])
        
        simplified_hours = []
        for slot in queue_hours:
            simplified_hours.append({
                "from": slot.get("from"),
                "to": slot.get("to"),
                "status": slot.get("status")
            })
        
        result[event_date] = simplified_hours
    
    return result

def format_schedule_notification(queue_id: str, date: str, hours: list, change_type: str, address: str = None) -> str:
    """
    Форматує сповіщення про зміну графіку.
    change_type: "new" | "updated"
    """
    # День тижня
    day_name = ""
    try:
        day, month, year = date.split('.')
        dt = datetime(int(year), int(month), int(day))
        days = ["Понеділок", "Вівторок", "Середа", "Четвер", "П'ятниця", "Субота", "Неділя"]
        day_name = days[dt.weekday()]
    except:
        pass

    # Заголовок
    if change_type == "new":
        header = f"📅 *Додано новий графік на {date}*"
    else:
        header = f"🔄 *Оновлено графік на {date}*"
    
    # Години
    schedule_lines = []
    if hours:
        for slot in hours:
            start = slot.get('from', '??')
            end = slot.get('to', '??')
            
            # Тривалість
            duration_str = ""
            try:
                start_h, start_m = map(int, start.split(':'))
                end_h, end_m = map(int, end.split(':'))
                start_minutes = start_h * 60 + start_m
                end_minutes = end_h * 60 + end_m
                if end_minutes == 0:
                    end_minutes = 24 * 60
                diff_minutes = end_minutes - start_minutes
                if diff_minutes > 0:
                    h = diff_minutes // 60
                    m = diff_minutes % 60
                    duration_str = f" ({h} год)" if m == 0 else f" ({h} год {m} хв)"
            except:
                pass
            
            schedule_lines.append(f"🔴 {start} - {end}{duration_str}")
        
        schedule_str = "\n".join(schedule_lines)
    else:
        schedule_str = "✅ Відключень не заплановано"
    
    address_line = f"📍 {address}\n" if address else ""

    text = (
        f"{header}\n"
        f"_{day_name}_\n\n"
        f"{address_line}"
        f"🔢 Черга: *{queue_id}*\n\n"
        f"{schedule_str}"
    )
    return text

async def scheduled_checker():
    logging.info("🚀 Monitor started")
    await asyncio.sleep(10)
    
    while True:
        for queue_id in QUEUES:
            data = await fetch_schedule(None, queue_id)
            if not data:
                continue

            # Витягуємо графіки для всіх дат
            current_schedules = extract_all_schedules(data, queue_id)
            if not current_schedules:
                continue
            
            # Завантажуємо збережений стан
            saved_state_json = await get_schedule_state(queue_id)
            saved_schedules = {}
            if saved_state_json:
                try:
                    saved_schedules = json.loads(saved_state_json)
                except:
                    saved_schedules = {}
            
            # Очищення старих дат (до сьогодні)
            today = datetime.now().date()
            old_dates = []
            for date_str in list(saved_schedules.keys()):
                try:
                    day, month, year = date_str.split('.')
                    date_obj = datetime(int(year), int(month), int(day)).date()
                    if date_obj < today:
                        old_dates.append(date_str)
                        del saved_schedules[date_str]
                except:
                    pass
            
            if old_dates:
                logging.info(f"Cleaned old dates for {queue_id}: {old_dates}")
            
            # Порівнюємо кожну дату окремо
            changes = []  # [(date, hours, "new"|"updated"), ...]
            
            for date, hours in current_schedules.items():
                current_hash = json.dumps(hours, sort_keys=True)
                
                if date not in saved_schedules:
                    # Нова дата - новий графік
                    changes.append((date, hours, "new"))
                    logging.info(f"New schedule for {queue_id} on {date}")
                elif saved_schedules[date] != current_hash:
                    # Дата є, але графік змінився
                    changes.append((date, hours, "updated"))
                    logging.info(f"Updated schedule for {queue_id} on {date}")
                
                # Оновлюємо збережений стан
                saved_schedules[date] = current_hash
            
            # Якщо є зміни - надсилаємо сповіщення
            if changes:
                subscribers = await get_users_by_queue(queue_id)
                
                if subscribers:
                    for user_id in subscribers:
                        try:
                            user_data = await get_user_data(user_id)
                            address = user_data.get("address") if isinstance(user_data, dict) else None
                            
                            # Надсилаємо окреме повідомлення для кожної зміненої дати
                            for i, (date, hours, change_type) in enumerate(changes):
                                msg = format_schedule_notification(queue_id, date, hours, change_type, address)
                                # Додаємо кнопку донату до останнього повідомлення
                                if i == len(changes) - 1:
                                    await bot.send_message(user_id, msg, parse_mode=ParseMode.MARKDOWN, reply_markup=get_donate_keyboard())
                                else:
                                    await bot.send_message(user_id, msg, parse_mode=ParseMode.MARKDOWN)
                                await asyncio.sleep(0.3)
                            
                            logging.info(f"Notifications sent to {user_id} for queue {queue_id}")
                        except Exception as e:
                            logging.error(f"Failed to send to {user_id}: {e}")
                        
                        await asyncio.sleep(0.5)
                
                # Зберігаємо оновлений стан
                await save_schedule_state(queue_id, json.dumps(saved_schedules))
            
            await asyncio.sleep(1)
        
        logging.info(f"Check completed. Next check in {CHECK_INTERVAL} seconds")
        await asyncio.sleep(CHECK_INTERVAL)

async def reminder_checker():
    """Перевіряє та надсилає нагадування про наближення подій"""
    logging.info("⏰ Reminder checker started")
    await asyncio.sleep(30)  # Початкова затримка
    
    while True:
        try:
            now = datetime.now()
            today_str = now.strftime("%d.%m.%Y")
            
            # Очищення старих нагадувань раз на добу (о 3:00)
            if now.hour == 3 and now.minute < 2:
                await cleanup_old_reminders()
            
            # Отримуємо всіх користувачів з підписками та увімкненими нагадуваннями
            cursor = db.users.find({
                "queues": {"$exists": True, "$ne": []},
                "$or": [{"reminders": True}, {"reminders": {"$exists": False}}]  # За замовчуванням увімкнено
            })
            users = await cursor.to_list(length=None)
            
            for user in users:
                user_id = user["user_id"]
                queues = user.get("queues", [])
                user_intervals = user.get("reminder_intervals", DEFAULT_REMINDER_INTERVALS)
                
                # Пропускаємо якщо користувач не обрав жодного інтервалу
                if not user_intervals:
                    continue
                
                for queue_id in queues:
                    # Отримуємо графік для черги
                    data = await fetch_schedule(None, queue_id)
                    if not data:
                        continue
                    
                    # Знаходимо графік на сьогодні
                    schedule_data = data if isinstance(data, list) else data.get("schedule", [])
                    
                    for record in schedule_data:
                        event_date = record.get("eventDate", "")
                        if event_date != today_str:
                            continue
                        
                        queue_data = record.get("queues", {}).get(queue_id, [])
                        
                        for slot in queue_data:
                            from_time = slot.get("from", "")
                            to_time = slot.get("to", "")
                            
                            if not from_time or not to_time:
                                continue
                            
                            # Перевіряємо нагадування для ВИМКНЕННЯ (from_time)
                            await check_and_send_reminder(
                                user_id, queue_id, today_str, from_time, "off", now, user_intervals
                            )
                            
                            # Перевіряємо нагадування для УВІМКНЕННЯ (to_time)
                            await check_and_send_reminder(
                                user_id, queue_id, today_str, to_time, "on", now, user_intervals
                            )
                    
                    await asyncio.sleep(0.1)
                
                await asyncio.sleep(0.1)
            
        except Exception as e:
            logging.error(f"Reminder checker error: {e}")
        
        # Перевіряємо кожну хвилину
        await asyncio.sleep(60)

async def check_and_send_reminder(user_id: int, queue_id: str, date_str: str, time_str: str, event_type: str, now: datetime, user_intervals: list[int]):
    """Перевіряє та надсилає нагадування якщо потрібно"""
    try:
        # Парсимо час події
        day, month, year = date_str.split('.')
        hour, minute = time_str.split(':')
        event_time = datetime(int(year), int(month), int(day), int(hour), int(minute))
        
        # Різниця в хвилинах
        diff = (event_time - now).total_seconds() / 60
        
        # Перевіряємо кожен інтервал нагадування (тільки ті, що обрав користувач)
        for minutes in user_intervals:
            # Нагадування актуальне якщо залишилось від (minutes-1) до (minutes+1) хвилин
            if minutes - 1 <= diff <= minutes + 1:
                # Перевіряємо чи вже відправлено
                event_key = f"{date_str}_{time_str}"
                already_sent = await get_sent_reminder(user_id, queue_id, event_key, event_type, minutes)
                
                if not already_sent:
                    # Форматуємо повідомлення
                    if event_type == "off":
                        emoji = "⚡🔴"
                        action = "вимкнення"
                    else:
                        emoji = "💡🟢"
                        action = "увімкнення"
                    
                    if minutes >= 60:
                        hours = minutes // 60
                        time_text = f"{hours} год" if hours == 1 else f"{hours} год"
                    else:
                        time_text = f"{minutes} хв"
                    
                    msg = (
                        f"{emoji} *Нагадування!*\n\n"
                        f"Через *{time_text}* о *{time_str}* — {action} світла\n"
                        f"🔢 Черга: *{queue_id}*"
                    )
                    
                    try:
                        await bot.send_message(user_id, msg, parse_mode=ParseMode.MARKDOWN)
                        await mark_reminder_sent(user_id, queue_id, event_key, event_type, minutes)
                        logging.info(f"Reminder sent: {user_id}, {queue_id}, {event_type} in {minutes}min at {time_str}")
                    except Exception as e:
                        logging.error(f"Failed to send reminder to {user_id}: {e}")
                
                break  # Відправляємо тільки одне нагадування за раз
                
    except Exception as e:
        logging.error(f"Error in check_and_send_reminder: {e}")

# --- ВЕБ-СЕРВЕР ---
async def get_users_count() -> int:
    """Повертає кількість користувачів"""
    try:
        count = await db.users.count_documents({})
        return count
    except:
        return 0

async def handle_index(request):
    """Головна сторінка"""
    template_path = BASE_DIR / "templates" / "index.html"
    
    try:
        with open(template_path, "r", encoding="utf-8") as f:
            html = f.read()
        
        users_count = await get_users_count()
        html = html.replace("{{users_count}}", str(users_count))
        html = html.replace("{{check_interval}}", str(CHECK_INTERVAL))
        
        return web.Response(text=html, content_type="text/html")
    except Exception as e:
        logging.error(f"Error loading template: {e}")
        return web.Response(text="Lumos Bot is running!", content_type="text/plain")

async def handle_health(request):
    """Health check для Render"""
    return web.json_response({
        "status": "ok",
        "service": "lumos-bot",
        "timestamp": datetime.now().isoformat()
    })

async def start_web_server():
    """Запуск веб-сервера"""
    app = web.Application()
    app.router.add_get("/", handle_index)
    app.router.add_get("/health", handle_health)
    
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    logging.info(f"🌐 Web server started on port {PORT}")

async def main():
    logging.info("🤖 Bot starting...")
    logging.info(f"📋 Config: APQE_PQFRTY={'SET' if APQE_PQFRTY else 'NOT SET'}, APSRC_PFRTY={'SET' if APSRC_PFRTY else 'NOT SET'}")
    logging.info(f"📋 MongoDB: {MONGO_URI[:20]}...")
    await init_db()
    
    try:
        # Запускаємо веб-сервер
        await start_web_server()
        
        # Запускаємо моніторинг графіків
        asyncio.create_task(scheduled_checker())
        
        # Запускаємо нагадування
        asyncio.create_task(reminder_checker())
        
        # Запускаємо бота
        await dp.start_polling(bot)
    finally:
        await close_db()

if __name__ == "__main__":
    asyncio.run(main())
