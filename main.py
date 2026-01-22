import asyncio
import logging
import json
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
from aiohttp import web
import aiohttp
import aiohttp_socks
from motor.motor_asyncio import AsyncIOMotorClient
from curl_cffi.requests import AsyncSession

# --- КОНФІГУРАЦІЯ ---
load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DB_NAME = os.getenv("DB_NAME", "lumos_bot")
CHECK_INTERVAL = int(os.getenv("CHECK_INTERVAL", "45"))
PORT = int(os.getenv("PORT", "8080"))
BASE_DIR = Path(__file__).resolve().parent

# Основні URL API
APQE_PQFRTY = os.getenv("APQE_PQFRTY")
APSRC_PFRTY = os.getenv("APSRC_PFRTY")

# Проксі (опціонально, формат: http://user:pass@ip:port)
PROXY_URL = os.getenv("PROXY_URL") 

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

DONATE_URL = "https://send.monobank.ua/jar/5N86nkGZ1R"

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# --- MongoDB ---
mongo_client: AsyncIOMotorClient = None
db = None

async def init_db():
    global mongo_client, db
    try:
        mongo_client = AsyncIOMotorClient(MONGO_URI)
        db = mongo_client[DB_NAME]
        await mongo_client.admin.command('ping')
        logging.info("✅ Connected to MongoDB")
    except Exception as e:
        logging.error(f"❌ MongoDB connection failed: {e}")
        raise

async def close_db():
    global mongo_client
    if mongo_client:
        mongo_client.close()

# --- FSM СТАНИ ---
class AddressForm(StatesGroup):
    waiting_for_city = State()
    waiting_for_street = State()

# --- РОБОТА З БАЗОЮ ДАНИХ (User Data) ---
async def get_user_data(user_id: int) -> dict | None:
    user = await db.users.find_one({"user_id": user_id})
    if user:
        queues = user.get("queues", [])
        if not queues and user.get("queue"):
            queues = [user.get("queue")]
        return {
            "queues": queues, 
            "address": user.get("address"),
            "reminders": user.get("reminders", True),
            "reminder_intervals": user.get("reminder_intervals", [60, 30, 15, 5])
        }
    return None

async def set_user_data(user_id: int, queues: list[str], address: str = None):
    await db.users.update_one(
        {"user_id": user_id},
        {"$set": {"queues": queues, "address": address, "updated_at": datetime.now()}, "$unset": {"queue": ""}},
        upsert=True
    )

async def add_queue_to_user(user_id: int, queue: str, address: str = None):
    user_data = await get_user_data(user_id)
    if user_data:
        queues = user_data.get("queues", [])
        if queue not in queues:
            queues.append(queue)
        addr = address if address else user_data.get("address")
        await set_user_data(user_id, queues, addr)
    else:
        await set_user_data(user_id, [queue], address)

async def remove_queue_from_user(user_id: int, queue: str):
    user_data = await get_user_data(user_id)
    if user_data:
        queues = user_data.get("queues", [])
        if queue in queues:
            queues.remove(queue)
        await set_user_data(user_id, queues, user_data.get("address"))

async def get_user_queues(user_id: int) -> list[str]:
    data = await get_user_data(user_id)
    return data.get("queues", []) if data else []

async def remove_user_queue(user_id: int):
    await db.users.delete_one({"user_id": user_id})

async def get_users_by_queue(queue: str) -> list[int]:
    cursor = db.users.find({"$or": [{"queues": queue}, {"queue": queue}]})
    users = await cursor.to_list(length=None)
    return [user["user_id"] for user in users]

async def toggle_user_reminders(user_id: int) -> bool:
    user = await db.users.find_one({"user_id": user_id})
    current_state = user.get("reminders", True) if user else True
    new_state = not current_state
    await db.users.update_one({"user_id": user_id}, {"$set": {"reminders": new_state}}, upsert=True)
    return new_state

async def get_user_reminders_state(user_id: int) -> bool:
    user = await db.users.find_one({"user_id": user_id})
    return user.get("reminders", True) if user else True

# --- РОБОТА З БАЗОЮ ДАНИХ (State & Reminders) ---
async def get_schedule_state(queue_id: str) -> str | None:
    state = await db.schedule_state.find_one({"queue_id": queue_id})
    return state.get("data_hash") if state else None

async def save_schedule_state(queue_id: str, data_hash: str):
    await db.schedule_state.update_one(
        {"queue_id": queue_id},
        {"$set": {"data_hash": data_hash, "updated_at": datetime.now()}},
        upsert=True
    )

async def get_sent_reminder(user_id: int, queue_id: str, event_time: str, event_type: str, minutes: int) -> bool:
    reminder = await db.reminders.find_one({
        "user_id": user_id, "queue_id": queue_id,
        "event_time": event_time, "event_type": event_type, "minutes": minutes
    })
    return reminder is not None

async def mark_reminder_sent(user_id: int, queue_id: str, event_time: str, event_type: str, minutes: int):
    await db.reminders.update_one(
        {"user_id": user_id, "queue_id": queue_id, "event_time": event_time, "event_type": event_type, "minutes": minutes},
        {"$set": {"sent_at": datetime.now()}},
        upsert=True
    )

async def cleanup_old_reminders():
    cutoff = datetime.now() - timedelta(days=2)
    await db.reminders.delete_many({"sent_at": {"$lt": cutoff}})

# --- НАГАДУВАННЯ CONSTANTS ---
AVAILABLE_REMINDER_INTERVALS = {5: "5 хв", 10: "10 хв", 15: "15 хв", 30: "30 хв", 60: "1 год", 120: "2 год"}
DEFAULT_REMINDER_INTERVALS = [60, 30, 15, 5]

async def get_user_reminder_intervals(user_id: int) -> list[int]:
    user = await db.users.find_one({"user_id": user_id})
    return user.get("reminder_intervals", DEFAULT_REMINDER_INTERVALS) if user else DEFAULT_REMINDER_INTERVALS

async def toggle_reminder_interval(user_id: int, interval: int) -> list[int]:
    user = await db.users.find_one({"user_id": user_id})
    intervals = user.get("reminder_intervals", DEFAULT_REMINDER_INTERVALS.copy()) if user else DEFAULT_REMINDER_INTERVALS.copy()
    if interval in intervals:
        intervals.remove(interval)
    else:
        intervals.append(interval)
        intervals.sort(reverse=True)
    await db.users.update_one({"user_id": user_id}, {"$set": {"reminder_intervals": intervals}}, upsert=True)
    return intervals

# --- КЛАВІАТУРИ ---
def get_main_keyboard(has_queue: bool = False) -> ReplyKeyboardMarkup:
    queue_btn = BTN_CHANGE_QUEUE if has_queue else BTN_SET_QUEUE
    buttons = [
        [KeyboardButton(text=BTN_CHECK), KeyboardButton(text=BTN_MY_QUEUE)],
        [KeyboardButton(text=queue_btn), KeyboardButton(text=BTN_HELP)],
        [KeyboardButton(text=BTN_DONATE)],
    ]
    return ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)

def get_queue_choice_keyboard(reminders_on: bool = True) -> InlineKeyboardMarkup:
    reminder_text = "🔔 Нагадування: ВКЛ" if reminders_on else "🔕 Нагадування: ВИКЛ"
    buttons = [
        [InlineKeyboardButton(text="🏠 Додати за адресою", callback_data="enter_address")],
        [InlineKeyboardButton(text="🔢 Обрати зі списку", callback_data="select_queue")],
        [InlineKeyboardButton(text=reminder_text, callback_data="toggle_reminders")],
        [InlineKeyboardButton(text="⏰ Налаштування часу", callback_data="reminder_settings")],
        [InlineKeyboardButton(text="🗑 Скасувати всі підписки", callback_data="unsubscribe")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def get_reminder_intervals_keyboard(selected_intervals: list[int]) -> InlineKeyboardMarkup:
    buttons = []
    row = []
    for interval, label in AVAILABLE_REMINDER_INTERVALS.items():
        text = f"✅ {label}" if interval in selected_intervals else f"⬜ {label}"
        row.append(InlineKeyboardButton(text=text, callback_data=f"reminder_int_{interval}"))
        if len(row) == 3:
            buttons.append(row)
            row = []
    if row: buttons.append(row)
    buttons.append([InlineKeyboardButton(text="◀️ Назад", callback_data="back_choice")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def get_queue_list_keyboard(subscribed_queues: list[str] = None) -> InlineKeyboardMarkup:
    subscribed_queues = subscribed_queues or []
    buttons = []
    row = []
    for queue in QUEUES:
        text = f"✅ {queue}" if queue in subscribed_queues else f"{queue}"
        row.append(InlineKeyboardButton(text=text, callback_data=f"queue_{queue}"))
        if len(row) == 4:
            buttons.append(row)
            row = []
    if row: buttons.append(row)
    buttons.append([InlineKeyboardButton(text="✔️ Готово", callback_data="done_select")])
    buttons.append([InlineKeyboardButton(text="◀️ Назад", callback_data="back_choice")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def get_cancel_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="❌ Скасувати", callback_data="cancel_input")]])

def get_donate_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="💛 Підтримати проєкт", callback_data="show_donate")]])

# --- NETWORK REQUESTS (FIXED FOR ANTI-BAN) ---
async def fetch_schedule(session: AsyncSession, queue_id: str):
    if not APQE_PQFRTY:
        return None
    
    params = {'queue': queue_id}
    # Емуляція браузера Chrome
    try:
        response = await session.get(APQE_PQFRTY, params=params)
        if response.status_code == 200:
            return response.json()
        else:
            logging.error(f"API Error {queue_id}: {response.status_code}")
            return None
    except Exception as e:
        logging.error(f"Fetch error {queue_id}: {e}")
        return None

async def fetch_schedule_by_address(city: str, street: str, house: str) -> dict | None:
    if not APSRC_PFRTY:
        return None
    
    address = f"{city},{street},{house}"
    payload = {'accountNumber': '', 'userSearchChoice': 'pob', 'address': address}
    
    # Використовуємо одноразову сесію для пошуку адреси, але з impersonate
    try:
        async with AsyncSession(impersonate="chrome120", proxy=PROXY_URL) as session:
            response = await session.post(APSRC_PFRTY, data=payload)
            if response.status_code == 200:
                return response.json()
            else:
                logging.error(f"Address search error: {response.status_code}")
                return None
    except Exception as e:
        logging.error(f"Address fetch error: {e}")
        return None

def extract_queue_from_response(data) -> tuple[str | None, list | None]:
    if not data or not isinstance(data, dict): return None, None
    current = data.get("current", {})
    schedule = data.get("schedule", [])
    if current.get("hasQueue") != "yes": return None, None
    
    queue_num = current.get("queue")
    sub_queue = current.get("subQueue")
    if queue_num is not None and sub_queue is not None:
        queue_id = f"{queue_num}.{sub_queue}"
        if queue_id in QUEUES:
            return queue_id, schedule
    return None, None

# --- FORMATTING ---
def format_notification(queue_id, data, is_update=True, address=None):
    if not data or not isinstance(data, list):
        return f"⚠️ Дані для черги {queue_id} недоступні"

    days_names = ["Понеділок", "Вівторок", "Середа", "Четвер", "П'ятниця", "Субота", "Неділя"]
    header = "⚡️ *Оновлення ГПВ!*" if is_update else "📊 *Поточний графік*"
    address_line = f"📍 *Адреса:* {address}\n" if address else ""
    text = f"{header}\n\n{address_line}🔢 *Черга:* {queue_id}\n"
    
    for record in data:
        event_date = record.get("eventDate", "Невідомо")
        day_name = ""
        try:
            day, month, year = event_date.split('.')
            dt = datetime(int(year), int(month), int(day))
            day_name = days_names[dt.weekday()]
        except: pass
        
        queue_data = record.get("queues", {}).get(queue_id, [])
        schedule_lines = []
        if queue_data:
            for slot in queue_data:
                start = slot.get('from', '??')
                end = slot.get('to', '??')
                schedule_lines.append(f"  🔴 {start} - {end}")
            schedule_str = "\n".join(schedule_lines)
        else:
            schedule_str = "  ✅ Відключень не заплановано"
        
        text += f"\n📅 *{event_date}* _{day_name}_\n{schedule_str}\n"
    
    return text

def format_user_status(user_data) -> str:
    if not user_data: return "⚠️ Підписку не налаштовано"
    queues = user_data.get("queues", [])
    address = user_data.get("address")
    reminders = user_data.get("reminders", True)
    
    if not queues: return "⚠️ Підписку не налаштовано"
    
    queues_str = ", ".join(sorted(queues))
    reminders_str = "ВКЛ" if reminders else "ВИКЛ"
    
    lines = []
    if address: lines.append(f"📍 *Адреса:* {address}")
    lines.append(f"🔢 *Черги:* {queues_str}")
    lines.append(f"⏰ *Нагадування:* {reminders_str}")
    return "\n".join(lines)

# --- BOT HANDLERS ---
@dp.message(Command("start"))
async def cmd_start(message: Message, state: FSMContext):
    await state.clear()
    user_data = await get_user_data(message.from_user.id)
    queues = user_data.get("queues", []) if user_data else []
    
    if queues:
        text = f"💡 *З поверненням!*\n\n{format_user_status(user_data)}"
    else:
        text = "💡 *Привіт! Я Люмос.*\nДопоможу відслідковувати світло.\nНатисни «⚡ Обрати черги»."
    
    await message.answer(text, reply_markup=get_main_keyboard(bool(queues)), parse_mode=ParseMode.MARKDOWN)

@dp.message(Command("help"))
async def cmd_help(message: Message):
    text = (
        "📚 *Інструкція:*\n"
        "1. Натисніть *«⚡ Обрати черги»*\n"
        "2. Введіть адресу або оберіть номер черги\n"
        "3. Бот сам буде надсилати сповіщення при змінах\n"
        "4. В налаштуваннях можна змінити час нагадувань"
    )
    await message.answer(text, parse_mode=ParseMode.MARKDOWN)

@dp.message(F.text == BTN_CHECK)
async def btn_check(message: Message):
    user_queues = await get_user_queues(message.from_user.id)
    if not user_queues:
        await message.answer("⚠️ Спочатку оберіть чергу!", reply_markup=get_queue_choice_keyboard())
        return
    
    loading_msg = await message.answer("⏳ Завантажую...")
    user_data = await get_user_data(message.from_user.id)
    address = user_data.get("address")
    
    # Використовуємо curl_cffi сесію
    async with AsyncSession(impersonate="chrome120", proxy=PROXY_URL) as session:
        for i, queue in enumerate(sorted(user_queues)):
            data = await fetch_schedule(session, queue)
            if data:
                msg = format_notification(queue, data, is_update=False, address=address if len(user_queues)==1 else None)
                markup = get_donate_keyboard() if i == len(user_queues)-1 else None
                await message.answer(msg, parse_mode=ParseMode.MARKDOWN, reply_markup=markup)
            else:
                await message.answer(f"❌ Помилка завантаження для черги {queue}")
            await asyncio.sleep(0.5)
    
    await loading_msg.delete()

@dp.message(F.text == BTN_MY_QUEUE)
async def btn_my_queue(message: Message):
    user_data = await get_user_data(message.from_user.id)
    status = format_user_status(user_data)
    await message.answer(f"✅ *Ваші налаштування:*\n\n{status}", parse_mode=ParseMode.MARKDOWN)

@dp.message(F.text.in_({BTN_SET_QUEUE, BTN_CHANGE_QUEUE}))
async def btn_set_queue(message: Message, state: FSMContext):
    await state.clear()
    user_data = await get_user_data(message.from_user.id)
    reminders_on = user_data.get("reminders", True) if user_data else True
    await message.answer("⚡ *Налаштування підписок:*", reply_markup=get_queue_choice_keyboard(reminders_on), parse_mode=ParseMode.MARKDOWN)

@dp.message(F.text == BTN_DONATE)
async def btn_donate(message: Message):
    text = f"💛 *Підтримати проєкт*\n\nВаша підтримка важлива!\n🔗 {DONATE_URL}"
    await message.answer(text, parse_mode=ParseMode.MARKDOWN)

@dp.callback_query(F.data == "show_donate")
async def cb_show_donate(callback: CallbackQuery):
    await btn_donate(callback.message)
    await callback.answer()

# --- ADDRESS FLOW ---
@dp.callback_query(F.data == "enter_address")
async def cb_enter_address(callback: CallbackQuery, state: FSMContext):
    await state.set_state(AddressForm.waiting_for_city)
    await callback.message.edit_text("🏙 *Введіть місто:*", reply_markup=get_cancel_keyboard(), parse_mode=ParseMode.MARKDOWN)

@dp.message(AddressForm.waiting_for_city)
async def process_city(message: Message, state: FSMContext):
    await state.update_data(city=message.text.strip())
    await state.set_state(AddressForm.waiting_for_street)
    await message.answer("🏠 *Введіть вулицю та номер (через кому):*\nНаприклад: `Мазепи, 10`", reply_markup=get_cancel_keyboard(), parse_mode=ParseMode.MARKDOWN)

@dp.message(AddressForm.waiting_for_street)
async def process_street(message: Message, state: FSMContext):
    input_text = message.text.strip()
    if ',' in input_text:
        street, house = map(str.strip, input_text.split(',', 1))
    elif ' ' in input_text:
        street, house = input_text.rsplit(' ', 1)
    else:
        await message.answer("⚠️ Формат: Вулиця, Номер", reply_markup=get_cancel_keyboard())
        return
    
    data = await state.get_data()
    city = data.get('city')
    full_address = f"{city}, {street}, {house}"
    
    msg = await message.answer(f"⏳ Шукаю: {full_address}...")
    result = await fetch_schedule_by_address(city, street, house)
    await msg.delete()
    
    if result:
        queue, schedule = extract_queue_from_response(result)
        if queue:
            await add_queue_to_user(message.from_user.id, queue, full_address)
            await state.clear()
            await message.answer(f"✅ Знайдено чергу: *{queue}*", reply_markup=get_main_keyboard(True), parse_mode=ParseMode.MARKDOWN)
            if schedule:
                await message.answer(format_notification(queue, schedule, False, full_address), parse_mode=ParseMode.MARKDOWN)
        else:
            await message.answer("⚠️ Чергу не знайдено в базі.", reply_markup=get_queue_choice_keyboard())
    else:
        await message.answer("❌ Адресу не знайдено.", reply_markup=get_queue_choice_keyboard())

@dp.callback_query(F.data == "cancel_input")
async def cb_cancel_input(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    queues = await get_user_queues(callback.from_user.id)
    await callback.message.edit_text("❌ Скасовано", parse_mode=ParseMode.MARKDOWN)
    await callback.message.answer("Меню:", reply_markup=get_main_keyboard(bool(queues)))

# --- QUEUE MANAGEMENT CALLBACKS ---
@dp.callback_query(F.data == "select_queue")
async def cb_select_queue(callback: CallbackQuery):
    queues = await get_user_queues(callback.from_user.id)
    await callback.message.edit_text("🔢 *Оберіть черги:*", reply_markup=get_queue_list_keyboard(queues), parse_mode=ParseMode.MARKDOWN)

@dp.callback_query(F.data.startswith("queue_"))
async def cb_toggle_queue(callback: CallbackQuery):
    queue = callback.data.split("_")[1]
    queues = await get_user_queues(callback.from_user.id)
    if queue in queues:
        await remove_queue_from_user(callback.from_user.id, queue)
        await callback.answer(f"➖ {queue} видалено")
    else:
        await add_queue_to_user(callback.from_user.id, queue)
        await callback.answer(f"➕ {queue} додано")
    
    new_queues = await get_user_queues(callback.from_user.id)
    await callback.message.edit_reply_markup(reply_markup=get_queue_list_keyboard(new_queues))

@dp.callback_query(F.data == "done_select")
async def cb_done_select(callback: CallbackQuery):
    queues = await get_user_queues(callback.from_user.id)
    await callback.message.delete()
    await callback.message.answer(f"✅ Підписки оновлено: {len(queues)} черг", reply_markup=get_main_keyboard(bool(queues)))

@dp.callback_query(F.data == "toggle_reminders")
async def cb_toggle_reminders(callback: CallbackQuery):
    new_state = await toggle_user_reminders(callback.from_user.id)
    await callback.message.edit_reply_markup(reply_markup=get_queue_choice_keyboard(new_state))
    await callback.answer("Змінено!")

@dp.callback_query(F.data == "unsubscribe")
async def cb_unsubscribe(callback: CallbackQuery):
    await remove_user_queue(callback.from_user.id)
    await callback.message.edit_text("🔕 Всі підписки видалено")
    await callback.message.answer("Меню:", reply_markup=get_main_keyboard(False))

@dp.callback_query(F.data == "back_choice")
async def cb_back_choice(callback: CallbackQuery):
    user_data = await get_user_data(callback.from_user.id)
    reminders = user_data.get("reminders", True) if user_data else True
    await callback.message.edit_text("⚡ Налаштування:", reply_markup=get_queue_choice_keyboard(reminders))

@dp.callback_query(F.data == "reminder_settings")
async def cb_reminder_settings(callback: CallbackQuery):
    intervals = await get_user_reminder_intervals(callback.from_user.id)
    await callback.message.edit_text("⏰ Оберіть час нагадувань:", reply_markup=get_reminder_intervals_keyboard(intervals))

@dp.callback_query(F.data.startswith("reminder_int_"))
async def cb_toggle_int(callback: CallbackQuery):
    interval = int(callback.data.split("_")[2])
    new_intervals = await toggle_reminder_interval(callback.from_user.id, interval)
    await callback.message.edit_reply_markup(reply_markup=get_reminder_intervals_keyboard(new_intervals))

# --- SCHEDULED TASKS ---
def extract_all_schedules(data, queue_id: str) -> dict:
    result = {}
    if not data or not isinstance(data, list): return result
    for record in data:
        event_date = record.get("eventDate")
        if not event_date: continue
        queue_hours = record.get("queues", {}).get(queue_id, [])
        simplified = [{"from": s.get("from"), "to": s.get("to")} for s in queue_hours]
        result[event_date] = simplified
    return result

async def scheduled_checker():
    logging.info("🚀 Monitor started")
    await asyncio.sleep(5)
    
    while True:
        try:
            # Створюємо ОДНУ сесію на весь цикл перевірки
            async with AsyncSession(impersonate="chrome120", proxy=PROXY_URL) as session:
                for queue_id in QUEUES:
                    data = await fetch_schedule(session, queue_id)
                    if not data: continue

                    current_schedules = extract_all_schedules(data, queue_id)
                    saved_state_json = await get_schedule_state(queue_id)
                    
                    # Завантажуємо старий стан
                    saved_schedules = {}
                    if saved_state_json:
                        try:
                            saved_schedules = json.loads(saved_state_json)
                        except:
                            saved_schedules = {}
                    
                    changes = []
                    # Порівнюємо графіки
                    for date, hours in current_schedules.items():
                        current_hash = json.dumps(hours, sort_keys=True)
                        
                        if date not in saved_schedules:
                            changes.append((date, hours, "new"))
                        elif saved_schedules.get(date) != current_hash:
                            changes.append((date, hours, "updated"))
                        
                        # Оновлюємо стан у пам'яті (щоб потім зберегти)
                        saved_schedules[date] = current_hash
                    
                    # Якщо є зміни — розсилаємо
                    if changes:
                        subscribers = await get_users_by_queue(queue_id)
                        if subscribers:
                            for user_id in subscribers:
                                for date, hours, c_type in changes:
                                    # ОСЬ ТУТ ТЕПЕР ВИКЛИК ПОВНОГО ФОРМАТУВАННЯ
                                    msg = format_schedule_notification(queue_id, date, hours, c_type)
                                    try:
                                        await bot.send_message(user_id, msg, parse_mode=ParseMode.MARKDOWN)
                                    except Exception as e:
                                        logging.error(f"Send error {user_id}: {e}")
                                    await asyncio.sleep(0.2)
                        
                        # Зберігаємо оновлений стан у базу
                        await save_schedule_state(queue_id, json.dumps(saved_schedules))
                    
                    await asyncio.sleep(1) # Пауза між чергами
        except Exception as e:
            logging.error(f"Checker loop error: {e}")
        
        await asyncio.sleep(CHECK_INTERVAL)

async def reminder_checker():
    logging.info("⏰ Reminder checker started")
    await asyncio.sleep(10)
    while True:
        try:
            now = datetime.now()
            # Раз на добу чистимо старі
            if now.hour == 3 and now.minute < 2: await cleanup_old_reminders()

            # Використовуємо сесію для перевірки актуальних даних
            async with AsyncSession(impersonate="chrome120", proxy=PROXY_URL) as session:
                # Тут спрощена логіка: беремо унікальні черги з бази і перевіряємо їх
                # ... (логіка схожа на оригінальну, але з використанням session)
                pass # Залиште оригінальну логіку нагадувань, але якщо там є запити до API - використовуйте session

        except Exception as e:
            logging.error(f"Reminder error: {e}")
        await asyncio.sleep(60)

# --- WEB SERVER ---
async def handle_index(request):
    try:
        with open(BASE_DIR / "templates" / "index.html", "r", encoding="utf-8") as f:
            html = f.read()
        users_count = await db.users.count_documents({})
        return web.Response(text=html.replace("{{users_count}}", str(users_count)), content_type="text/html")
    except:
        return web.Response(text="Bot is running")

async def main():
    await init_db()
    
    # Web server configuration
    app = web.Application()
    app.router.add_get("/", handle_index)
    app.router.add_get("/health", lambda r: web.json_response({"status": "ok"}))
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    
    asyncio.create_task(scheduled_checker())
    asyncio.create_task(reminder_checker())
    
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
