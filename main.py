import asyncio
import logging
import json
import ssl
import os
from datetime import datetime
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
from motor.motor_asyncio import AsyncIOMotorClient

# --- КОНФІГУРАЦІЯ ---
load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DB_NAME = os.getenv("DB_NAME", "lumos_bot")
CHECK_INTERVAL = int(os.getenv("CHECK_INTERVAL", "45"))

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
BTN_MY_QUEUE = "📋 Моя підписка"
BTN_SET_QUEUE = "⚡ Обрати чергу"
BTN_CHANGE_QUEUE = "✏️ Змінити чергу"
BTN_HELP = "❓ Допомога"

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# --- MongoDB ---
mongo_client: AsyncIOMotorClient = None
db = None

async def init_db():
    """Ініціалізація підключення до MongoDB"""
    global mongo_client, db
    mongo_client = AsyncIOMotorClient(MONGO_URI)
    db = mongo_client[DB_NAME]
    logging.info("✅ Connected to MongoDB")

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
        return {"queue": user.get("queue"), "address": user.get("address")}
    return None

async def set_user_data(user_id: int, queue: str, address: str = None):
    """Зберігає дані користувача в MongoDB"""
    await db.users.update_one(
        {"user_id": user_id},
        {"$set": {"queue": queue, "address": address, "updated_at": datetime.now()}},
        upsert=True
    )

async def get_user_queue(user_id: int) -> str | None:
    """Отримує чергу користувача"""
    data = await get_user_data(user_id)
    if data:
        return data.get("queue")
    return None

async def remove_user_queue(user_id: int):
    """Видаляє підписку користувача"""
    await db.users.delete_one({"user_id": user_id})

async def get_users_by_queue(queue: str) -> list[int]:
    """Повертає список user_id підписаних на певну чергу"""
    cursor = db.users.find({"queue": queue})
    users = await cursor.to_list(length=None)
    return [user["user_id"] for user in users]

async def get_schedule_state(queue_id: str) -> str | None:
    """Отримує збережений стан графіку для черги"""
    state = await db.schedule_state.find_one({"queue_id": queue_id})
    if state:
        return state.get("data_hash")
    return None

async def save_schedule_state(queue_id: str, data_hash: str):
    """Зберігає стан графіку для черги"""
    await db.schedule_state.update_one(
        {"queue_id": queue_id},
        {"$set": {"data_hash": data_hash, "updated_at": datetime.now()}},
        upsert=True
    )

# --- КЛАВІАТУРИ ---
def get_main_keyboard(has_queue: bool = False) -> ReplyKeyboardMarkup:
    """Головна клавіатура (Reply Keyboard)"""
    queue_btn = BTN_CHANGE_QUEUE if has_queue else BTN_SET_QUEUE
    
    buttons = [
        [KeyboardButton(text=BTN_CHECK), KeyboardButton(text=BTN_MY_QUEUE)],
        [KeyboardButton(text=queue_btn), KeyboardButton(text=BTN_HELP)],
    ]
    return ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)

def get_queue_choice_keyboard() -> InlineKeyboardMarkup:
    """Вибір способу встановлення черги"""
    buttons = [
        [InlineKeyboardButton(text="🏠 Ввести адресу", callback_data="enter_address")],
        [InlineKeyboardButton(text="🔢 Обрати з списку", callback_data="select_queue")],
        [InlineKeyboardButton(text="❌ Скасувати підписку", callback_data="unsubscribe")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def get_queue_list_keyboard() -> InlineKeyboardMarkup:
    """Клавіатура вибору черги зі списку"""
    buttons = []
    row = []
    for queue in QUEUES:
        row.append(InlineKeyboardButton(text=f"{queue}", callback_data=f"queue_{queue}"))
        if len(row) == 4:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)
    
    buttons.append([InlineKeyboardButton(text="◀️ Назад", callback_data="back_choice")])
    
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def get_cancel_keyboard() -> InlineKeyboardMarkup:
    """Клавіатура скасування"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="❌ Скасувати", callback_data="cancel_input")]
    ])

# --- ОТРИМАННЯ ДАНИХ ---
def get_ssl_context():
    ssl_context = ssl.create_default_context()
    ssl_context.set_ciphers('DEFAULT@SECLEVEL=1')
    return ssl_context

async def fetch_schedule(session, queue_id):
    params = {'queue': queue_id}
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }
    try:
        async with session.get(APQE_PQFRTY, params=params, headers=headers) as response:
            if response.status == 200:
                return await response.json()
            return None
    except Exception as e:
        logging.error(f"Error fetching {queue_id}: {e}")
        return None

async def fetch_schedule_by_address(city: str, street: str, house: str) -> dict | None:
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
                    logging.error(f"Address search failed: {response.status}")
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
    if not data or not isinstance(data, list):
        return f"⚠️ Отримано некоректні дані для черги {queue_id}"

    record = data[0]
    
    event_date = record.get("eventDate", "Невідомо")
    approved_since = record.get("scheduleApprovedSince", "Не вказано")
    
    day_name = ""
    try:
        dt = datetime.strptime(event_date, "%d.%m.%Y")
        days = ["Понеділок", "Вівторок", "Середа", "Четвер", "П'ятниця", "Субота", "Неділя"]
        day_name = days[dt.weekday()]
    except:
        pass

    queue_data = record.get("queues", {}).get(queue_id, [])
    
    schedule_lines = []
    if queue_data:
        for slot in queue_data:
            start = slot.get('from', '??')
            end = slot.get('to', '??')
            icon = "🔴"
            
            # Розрахунок тривалості
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
                    hours = diff_minutes // 60
                    minutes = diff_minutes % 60
                    if minutes == 0:
                        duration_str = f" ({hours} год)"
                    else:
                        duration_str = f" ({hours} год {minutes} хв)"
            except:
                pass
            
            schedule_lines.append(f"{icon} {start} - {end}{duration_str}")
        
        schedule_str = "\n".join(schedule_lines)
    else:
        schedule_str = "✅ Відключень не заплановано"

    header = "⚡️ *Оновлення ГПВ!*" if is_update else "📊 *Поточний графік*"
    
    address_line = f"📍 *Адреса:* {address}\n" if address else ""
    
    text = (
        f"{header}\n\n"
        f"{address_line}"
        f"📅 *Дата:* {event_date} ({day_name})\n"
        f"🔢 *Черга:* {queue_id}\n"
        f"🕒 *Офіційно затверджено:* {approved_since}\n\n"
        f"*Графік відключень:*\n"
        f"{schedule_str}"
    )
    return text

def format_user_status(user_data) -> str:
    """Форматує статус користувача"""
    if user_data:
        if isinstance(user_data, dict):
            queue = user_data.get("queue")
            address = user_data.get("address")
            if address:
                return f"📍 *Адреса:* {address}\n🔢 *Черга:* {queue}"
            else:
                return f"🔢 *Черга:* {queue}"
        else:
            return f"🔢 *Черга:* {user_data}"
    return "⚠️ Підписку не налаштовано"

# --- ХЕНДЛЕРИ КОМАНД ---
@dp.message(Command("start"))
async def cmd_start(message: Message, state: FSMContext):
    await state.clear()
    user_data = await get_user_data(message.from_user.id)
    has_queue = user_data is not None
    
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
        f"*{BTN_CHECK}* - переглянути поточний графік\n"
        f"*{BTN_MY_QUEUE}* - інформація про вашу підписку\n"
        f"*{BTN_SET_QUEUE}/{BTN_CHANGE_QUEUE}* - налаштувати чергу\n\n"
        "🔔 *Як це працює:*\n"
        "1. Введіть адресу або оберіть чергу\n"
        "2. Бот автоматично перевіряє графік\n"
        "3. При змінах вам прийде сповіщення"
    )
    await message.answer(text, parse_mode=ParseMode.MARKDOWN)

# --- ХЕНДЛЕРИ КНОПОК КЛАВІАТУРИ ---
@dp.message(F.text == BTN_CHECK)
async def btn_check(message: Message):
    user_queue = await get_user_queue(message.from_user.id)
    
    if not user_queue:
        await message.answer(
            "⚠️ Спочатку оберіть чергу!",
            reply_markup=get_queue_choice_keyboard(),
            parse_mode=ParseMode.MARKDOWN
        )
        return
    
    loading_msg = await message.answer("⏳ Завантажую графік...")
    
    ssl_context = get_ssl_context()
    connector = aiohttp.TCPConnector(ssl=ssl_context)
    
    async with aiohttp.ClientSession(connector=connector) as session:
        data = await fetch_schedule(session, user_queue)
        
        await loading_msg.delete()
        
        if data:
            user_data = await get_user_data(message.from_user.id)
            address = user_data.get("address") if isinstance(user_data, dict) else None
            msg = format_notification(user_queue, data, is_update=False, address=address)
            await message.answer(msg, parse_mode=ParseMode.MARKDOWN)
        else:
            await message.answer("❌ Не вдалося отримати дані. Спробуйте пізніше.")

@dp.message(F.text == BTN_MY_QUEUE)
async def btn_my_queue(message: Message):
    user_data = await get_user_data(message.from_user.id)
    status = format_user_status(user_data)
    
    if user_data:
        text = f"✅ *Ваша підписка:*\n\n{status}\n\n🔔 Ви отримуватимете сповіщення про зміни в графіку."
    else:
        text = f"⚠️ *Підписку не налаштовано*\n\nОберіть чергу, щоб отримувати сповіщення."
    
    await message.answer(text, parse_mode=ParseMode.MARKDOWN)

@dp.message(F.text.in_({BTN_SET_QUEUE, BTN_CHANGE_QUEUE}))
async def btn_set_queue(message: Message, state: FSMContext):
    await state.clear()
    user_data = await get_user_data(message.from_user.id)
    
    if user_data:
        status = format_user_status(user_data)
        text = f"✏️ *Змінити чергу*\n\n*Поточна підписка:*\n{status}\n\nОберіть спосіб:"
    else:
        text = "⚡ *Оберіть спосіб налаштування:*"
    
    await message.answer(text, reply_markup=get_queue_choice_keyboard(), parse_mode=ParseMode.MARKDOWN)

@dp.message(F.text == BTN_HELP)
async def btn_help(message: Message):
    await cmd_help(message)

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
            await set_user_data(message.from_user.id, queue, full_address)
            await state.clear()
            
            text = (
                f"✅ *Адресу знайдено!*\n\n"
                f"📍 *Адреса:* {full_address}\n"
                f"🔢 *Ваша черга:* {queue}\n\n"
                f"🔔 Тепер ви отримуватимете сповіщення про зміни в графіку."
            )
            await message.answer(text, reply_markup=get_main_keyboard(has_queue=True), parse_mode=ParseMode.MARKDOWN)
            
            msg = format_notification(queue, schedule, is_update=False, address=full_address)
            await message.answer(msg, parse_mode=ParseMode.MARKDOWN)
        else:
            await state.clear()
            await message.answer(
                "⚠️ Не вдалося визначити чергу для цієї адреси.\n\n"
                "Спробуйте ввести адресу ще раз або оберіть чергу вручну.",
                reply_markup=get_queue_choice_keyboard(),
                parse_mode=ParseMode.MARKDOWN
            )
    else:
        await state.clear()
        await message.answer(
            "❌ Адресу не знайдено.\n\n"
            "Перевірте правильність написання та спробуйте ще раз.",
            reply_markup=get_queue_choice_keyboard(),
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
    user_data = await get_user_data(callback.from_user.id)
    has_queue = user_data is not None
    
    await callback.message.edit_text("❌ *Введення скасовано*", parse_mode=ParseMode.MARKDOWN)
    await callback.message.answer("Оберіть дію:", reply_markup=get_main_keyboard(has_queue))
    await callback.answer()

@dp.callback_query(F.data == "select_queue")
async def cb_select_queue(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    text = "🔢 *Оберіть свою чергу:*"
    await callback.message.edit_text(text, reply_markup=get_queue_list_keyboard(), parse_mode=ParseMode.MARKDOWN)
    await callback.answer()

@dp.callback_query(F.data == "back_choice")
async def cb_back_choice(callback: CallbackQuery):
    user_data = await get_user_data(callback.from_user.id)
    
    if user_data:
        status = format_user_status(user_data)
        text = f"✏️ *Змінити чергу*\n\n*Поточна підписка:*\n{status}\n\nОберіть спосіб:"
    else:
        text = "⚡ *Оберіть спосіб налаштування:*"
    
    await callback.message.edit_text(text, reply_markup=get_queue_choice_keyboard(), parse_mode=ParseMode.MARKDOWN)
    await callback.answer()

@dp.callback_query(F.data.startswith("queue_"))
async def cb_queue_select(callback: CallbackQuery):
    queue = callback.data.replace("queue_", "")
    
    if queue not in QUEUES:
        await callback.answer("❌ Невідома черга!", show_alert=True)
        return
    
    await set_user_data(callback.from_user.id, queue, None)
    
    text = (
        f"✅ *Чергу обрано: {queue}*\n\n"
        f"🔔 Тепер ви отримуватимете сповіщення "
        f"про зміни в графіку."
    )
    
    await callback.message.edit_text(text, parse_mode=ParseMode.MARKDOWN)
    await callback.message.answer("Меню оновлено:", reply_markup=get_main_keyboard(has_queue=True))
    await callback.answer(f"✅ Черга {queue} обрана!")
    
    ssl_context = get_ssl_context()
    connector = aiohttp.TCPConnector(ssl=ssl_context)
    
    async with aiohttp.ClientSession(connector=connector) as session:
        data = await fetch_schedule(session, queue)
        if data:
            msg = format_notification(queue, data, is_update=False)
            await callback.message.answer(msg, parse_mode=ParseMode.MARKDOWN)

@dp.callback_query(F.data == "unsubscribe")
async def cb_unsubscribe(callback: CallbackQuery):
    user_queue = await get_user_queue(callback.from_user.id)
    
    if not user_queue:
        await callback.answer("ℹ️ У вас немає активної підписки", show_alert=True)
        return
    
    await remove_user_queue(callback.from_user.id)
    
    text = "🔕 *Підписку скасовано*\n\nВи більше не отримуватимете сповіщення."
    await callback.message.edit_text(text, parse_mode=ParseMode.MARKDOWN)
    await callback.message.answer("Меню оновлено:", reply_markup=get_main_keyboard(has_queue=False))
    await callback.answer("✅ Підписку скасовано")

# --- ОСНОВНИЙ ЦИКЛ ПЕРЕВІРКИ ---
def extract_schedule_hours(data, queue_id: str) -> dict | None:
    """
    Витягує тільки дату та години відключень для порівняння.
    """
    if not data or not isinstance(data, list) or len(data) == 0:
        return None
    
    record = data[0]
    event_date = record.get("eventDate")
    queue_hours = record.get("queues", {}).get(queue_id, [])
    
    simplified_hours = []
    for slot in queue_hours:
        simplified_hours.append({
            "from": slot.get("from"),
            "to": slot.get("to"),
            "status": slot.get("status")
        })
    
    return {
        "eventDate": event_date,
        "hours": simplified_hours
    }

async def scheduled_checker():
    logging.info("🚀 Monitor started")
    await asyncio.sleep(10)
    
    ssl_context = get_ssl_context()
    connector = aiohttp.TCPConnector(ssl=ssl_context)
    
    async with aiohttp.ClientSession(connector=connector) as session:
        while True:
            for queue_id in QUEUES:
                data = await fetch_schedule(session, queue_id)
                if not data:
                    continue

                schedule_data = extract_schedule_hours(data, queue_id)
                if not schedule_data:
                    continue
                
                current_hash = json.dumps(schedule_data, sort_keys=True)
                saved_hash = await get_schedule_state(queue_id)
                
                if saved_hash != current_hash:
                    logging.info(f"Schedule changed for queue {queue_id}: {schedule_data}")
                    
                    subscribers = await get_users_by_queue(queue_id)
                    
                    if subscribers:
                        for user_id in subscribers:
                            try:
                                user_data = await get_user_data(user_id)
                                address = user_data.get("address") if isinstance(user_data, dict) else None
                                msg = format_notification(queue_id, data, is_update=True, address=address)
                                await bot.send_message(user_id, msg, parse_mode=ParseMode.MARKDOWN)
                                logging.info(f"Notification sent to {user_id} for queue {queue_id}")
                            except Exception as e:
                                logging.error(f"Failed to send to {user_id}: {e}")
                            
                            await asyncio.sleep(0.5)
                    
                    await save_schedule_state(queue_id, current_hash)
                
                await asyncio.sleep(1)
            
            logging.info(f"Check completed. Next check in {CHECK_INTERVAL} seconds")
            await asyncio.sleep(CHECK_INTERVAL)

async def main():
    logging.info("🤖 Bot starting...")
    await init_db()
    
    try:
        asyncio.create_task(scheduled_checker())
        await dp.start_polling(bot)
    finally:
        await close_db()

if __name__ == "__main__":
    asyncio.run(main())
