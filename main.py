import os
import sqlite3
import json
import threading
import time
import datetime
import traceback
from flask import Flask

import vk_api
from vk_api.bot_longpoll import VkBotLongPoll, VkBotEventType
from vk_api.keyboard import VkKeyboard, VkKeyboardColor

# ==================== НАСТРОЙКИ ====================
GROUP_TOKEN = os.environ.get('GROUP_TOKEN', 'vk1.a.nF-zqzyP2uAJATyhkFxaptOMelXcP6cyJoXGdh-mP7BF5rFR0u6SeLaj7d1blOMGje6wZLGAHh-3m9lLtXKcaoRZ6T4UMR1E1zlk7JJHHn-8RrRwjxpM8470E56Z5SyTVajEwB5dlYgtBenHrZqVNRGaRLIk6S1dyx0-F0gDPW9az9i0tBklhjkShM6YmR3-j2yjcHnZpOKXkUAFQV4Rrg')
GROUP_ID = int(os.environ.get('GROUP_ID', '236429857'))
ADMINS = [408937441, 576380600, 506670282, 516249502, 505314801]  # VK user IDs админов

DB_PATH = 'bot.db'
MAX_PER_SLOT = 10
DELETE_DELAY = 20  # секунд до удаления своих сообщений

# ==================== РАСПИСАНИЕ ====================
# Четная неделя: Пн 16:00, Пт 16:30, Сб 14:00
# Нечетная неделя: Пн 17:30, Пт 16:30, Сб 14:00

EVEN_SLOTS = [
    {"day": "Пн", "time": "16:00", "weekday": 0},
    {"day": "Пт", "time": "16:30", "weekday": 4},
    {"day": "Сб", "time": "14:00", "weekday": 5},
]

ODD_SLOTS = [
    {"day": "Пн", "time": "17:30", "weekday": 0},
    {"day": "Пт", "time": "16:30", "weekday": 4},
    {"day": "Сб", "time": "14:00", "weekday": 5},
]

# ==================== СОСТОЯНИЯ ====================
user_states = {}
user_states_lock = threading.Lock()

# ==================== FLASK ====================
app = Flask(__name__)


@app.route('/')
def index():
    return 'VK Bot is running!', 200


@app.route('/health')
def health():
    return 'OK', 200


# ==================== DATABASE ====================
def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS bookings (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        slot TEXT NOT NULL,
        subgroup INTEGER NOT NULL,
        timestamp TEXT NOT NULL
    )''')
    c.execute('''CREATE TABLE IF NOT EXISTS suggestions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        slot TEXT NOT NULL,
        theme_text TEXT NOT NULL,
        timestamp TEXT DEFAULT CURRENT_TIMESTAMP
    )''')
    c.execute('''CREATE TABLE IF NOT EXISTS feedback (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        text TEXT NOT NULL,
        timestamp TEXT DEFAULT CURRENT_TIMESTAMP
    )''')
    conn.commit()
    conn.close()
    print("[DB] Initialized.")


def db_execute(query, params=(), fetch=False, fetchone=False):
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    c.execute(query, params)
    result = None
    if fetchone:
        result = c.fetchone()
    elif fetch:
        result = c.fetchall()
    conn.commit()
    conn.close()
    return result


# ==================== SLOT HELPERS ====================
def is_even_week():
    """Возвращает True, если текущая неделя четная (ISO week number % 2 == 0)."""
    now = datetime.datetime.now()
    week_num = now.isocalendar()[1]
    return week_num % 2 == 0


def get_current_week_type():
    return "even" if is_even_week() else "odd"


def get_current_slots():
    """Возвращает список слотов для текущей недели."""
    if is_even_week():
        return EVEN_SLOTS
    else:
        return ODD_SLOTS


def get_slot_dates():
    """
    Возвращает словарь с полными датами для каждого слота текущей недели.
    Ключ: "day_time" (напр. "Пн_16:00"), значение: дата (datetime.date).
    """
    now = datetime.datetime.now()
    today = now.date()
    current_weekday = today.weekday()  # 0=Пн, 6=Вс

    slots = get_current_slots()
    result = []
    for slot in slots:
        diff = slot["weekday"] - current_weekday
        slot_date = today + datetime.timedelta(days=diff)
        result.append({
            "day": slot["day"],
            "time": slot["time"],
            "weekday": slot["weekday"],
            "date": slot_date,
        })
    return result


def make_slot_key(slot_info, subgroup):
    """Создает уникальный ключ слота: 'even_Пн_16:00_2024-01-15'"""
    week_type = get_current_week_type()
    return f"{week_type}_{slot_info['day']}_{slot_info['time']}_{slot_info['date'].isoformat()}"


def make_slot_key_full(slot_info, subgroup):
    """Полный ключ с подгруппой: 'even_Пн_16:00_2024-01-15_1'"""
    base = make_slot_key(slot_info, subgroup)
    return f"{base}_{subgroup}"


def parse_slot_display(slot_key, subgroup):
    """Из ключа слота делает читаемую строку."""
    parts = slot_key.split('_')
    # even_Пн_16:00_2024-01-15
    if len(parts) >= 4:
        week_type = "Чёт" if parts[0] == "even" else "Нечёт"
        day = parts[1]
        time_str = parts[2]
        date_str = parts[3]
        return f"{day} {date_str} {time_str} Подгр{subgroup} ({week_type})"
    return f"{slot_key} Подгр{subgroup}"


def count_booked(slot_key, subgroup):
    """Считает количество записанных на слот+подгруппу."""
    row = db_execute(
        "SELECT COUNT(*) as cnt FROM bookings WHERE slot=? AND subgroup=?",
        (slot_key, subgroup), fetchone=True
    )
    return row['cnt'] if row else 0


def is_user_booked(user_id, slot_key, subgroup):
    """Проверяет, записан ли юзер на слот."""
    row = db_execute(
        "SELECT id FROM bookings WHERE user_id=? AND slot=? AND subgroup=?",
        (user_id, slot_key, subgroup), fetchone=True
    )
    return row is not None


def get_user_bookings(user_id):
    """Возвращает все записи юзера."""
    return db_execute(
        "SELECT * FROM bookings WHERE user_id=? ORDER BY timestamp DESC",
        (user_id,), fetch=True
    )


def get_slot_bookings(slot_key, subgroup):
    """Возвращает все записи на слот."""
    return db_execute(
        "SELECT * FROM bookings WHERE slot=? AND subgroup=? ORDER BY id",
        (slot_key, subgroup), fetch=True
    )


# ==================== VK API HELPERS ====================
vk_session = None
vk = None
longpoll = None


def init_vk():
    global vk_session, vk, longpoll
    vk_session = vk_api.VkApi(token=GROUP_TOKEN)
    vk = vk_session.get_api()
    longpoll = VkBotLongPoll(vk_session, GROUP_ID)
    print("[VK] Initialized.")


def send_message(peer_id, text, keyboard=None):
    """Отправляет сообщение и возвращает message_id."""
    try:
        params = {
            'peer_id': peer_id,
            'message': text,
            'random_id': int(time.time() * 1000000) % (2 ** 31),
        }
        if keyboard:
            params['keyboard'] = keyboard
        result = vk.messages.send(**params)
        print(f"[SEND] peer={peer_id}, msg_id={result}, text={text[:50]}...")
        return result
    except Exception as e:
        print(f"[ERROR] send_message: {e}")
        traceback.print_exc()
        return None


def delete_message_later(peer_id, message_id, delay=DELETE_DELAY):
    """Удаляет сообщение бота через delay секунд в отдельном потоке."""
    def _delete():
        time.sleep(delay)
        try:
            # Для бесед используем delete_for_all
            vk.messages.delete(
                peer_id=peer_id,
                message_ids=message_id,
                delete_for_all=1,
                group_id=GROUP_ID
            )
            print(f"[DELETE] peer={peer_id}, msg_id={message_id}")
        except Exception as e:
            print(f"[DELETE ERROR] peer={peer_id}, msg_id={message_id}: {e}")

    t = threading.Thread(target=_delete, daemon=True)
    t.start()


def send_and_delete(peer_id, text, keyboard=None, delay=DELETE_DELAY):
    """Отправляет сообщение и ставит на удаление."""
    msg_id = send_message(peer_id, text, keyboard)
    if msg_id:
        delete_message_later(peer_id, msg_id, delay)
    return msg_id


# ==================== КЛАВИАТУРЫ ====================
def make_main_kb(user_id, is_chat=False):
    kb = VkKeyboard(one_time=False)
    kb.add_button("📝 Записаться", color=VkKeyboardColor.PRIMARY)
    kb.add_line()
    kb.add_button("📋 Мои записи", color=VkKeyboardColor.POSITIVE)
    if not is_chat:
        kb.add_line()
        kb.add_button("💬 Фидбек", color=VkKeyboardColor.SECONDARY)
    if user_id in ADMINS and not is_chat:
        kb.add_line()
        kb.add_button("🔧 Админка", color=VkKeyboardColor.NEGATIVE)
    return kb.get_keyboard()


def make_slots_kb():
    """Клавиатура с 6 кнопками (3 дня x 2 подгруппы) + Назад."""
    kb = VkKeyboard(one_time=False)
    slot_dates = get_slot_dates()

    for i, slot_info in enumerate(slot_dates):
        day = slot_info['day']
        t = slot_info['time']
        date_str = slot_info['date'].strftime('%d.%m')
        count1 = count_booked(make_slot_key(slot_info, 1), 1)
        count2 = count_booked(make_slot_key(slot_info, 2), 2)

        label1 = f"{day} {date_str} {t} П1 [{count1}/{MAX_PER_SLOT}]"
        label2 = f"{day} {date_str} {t} П2 [{count2}/{MAX_PER_SLOT}]"

        # Ограничение длины кнопки VK - 40 символов
        if len(label1) > 40:
            label1 = label1[:40]
        if len(label2) > 40:
            label2 = label2[:40]

        kb.add_button(label1, color=VkKeyboardColor.PRIMARY)
        kb.add_button(label2, color=VkKeyboardColor.PRIMARY)
        if i < len(slot_dates) - 1:
            kb.add_line()

    kb.add_line()
    kb.add_button("⬅ Назад", color=VkKeyboardColor.SECONDARY)
    return kb.get_keyboard()


def make_my_records_kb():
    kb = VkKeyboard(one_time=False)
    kb.add_button("💡 Предложить тему", color=VkKeyboardColor.PRIMARY)
    kb.add_line()
    kb.add_button("❌ Удалить запись", color=VkKeyboardColor.NEGATIVE)
    kb.add_line()
    kb.add_button("⬅ Назад", color=VkKeyboardColor.SECONDARY)
    return kb.get_keyboard()


def make_back_kb():
    kb = VkKeyboard(one_time=False)
    kb.add_button("⬅ Назад", color=VkKeyboardColor.SECONDARY)
    return kb.get_keyboard()


def make_admin_kb():
    kb = VkKeyboard(one_time=False)
    kb.add_button("📊 Записи", color=VkKeyboardColor.PRIMARY)
    kb.add_line()
    kb.add_button("📨 Фидбек (админ)", color=VkKeyboardColor.POSITIVE)
    kb.add_line()
    kb.add_button("⬅ Назад", color=VkKeyboardColor.SECONDARY)
    return kb.get_keyboard()


def make_admin_slots_kb():
    """Клавиатура слотов для админа."""
    kb = VkKeyboard(one_time=False)
    slot_dates = get_slot_dates()

    for i, slot_info in enumerate(slot_dates):
        day = slot_info['day']
        t = slot_info['time']
        date_str = slot_info['date'].strftime('%d.%m')
        count1 = count_booked(make_slot_key(slot_info, 1), 1)
        count2 = count_booked(make_slot_key(slot_info, 2), 2)

        label1 = f"A:{day} {date_str} {t} П1 [{count1}]"
        label2 = f"A:{day} {date_str} {t} П2 [{count2}]"

        if len(label1) > 40:
            label1 = label1[:40]
        if len(label2) > 40:
            label2 = label2[:40]

        kb.add_button(label1, color=VkKeyboardColor.PRIMARY)
        kb.add_button(label2, color=VkKeyboardColor.PRIMARY)
        if i < len(slot_dates) - 1:
            kb.add_line()

    kb.add_line()
    kb.add_button("⬅ Назад", color=VkKeyboardColor.SECONDARY)
    return kb.get_keyboard()


def make_admin_slot_actions_kb():
    kb = VkKeyboard(one_time=False)
    kb.add_button("🗑 Удалить запись", color=VkKeyboardColor.NEGATIVE)
    kb.add_line()
    kb.add_button("📝 Список тем", color=VkKeyboardColor.POSITIVE)
    kb.add_line()
    kb.add_button("⬅ Назад", color=VkKeyboardColor.SECONDARY)
    return kb.get_keyboard()


def make_stop_kb():
    kb = VkKeyboard(one_time=False)
    kb.add_button("🛑 Стоп", color=VkKeyboardColor.NEGATIVE)
    return kb.get_keyboard()


# ==================== STATE MANAGEMENT ====================
def get_state(user_id):
    with user_states_lock:
        return user_states.get(user_id, {'state': 'main', 'data': {}})


def set_state(user_id, state, data=None):
    with user_states_lock:
        user_states[user_id] = {'state': state, 'data': data or {}}


def clear_state(user_id):
    with user_states_lock:
        user_states.pop(user_id, None)


# ==================== SLOT PARSING ====================
def parse_slot_from_button(text):
    """
    Парсит текст кнопки слота, возвращает (slot_key, subgroup) или None.
    Формат: "Пн 15.01 16:00 П1 [3/10]" или "A:Пн 15.01 16:00 П1 [3]"
    """
    try:
        clean = text.strip()
        is_admin = clean.startswith("A:")
        if is_admin:
            clean = clean[2:]

        parts = clean.split()
        # parts: ['Пн', '15.01', '16:00', 'П1', '[3/10]'] или ['Пн', '15.01', '16:00', 'П2', '[3]']
        if len(parts) < 4:
            return None

        day = parts[0]
        date_short = parts[1]  # '15.01'
        time_str = parts[2]  # '16:00'
        subgroup_str = parts[3]  # 'П1' или 'П2'

        subgroup = int(subgroup_str.replace('П', ''))

        # Определяем полную дату
        now = datetime.datetime.now()
        year = now.year
        day_num, month_num = date_short.split('.')
        slot_date = datetime.date(year, int(month_num), int(day_num))

        week_type = get_current_week_type()
        slot_key = f"{week_type}_{day}_{time_str}_{slot_date.isoformat()}"

        return slot_key, subgroup

    except Exception as e:
        print(f"[PARSE ERROR] '{text}': {e}")
        return None


# ==================== ОБРАБОТЧИКИ ====================
def handle_main_menu(peer_id, user_id, is_chat=False):
    """Показывает главное меню."""
    bookings = get_user_bookings(user_id)
    greeting = "👋 Привет! Я бот для записи на занятия.\n"

    if bookings:
        greeting += "\n📌 Ваши текущие записи:\n"
        for b in bookings:
            display = parse_slot_display(b['slot'], b['subgroup'])
            greeting += f"  • {display}\n"
    else:
        greeting += "\nУ вас пока нет записей.\n"

    greeting += "\nВыберите действие:"
    kb = make_main_kb(user_id, is_chat)
    send_and_delete(peer_id, greeting, kb, delay=DELETE_DELAY)
    set_state(user_id, 'main', {'peer_id': peer_id, 'is_chat': is_chat})


def handle_record_menu(peer_id, user_id):
    """Показывает меню записи со слотами."""
    week_type = "Чётная" if is_even_week() else "Нечётная"
    text = f"📅 Текущая неделя: {week_type}\n"
    text += f"Выберите слот для записи (макс {MAX_PER_SLOT} чел.):"
    kb = make_slots_kb()
    send_and_delete(peer_id, text, kb, delay=DELETE_DELAY)
    set_state(user_id, 'record_select', {'peer_id': peer_id})


def handle_my_records(peer_id, user_id):
    """Показывает записи юзера."""
    bookings = get_user_bookings(user_id)
    if not bookings:
        text = "📋 У вас нет записей."
    else:
        text = "📋 Ваши записи:\n"
        for i, b in enumerate(bookings, 1):
            display = parse_slot_display(b['slot'], b['subgroup'])
            text += f"  {i}. {display} (записан {b['timestamp']})\n"

    kb = make_my_records_kb()
    send_and_delete(peer_id, text, kb, delay=DELETE_DELAY)
    set_state(user_id, 'my_records', {'peer_id': peer_id})


def handle_slot_booking(peer_id, user_id, text):
    """Обработка выбора слота для записи."""
    parsed = parse_slot_from_button(text)
    if not parsed:
        send_and_delete(peer_id, "⚠ Не удалось распознать слот. Попробуйте ещё раз.", delay=10)
        return

    slot_key, subgroup = parsed

    # Проверяем, не записан ли уже
    if is_user_booked(user_id, slot_key, subgroup):
        send_and_delete(peer_id, "⚠ Вы уже записаны на этот слот!", delay=10)
        return

    # Проверяем лимит
    count = count_booked(slot_key, subgroup)
    if count >= MAX_PER_SLOT:
        send_and_delete(peer_id, "⚠ Слот заполнен! Выберите другой.", delay=10)
        return

    # Записываем
    now_str = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    db_execute(
        "INSERT INTO bookings (user_id, slot, subgroup, timestamp) VALUES (?, ?, ?, ?)",
        (user_id, slot_key, subgroup, now_str)
    )

    display = parse_slot_display(slot_key, subgroup)
    msg = f"✅ Вы записаны на {display}\n📅 Дата записи: {now_str}"
    send_and_delete(peer_id, msg, delay=DELETE_DELAY)

    # Обновляем меню записи
    time.sleep(1)
    handle_record_menu(peer_id, user_id)


def handle_feedback_input(peer_id, user_id):
    """Начинает прием фидбека."""
    text = "💬 Напишите ваше мнение/предложение.\nОтправьте текст или нажмите «Назад»."
    kb = make_back_kb()
    send_and_delete(peer_id, text, kb, delay=30)
    set_state(user_id, 'wait_feedback', {'peer_id': peer_id})


def handle_suggest_theme(peer_id, user_id):
    """Начинает прием тем."""
    bookings = get_user_bookings(user_id)
    if not bookings:
        send_and_delete(peer_id, "⚠ У вас нет записей, чтобы предложить тему.", delay=10)
        return

    text = "💡 Напишите темы для занятий.\nКаждое сообщение — одна тема.\nНажмите «🛑 Стоп» для выхода."
    kb = make_stop_kb()
    send_and_delete(peer_id, text, kb, delay=30)
    set_state(user_id, 'wait_theme', {'peer_id': peer_id})


def handle_delete_record_menu(peer_id, user_id):
    """Показывает список записей для удаления."""
    bookings = get_user_bookings(user_id)
    if not bookings:
        send_and_delete(peer_id, "📋 У вас нет записей для удаления.", delay=10)
        handle_my_records(peer_id, user_id)
        return

    text = "❌ Выберите номер записи для удаления:\n"
    for i, b in enumerate(bookings, 1):
        display = parse_slot_display(b['slot'], b['subgroup'])
        text += f"  {i}. {display}\n"
    text += "\nНапишите номер или «Назад»."

    kb = make_back_kb()
    send_and_delete(peer_id, text, kb, delay=30)
    set_state(user_id, 'wait_delete_num', {
        'peer_id': peer_id,
        'bookings': [{'id': b['id'], 'slot': b['slot'], 'subgroup': b['subgroup']} for b in bookings]
    })


# ==================== АДМИНКА ====================
def handle_admin_menu(peer_id, user_id):
    text = "🔧 Панель администратора:"
    kb = make_admin_kb()
    send_and_delete(peer_id, text, kb, delay=DELETE_DELAY)
    set_state(user_id, 'admin_main', {'peer_id': peer_id})


def handle_admin_records(peer_id, user_id):
    week_type = "Чётная" if is_even_week() else "Нечётная"
    text = f"📊 Записи (неделя: {week_type}).\nВыберите слот:"
    kb = make_admin_slots_kb()
    send_and_delete(peer_id, text, kb, delay=DELETE_DELAY)
    set_state(user_id, 'admin_records_select', {'peer_id': peer_id})


def handle_admin_slot_view(peer_id, user_id, text_btn):
    """Показывает записи на выбранный слот (админ)."""
    parsed = parse_slot_from_button(text_btn)
    if not parsed:
        send_and_delete(peer_id, "⚠ Не удалось распознать слот.", delay=10)
        return

    slot_key, subgroup = parsed
    bookings = get_slot_bookings(slot_key, subgroup)

    display = parse_slot_display(slot_key, subgroup)
    if not bookings:
        text = f"📊 {display}\nЗаписей нет."
    else:
        text = f"📊 {display}\nЗаписанные ({len(bookings)}/{MAX_PER_SLOT}):\n"
        for i, b in enumerate(bookings, 1):
            text += f"  {i}. vk.com/id{b['user_id']} (ID: {b['user_id']})\n"

    kb = make_admin_slot_actions_kb()
    send_and_delete(peer_id, text, kb, delay=30)
    set_state(user_id, 'admin_slot_view', {
        'peer_id': peer_id,
        'slot_key': slot_key,
        'subgroup': subgroup,
        'bookings': [{'id': b['id'], 'user_id': b['user_id']} for b in bookings]
    })


def handle_admin_feedback(peer_id, user_id):
    rows = db_execute(
        "SELECT * FROM feedback ORDER BY id DESC LIMIT 10", fetch=True
    )
    if not rows:
        text = "📨 Фидбеков пока нет."
    else:
        text = "📨 Последние фидбеки:\n\n"
        for r in rows:
            text += f"🔹 ID{r['user_id']} ({r['timestamp']}):\n{r['text']}\n\n"

    kb = make_back_kb()
    send_and_delete(peer_id, text, kb, delay=30)
    set_state(user_id, 'admin_feedback_view', {'peer_id': peer_id})


def handle_admin_delete_record(peer_id, user_id):
    state = get_state(user_id)
    data = state.get('data', {})
    bookings = data.get('bookings', [])
    slot_key = data.get('slot_key', '')
    subgroup = data.get('subgroup', 1)

    if not bookings:
        send_and_delete(peer_id, "Нет записей для удаления.", delay=10)
        return

    text = "Напишите номер записи для удаления:\n"
    for i, b in enumerate(bookings, 1):
        text += f"  {i}. vk.com/id{b['user_id']}\n"

    kb = make_back_kb()
    send_and_delete(peer_id, text, kb, delay=30)
    set_state(user_id, 'admin_wait_delete_num', {
        'peer_id': peer_id,
        'slot_key': slot_key,
        'subgroup': subgroup,
        'bookings': bookings
    })


def handle_admin_themes_list(peer_id, user_id):
    state = get_state(user_id)
    data = state.get('data', {})
    slot_key = data.get('slot_key', '')
    subgroup = data.get('subgroup', 1)

    rows = db_execute(
        "SELECT * FROM suggestions WHERE slot=? ORDER BY id",
        (slot_key,), fetch=True
    )

    display = parse_slot_display(slot_key, subgroup)
    if not rows:
        text = f"📝 Темы для {display}:\nТем пока нет."
    else:
        text = f"📝 Темы для {display}:\n\n"
        for r in rows:
            text += f"🔹 ID{r['user_id']}: {r['theme_text']}\n"

    kb = make_back_kb()
    send_and_delete(peer_id, text, kb, delay=30)
    set_state(user_id, 'admin_themes_view', {
        'peer_id': peer_id,
        'slot_key': slot_key,
        'subgroup': subgroup,
        'bookings': data.get('bookings', [])
    })


# ==================== MAIN HANDLER ====================
def process_message(peer_id, from_id, text, is_chat=False):
    """Главный обработчик сообщений."""
    user_id = from_id
    text = text.strip()
    state_info = get_state(user_id)
    state = state_info['state']
    data = state_info.get('data', {})

    print(f"[MSG] user={user_id}, peer={peer_id}, state={state}, text={text[:50]}")

    # Универсальные команды
    if text.lower() in ['начать', 'start', 'привет', 'меню', 'начало']:
        handle_main_menu(peer_id, user_id, is_chat)
        return

    # ==================== MAIN STATE ====================
    if state == 'main' or state == '':
        if text == "📝 Записаться":
            handle_record_menu(peer_id, user_id)
        elif text == "📋 Мои записи":
            handle_my_records(peer_id, user_id)
        elif text == "💬 Фидбек" and not is_chat:
            handle_feedback_input(peer_id, user_id)
        elif text == "🔧 Админка" and user_id in ADMINS and not is_chat:
            handle_admin_menu(peer_id, user_id)
        else:
            handle_main_menu(peer_id, user_id, is_chat)
        return

    # ==================== BACK ====================
    if text == "⬅ Назад":
        if state in ['record_select', 'my_records', 'wait_feedback']:
            handle_main_menu(peer_id, user_id, is_chat)
        elif state in ['wait_theme', 'wait_delete_num']:
            handle_my_records(peer_id, user_id)
        elif state in ['admin_main']:
            handle_main_menu(peer_id, user_id, is_chat)
        elif state in ['admin_records_select', 'admin_feedback_view']:
            handle_admin_menu(peer_id, user_id)
        elif state in ['admin_slot_view', 'admin_wait_delete_num', 'admin_themes_view']:
            handle_admin_records(peer_id, user_id)
        else:
            handle_main_menu(peer_id, user_id, is_chat)
        return

    # ==================== RECORD SELECT ====================
    if state == 'record_select':
        handle_slot_booking(peer_id, user_id, text)
        return

    # ==================== MY RECORDS ====================
    if state == 'my_records':
        if text == "💡 Предложить тему":
            handle_suggest_theme(peer_id, user_id)
        elif text == "❌ Удалить запись":
            handle_delete_record_menu(peer_id, user_id)
        elif text == "📝 Записаться":
            handle_record_menu(peer_id, user_id)
        else:
            handle_my_records(peer_id, user_id)
        return

    # ==================== WAIT FEEDBACK ====================
    if state == 'wait_feedback':
        # Сохраняем фидбек
        now_str = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        db_execute(
            "INSERT INTO feedback (user_id, text, timestamp) VALUES (?, ?, ?)",
            (user_id, text, now_str)
        )
        send_and_delete(peer_id, "💚 Спасибо! Мы читаем все отзывы!", delay=15)
        time.sleep(1)
        handle_main_menu(peer_id, user_id, is_chat)
        return

    # ==================== WAIT THEME ====================
    if state == 'wait_theme':
        if text == "🛑 Стоп" or text.lower() == "стоп":
            send_and_delete(peer_id, "✅ Темы сохранены!", delay=10)
            time.sleep(1)
            handle_my_records(peer_id, user_id)
            return

        # Сохраняем тему для всех слотов юзера
        bookings = get_user_bookings(user_id)
        if bookings:
            for b in bookings:
                db_execute(
                    "INSERT INTO suggestions (user_id, slot, theme_text) VALUES (?, ?, ?)",
                    (user_id, b['slot'], text)
                )
            send_and_delete(peer_id, f"📝 Тема «{text[:50]}» сохранена!", delay=10)
        else:
            send_and_delete(peer_id, "⚠ У вас нет записей.", delay=10)
            handle_my_records(peer_id, user_id)
        return

    # ==================== WAIT DELETE NUM ====================
    if state == 'wait_delete_num':
        try:
            num = int(text)
            bookings_list = data.get('bookings', [])
            if 1 <= num <= len(bookings_list):
                booking = bookings_list[num - 1]
                booking_id = booking['id']
                slot_key = booking['slot']

                # Удаляем запись
                db_execute("DELETE FROM bookings WHERE id=?", (booking_id,))
                # Удаляем темы юзера по этому слоту
                db_execute(
                    "DELETE FROM suggestions WHERE user_id=? AND slot=?",
                    (user_id, slot_key)
                )
                send_and_delete(peer_id, "✅ Запись удалена!", delay=10)
                time.sleep(1)
                handle_my_records(peer_id, user_id)
            else:
                send_and_delete(peer_id, "⚠ Неверный номер. Попробуйте ещё.", delay=10)
        except ValueError:
            send_and_delete(peer_id, "⚠ Введите номер записи.", delay=10)
        return

    # ==================== ADMIN STATES ====================
    if state == 'admin_main':
        if text == "📊 Записи":
            handle_admin_records(peer_id, user_id)
        elif text == "📨 Фидбек (админ)":
            handle_admin_feedback(peer_id, user_id)
        else:
            handle_admin_menu(peer_id, user_id)
        return

    if state == 'admin_records_select':
        handle_admin_slot_view(peer_id, user_id, text)
        return

    if state == 'admin_slot_view':
        if text == "🗑 Удалить запись":
            handle_admin_delete_record(peer_id, user_id)
        elif text == "📝 Список тем":
            handle_admin_themes_list(peer_id, user_id)
        else:
            handle_admin_records(peer_id, user_id)
        return

    if state == 'admin_wait_delete_num':
        try:
            num = int(text)
            bookings_list = data.get('bookings', [])
            if 1 <= num <= len(bookings_list):
                booking = bookings_list[num - 1]
                booking_id = booking['id']
                db_execute("DELETE FROM bookings WHERE id=?", (booking_id,))
                send_and_delete(peer_id, "✅ Запись удалена!", delay=10)
                time.sleep(1)
                # Обновляем вид слота
                handle_admin_slot_view(peer_id, user_id,
                                       f"A:{data.get('slot_key', '')}_{data.get('subgroup', 1)}")
                # Вернёмся к выбору слотов
                handle_admin_records(peer_id, user_id)
            else:
                send_and_delete(peer_id, "⚠ Неверный номер.", delay=10)
        except ValueError:
            send_and_delete(peer_id, "⚠ Введите номер.", delay=10)
        return

    if state == 'admin_themes_view':
        handle_admin_records(peer_id, user_id)
        return

    if state == 'admin_feedback_view':
        handle_admin_menu(peer_id, user_id)
        return

    # Fallback
    handle_main_menu(peer_id, user_id, is_chat)


# ==================== LONGPOLL WORKER ====================
def longpoll_worker():
    """Основной цикл обработки событий VK."""
    global longpoll
    print("[LONGPOLL] Starting...")
    while True:
        try:
            if longpoll is None:
                init_vk()
            for event in longpoll.listen():
                if event.type == VkBotEventType.MESSAGE_NEW:
                    msg = event.obj.message
                    peer_id = msg['peer_id']
                    from_id = msg['from_id']
                    text = msg.get('text', '')

                    # Игнорируем сообщения от групп (from_id < 0)
                    if from_id < 0:
                        continue

                    is_chat = peer_id != from_id  # беседа
                    # В беседе peer_id > 2000000000

                    # Обрабатываем в отдельном потоке чтобы не блокировать longpoll
                    t = threading.Thread(
                        target=process_message,
                        args=(peer_id, from_id, text, is_chat),
                        daemon=True
                    )
                    t.start()

        except Exception as e:
            print(f"[LONGPOLL ERROR] {e}")
            traceback.print_exc()
            time.sleep(5)
            try:
                init_vk()
            except Exception:
                pass


# ==================== MAIN ====================
if __name__ == '__main__':
    print("=" * 50)
    print("VK Bot Starting...")
    print(f"Week type: {'Even' if is_even_week() else 'Odd'}")
    print(f"ISO week: {datetime.datetime.now().isocalendar()[1]}")
    print(f"Admins: {ADMINS}")
    print("=" * 50)

    # Инициализация
    init_db()
    init_vk()

    # Запуск LongPoll в фоновом потоке
    lp_thread = threading.Thread(target=longpoll_worker, daemon=True)
    lp_thread.start()

    # Запуск Flask (для хостинга — Render/Railway нужен HTTP-порт)
    port = int(os.environ.get('PORT', 5000))
    print(f"[FLASK] Starting on port {port}")
    app.run(host='0.0.0.0', port=port)