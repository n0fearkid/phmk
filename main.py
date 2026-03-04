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
DELETE_DELAY = 8  # секунд до удаления сообщений бота

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

# Хранит последнее сообщение бота для каждого peer_id
# чтобы НЕ удалять его (удаляем только предпоследние)
last_bot_msg = {}
last_bot_msg_lock = threading.Lock()

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
    now = datetime.datetime.now()
    week_num = now.isocalendar()[1]
    return week_num % 2 == 0


def get_current_week_type():
    return "even" if is_even_week() else "odd"


def get_current_slots():
    if is_even_week():
        return EVEN_SLOTS
    else:
        return ODD_SLOTS


def get_available_slots():
    """
    Возвращает только БУДУЩИЕ слоты текущей недели.
    Прошедшие даты/время отфильтровываются.
    """
    now = datetime.datetime.now()
    today = now.date()
    current_weekday = today.weekday()

    slots = get_current_slots()
    result = []
    for slot in slots:
        diff = slot["weekday"] - current_weekday
        slot_date = today + datetime.timedelta(days=diff)

        # Парсим время слота
        hour, minute = map(int, slot["time"].split(':'))
        slot_datetime = datetime.datetime.combine(slot_date, datetime.time(hour, minute))

        # Пропускаем прошедшие
        if slot_datetime < now:
            continue

        result.append({
            "day": slot["day"],
            "time": slot["time"],
            "weekday": slot["weekday"],
            "date": slot_date,
            "datetime": slot_datetime,
        })
    return result


def make_slot_key(slot_info):
    """Ключ слота: 'even_Пн_16:00_2025-01-15'"""
    week_type = get_current_week_type()
    return f"{week_type}_{slot_info['day']}_{slot_info['time']}_{slot_info['date'].isoformat()}"


def parse_slot_display(slot_key):
    """Из ключа слота → читаемая строка."""
    parts = slot_key.split('_')
    if len(parts) >= 4:
        week_type = "Чёт" if parts[0] == "even" else "Нечёт"
        day = parts[1]
        time_str = parts[2]
        date_str = parts[3]
        return f"{day} {date_str} {time_str} ({week_type})"
    return slot_key


def count_booked(slot_key):
    row = db_execute(
        "SELECT COUNT(*) as cnt FROM bookings WHERE slot=?",
        (slot_key,), fetchone=True
    )
    return row['cnt'] if row else 0


def is_user_booked(user_id, slot_key):
    row = db_execute(
        "SELECT id FROM bookings WHERE user_id=? AND slot=?",
        (user_id, slot_key), fetchone=True
    )
    return row is not None


def get_user_bookings(user_id):
    return db_execute(
        "SELECT * FROM bookings WHERE user_id=? ORDER BY timestamp DESC",
        (user_id,), fetch=True
    )


def get_slot_bookings(slot_key):
    return db_execute(
        "SELECT * FROM bookings WHERE slot=? ORDER BY id",
        (slot_key,), fetch=True
    )


def get_slot_themes(slot_key):
    return db_execute(
        "SELECT * FROM suggestions WHERE slot=? ORDER BY id",
        (slot_key,), fetch=True
    )


# ==================== VK API ====================
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
    try:
        params = {
            'peer_id': peer_id,
            'message': text,
            'random_id': int(time.time() * 1000000) % (2 ** 31),
        }
        if keyboard:
            params['keyboard'] = keyboard
        result = vk.messages.send(**params)
        print(f"[SEND] peer={peer_id}, msg_id={result}")
        return result
    except Exception as e:
        print(f"[ERROR] send_message: {e}")
        traceback.print_exc()
        return None


def delete_message_later(peer_id, message_id, delay=DELETE_DELAY):
    def _delete():
        time.sleep(delay)
        try:
            vk.messages.delete(
                peer_id=peer_id,
                message_ids=message_id,
                delete_for_all=1,
                group_id=GROUP_ID
            )
            print(f"[DELETE] peer={peer_id}, msg_id={message_id}")
        except Exception as e:
            print(f"[DELETE ERR] {e}")

    t = threading.Thread(target=_delete, daemon=True)
    t.start()


def send_auto(peer_id, text, keyboard=None, delay=DELETE_DELAY):
    """
    Отправляет сообщение.
    Предыдущее сообщение бота в этом чате ставится на удаление.
    Текущее (последнее) НЕ удаляется.
    """
    msg_id = send_message(peer_id, text, keyboard)
    if msg_id is None:
        return None

    with last_bot_msg_lock:
        prev = last_bot_msg.get(peer_id)
        last_bot_msg[peer_id] = msg_id

    # Удаляем ПРЕДЫДУЩЕЕ сообщение бота, а не текущее
    if prev:
        delete_message_later(peer_id, prev, delay)

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
    """Кнопки доступных (будущих) слотов + Назад."""
    kb = VkKeyboard(one_time=False)
    available = get_available_slots()

    if not available:
        kb.add_button("Нет доступных слотов", color=VkKeyboardColor.SECONDARY)
        kb.add_line()
        kb.add_button("⬅ Назад", color=VkKeyboardColor.SECONDARY)
        return kb.get_keyboard()

    for i, slot_info in enumerate(available):
        day = slot_info['day']
        t = slot_info['time']
        date_str = slot_info['date'].strftime('%d.%m')
        slot_key = make_slot_key(slot_info)
        count = count_booked(slot_key)

        label = f"{day} {date_str} {t} [{count}/{MAX_PER_SLOT}]"
        if len(label) > 40:
            label = label[:40]

        color = VkKeyboardColor.PRIMARY if count < MAX_PER_SLOT else VkKeyboardColor.SECONDARY
        kb.add_button(label, color=color)
        if i < len(available) - 1:
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


def make_stop_kb():
    kb = VkKeyboard(one_time=False)
    kb.add_button("🛑 Стоп", color=VkKeyboardColor.NEGATIVE)
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
    kb = VkKeyboard(one_time=False)
    available = get_available_slots()

    if not available:
        kb.add_button("Нет слотов", color=VkKeyboardColor.SECONDARY)
        kb.add_line()
        kb.add_button("⬅ Назад", color=VkKeyboardColor.SECONDARY)
        return kb.get_keyboard()

    for i, slot_info in enumerate(available):
        day = slot_info['day']
        t = slot_info['time']
        date_str = slot_info['date'].strftime('%d.%m')
        slot_key = make_slot_key(slot_info)
        count = count_booked(slot_key)

        label = f"A:{day} {date_str} {t} [{count}]"
        if len(label) > 40:
            label = label[:40]

        kb.add_button(label, color=VkKeyboardColor.PRIMARY)
        if i < len(available) - 1:
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


def make_user_slots_select_kb(bookings):
    """Клавиатура выбора своего слота (для тем/удаления)."""
    kb = VkKeyboard(one_time=False)
    for i, b in enumerate(bookings):
        display = parse_slot_display(b['slot'])
        label = f"{i + 1}. {display}"
        if len(label) > 40:
            label = label[:40]
        kb.add_button(label, color=VkKeyboardColor.PRIMARY)
        if i < len(bookings) - 1:
            kb.add_line()
    kb.add_line()
    kb.add_button("⬅ Назад", color=VkKeyboardColor.SECONDARY)
    return kb.get_keyboard()


# ==================== STATE ====================
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
    Парсит кнопку вида 'Пн 15.01 16:00 [3/10]' или 'A:Пн 15.01 16:00 [3]'
    Возвращает slot_key или None.
    """
    try:
        clean = text.strip()
        if clean.startswith("A:"):
            clean = clean[2:]

        parts = clean.split()
        if len(parts) < 3:
            return None

        day = parts[0]       # 'Пн'
        date_short = parts[1]  # '15.01'
        time_str = parts[2]  # '16:00'

        now = datetime.datetime.now()
        year = now.year
        day_num, month_num = date_short.split('.')
        slot_date = datetime.date(year, int(month_num), int(day_num))

        week_type = get_current_week_type()
        slot_key = f"{week_type}_{day}_{time_str}_{slot_date.isoformat()}"
        return slot_key

    except Exception as e:
        print(f"[PARSE ERR] '{text}': {e}")
        return None


# ==================== ОБРАБОТЧИКИ ====================
def handle_main_menu(peer_id, user_id, is_chat=False):
    bookings = get_user_bookings(user_id)
    text = "👋 Привет! Я бот для записи на занятия.\n"

    if bookings:
        text += "\n📌 Ваши записи:\n"
        for b in bookings:
            display = parse_slot_display(b['slot'])
            text += f"  • {display}\n"
    else:
        text += "\nУ вас пока нет записей.\n"

    text += "\nВыберите действие:"
    kb = make_main_kb(user_id, is_chat)
    send_auto(peer_id, text, kb)
    set_state(user_id, 'main', {'peer_id': peer_id, 'is_chat': is_chat})


def handle_record_menu(peer_id, user_id):
    available = get_available_slots()
    week_type = "Чётная" if is_even_week() else "Нечётная"

    if not available:
        text = f"📅 Неделя: {week_type}\n❌ Все слоты на этой неделе уже прошли."
    else:
        text = f"📅 Неделя: {week_type}\nВыберите слот (макс {MAX_PER_SLOT} чел.):"

    kb = make_slots_kb()
    send_auto(peer_id, text, kb)
    set_state(user_id, 'record_select', {'peer_id': peer_id})


def handle_my_records(peer_id, user_id):
    bookings = get_user_bookings(user_id)
    if not bookings:
        text = "📋 У вас нет записей."
    else:
        text = "📋 Ваши записи:\n"
        for i, b in enumerate(bookings, 1):
            display = parse_slot_display(b['slot'])
            text += f"  {i}. {display} (записан {b['timestamp']})\n"

    kb = make_my_records_kb()
    send_auto(peer_id, text, kb)
    set_state(user_id, 'my_records', {'peer_id': peer_id})


def handle_slot_booking(peer_id, user_id, text):
    if text == "Нет доступных слотов":
        send_auto(peer_id, "❌ Слотов нет. Ждите следующей недели.")
        return

    slot_key = parse_slot_from_button(text)
    if not slot_key:
        send_auto(peer_id, "⚠ Не удалось распознать слот.")
        return

    if is_user_booked(user_id, slot_key):
        send_auto(peer_id, "⚠ Вы уже записаны на этот слот!")
        return

    count = count_booked(slot_key)
    if count >= MAX_PER_SLOT:
        send_auto(peer_id, "⚠ Слот заполнен! Выберите другой.")
        return

    now_str = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    db_execute(
        "INSERT INTO bookings (user_id, slot, timestamp) VALUES (?, ?, ?)",
        (user_id, slot_key, now_str)
    )

    display = parse_slot_display(slot_key)
    msg = f"✅ Вы записаны на {display}\n📅 Дата записи: {now_str}"
    send_auto(peer_id, msg)

    time.sleep(1)
    handle_record_menu(peer_id, user_id)


def handle_feedback_input(peer_id, user_id):
    text = "💬 Напишите ваше мнение/предложение.\nИли нажмите «Назад»."
    kb = make_back_kb()
    send_auto(peer_id, text, kb)
    set_state(user_id, 'wait_feedback', {'peer_id': peer_id})


def handle_suggest_theme_select(peer_id, user_id):
    """Выбор занятия, к которому предложить тему."""
    bookings = get_user_bookings(user_id)
    if not bookings:
        send_auto(peer_id, "⚠ У вас нет записей.")
        time.sleep(1)
        handle_my_records(peer_id, user_id)
        return

    if len(bookings) == 1:
        # Одна запись — сразу к вводу тем
        slot_key = bookings[0]['slot']
        start_theme_input(peer_id, user_id, slot_key)
        return

    text = "💡 Выберите занятие для предложения темы:"
    kb = make_user_slots_select_kb(bookings)
    send_auto(peer_id, text, kb)
    set_state(user_id, 'theme_select_slot', {
        'peer_id': peer_id,
        'bookings': [{'id': b['id'], 'slot': b['slot']} for b in bookings]
    })


def start_theme_input(peer_id, user_id, slot_key):
    display = parse_slot_display(slot_key)
    text = f"💡 Занятие: {display}\n\nПишите темы — каждое сообщение = одна тема.\nНажмите «🛑 Стоп» для выхода."
    kb = make_stop_kb()
    send_auto(peer_id, text, kb)
    set_state(user_id, 'wait_theme', {'peer_id': peer_id, 'slot_key': slot_key})


def handle_delete_record_menu(peer_id, user_id):
    bookings = get_user_bookings(user_id)
    if not bookings:
        send_auto(peer_id, "📋 Нет записей для удаления.")
        time.sleep(1)
        handle_my_records(peer_id, user_id)
        return

    text = "❌ Выберите запись для удаления:\n"
    for i, b in enumerate(bookings, 1):
        display = parse_slot_display(b['slot'])
        text += f"  {i}. {display}\n"
    text += "\nНапишите номер или «Назад»."

    kb = make_back_kb()
    send_auto(peer_id, text, kb)
    set_state(user_id, 'wait_delete_num', {
        'peer_id': peer_id,
        'bookings': [{'id': b['id'], 'slot': b['slot']} for b in bookings]
    })


# ==================== АДМИНКА ====================
def handle_admin_menu(peer_id, user_id):
    text = "🔧 Панель администратора:"
    kb = make_admin_kb()
    send_auto(peer_id, text, kb)
    set_state(user_id, 'admin_main', {'peer_id': peer_id})


def handle_admin_records(peer_id, user_id):
    week_type = "Чётная" if is_even_week() else "Нечётная"
    text = f"📊 Записи (неделя: {week_type}).\nВыберите слот:"
    kb = make_admin_slots_kb()
    send_auto(peer_id, text, kb)
    set_state(user_id, 'admin_records_select', {'peer_id': peer_id})


def handle_admin_slot_view(peer_id, user_id, text_btn):
    slot_key = parse_slot_from_button(text_btn)
    if not slot_key:
        send_auto(peer_id, "⚠ Не удалось распознать слот.")
        return

    bookings = get_slot_bookings(slot_key)
    themes = get_slot_themes(slot_key)
    display = parse_slot_display(slot_key)

    text = f"📊 {display}\n\n"

    if not bookings:
        text += "Записей нет.\n"
    else:
        text += f"👥 Записанные ({len(bookings)}/{MAX_PER_SLOT}):\n"
        for i, b in enumerate(bookings, 1):
            text += f"  {i}. vk.com/id{b['user_id']}\n"

    text += f"\n📝 Темы ({len(themes)}):\n"
    if not themes:
        text += "  Тем пока нет.\n"
    else:
        for t in themes:
            text += f"  • {t['theme_text']} (от id{t['user_id']})\n"

    kb = make_admin_slot_actions_kb()
    send_auto(peer_id, text, kb)
    set_state(user_id, 'admin_slot_view', {
        'peer_id': peer_id,
        'slot_key': slot_key,
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
            text += f"🔹 id{r['user_id']} ({r['timestamp']}):\n{r['text']}\n\n"

    kb = make_back_kb()
    send_auto(peer_id, text, kb)
    set_state(user_id, 'admin_feedback_view', {'peer_id': peer_id})


def handle_admin_delete_record(peer_id, user_id):
    state = get_state(user_id)
    data = state.get('data', {})
    bookings = data.get('bookings', [])
    slot_key = data.get('slot_key', '')

    if not bookings:
        send_auto(peer_id, "Нет записей для удаления.")
        return

    text = "Напишите номер записи для удаления:\n"
    for i, b in enumerate(bookings, 1):
        text += f"  {i}. vk.com/id{b['user_id']}\n"

    kb = make_back_kb()
    send_auto(peer_id, text, kb)
    set_state(user_id, 'admin_wait_delete_num', {
        'peer_id': peer_id,
        'slot_key': slot_key,
        'bookings': bookings
    })


def handle_admin_themes_list(peer_id, user_id):
    state = get_state(user_id)
    data = state.get('data', {})
    slot_key = data.get('slot_key', '')

    themes = get_slot_themes(slot_key)
    display = parse_slot_display(slot_key)

    if not themes:
        text = f"📝 Темы для {display}:\nТем пока нет."
    else:
        text = f"📝 Темы для {display}:\n\n"
        for t in themes:
            text += f"  • {t['theme_text']} (от id{t['user_id']})\n"

    kb = make_back_kb()
    send_auto(peer_id, text, kb)
    set_state(user_id, 'admin_themes_view', {
        'peer_id': peer_id,
        'slot_key': slot_key,
        'bookings': data.get('bookings', [])
    })


# ==================== MAIN HANDLER ====================
def process_message(peer_id, from_id, text, is_chat=False):
    user_id = from_id
    text = text.strip()
    state_info = get_state(user_id)
    state = state_info['state']
    data = state_info.get('data', {})

    print(f"[MSG] user={user_id}, peer={peer_id}, state={state}, text={text[:60]}")

    # Универсальный старт
    if text.lower() in ['начать', 'start', 'привет', 'меню', 'начало']:
        handle_main_menu(peer_id, user_id, is_chat)
        return

    # ---- НАЗАД ----
    if text == "⬅ Назад":
        if state in ['record_select', 'my_records', 'wait_feedback']:
            handle_main_menu(peer_id, user_id, is_chat)
        elif state in ['wait_theme', 'wait_delete_num', 'theme_select_slot']:
            handle_my_records(peer_id, user_id)
        elif state == 'admin_main':
            handle_main_menu(peer_id, user_id, is_chat)
        elif state in ['admin_records_select', 'admin_feedback_view']:
            handle_admin_menu(peer_id, user_id)
        elif state in ['admin_slot_view', 'admin_wait_delete_num', 'admin_themes_view']:
            handle_admin_records(peer_id, user_id)
        else:
            handle_main_menu(peer_id, user_id, is_chat)
        return

    # ---- MAIN ----
    if state == 'main':
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

    # ---- RECORD SELECT ----
    if state == 'record_select':
        handle_slot_booking(peer_id, user_id, text)
        return

    # ---- MY RECORDS ----
    if state == 'my_records':
        if text == "💡 Предложить тему":
            handle_suggest_theme_select(peer_id, user_id)
        elif text == "❌ Удалить запись":
            handle_delete_record_menu(peer_id, user_id)
        else:
            handle_my_records(peer_id, user_id)
        return

    # ---- THEME SELECT SLOT ----
    if state == 'theme_select_slot':
        bookings = data.get('bookings', [])
        # Пробуем по номеру кнопки
        selected_slot = None
        for i, b in enumerate(bookings):
            display = parse_slot_display(b['slot'])
            label = f"{i + 1}. {display}"
            if text.startswith(f"{i + 1}.") or text == label[:40]:
                selected_slot = b['slot']
                break

        if not selected_slot:
            # Пробуем как число
            try:
                num = int(text)
                if 1 <= num <= len(bookings):
                    selected_slot = bookings[num - 1]['slot']
            except ValueError:
                pass

        if selected_slot:
            start_theme_input(peer_id, user_id, selected_slot)
        else:
            send_auto(peer_id, "⚠ Выберите занятие из списка.")
        return

    # ---- WAIT THEME ----
    if state == 'wait_theme':
        if text == "🛑 Стоп" or text.lower() == "стоп":
            send_auto(peer_id, "✅ Темы сохранены!")
            time.sleep(1)
            handle_my_records(peer_id, user_id)
            return

        slot_key = data.get('slot_key', '')
        if slot_key:
            db_execute(
                "INSERT INTO suggestions (user_id, slot, theme_text) VALUES (?, ?, ?)",
                (user_id, slot_key, text)
            )
            display = parse_slot_display(slot_key)
            send_auto(peer_id, f"📝 Тема сохранена для {display}!\nПродолжайте или «🛑 Стоп».",
                       make_stop_kb())
        else:
            send_auto(peer_id, "⚠ Ошибка: слот не найден.")
            handle_my_records(peer_id, user_id)
        return

    # ---- WAIT FEEDBACK ----
    if state == 'wait_feedback':
        now_str = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        db_execute(
            "INSERT INTO feedback (user_id, text, timestamp) VALUES (?, ?, ?)",
            (user_id, text, now_str)
        )
        send_auto(peer_id, "💚 Спасибо! Мы читаем все отзывы!")
        time.sleep(1)
        handle_main_menu(peer_id, user_id, is_chat)
        return

    # ---- WAIT DELETE NUM ----
    if state == 'wait_delete_num':
        try:
            num = int(text)
            bookings_list = data.get('bookings', [])
            if 1 <= num <= len(bookings_list):
                booking = bookings_list[num - 1]
                db_execute("DELETE FROM bookings WHERE id=?", (booking['id'],))
                db_execute(
                    "DELETE FROM suggestions WHERE user_id=? AND slot=?",
                    (user_id, booking['slot'])
                )
                send_auto(peer_id, "✅ Запись удалена!")
                time.sleep(1)
                handle_my_records(peer_id, user_id)
            else:
                send_auto(peer_id, "⚠ Неверный номер.")
        except ValueError:
            send_auto(peer_id, "⚠ Введите номер записи.")
        return

    # ---- ADMIN MAIN ----
    if state == 'admin_main':
        if text == "📊 Записи":
            handle_admin_records(peer_id, user_id)
        elif text == "📨 Фидбек (админ)":
            handle_admin_feedback(peer_id, user_id)
        else:
            handle_admin_menu(peer_id, user_id)
        return

    # ---- ADMIN RECORDS SELECT ----
    if state == 'admin_records_select':
        if text == "Нет слотов":
            send_auto(peer_id, "❌ Слотов нет.")
            return
        handle_admin_slot_view(peer_id, user_id, text)
        return

    # ---- ADMIN SLOT VIEW ----
    if state == 'admin_slot_view':
        if text == "🗑 Удалить запись":
            handle_admin_delete_record(peer_id, user_id)
        elif text == "📝 Список тем":
            handle_admin_themes_list(peer_id, user_id)
        else:
            handle_admin_records(peer_id, user_id)
        return

    # ---- ADMIN WAIT DELETE NUM ----
    if state == 'admin_wait_delete_num':
        try:
            num = int(text)
            bookings_list = data.get('bookings', [])
            if 1 <= num <= len(bookings_list):
                booking = bookings_list[num - 1]
                db_execute("DELETE FROM bookings WHERE id=?", (booking['id'],))
                send_auto(peer_id, "✅ Запись удалена!")
                time.sleep(1)
                handle_admin_records(peer_id, user_id)
            else:
                send_auto(peer_id, "⚠ Неверный номер.")
        except ValueError:
            send_auto(peer_id, "⚠ Введите номер.")
        return

    # ---- ADMIN THEMES VIEW ----
    if state == 'admin_themes_view':
        handle_admin_slot_view(peer_id, user_id,
                                f"A:{data.get('slot_key', '')}")
        return

    # ---- ADMIN FEEDBACK VIEW ----
    if state == 'admin_feedback_view':
        handle_admin_menu(peer_id, user_id)
        return

    # Fallback
    handle_main_menu(peer_id, user_id, is_chat)


# ==================== LONGPOLL ====================
def longpoll_worker():
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

                    if from_id < 0:
                        continue

                    is_chat = peer_id != from_id

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
    now = datetime.datetime.now()
    print(f"Time: {now}")
    print(f"ISO week: {now.isocalendar()[1]}")
    print(f"Week type: {'Even' if is_even_week() else 'Odd'}")
    print(f"Admins: {ADMINS}")
    print(f"Available slots: {len(get_available_slots())}")
    for s in get_available_slots():
        print(f"  {s['day']} {s['date']} {s['time']}")
    print("=" * 50)

    init_db()
    init_vk()

    lp_thread = threading.Thread(target=longpoll_worker, daemon=True)
    lp_thread.start()

    port = int(os.environ.get('PORT', 5000))
    print(f"[FLASK] Starting on port {port}")
    app.run(host='0.0.0.0', port=port)
