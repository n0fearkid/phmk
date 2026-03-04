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
DELETE_DELAY = 8
SLOTS_TO_SHOW = 4  # сколько ближайших занятий показывать

# ==================== РАСПИСАНИЕ ====================
# weekday: 0=Пн, 1=Вт, 2=Ср, 3=Чт, 4=Пт, 5=Сб, 6=Вс
# parity: "even" = чётная неделя, "odd" = нечётная

SCHEDULE = [
    {"day": "Пн", "time": "16:00", "weekday": 0, "parity": "even"},
    {"day": "Пт", "time": "16:30", "weekday": 4, "parity": "even"},
    {"day": "Сб", "time": "14:00", "weekday": 5, "parity": "even"},
    {"day": "Пн", "time": "17:30", "weekday": 0, "parity": "odd"},
    {"day": "Пт", "time": "16:30", "weekday": 4, "parity": "odd"},
    {"day": "Сб", "time": "14:00", "weekday": 5, "parity": "odd"},
]

# ==================== СОСТОЯНИЯ ====================
user_states = {}
user_states_lock = threading.Lock()

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


# ==================== SLOT ENGINE ====================
def get_week_parity(date_obj):
    """Возвращает 'even' или 'odd' для даты."""
    week_num = date_obj.isocalendar()[1]
    return "even" if week_num % 2 == 0 else "odd"


def get_next_n_slots(n=SLOTS_TO_SHOW):
    """
    Находит N ближайших БУДУЩИХ занятий от текущего момента.
    Перебирает дни вперёд, проверяет расписание и чётность недели.
    Возвращает список словарей с date, datetime, day, time, slot_key.
    """
    now = datetime.datetime.now()
    today = now.date()
    result = []
    
    # Перебираем до 60 дней вперёд (хватит с запасом)
    for days_ahead in range(0, 60):
        check_date = today + datetime.timedelta(days=days_ahead)
        check_weekday = check_date.weekday()
        check_parity = get_week_parity(check_date)
        
        # Ищем слоты на этот день
        for slot in SCHEDULE:
            if slot["weekday"] != check_weekday:
                continue
            if slot["parity"] != check_parity:
                continue
            
            # Собираем datetime слота
            hour, minute = map(int, slot["time"].split(':'))
            slot_dt = datetime.datetime.combine(
                check_date, datetime.time(hour, minute)
            )
            
            # Пропускаем прошедшие
            if slot_dt <= now:
                continue
            
            slot_key = f"{check_parity}_{slot['day']}_{slot['time']}_{check_date.isoformat()}"
            
            result.append({
                "day": slot["day"],
                "time": slot["time"],
                "date": check_date,
                "datetime": slot_dt,
                "slot_key": slot_key,
                "parity": check_parity,
            })
            
            if len(result) >= n:
                return result
    
    return result


def parse_slot_display(slot_key):
    """Из ключа слота → читаемая строка."""
    parts = slot_key.split('_')
    if len(parts) >= 4:
        week_type = "чёт" if parts[0] == "even" else "нечёт"
        day = parts[1]
        time_str = parts[2]
        date_str = parts[3]
        try:
            d = datetime.date.fromisoformat(date_str)
            date_display = d.strftime('%d.%m')
        except Exception:
            date_display = date_str
        return f"{day} {date_display} {time_str} ({week_type})"
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
        "SELECT * FROM bookings WHERE user_id=? ORDER BY slot ASC",
        (user_id,), fetch=True
    )


def get_user_future_bookings(user_id):
    """Только будущие записи пользователя."""
    all_bookings = get_user_bookings(user_id)
    if not all_bookings:
        return []
    
    now = datetime.datetime.now()
    future = []
    for b in all_bookings:
        # Парсим дату из slot_key
        parts = b['slot'].split('_')
        if len(parts) >= 4:
            try:
                date_str = parts[3]
                time_str = parts[2]
                d = datetime.date.fromisoformat(date_str)
                hour, minute = map(int, time_str.split(':'))
                slot_dt = datetime.datetime.combine(d, datetime.time(hour, minute))
                if slot_dt > now:
                    future.append(b)
            except Exception:
                future.append(b)  # если не удалось распарсить — оставляем
        else:
            future.append(b)
    return future


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
    Предыдущее сообщение бота удаляется.
    Последнее (текущее) остаётся.
    """
    msg_id = send_message(peer_id, text, keyboard)
    if msg_id is None:
        return None

    with last_bot_msg_lock:
        prev = last_bot_msg.get(peer_id)
        last_bot_msg[peer_id] = msg_id

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
    """4 кнопки ближайших занятий + Назад."""
    kb = VkKeyboard(one_time=False)
    slots = get_next_n_slots(SLOTS_TO_SHOW)

    if not slots:
        kb.add_button("Нет доступных занятий", color=VkKeyboardColor.SECONDARY)
        kb.add_line()
        kb.add_button("⬅ Назад", color=VkKeyboardColor.SECONDARY)
        return kb.get_keyboard()

    for i, s in enumerate(slots):
        date_str = s['date'].strftime('%d.%m')
        count = count_booked(s['slot_key'])
        parity = "чёт" if s['parity'] == "even" else "нечёт"

        label = f"{s['day']} {date_str} {s['time']} [{count}/{MAX_PER_SLOT}]"
        if len(label) > 40:
            label = label[:40]

        color = VkKeyboardColor.PRIMARY if count < MAX_PER_SLOT else VkKeyboardColor.SECONDARY
        kb.add_button(label, color=color)
        if i < len(slots) - 1:
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
    slots = get_next_n_slots(SLOTS_TO_SHOW)

    if not slots:
        kb.add_button("Нет слотов", color=VkKeyboardColor.SECONDARY)
        kb.add_line()
        kb.add_button("⬅ Назад", color=VkKeyboardColor.SECONDARY)
        return kb.get_keyboard()

    for i, s in enumerate(slots):
        date_str = s['date'].strftime('%d.%m')
        count = count_booked(s['slot_key'])

        label = f"A:{s['day']} {date_str} {s['time']} [{count}]"
        if len(label) > 40:
            label = label[:40]

        kb.add_button(label, color=VkKeyboardColor.PRIMARY)
        if i < len(slots) - 1:
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
    Парсит кнопку: 'Пн 15.01 16:00 [3/10]' или 'A:Пн 15.01 16:00 [3]'
    Возвращает slot_key или None.
    """
    try:
        clean = text.strip()
        if clean.startswith("A:"):
            clean = clean[2:]

        parts = clean.split()
        if len(parts) < 3:
            return None

        day = parts[0]
        date_short = parts[1]
        time_str = parts[2]

        now = datetime.datetime.now()
        day_num, month_num = date_short.split('.')
        
        # Определяем год: если месяц меньше текущего — следующий год
        year = now.year
        if int(month_num) < now.month - 1:
            year += 1
        
        slot_date = datetime.date(year, int(month_num), int(day_num))
        parity = get_week_parity(slot_date)
        slot_key = f"{parity}_{day}_{time_str}_{slot_date.isoformat()}"
        return slot_key

    except Exception as e:
        print(f"[PARSE ERR] '{text}': {e}")
        return None


# ==================== ОБРАБОТЧИКИ ====================
def handle_main_menu(peer_id, user_id, is_chat=False):
    bookings = get_user_future_bookings(user_id)
    text = "👋 Привет! Я бот для записи на занятия.\n"

    if bookings:
        text += "\n📌 Ваши ближайшие записи:\n"
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
    slots = get_next_n_slots(SLOTS_TO_SHOW)

    if not slots:
        text = "📅 Нет доступных занятий в ближайшее время."
    else:
        text = f"📅 Ближайшие {len(slots)} занятий:\n"
        text += f"(макс {MAX_PER_SLOT} чел. на занятие)\n"
        text += "Выберите слот:"

    kb = make_slots_kb()
    send_auto(peer_id, text, kb)
    set_state(user_id, 'record_select', {'peer_id': peer_id})


def handle_my_records(peer_id, user_id):
    bookings = get_user_future_bookings(user_id)
    if not bookings:
        text = "📋 У вас нет предстоящих записей."
    else:
        text = "📋 Ваши записи:\n"
        for i, b in enumerate(bookings, 1):
            display = parse_slot_display(b['slot'])
            text += f"  {i}. {display} (записан {b['timestamp']})\n"

    kb = make_my_records_kb()
    send_auto(peer_id, text, kb)
    set_state(user_id, 'my_records', {'peer_id': peer_id})


def handle_slot_booking(peer_id, user_id, text):
    if text == "Нет доступных занятий":
        send_auto(peer_id, "❌ Занятий нет.")
        return

    slot_key = parse_slot_from_button(text)
    if not slot_key:
        send_auto(peer_id, "⚠ Не удалось распознать слот.")
        return

    if is_user_booked(user_id, slot_key):
        send_auto(peer_id, "⚠ Вы уже записаны на это занятие!")
        time.sleep(1)
        handle_record_menu(peer_id, user_id)
        return

    count = count_booked(slot_key)
    if count >= MAX_PER_SLOT:
        send_auto(peer_id, "⚠ Занятие заполнено! Выберите другое.")
        time.sleep(1)
        handle_record_menu(peer_id, user_id)
        return

    now_str = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    db_execute(
        "INSERT INTO bookings (user_id, slot, timestamp) VALUES (?, ?, ?)",
        (user_id, slot_key, now_str)
    )

    display = parse_slot_display(slot_key)
    msg = f"✅ Вы записаны на {display}\n📅 Дата записи: {now_str}"
    send_auto(peer_id, msg)

    time.sleep(1.5)
    handle_record_menu(peer_id, user_id)


def handle_feedback_input(peer_id, user_id):
    text = "💬 Напишите ваше мнение/предложение.\nИли нажмите «Назад»."
    kb = make_back_kb()
    send_auto(peer_id, text, kb)
    set_state(user_id, 'wait_feedback', {'peer_id': peer_id})


def handle_suggest_theme_select(peer_id, user_id):
    bookings = get_user_future_bookings(user_id)
    if not bookings:
        send_auto(peer_id, "⚠ У вас нет записей.")
        time.sleep(1)
        handle_my_records(peer_id, user_id)
        return

    if len(bookings) == 1:
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
    bookings = get_user_future_bookings(user_id)
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
    slots = get_next_n_slots(SLOTS_TO_SHOW)
    count = len(slots)
    text = f"📊 Ближайшие {count} занятий.\nВыберите слот:"
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
        text += "👥 Записей нет.\n"
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
        selected_slot = None

        # По тексту кнопки
        for i, b in enumerate(bookings):
            display = parse_slot_display(b['slot'])
            label = f"{i + 1}. {display}"
            if text.startswith(f"{i + 1}.") or text == label[:40]:
                selected_slot = b['slot']
                break

        # По номеру
        if not selected_slot:
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
            send_auto(peer_id,
                       f"📝 Тема сохранена для {display}!\nПродолжайте или «🛑 Стоп».",
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
        slot_key = data.get('slot_key', '')
        # Возвращаемся к просмотру слота
        handle_admin_slot_view(peer_id, user_id, f"A:x {slot_key}")
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
    print(f"Week parity: {'even' if is_even_week() else 'odd'}")
    print(f"Admins: {ADMINS}")

    upcoming = get_next_n_slots(SLOTS_TO_SHOW)
    print(f"Next {SLOTS_TO_SHOW} slots:")
    for s in upcoming:
        print(f"  {s['day']} {s['date']} {s['time']} ({s['parity']})")

    print("=" * 50)

    init_db()
    init_vk()

    lp_thread = threading.Thread(target=longpoll_worker, daemon=True)
    lp_thread.start()

    port = int(os.environ.get('PORT', 5000))
    print(f"[FLASK] Starting on port {port}")
    app.run(host='0.0.0.0', port=port)
    app.run(host='0.0.0.0', port=port)

