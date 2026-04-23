from http.server import BaseHTTPRequestHandler
import json
import os
import requests
import traceback
from datetime import datetime, timedelta
import uuid

TOKEN = "8269135710:AAE9mv55_QJOg3VN6U7JploC6KqigKBZf6Y"
TELEGRAM_URL = f"https://api.telegram.org/bot{TOKEN}"

DB_FILE = "/tmp/database.json"
STATE_FILE = "/tmp/user_states.json"

def load_db():
    if os.path.exists(DB_FILE):
        with open(DB_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"masters": {}, "appointments": [], "links": {}}

def save_db(data):
    with open(DB_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def load_states():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}

def save_states(data):
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def send_message(chat_id, text, reply_markup=None, parse_mode="Markdown"):
    payload = {"chat_id": chat_id, "text": text}
    if reply_markup:
        payload["reply_markup"] = json.dumps(reply_markup)
    if parse_mode:
        payload["parse_mode"] = parse_mode
    requests.post(f"{TELEGRAM_URL}/sendMessage", json=payload)

# === КЛАВИАТУРЫ ===
def master_menu():
    return {
        "keyboard": [
            ["📅 Моё расписание", "➕ Новая запись"],
            ["👥 Клиенты", "📊 Статистика"],
            ["🔗 Моя ссылка", "⚙️ Настройки"]
        ],
        "resize_keyboard": True
    }

def settings_menu():
    return {
        "keyboard": [
            ["💈 Мои услуги", "⏰ Рабочие часы"],
            ["🔙 Назад в меню"]
        ],
        "resize_keyboard": True
    }

def services_inline(services):
    buttons = []
    for s in services:
        buttons.append([{"text": f"❌ {s}", "callback_data": f"del_service_{s}"}])
    buttons.append([{"text": "➕ Добавить услугу", "callback_data": "add_service"}])
    buttons.append([{"text": "🔙 Назад", "callback_data": "back_to_settings"}])
    return {"inline_keyboard": buttons}

# === ОСНОВНЫЕ ФУНКЦИИ ===
def handle_start(chat_id, user_name):
    db = load_db()
    if str(chat_id) in db["masters"]:
        send_message(chat_id, f"С возвращением, мастер {user_name}!", reply_markup=master_menu())
    else:
        keyboard = {
            "keyboard": [["👤 Я мастер", "👥 Я клиент"]],
            "resize_keyboard": True
        }
        send_message(chat_id, "👋 Добро пожаловать в *График.Про*!\n\nЯ помогу записывать клиентов и не терять деньги.\n\n*Кто вы?*", reply_markup=keyboard)

def handle_master_registration(chat_id, user_name, username):
    db = load_db()
    db["masters"][str(chat_id)] = {
        "name": user_name,
        "username": username,
        "services": [],
        "registered_at": datetime.now().isoformat()
    }
    save_db(db)
    send_message(chat_id, "✅ Вы зарегистрированы как мастер!\n🔗 Теперь вам доступна персональная ссылка для клиентов.\nНажмите *«Моя ссылка»* в меню.", reply_markup=master_menu())

def handle_master_link(chat_id):
    db = load_db()
    master = db["masters"].get(str(chat_id))
    if not master:
        send_message(chat_id, "Сначала зарегистрируйтесь как мастер.")
        return

    # Проверяем, есть ли уже ссылка
    existing_link = None
    for link_id, link_data in db.get("links", {}).items():
        if link_data.get("master_id") == str(chat_id):
            existing_link = link_id
            break

    if not existing_link:
        existing_link = str(uuid.uuid4())[:8]
        if "links" not in db:
            db["links"] = {}
        db["links"][existing_link] = {"master_id": str(chat_id), "created_at": datetime.now().isoformat()}
        save_db(db)

    bot_username = "grafikpro_bot"
    deeplink = f"https://t.me/{bot_username}?start=master_{existing_link}"
    
    send_message(chat_id, f"🔗 *Ваша ссылка для клиентов:*\n\n`{deeplink}`\n\n📨 Отправьте её клиенту. Он перейдет по ней и сможет записаться.", parse_mode="Markdown")

def handle_client_start_from_link(chat_id, link_id):
    db = load_db()
    link_data = db.get("links", {}).get(link_id)
    if not link_data:
        send_message(chat_id, "❌ Ссылка недействительна.")
        return

    master_id = link_data["master_id"]
    master = db["masters"].get(master_id)
    if not master:
        send_message(chat_id, "❌ Мастер больше не принимает записи.")
        return

    services = master.get("services", [])
    if not services:
        send_message(chat_id, "❌ У мастера пока нет услуг. Попробуйте позже.")
        return

    text = f"📝 *Запись к мастеру {master['name']}*\n\nВыберите услугу:"
    buttons = []
    for s in services:
        buttons.append([{"text": s, "callback_data": f"client_service_{link_id}_{s}"}])
    
    send_message(chat_id, text, reply_markup={"inline_keyboard": buttons})

def handle_client_service_select(chat_id, link_id, service_name, message_id):
    db = load_db()
    link_data = db.get("links", {}).get(link_id)
    if not link_data:
        return
    
    # Предлагаем время (упрощенно, на завтра)
    tomorrow = datetime.now() + timedelta(days=1)
    time_slots = ["09:00", "11:00", "13:00", "15:00", "17:00"]
    
    text = f"📅 *Выберите время:*\nУслуга: *{service_name}*\nДата: *{tomorrow.strftime('%d.%m.%Y')}*"
    buttons = []
    for t in time_slots:
        buttons.append([{"text": t, "callback_data": f"client_time_{link_id}_{service_name}_{tomorrow.strftime('%Y-%m-%d')}_{t}"}])
    
    send_message(chat_id, text, reply_markup={"inline_keyboard": buttons})

def handle_client_time_select(chat_id, link_id, service_name, date, time):
    db = load_db()
    link_data = db.get("links", {}).get(link_id)
    if not link_data:
        return
    
    master_id = link_data["master_id"]
    master = db["masters"].get(master_id)
    
    # Создаем запись
    appointment = {
        "id": str(uuid.uuid4())[:8],
        "master_id": master_id,
        "client_id": str(chat_id),
        "service": service_name,
        "date": date,
        "time": time,
        "status": "confirmed",
        "created_at": datetime.now().isoformat()
    }
    
    if "appointments" not in db:
        db["appointments"] = []
    db["appointments"].append(appointment)
    save_db(db)
    
    # Уведомляем клиента
    send_message(chat_id, f"✅ *Запись подтверждена!*\n\nМастер: *{master['name']}*\nУслуга: *{service_name}*\nДата: *{date}*\nВремя: *{time}*\n\n📌 Мы напомним вам за час до записи.")
    
    # Уведомляем мастера
    send_message(int(master_id), f"🔔 *Новая запись!*\n\nКлиент записался на услугу *{service_name}*\nДата: *{date}* в *{time}*")

def handle_settings_services(chat_id):
    db = load_db()
    master = db["masters"].get(str(chat_id))
    if not master:
        return
    
    services = master.get("services", [])
    if services:
        text = "💈 *Ваши услуги:*\nНажмите на услугу, чтобы удалить."
    else:
        text = "💈 У вас пока нет услуг."
    send_message(chat_id, text, reply_markup=services_inline(services))

def handle_add_service_prompt(chat_id):
    states = load_states()
    states[str(chat_id)] = {"state": "adding_service"}
    save_states(states)
    keyboard = {"keyboard": [["🔙 Отмена"]], "resize_keyboard": True}
    send_message(chat_id, "✏️ Введите название услуги:", reply_markup=keyboard)

def handle_add_service_name(chat_id, service_name):
    db = load_db()
    master = db["masters"].get(str(chat_id))
    if master:
        master.setdefault("services", []).append(service_name)
        save_db(db)
    
    states = load_states()
    states.pop(str(chat_id), None)
    save_states(states)
    
    send_message(chat_id, f"✅ Услуга *«{service_name}»* добавлена!", reply_markup=settings_menu())
    handle_settings_services(chat_id)

def handle_delete_service(chat_id, service_name):
    db = load_db()
    master = db["masters"].get(str(chat_id))
    if master:
        master["services"] = [s for s in master.get("services", []) if s != service_name]
        save_db(db)
    handle_settings_services(chat_id)

def handle_text(chat_id, user_name, username, text):
    db = load_db()
    states = load_states()
    
    user_state = states.get(str(chat_id), {}).get("state")
    if user_state == "adding_service":
        if text == "🔙 Отмена":
            states.pop(str(chat_id), None)
            save_states(states)
            send_message(chat_id, "❌ Отменено.", reply_markup=settings_menu())
        else:
            handle_add_service_name(chat_id, text)
        return
    
    if text == "👤 Я мастер":
        handle_master_registration(chat_id, user_name, username) if str(chat_id) not in db.get("masters",{}) else send_message(chat_id, "Уже зарегистрированы!", reply_markup=master_menu())
    elif text == "👥 Я клиент":
        send_message(chat_id, "Используйте ссылку от мастера для записи.")
    elif text == "⚙️ Настройки":
        send_message(chat_id, "⚙️ *Настройки*", reply_markup=settings_menu())
    elif text == "💈 Мои услуги":
        handle_settings_services(chat_id)
    elif text == "🔙 Назад в меню":
        send_message(chat_id, "Главное меню", reply_markup=master_menu())
    elif text == "🔗 Моя ссылка":
        handle_master_link(chat_id)
    elif text == "📅 Моё расписание":
        send_message(chat_id, "📭 Записей пока нет.")
    elif text == "👥 Клиенты":
        send_message(chat_id, "👥 Пока пусто.")
    elif text == "📊 Статистика":
        send_message(chat_id, "📊 Ждем первых записей.")
    else:
        send_message(chat_id, "Используйте меню.", reply_markup=master_menu())

def handle_callback(chat_id, data, message_id=None):
    if data == "add_service":
        handle_add_service_prompt(chat_id)
    elif data.startswith("del_service_"):
        handle_delete_service(chat_id, data.replace("del_service_", "", 1))
    elif data == "back_to_settings":
        send_message(chat_id, "⚙️ Настройки", reply_markup=settings_menu())
    elif data.startswith("client_service_"):
        parts = data.replace("client_service_", "").split("_", 1)
        link_id, service_name = parts[0], parts[1]
        handle_client_service_select(chat_id, link_id, service_name, message_id)
    elif data.startswith("client_time_"):
        parts = data.replace("client_time_", "").split("_", 3)
        link_id, service_name, date, time = parts
        handle_client_time_select(chat_id, link_id, service_name, date, time)

def process_update(update):
    if "message" in update:
        msg = update["message"]
        chat_id = msg["chat"]["id"]
        user_name = msg["from"].get("first_name", "Пользователь")
        username = msg["from"].get("username", "")
        
        if "text" in msg:
            text = msg["text"]
            if text.startswith("/start"):
                if "master_" in text:
                    link_id = text.split("master_")[1]
                    handle_client_start_from_link(chat_id, link_id)
                else:
                    handle_start(chat_id, user_name)
            else:
                handle_text(chat_id, user_name, username, text)
    
    elif "callback_query" in update:
        cb = update["callback_query"]
        chat_id = cb["message"]["chat"]["id"]
        data = cb["data"]
        message_id = cb["message"]["message_id"]
        handle_callback(chat_id, data, message_id)

class handler(BaseHTTPRequestHandler):
    def do_POST(self):
        content_length = int(self.headers.get('Content-Length', 0))
        post_data = self.rfile.read(content_length) if content_length else b''
        try:
            update = json.loads(post_data.decode('utf-8'))
            process_update(update)
        except Exception as e:
            print(f"Error: {e}\n{traceback.format_exc()}")
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps({"status": "ok"}).encode())
    
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps({"status": "bot online"}).encode())