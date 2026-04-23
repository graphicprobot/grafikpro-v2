from http.server import BaseHTTPRequestHandler
import json
import os
import requests
import traceback
from datetime import datetime, timedelta
import uuid
import firebase_admin
from firebase_admin import credentials, firestore

TOKEN = "8269135710:AAE9mv55_QJOg3VN6U7JploC6KqigKBZf6Y"
TELEGRAM_URL = f"https://api.telegram.org/bot{TOKEN}"

# === FIREBASE ===
cred = credentials.Certificate("firebase_key.json")
firebase_admin.initialize_app(cred)
db = firestore.client()

# === API Telegram ===
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

# === ВРЕМЕННЫЕ СОСТОЯНИЯ ===
STATES = {}

# === ОСНОВНЫЕ ФУНКЦИИ ===
def handle_start(chat_id, user_name):
    master_ref = db.collection("masters").document(str(chat_id))
    master = master_ref.get()
    
    if master.exists:
        send_message(chat_id, f"С возвращением, мастер {user_name}!", reply_markup=master_menu())
    else:
        keyboard = {
            "keyboard": [["👤 Я мастер", "👥 Я клиент"]],
            "resize_keyboard": True
        }
        send_message(chat_id, "👋 Добро пожаловать в *График.Про*!\n\nЯ помогу записывать клиентов и не терять деньги.\n\n*Кто вы?*", reply_markup=keyboard)

def handle_master_registration(chat_id, user_name, username):
    master_ref = db.collection("masters").document(str(chat_id))
    if not master_ref.get().exists:
        master_ref.set({
            "name": user_name,
            "username": username,
            "services": [],
            "created_at": datetime.now().isoformat()
        })
    send_message(chat_id, "✅ Вы зарегистрированы как мастер!\n🔗 Нажмите *«Моя ссылка»* в меню.", reply_markup=master_menu())

def handle_master_link(chat_id):
    master_ref = db.collection("masters").document(str(chat_id))
    master = master_ref.get()
    if not master.exists:
        send_message(chat_id, "Сначала зарегистрируйтесь как мастер.")
        return
    
    links_ref = db.collection("links").where("master_id", "==", str(chat_id)).limit(1)
    links = links_ref.get()
    
    if len(links) > 0:
        link_id = links[0].id
    else:
        link_id = str(uuid.uuid4())[:8]
        db.collection("links").document(link_id).set({
            "master_id": str(chat_id),
            "created_at": datetime.now().isoformat()
        })
    
    bot_username = "grafikpro_bot"
    deeplink = f"https://t.me/{bot_username}?start=master_{link_id}"
    send_message(chat_id, f"🔗 *Ваша ссылка для клиентов:*\n\n`{deeplink}`\n\n📨 Отправьте её клиенту.", parse_mode="Markdown")

def handle_client_start_from_link(chat_id, link_id):
    link_ref = db.collection("links").document(link_id)
    link = link_ref.get()
    if not link.exists:
        send_message(chat_id, "❌ Ссылка недействительна.")
        return
    
    master_id = link.get("master_id")
    master_ref = db.collection("masters").document(master_id)
    master = master_ref.get()
    if not master.exists:
        send_message(chat_id, "❌ Мастер не найден.")
        return
    
    services = master.get("services", [])
    if not services:
        send_message(chat_id, "❌ У мастера пока нет услуг.")
        return
    
    text = f"📝 *Запись к {master.get('name')}*\n\nВыберите услугу:"
    buttons = [[{"text": s, "callback_data": f"client_service_{link_id}_{s}"}] for s in services]
    send_message(chat_id, text, reply_markup={"inline_keyboard": buttons})

def handle_client_service_select(chat_id, link_id, service_name):
    tomorrow = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d')
    time_slots = ["09:00", "11:00", "13:00", "15:00", "17:00"]
    
    text = f"📅 *Выберите время:*\nУслуга: *{service_name}*\nДата: *{tomorrow}*"
    buttons = [[{"text": t, "callback_data": f"client_time_{link_id}_{service_name}_{tomorrow}_{t}"}] for t in time_slots]
    send_message(chat_id, text, reply_markup={"inline_keyboard": buttons})

def handle_client_time_select(chat_id, link_id, service_name, date, time):
    link_ref = db.collection("links").document(link_id)
    link = link_ref.get()
    master_id = link.get("master_id")
    master = db.collection("masters").document(master_id).get()
    master_name = master.get("name", "Мастер")
    
    db.collection("appointments").add({
        "master_id": master_id,
        "client_id": str(chat_id),
        "service": service_name,
        "date": date,
        "time": time,
        "status": "confirmed",
        "created_at": datetime.now().isoformat()
    })
    
    send_message(chat_id, f"✅ *Запись подтверждена!*\n\nМастер: *{master_name}*\nУслуга: *{service_name}*\nДата: *{date}*\nВремя: *{time}*\n\n📌 Мы напомним вам за час до записи.")
    send_message(int(master_id), f"🔔 *Новая запись!*\n\nКлиент записался на *{service_name}*\nДата: *{date}* в *{time}*")

def handle_settings_services(chat_id):
    master = db.collection("masters").document(str(chat_id)).get()
    if not master.exists:
        return
    services = master.get("services", [])
    text = "💈 *Ваши услуги:*\nНажмите на услугу, чтобы удалить." if services else "💈 У вас пока нет услуг."
    send_message(chat_id, text, reply_markup=services_inline(services))

def handle_add_service_prompt(chat_id):
    STATES[str(chat_id)] = {"state": "adding_service"}
    keyboard = {"keyboard": [["🔙 Отмена"]], "resize_keyboard": True}
    send_message(chat_id, "✏️ Введите название услуги:", reply_markup=keyboard)

def handle_add_service_name(chat_id, service_name):
    master_ref = db.collection("masters").document(str(chat_id))
    master = master_ref.get()
    if master.exists:
        services = master.get("services", [])
        services.append(service_name)
        master_ref.update({"services": services})
    
    STATES.pop(str(chat_id), None)
    send_message(chat_id, f"✅ Услуга *«{service_name}»* добавлена!", reply_markup=settings_menu())
    handle_settings_services(chat_id)

def handle_delete_service(chat_id, service_name):
    master_ref = db.collection("masters").document(str(chat_id))
    master = master_ref.get()
    if master.exists:
        services = master.get("services", [])
        services = [s for s in services if s != service_name]
        master_ref.update({"services": services})
    handle_settings_services(chat_id)

def handle_text(chat_id, user_name, username, text):
    state = STATES.get(str(chat_id), {}).get("state")
    
    if state == "adding_service":
        if text == "🔙 Отмена":
            STATES.pop(str(chat_id), None)
            send_message(chat_id, "❌ Отменено.", reply_markup=settings_menu())
        else:
            handle_add_service_name(chat_id, text)
        return
    
    if text == "👤 Я мастер":
        handle_master_registration(chat_id, user_name, username)
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

def handle_callback(chat_id, data):
    if data == "add_service":
        handle_add_service_prompt(chat_id)
    elif data.startswith("del_service_"):
        handle_delete_service(chat_id, data.replace("del_service_", "", 1))
    elif data == "back_to_settings":
        send_message(chat_id, "⚙️ Настройки", reply_markup=settings_menu())
    elif data.startswith("client_service_"):
        parts = data.replace("client_service_", "").split("_", 1)
        link_id, service_name = parts[0], parts[1]
        handle_client_service_select(chat_id, link_id, service_name)
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
        handle_callback(chat_id, data)

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