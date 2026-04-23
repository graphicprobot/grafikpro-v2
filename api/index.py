from http.server import BaseHTTPRequestHandler
import json
import os
import requests
import traceback
from datetime import datetime, timedelta
import uuid

TOKEN = "8269135710:AAE9mv55_QJOg3VN6U7JploC6KqigKBZf6Y"
TELEGRAM_URL = f"https://api.telegram.org/bot{TOKEN}"

# === FIRESTORE REST API ===
FIRESTORE_URL = "https://firestore.googleapis.com/v1/projects/grafikpro-d3500/databases/(default)/documents"
API_KEY = "AIzaSyAmP4IW-mcqhXT1L6s4vx5_Z7IZbi1YqI8"

def firestore_get(collection, doc_id):
    url = f"{FIRESTORE_URL}/{collection}/{doc_id}?key={API_KEY}"
    r = requests.get(url)
    if r.status_code == 200:
        data = r.json()
        fields = data.get("fields", {})
        result = {}
        for key, value in fields.items():
            if "stringValue" in value:
                result[key] = value["stringValue"]
            elif "arrayValue" in value:
                vals = value["arrayValue"].get("values", [])
                if vals:
                    result[key] = [v.get("stringValue", str(v.get("integerValue", ""))) for v in vals]
                else:
                    result[key] = []
            elif "integerValue" in value:
                result[key] = int(value["integerValue"])
            elif "booleanValue" in value:
                result[key] = value["booleanValue"]
        return result
    return None

def firestore_set(collection, doc_id, data):
    url = f"{FIRESTORE_URL}/{collection}/{doc_id}?key={API_KEY}"
    fields = {}
    for key, val in data.items():
        if isinstance(val, str):
            fields[key] = {"stringValue": val}
        elif isinstance(val, list):
            fields[key] = {"arrayValue": {"values": [{"stringValue": str(item)} for item in val]}}
        elif isinstance(val, bool):
            fields[key] = {"booleanValue": val}
        elif isinstance(val, int):
            fields[key] = {"integerValue": str(val)}
    body = {"fields": fields}
    r = requests.patch(url, json=body)
    return r.status_code in [200, 201]

def firestore_update(collection, doc_id, data):
    return firestore_set(collection, doc_id, data)

def firestore_query(collection, field, operator, value):
    url = f"{FIRESTORE_URL}:runQuery?key={API_KEY}"
    body = {
        "structuredQuery": {
            "from": [{"collectionId": collection}],
            "where": {
                "fieldFilter": {
                    "field": {"fieldPath": field},
                    "op": operator,
                    "value": {"stringValue": str(value)}
                }
            }
        }
    }
    r = requests.post(url, json=body)
    results = []
    if r.status_code == 200:
        docs = r.json()
        if isinstance(docs, list):
            for doc in docs:
                if "document" in doc:
                    doc_data = {}
                    fields = doc["document"].get("fields", {})
                    for key, val in fields.items():
                        if "stringValue" in val:
                            doc_data[key] = val["stringValue"]
                        elif "arrayValue" in val:
                            vals = val["arrayValue"].get("values", [])
                            doc_data[key] = [v.get("stringValue", "") for v in vals]
                    doc_name = doc["document"].get("name", "")
                    doc_id = doc_name.split("/")[-1] if doc_name else ""
                    doc_data["_id"] = doc_id
                    results.append(doc_data)
    return results

def firestore_add(collection, data):
    url = f"{FIRESTORE_URL}/{collection}?key={API_KEY}"
    fields = {}
    for key, val in data.items():
        if isinstance(val, str):
            fields[key] = {"stringValue": val}
        elif isinstance(val, list):
            fields[key] = {"arrayValue": {"values": [{"stringValue": str(item)} for item in val]}}
        elif isinstance(val, bool):
            fields[key] = {"booleanValue": val}
        elif isinstance(val, int):
            fields[key] = {"integerValue": str(val)}
    body = {"fields": fields}
    r = requests.post(url, json=body)
    return r.status_code in [200, 201]

# === API Telegram ===
def send_message(chat_id, text, reply_markup=None, parse_mode=None):
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
    master = firestore_get("masters", str(chat_id))
    
    if master:
        send_message(chat_id, f"С возвращением, мастер {user_name}!", reply_markup=master_menu())
    else:
        keyboard = {
            "keyboard": [["👤 Я мастер", "👥 Я клиент"]],
            "resize_keyboard": True
        }
        send_message(chat_id, "👋 Добро пожаловать в *График.Про*!\n\nКто вы?", reply_markup=keyboard)

def handle_master_registration(chat_id, user_name, username):
    master = firestore_get("masters", str(chat_id))
    if not master:
        firestore_set("masters", str(chat_id), {
            "name": user_name,
            "username": username,
            "services": [],
            "created_at": datetime.now().isoformat()
        })
    send_message(chat_id, "✅ Вы зарегистрированы как мастер!", reply_markup=master_menu())

def handle_master_link(chat_id):
    master = firestore_get("masters", str(chat_id))
    if not master:
        send_message(chat_id, "Сначала зарегистрируйтесь как мастер.")
        return
    
    links = firestore_query("links", "master_id", "EQUAL", str(chat_id))
    
    if links:
        link_id = links[0]["_id"]
    else:
        link_id = str(uuid.uuid4())[:8]
        firestore_set("links", link_id, {
            "master_id": str(chat_id),
            "created_at": datetime.now().isoformat()
        })
    
    deeplink = f"https://t.me/grafikpro_bot?start=master_{link_id}"
    send_message(chat_id, f"🔗 *Ваша ссылка:*\n\n`{deeplink}`", parse_mode="Markdown")

def handle_client_start_from_link(chat_id, link_id):
    link = firestore_get("links", link_id)
    if not link:
        send_message(chat_id, "❌ Ссылка недействительна.")
        return
    
    master_id = link.get("master_id")
    master = firestore_get("masters", master_id)
    if not master:
        send_message(chat_id, "❌ Мастер не найден.")
        return
    
    services = master.get("services", [])
    if not services:
        send_message(chat_id, "❌ У мастера пока нет услуг.")
        return
    
    text = f"📝 *Запись к {master.get('name')}*\nВыберите услугу:"
    buttons = [[{"text": s, "callback_data": f"client_service_{link_id}_{s}"}] for s in services]
    send_message(chat_id, text, reply_markup={"inline_keyboard": buttons})

def handle_client_service_select(chat_id, link_id, service_name):
    tomorrow = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d')
    time_slots = ["09:00", "11:00", "13:00", "15:00", "17:00"]
    
    text = f"📅 *Выберите время:*\n{service_name} | {tomorrow}"
    buttons = [[{"text": t, "callback_data": f"client_time_{link_id}_{service_name}_{tomorrow}_{t}"}] for t in time_slots]
    send_message(chat_id, text, reply_markup={"inline_keyboard": buttons})

def handle_client_time_select(chat_id, link_id, service_name, date, time):
    link = firestore_get("links", link_id)
    master_id = link.get("master_id")
    master = firestore_get("masters", master_id)
    master_name = master.get("name", "Мастер") if master else "Мастер"
    
    firestore_add("appointments", {
        "master_id": master_id,
        "client_id": str(chat_id),
        "service": service_name,
        "date": date,
        "time": time,
        "status": "confirmed",
        "created_at": datetime.now().isoformat()
    })
    
    send_message(chat_id, f"✅ *Запись подтверждена!*\n\n{master_name}\n{service_name}\n{date} в {time}")
    send_message(int(master_id), f"🔔 *Новая запись!*\n\n{service_name}\n{date} в {time}")

def handle_settings_services(chat_id):
    master = firestore_get("masters", str(chat_id))
    if not master:
        send_message(chat_id, "Сначала зарегистрируйтесь как мастер.")
        return
    services = master.get("services", [])
    if services:
        text = "💈 *Ваши услуги:*\nНажмите на услугу, чтобы удалить."
    else:
        text = "💈 *У вас пока нет услуг.*\nНажмите «Добавить услугу»."
    send_message(chat_id, text, reply_markup=services_inline(services))

def handle_add_service_prompt(chat_id):
    STATES[str(chat_id)] = {"state": "adding_service"}
    keyboard = {"keyboard": [["🔙 Отмена"]], "resize_keyboard": True}
    send_message(chat_id, "✏️ Введите название услуги:", reply_markup=keyboard)

def handle_add_service_name(chat_id, service_name):
    master = firestore_get("masters", str(chat_id))
    if master:
        services = master.get("services", [])
        services.append(service_name)
        firestore_update("masters", str(chat_id), {"services": services})
    
    STATES.pop(str(chat_id), None)
    send_message(chat_id, f"✅ *«{service_name}»* добавлена!", reply_markup=settings_menu())
    handle_settings_services(chat_id)

def handle_delete_service(chat_id, service_name):
    master = firestore_get("masters", str(chat_id))
    if master:
        services = master.get("services", [])
        services = [s for s in services if s != service_name]
        firestore_update("masters", str(chat_id), {"services": services})
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
    elif text == "⏰ Рабочие часы":
        send_message(chat_id, "⏰ Настройка рабочего времени появится в следующем обновлении.")
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