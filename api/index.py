from http.server import BaseHTTPRequestHandler
import json
import requests
import traceback
from datetime import datetime, timedelta
import uuid
import re
import threading
import time
import os

# Конфигурация
TOKEN = os.environ.get("TELEGRAM_TOKEN", "8269135710:AAE9mv55_QJOg3VN6U7JploC6KqigKBZf6Y")
API_KEY = os.environ.get("FIREBASE_API_KEY", "AIzaSyAmP4IW-mcqhXT1L6s4vx5_Z7IZbi1YqI8")

TELEGRAM_URL = f"https://api.telegram.org/bot{TOKEN}"
FIRESTORE_URL = "https://firestore.googleapis.com/v1/projects/grafikpro-d3500/databases/(default)/documents"

# Хранилище состояний
STATES = {}

# === FIRESTORE ===
def firestore_get(collection, doc_id):
    try:
        r = requests.get(f"{FIRESTORE_URL}/{collection}/{doc_id}?key={API_KEY}", timeout=10)
        if r.status_code != 200:
            return None
        fields = r.json().get("fields", {})
        result = {}
        for key, value in fields.items():
            if "stringValue" in value:
                result[key] = value["stringValue"]
            elif "integerValue" in value:
                result[key] = int(value["integerValue"])
            elif "booleanValue" in value:
                result[key] = value["booleanValue"]
            elif "arrayValue" in value:
                arr = []
                for v in value["arrayValue"].get("values", []):
                    if "stringValue" in v:
                        arr.append(v["stringValue"])
                    elif "mapValue" in v:
                        inner = v["mapValue"].get("fields", {})
                        item = {}
                        for k, iv in inner.items():
                            if "stringValue" in iv:
                                item[k] = iv["stringValue"]
                            elif "integerValue" in iv:
                                item[k] = int(iv["integerValue"])
                        arr.append(item)
                result[key] = arr
            elif "mapValue" in value:
                inner = {}
                for k, iv in value["mapValue"].get("fields", {}).items():
                    if "stringValue" in iv:
                        inner[k] = iv["stringValue"]
                    elif "integerValue" in iv:
                        inner[k] = int(iv["integerValue"])
                result[key] = inner
        return result
    except:
        return None

def firestore_set(collection, doc_id, data):
    try:
        fields = {}
        for key, val in data.items():
            if isinstance(val, str):
                fields[key] = {"stringValue": val}
            elif isinstance(val, int):
                fields[key] = {"integerValue": str(val)}
            elif isinstance(val, bool):
                fields[key] = {"booleanValue": val}
            elif isinstance(val, list):
                items = []
                for item in val:
                    if isinstance(item, str):
                        items.append({"stringValue": item})
                    elif isinstance(item, dict):
                        map_fields = {}
                        for k, v in item.items():
                            if isinstance(v, str):
                                map_fields[k] = {"stringValue": v}
                            elif isinstance(v, int):
                                map_fields[k] = {"integerValue": str(v)}
                        items.append({"mapValue": {"fields": map_fields}})
                fields[key] = {"arrayValue": {"values": items}}
            elif isinstance(val, dict):
                map_fields = {}
                for k, v in val.items():
                    if isinstance(v, str):
                        map_fields[k] = {"stringValue": v}
                    elif isinstance(v, int):
                        map_fields[k] = {"integerValue": str(v)}
                fields[key] = {"mapValue": {"fields": map_fields}}
        
        body = {"fields": fields}
        url = f"{FIRESTORE_URL}/{collection}/{doc_id}?key={API_KEY}"
        r = requests.patch(url, json=body, timeout=10)
        
        if r.status_code in [200, 201]:
            return True
        
        create_url = f"{FIRESTORE_URL}/{collection}?documentId={doc_id}&key={API_KEY}"
        r2 = requests.post(create_url, json=body, timeout=10)
        return r2.status_code in [200, 201]
    except:
        return False

def firestore_add(collection, data):
    try:
        doc_id = str(uuid.uuid4())[:12]
        fields = {}
        for key, val in data.items():
            if isinstance(val, str):
                fields[key] = {"stringValue": val}
            elif isinstance(val, int):
                fields[key] = {"integerValue": str(val)}
        
        body = {"fields": fields}
        url = f"{FIRESTORE_URL}/{collection}?documentId={doc_id}&key={API_KEY}"
        r = requests.post(url, json=body, timeout=10)
        
        if r.status_code in [200, 201]:
            return doc_id
        return None
    except:
        return None

def firestore_query(collection, field, operator, value):
    try:
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
        
        r = requests.post(f"{FIRESTORE_URL}:runQuery?key={API_KEY}", json=body, timeout=10)
        results = []
        
        if r.status_code == 200:
            for doc in r.json():
                if "document" in doc:
                    fields = doc["document"].get("fields", {})
                    data = {}
                    for key, val in fields.items():
                        if "stringValue" in val:
                            data[key] = val["stringValue"]
                        elif "integerValue" in val:
                            data[key] = int(val["integerValue"])
                    data["_id"] = doc["document"]["name"].split("/")[-1]
                    results.append(data)
        
        return results
    except:
        return []

# === ОТПРАВКА СООБЩЕНИЙ ===
def send_message(chat_id, text, reply_markup=None, parse_mode="Markdown"):
    try:
        payload = {"chat_id": chat_id, "text": text, "parse_mode": parse_mode}
        if reply_markup:
            payload["reply_markup"] = json.dumps(reply_markup)
        requests.post(f"{TELEGRAM_URL}/sendMessage", json=payload, timeout=10)
    except:
        pass

# === МЕНЮ ===
def master_menu():
    return {
        "keyboard": [
            ["📊 Дашборд", "🗓 Календарь"],
            ["📅 Расписание", "➕ Новая запись"],
            ["👥 Клиенты", "🔗 Моя ссылка"],
            ["⚙️ Настройки", "❓ Помощь"]
        ],
        "resize_keyboard": True
    }

def client_menu():
    return {
        "keyboard": [
            ["📋 Мои записи"],
            ["🔍 Найти мастера"],
            ["❓ Помощь"]
        ],
        "resize_keyboard": True
    }

def settings_menu():
    return {
        "keyboard": [
            ["💈 Услуги", "⏰ Часы"],
            ["📍 Адрес", "🖼 Портфолио"],
            ["🚷 Чёрный список", "🔙 В меню"]
        ],
        "resize_keyboard": True
    }

# === СТАРТ ===
def handle_start(chat_id, user_name):
    master = firestore_get("masters", str(chat_id))
    client = firestore_get("clients", str(chat_id))
    
    if master:
        send_message(chat_id, f"👋 С возвращением, {user_name}!", reply_markup=master_menu())
    elif client:
        send_message(chat_id, f"👋 {user_name}!", reply_markup=client_menu())
    else:
        send_message(
            chat_id,
            "👋 *Добро пожаловать в График.Про!*\n\nКто вы?",
            reply_markup={"keyboard": [["👤 Я мастер"], ["👥 Я клиент"]], "resize_keyboard": True}
        )

def handle_master_registration(chat_id, user_name, username):
    firestore_set("masters", str(chat_id), {
        "name": user_name,
        "username": username,
        "phone": "",
        "services": [],
        "schedule": {
            "monday": {"start": "09:00", "end": "18:00"},
            "tuesday": {"start": "09:00", "end": "18:00"},
            "wednesday": {"start": "09:00", "end": "18:00"},
            "thursday": {"start": "09:00", "end": "18:00"},
            "friday": {"start": "09:00", "end": "18:00"},
            "saturday": {"start": "10:00", "end": "15:00"},
            "sunday": None
        },
        "address": "",
        "completed_onboarding": True,
        "blacklist": [],
        "portfolio": [],
        "rating": 0,
        "ratings_count": 0
    })
    send_message(chat_id, f"✅ {user_name}, вы зарегистрированы как мастер!", reply_markup=master_menu())

def handle_help(chat_id, is_master):
    if is_master:
        text = (
            "📖 *Помощь мастеру*\n\n"
            "📊 Дашборд — доход и статистика\n"
            "🗓 Календарь — расписание на 30 дней\n"
            "📅 Расписание — список записей\n"
            "➕ Новая запись — создать вручную\n"
            "👥 Клиенты — база клиентов\n"
            "🔗 Моя ссылка — ссылка для записи\n"
            "⚙️ Настройки — услуги, часы, адрес"
        )
    else:
        text = (
            "📖 *Помощь клиенту*\n\n"
            "📋 Мои записи — ваши записи\n"
            "🔍 Найти мастера — поиск по номеру\n"
            "💡 Запишитесь по ссылке мастера"
        )
    
    send_message(chat_id, text)

# === ОБРАБОТЧИК ТЕКСТА ===
def handle_text(chat_id, user_name, username, text):
    state = STATES.get(str(chat_id), {}).get("state")
    is_master = firestore_get("masters", str(chat_id))
    is_client = firestore_get("clients", str(chat_id))
    
    # Для отладки
    print(f"DEBUG: chat_id={chat_id}, is_master={bool(is_master)}, is_client={bool(is_client)}")
    
    # Обработка состояний
    if state == "adding_service_name":
        return handle_add_service_name(chat_id, text)
    elif state == "adding_service_price":
        return handle_add_service_price(chat_id, text)
    elif state == "adding_service_duration":
        return handle_add_service_duration(chat_id, text)
    elif state == "setting_address":
        return handle_address_set(chat_id, text)
    elif state == "adding_to_blacklist":
        return handle_add_to_blacklist(chat_id, text)
    
    # Главное меню
    if text == "👤 Я мастер":
        handle_master_registration(chat_id, user_name, username)
    elif text == "👥 Я клиент":
        if not is_client:
            firestore_set("clients", str(chat_id), {"created_at": datetime.now().isoformat()})
        send_message(chat_id, "👥 Клиентский кабинет", reply_markup=client_menu())
    elif text == "📊 Дашборд" and is_master:
        handle_dashboard(chat_id)
    elif text == "🗓 Календарь" and is_master:
        send_message(chat_id, "📅 Календарь откроется в браузере")
    elif text == "📅 Расписание" and is_master:
        handle_schedule(chat_id)
    elif text == "➕ Новая запись" and is_master:
        send_message(chat_id, "➕ Функция в разработке")
    elif text == "👥 Клиенты" and is_master:
        handle_clients_list(chat_id)
    elif text == "🔗 Моя ссылка" and is_master:
        handle_master_link(chat_id)
    elif text == "⚙️ Настройки" and is_master:
        send_message(chat_id, "⚙️ *Настройки*", reply_markup=settings_menu())
    elif text == "💈 Услуги" and is_master:
        handle_services_settings(chat_id)
    elif text == "⏰ Часы" and is_master:
        send_message(chat_id, "⏰ Функция в разработке")
    elif text == "📍 Адрес" and is_master:
        STATES[str(chat_id)] = {"state": "setting_address"}
        send_message(chat_id, "📍 Введите адрес:", reply_markup={"keyboard": [["🔙 Отмена"]], "resize_keyboard": True})
    elif text == "🖼 Портфолио" and is_master:
        send_message(chat_id, "🖼 Отправьте фото для портфолио")
    elif text == "🚷 Чёрный список" and is_master:
        handle_blacklist(chat_id)
    elif text == "📋 Мои записи":
        handle_client_appointments(chat_id)
    elif text == "🔍 Найти мастера":
        send_message(chat_id, "🔍 Введите номер телефона мастера")
    elif text == "❓ Помощь":
        # ИСПРАВЛЕНО: передаём True если is_master не None
        handle_help(chat_id, bool(is_master))
    elif text == "🔙 В меню":
        STATES.pop(str(chat_id), None)
        if is_master:
            send_message(chat_id, "Главное меню", reply_markup=master_menu())
        elif is_client:
            send_message(chat_id, "Главное меню", reply_markup=client_menu())
        else:
            send_message(chat_id, "Главное меню")

# === ДАШБОРД ===
def handle_dashboard(chat_id):
    appointments = firestore_query("appointments", "master_id", "EQUAL", str(chat_id))
    today = datetime.now().strftime('%Y-%m-%d')
    today_appts = [a for a in appointments if a.get("date") == today and a.get("status") != "cancelled"]
    
    master = firestore_get("masters", str(chat_id))
    services = master.get("services", []) if master else []
    
    total = 0
    for a in today_appts:
        svc_name = a.get("service", "")
        for s in services:
            if isinstance(s, dict) and s.get("name") == svc_name:
                total += s.get("price", 0)
    
    text = f"📊 *Дашборд на сегодня*\n\n📅 Записей: {len(today_appts)}\n💰 Доход: {total}₽"
    send_message(chat_id, text)

# === РАСПИСАНИЕ ===
def handle_schedule(chat_id):
    appointments = firestore_query("appointments", "master_id", "EQUAL", str(chat_id))
    if not appointments:
        return send_message(chat_id, "📭 Записей пока нет")
    
    appointments.sort(key=lambda a: (a.get("date", ""), a.get("time", "")))
    text = "📅 *Расписание:*\n"
    
    for a in appointments[:10]:
        if a.get("status") == "cancelled":
            continue
        icon = {"confirmed": "🟡", "completed": "✅", "no_show": "❌"}.get(a.get("status"), "")
        text += f"\n{icon} {a.get('date')} {a.get('time')} — {a.get('service')}\n  {a.get('client_name', '?')} | {a.get('client_phone', '?')}"
    
    send_message(chat_id, text, reply_markup=master_menu())

# === КЛИЕНТЫ ===
def handle_clients_list(chat_id):
    appointments = firestore_query("appointments", "master_id", "EQUAL", str(chat_id))
    if not appointments:
        return send_message(chat_id, "👥 Пока нет клиентов")
    
    clients = {}
    for a in appointments:
        phone = a.get("client_phone", "")
        if phone not in clients:
            clients[phone] = {"name": a.get("client_name", "?"), "phone": phone, "count": 0}
        clients[phone]["count"] += 1
    
    text = "👥 *Клиенты:*\n"
    for phone, data in list(clients.items())[:10]:
        text += f"\n• {data['name']} | {phone}\n  Записей: {data['count']}"
    
    send_message(chat_id, text)

# === ССЫЛКА МАСТЕРА ===
def handle_master_link(chat_id):
    links = firestore_query("links", "master_id", "EQUAL", str(chat_id))
    link_id = links[0]["_id"] if links else str(uuid.uuid4())[:8]
    
    if not links:
        firestore_set("links", link_id, {"master_id": str(chat_id)})
    
    link = f"https://t.me/GrafikProBot?start=master_{link_id}"
    send_message(chat_id, f"🔗 *Ваша ссылка для записи:*\n\n`{link}`")

# === УСЛУГИ ===
def handle_services_settings(chat_id):
    master = firestore_get("masters", str(chat_id))
    if not master:
        return
    
    services = [s for s in master.get("services", []) if isinstance(s, dict) and s.get("name")]
    
    if services:
        text = "💈 *Услуги:*\n" + "\n".join([f"• {s['name']} — {s['price']}₽ ({s.get('duration', 60)}мин)" for s in services])
    else:
        text = "💈 Нет услуг"
    
    send_message(chat_id, text, reply_markup={
        "inline_keyboard": [
            [{"text": "➕ Добавить услугу", "callback_data": "addservice"}],
            [{"text": "🔙 Назад", "callback_data": "settings_back"}]
        ]
    })

def handle_add_service_name(chat_id, name):
    STATES[str(chat_id)] = {"state": "adding_service_price", "name": name.strip()}
    send_message(chat_id, f"💰 Цена для «{name}»:")

def handle_add_service_price(chat_id, price_text):
    try:
        price = int(price_text.strip())
    except:
        return send_message(chat_id, "❌ Введите число")
    
    state = STATES.get(str(chat_id), {})
    STATES[str(chat_id)] = {"state": "adding_service_duration", "name": state.get("name"), "price": price}
    send_message(chat_id, f"⏱ Длительность (минут):")

def handle_add_service_duration(chat_id, dur_text):
    try:
        duration = int(dur_text.strip())
    except:
        return send_message(chat_id, "❌ Введите число")
    
    state = STATES.get(str(chat_id), {})
    if not state:
        STATES.pop(str(chat_id), None)
        return send_message(chat_id, "❌ Сессия истекла. Начните заново", reply_markup=master_menu())
    
    # Проверяем существует ли мастер
    master = firestore_get("masters", str(chat_id))
    if not master:
        # Создаем мастера если его нет
        firestore_set("masters", str(chat_id), {
            "name": "Мастер",
            "username": "",
            "phone": "",
            "services": [],
            "schedule": {
                "monday": {"start": "09:00", "end": "18:00"},
                "tuesday": {"start": "09:00", "end": "18:00"},
                "wednesday": {"start": "09:00", "end": "18:00"},
                "thursday": {"start": "09:00", "end": "18:00"},
                "friday": {"start": "09:00", "end": "18:00"},
                "saturday": {"start": "10:00", "end": "15:00"},
                "sunday": None
            },
            "address": "",
            "completed_onboarding": True,
            "blacklist": [],
            "portfolio": [],
            "rating": 0,
            "ratings_count": 0
        })
        master = firestore_get("masters", str(chat_id))
    
    services = [s for s in master.get("services", []) if isinstance(s, dict) and s.get("name")]
    services.append({"name": state["name"], "price": state["price"], "duration": duration})
    
    firestore_set("masters", str(chat_id), {"services": services})
    STATES.pop(str(chat_id), None)
    
    send_message(chat_id, f"✅ *{state['name']}* — {state['price']}₽, {duration}мин", reply_markup=settings_menu())

# === АДРЕС ===
def handle_address_set(chat_id, text):
    firestore_set("masters", str(chat_id), {"address": text.strip()})
    STATES.pop(str(chat_id), None)
    send_message(chat_id, f"✅ Адрес сохранён: {text}", reply_markup=settings_menu())

# === ЧЁРНЫЙ СПИСОК ===
def handle_blacklist(chat_id):
    master = firestore_get("masters", str(chat_id))
    blacklist = master.get("blacklist", []) if master else []
    
    text = "🚷 *Чёрный список:*\n"
    text += "\n".join([f"• {b}" for b in blacklist]) if blacklist else "Пуст"
    
    send_message(chat_id, text, reply_markup={
        "inline_keyboard": [
            [{"text": "➕ Добавить", "callback_data": "add_to_blacklist"}],
            [{"text": "🔙 Назад", "callback_data": "settings_back"}]
        ]
    })

def handle_add_to_blacklist(chat_id, phone):
    phone_clean = re.sub(r'[^0-9+]', '', phone)
    master = firestore_get("masters", str(chat_id))
    blacklist = master.get("blacklist", []) if master else []
    
    if phone_clean not in blacklist:
        blacklist.append(phone_clean)
        firestore_set("masters", str(chat_id), {"blacklist": blacklist})
    
    STATES.pop(str(chat_id), None)
    send_message(chat_id, f"✅ {phone_clean} добавлен в чёрный список", reply_markup=settings_menu())

# === КЛИЕНТСКИЕ ЗАПИСИ ===
def handle_client_appointments(chat_id):
    appointments = firestore_query("appointments", "client_id", "EQUAL", str(chat_id))
    active = [a for a in appointments if a.get("status") != "cancelled"]
    
    if not active:
        return send_message(chat_id, "📋 У вас пока нет записей")
    
    active.sort(key=lambda a: (a.get("date", ""), a.get("time", "")))
    text = "📋 *Мои записи:*\n"
    
    for a in active:
        master = firestore_get("masters", a.get("master_id", ""))
        master_name = master.get("name", "Мастер") if master else "Мастер"
        text += f"\n• {a.get('date')} в {a.get('time')}\n  {a.get('service')} у {master_name}"
    
    send_message(chat_id, text)

# === ОБРАБОТКА CALLBACK ===
def handle_callback(chat_id, data):
    if data == "addservice":
        STATES[str(chat_id)] = {"state": "adding_service_name"}
        send_message(chat_id, "✏️ Название услуги:", reply_markup={"keyboard": [["🔙 Отмена"]], "resize_keyboard": True})
    elif data == "settings_back":
        send_message(chat_id, "⚙️ Настройки", reply_markup=settings_menu())
    elif data == "add_to_blacklist":
        STATES[str(chat_id)] = {"state": "adding_to_blacklist"}
        send_message(chat_id, "🚷 Введите номер телефона:", reply_markup={"keyboard": [["🔙 Отмена"]], "resize_keyboard": True})

# === ОБРАБОТЧИК ЗАПРОСОВ ===
class handler(BaseHTTPRequestHandler):
    def do_POST(self):
        try:
            content_length = int(self.headers.get('Content-Length', 0))
            if content_length:
                body = self.rfile.read(content_length)
                update = json.loads(body.decode('utf-8'))
                self.process_update(update)
            
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps({"status": "ok"}).encode())
        except Exception as e:
            print(f"Error: {e}")
            traceback.print_exc()
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps({"status": "error"}).encode())
    
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps({"status": "bot online"}).encode())
    
    def process_update(self, update):
        if "message" in update:
            msg = update["message"]
            chat_id = str(msg["chat"]["id"])
            user_name = msg["from"].get("first_name", "Пользователь")
            username = msg["from"].get("username", "")
            
            if "photo" in msg:
                return
            
            text = msg.get("text", "")
            
            if text.startswith("/start"):
                if "master_" in text:
                    link_id = text.split("master_")[1]
                    handle_client_booking_start(chat_id, link_id)
                else:
                    handle_start(chat_id, user_name)
            elif text == "🔙 Отмена":
                STATES.pop(chat_id, None)
                is_master = firestore_get("masters", chat_id)
                menu = master_menu() if is_master else client_menu()
                send_message(chat_id, "❌ Отменено", reply_markup=menu)
            else:
                handle_text(chat_id, user_name, username, text)
        
        elif "callback_query" in update:
            cb = update["callback_query"]
            chat_id = str(cb["message"]["chat"]["id"])
            data = cb["data"]
            handle_callback(chat_id, data)

def handle_client_booking_start(chat_id, link_id):
    if not firestore_get("clients", chat_id):
        firestore_set("clients", chat_id, {"created_at": datetime.now().isoformat()})
    
    link = firestore_get("links", link_id)
    if not link:
        return send_message(chat_id, "❌ Ссылка недействительна")
    
    master = firestore_get("masters", link["master_id"])
    if not master:
        return send_message(chat_id, "❌ Мастер не найден")
    
    services = [s for s in master.get("services", []) if isinstance(s, dict) and s.get("name")]
    if not services:
        return send_message(chat_id, "❌ Нет доступных услуг")
    
    text = f"👤 *{master.get('name', 'Мастер')}*\n\nВыберите услугу:"
    buttons = [[{"text": f"{s['name']} — {s['price']}₽", "callback_data": f"book_{link_id}_{s['name']}"}] for s in services]
    
    send_message(chat_id, text, reply_markup={"inline_keyboard": buttons})