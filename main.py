import os
import logging
import re
from enum import Enum, auto
from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup
)
from telegram.ext import (
    Updater,
    CommandHandler,
    MessageHandler,
    Filters,
    CallbackContext,
    CallbackQueryHandler,
    ConversationHandler,
)
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# ------------------ Ограничение доступа по ID ------------------
AUTHORIZED_USERS = []
_auth_str = os.getenv("AUTHORIZED_USERS", "")
if _auth_str:
    for part in _auth_str.split(","):
        part = part.strip()
        if part.isdigit():
            AUTHORIZED_USERS.append(int(part))

def check_authorized(update: Update) -> bool:
    """Проверяем, есть ли user_id в списке AUTHORIZED_USERS."""
    user_id = update.effective_user.id
    if user_id not in AUTHORIZED_USERS:
        if update.effective_message:
            update.effective_message.reply_text(
                "Извините, у вас нет прав пользоваться этим ботом."
            )
        return False
    return True

# ------------------ Google Sheets подключение ------------------
SCOPES = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.file",
    "https://www.googleapis.com/auth/drive"
]
CREDENTIALS_PATH = os.getenv("GOOGLE_CREDENTIALS", "credentials.json")
try:
    creds = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_PATH, SCOPES)
    client = gspread.authorize(creds)
except Exception as e:
    logger.error(f"Ошибка аутентификации Google Sheets: {e}")
    exit(1)

SHEET_NAME = os.getenv("GOOGLE_SHEET_NAME", "IRELeads")
try:
    sheet = client.open(SHEET_NAME).sheet1
except Exception as e:
    logger.error(f"Не удалось открыть Google Sheet '{SHEET_NAME}': {e}")
    exit(1)

# Допустим, сделали 32 столбца (добавили messenger_call)
REQUIRED_HEADERS = [
    "Имя клиента",
    "Телефон",
    "Telegram",
    "WhatsApp",
    "Email",
    "Мессенджер",
    "Purpose",
    "Payment method",
    "UTM",
    "Project",
    "Region",  # Авто
    "Заметки",
    "Источник лида",
    "Часовой пояс",
    "Доступность в мессенджерах",
    "Сегментация",
    "Часовой пояс (GMT)",
    "Звонок IP-телефонией",
    "Альтернативный номер",
    "Личный номер",
    "Сообщение в мессенджеры (Повторное касание)",
    "Перенос встречи",
    "Прогрев / прогресс",
    "Интерес",
    "Результат (CRM)",
    "Задача на повтор",
    "Бюджет",
    "Цель",
    "Предпочтения",
    "Видеозвонок / осмотр объекта",
    "Отправить материалы лиду",
    "Связаться через мессенджер",  # <== Новое поле
]

def setup_sheet():
    existing = sheet.row_values(1)
    if not existing:
        sheet.insert_row(REQUIRED_HEADERS, 1)
        logger.info("Заголовки добавлены (была пустая таблица).")
    else:
        if existing[:len(REQUIRED_HEADERS)] != REQUIRED_HEADERS:
            sheet.delete_rows(1)
            sheet.insert_row(REQUIRED_HEADERS, 1)
            logger.info("Заголовки обновлены.")
        else:
            logger.info("Заголовки уже ОК.")

setup_sheet()

# ------------------ Состояния ------------------
class LeadStates(Enum):
    BULK_DATA = auto()
    EDITING = auto()

# ------------------ Парсинг лида ------------------
FIELD_PATTERNS = {
    "Имя клиента": re.compile(r"Имя клиента:\s*(.+)", re.IGNORECASE),
    "Телефон": re.compile(r"Телефон:\s*(\+?\d{6,15})", re.IGNORECASE),
    "Telegram": re.compile(r"Telegram:\s*@?(\w+)", re.IGNORECASE),
    "WhatsApp": re.compile(r"WhatsApp:\s*(\+?\d{6,15})", re.IGNORECASE),
    "Email": re.compile(r"Email:\s*([\w\.-]+@[\w\.-]+\.\w+)?", re.IGNORECASE),
    "Мессенджер": re.compile(r"Мессенджер:\s*(.+)", re.IGNORECASE),
    "Purpose": re.compile(r"Purpose:\s*(.+)", re.IGNORECASE),
    "Payment method": re.compile(r"Paym?ent\s*method:\s*(.+)", re.IGNORECASE),
    "UTM": re.compile(r"UTM:\s*(.+)", re.IGNORECASE),
    "Project": re.compile(r"Project:\s*(.+)", re.IGNORECASE),
    "Region": re.compile(r"Region:\s*(.+)", re.IGNORECASE),
}

def parse_lead_text(text: str) -> dict:
    lead_data = {}
    for field, regex in FIELD_PATTERNS.items():
        match = regex.search(text)
        if match and match.group(1):
            raw = match.group(1)
            lead_data[field] = raw.strip()
        else:
            lead_data[field] = ""
    return lead_data

def validate_phone(phone: str) -> bool:
    if not phone:
        return True
    return bool(re.match(r'^\+?\d{6,15}$', phone))

def validate_email(email: str) -> bool:
    if not email:
        return True
    return bool(re.match(r"[^@]+@[^@]+\.[^@]+", email))

# ------------- Авто ЧП и регион ---------------
COUNTRY_TZ_MAP = {
    "34": "GMT+1",
    "7": "GMT+3",
    "1": "GMT-5",
}

def guess_tz_by_phone(phone: str) -> str:
    if not phone.startswith("+"):
        return "Другой"
    digits = phone[1:]
    for ccode in sorted(COUNTRY_TZ_MAP.keys(), key=len, reverse=True):
        if digits.startswith(ccode):
            return COUNTRY_TZ_MAP[ccode]
    return "Другой"

def guess_region_by_phone(phone: str) -> str:
    if not phone.startswith("+"):
        return "Unknown"
    digits = phone[1:]
    if digits.startswith("34"):
        return "Spain"
    if digits.startswith("7"):
        return "Russia"
    if digits.startswith("1"):
        return "USA"
    return "Unknown"

def parse_utm_details(utm_str: str) -> str:
    """
    Раскладываем строку UTM, разделяя по символам '|' и '&'.
    Если внутри фрагмента найдётся '=', выводим key = val;
    иначе показываем весь фрагмент как есть.
    """
    if not utm_str:
        return "UTM-данные: (не указаны)"

    # Разделяем строку сначала по '|'
    pipe_parts = utm_str.split('|')

    lines = []
    for part in pipe_parts:
        part = part.strip()
        # внутри каждого part ещё делим по '&'
        amp_parts = part.split('&')
        for chunk in amp_parts:
            chunk = chunk.strip()
            if not chunk:
                continue

            # Если есть '=', разбираем key=val
            if '=' in chunk:
                kv = chunk.split('=', 1)
                if len(kv) == 2:
                    k, v = kv
                    lines.append(f"{k.strip()} = {v.strip()}")
                else:
                    lines.append(chunk)  # На всякий случай
            else:
                # Нет '=', выводим как есть
                lines.append(chunk)

    if not lines:
        return "UTM-данные: (пусто)"

    return "UTM-данные:\n" + "\n".join(lines)


# ------------------ Поля и маппинги -----------
FIELD_CODES = {
    "Источник лида": "src",
    "Часовой пояс": "tz",
    "Доступность в мессенджерах": "msg_avail",
    "Сегментация": "seg",
    "Звонок IP-телефонией": "ip_call",
    "Альтернативный номер": "alt_phone_yesno",
    "Личный номер": "pers_phone_yesno",
    "Перенос встречи": "meet_postpone",
    "Сообщение в мессенджеры (Повторное касание)": "msg_touch",
    "Заметки": "notes",
    "Бюджет": "budget",
    "Цель": "goal",
    "Предпочтения": "prefs",
    "Видеозвонок / осмотр объекта": "vid_call",
    "Отправить материалы лиду": "send_mat",
    "Прогрев / прогресс": "progress",
    "Интерес": "interest",
    "Результат (CRM)": "crm_result",
    "Задача на повтор": "task_repeat",
    # Новое:
    "Связаться через мессенджер": "messenger_call",
}
CODE_TO_FIELD = {v: k for k, v in FIELD_CODES.items()}

OPTIONS_MAP = {
    "src": {"tg": "Telegram", "other": "Другой"},
    "tz": {"gmt3": "GMT+3", "other": "Другой"},
    "msg_avail": {"tg": "Telegram", "wa": "WhatsApp", "vb": "Viber", "ln": "Line", "none": "Нет"},
    "seg": {"buyer": "покупатель", "investor": "инвестор"},
    "ip_call": {"yes": "Да", "no": "Нет"},
    "alt_phone_yesno": {"yes": "Да", "no": "Нет"},
    "pers_phone_yesno": {"yes": "Да", "no": "Нет"},
    "meet_postpone": {"earlier": "Да, контактировать на день раньше", "no": "Нет"},
    "vid_call": {"yes": "Да", "no": "Нет"},
    "send_mat": {"yes": "Да", "no": "Нет"},
    "progress": {"details": "Прогрев: согласие на детали", "transfer": "Прогрев: перенос"},
    "interest": {"high": "Высокий", "med": "Средний", "low": "Низкий"},
    "crm_result": {"success": "Успех", "fail": "Неудача", "repeat": "Повтор касания"},
    "task_repeat": {"yes": "Да", "no": "Нет"},
    "messenger_call": {"yes": "Да", "no": "Нет"},
}

FIELD_TO_COL = {
    "notes": 12,
    "src": 13,
    "tz": 14,
    "msg_avail": 15,
    "seg": 16,
    "ip_call": 18,
    "alt_phone_yesno": 19,
    "pers_phone_yesno": 20,
    "msg_touch": 21,
    "meet_postpone": 22,
    "progress": 23,
    "interest": 24,
    "crm_result": 25,
    "task_repeat": 26,
    "budget": 27,
    "goal": 28,
    "prefs": 29,
    "vid_call": 30,
    "send_mat": 31,
    "messenger_call": 32,
}

ALGORITHM_TEXT = (
    "Вот алгоритм действий для подготовки и разговора с клиентом:\n\n"
    "1) Подготовка к контакту:\n"
    "   - Определить источник лида.\n"
    "   - Уточнить регион клиента (авто).\n"
    "   - Проверить часовой пояс.\n"
    "   - Узнать доступность в мессенджерах.\n"
    "   - Подготовить материалы и локальные номера.\n"
    "2) Первый контакт:\n"
    "   - Попробовать дозвон через телефонию.\n"
    "   - При недоступности - попробовать WhatsApp/Telegram.\n"
    "   - Записать результат в CRM.\n"
    "3) Продолжение:\n"
    "   - Повторное касание через 1 день.\n"
    "   - Поддержка интереса, отправка материалов.\n"
    "4) Разговор с клиентом:\n"
    "   - Представиться.\n"
    "   - Уточнить бюджет/цель/условия.\n"
    "   - Презентация объекта.\n"
    "   - Записать предпочтения в CRM.\n"
    "5) Завершение / финализация:\n"
    "   - Если готов - организовать видеозвонок или осмотр.\n"
    "   - Иначе - запланировать повтор.\n"
    "6) Постконтактная работа:\n"
    "   - Записать итог контакта.\n"
    "   - Продолжить взаимодействие, если клиент \"отложил\".\n"
)

def cmd_algorithm(update: Update, context: CallbackContext):
    """Показываем алгоритм взаимодействия с клиентом."""
    if not check_authorized(update):
        return
    update.message.reply_text(ALGORITHM_TEXT)

def cmd_start(update: Update, context: CallbackContext):
    """Старт. Просим пользователя вставить лид."""
    if not check_authorized(update):
        return

    text = (
        "Привет! Я бот, помогающий заполнить лид. "
        "Скопируйте и вставьте данные в таком формате:\n\n"
        "Имя клиента: Иван Иванов\n"
        "Телефон: +71234567890\n"
        "Telegram: @ivanov\n"
        "WhatsApp: +71234567890\n"
        "Email: ivanov@example.com\n"
        "Мессенджер: WhatsApp\n"
        "Purpose: Business meeting\n"
        "Payment method: Credit card\n"
        "UTM: utm_source=telegram&utm_medium=bot\n"
        "Project: Intellect | Cove Edition | 300k+ | ENG\n"
        "Region: Dubai (необязательно)\n\n"
        "Потом я выведу меню, чтобы можно было уточнить и проверить всё остальное!"
    )
    update.message.reply_text(text)
    return LeadStates.BULK_DATA

def process_lead(update: Update, context: CallbackContext):
    if not check_authorized(update):
        return LeadStates.BULK_DATA

    user_input = update.message.text.strip()
    lead = parse_lead_text(user_input)

    # Обязательное поле
    if not lead["Имя клиента"]:
        update.message.reply_text("Ой! Похоже, вы не указали 'Имя клиента'. Пожалуйста, повторите.")
        return LeadStates.BULK_DATA

    phone_candidate = lead.get("Телефон") or lead.get("WhatsApp")
    if not (phone_candidate or lead.get("Мессенджер")):
        update.message.reply_text(
            "Чтобы связаться, нужен либо телефон, либо мессенджер. Пожалуйста, добавьте одно из них."
        )
        return LeadStates.BULK_DATA

    if phone_candidate and not validate_phone(phone_candidate):
        update.message.reply_text("Формат телефона кажется неправильным. Попробуйте ещё раз.")
        return LeadStates.BULK_DATA
    if lead["Email"] and not validate_email(lead["Email"]):
        update.message.reply_text("Email выглядит некорректным. Исправьте или уберите поле Email.")
        return LeadStates.BULK_DATA

    # Автоопределение региона / час.пояса
    tz_auto = ""
    region_auto = lead.get("Region", "")
    if phone_candidate:
        tz_auto = guess_tz_by_phone(phone_candidate)
        if not region_auto:
            region_auto = guess_region_by_phone(phone_candidate)

    # Формируем row (32 столбца!)
    row = [
        lead.get("Имя клиента", ""),         # 1
        lead.get("Телефон", ""),            # 2
        lead.get("Telegram", ""),           # 3
        lead.get("WhatsApp", ""),           # 4
        lead.get("Email", ""),              # 5
        lead.get("Мессенджер", ""),         # 6
        lead.get("Purpose", ""),            # 7
        lead.get("Payment method", ""),     # 8
        lead.get("UTM", ""),                # 9
        lead.get("Project", ""),            # 10
        region_auto,                        # 11
        "",                                 # 12 Заметки
        lead.get("Источник лида", ""),      # 13
        tz_auto,                            # 14
        "",                                 # 15 msg_avail
        "",                                 # 16 seg
        "",                                 # 17 tz GMT
        "",                                 # 18 ip_call
        "",                                 # 19 alt_phone
        "",                                 # 20 pers_phone
        "",                                 # 21 msg_touch
        "",                                 # 22 meet_postpone
        "",                                 # 23 progress
        "",                                 # 24 interest
        "",                                 # 25 crm_result
        "",                                 # 26 task_repeat
        "",                                 # 27 budget
        "",                                 # 28 goal
        "",                                 # 29 prefs
        "",                                 # 30 vid_call
        "",                                 # 31 send_mat
        "",                                 # 32 messenger_call
    ]

    # Сохраняем
    try:
        sheet.append_row(row)
    except Exception as e:
        logger.error(f"Ошибка при сохранении: {e}")
        update.message.reply_text("Ой, что-то пошло не так при записи в Google Sheets. Попробуйте позже.")
        return LeadStates.BULK_DATA

    new_row_index = len(sheet.get_all_values())
    logger.info(f"Лид сохранён в строку {new_row_index}.")

    # --- Дополнительное сообщение об auto-Region, timeZone, UTM ---
    # Распаковка UTM
    utm_str = lead.get("UTM", "")
    utm_info = parse_utm_details(utm_str)
    summary_text = (
        "Данные лида сохранены!\n\n"
        f"Определён часовой пояс: {tz_auto or 'N/A'}\n"
        f"Определён регион: {region_auto or 'N/A'}\n\n"
        f"{utm_info}\n"
        "Теперь вы можете уточнить дополнительные поля в меню ниже."
    )
    update.message.reply_text(summary_text)
    # ---

    show_main_menu(update, context, new_row_index)
    return LeadStates.EDITING

def show_main_menu(update_or_query, context: CallbackContext, row_index: int):
    # ... тот же код формирования меню ...
    try:
        row_vals = sheet.row_values(row_index)
    except Exception as e:
        logger.error(f"Ошибка при чтении строки {row_index}: {e}")
        _reply(update_or_query, "Не смог получить данные о лиде. Попробуйте позже.")
        return

    def get_val(field_code: str):
        col = FIELD_TO_COL.get(field_code, 0)
        if col < 1 or col > len(row_vals):
            return ""
        return (row_vals[col-1] or "").strip().lower()

    keyboard = []

    # 1) Подготовка (источник, мессенджеры)
    keyboard.append([InlineKeyboardButton("=== (1) Подготовка к контакту ===", callback_data="noop")])
    # Источник лида
    src_val = get_val("src")
    tg_icon = "✅" if src_val=="telegram" else "❌"
    other_icon = "✅" if src_val=="другой" else "❌"
    keyboard.append([
        InlineKeyboardButton(f"Источник: Telegram {tg_icon}", callback_data=f"src:tg:{row_index}"),
        InlineKeyboardButton(f"Другой {other_icon}", callback_data=f"src:other:{row_index}")
    ])
    # Доступность
    ma_v = get_val("msg_avail")
    def icon2(x): return "✅" if ma_v==x.lower() else "❌"
    keyboard.append([
        InlineKeyboardButton(f"Tg {icon2('telegram')}", callback_data=f"msg_avail:tg:{row_index}"),
        InlineKeyboardButton(f"WA {icon2('whatsapp')}", callback_data=f"msg_avail:wa:{row_index}"),
        InlineKeyboardButton(f"Viber {icon2('viber')}", callback_data=f"msg_avail:vb:{row_index}")
    ])
    keyboard.append([
        InlineKeyboardButton(f"Line {icon2('line')}", callback_data=f"msg_avail:ln:{row_index}"),
        InlineKeyboardButton(f"Нет {icon2('нет')}", callback_data=f"msg_avail:none:{row_index}")
    ])
    # Сегментация
    seg_v = get_val("seg")
    buy_icon = "✅" if seg_v=="покупатель" else "❌"
    inv_icon = "✅" if seg_v=="инвестор" else "❌"
    keyboard.append([
        InlineKeyboardButton(f"Сегментация: покупатель {buy_icon}", callback_data=f"seg:buyer:{row_index}"),
        InlineKeyboardButton(f"инвестор {inv_icon}", callback_data=f"seg:investor:{row_index}")
    ])

    # 2) Первый контакт
    keyboard.append([InlineKeyboardButton("=== (2) Первый контакт ===", callback_data="noop")])
    # Звонок IP
    ipcall = get_val("ip_call")
    yes_icon = "✅" if ipcall=="да" else "❌"
    no_icon = "✅" if ipcall=="нет" else "❌"
    keyboard.append([
        InlineKeyboardButton(f"Звонок IP: Да {yes_icon}", callback_data=f"ip_call:yes:{row_index}"),
        InlineKeyboardButton(f"Нет {no_icon}", callback_data=f"ip_call:no:{row_index}")
    ])
    # Альт.номер
    alt_v = get_val("alt_phone_yesno")
    alt_yes = "✅" if alt_v=="да" else "❌"
    alt_no = "✅" if alt_v=="нет" else "❌"
    keyboard.append([
        InlineKeyboardButton(f"Альтернативный номер: Да {alt_yes}", callback_data=f"alt_phone_yesno:yes:{row_index}"),
        InlineKeyboardButton(f"Нет {alt_no}", callback_data=f"alt_phone_yesno:no:{row_index}")
    ])
    # Личный номер
    per_v = get_val("pers_phone_yesno")
    per_yes = "✅" if per_v=="да" else "❌"
    per_no = "✅" if per_v=="нет" else "❌"
    keyboard.append([
        InlineKeyboardButton(f"Личный номер: Да {per_yes}", callback_data=f"pers_phone_yesno:yes:{row_index}"),
        InlineKeyboardButton(f"Нет {per_no}", callback_data=f"pers_phone_yesno:no:{row_index}")
    ])
    # Связаться через мессенджер
    mc_v = get_val("messenger_call")
    mc_yes = "✅" if mc_v=="да" else "❌"
    mc_no = "✅" if mc_v=="нет" else "❌"
    keyboard.append([
        InlineKeyboardButton(f"Связаться через мессенджер: Да {mc_yes}", callback_data=f"messenger_call:yes:{row_index}"),
        InlineKeyboardButton(f"Нет {mc_no}", callback_data=f"messenger_call:no:{row_index}")
    ])

    # 3) Продолжение
    keyboard.append([InlineKeyboardButton("=== (3) Продолжение ===", callback_data="noop")])
    meet_v = get_val("meet_postpone")
    earl_icon = "✅" if meet_v=="да, контактировать на день раньше" else "❌"
    no_meet_icon = "✅" if meet_v=="нет" else "❌"
    keyboard.append([
        InlineKeyboardButton(f"Перенос встречи: 'День раньше' {earl_icon}", callback_data=f"meet_postpone:earlier:{row_index}"),
        InlineKeyboardButton(f"Нет {no_meet_icon}", callback_data=f"meet_postpone:no:{row_index}")
    ])
    # Прогрев/прогресс
    prog_v = get_val("progress")
    dt_icon = "✅" if prog_v=="прогрев: согласие на детали" else "❌"
    tr_icon = "✅" if prog_v=="прогрев: перенос" else "❌"
    keyboard.append([
        InlineKeyboardButton(f"Прогрев: детали {dt_icon}", callback_data=f"progress:details:{row_index}"),
        InlineKeyboardButton(f"Прогрев: перенос {tr_icon}", callback_data=f"progress:transfer:{row_index}")
    ])
    # Интерес
    int_v = get_val("interest")
    hi_ic = "✅" if int_v=="высокий" else "❌"
    med_ic = "✅" if int_v=="средний" else "❌"
    low_ic = "✅" if int_v=="низкий" else "❌"
    keyboard.append([
        InlineKeyboardButton(f"Интерес: Высокий {hi_ic}", callback_data=f"interest:high:{row_index}"),
        InlineKeyboardButton(f"Средний {med_ic}", callback_data=f"interest:med:{row_index}"),
        InlineKeyboardButton(f"Низкий {low_ic}", callback_data=f"interest:low:{row_index}")
    ])
    # Результат CRM
    crm_v = get_val("crm_result")
    su_ic = "✅" if crm_v=="успех" else "❌"
    fa_ic = "✅" if crm_v=="неудача" else "❌"
    re_ic = "✅" if crm_v=="повтор касания" else "❌"
    keyboard.append([
        InlineKeyboardButton(f"Результат: Успех {su_ic}", callback_data=f"crm_result:success:{row_index}"),
        InlineKeyboardButton(f"Неудача {fa_ic}", callback_data=f"crm_result:fail:{row_index}"),
        InlineKeyboardButton(f"Повтор {re_ic}", callback_data=f"crm_result:repeat:{row_index}")
    ])
    # Задача на повтор
    task_v = get_val("task_repeat")
    ty = "✅" if task_v=="да" else "❌"
    tn = "✅" if task_v=="нет" else "❌"
    keyboard.append([
        InlineKeyboardButton(f"Задача на повтор: Да {ty}", callback_data=f"task_repeat:yes:{row_index}"),
        InlineKeyboardButton(f"Нет {tn}", callback_data=f"task_repeat:no:{row_index}")
    ])

    # 4) Повторное касание
    keyboard.append([InlineKeyboardButton("=== (4) Повторное касание ===", callback_data="noop")])
    keyboard.append([
        InlineKeyboardButton("Сообщение (ввести)", callback_data=f"input:msg_touch:{row_index}")
    ])

    # 5) Общение
    keyboard.append([InlineKeyboardButton("=== (5) Общение с лидом ===", callback_data="noop")])
    keyboard.append([
        InlineKeyboardButton("Заметки (доп. запись)", callback_data=f"input:notes:{row_index}")
    ])
    keyboard.append([
        InlineKeyboardButton("Бюджет (ввести)", callback_data=f"input:budget:{row_index}"),
        InlineKeyboardButton("Цель (ввести)", callback_data=f"input:goal:{row_index}")
    ])
    keyboard.append([
        InlineKeyboardButton("Предпочтения (ввести)", callback_data=f"input:prefs:{row_index}")
    ])

    # 6) Финализация
    keyboard.append([InlineKeyboardButton("=== (6) Финализация ===", callback_data="noop")])
    vid_val = get_val("vid_call")
    v_yes = "✅" if vid_val=="да" else "❌"
    v_no = "✅" if vid_val=="нет" else "❌"
    keyboard.append([
        InlineKeyboardButton(f"Видеозвонок: Да {v_yes}", callback_data=f"vid_call:yes:{row_index}"),
        InlineKeyboardButton(f"Нет {v_no}", callback_data=f"vid_call:no:{row_index}")
    ])
    sm_val = get_val("send_mat")
    sm_yes = "✅" if sm_val=="да" else "❌"
    sm_no = "✅" if sm_val=="нет" else "❌"
    keyboard.append([
        InlineKeyboardButton(f"Отправить материалы: Да {sm_yes}", callback_data=f"send_mat:yes:{row_index}"),
        InlineKeyboardButton(f"Нет {sm_no}", callback_data=f"send_mat:no:{row_index}")
    ])

    # Завершить
    finish_data = f"finish:_:{row_index}"
    keyboard.append([InlineKeyboardButton("Завершить", callback_data=finish_data)])

    markup = InlineKeyboardMarkup(keyboard)
    _reply(update_or_query, 
           "Пожалуйста, уточните нужные пункты или введите текстовые поля:\n"
           "(Нажмите «Завершить», когда всё заполнено.)", 
           markup)

def _reply(update_or_query, text, reply_markup=None):
    # Проверяем, есть ли вообще callback_query
    if getattr(update_or_query, 'callback_query', None) is not None:
        update_or_query.callback_query.edit_message_text(text, reply_markup=reply_markup)
    else:
        update_or_query.message.reply_text(text, reply_markup=reply_markup)

def handle_button(update: Update, context: CallbackContext):
    if not check_authorized(update):
        return LeadStates.BULK_DATA

    query = update.callback_query
    query.answer()
    data = query.data.split(":")
    if len(data) != 3:
        query.edit_message_text("Что-то не так с данными кнопки (callback_data).")
        return LeadStates.EDITING

    field_code, short_code, row_str = data
    row_idx = int(row_str)

    if field_code == "noop":
        show_main_menu(query, context, row_idx)
        return LeadStates.EDITING

    if field_code == "finish":
        query.edit_message_text(
            "Все данные обновлены! Я готов принять нового лида.\n"
            "Просто пришлите следующий лид в том же формате."
        )
        return LeadStates.BULK_DATA

    if field_code == "input":
        context.user_data["editing_field"] = short_code
        context.user_data["editing_row"] = row_idx
        field_name = CODE_TO_FIELD.get(short_code, short_code)
        query.edit_message_text(f"Напишите, пожалуйста, {field_name}:")
        return LeadStates.EDITING

    col = FIELD_TO_COL.get(field_code)
    if not col:
        query.edit_message_text("Неизвестное поле для обновления.")
        return LeadStates.EDITING

    if field_code not in OPTIONS_MAP or short_code not in OPTIONS_MAP[field_code]:
        query.edit_message_text("Неизвестная опция.")
        return LeadStates.EDITING

    new_text = OPTIONS_MAP[field_code][short_code]
    try:
        current_val = sheet.cell(row_idx, col).value or ""
    except Exception as e:
        logger.error(f"Ошибка при чтении Google Sheets: {e}")
        query.edit_message_text("Ошибка при чтении данных. Попробуйте позже.")
        return LeadStates.EDITING

    if current_val.lower() == new_text.lower():
        new_val = ""
    else:
        new_val = new_text

    try:
        sheet.update_cell(row_idx, col, new_val)
        disp_val = new_val if new_val else "Пусто"
        field_name = CODE_TO_FIELD.get(field_code, field_code)
        query.edit_message_text(f"{field_name} → {disp_val}")
        show_main_menu(query, context, row_idx)
    except Exception as e:
        logger.error(f"Ошибка update_cell: {e}")
        query.edit_message_text("Произошла ошибка при обновлении. Попробуйте ещё раз.")

    return LeadStates.EDITING

def handle_text_input(update: Update, context: CallbackContext):
    if not check_authorized(update):
        return LeadStates.BULK_DATA

    text = update.message.text.strip()
    code = context.user_data.get("editing_field")
    row_idx = context.user_data.get("editing_row")
    if not code or not row_idx:
        update.message.reply_text("Не найдено, какое поле надо заполнить. Попробуйте заново.")
        return LeadStates.BULK_DATA

    col = FIELD_TO_COL.get(code, 0)
    if col < 1:
        update.message.reply_text("Что-то не так с этим полем.")
        return LeadStates.BULK_DATA

    try:
        if code == "notes":
            existing_val = sheet.cell(row_idx, col).value or ""
            if existing_val:
                new_val = existing_val + "; " + text
            else:
                new_val = text
            sheet.update_cell(row_idx, col, new_val)
        else:
            sheet.update_cell(row_idx, col, text)

        field_name = CODE_TO_FIELD.get(code, code)
        update.message.reply_text(f"Отлично, поле «{field_name}» теперь: «{text}».")
        show_main_menu(update, context, row_idx)
        return LeadStates.EDITING
    except Exception as e:
        logger.error(f"Ошибка при записи в Sheets: {e}")
        update.message.reply_text("Произошла ошибка при сохранении. Попробуйте позже.")
        return LeadStates.BULK_DATA

def cmd_cancel(update: Update, context: CallbackContext):
    if not check_authorized(update):
        return ConversationHandler.END
    update.message.reply_text("Процесс отменён. Введите /start, чтобы начать заново.")
    return ConversationHandler.END

def unknown(update: Update, context: CallbackContext):
    if not check_authorized(update):
        return
    update.message.reply_text("Неизвестная команда. Попробуйте /start или /algorithm.")

def error_handler(update: object, context: CallbackContext):
    logger.error("Exception while handling an update:", exc_info=context.error)
    if isinstance(update, Update) and update.effective_message:
        update.effective_message.reply_text("Произошла ошибка. Попробуйте позже.")

def main():
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    if not token:
        logger.error("Не задан TELEGRAM_BOT_TOKEN в .env")
        return

    updater = Updater(token, use_context=True)
    dp = updater.dispatcher

    # Добавим команду /algorithm для вывода алгоритма
    dp.add_handler(CommandHandler('algorithm', cmd_algorithm))

    conv_handler = ConversationHandler(
        entry_points=[CommandHandler('start', cmd_start)],
        states={
            LeadStates.BULK_DATA: [
                MessageHandler(Filters.text & ~Filters.command, process_lead)
            ],
            LeadStates.EDITING: [
                CallbackQueryHandler(handle_button),
                MessageHandler(Filters.text & ~Filters.command, handle_text_input),
            ],
        },
        fallbacks=[CommandHandler('cancel', cmd_cancel)],
    )

    dp.add_handler(conv_handler)
    dp.add_handler(MessageHandler(Filters.command, unknown))
    dp.add_error_handler(error_handler)

    updater.start_polling()
    logger.info("Бот запущен, ожидаю лиды...")
    updater.idle()

if __name__ == '__main__':
    main()
