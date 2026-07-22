import asyncio
import logging
import sqlite3
import json
import os
import re
import csv
import random
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
from pathlib import Path
from threading import Thread
import shutil

# ===================== БИБЛИОТЕКИ =====================
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, InputFile, InputMediaPhoto, InputMediaVideo
from telegram.ext import (
    Application, CommandHandler, MessageHandler, CallbackQueryHandler,
    ConversationHandler, filters, ContextTypes, JobQueue
)
from telethon import TelegramClient, events
from telethon.tl.functions.messages import GetDialogsRequest, SendMessageRequest
from telethon.tl.types import InputPeerChannel, InputPeerUser, MessageMediaPhoto, MessageMediaDocument
from telethon.errors import FloodWaitError, RPCError
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
import pandas as pd
from flask import Flask, render_template, jsonify, request
import threading
import asyncio

# ===================== КОНФИГУРАЦИЯ =====================
BOT_TOKEN = "8901120783:AAHxSXhhpPk-BAsYRqiPAMKCdbICR9cCBzo"
API_ID = 123456
API_HASH = "ваш_api_hash"
ADMIN_IDS = [8562897889]

# Цены
PRICE_MONTH = 0
PRICE_YEAR = 0
PRICE_LIFETIME = 0

# Настройки рассылки
MAX_SESSIONS_PER_USER = 10
MAX_GROUPS_PER_SESSION = 50
MESSAGE_DELAY_MIN = 5  # секунд между сообщениями
MESSAGE_DELAY_MAX = 15
MAX_THREADS = 5
BATCH_SIZE = 10  # сообщений в одном пакете

# Пути
DB_PATH = "lemon_spreader.db"
SESSIONS_DIR = "sessions"
EXPORT_DIR = "exports"
MEDIA_DIR = "media"
TEMPLATES_DIR = "templates"
LOG_DIR = "logs"

# Создаем директории
for dir_path in [SESSIONS_DIR, EXPORT_DIR, MEDIA_DIR, TEMPLATES_DIR, LOG_DIR]:
    Path(dir_path).mkdir(exist_ok=True)

# Flask приложение (для веб-интерфейса)
flask_app = Flask(__name__)

# ===================== РАСШИРЕННАЯ БАЗА ДАННЫХ =====================
class DatabaseV2:
    def __init__(self, db_path: str = DB_PATH):
        self.db_path = db_path
        self.init_db()

    def init_db(self):
        """Инициализация всех таблиц с новыми полями"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # Таблица сессий (расширенная)
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS sessions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    phone TEXT UNIQUE NOT NULL,
                    session_string TEXT NOT NULL,
                    session_file TEXT,
                    added_by INTEGER,
                    is_active INTEGER DEFAULT 1,
                    is_banned INTEGER DEFAULT 0,
                    messages_sent INTEGER DEFAULT 0,
                    messages_failed INTEGER DEFAULT 0,
                    last_activity TIMESTAMP,
                    added_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    expires_at TIMESTAMP,
                    note TEXT
                )
            ''')
            
            # Таблица групп (расширенная)
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS groups (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    group_id TEXT NOT NULL,
                    group_name TEXT,
                    group_hash TEXT,
                    session_id INTEGER,
                    added_by INTEGER,
                    is_blacklisted INTEGER DEFAULT 0,
                    members_count INTEGER DEFAULT 0,
                    added_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_broadcast TIMESTAMP,
                    FOREIGN KEY (session_id) REFERENCES sessions (id)
                )
            ''')
            
            # Таблица пользователей (расширенная)
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY,
                    username TEXT,
                    first_name TEXT,
                    last_name TEXT,
                    subscription_end TIMESTAMP,
                    subscription_type TEXT DEFAULT 'free',
                    is_admin INTEGER DEFAULT 0,
                    referrer_id INTEGER,
                    balance REAL DEFAULT 0,
                    total_broadcasts INTEGER DEFAULT 0,
                    total_messages INTEGER DEFAULT 0,
                    reg_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_activity TIMESTAMP,
                    language TEXT DEFAULT 'ru'
                )
            ''')
            
            # Таблица рефералов
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS referrals (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    referrer_id INTEGER,
                    referred_id INTEGER,
                    date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    reward REAL DEFAULT 0
                )
            ''')
            
            # Таблица рассылок (расширенная)
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS broadcasts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    session_id INTEGER,
                    group_id TEXT,
                    message TEXT,
                    media_id TEXT,
                    status TEXT DEFAULT 'pending',
                    send_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    scheduled_date TIMESTAMP,
                    error TEXT,
                    is_auto INTEGER DEFAULT 0,
                    job_id TEXT,
                    retry_count INTEGER DEFAULT 0
                )
            ''')
            
            # Таблица медиа-файлов
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS media (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    file_id TEXT,
                    file_name TEXT,
                    file_type TEXT,
                    file_size INTEGER,
                    added_by INTEGER,
                    added_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Таблица шаблонов
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS templates (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    content TEXT NOT NULL,
                    created_by INTEGER,
                    is_public INTEGER DEFAULT 0,
                    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Таблица черного списка
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS blacklist (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    entity_id TEXT NOT NULL,
                    entity_type TEXT CHECK(entity_type IN ('user', 'group', 'channel')),
                    reason TEXT,
                    added_by INTEGER,
                    added_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Таблица статистики по дням
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS daily_stats (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    date DATE UNIQUE,
                    total_messages INTEGER DEFAULT 0,
                    total_sessions INTEGER DEFAULT 0,
                    total_users INTEGER DEFAULT 0,
                    revenue REAL DEFAULT 0
                )
            ''')
            
            # Добавляем админа
            for admin_id in ADMIN_IDS:
                cursor.execute('''
                    INSERT OR IGNORE INTO users (user_id, is_admin, subscription_end, subscription_type)
                    VALUES (?, 1, datetime('now', '+100 years'), 'lifetime')
                ''', (admin_id,))
            
            conn.commit()

    # ---------- РАСШИРЕННЫЕ МЕТОДЫ ----------
    def get_session_stats(self, session_id: int) -> Dict:
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute('''
                SELECT 
                    COUNT(DISTINCT group_id) as groups_count,
                    SUM(CASE WHEN status = 'sent' THEN 1 ELSE 0 END) as sent,
                    SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed,
                    messages_sent,
                    messages_failed
                FROM broadcasts b
                JOIN sessions s ON b.session_id = s.id
                WHERE b.session_id = ?
            ''', (session_id,))
            row = cursor.fetchone()
            return dict(row) if row else {}

    def update_daily_stats(self):
        """Обновление ежедневной статистики"""
        today = datetime.now().date()
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT OR REPLACE INTO daily_stats (date, total_messages, total_sessions, total_users)
                SELECT 
                    date(?),
                    COUNT(*) as messages,
                    COUNT(DISTINCT session_id) as sessions,
                    COUNT(DISTINCT user_id) as users
                FROM broadcasts b
                JOIN sessions s ON b.session_id = s.id
                JOIN users u ON s.added_by = u.user_id
                WHERE DATE(b.send_date) = date(?)
            ''', (today, today))
            conn.commit()

    def get_blacklist(self, entity_type: str = None) -> List[str]:
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            if entity_type:
                cursor.execute(
                    "SELECT entity_id FROM blacklist WHERE entity_type = ?",
                    (entity_type,)
                )
            else:
                cursor.execute("SELECT entity_id FROM blacklist")
            return [row[0] for row in cursor.fetchall()]

    def add_to_blacklist(self, entity_id: str, entity_type: str, reason: str, user_id: int):
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "INSERT OR IGNORE INTO blacklist (entity_id, entity_type, reason, added_by) VALUES (?, ?, ?, ?)",
                (entity_id, entity_type, reason, user_id)
            )
            conn.commit()

    def remove_from_blacklist(self, entity_id: str):
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM blacklist WHERE entity_id = ?", (entity_id,))
            conn.commit()

    # ---------- РАБОТА С МЕДИА ----------
    def add_media(self, file_id: str, file_name: str, file_type: str, file_size: int, user_id: int) -> int:
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "INSERT INTO media (file_id, file_name, file_type, file_size, added_by) VALUES (?, ?, ?, ?, ?)",
                (file_id, file_name, file_type, file_size, user_id)
            )
            conn.commit()
            return cursor.lastrowid

    def get_media(self, media_id: int) -> Optional[Dict]:
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM media WHERE id = ?", (media_id,))
            row = cursor.fetchone()
            return dict(row) if row else None

    def get_user_media(self, user_id: int) -> List[Dict]:
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute(
                "SELECT * FROM media WHERE added_by = ? ORDER BY added_date DESC",
                (user_id,)
            )
            return [dict(row) for row in cursor.fetchall()]

    # ---------- РАБОТА С ШАБЛОНАМИ ----------
    def add_template(self, name: str, content: str, user_id: int, is_public: int = 0) -> int:
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "INSERT INTO templates (name, content, created_by, is_public) VALUES (?, ?, ?, ?)",
                (name, content, user_id, is_public)
            )
            conn.commit()
            return cursor.lastrowid

    def get_templates(self, user_id: int) -> List[Dict]:
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute(
                "SELECT * FROM templates WHERE created_by = ? OR is_public = 1",
                (user_id,)
            )
            return [dict(row) for row in cursor.fetchall()]

    def delete_template(self, template_id: int) -> bool:
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM templates WHERE id = ?", (template_id,))
            conn.commit()
            return cursor.rowcount > 0

    # ---------- ЭКСПОРТ СТАТИСТИКИ ----------
    def export_stats_csv(self, user_id: int) -> str:
        """Экспорт статистики в CSV"""
        filename = f"{EXPORT_DIR}/stats_{user_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        with sqlite3.connect(self.db_path) as conn:
            df = pd.read_sql_query(
                '''
                SELECT 
                    b.send_date,
                    s.phone,
                    g.group_name,
                    b.status,
                    CASE WHEN b.is_auto = 1 THEN 'Авто' ELSE 'Ручная' END as type
                FROM broadcasts b
                JOIN sessions s ON b.session_id = s.id
                JOIN groups g ON b.group_id = g.group_id
                WHERE s.added_by = ?
                ORDER BY b.send_date DESC
                ''',
                conn,
                params=(user_id,)
            )
            df.to_csv(filename, index=False, encoding='utf-8-sig')
        return filename

db = DatabaseV2()

# ===================== РАСШИРЕННЫЙ МЕНЕДЖЕР СЕССИЙ =====================
class SessionManagerV2:
    def __init__(self):
        self.clients: Dict[int, TelegramClient] = {}
        self.running_tasks: Dict[int, asyncio.Task] = {}
        self.scheduler = AsyncIOScheduler()
        self.semaphore = asyncio.Semaphore(MAX_THREADS)
        self.flood_wait: Dict[int, datetime] = {}

    async def create_session(self, phone: str, session_string: str) -> Optional[TelegramClient]:
        try:
            session_file = f"{SESSIONS_DIR}/session_{phone.replace('+', '')}.session"
            client = TelegramClient(session_file, API_ID, API_HASH)
            await client.start(phone=phone)
            
            # Сохраняем session_string в БД
            session_string_saved = client.session.save()
            db.add_session(phone, session_string_saved, 0)  # user_id будет передан позже
            
            return client
        except Exception as e:
            logging.error(f"Session creation error: {e}")
            return None

    async def get_client(self, session_id: int, session_string: str) -> Optional[TelegramClient]:
        if session_id in self.clients:
            return self.clients[session_id]
        
        session_file = f"{SESSIONS_DIR}/client_{session_id}.session"
        client = TelegramClient(session_file, API_ID, API_HASH)
        try:
            await client.start()
            self.clients[session_id] = client
            return client
        except Exception as e:
            logging.error(f"Client start error: {e}")
            return None

    async def send_message_with_delay(
        self,
        session_id: int,
        session_string: str,
        group_id: str,
        group_hash: str,
        message: str,
        broadcast_id: int,
        media_file: str = None,
        inline_buttons: List[Tuple[str, str]] = None
    ) -> bool:
        """Отправка с задержкой и анти-спамом"""
        async with self.semaphore:
            # Проверка на flood wait
            if session_id in self.flood_wait:
                wait_until = self.flood_wait[session_id]
                if wait_until > datetime.now():
                    wait_seconds = (wait_until - datetime.now()).seconds
                    logging.info(f"Flood wait for session {session_id}: {wait_seconds}s")
                    await asyncio.sleep(wait_seconds)
            
            # Задержка между сообщениями
            delay = random.randint(MESSAGE_DELAY_MIN, MESSAGE_DELAY_MAX)
            await asyncio.sleep(delay)
            
            client = await self.get_client(session_id, session_string)
            if not client:
                db.update_broadcast_status(broadcast_id, 'failed', 'Client not available')
                return False
            
            try:
                from telethon.tl.types import InputPeerChannel, KeyboardButton, ReplyKeyboardMarkup
                from telethon.tl.functions.messages import SendMessageRequest
                
                peer = InputPeerChannel(
                    channel_id=int(group_id) if group_id.isdigit() else group_id,
                    access_hash=int(group_hash)
                )
                
                # Подготовка кнопок
                buttons = None
                if inline_buttons:
                    from telethon.tl.types import KeyboardButtonRow, KeyboardButton
                    rows = []
                    for i in range(0, len(inline_buttons), 2):
                        row = []
                        for j in range(i, min(i+2, len(inline_buttons))):
                            text, url = inline_buttons[j]
                            row.append(KeyboardButton(text, url=url))
                        rows.append(KeyboardButtonRow(row))
                    buttons = rows
                
                # Отправка с медиа или без
                if media_file:
                    # Отправка файла
                    if media_file.startswith('http'):
                        await client.send_file(peer, media_file, caption=message, buttons=buttons)
                    else:
                        await client.send_file(peer, open(media_file, 'rb'), caption=message, buttons=buttons)
                else:
                    await client.send_message(peer, message, buttons=buttons)
                
                db.update_broadcast_status(broadcast_id, 'sent')
                return True
                
            except FloodWaitError as e:
                wait_time = e.seconds
                self.flood_wait[session_id] = datetime.now() + timedelta(seconds=wait_time)
                db.update_broadcast_status(broadcast_id, 'failed', f'Flood wait: {wait_time}s')
                logging.warning(f"FloodWait for session {session_id}: {wait_time}s")
                return False
                
            except RPCError as e:
                error_msg = str(e)
                db.update_broadcast_status(broadcast_id, 'failed', error_msg)
                logging.error(f"RPCError: {e}")
                return False
                
            except Exception as e:
                error_msg = str(e)
                db.update_broadcast_status(broadcast_id, 'failed', error_msg)
                logging.error(f"Send error: {e}")
                return False

    async def clone_session(self, source_session_id: int, new_phone: str) -> bool:
        """Клонирование сессии для нового номера"""
        sessions = db.get_sessions()
        source = next((s for s in sessions if s['id'] == source_session_id), None)
        if not source:
            return False
        
        try:
            # Создаем копию сессии
            client = await self.create_session(new_phone, source['session_string'])
            if client:
                return True
        except Exception as e:
            logging.error(f"Clone error: {e}")
        return False

    async def get_group_members_count(self, session_id: int, group_id: str, group_hash: str) -> int:
        """Получение количества участников группы"""
        client = await self.get_client(session_id, '')
        if not client:
            return 0
        
        try:
            from telethon.tl.functions.channels import GetFullChannelRequest
            from telethon.tl.types import InputChannel
            
            channel = InputChannel(int(group_id), int(group_hash))
            full_chat = await client(GetFullChannelRequest(channel))
            return full_chat.full_chat.participants_count
        except:
            return 0

    async def setup_auto_responder(self, session_id: int, response_text: str):
        """Авто-ответ на входящие сообщения"""
        client = await self.get_client(session_id, '')
        if not client:
            return
        
        @client.on(events.NewMessage(incoming=True))
        async def auto_respond(event):
            if event.is_private and not event.out:
                try:
                    await event.reply(response_text)
                except:
                    pass

    async def close_all(self):
        for client in self.clients.values():
            await client.disconnect()
        self.clients.clear()

session_manager = SessionManagerV2()

# ===================== РАСПИСАНИЕ (APScheduler) =====================
class SchedulerManager:
    def __init__(self):
        self.scheduler = AsyncIOScheduler()
        self.jobs = {}

    async def add_scheduled_broadcast(
        self,
        user_id: int,
        session_id: int,
        group_ids: List[str],
        message: str,
        schedule_time: datetime,
        repeat: str = None  # None, 'daily', 'weekly', 'monthly'
    ) -> str:
        """Добавление запланированной рассылки"""
        job_id = f"broadcast_{user_id}_{datetime.now().timestamp()}"
        
        # Сохраняем в БД
        for group_id in group_ids:
            db.add_broadcast(session_id, group_id, message)
        
        if repeat == 'daily':
            trigger = CronTrigger(hour=schedule_time.hour, minute=schedule_time.minute)
        elif repeat == 'weekly':
            trigger = CronTrigger(day_of_week=schedule_time.weekday(), hour=schedule_time.hour, minute=schedule_time.minute)
        elif repeat == 'monthly':
            trigger = CronTrigger(day=schedule_time.day, hour=schedule_time.hour, minute=schedule_time.minute)
        else:
            trigger = schedule_time
        
        job = self.scheduler.add_job(
            self._execute_scheduled_broadcast,
            trigger,
            args=[user_id, session_id, group_ids, message],
            id=job_id
        )
        
        self.jobs[job_id] = {
            'job': job,
            'user_id': user_id,
            'session_id': session_id,
            'group_ids': group_ids,
            'message': message,
            'schedule_time': schedule_time,
            'repeat': repeat
        }
        
        return job_id

    async def _execute_scheduled_broadcast(self, user_id: int, session_id: int, group_ids: List[str], message: str):
        """Выполнение запланированной рассылки"""
        # Получаем сессию и группы
        sessions = db.get_sessions(user_id)
        session = next((s for s in sessions if s['id'] == session_id), None)
        if not session:
            return
        
        groups = db.get_groups(user_id)
        selected_groups = [g for g in groups if g['group_id'] in group_ids]
        
        for group in selected_groups:
            broadcast_id = db.add_broadcast(session_id, group['group_id'], message)
            await session_manager.send_message_with_delay(
                session_id,
                session['session_string'],
                group['group_id'],
                group['group_hash'],
                message,
                broadcast_id
            )

    def remove_scheduled_job(self, job_id: str):
        """Удаление запланированной задачи"""
        if job_id in self.jobs:
            self.scheduler.remove_job(job_id)
            del self.jobs[job_id]
            return True
        return False

    def get_scheduled_jobs(self, user_id: int) -> List[Dict]:
        """Получение списка задач пользователя"""
        jobs = []
        for job_id, data in self.jobs.items():
            if data['user_id'] == user_id:
                jobs.append({
                    'id': job_id,
                    'session_id': data['session_id'],
                    'group_ids': data['group_ids'],
                    'message': data['message'][:50] + '...',
                    'schedule_time': data['schedule_time'],
                    'repeat': data['repeat'] or 'once'
                })
        return jobs

scheduler_manager = SchedulerManager()

# ===================== ВЕБ-ИНТЕРФЕЙС (Flask) =====================
@flask_app.route('/')
def web_dashboard():
    return render_template('dashboard.html')

@flask_app.route('/api/stats')
def api_stats():
    """API для получения статистики"""
    total_users = len(db.get_sessions())
    total_sessions = len(db.get_sessions())
    total_broadcasts = len(db.get_groups())
    today = datetime.now().date()
    
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT total_messages FROM daily_stats WHERE date = ?",
            (today,)
        )
        row = cursor.fetchone()
        today_messages = row[0] if row else 0
    
    return jsonify({
        'total_users': total_users,
        'total_sessions': total_sessions,
        'total_broadcasts': total_broadcasts,
        'today_messages': today_messages,
        'status': 'online'
    })

def run_flask():
    flask_app.run(host='0.0.0.0', port=5000, debug=False)

# ===================== БОТ-ХЭНДЛЕРЫ (РАСШИРЕННЫЕ) =====================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    user_id = user.id
    
    # Регистрация с рефералкой
    referrer_id = None
    if context.args and context.args[0].isdigit():
        referrer_id = int(context.args[0])
        # Начисление бонуса рефереру
        db.register_user(user_id, user.username, user.first_name, user.last_name, referrer_id)
    else:
        db.register_user(user_id, user.username, user.first_name, user.last_name)
    
    has_sub = db.has_subscription(user_id)
    is_admin = user_id in ADMIN_IDS
    
    keyboard = [
        [InlineKeyboardButton("📤 Сделать рассылку", callback_data='broadcast_start')],
        [InlineKeyboardButton("👥 Мои группы", callback_data='my_groups')],
        [InlineKeyboardButton("📱 Мои сессии", callback_data='my_sessions')],
        [InlineKeyboardButton("⏰ Запланировать", callback_data='schedule_broadcast')],
        [InlineKeyboardButton("📎 Шаблоны", callback_data='templates_menu')],
        [InlineKeyboardButton("🖼️ Медиа-библиотека", callback_data='media_library')],
        [InlineKeyboardButton("🚫 Черный список", callback_data='blacklist_menu')],
        [InlineKeyboardButton("📊 Статистика", callback_data='stats_menu')],
        [InlineKeyboardButton("💎 Подписка", callback_data='subscription_info')],
        [InlineKeyboardButton("🎁 Реферальная система", callback_data='referral_menu')],
        [InlineKeyboardButton("⚙️ Настройки", callback_data='settings_menu')],
        [InlineKeyboardButton("📈 Экспорт данных", callback_data='export_menu')],
    ]
    
    if is_admin:
        keyboard.append([InlineKeyboardButton("👑 Админ-панель", callback_data='admin_panel')])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    status = "🔓 ПРЕМИУМ" if has_sub else "🔒 БЕСПЛАТНЫЙ"
    
    # Получаем статистику пользователя
    sessions = len(db.get_sessions(user_id))
    groups = len(db.get_groups(user_id))
    broadcasts = db.get_broadcast_history(user_id)
    
    await update.message.reply_text(
        f"🍋 **LEMON SPREADER ULTRA v5.0**\n\n"
        f"👋 Привет, {user.first_name}!\n"
        f"📊 Статус: {status}\n"
        f"📱 Сессий: {sessions}\n"
        f"👥 Групп: {groups}\n"
        f"📤 Рассылок: {len(broadcasts)}\n"
        f"💰 Баланс: {db.get_user(user_id).get('balance', 0)} ₽\n\n"
        f"💡 Функции:\n"
        f"✅ Авто-рассылка по расписанию\n"
        f"✅ Медиа-файлы\n"
        f"✅ Инлайн-кнопки\n"
        f"✅ Шаблоны сообщений\n"
        f"✅ Многопоточная отправка\n"
        f"✅ Веб-интерфейс\n\n"
        f"⚡ Выбери действие:",
        reply_markup=reply_markup,
        parse_mode='Markdown'
    )

# ========== НОВЫЕ ОБРАБОТЧИКИ ==========
async def handle_schedule_broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    keyboard = [
        [InlineKeyboardButton("📅 На сегодня", callback_data='schedule_today')],
        [InlineKeyboardButton("📅 На завтра", callback_data='schedule_tomorrow')],
        [InlineKeyboardButton("📅 Выбрать дату", callback_data='schedule_date')],
        [InlineKeyboardButton("🔄 Ежедневно", callback_data='schedule_daily')],
        [InlineKeyboardButton("📆 Еженедельно", callback_data='schedule_weekly')],
        [InlineKeyboardButton("📅 Ежемесячно", callback_data='schedule_monthly')],
        [InlineKeyboardButton("📋 Мои задачи", callback_data='scheduled_list')],
        [InlineKeyboardButton("🔙 Назад", callback_data='main_menu')],
    ]
    
    await query.edit_message_text(
        "⏰ **Запланированная рассылка**\n\n"
        "Выберите периодичность:\n\n"
        "• Сегодня — разово\n"
        "• Завтра — разово\n"
        "• Ежедневно — в определенное время\n"
        "• Еженедельно — в определенный день\n"
        "• Ежемесячно — в определенную дату",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode='Markdown'
    )

async def handle_templates_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    templates = db.get_templates(user_id)
    
    keyboard = []
    for template in templates:
        keyboard.append([
            InlineKeyboardButton(
                f"📝 {template['name']}", 
                callback_data=f'template_use_{template["id"]}'
            )
        ])
    
    keyboard.append([InlineKeyboardButton("➕ Создать шаблон", callback_data='template_create')])
    keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data='main_menu')])
    
    if not templates:
        text = "📎 **У вас нет шаблонов**\n\nСоздайте шаблон для быстрой рассылки."
    else:
        text = f"📎 **Ваши шаблоны ({len(templates)})**\n\nВыберите для использования:"
    
    await update.callback_query.edit_message_text(
        text,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode='Markdown'
    )

async def handle_media_library(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    media_files = db.get_user_media(user_id)
    
    keyboard = []
    for media in media_files[:10]:
        keyboard.append([
            InlineKeyboardButton(
                f"🖼️ {media['file_name'][:30]}", 
                callback_data=f'media_use_{media["id"]}'
            )
        ])
    
    keyboard.append([InlineKeyboardButton("📤 Загрузить медиа", callback_data='media_upload')])
    keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data='main_menu')])
    
    text = f"🖼️ **Медиа-библиотека ({len(media_files)})**\n\n"
    text += "Выберите файл для рассылки или загрузите новый."
    
    await update.callback_query.edit_message_text(
        text,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode='Markdown'
    )

async def handle_blacklist_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = [
        [InlineKeyboardButton("🚫 Заблокировать группу", callback_data='blacklist_add_group')],
        [InlineKeyboardButton("🚫 Заблокировать пользователя", callback_data='blacklist_add_user')],
        [InlineKeyboardButton("📋 Список блокировок", callback_data='blacklist_list')],
        [InlineKeyboardButton("🔙 Назад", callback_data='main_menu')],
    ]
    
    await update.callback_query.edit_message_text(
        "🚫 **Черный список**\n\n"
        "Управление заблокированными группами и пользователями.\n"
        "Из черного списка рассылка не производится.",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode='Markdown'
    )

async def handle_stats_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    
    # Получаем подробную статистику
    sessions = db.get_sessions(user_id)
    total_sent = 0
    total_failed = 0
    groups_count = len(db.get_groups(user_id))
    
    for session in sessions:
        stats = db.get_session_stats(session['id'])
        total_sent += stats.get('sent', 0)
        total_failed += stats.get('failed', 0)
    
    text = (
        f"📊 **Ваша статистика**\n\n"
        f"📱 Сессий: {len(sessions)}\n"
        f"👥 Групп: {groups_count}\n"
        f"✅ Успешных рассылок: {total_sent}\n"
        f"❌ Неудачных: {total_failed}\n"
        f"📈 Успешность: {round(total_sent/(total_sent+total_failed)*100 if total_sent+total_failed > 0 else 0)}%\n"
        f"💰 Баланс: {db.get_user(user_id).get('balance', 0)} ₽\n\n"
        f"📋 Последние 5 рассылок:"
    )
    
    history = db.get_broadcast_history(user_id)[:5]
    for item in history:
        status = "✅" if item['status'] == 'sent' else "❌"
        text += f"\n{status} {item['send_date'][:10]} → {item['phone']}"
    
    keyboard = [[InlineKeyboardButton("📥 Экспорт CSV", callback_data='export_stats')]]
    keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data='main_menu')])
    
    await update.callback_query.edit_message_text(
        text,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode='Markdown'
    )

async def handle_referral_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    user = db.get_user(user_id)
    
    # Генерируем реферальную ссылку
    bot_username = (await context.bot.get_me()).username
    referral_link = f"https://t.me/{bot_username}?start={user_id}"
    
    text = (
        f"🎁 **Реферальная система**\n\n"
        f"Приглашай друзей и зарабатывай!\n\n"
        f"🔗 Твоя ссылка:\n`{referral_link}`\n\n"
        f"📊 Приглашено: {db.get_groups(user_id)}\n"  # упрощенно
        f"💰 Заработано: {user.get('balance', 0)} ₽\n\n"
        f"⚡ За каждого приглашенного друга ты получаешь 10% от его пополнений!"
    )
    
    keyboard = [
        [InlineKeyboardButton("📋 История рефералов", callback_data='referral_history')],
        [InlineKeyboardButton("🔙 Назад", callback_data='main_menu')],
    ]
    
    await update.callback_query.edit_message_text(
        text,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode='Markdown'
    )

async def handle_export_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = [
        [InlineKeyboardButton("📊 CSV-отчет", callback_data='export_csv')],
        [InlineKeyboardButton("📈 Excel-отчет", callback_data='export_excel')],
        [InlineKeyboardButton("📋 Экспорт сессий", callback_data='export_sessions')],
        [InlineKeyboardButton("🔙 Назад", callback_data='main_menu')],
    ]
    
    await update.callback_query.edit_message_text(
        "📈 **Экспорт данных**\n\n"
        "Выберите формат выгрузки:\n\n"
        "• CSV — универсальный\n"
        "• Excel — для анализа\n"
        "• Сессии — бэкап аккаунтов",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode='Markdown'
    )

async def handle_export_csv(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    filename = db.export_stats_csv(user_id)
    
    with open(filename, 'rb') as f:
        await update.callback_query.message.reply_document(
            document=InputFile(f, filename=os.path.basename(filename)),
            caption="📊 Ваш отчет готов!"
        )
    
    await update.callback_query.answer("Экспорт завершен!")

# ========== ОСНОВНАЯ ФУНКЦИЯ ==========
def main():
    # Запуск Flask в отдельном потоке
    flask_thread = Thread(target=run_flask, daemon=True)
    flask_thread.start()
    
    # Инициализация бота
    application = Application.builder().token(BOT_TOKEN).build()
    
    # Регистрируем обработчики
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("cancel", cancel))
    
    # Добавляем все новые обработчики
    application.add_handler(CallbackQueryHandler(handle_schedule_broadcast, pattern='^schedule_broadcast$'))
    application.add_handler(CallbackQueryHandler(handle_templates_menu, pattern='^templates_menu$'))
    application.add_handler(CallbackQueryHandler(handle_media_library, pattern='^media_library$'))
    application.add_handler(CallbackQueryHandler(handle_blacklist_menu, pattern='^blacklist_menu$'))
    application.add_handler(CallbackQueryHandler(handle_stats_menu, pattern='^stats_menu$'))
    application.add_handler(CallbackQueryHandler(handle_referral_menu, pattern='^referral_menu$'))
    application.add_handler(CallbackQueryHandler(handle_export_menu, pattern='^export_menu$'))
    application.add_handler(CallbackQueryHandler(handle_export_csv, pattern='^export_csv$'))
    application.add_handler(CallbackQueryHandler(handle_callback))
    
    # Запускаем планировщик
    scheduler_manager.scheduler.start()
    
    print("🍋 LEMON SPREADER ULTRA v5.0 ЗАПУЩЕН!")
    print("💀 ВСЕ ФУНКЦИИ АКТИВИРОВАНЫ!")
    print("🌐 Веб-интерфейс: http://localhost:5000")
    
    application.run_polling()

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(f"{LOG_DIR}/bot.log"),
            logging.StreamHandler()
        ]
    )
    main()
