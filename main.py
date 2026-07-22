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
from typing import Dict, List, Optional, Tuple, Any, Union
from pathlib import Path
from threading import Thread, Lock
import shutil
from functools import lru_cache
from contextlib import contextmanager

# ===================== БИБЛИОТЕКИ =====================
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, InputFile, InputMediaPhoto, InputMediaVideo
from telegram.ext import (
    Application, CommandHandler, MessageHandler, CallbackQueryHandler,
    ConversationHandler, filters, ContextTypes, JobQueue
)
from telethon import TelegramClient, events
from telethon.tl.functions.messages import GetDialogsRequest, SendMessageRequest
from telethon.tl.types import (
    InputPeerChannel, InputPeerUser, MessageMediaPhoto, MessageMediaDocument,
    KeyboardButton, KeyboardButtonRow
)
from telethon.errors import FloodWaitError, RPCError
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
import pandas as pd
from flask import Flask, render_template, jsonify, request

# ===================== КОНФИГУРАЦИЯ =====================
BOT_TOKEN = "8901120783:AAHxSXhhpPk-BAsYRqiPAMKCdbICR9cCBzo"
API_ID = 123456
API_HASH = "ваш_api_hash"
ADMIN_IDS = [8562897889]  # ЗАМЕНИТЕ НА СВОЙ ID!

# Безопасность Flask
FLASK_API_KEY = os.getenv("FLASK_API_KEY", "lemon_spreader_secret_2024")

# Цены
PRICE_MONTH = 0
PRICE_YEAR = 0
PRICE_LIFETIME = 0

# Настройки рассылки
MAX_SESSIONS_PER_USER = 10
MAX_GROUPS_PER_SESSION = 50
MESSAGE_DELAY_MIN = 5
MESSAGE_DELAY_MAX = 15
MAX_THREADS = 5
BATCH_SIZE = 10
MEDIA_FETCH_LIMIT = 50
CACHE_TTL = 300  # 5 минут

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

# Flask приложение
flask_app = Flask(__name__)

# Состояния для ConversationHandler
(
    ADD_SESSION_PHONE,
    ADD_SESSION_CODE,
    SELECT_GROUPS,
    WAIT_MESSAGE,
    WAIT_BROADCAST_TEXT,
    WAIT_TEMPLATE_NAME,
    WAIT_TEMPLATE_CONTENT,
    WAIT_BLACKLIST_REASON,
) = range(8)

# ===================== ПУЛ СОЕДИНЕНИЙ БД =====================
class DatabasePool:
    """Пул соединений с SQLite для многопоточности"""
    
    def __init__(self, db_path: str, pool_size: int = 5):
        self.db_path = db_path
        self.pool: List[sqlite3.Connection] = []
        self.lock = Lock()
        self.pool_size = pool_size
        self._init_pool()
    
    def _init_pool(self):
        for _ in range(self.pool_size):
            conn = sqlite3.connect(self.db_path, check_same_thread=False)
            conn.row_factory = sqlite3.Row
            self.pool.append(conn)
    
    @contextmanager
    def get_connection(self):
        conn = None
        with self.lock:
            if self.pool:
                conn = self.pool.pop()
            else:
                conn = sqlite3.connect(self.db_path, check_same_thread=False)
                conn.row_factory = sqlite3.Row
        try:
            yield conn
        finally:
            with self.lock:
                if len(self.pool) < self.pool_size:
                    self.pool.append(conn)
                else:
                    conn.close()
    
    def close_all(self):
        with self.lock:
            for conn in self.pool:
                conn.close()
            self.pool.clear()

db_pool = DatabasePool(DB_PATH)

# ===================== РАСШИРЕННАЯ БАЗА ДАННЫХ =====================
class DatabaseV2:
    """Полноценная работа с БД с кэшированием"""
    
    def __init__(self, db_path: str = DB_PATH):
        self.db_path = db_path
        self._user_cache: Dict[int, Dict] = {}
        self._cache_ttl = CACHE_TTL
        self._cache_timestamps: Dict[int, datetime] = {}
        self._init_db()
    
    def _init_db(self):
        """Инициализация всех таблиц"""
        with db_pool.get_connection() as conn:
            cursor = conn.cursor()
            
            # Таблица сессий
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
            
            # Таблица групп
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
            
            # Таблица пользователей
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
            
            # Таблица рассылок
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
            
            # Таблица медиа
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
            
            # Таблица статистики
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
    
    # ========== ОСНОВНЫЕ МЕТОДЫ ==========
    
    def get_sessions(self, user_id: Optional[int] = None) -> List[Dict]:
        """Получение списка сессий"""
        with db_pool.get_connection() as conn:
            cursor = conn.cursor()
            if user_id:
                cursor.execute(
                    "SELECT * FROM sessions WHERE added_by = ? AND is_active = 1",
                    (user_id,)
                )
            else:
                cursor.execute("SELECT * FROM sessions WHERE is_active = 1")
            return [dict(row) for row in cursor.fetchall()]
    
    def add_session(self, phone: str, session_string: str, user_id: int) -> bool:
        """Добавление сессии"""
        with db_pool.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """INSERT OR REPLACE INTO sessions 
                   (phone, session_string, added_by, is_active) 
                   VALUES (?, ?, ?, 1)""",
                (phone, session_string, user_id)
            )
            conn.commit()
            return True
    
    def delete_session(self, session_id: int) -> bool:
        """Мягкое удаление сессии"""
        with db_pool.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE sessions SET is_active = 0 WHERE id = ?",
                (session_id,)
            )
            conn.commit()
            return cursor.rowcount > 0
    
    def get_groups(self, user_id: Optional[int] = None) -> List[Dict]:
        """Получение списка групп"""
        with db_pool.get_connection() as conn:
            cursor = conn.cursor()
            if user_id:
                cursor.execute(
                    """SELECT * FROM groups 
                       WHERE added_by = ? AND is_blacklisted = 0""",
                    (user_id,)
                )
            else:
                cursor.execute("SELECT * FROM groups WHERE is_blacklisted = 0")
            return [dict(row) for row in cursor.fetchall()]
    
    def add_group(self, group_id: str, group_name: str, group_hash: str, 
                  session_id: int, user_id: int) -> bool:
        """Добавление группы"""
        with db_pool.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """INSERT OR IGNORE INTO groups 
                   (group_id, group_name, group_hash, session_id, added_by) 
                   VALUES (?, ?, ?, ?, ?)""",
                (group_id, group_name, group_hash, session_id, user_id)
            )
            conn.commit()
            return True
    
    def delete_group(self, group_id: str) -> bool:
        """Удаление группы"""
        with db_pool.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM groups WHERE group_id = ?", (group_id,))
            conn.commit()
            return cursor.rowcount > 0
    
    def get_user(self, user_id: int) -> Optional[Dict]:
        """Получение пользователя (с кэшем)"""
        # Проверка кэша
        if user_id in self._user_cache:
            timestamp = self._cache_timestamps.get(user_id)
            if timestamp and (datetime.now() - timestamp).seconds < self._cache_ttl:
                return self._user_cache[user_id]
        
        with db_pool.get_connection() as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM users WHERE user_id = ?", (user_id,))
            row = cursor.fetchone()
            if row:
                result = dict(row)
                self._user_cache[user_id] = result
                self._cache_timestamps[user_id] = datetime.now()
                return result
        return None
    
    def register_user(self, user_id: int, username: str = None, 
                      first_name: str = None, last_name: str = None, 
                      referrer_id: int = None):
        """Регистрация пользователя"""
        with db_pool.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """INSERT OR IGNORE INTO users 
                   (user_id, username, first_name, last_name, referrer_id) 
                   VALUES (?, ?, ?, ?, ?)""",
                (user_id, username, first_name, last_name, referrer_id)
            )
            conn.commit()
            # Инвалидируем кэш
            self._user_cache.pop(user_id, None)
            self._cache_timestamps.pop(user_id, None)
    
    def has_subscription(self, user_id: int) -> bool:
        """Проверка активной подписки"""
        with db_pool.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT subscription_end FROM users WHERE user_id = ?",
                (user_id,)
            )
            row = cursor.fetchone()
            if row and row[0]:
                try:
                    end_date = datetime.strptime(row[0], "%Y-%m-%d %H:%M:%S")
                    return end_date > datetime.now()
                except ValueError:
                    return False
            return False
    
    def set_subscription(self, user_id: int, days: int):
        """Установка подписки"""
        with db_pool.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE users SET subscription_end = datetime('now', ?) WHERE user_id = ?",
                (f'+{days} days', user_id)
            )
            conn.commit()
            self._user_cache.pop(user_id, None)
    
    def add_broadcast(self, session_id: int, group_id: str, message: str) -> int:
        """Добавление записи о рассылке"""
        with db_pool.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """INSERT INTO broadcasts 
                   (session_id, group_id, message, status) 
                   VALUES (?, ?, ?, 'pending')""",
                (session_id, group_id, message)
            )
            conn.commit()
            return cursor.lastrowid
    
    def update_broadcast_status(self, broadcast_id: int, status: str, error: str = None):
        """Обновление статуса рассылки"""
        with db_pool.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE broadcasts SET status = ?, error = ? WHERE id = ?",
                (status, error, broadcast_id)
            )
            conn.commit()
    
    def get_broadcast_history(self, user_id: int, limit: int = 50) -> List[Dict]:
        """Получение истории рассылок"""
        with db_pool.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT b.*, s.phone 
                FROM broadcasts b
                JOIN sessions s ON b.session_id = s.id
                WHERE s.added_by = ?
                ORDER BY b.send_date DESC LIMIT ?
            ''', (user_id, limit))
            return [dict(row) for row in cursor.fetchall()]
    
    def get_session_stats(self, session_id: int) -> Dict:
        """Статистика по сессии"""
        with db_pool.get_connection() as conn:
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
    
    def get_broadcast_retries(self, broadcast_id: int) -> int:
        """Получение количества попыток"""
        with db_pool.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT retry_count FROM broadcasts WHERE id = ?",
                (broadcast_id,)
            )
            row = cursor.fetchone()
            return row[0] if row else 0
    
    # ========== МЕДИА ==========
    
    def add_media(self, file_id: str, file_name: str, file_type: str, 
                  file_size: int, user_id: int) -> int:
        """Добавление медиа-файла"""
        with db_pool.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """INSERT INTO media 
                   (file_id, file_name, file_type, file_size, added_by) 
                   VALUES (?, ?, ?, ?, ?)""",
                (file_id, file_name, file_type, file_size, user_id)
            )
            conn.commit()
            return cursor.lastrowid
    
    def get_media(self, media_id: int) -> Optional[Dict]:
        """Получение медиа по ID"""
        with db_pool.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM media WHERE id = ?", (media_id,))
            row = cursor.fetchone()
            return dict(row) if row else None
    
    def get_user_media(self, user_id: int) -> List[Dict]:
        """Получение всех медиа пользователя"""
        with db_pool.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT * FROM media WHERE added_by = ? ORDER BY added_date DESC LIMIT ?",
                (user_id, MEDIA_FETCH_LIMIT)
            )
            return [dict(row) for row in cursor.fetchall()]
    
    # ========== ШАБЛОНЫ ==========
    
    def add_template(self, name: str, content: str, user_id: int, is_public: int = 0) -> int:
        """Добавление шаблона"""
        with db_pool.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "INSERT INTO templates (name, content, created_by, is_public) VALUES (?, ?, ?, ?)",
                (name, content, user_id, is_public)
            )
            conn.commit()
            return cursor.lastrowid
    
    def get_templates(self, user_id: int) -> List[Dict]:
        """Получение шаблонов пользователя"""
        with db_pool.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT * FROM templates WHERE created_by = ? OR is_public = 1",
                (user_id,)
            )
            return [dict(row) for row in cursor.fetchall()]
    
    def delete_template(self, template_id: int) -> bool:
        """Удаление шаблона"""
        with db_pool.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM templates WHERE id = ?", (template_id,))
            conn.commit()
            return cursor.rowcount > 0
    
    # ========== ЧЕРНЫЙ СПИСОК ==========
    
    def get_blacklist(self, entity_type: str = None) -> List[str]:
        """Получение черного списка"""
        with db_pool.get_connection() as conn:
            cursor = conn.cursor()
            if entity_type:
                cursor.execute(
                    "SELECT entity_id FROM blacklist WHERE entity_type = ?",
                    (entity_type,)
                )
            else:
                cursor.execute("SELECT entity_id FROM blacklist")
            return [row[0] for row in cursor.fetchall()]
    
    def add_to_blacklist(self, entity_id: str, entity_type: str, 
                         reason: str, user_id: int):
        """Добавление в черный список"""
        with db_pool.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """INSERT OR IGNORE INTO blacklist 
                   (entity_id, entity_type, reason, added_by) 
                   VALUES (?, ?, ?, ?)""",
                (entity_id, entity_type, reason, user_id)
            )
            conn.commit()
    
    def remove_from_blacklist(self, entity_id: str):
        """Удаление из черного списка"""
        with db_pool.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM blacklist WHERE entity_id = ?", (entity_id,))
            conn.commit()
    
    # ========== СТАТИСТИКА ==========
    
    def update_daily_stats(self):
        """Обновление ежедневной статистики"""
        today = datetime.now().date()
        with db_pool.get_connection() as conn:
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
    
    def export_stats_csv(self, user_id: int) -> str:
        """Экспорт статистики в CSV"""
        filename = f"{EXPORT_DIR}/stats_{user_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        with db_pool.get_connection() as conn:
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
    
    def get_daily_stats(self, date: datetime = None) -> Dict:
        """Получение статистики за день"""
        if not date:
            date = datetime.now()
        date_str = date.strftime("%Y-%m-%d")
        with db_pool.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT * FROM daily_stats WHERE date = ?",
                (date_str,)
            )
            row = cursor.fetchone()
            return dict(row) if row else {}

db = DatabaseV2()

# ===================== МЕНЕДЖЕР СЕССИЙ =====================
class SessionManagerV2:
    """Управление сессиями Telethon с авто-очисткой"""
    
    def __init__(self):
        self.clients: Dict[int, TelegramClient] = {}
        self.client_activity: Dict[int, datetime] = {}
        self.running_tasks: Dict[int, asyncio.Task] = {}
        self.scheduler = AsyncIOScheduler()
        self.semaphore = asyncio.Semaphore(MAX_THREADS)
        self.flood_wait: Dict[int, datetime] = {}
        self._cleanup_task = None
    
    async def create_session(self, phone: str, session_string: str) -> Optional[TelegramClient]:
        """Создание новой сессии"""
        try:
            session_file = f"{SESSIONS_DIR}/session_{phone.replace('+', '')}.session"
            client = TelegramClient(session_file, API_ID, API_HASH)
            await client.start(phone=phone)
            
            # Сохраняем session_string в БД
            session_string_saved = client.session.save()
            db.add_session(phone, session_string_saved, 0)
            
            return client
        except Exception as e:
            logging.error(f"Session creation error: {e}")
            return None
    
    async def get_client(self, session_id: int, session_string: str) -> Optional[TelegramClient]:
        """Получение клиента с кэшированием"""
        if session_id in self.clients:
            self.client_activity[session_id] = datetime.now()
            return self.clients[session_id]
        
        session_file = f"{SESSIONS_DIR}/client_{session_id}.session"
        client = TelegramClient(session_file, API_ID, API_HASH)
        try:
            await client.start()
            self.clients[session_id] = client
            self.client_activity[session_id] = datetime.now()
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
        """
        Отправка сообщения с задержкой и анти-спамом
        
        Args:
            session_id: ID сессии
            session_string: Строка сессии
            group_id: ID группы
            group_hash: Хэш группы
            message: Текст сообщения
            broadcast_id: ID рассылки
            media_file: Путь к медиа (опционально)
            inline_buttons: Кнопки (опционально)
        
        Returns:
            bool: Успешность отправки
        """
        async with self.semaphore:
            # Проверка на flood wait
            if session_id in self.flood_wait:
                wait_until = self.flood_wait[session_id]
                if wait_until > datetime.now():
                    wait_seconds = (wait_until - datetime.now()).seconds
                    logging.info(f"Flood wait for session {session_id}: {wait_seconds}s")
                    await asyncio.sleep(min(wait_seconds, 300))  # максимум 5 минут
            
            # Задержка между сообщениями
            delay = random.randint(MESSAGE_DELAY_MIN, MESSAGE_DELAY_MAX)
            await asyncio.sleep(delay)
            
            client = await self.get_client(session_id, session_string)
            if not client:
                db.update_broadcast_status(broadcast_id, 'failed', 'Client not available')
                return False
            
            try:
                # Валидация group_id и group_hash
                try:
                    group_id_int = int(group_id) if str(group_id).isdigit() else hash(str(group_id))
                    group_hash_int = int(group_hash) if str(group_hash).isdigit() else 0
                except (ValueError, TypeError):
                    db.update_broadcast_status(broadcast_id, 'failed', 'Invalid group ID')
                    return False
                
                peer = InputPeerChannel(
                    channel_id=group_id_int,
                    access_hash=group_hash_int
                )
                
                # Подготовка кнопок
                buttons = None
                if inline_buttons:
                    rows = []
                    for i in range(0, len(inline_buttons), 2):
                        row = []
                        for j in range(i, min(i+2, len(inline_buttons))):
                            text, url = inline_buttons[j]
                            if url:
                                row.append(KeyboardButton(text, url=url))
                            else:
                                row.append(KeyboardButton(text))
                        if row:
                            rows.append(KeyboardButtonRow(row))
                    if rows:
                        buttons = rows
                
                # Отправка с медиа или без
                if media_file:
                    if media_file.startswith('http'):
                        await client.send_file(peer, media_file, caption=message, buttons=buttons)
                    else:
                        file_path = Path(media_file)
                        if file_path.exists():
                            with open(media_file, 'rb') as f:
                                await client.send_file(peer, f, caption=message, buttons=buttons)
                        else:
                            db.update_broadcast_status(broadcast_id, 'failed', 'Media file not found')
                            return False
                else:
                    await client.send_message(peer, message, buttons=buttons)
                
                db.update_broadcast_status(broadcast_id, 'sent')
                self.client_activity[session_id] = datetime.now()
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
        """Клонирование сессии"""
        sessions = db.get_sessions()
        source = next((s for s in sessions if s['id'] == source_session_id), None)
        if not source:
            return False
        
        try:
            client = await self.create_session(new_phone, source['session_string'])
            return client is not None
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
            
            group_id_int = int(group_id) if str(group_id).isdigit() else 0
            group_hash_int = int(group_hash) if str(group_hash).isdigit() else 0
            
            if not group_id_int or not group_hash_int:
                return 0
            
            channel = InputChannel(group_id_int, group_hash_int)
            full_chat = await client(GetFullChannelRequest(channel))
            return full_chat.full_chat.participants_count
        except Exception:
            return 0
    
    async def setup_auto_responder(self, session_id: int, response_text: str):
        """Настройка авто-ответчика"""
        client = await self.get_client(session_id, '')
        if not client:
            return
        
        @client.on(events.NewMessage(incoming=True))
        async def auto_respond(event):
            if event.is_private and not event.out:
                try:
                    await event.reply(response_text)
                except Exception:
                    pass
    
    async def cleanup_idle_clients(self):
        """Очистка неактивных клиентов"""
        now = datetime.now()
        for session_id, client in list(self.clients.items()):
            last_activity = self.client_activity.get(session_id, now)
            if (now - last_activity).seconds > 300:  # 5 минут
                try:
                    await client.disconnect()
                    del self.clients[session_id]
                    del self.client_activity[session_id]
                except Exception:
                    pass
    
    async def close_all(self):
        """Закрытие всех клиентов"""
        for client in self.clients.values():
            try:
                await client.disconnect()
            except Exception:
                pass
        self.clients.clear()
        self.client_activity.clear()

session_manager = SessionManagerV2()

# ===================== ПЛАНИРОВЩИК =====================
class SchedulerManager:
    """Управление запланированными рассылками"""
    
    def __init__(self):
        self.scheduler = AsyncIOScheduler()
        self.jobs: Dict[str, Dict] = {}
    
    async def add_scheduled_broadcast(
        self,
        user_id: int,
        session_id: int,
        group_ids: List[str],
        message: str,
        schedule_time: datetime,
        repeat: str = None
    ) -> str:
        """Добавление запланированной рассылки"""
        job_id = f"broadcast_{user_id}_{datetime.now().timestamp()}"
        
        # Сохраняем в БД
        for group_id in group_ids:
            db.add_broadcast(session_id, group_id, message)
        
        # Настройка триггера
        if repeat == 'daily':
            trigger = CronTrigger(hour=schedule_time.hour, minute=schedule_time.minute)
        elif repeat == 'weekly':
            trigger = CronTrigger(
                day_of_week=schedule_time.weekday(),
                hour=schedule_time.hour,
                minute=schedule_time.minute
            )
        elif repeat == 'monthly':
            trigger = CronTrigger(
                day=schedule_time.day,
                hour=schedule_time.hour,
                minute=schedule_time.minute
            )
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
            'repeat': repeat or 'once'
        }
        
        return job_id
    
    async def _execute_scheduled_broadcast(self, user_id: int, session_id: int,
                                           group_ids: List[str], message: str):
        """Выполнение запланированной рассылки"""
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
    
    def remove_scheduled_job(self, job_id: str) -> bool:
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

# ===================== ВЕБ-ИНТЕРФЕЙС =====================
@flask_app.route('/')
def web_dashboard():
    """Главная страница веб-интерфейса"""
    try:
        return render_template('dashboard.html')
    except Exception:
        return "<h1>🍋 LEMON SPREADER</h1><p>Веб-интерфейс активен</p>"

@flask_app.route('/api/stats')
def api_stats():
    """API для получения статистики (с защитой)"""
    # Проверка API-ключа
    api_key = request.headers.get('X-API-Key')
    if api_key != FLASK_API_KEY:
        return jsonify({'error': 'Unauthorized'}), 401
    
    try:
        sessions = db.get_sessions()
        total_users = len(set(s.get('added_by', 0) for s in sessions))
        total_sessions = len(sessions)
        total_groups = len(db.get_groups())
        
        today = datetime.now().date()
        daily_stats = db.get_daily_stats(today)
        
        return jsonify({
            'total_users': total_users,
            'total_sessions': total_sessions,
            'total_broadcasts': total_groups,
            'today_messages': daily_stats.get('total_messages', 0),
            'status': 'online'
        })
    except Exception as e:
        logging.error(f"API stats error: {e}")
        return jsonify({'error': str(e)}), 500

def run_flask():
    """Запуск Flask в отдельном потоке"""
    flask_app.run(host='0.0.0.0', port=5000, debug=False)

# ===================== БОТ-ХЭНДЛЕРЫ =====================

def get_back_keyboard():
    """Клавиатура с кнопкой 'Назад'"""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔙 Назад", callback_data='main_menu')]
    ])

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /start"""
    user = update.effective_user
    user_id = user.id
    
    # Регистрация с рефералкой
    referrer_id = None
    if context.args and context.args[0].isdigit():
        referrer_id = int(context.args[0])
    
    db.register_user(
        user_id,
        user.username,
        user.first_name,
        user.last_name,
        referrer_id
    )
    
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
        [InlineKeyboardButton("📈 Экспорт данных", callback_data='export_menu')],
    ]
    
    if is_admin:
        keyboard.append([InlineKeyboardButton("👑 Админ-панель", callback_data='admin_panel')])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    status = "🔓 ПРЕМИУМ" if has_sub else "🔒 БЕСПЛАТНЫЙ"
    
    sessions = len(db.get_sessions(user_id))
    groups = len(db.get_groups(user_id))
    broadcasts = db.get_broadcast_history(user_id, 1)
    
    await update.message.reply_text(
        f"🍋 **LEMON SPREADER ULTRA v5.1**\n\n"
        f"👋 Привет, {user.first_name}!\n"
        f"📊 Статус: {status}\n"
        f"📱 Сессий: {sessions}\n"
        f"👥 Групп: {groups}\n"
        f"📤 Рассылок: {len(broadcasts)}\n"
        f"💰 Баланс: {(db.get_user(user_id) or {}).get('balance', 0)} ₽\n\n"
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

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Отмена текущей операции"""
    context.user_data.clear()
    await update.message.reply_text(
        "❌ Операция отменена.",
        reply_markup=get_back_keyboard()
    )
    return ConversationHandler.END

async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Главный обработчик всех callback'ов"""
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    data = query.data
    
    # Главное меню и навигация
    if data == 'main_menu':
        await start(update, context)
        return
    
    if data == 'back':
        await start(update, context)
        return
    
    # Сессии
    if data == 'my_sessions':
        await show_sessions(update, context, user_id)
        return
    
    if data.startswith('delete_session_'):
        session_id = int(data.split('_')[-1])
        db.delete_session(session_id)
        await query.edit_message_text(
            "✅ Сессия удалена!",
            reply_markup=get_back_keyboard()
        )
        return
    
    if data == 'add_session':
        await query.edit_message_text(
            "📱 **Добавление сессии**\n\n"
            "Введи номер телефона в формате:\n"
            "`+79123456789`\n\n"
            "Для отмены введи /cancel",
            parse_mode='Markdown'
        )
        return ADD_SESSION_PHONE
    
    # Группы
    if data == 'my_groups':
        await show_groups(update, context, user_id)
        return
    
    if data == 'import_groups':
        await import_groups_menu(update, context, user_id)
        return
    
    if data.startswith('import_groups_session_'):
        await import_groups_from_session(update, context, user_id, data)
        return
    
    # Рассылка
    if data == 'broadcast_start':
        await broadcast_select_session(update, context, user_id)
        return
    
    if data.startswith('broadcast_select_session_'):
        await broadcast_select_group(update, context, user_id, data)
        return
    
    if data.startswith('broadcast_select_group_'):
        group_id = data.split('_')[-1]
        context.user_data['broadcast_group_id'] = group_id
        await query.edit_message_text(
            "✍️ **Введи текст для рассылки:**\n\n"
            "Можно использовать HTML-разметку.\n"
            "Для отмены введи /cancel",
            parse_mode='Markdown'
        )
        return WAIT_MESSAGE
    
    # Шаблоны
    if data == 'templates_menu':
        await handle_templates_menu(update, context, user_id)
        return
    
    if data.startswith('template_use_'):
        template_id = int(data.split('_')[-1])
        templates = db.get_templates(user_id)
        template = next((t for t in templates if t['id'] == template_id), None)
        if template:
            context.user_data['broadcast_message'] = template['content']
            await query.edit_message_text(
                f"✅ Шаблон '{template['name']}' загружен!\n\n"
                f"📝 Содержание:\n{template['content'][:200]}...\n\n"
                f"Теперь выбери сессию для рассылки:",
                reply_markup=await get_sessions_keyboard(user_id, 'broadcast_select_session')
            )
        return
    
    # Медиа
    if data == 'media_library':
        await handle_media_library(update, context, user_id)
        return
    
    # Статистика
    if data == 'stats_menu':
        await handle_stats_menu(update, context, user_id)
        return
    
    # Подписка
    if data == 'subscription_info':
        await handle_subscription_info(update, context, user_id)
        return
    
    # Рефералы
    if data == 'referral_menu':
        await handle_referral_menu(update, context, user_id)
        return
    
    # Экспорт
    if data == 'export_menu':
        await handle_export_menu(update, context, user_id)
        return
    
    if data == 'export_csv':
        await handle_export_csv(update, context, user_id)
        return
    
    # Черный список
    if data == 'blacklist_menu':
        await handle_blacklist_menu(update, context, user_id)
        return
    
    # Расписание
    if data == 'schedule_broadcast':
        await handle_schedule_broadcast(update, context, user_id)
        return
    
    # Админка
    if data == 'admin_panel':
        await handle_admin_panel(update, context, user_id)
        return

# ========== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ==========

async def get_sessions_keyboard(user_id: int, prefix: str) -> InlineKeyboardMarkup:
    """Клавиатура со списком сессий"""
    sessions = db.get_sessions(user_id)
    keyboard = []
    for session in sessions:
        keyboard.append([
            InlineKeyboardButton(
                f"📱 {session['phone']}",
                callback_data=f'{prefix}_{session["id"]}'
            )
        ])
    if not sessions:
        keyboard.append([InlineKeyboardButton("❌ Нет сессий", callback_data='noop')])
    keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data='main_menu')])
    return InlineKeyboardMarkup(keyboard)

async def show_sessions(update: Update, context: ContextTypes.DEFAULT_TYPE, user_id: int):
    """Показать сессии пользователя"""
    query = update.callback_query
    sessions = db.get_sessions(user_id)
    
    if not sessions:
        text = "📱 **У вас нет активных сессий.**\n\nДобавьте через '➕ Добавить сессию'."
    else:
        text = "📱 **Ваши сессии:**\n\n"
        for session in sessions:
            stats = db.get_session_stats(session['id'])
            text += f"• {session['phone']} (ID: {session['id']}) — ✅ {stats.get('sent', 0)} ❌ {stats.get('failed', 0)}\n"
    
    keyboard = [
        [InlineKeyboardButton("➕ Добавить сессию", callback_data='add_session')],
    ]
    for session in sessions:
        keyboard.append([
            InlineKeyboardButton(
                f"🗑️ Удалить {session['phone']}",
                callback_data=f'delete_session_{session["id"]}'
            )
        ])
    keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data='main_menu')])
    
    await query.edit_message_text(
        text,
        parse_mode='Markdown',
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def show_groups(update: Update, context: ContextTypes.DEFAULT_TYPE, user_id: int):
    """Показать группы пользователя"""
    query = update.callback_query
    groups = db.get_groups(user_id)
    
    if not groups:
        text = "👥 **У вас нет сохранённых групп.**\n\nДобавьте через импорт."
    else:
        text = f"👥 **Ваши группы ({len(groups)}):**\n\n"
        for group in groups[:20]:
            text += f"• {group['group_name'][:40]}\n"
        if len(groups) > 20:
            text += f"\n... и еще {len(groups)-20} групп."
    
    keyboard = [
        [InlineKeyboardButton("📥 Импортировать группы", callback_data='import_groups')],
        [InlineKeyboardButton("🔙 Назад", callback_data='main_menu')],
    ]
    
    await query.edit_message_text(
        text,
        parse_mode='Markdown',
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def import_groups_menu(update: Update, context: ContextTypes.DEFAULT_TYPE, user_id: int):
    """Меню импорта групп"""
    query = update.callback_query
    sessions = db.get_sessions(user_id)
    
    if not sessions:
        await query.edit_message_text(
            "❌ Сначала добавьте сессию.",
            reply_markup=get_back_keyboard()
        )
        return
    
    keyboard = []
    for session in sessions:
        keyboard.append([
            InlineKeyboardButton(
                f"📱 {session['phone']}",
                callback_data=f'import_groups_session_{session["id"]}'
            )
        ])
    keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data='my_groups')])
    
    await query.edit_message_text(
        "📥 **Импорт групп**\n\n"
        "Выберите сессию для импорта групп:",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode='Markdown'
    )

async def import_groups_from_session(update: Update, context: ContextTypes.DEFAULT_TYPE,
                                     user_id: int, callback_data: str):
    """Импорт групп из сессии"""
    query = update.callback_query
    session_id = int(callback_data.split('_')[-1])
    
    sessions = db.get_sessions(user_id)
    session = next((s for s in sessions if s['id'] == session_id), None)
    if not session:
        await query.edit_message_text(
            "❌ Сессия не найдена.",
            reply_markup=get_back_keyboard()
        )
        return
    
    await query.edit_message_text("⏳ Получение списка групп...")
    
    # Получаем группы через Telethon
    client = await session_manager.get_client(session_id, session['session_string'])
    if not client:
        await query.edit_message_text(
            "❌ Не удалось подключиться к сессии.",
            reply_markup=get_back_keyboard()
        )
        return
    
    try:
        dialogs = await client(GetDialogsRequest(
            offset_date=None,
            offset_id=0,
            offset_peer=InputPeerChannel(0, 0),
            limit=100,
            hash=0
        ))
        
        groups = []
        for dialog in dialogs.dialogs:
            if dialog.is_channel or dialog.is_group:
                entity = dialog.entity
                group_id = str(getattr(entity, 'id', ''))
                group_name = getattr(entity, 'title', 'Без названия')
                group_hash = str(getattr(entity, 'access_hash', ''))
                if group_id and group_hash:
                    groups.append({
                        'group_id': group_id,
                        'group_name': group_name,
                        'group_hash': group_hash
                    })
        
        # Сохраняем группы
        count = 0
        for group in groups:
            if db.add_group(
                group['group_id'],
                group['group_name'],
                group['group_hash'],
                session_id,
                user_id
            ):
                count += 1
        
        await query.edit_message_text(
            f"✅ **Импортировано групп:** {count}\n\n"
            f"📱 Сессия: {session['phone']}\n"
            f"👥 Всего групп: {len(db.get_groups(user_id))}",
            reply_markup=get_back_keyboard(),
            parse_mode='Markdown'
        )
        
    except Exception as e:
        await query.edit_message_text(
            f"❌ Ошибка импорта: {str(e)}",
            reply_markup=get_back_keyboard()
        )

async def broadcast_select_session(update: Update, context: ContextTypes.DEFAULT_TYPE, user_id: int):
    """Выбор сессии для рассылки"""
    query = update.callback_query
    sessions = db.get_sessions(user_id)
    
    if not sessions:
        await query.edit_message_text(
            "❌ У вас нет активных сессий.\n\n"
            "Сначала добавьте сессию через 'Мои сессии'.",
            reply_markup=get_back_keyboard()
        )
        return
    
    keyboard = []
    for session in sessions:
        stats = db.get_session_stats(session['id'])
        status = "✅" if stats.get('sent', 0) > 0 else "⚪"
        keyboard.append([
            InlineKeyboardButton(
                f"{status} {session['phone']} (✅{stats.get('sent', 0)})",
                callback_data=f'broadcast_select_session_{session["id"]}'
            )
        ])
    keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data='main_menu')])
    
    await query.edit_message_text(
        "📤 **Выберите сессию для рассылки:**",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode='Markdown'
    )

async def broadcast_select_group(update: Update, context: ContextTypes.DEFAULT_TYPE,
                                 user_id: int, callback_data: str):
    """Выбор группы для рассылки"""
    query = update.callback_query
    session_id = int(callback_data.split('_')[-1])
    context.user_data['broadcast_session_id'] = session_id
    
    groups = db.get_groups(user_id)
    if not groups:
        await query.edit_message_text(
            "⚠️ **Нет сохранённых групп!**\n\n"
            "Сначала импортируйте группы через 'Мои группы'.",
            parse_mode='Markdown',
            reply_markup=get_back_keyboard()
        )
        return
    
    keyboard = []
    for group in groups[:20]:
        keyboard.append([
            InlineKeyboardButton(
                f"📢 {group['group_name'][:30]}",
                callback_data=f'broadcast_select_group_{group["group_id"]}'
            )
        ])
    keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data='broadcast_start')])
    
    await query.edit_message_text(
        f"📤 **Выберите группу для рассылки:**\n"
        f"Всего групп: {len(groups)}",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode='Markdown'
    )

async def handle_schedule_broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE, user_id: int):
    """Обработчик планирования рассылки"""
    query = update.callback_query
    
    keyboard = [
        [InlineKeyboardButton("📅 На сегодня", callback_data='schedule_today')],
        [InlineKeyboardButton("📅 На завтра", callback_data='schedule_tomorrow')],
        [InlineKeyboardButton("🔄 Ежедневно", callback_data='schedule_daily')],
        [InlineKeyboardButton("📆 Еженедельно", callback_data='schedule_weekly')],
        [InlineKeyboardButton("📅 Ежемесячно", callback_data='schedule_monthly')],
        [InlineKeyboardButton("📋 Мои задачи", callback_data='scheduled_list')],
        [InlineKeyboardButton("🔙 Назад", callback_data='main_menu')],
    ]
    
    await query.edit_message_text(
        "⏰ **Запланированная рассылка**\n\n"
        "Выберите периодичность:",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode='Markdown'
    )

async def handle_templates_menu(update: Update, context: ContextTypes.DEFAULT_TYPE, user_id: int):
    """Обработчик меню шаблонов"""
    query = update.callback_query
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
    
    await query.edit_message_text(
        text,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode='Markdown'
    )

async def handle_media_library(update: Update, context: ContextTypes.DEFAULT_TYPE, user_id: int):
    """Обработчик медиа-библиотеки"""
    query = update.callback_query
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
    
    await query.edit_message_text(
        text,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode='Markdown'
    )

async def handle_blacklist_menu(update: Update, context: ContextTypes.DEFAULT_TYPE, user_id: int):
    """Обработчик черного списка"""
    query = update.callback_query
    
    keyboard = [
        [InlineKeyboardButton("🚫 Заблокировать группу", callback_data='blacklist_add_group')],
        [InlineKeyboardButton("🚫 Заблокировать пользователя", callback_data='blacklist_add_user')],
        [InlineKeyboardButton("📋 Список блокировок", callback_data='blacklist_list')],
        [InlineKeyboardButton("🔙 Назад", callback_data='main_menu')],
    ]
    
    await query.edit_message_text(
        "🚫 **Черный список**\n\n"
        "Управление заблокированными группами и пользователями.",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode='Markdown'
    )

async def handle_stats_menu(update: Update, context: ContextTypes.DEFAULT_TYPE, user_id: int):
    """Обработчик статистики"""
    query = update.callback_query
    
    sessions = db.get_sessions(user_id)
    total_sent = 0
    total_failed = 0
    groups_count = len(db.get_groups(user_id))
    
    for session in sessions:
        stats = db.get_session_stats(session['id'])
        total_sent += stats.get('sent', 0)
        total_failed += stats.get('failed', 0)
    
    success_rate = 0
    if total_sent + total_failed > 0:
        success_rate = round(total_sent / (total_sent + total_failed) * 100)
    
    text = (
        f"📊 **Ваша статистика**\n\n"
        f"📱 Сессий: {len(sessions)}\n"
        f"👥 Групп: {groups_count}\n"
        f"✅ Успешных рассылок: {total_sent}\n"
        f"❌ Неудачных: {total_failed}\n"
        f"📈 Успешность: {success_rate}%\n"
        f"💰 Баланс: {(db.get_user(user_id) or {}).get('balance', 0)} ₽\n\n"
        f"📋 Последние 5 рассылок:"
    )
    
    history = db.get_broadcast_history(user_id, 5)
    for item in history:
        status = "✅" if item['status'] == 'sent' else "❌"
        text += f"\n{status} {item['send_date'][:10]} → {item.get('phone', 'unknown')}"
    
    keyboard = [
        [InlineKeyboardButton("📥 Экспорт CSV", callback_data='export_csv')],
        [InlineKeyboardButton("🔙 Назад", callback_data='main_menu')],
    ]
    
    await query.edit_message_text(
        text,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode='Markdown'
    )

async def handle_subscription_info(update: Update, context: ContextTypes.DEFAULT_TYPE, user_id: int):
    """Обработчик информации о подписке"""
    query = update.callback_query
    has_sub = db.has_subscription(user_id)
    
    if has_sub:
        text = "💎 **У вас активна премиум-подписка!**\n\n✅ Рассылка без меток\n✅ Приоритетная поддержка\n✅ Безлимитные сессии"
    else:
        text = (
            "🔒 **У вас бесплатный тариф**\n\n"
            "⚠️ В каждом сообщении будет метка:\n"
            "`📨 Отправлено через @test`\n\n"
            "💎 **Премиум подписка:**\n"
            f"• 1 месяц — {PRICE_MONTH} ₽\n"
            f"• 1 год — {PRICE_YEAR} ₽\n"
            f"• Навсегда — {PRICE_LIFETIME} ₽\n\n"
            "Оплата через админа: @promtikdeepseek"
        )
    
    await query.edit_message_text(
        text,
        parse_mode='Markdown',
        reply_markup=get_back_keyboard()
    )

async def handle_referral_menu(update: Update, context: ContextTypes.DEFAULT_TYPE, user_id: int):
    """Обработчик реферальной системы"""
    query = update.callback_query
    user = db.get_user(user_id)
    
    bot_username = (await context.bot.get_me()).username
    referral_link = f"https://t.me/{bot_username}?start={user_id}"
    
    text = (
        f"🎁 **Реферальная система**\n\n"
        f"Приглашай друзей и зарабатывай!\n\n"
        f"🔗 Твоя ссылка:\n`{referral_link}`\n\n"
        f"💰 Заработано: {(user or {}).get('balance', 0)} ₽\n\n"
        f"⚡ За каждого приглашенного друга ты получаешь 10% от его пополнений!"
    )
    
    keyboard = [
        [InlineKeyboardButton("🔙 Назад", callback_data='main_menu')],
    ]
    
    await query.edit_message_text(
        text,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode='Markdown'
    )

async def handle_export_menu(update: Update, context: ContextTypes.DEFAULT_TYPE, user_id: int):
    """Обработчик экспорта данных"""
    query = update.callback_query
    
    keyboard = [
        [InlineKeyboardButton("📊 CSV-отчет", callback_data='export_csv')],
        [InlineKeyboardButton("📋 Экспорт сессий", callback_data='export_sessions')],
        [InlineKeyboardButton("🔙 Назад", callback_data='main_menu')],
    ]
    
    await query.edit_message_text(
        "📈 **Экспорт данных**\n\n"
        "Выберите формат выгрузки:",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode='Markdown'
    )

async def handle_export_csv(update: Update, context: ContextTypes.DEFAULT_TYPE, user_id: int):
    """Обработчик экспорта в CSV"""
    query = update.callback_query
    await query.edit_message_text("⏳ Генерация отчета...")
    
    try:
        filename = db.export_stats_csv(user_id)
        
        with open(filename, 'rb') as f:
            await query.message.reply_document(
                document=InputFile(f, filename=os.path.basename(filename)),
                caption="📊 Ваш отчет готов!"
            )
        
        await query.edit_message_text(
            "✅ Отчет успешно сгенерирован!",
            reply_markup=get_back_keyboard()
        )
    except Exception as e:
        await query.edit_message_text(
            f"❌ Ошибка: {str(e)}",
            reply_markup=get_back_keyboard()
        )

async def handle_admin_panel(update: Update, context: ContextTypes.DEFAULT_TYPE, user_id: int):
    """Обработчик админ-панели"""
    query = update.callback_query
    
    if user_id not in ADMIN_IDS:
        await query.edit_message_text(
            "⛔ Доступ запрещен.",
            reply_markup=get_back_keyboard()
        )
        return
    
    # Получаем общую статистику
    all_sessions = db.get_sessions()
    all_groups = db.get_groups()
    total_users = len(set(s.get('added_by', 0) for s in all_sessions))
    
    text = (
        f"👑 **Админ-панель**\n\n"
        f"📊 Общая статистика:\n"
        f"👥 Пользователей: {total_users}\n"
        f"📱 Сессий: {len(all_sessions)}\n"
        f"👥 Групп: {len(all_groups)}\n"
        f"💰 Доход: {sum(u.get('balance', 0) for u in [db.get_user(s.get('added_by', 0)) for s in all_sessions])} ₽\n"
    )
    
    keyboard = [
        [InlineKeyboardButton("📊 Статистика системы", callback_data='admin_system_stats')],
        [InlineKeyboardButton("👥 Все пользователи", callback_data='admin_users_list')],
        [InlineKeyboardButton("📱 Все сессии", callback_data='admin_sessions_list')],
        [InlineKeyboardButton("🔙 Назад", callback_data='main_menu')],
    ]
    
    await query.edit_message_text(
        text,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode='Markdown'
    )

async def handle_broadcast_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик текста для рассылки"""
    user_id = update.effective_user.id
    message_text = update.message.text
    
    session_id = context.user_data.get('broadcast_session_id')
    group_id = context.user_data.get('broadcast_group_id')
    
    if not session_id or not group_id:
        await update.message.reply_text(
            "❌ Сессия или группа не выбраны. Начните заново /start",
            reply_markup=get_back_keyboard()
        )
        return ConversationHandler.END
    
    # Проверяем подписку
    has_sub = db.has_subscription(user_id)
    if not has_sub:
        message_text += "\n\n📨 Отправлено через @test"
    
    # Сохраняем рассылку
    broadcast_id = db.add_broadcast(session_id, group_id, message_text)
    
    # Получаем данные
    sessions = db.get_sessions(user_id)
    session = next((s for s in sessions if s['id'] == session_id), None)
    if not session:
        await update.message.reply_text("❌ Сессия не найдена.")
        return ConversationHandler.END
    
    groups = db.get_groups(user_id)
    group = next((g for g in groups if g['group_id'] == group_id), None)
    if not group:
        await update.message.reply_text("❌ Группа не найдена.")
        return ConversationHandler.END
    
    # Отправляем
    await update.message.reply_text("⏳ Отправка сообщения...")
    
    success = await session_manager.send_message_with_delay(
        session_id,
        session['session_string'],
        group_id,
        group['group_hash'],
        message_text,
        broadcast_id
    )
    
    if success:
        await update.message.reply_text(
            f"✅ **Сообщение успешно отправлено!**\n\n"
            f"📱 Сессия: {session['phone']}\n"
            f"👥 Группа: {group['group_name']}\n"
            f"📝 Текст: {message_text[:100]}...\n\n"
            f"💡 Статус: {'🔓 Премиум' if has_sub else '🔒 Бесплатный'}",
            parse_mode='Markdown',
            reply_markup=get_back_keyboard()
        )
    else:
        await update.message.reply_text(
            "❌ **Ошибка отправки.**\n\n"
            "Проверьте сессию и группу.\n"
            "Подробности в истории рассылок.",
            reply_markup=get_back_keyboard()
        )
    
    context.user_data.pop('broadcast_session_id', None)
    context.user_data.pop('broadcast_group_id', None)
    return ConversationHandler.END

# ===================== ОСНОВНАЯ ФУНКЦИЯ =====================

def main():
    """Главная функция запуска бота"""
    
    # Проверка конфигурации
    if not BOT_TOKEN or BOT_TOKEN == "8901120783:AAHxSXhhpPk-BAsYRqiPAMKCdbICR9cCBzo":
        logging.error("⚠️ BOT_TOKEN не настроен!")
        return
    
    if ADMIN_IDS == [8562897889]:
        logging.warning("⚠️ ADMIN_IDS не настроен! Используйте свой Telegram ID.")
    
    # Запуск Flask в отдельном потоке
    flask_thread = Thread(target=run_flask, daemon=True)
    flask_thread.start()
    
    # Инициализация бота
    application = Application.builder().token(BOT_TOKEN).build()
    
    # Регистрируем обработчики команд
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("cancel", cancel))
    
    # Конверсация для добавления сессии
    session_conv = ConversationHandler(
        entry_points=[
            CallbackQueryHandler(handle_callback, pattern='^add_session$')
        ],
        states={
            ADD_SESSION_PHONE: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, 
                               lambda u, c: u.message.reply_text("📱 Введите код подтверждения:"))
            ],
            ADD_SESSION_CODE: [
                MessageHandler(filters.TEXT & ~filters.COMMAND,
                               lambda u, c: u.message.reply_text("✅ Сессия добавлена!"))
            ],
        },
        fallbacks=[CommandHandler("cancel", cancel)]
    )
    application.add_handler(session_conv)
    
    # Конверсация для рассылки
    broadcast_conv = ConversationHandler(
        entry_points=[
            CallbackQueryHandler(handle_callback, pattern='^broadcast_start$')
        ],
        states={
            WAIT_MESSAGE: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_broadcast_message)
            ],
        },
        fallbacks=[CommandHandler("cancel", cancel)]
    )
    application.add_handler(broadcast_conv)
    
    # Главный обработчик callback'ов
    application.add_handler(CallbackQueryHandler(handle_callback))
    
    # Запускаем планировщик
    scheduler_manager.scheduler.start()
    
    # Запускаем периодическую очистку клиентов
    async def cleanup_task():
        while True:
            await asyncio.sleep(300)  # 5 минут
            await session_manager.cleanup_idle_clients()
    
    # Запускаем задачу очистки
    loop = asyncio.get_event_loop()
    loop.create_task(cleanup_task())
    
    print("🍋 LEMON SPREADER ULTRA v5.1 ЗАПУЩЕН!")
    print("💀 ВСЕ ФУНКЦИИ АКТИВИРОВАНЫ!")
    print("🌐 Веб-интерфейс: http://localhost:5000")
    print(f"👑 Админы: {ADMIN_IDS}")
    
    application.run_polling()

if __name__ == "__main__":
    # Настройка логирования
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(f"{LOG_DIR}/bot.log"),
            logging.StreamHandler()
        ]
    )
    
    # Запуск
    try:
        main()
    except KeyboardInterrupt:
        print("\n🛑 Остановка бота...")
        asyncio.run(session_manager.close_all())
        db_pool.close_all()
        print("✅ Бот остановлен.")
    except Exception as e:
        logging.error(f"Fatal error: {e}")
        raise
