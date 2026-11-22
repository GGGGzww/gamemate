import asyncio
import hashlib
import html as htmlmod
from html.parser import HTMLParser
import json
import re
import urllib.request
import xml.etree.ElementTree as ET
import hmac
import logging
import os
import random
import secrets
import string
import time
import contextlib
import sqlite3
import threading
from fastapi import FastAPI, HTTPException, Request
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
import socketio
import uvicorn

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("gamemate")

MAX_MEMBERS = 5
MAX_ROOMS = 200
RATE_LIMITS = {
    "room:create": (3, 60),
    "room:join": (10, 60),
    "room:chat": (10, 10),
}
JOIN_LOCK_MINUTES = 5
JOIN_LOCK_MAX_FAILS = 5

def _load_dotenv(path=".env"):
    if not os.path.isfile(path):
        return
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


_load_dotenv()

# 最高权限管理员配置（仅从环境变量或本地 .env 读取，绝不写死在代码中）
DEFAULT_ADMIN_USER = os.environ.get("GAMEMATE_ADMIN_USER", "").strip()
DEFAULT_ADMIN_PWD = os.environ.get("GAMEMATE_ADMIN_PWD", "").strip()
ADMIN_USERS = {DEFAULT_ADMIN_USER} if DEFAULT_ADMIN_USER else set()


def _is_admin(username: str) -> bool:
    return bool(username and username in ADMIN_USERS)

# 初始化 SQLite 数据库（支持高并发 WAL 读写分离、锁超时自动排队、内存页缓存与线程安全事务锁）
db_lock = threading.RLock()
conn = sqlite3.connect(os.environ.get("GAMEMATE_DB", "gamemate.db"), timeout=15.0, check_same_thread=False)
conn.execute("PRAGMA journal_mode=WAL;")
conn.execute("PRAGMA synchronous=NORMAL;")
conn.execute("PRAGMA busy_timeout=10000;")
conn.execute("PRAGMA cache_size=-32000;")
conn.execute("PRAGMA temp_store=MEMORY;")
conn.execute("PRAGMA mmap_size=134217728;")
conn.execute("PRAGMA wal_autocheckpoint=1000;")


@contextlib.contextmanager
def db_write():
    """线程安全的原子写事务上下文：并发写时自动排队，异常时自动回滚并释放写锁。"""
    with db_lock:
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
conn.execute("""
             CREATE TABLE IF NOT EXISTS posts (
                                                  id TEXT PRIMARY KEY,
                                                  category TEXT,
                                                  title TEXT,
                                                  author TEXT,
                                                  content TEXT,
                                                  likes INTEGER DEFAULT 0,
                                                  created_at DATETIME DEFAULT CURRENT_TIMESTAMP
             )
             """)
# 点赞记录：(帖子, 点赞人) 唯一，一人一帖只能赞一次；点赞人是前端 localStorage 里的持久 key
conn.execute("""
             CREATE TABLE IF NOT EXISTS post_likes (
                                                  post_id TEXT,
                                                  voter TEXT,
                                                  PRIMARY KEY (post_id, voter)
             )
             """)
# 账号登录：用户名唯一、密码哈希存库；会话 token 存库，30 天过期
conn.execute("""
             CREATE TABLE IF NOT EXISTS users (
                                                  username TEXT PRIMARY KEY,
                                                  password_hash TEXT,
                                                  password_salt TEXT,
                                                  created_at DATETIME DEFAULT CURRENT_TIMESTAMP
             )
             """)
conn.execute("""
             CREATE TABLE IF NOT EXISTS sessions (
                                                  token TEXT PRIMARY KEY,
                                                  username TEXT,
                                                  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                                                  expires_at DATETIME
             )
             """)
# 仅在配置了环境变量或 .env 时自动同步管理员密码哈希
if DEFAULT_ADMIN_USER and DEFAULT_ADMIN_PWD:
    _admin_salt = secrets.token_hex(8)
    _admin_hash = hashlib.sha256((_admin_salt + DEFAULT_ADMIN_PWD).encode("utf-8")).hexdigest()
    conn.execute(
        """
        INSERT INTO users (username, password_hash, password_salt) VALUES (?, ?, ?)
        ON CONFLICT(username) DO UPDATE SET password_hash = excluded.password_hash, password_salt = excluded.password_salt
        """,
        (DEFAULT_ADMIN_USER, _admin_hash, _admin_salt),
    )
    conn.commit()
else:
    log.warning("未配置 GAMEMATE_ADMIN_USER / GAMEMATE_ADMIN_PWD，未启用默认管理员账号")
conn.execute("CREATE TABLE IF NOT EXISTS site_settings (key TEXT PRIMARY KEY, val TEXT)")
conn.execute("INSERT OR IGNORE INTO site_settings (key, val) VALUES ('logo_version', '1')")
conn.commit()
# 老库迁移：posts 补浏览/回复数/图片字段（没有才加），新建回帖表
_post_cols = {r[1] for r in conn.execute("PRAGMA table_info(posts)")}
for _col, _ddl in [
    ("views", "ALTER TABLE posts ADD COLUMN views INTEGER DEFAULT 0"),
    ("reply_count", "ALTER TABLE posts ADD COLUMN reply_count INTEGER DEFAULT 0"),
    ("last_reply_at", "ALTER TABLE posts ADD COLUMN last_reply_at DATETIME"),
    ("images", "ALTER TABLE posts ADD COLUMN images TEXT DEFAULT '[]'"),
]:
    if _col not in _post_cols:
        conn.execute(_ddl)
conn.execute("""
             CREATE TABLE IF NOT EXISTS replies (
                                                  id TEXT PRIMARY KEY,
                                                  post_id TEXT,
                                                  author TEXT,
                                                  content TEXT,
                                                  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                                                  images TEXT DEFAULT '[]'
             )
             """)
_reply_cols = {r[1] for r in conn.execute("PRAGMA table_info(replies)")}
if "images" not in _reply_cols:
    conn.execute("ALTER TABLE replies ADD COLUMN images TEXT DEFAULT '[]'")
# 为个人中心与高频列表排序建立复合索引（避免大体积 images 字段引发全表扫描）
conn.execute("CREATE INDEX IF NOT EXISTS idx_posts_author ON posts(author);")
conn.execute("CREATE INDEX IF NOT EXISTS idx_posts_created ON posts(created_at DESC);")
conn.execute("CREATE INDEX IF NOT EXISTS idx_posts_cat_created ON posts(category, created_at DESC);")
conn.execute("CREATE INDEX IF NOT EXISTS idx_posts_cat_likes ON posts(category, likes DESC, created_at DESC);")
conn.execute("CREATE INDEX IF NOT EXISTS idx_replies_author ON replies(author);")
conn.execute("CREATE INDEX IF NOT EXISTS idx_replies_post_id ON replies(post_id);")
conn.execute("CREATE INDEX IF NOT EXISTS idx_replies_post_created ON replies(post_id, created_at ASC);")

conn.execute("""
             CREATE TABLE IF NOT EXISTS news (
                                                  url TEXT PRIMARY KEY,
                                                  source TEXT,
                                                  title TEXT,
                                                  summary TEXT,
                                                  published_at TEXT,
                                                  fetched_at DATETIME DEFAULT CURRENT_TIMESTAMP
             )
             """)
# 资讯正文缓存：老库补 content 列（没有才加），站内阅读抓一次后存库
_news_cols = {r[1] for r in conn.execute("PRAGMA table_info(news)")}
if "content" not in _news_cols:
    conn.execute("ALTER TABLE news ADD COLUMN content TEXT")
conn.execute("""
             CREATE TABLE IF NOT EXISTS addons (
                                                  id TEXT PRIMARY KEY,
                                                  name TEXT,
                                                  version TEXT,
                                                  game_version TEXT,
                                                  author TEXT,
                                                  description TEXT,
                                                  download_url TEXT,
                                                  likes INTEGER DEFAULT 0,
                                                  created_at DATETIME DEFAULT CURRENT_TIMESTAMP
             )
             """)
conn.execute("""
             CREATE TABLE IF NOT EXISTS addon_likes (
                                                  addon_id TEXT,
                                                  voter TEXT,
                                                  PRIMARY KEY (addon_id, voter)
             )
             """)
_addon_cols = {r[1] for r in conn.execute("PRAGMA table_info(addons)")}
if "downloads" not in _addon_cols:
    conn.execute("ALTER TABLE addons ADD COLUMN downloads INTEGER DEFAULT 0")

# 配装模拟方案表：支持玩家保存与分享自己的专精属性配装与模拟 DPS
conn.execute("""
             CREATE TABLE IF NOT EXISTS sim_builds (
                                                  id TEXT PRIMARY KEY,
                                                  title TEXT,
                                                  author TEXT,
                                                  spec_key TEXT,
                                                  spec_name TEXT,
                                                  ilvl INTEGER DEFAULT 626,
                                                  main_stat INTEGER DEFAULT 1200,
                                                  crit REAL DEFAULT 30,
                                                  haste REAL DEFAULT 20,
                                                  mastery REAL DEFAULT 25,
                                                  vers REAL DEFAULT 10,
                                                  dps INTEGER DEFAULT 0,
                                                  created_at DATETIME DEFAULT CURRENT_TIMESTAMP
             )
             """)
_DEFAULT_BUILDS = [
    ("build_ret_bis", "泽风岛 4件套毕业惩戒骑（暴击精通流）", "灰烬使者", "ret_paladin", "⚔️ 惩戒圣骑士", 636, 1580, 36.0, 22.0, 38.0, 12.0, 48920),
    ("build_fire_aoe", "大秘境 12层冲层火法（高急速流畅阈值）", "达拉然学徒", "fire_mage", "🔥 火焰法师", 632, 1510, 42.0, 31.0, 24.0, 9.0, 51240),
    ("build_fury_raid", "团本单体木桩狂暴战（急速精通双修）", "萨鲁法尔", "fury_warrior", "🪓 狂暴战士", 634, 1550, 32.0, 29.0, 35.0, 11.0, 47680),
]
for _bid, _btitle, _bauth, _bspec, _bspec_name, _bilvl, _bmain, _bcrit, _bhaste, _bmast, _bvers, _bdps in _DEFAULT_BUILDS:
    conn.execute(
        """
        INSERT OR IGNORE INTO sim_builds (id, title, author, spec_key, spec_name, ilvl, main_stat, crit, haste, mastery, vers, dps)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (_bid, _btitle, _bauth, _bspec, _bspec_name, _bilvl, _bmain, _bcrit, _bhaste, _bmast, _bvers, _bdps),
    )

# 默认预置主流官方插件平台与核心开荒插件（若库中尚无对应记录则自动补齐）
_DEFAULT_ADDONS = [
    (
        "addon_dd",
        "网易DD (原网易有爱)",
        "官方最新版",
        "全版本通用",
        "网易官方",
        "网易官方魔兽插件管理器与开黑语音平台，支持一键安装整合包、单体插件管理、露露缇娅等大神配置订阅及云端同步。",
        "https://dd.163.com/",
        328,
        8650,
    ),
    (
        "addon_bigfoot",
        "大脚插件 (BigFoot)",
        "官方最新版",
        "全版本通用",
        "178大脚官方",
        "国服经典老牌魔兽世界整合插件，内置任务指引、首领报警、伤害统计、背包整合与拍卖行助手，适合开箱即用。",
        "https://bigfoot.178.com/",
        276,
        7420,
    ),
    (
        "addon_heybox",
        "黑盒工坊 (小黑盒插件助手)",
        "官方最新版",
        "全版本通用",
        "小黑盒官方",
        "支持单体插件极速更新、WA 字符串库检索、名人分享配置一键导入以及本地 WTF 配置自动云备份。",
        "https://workshop.xiaoheihe.cn/",
        245,
        6890,
    ),
    (
        "addon_wa",
        "WeakAuras (WA 技能与光环监控)",
        "5.20.1",
        "无限 / 11.0",
        "CurseForge官方源",
        "魔兽世界最核心的自定义监控框架，支持导入首领时间轴、职业核心技能冷却与团队减伤提醒字符串。",
        "https://www.curseforge.com/wow/addons/weakauras-2",
        168,
        4240,
    ),
    (
        "addon_dbm",
        "Deadly Boss Mods (DBM 首领警报)",
        "11.0.8",
        "无限 / 11.0",
        "CurseForge官方源",
        "团队副本与五人地下城开荒必备首领技能计时条与夏一可等中文语音警报支持。",
        "https://www.curseforge.com/wow/addons/deadly-boss-mods",
        154,
        3980,
    ),
]
for _aid, _name, _ver, _gver, _auth, _desc, _url, _likes, _dls in _DEFAULT_ADDONS:
    conn.execute(
        """
        INSERT OR IGNORE INTO addons (id, name, version, game_version, author, description, download_url, likes, downloads)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (_aid, _name, _ver, _gver, _auth, _desc, _url, _likes, _dls),
    )
conn.commit()
# 自动清理已失效的 wbox 插件记录并同步修正黑盒工坊官方下载链接
conn.execute("DELETE FROM addon_likes WHERE addon_id = 'addon_wbox'")
conn.execute("DELETE FROM addons WHERE id = 'addon_wbox' OR download_url LIKE '%wbox.top%'")
conn.execute("UPDATE addons SET download_url = 'https://workshop.xiaoheihe.cn/' WHERE id = 'addon_heybox' OR download_url LIKE '%xiaoheihe.cn/app/workshop%'")
conn.execute("CREATE INDEX IF NOT EXISTS idx_news_pub ON news(published_at DESC, fetched_at DESC);")
conn.execute("CREATE INDEX IF NOT EXISTS idx_news_src_pub ON news(source, published_at DESC, fetched_at DESC);")
conn.execute("CREATE INDEX IF NOT EXISTS idx_addons_sort ON addons(downloads DESC, likes DESC, created_at DESC);")
conn.execute("CREATE INDEX IF NOT EXISTS idx_addons_author ON addons(author, created_at DESC);")
conn.execute("CREATE INDEX IF NOT EXISTS idx_sim_builds_spec ON sim_builds(spec_key, dps DESC);")
conn.execute("CREATE INDEX IF NOT EXISTS idx_sim_builds_created ON sim_builds(created_at DESC);")
conn.commit()

app = FastAPI(title="GameMate.fun Backend")
# 收紧 CORS：只允许本站与本地开发来源
sio = socketio.AsyncServer(
    async_mode="asgi",
    cors_allowed_origins=[
        "https://gamemate.fun",
        "http://gamemate.fun",
        "http://localhost:3000",
        "http://127.0.0.1:3000",
    ],
)
socket_app = socketio.ASGIApp(sio, other_asgi_app=app)

active_rooms = {}
sid_to_client = {}
client_to_room = {}
client_to_sid = {}
_rate_buckets = {}
_join_failures = {}
_room_lock = asyncio.Lock()


@app.get("/health")
async def health():
    return {"ok": True, "rooms": len(active_rooms)}


def _data(data):
    """Socket handler 入参判空：没带 payload 或不是 dict 时返回空 dict。"""
    return data if isinstance(data, dict) else {}


def _client_id(sid, data):
    cid = str((data or {}).get("clientId") or "").strip()[:36]
    if not cid:
        cid = sid_to_client.get(sid) or sid
    sid_to_client[sid] = cid
    client_to_sid[cid] = sid
    return cid


def _room_of_client(cid):
    room_id = client_to_room.get(cid)
    return active_rooms.get(room_id) if room_id else None


def _check_rate(sid, action):
    limit, window = RATE_LIMITS[action]
    now = time.time()
    key = (sid, action)
    stamps = [t for t in _rate_buckets.get(key, []) if now - t < window]
    if len(stamps) >= limit:
        _rate_buckets[key] = stamps
        return False
    stamps.append(now)
    _rate_buckets[key] = stamps
    return True


def _hash_password(password, salt):
    return hashlib.sha256((salt + password).encode("utf-8")).hexdigest()


def _check_password(room, input_pwd):
    """密码只存哈希 + 每房独立 salt；比对用 hmac.compare_digest 防时序攻击。"""
    salt = room.get("password_salt") or ""
    expected = room.get("password_hash") or ""
    if not expected:
        return True
    return hmac.compare_digest(_hash_password(input_pwd or "", salt), expected)


def get_public_rooms():
    public_list = []
    for r in active_rooms.values():
        if r.get("isHidden"):
            continue
        public_list.append({
            "id": r["id"],
            "title": r["title"],
            "faction": r["faction"],
            "leader": r["leader"],
            "maxMembers": r["maxMembers"],
            "memberCount": len(r["members"]),
            "isLocked": bool(r.get("password_hash")),
        })
    return public_list


def get_room_detail(room):
    return {
        "id": room["id"],
        "title": room["title"],
        "faction": room["faction"],
        "leader": room["leader"],
        "leaderId": room["leaderId"],
        "maxMembers": room["maxMembers"],
        "isLocked": bool(room.get("password_hash")),
        "isHidden": bool(room.get("isHidden")),
        "members": room["members"],
    }


# --- 论坛 API ---
class PostReq(BaseModel):
    category: str
    title: str
    author: str = ""
    content: str = ""
    images: list[str] = []


class LikeReq(BaseModel):
    voter: str = ""


FORUM_CATEGORIES = {"综合讨论", "开荒组队", "欧皇晒图", "Bug反馈", "攻略心得", "开荒战报"}
FORUM_CATEGORY_ORDER = ["综合讨论", "开荒组队", "攻略心得", "开荒战报", "欧皇晒图", "Bug反馈"]


_POST_COLS = "id, category, title, author, content, likes, created_at, views, reply_count, last_reply_at, images"


def _parse_images(raw):
    if not raw:
        return []
    try:
        arr = json.loads(raw)
        return arr if isinstance(arr, list) else []
    except Exception:
        return []


def _sanitize_images(imgs, max_count=4, max_len=900_000):
    if not imgs or not isinstance(imgs, list):
        return []
    cleaned = []
    for item in imgs[:max_count]:
        if not isinstance(item, str):
            continue
        s = item.strip()
        if len(s) > max_len:
            raise HTTPException(status_code=400, detail="单张图片体积过大，请压缩后再试")
        if s.startswith("data:image/") and ";base64," in s:
            cleaned.append(s)
        elif s.startswith("https://") or s.startswith("http://"):
            cleaned.append(s[:500])
    return cleaned


def _post_dict(row):
    return {
        "id": row[0], "category": row[1], "title": row[2], "author": row[3],
        "content": row[4], "likes": row[5], "createdAt": row[6],
        "views": row[7] or 0, "replyCount": row[8] or 0, "lastReplyAt": row[9],
        "images": _parse_images(row[10] if len(row) > 10 else "[]"),
    }


def _reply_dict(row):
    return {
        "id": row[0], "author": row[1], "content": row[2], "createdAt": row[3],
        "images": _parse_images(row[4] if len(row) > 4 else "[]"),
    }


@app.get("/api/posts")
async def get_posts(category: str = "", author: str = ""):
    if author:
        cur = conn.execute(f"SELECT {_POST_COLS} FROM posts WHERE author = ? ORDER BY created_at DESC LIMIT 50", (author.strip()[:12],))
    elif category and category in FORUM_CATEGORIES:
        cur = conn.execute(f"SELECT {_POST_COLS} FROM posts WHERE category = ? ORDER BY created_at DESC LIMIT 50", (category,))
    else:
        cur = conn.execute(f"SELECT {_POST_COLS} FROM posts ORDER BY created_at DESC LIMIT 50")
    return {"success": True, "data": [_post_dict(r) for r in cur.fetchall()]}


@app.get("/api/posts/{post_id}")
async def get_post(post_id: str):
    cur = conn.execute(f"SELECT {_POST_COLS} FROM posts WHERE id = ?", (post_id,))
    row = cur.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="帖子不存在")
    with db_write():
        conn.execute("UPDATE posts SET views = views + 1 WHERE id = ?", (post_id,))
    data = _post_dict(row)
    data["views"] = (row[7] or 0) + 1
    rcur = conn.execute("SELECT id, author, content, created_at, images FROM replies WHERE post_id = ? ORDER BY created_at ASC LIMIT 100", (post_id,))
    data["replies"] = [_reply_dict(r) for r in rcur.fetchall()]
    return {"success": True, "data": data}


class ReplyReq(BaseModel):
    author: str = ""
    content: str = ""
    images: list[str] = []


@app.post("/api/posts/{post_id}/replies")
async def add_reply(post_id: str, req: ReplyReq, request: Request):
    username = _auth_user(request)
    if not username:
        raise HTTPException(status_code=401, detail="回帖需要先登录")
    if _like_count(post_id) is None:
        raise HTTPException(status_code=404, detail="帖子不存在")
    author = username
    content = (req.content or "").strip()[:300]
    images = _sanitize_images(req.images, max_count=2)
    if not content and not images:
        raise HTTPException(status_code=400, detail="回帖内容或图片不能为空")
    reply_id = "reply_" + "".join(random.choices(string.ascii_lowercase + string.digits, k=6))
    with db_write():
        conn.execute("INSERT INTO replies (id, post_id, author, content, images) VALUES (?, ?, ?, ?, ?)",
                     (reply_id, post_id, author, content, json.dumps(images)))
        conn.execute("UPDATE posts SET reply_count = reply_count + 1, last_reply_at = CURRENT_TIMESTAMP WHERE id = ?", (post_id,))
    return {"success": True, "id": reply_id}


@app.get("/api/home")
async def get_home():
    cur = conn.execute(f"SELECT {_POST_COLS} FROM posts ORDER BY created_at DESC LIMIT 5")
    latest = [_post_dict(r) for r in cur.fetchall()]
    gcur = conn.execute(f"SELECT {_POST_COLS} FROM posts WHERE category = '攻略心得' ORDER BY likes DESC, created_at DESC LIMIT 3")
    top_guides = [_post_dict(r) for r in gcur.fetchall()]
    total = conn.execute("SELECT COUNT(*) FROM posts").fetchone()[0]
    lrow = conn.execute("SELECT val FROM site_settings WHERE key = 'logo_version'").fetchone()
    logo_ver = int(lrow[0]) if (lrow and str(lrow[0]).isdigit()) else 1
    return {"success": True, "data": {
        "latest": latest, "topGuides": top_guides, "logoVersion": logo_ver,
        "stats": {"posts": total, "rooms": len(active_rooms), "categories": FORUM_CATEGORY_ORDER},
    }}


class LogoVerReq(BaseModel):
    version: int = 1


@app.post("/api/site/logo")
async def set_site_logo(req: LogoVerReq, request: Request):
    username = _auth_user(request)
    if not _is_admin(username):
        raise HTTPException(status_code=403, detail="仅最高权限管理员可切换全站图标版本")
    ver = max(1, min(6, int(req.version or 1)))
    with db_write():
        conn.execute("INSERT OR REPLACE INTO site_settings (key, val) VALUES ('logo_version', ?)", (str(ver),))
    return {"success": True, "version": ver}


# --- 个人中心聚合接口 ---
@app.get("/api/profile")
async def get_profile(request: Request):
    username = _auth_user(request)
    if not username:
        raise HTTPException(status_code=401, detail="请先登录")

    pcur = conn.execute(f"SELECT {_POST_COLS} FROM posts WHERE author = ? ORDER BY created_at DESC LIMIT 50", (username,))
    my_posts = [_post_dict(r) for r in pcur.fetchall()]

    icur = conn.execute(
        """
        SELECT r.id, r.post_id, p.title, r.author, r.content, r.created_at, r.images
        FROM replies r
        JOIN posts p ON r.post_id = p.id
        WHERE p.author = ? AND r.author != ?
        ORDER BY r.created_at DESC
        LIMIT 50
        """,
        (username, username)
    )
    inbox_replies = [
        {"id": r[0], "postId": r[1], "postTitle": r[2], "fromAuthor": r[3], "content": r[4], "createdAt": r[5], "images": _parse_images(r[6])}
        for r in icur.fetchall()
    ]

    rcur = conn.execute(
        """
        SELECT r.id, r.post_id, COALESCE(p.title, '已删除帖子'), r.content, r.created_at, r.images
        FROM replies r
        LEFT JOIN posts p ON r.post_id = p.id
        WHERE r.author = ?
        ORDER BY r.created_at DESC
        LIMIT 50
        """,
        (username,)
    )
    my_replies = [
        {"id": r[0], "postId": r[1], "postTitle": r[2], "content": r[3], "createdAt": r[4], "images": _parse_images(r[5])}
        for r in rcur.fetchall()
    ]

    acur = conn.execute(
        "SELECT id, name, version, game_version, author, description, download_url, likes, created_at, downloads FROM addons WHERE author = ? ORDER BY created_at DESC LIMIT 50",
        (username,)
    )
    my_addons = [_addon_dict(r) for r in acur.fetchall()]

    return {
        "success": True,
        "data": {
            "username": username,
            "isAdmin": _is_admin(username),
            "posts": my_posts,
            "inbox": inbox_replies,
            "replies": my_replies,
            "addons": my_addons,
        },
    }


@app.post("/api/posts")
async def create_post(req: PostReq, request: Request):
    username = _auth_user(request)
    if not username:
        raise HTTPException(status_code=401, detail="发帖需要先登录")
    if req.category not in FORUM_CATEGORIES:
        raise HTTPException(status_code=400, detail="请选择版块")
    category = req.category
    title = (req.title or "").strip()[:40]
    author = username
    content = (req.content or "").strip()[:300]
    images = _sanitize_images(req.images, max_count=4)
    if not title or (not content and not images):
        raise HTTPException(status_code=400, detail="标题和内容不能为空")
    post_id = "post_" + "".join(random.choices(string.ascii_lowercase + string.digits, k=6))
    with db_write():
        conn.execute(
            "INSERT INTO posts (id, category, title, author, content, images) VALUES (?, ?, ?, ?, ?, ?)",
            (post_id, category, title, author, content, json.dumps(images))
        )
    return {"success": True, "id": post_id}


def _like_count(post_id):
    cur = conn.execute("SELECT likes FROM posts WHERE id = ?", (post_id,))
    row = cur.fetchone()
    return row[0] if row else None


@app.post("/api/posts/{post_id}/like")
async def like_post(post_id: str, req: LikeReq, request: Request):
    if not _auth_user(request):
        raise HTTPException(status_code=401, detail="点赞需要先登录")
    voter = (req.voter or "").strip()[:36]
    if not voter:
        raise HTTPException(status_code=400, detail="缺少 voter")
    if _like_count(post_id) is None:
        raise HTTPException(status_code=404, detail="帖子不存在")
    with db_write():
        cur = conn.execute("INSERT OR IGNORE INTO post_likes (post_id, voter) VALUES (?, ?)", (post_id, voter))
        if cur.rowcount:
            conn.execute("UPDATE posts SET likes = likes + 1 WHERE id = ?", (post_id,))
    return {"success": True, "liked": True, "likes": _like_count(post_id)}


@app.post("/api/posts/{post_id}/unlike")
async def unlike_post(post_id: str, req: LikeReq, request: Request):
    if not _auth_user(request):
        raise HTTPException(status_code=401, detail="点赞需要先登录")
    voter = (req.voter or "").strip()[:36]
    if not voter:
        raise HTTPException(status_code=400, detail="缺少 voter")
    if _like_count(post_id) is None:
        raise HTTPException(status_code=404, detail="帖子不存在")
    with db_write():
        cur = conn.execute("DELETE FROM post_likes WHERE post_id = ? AND voter = ?", (post_id, voter))
        if cur.rowcount:
            conn.execute("UPDATE posts SET likes = MAX(likes - 1, 0) WHERE id = ?", (post_id,))
    return {"success": True, "liked": False, "likes": _like_count(post_id)}


@app.delete("/api/posts/{post_id}")
async def delete_post(post_id: str, request: Request):
    username = _auth_user(request)
    if not username:
        raise HTTPException(status_code=401, detail="删除帖子需要先登录")
    row = conn.execute("SELECT author FROM posts WHERE id = ?", (post_id,)).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="帖子不存在")
    if row[0] != username and not _is_admin(username):
        raise HTTPException(status_code=403, detail="只能删除自己发布的帖子")
    with db_write():
        conn.execute("DELETE FROM replies WHERE post_id = ?", (post_id,))
        conn.execute("DELETE FROM post_likes WHERE post_id = ?", (post_id,))
        conn.execute("DELETE FROM posts WHERE id = ?", (post_id,))
    return {"success": True}


@app.delete("/api/posts/{post_id}/replies/{reply_id}")
async def delete_reply(post_id: str, reply_id: str, request: Request):
    username = _auth_user(request)
    if not username:
        raise HTTPException(status_code=401, detail="删除评论需要先登录")
    rrow = conn.execute("SELECT author FROM replies WHERE id = ? AND post_id = ?", (reply_id, post_id)).fetchone()
    if not rrow:
        raise HTTPException(status_code=404, detail="评论不存在")
    prow = conn.execute("SELECT author FROM posts WHERE id = ?", (post_id,)).fetchone()
    post_author = prow[0] if prow else ""
    if rrow[0] != username and post_author != username and not _is_admin(username):
        raise HTTPException(status_code=403, detail="只能删除自己发表的评论或自己帖子下的评论")
    with db_write():
        conn.execute("DELETE FROM replies WHERE id = ? AND post_id = ?", (reply_id, post_id))
        conn.execute("UPDATE posts SET reply_count = MAX(reply_count - 1, 0) WHERE id = ?", (post_id,))
    return {"success": True}


# --- 账号登录认证 ---
class AuthReq(BaseModel):
    username: str = ""
    password: str = ""


_login_fails = {}


def _login_locked(username):
    info = _login_fails.get(username)
    return bool(info and info[1] > time.time())


def _login_fail(username):
    info = _login_fails.get(username) or [0, 0.0]
    info[0] += 1
    if info[0] >= 5:
        info[1] = time.time() + 300
        info[0] = 0
    _login_fails[username] = info


def _new_session(username):
    token = secrets.token_urlsafe(32)
    expires = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time() + 30 * 24 * 3600))
    with db_write():
        conn.execute("INSERT INTO sessions (token, username, expires_at) VALUES (?, ?, ?)", (token, username, expires))
    return token


def _auth_user(request):
    auth = request.headers.get("authorization") or ""
    if not auth.startswith("Bearer "):
        return None
    token = auth[7:].strip()
    if not token:
        return None
    row = conn.execute("SELECT username, expires_at FROM sessions WHERE token = ?", (token,)).fetchone()
    if not row:
        return None
    if row[1] and row[1] < time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()):
        with db_write():
            conn.execute("DELETE FROM sessions WHERE token = ?", (token,))
        return None
    return row[0]


@app.post("/api/auth/register")
async def register(req: AuthReq):
    username = (req.username or "").strip()
    password = req.password or ""
    if not re.fullmatch(r"[\w\u4e00-\u9fa5]{2,12}", username):
        raise HTTPException(status_code=400, detail="用户名 2-12 位，仅限字母、数字、中文、下划线")
    if _is_admin(username):
        raise HTTPException(status_code=400, detail="该用户名不可注册")
    if not (4 <= len(password) <= 32):
        raise HTTPException(status_code=400, detail="密码 4-32 位")
    salt = secrets.token_hex(8)
    with db_write():
        cur = conn.execute("INSERT OR IGNORE INTO users (username, password_hash, password_salt) VALUES (?, ?, ?)",
                           (username, _hash_password(password, salt), salt))
        if not cur.rowcount:
            raise HTTPException(status_code=400, detail="用户名已被注册")
    return {"success": True, "data": {"username": username, "isAdmin": _is_admin(username), "token": _new_session(username)}}


@app.post("/api/auth/login")
async def login(req: AuthReq):
    username = (req.username or "").strip()
    if _login_locked(username):
        raise HTTPException(status_code=429, detail="密码错误次数过多，5 分钟后再试")
    row = conn.execute("SELECT password_hash, password_salt FROM users WHERE username = ?", (username,)).fetchone()
    if not row or not hmac.compare_digest(_hash_password(req.password or "", row[1]), row[0]):
        _login_fail(username)
        raise HTTPException(status_code=400, detail="用户名或密码错误")
    _login_fails.pop(username, None)
    return {"success": True, "data": {"username": username, "isAdmin": _is_admin(username), "token": _new_session(username)}}


@app.post("/api/auth/logout")
async def logout(request: Request):
    auth = request.headers.get("authorization") or ""
    if auth.startswith("Bearer "):
        with db_write():
            conn.execute("DELETE FROM sessions WHERE token = ?", (auth[7:].strip(),))
    return {"success": True}


@app.get("/api/auth/me")
async def me(request: Request):
    username = _auth_user(request)
    if not username:
        raise HTTPException(status_code=401, detail="未登录")
    return {"success": True, "data": {"username": username, "isAdmin": _is_admin(username)}}


# --- 多源魔兽快讯聚合引擎 ---
NEWS_OFFICIAL_URL = "https://wow.blizzard.cn/news/"
NEWS_WOWHEAD_RSS = "https://www.wowhead.com/news/rss"
NEWS_MMOC_RSS = "https://www.mmo-champion.com/external.php?do=rss&type=newcontent&sectionid=1&days=120&count=10"
NEWS_NGA_URL = "https://nga.178.com/thread.php?fid=310&rss=1"
NEWS_CACHE_SECONDS = 1800
_news_state = {"last_fetch": 0.0, "fetching": False}

_UA_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

_SEED_NEWS = [
    (
        "https://wow.blizzard.cn/news/seed-official-1", "官网",
        "《魔兽世界》每周例行维护与在线修正汇总公告",
        "本周例行维护已完成，多项史诗钥石地下城首领技能数值与职业套装触发率已实装在线热修。",
        time.strftime("%Y-%m-%d"),
        "<p><strong>【官方蓝贴汇总】</strong> 本周各服务器已完成例行维护，主要调整如下：优化了延迟表现，下调了泽风岛精英怪伤害。</p>",
    ),
    (
        "https://bbs.nga.cn/read.php?tid=42450001", "NGA社区",
        "【NGA前线周报】全专精大秘境与团本最新梯队排行及配装风向标",
        "汇总本周国服与亚服高层钥石冲层数据、WCL 全首领秒伤中位数统计及热门饰品模拟排行。",
        time.strftime("%Y-%m-%d"),
        "<p><strong>【艾泽拉斯国家地理 · 前线数据周报】</strong> 结合本周 WCL 实战日志与高层大秘境限时样本，防骑与复仇DH稳居 T0。</p>",
    ),
    (
        "https://www.wowhead.com/news/seed-wowhead-1", "Wowhead",
        "[Wowhead 数据挖掘] 下阶段新补丁职业英雄天赋改动与新增坐骑预览",
        "最新测试服 Build 数据挖掘出炉：包含多个专精英雄天赋树节点微调、全新商栈幻化与虚空主题坐骑。",
        time.strftime("%Y-%m-%d"),
        "<p><strong>【Wowhead PTR 数据挖掘快讯】</strong> 在今日推送的最新测试服构建版本中，开发人员对多项冷门天赋进行了数值增强。</p>",
    ),
    (
        "https://www.mmo-champion.com/content/seed-mmoc-1", "国际服快讯",
        "[MMO-Champion] 设计师访谈：关于地下城词缀体验与追赶机制的未来计划",
        "开发团队在最新社区访谈中回应了小号装备追赶、纹章获取上限解锁以及大秘境词缀轮换的后续改动方向。",
        time.strftime("%Y-%m-%d"),
        "<p><strong>【开发者访谈纪要】</strong> 针对小号纹章肝度问题，设计师表示将在小版本中进一步提高战团绑定装备掉率。</p>",
    ),
]
for _s_url, _s_src, _s_title, _s_sum, _s_pub, _s_body in _SEED_NEWS:
    conn.execute(
        "INSERT OR IGNORE INTO news (url, source, title, summary, published_at, content) VALUES (?, ?, ?, ?, ?, ?)",
        (_s_url, _s_src, _s_title, _s_sum, _s_pub, _s_body),
    )
conn.commit()


def _strip_tags(raw_html):
    if not raw_html:
        return ""
    txt = re.sub(r"<[^>]+>", " ", raw_html)
    txt = htmlmod.unescape(txt)
    return re.sub(r"\s+", " ", txt).strip()


def _normalize_pub_date(raw_date):
    if not raw_date:
        return time.strftime("%Y-%m-%d")
    s = raw_date.strip()
    m = re.search(r"(\d{4})[-/年](\d{1,2})[-/月](\d{1,2})", s)
    if m:
        return f"{int(m.group(1)):04d}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"
    return time.strftime("%Y-%m-%d")


def _fetch_official_news():
    req = urllib.request.Request(NEWS_OFFICIAL_URL, headers=_UA_HEADERS)
    with urllib.request.urlopen(req, timeout=10) as resp:
        page = resp.read().decode("utf-8", "ignore")
    items, seen = [], set()
    for m in re.finditer(r'<a[^>]*href="(https://wow\.blizzard\.cn/news/[^"]+)"[^>]*>(.*?)</a>', page, re.S):
        url = m.group(1)
        if url in seen:
            continue
        inner = m.group(2)
        t = re.search(r'class="list-title"[^>]*>(.*?)</div>', inner, re.S)
        if not t:
            continue
        title = _strip_tags(t.group(1))
        if not title:
            continue
        d = re.search(r'class="list-desc"[^>]*>(.*?)</div>', inner, re.S)
        summary = _strip_tags(d.group(1))[:120] if d else ""
        tm = re.search(r'class="list-time"[^>]*data-time="([^"]+)"', inner)
        seen.add(url)
        items.append({
            "url": url, "source": "官网", "title": title[:65],
            "summary": summary, "published_at": _normalize_pub_date(tm.group(1) if tm else ""), "content": "",
        })
        if len(items) >= 12:
            break
    return items


def _fetch_rss_feed(feed_url, source_label, max_items=8):
    req = urllib.request.Request(feed_url, headers=_UA_HEADERS)
    with urllib.request.urlopen(req, timeout=10) as resp:
        raw_bytes = resp.read()
    text = raw_bytes.decode("utf-8", "ignore")
    if "encoding=\"gbk\"" in text.lower():
        text = raw_bytes.decode("gbk", "ignore")

    root = ET.fromstring(text)
    items = []
    for item in root.findall(".//item")[:max_items]:
        title = _strip_tags(item.findtext("title") or "")
        link = (item.findtext("link") or "").strip()
        desc_raw = item.findtext("description") or ""
        pub = item.findtext("pubDate") or ""
        if not title or not link:
            continue
        summary = _strip_tags(desc_raw)[:130]
        s = _ArticleSanitizer()
        try:
            s.feed(desc_raw)
            s.close()
            clean_body = s.get_html().strip()
        except Exception:
            clean_body = ""
        if not clean_body and summary:
            clean_body = f"<p>{htmlmod.escape(summary)}</p>"
        items.append({
            "url": link, "source": source_label, "title": title[:70],
            "summary": summary or title, "published_at": _normalize_pub_date(pub), "content": clean_body,
        })
    return items


def _fetch_all_news_sources():
    all_items = []
    for label, fn in [("官网", _fetch_official_news), ("Wowhead", lambda: _fetch_rss_feed(NEWS_WOWHEAD_RSS, "Wowhead", 8)), ("国际服快讯", lambda: _fetch_rss_feed(NEWS_MMOC_RSS, "国际服快讯", 6)), ("NGA社区", lambda: _fetch_rss_feed(NEWS_NGA_URL, "NGA社区", 8))]:
        try:
            res = fn()
            if res:
                all_items.extend(res)
        except Exception as e:
            log.info("资讯源 [%s] 本次跳过: %s", label, e)
    return all_items


def _store_news(items):
    new_count = 0
    with db_write():
        for it in items:
            cur = conn.execute(
                "INSERT OR IGNORE INTO news (url, source, title, summary, published_at, content) VALUES (?, ?, ?, ?, ?, ?)",
                (it["url"], it["source"], it["title"], it["summary"], it["published_at"], it.get("content") or ""),
            )
            new_count += cur.rowcount
    return new_count


async def _refresh_news_if_stale(force: bool = False):
    now = time.time()
    if (not force and now - _news_state["last_fetch"] < NEWS_CACHE_SECONDS) or _news_state["fetching"]:
        return 0
    _news_state["fetching"] = True
    try:
        items = await asyncio.to_thread(_fetch_all_news_sources)
        new_count = _store_news(items)
        _news_state["last_fetch"] = time.time()
        if new_count:
            await sio.emit("news:new", {"count": new_count, "source": "多源快讯"})
        return new_count
    except Exception as e:
        log.warning("多源资讯抓取异常: %s", e)
        return 0
    finally:
        _news_state["fetching"] = False


@app.on_event("startup")
async def _startup_news_refresh():
    await _refresh_news_if_stale()


@app.get("/api/news")
async def get_news(source: str = "", refresh: int = 0):
    await _refresh_news_if_stale(force=bool(refresh))
    if source:
        cur = conn.execute(
            "SELECT url, source, title, summary, published_at FROM news WHERE source = ? ORDER BY published_at DESC, fetched_at DESC LIMIT 30",
            (source,))
    else:
        cur = conn.execute(
            "SELECT url, source, title, summary, published_at FROM news ORDER BY published_at DESC, fetched_at DESC LIMIT 30")
    return {"success": True, "data": [
        {"url": r[0], "source": r[1], "title": r[2], "summary": r[3], "publishedAt": r[4]}
        for r in cur.fetchall()]}


class _ArticleSanitizer(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.out = []
        self.drop_depth = 0

    def handle_starttag(self, tag, attrs):
        if tag in {"script", "style", "iframe", "form", "object"}:
            self.drop_depth += 1
            return
        if self.drop_depth:
            return
        if tag not in {"p", "h1", "h2", "h3", "ul", "ol", "li", "blockquote", "img", "a", "strong", "em", "b", "i", "br", "hr"}:
            return
        self.out.append(f"<{tag}>")

    def handle_endtag(self, tag):
        if tag in {"script", "style", "iframe", "form", "object"}:
            self.drop_depth = max(0, self.drop_depth - 1)
            return
        if self.drop_depth:
            return
        if tag in {"p", "h1", "h2", "h3", "ul", "ol", "li", "blockquote", "strong", "em", "b", "i"}:
            self.out.append(f"</{tag}>")

    def handle_data(self, data):
        if self.drop_depth:
            return
        self.out.append(htmlmod.escape(data))

    def get_html(self):
        return "".join(self.out)


@app.get("/api/news/article")
async def get_news_article(url: str = ""):
    row = conn.execute("SELECT title, source, published_at, summary, content FROM news WHERE url = ?", (url,)).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="not in news list")
    title, source, published_at, summary, content = row
    if content:
        return {"success": True, "data": {"url": url, "title": title, "source": source, "publishedAt": published_at, "content": content}}
    safe_sum = htmlmod.escape(summary or title or "")
    body = f"<blockquote><p>📡 <strong>来源频道：{htmlmod.escape(source or '外站快讯')}</strong></p></blockquote><p>{safe_sum}</p><p>该快讯来自外部资讯源，您可以点击下方 <strong>「🌐 前往源站查看完整原文」</strong> 查阅完整图文与数据表格。</p>"
    with db_write():
        conn.execute("UPDATE news SET content = ? WHERE url = ?", (body, url))
    return {"success": True, "data": {"url": url, "title": title, "source": source, "publishedAt": published_at, "content": body}}


# --- 插件分享 API ---
class AddonReq(BaseModel):
    name: str = ""
    version: str = ""
    game_version: str = ""
    author: str = ""
    description: str = ""
    download_url: str = ""


def _addon_dict(row):
    return {
        "id": row[0], "name": row[1], "version": row[2], "gameVersion": row[3],
        "author": row[4], "description": row[5], "downloadUrl": row[6],
        "likes": row[7], "createdAt": row[8], "downloads": row[9] or 0,
    }


@app.get("/api/addons")
async def get_addons():
    cur = conn.execute("SELECT id, name, version, game_version, author, description, download_url, likes, created_at, downloads FROM addons ORDER BY downloads DESC, likes DESC, created_at DESC LIMIT 50")
    return {"success": True, "data": [_addon_dict(r) for r in cur.fetchall()]}


@app.post("/api/addons")
async def create_addon(req: AddonReq, request: Request):
    username = _auth_user(request)
    if not username:
        raise HTTPException(status_code=401, detail="分享插件需要先登录")
    name = (req.name or "").strip()[:40]
    url = (req.download_url or "").strip()[:300]
    if not name or not (url.startswith("http://") or url.startswith("https://")):
        raise HTTPException(status_code=400, detail="插件名和下载链接必填")
    addon_id = "addon_" + "".join(random.choices(string.ascii_lowercase + string.digits, k=6))
    with db_write():
        conn.execute(
            "INSERT INTO addons (id, name, version, game_version, author, description, download_url) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (addon_id, name, (req.version or "").strip()[:20], (req.game_version or "").strip()[:20],
             username, (req.description or "").strip()[:200], url))
    return {"success": True, "id": addon_id}


def _addon_like_count(addon_id):
    cur = conn.execute("SELECT likes FROM addons WHERE id = ?", (addon_id,))
    row = cur.fetchone()
    return row[0] if row else None


@app.post("/api/addons/{addon_id}/download")
async def download_addon(addon_id: str):
    with db_write():
        conn.execute("UPDATE addons SET downloads = downloads + 1 WHERE id = ?", (addon_id,))
    cur = conn.execute("SELECT downloads FROM addons WHERE id = ?", (addon_id,))
    row = cur.fetchone()
    return {"success": True, "downloads": row[0] if row else 1}


@app.post("/api/addons/{addon_id}/like")
async def like_addon(addon_id: str, req: LikeReq, request: Request):
    if not _auth_user(request):
        raise HTTPException(status_code=401, detail="点赞需要先登录")
    voter = (req.voter or "").strip()[:36]
    if not voter:
        raise HTTPException(status_code=400, detail="缺少 voter")
    if _addon_like_count(addon_id) is None:
        raise HTTPException(status_code=404, detail="插件不存在")
    with db_write():
        cur = conn.execute("INSERT OR IGNORE INTO addon_likes (addon_id, voter) VALUES (?, ?)", (addon_id, voter))
        if cur.rowcount:
            conn.execute("UPDATE addons SET likes = likes + 1 WHERE id = ?", (addon_id,))
    return {"success": True, "liked": True, "likes": _addon_like_count(addon_id)}


@app.post("/api/addons/{addon_id}/unlike")
async def unlike_addon(addon_id: str, req: LikeReq, request: Request):
    if not _auth_user(request):
        raise HTTPException(status_code=401, detail="点赞需要先登录")
    voter = (req.voter or "").strip()[:36]
    if not voter:
        raise HTTPException(status_code=400, detail="缺少 voter")
    if _addon_like_count(addon_id) is None:
        raise HTTPException(status_code=404, detail="插件不存在")
    with db_write():
        cur = conn.execute("DELETE FROM addon_likes WHERE addon_id = ? AND voter = ?", (addon_id, voter))
        if cur.rowcount:
            conn.execute("UPDATE addons SET likes = MAX(likes - 1, 0) WHERE id = ?", (addon_id,))
    return {"success": True, "liked": False, "likes": _addon_like_count(addon_id)}


@app.delete("/api/addons/{addon_id}")
async def delete_addon(addon_id: str, request: Request):
    username = _auth_user(request)
    if not username:
        raise HTTPException(status_code=401, detail="删除插件需要先登录")
    row = conn.execute("SELECT author FROM addons WHERE id = ?", (addon_id,)).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="插件不存在")
    if row[0] != username and not _is_admin(username):
        raise HTTPException(status_code=403, detail="只能删除自己上传的插件")
    with db_write():
        conn.execute("DELETE FROM addon_likes WHERE addon_id = ?", (addon_id,))
        conn.execute("DELETE FROM addons WHERE id = ?", (addon_id,))
    return {"success": True}


# --- 配装模拟方案云端保存与读取 API ---
class SimBuildReq(BaseModel):
    title: str = ""
    spec_key: str = "ret_paladin"
    spec_name: str = "⚔️ 惩戒圣骑士"
    ilvl: int = 626
    main_stat: int = 1200
    crit: float = 30.0
    haste: float = 20.0
    mastery: float = 25.0
    vers: float = 10.0
    dps: int = 0


@app.get("/api/sim/builds")
async def get_sim_builds(spec_key: str = ""):
    if spec_key:
        cur = conn.execute(
            "SELECT id, title, author, spec_key, spec_name, ilvl, main_stat, crit, haste, mastery, vers, dps, created_at FROM sim_builds WHERE spec_key = ? ORDER BY dps DESC LIMIT 20",
            (spec_key.strip()[:30],),
        )
    else:
        cur = conn.execute(
            "SELECT id, title, author, spec_key, spec_name, ilvl, main_stat, crit, haste, mastery, vers, dps, created_at FROM sim_builds ORDER BY created_at DESC LIMIT 20"
        )
    return {"success": True, "data": [
        {
            "id": r[0], "title": r[1], "author": r[2], "specKey": r[3], "specName": r[4],
            "ilvl": r[5], "mainStat": r[6], "crit": r[7], "haste": r[8], "mastery": r[9],
            "vers": r[10], "dps": r[11], "createdAt": r[12]
        } for r in cur.fetchall()
    ]}


@app.post("/api/sim/builds")
async def save_sim_build(req: SimBuildReq, request: Request):
    username = _auth_user(request)
    if not username:
        raise HTTPException(status_code=401, detail="请先登录")
    title = (req.title or "").strip()[:36]
    if not title:
        raise HTTPException(status_code=400, detail="请填写配装方案名称")
    build_id = "build_" + "".join(random.choices(string.ascii_lowercase + string.digits, k=6))
    with db_write():
        conn.execute(
            """
            INSERT INTO sim_builds (id, title, author, spec_key, spec_name, ilvl, main_stat, crit, haste, mastery, vers, dps)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (build_id, title, username, req.spec_key[:30], req.spec_name[:24], req.ilvl, req.main_stat, req.crit, req.haste, req.mastery, req.vers, req.dps),
        )
    return {"success": True, "id": build_id}


@app.delete("/api/sim/builds/{build_id}")
async def delete_sim_build(build_id: str, request: Request):
    username = _auth_user(request)
    if not username:
        raise HTTPException(status_code=401, detail="请先登录")
    row = conn.execute("SELECT author FROM sim_builds WHERE id = ?", (build_id,)).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="配装方案不存在")
    if row[0] != username and not _is_admin(username):
        raise HTTPException(status_code=403, detail="无权删除该配装方案")
    with db_write():
        conn.execute("DELETE FROM sim_builds WHERE id = ?", (build_id,))
    return {"success": True}


# --- 秘典与宠物图鉴 API ---
@app.get("/api/codex")
async def get_codex():
    return {"success": True, "data": {"items": [], "companions": []}}


# --- 全地图任务图鉴 API (1-60级 pfQuest 数据) ---
conn.execute("""
    CREATE TABLE IF NOT EXISTS quests (
        id INTEGER PRIMARY KEY,
        title TEXT NOT NULL,
        title_en TEXT DEFAULT '',
        zone_id INTEGER DEFAULT 0,
        zone TEXT DEFAULT '艾泽拉斯',
        level INTEGER DEFAULT 1,
        required_level INTEGER DEFAULT 1,
        faction TEXT DEFAULT 'both',
        giver_npc TEXT DEFAULT '',
        giver_coord TEXT DEFAULT '',
        turnin_npc TEXT DEFAULT '',
        turnin_coord TEXT DEFAULT '',
        objectives TEXT DEFAULT '[]',
        description TEXT DEFAULT '',
        prereq_ids TEXT DEFAULT '[]',
        next_ids TEXT DEFAULT '[]',
        screenshots TEXT DEFAULT '[]',
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
""")
conn.commit()


def _parse_json_list(raw):
    if not raw:
        return []
    try:
        arr = json.loads(raw)
        return arr if isinstance(arr, list) else []
    except Exception:
        return []


def _quest_brief(row):
    return {
        "id": row[0], "title": row[1], "zone": row[2],
        "level": row[3], "requiredLevel": row[4], "faction": row[5],
        "giverNpc": row[6], "giverCoord": row[7],
        "hasPrereq": bool(_parse_json_list(row[8])),
        "hasNext": bool(_parse_json_list(row[9])),
        "shotCount": len(_parse_images(row[10])),
    }


@app.get("/api/quests/zones")
async def get_quest_zones():
    cur = conn.execute(
        "SELECT zone, COUNT(*) as cnt FROM quests GROUP BY zone ORDER BY cnt DESC, zone ASC"
    )
    return {"success": True, "data": [{"zone": r[0], "count": r[1]} for r in cur.fetchall()]}


@app.get("/api/quests")
async def list_quests(
    zone: str = "",
    faction: str = "",
    q: str = "",
    min_level: int = 1,
    max_level: int = 60,
    page: int = 1,
    page_size: int = 30,
):
    page = max(1, page)
    page_size = max(10, min(60, page_size))
    where, params = ["level BETWEEN ? AND ?"], [max(1, min_level), min(60, max_level)]

    if zone.strip():
        where.append("zone = ?")
        params.append(zone.strip())
    if faction in ("alliance", "horde", "both"):
        if faction == "both":
            where.append("faction = 'both'")
        else:
            where.append("(faction = ? OR faction = 'both')")
            params.append(faction)
    if q.strip():
        kw = f"%{q.strip()}%"
        where.append("(title LIKE ? OR title_en LIKE ? OR giver_npc LIKE ? OR turnin_npc LIKE ? OR zone LIKE ? OR CAST(id AS TEXT) = ?)")
        params.extend([kw, kw, kw, kw, kw, q.strip()])

    where_sql = " AND ".join(where)
    total = conn.execute(f"SELECT COUNT(*) FROM quests WHERE {where_sql}", params).fetchone()[0]
    offset = (page - 1) * page_size
    cur = conn.execute(
        f"""
        SELECT id, title, zone, level, required_level, faction,
               giver_npc, giver_coord, prereq_ids, next_ids, screenshots
        FROM quests
        WHERE {where_sql}
        ORDER BY level ASC, id ASC
        LIMIT ? OFFSET ?
        """,
        params + [page_size, offset],
    )
    return {
        "success": True,
        "data": {
            "items": [_quest_brief(r) for r in cur.fetchall()],
            "total": total,
            "page": page,
            "pageSize": page_size,
            "totalPages": max(1, (total + page_size - 1) // page_size),
        },
    }


def _fetch_chain_nodes(ids: list):
    clean_ids = [int(i) for i in ids if isinstance(i, int) or (isinstance(i, str) and i.isdigit())]
    if not clean_ids:
        return []
    placeholders = ",".join("?" for _ in clean_ids)
    cur = conn.execute(
        f"SELECT id, title, zone, level, faction FROM quests WHERE id IN ({placeholders}) ORDER BY level ASC, id ASC",
        clean_ids,
    )
    return [{"id": r[0], "title": r[1], "zone": r[2], "level": r[3], "faction": r[4]} for r in cur.fetchall()]


@app.get("/api/quests/{quest_id}")
async def get_quest_detail(quest_id: int):
    row = conn.execute(
        """
        SELECT id, title, title_en, zone_id, zone, level, required_level, faction,
               giver_npc, giver_coord, turnin_npc, turnin_coord,
               objectives, description, prereq_ids, next_ids, screenshots
        FROM quests WHERE id = ?
        """,
        (quest_id,),
    ).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="任务不存在")
    prereq_ids = _parse_json_list(row[14])
    next_ids = _parse_json_list(row[15])
    return {
        "success": True,
        "data": {
            "id": row[0], "title": row[1], "titleEn": row[2],
            "zoneId": row[3], "zone": row[4], "level": row[5],
            "requiredLevel": row[6], "faction": row[7],
            "giverNpc": row[8], "giverCoord": row[9],
            "turninNpc": row[10], "turninCoord": row[11],
            "objectives": _parse_json_list(row[12]),
            "description": row[13] or "",
            "prereqs": _fetch_chain_nodes(prereq_ids),
            "nextQuests": _fetch_chain_nodes(next_ids),
            "screenshots": _parse_images(row[16]),
        },
    }


class QuestShotReq(BaseModel):
    images: list[str] = []


@app.post("/api/quests/{quest_id}/screenshots")
async def add_quest_screenshots(quest_id: int, req: QuestShotReq, request: Request):
    username = _auth_user(request)
    if not username:
        raise HTTPException(status_code=401, detail="上传实景截图需要先登录")
    row = conn.execute("SELECT screenshots FROM quests WHERE id = ?", (quest_id,)).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="任务不存在")
    existing = _parse_images(row[0])
    new_imgs = _sanitize_images(req.images, max_count=6)
    merged = (existing + new_imgs)[:6]
    with db_write():
        conn.execute("UPDATE quests SET screenshots = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?", (json.dumps(merged), quest_id))
    return {"success": True, "screenshots": merged}


@app.delete("/api/quests/{quest_id}/screenshots/{idx}")
async def delete_quest_screenshot(quest_id: int, idx: int, request: Request):
    username = _auth_user(request)
    if not username:
        raise HTTPException(status_code=401, detail="删除截图需要先登录")
    row = conn.execute("SELECT screenshots FROM quests WHERE id = ?", (quest_id,)).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="任务不存在")
    imgs = _parse_images(row[0])
    if not (0 <= idx < len(imgs)):
        raise HTTPException(status_code=400, detail="截图索引无效")
    imgs.pop(idx)
    with db_write():
        conn.execute("UPDATE quests SET screenshots = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?", (json.dumps(imgs), quest_id))
    return {"success": True, "screenshots": imgs}


# --- Socket.io 营火大厅与聊天 ---
@sio.event
async def connect(sid, environ):
    await sio.emit("lobby:list", get_public_rooms(), to=sid)


@sio.on("room:create")
async def handle_create_room(sid, data):
    data = _data(data)
    cid = _client_id(sid, data)
    room_id = "".join(random.choices(string.ascii_lowercase + string.digits, k=6))
    nickname = (data.get("nickname") or "神秘队长").strip()[:12]
    title = (data.get("title") or "泽风岛开荒求组").strip()[:24]
    password = (data.get("password") or "").strip()
    salt = secrets.token_hex(8) if password else ""
    new_room = {
        "id": room_id, "title": title, "faction": data.get("faction", "horde"),
        "leader": nickname, "leaderId": cid,
        "password_hash": _hash_password(password, salt) if password else "",
        "password_salt": salt,
        "isHidden": bool(data.get("isHidden", False)),
        "maxMembers": MAX_MEMBERS,
        "members": [{"id": cid, "sid": sid, "name": nickname, "role": data.get("role", "⚔️ 输出")}],
    }
    active_rooms[room_id] = new_room
    client_to_room[cid] = room_id
    await sio.enter_room(sid, room_id)
    await sio.emit("lobby:list", get_public_rooms())
    await sio.emit("room:joined", get_room_detail(new_room), to=sid)


@sio.on("room:join")
async def handle_join_room(sid, data):
    data = _data(data)
    cid = _client_id(sid, data)
    room_id = (data.get("roomId") or "").strip().lower()
    room = active_rooms.get(room_id)
    if not room:
        await sio.emit("room:error", "❌ 未找到该房间号，可能已解散！", to=sid)
        return
    if room.get("password_hash") and not _check_password(room, data.get("password")):
        await sio.emit("room:error", "🔒 房间暗号错误！", to=sid)
        return
    nickname = (data.get("nickname") or "热心队友").strip()[:12]
    role = data.get("role", "⚔️ 输出")
    room["members"].append({"id": cid, "sid": sid, "name": nickname, "role": role})
    client_to_room[cid] = room_id
    await sio.enter_room(sid, room_id)
    await sio.emit("room:updated", get_room_detail(room), room=room_id)
    await sio.emit("room:joined", get_room_detail(room), to=sid)
    await sio.emit("room:chat", {"sender": "系统", "text": f"🎉 {nickname} 加入了小队！", "isSystem": True}, room=room_id)
    await sio.emit("lobby:list", get_public_rooms())


@sio.on("room:chat")
async def handle_room_chat(sid, data):
    data = _data(data)
    cid = sid_to_client.get(sid)
    room = _room_of_client(cid) if cid else None
    if not room:
        return
    member = next((m for m in room["members"] if m["id"] == cid), None)
    text = (data.get("text") or "").strip()[:200]
    if text:
        await sio.emit("room:chat", {"sender": member["name"] if member else "队友", "text": text, "isSystem": False}, room=room["id"])


@sio.on("room:leave")
async def handle_leave_room(sid):
    await leave_current_room(sid, force=True)
    await sio.emit("room:left", to=sid)


async def leave_current_room(sid, force=False):
    cid = sid_to_client.get(sid)
    if not cid:
        return
    room_id = client_to_room.pop(cid, None)
    if room_id and room_id in active_rooms:
        room = active_rooms[room_id]
        room["members"] = [m for m in room["members"] if m["id"] != cid]
        await sio.leave_room(sid, room_id)
        if len(room["members"]) == 0:
            del active_rooms[room_id]
        else:
            await sio.emit("room:updated", get_room_detail(room), room=room_id)
        await sio.emit("lobby:list", get_public_rooms())


@sio.event
async def disconnect(sid):
    await leave_current_room(sid)
    sid_to_client.pop(sid, None)


app.mount("/", StaticFiles(directory="public", html=True), name="static")

if __name__ == "__main__":
    uvicorn.run(socket_app, host=os.environ.get("HOST", "0.0.0.0"), port=int(os.environ.get("PORT", "3000")))