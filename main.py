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
import sqlite3
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

# 初始化 SQLite 数据库（支持营火、论坛、配装、个人中心等）
conn = sqlite3.connect(os.environ.get("GAMEMATE_DB", "gamemate.db"), check_same_thread=False)
conn.execute("PRAGMA journal_mode=WAL;")
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
# 为个人中心查询加速建立索引
conn.execute("CREATE INDEX IF NOT EXISTS idx_posts_author ON posts(author);")
conn.execute("CREATE INDEX IF NOT EXISTS idx_replies_author ON replies(author);")
conn.execute("CREATE INDEX IF NOT EXISTS idx_replies_post_id ON replies(post_id);")

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
        "https://www.xiaoheihe.cn/app/workshop",
        245,
        6890,
    ),
    (
        "addon_wbox",
        "新手盒子 (Wbox)",
        "官方最新版",
        "全版本通用",
        "新手盒子官方",
        "聚合海量主播与高玩界面整合包、WA 字符串与实用工具箱，内置换肤与配置迁移功能。",
        "https://www.wbox.top/",
        198,
        5310,
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
    """校验前端上传的 DataURL 或安全图片链接，限制张数与单张体积。"""
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
    conn.execute("UPDATE posts SET views = views + 1 WHERE id = ?", (post_id,))
    conn.commit()
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
    conn.execute("INSERT INTO replies (id, post_id, author, content, images) VALUES (?, ?, ?, ?, ?)",
                 (reply_id, post_id, author, content, json.dumps(images)))
    conn.execute("UPDATE posts SET reply_count = reply_count + 1, last_reply_at = CURRENT_TIMESTAMP WHERE id = ?", (post_id,))
    conn.commit()
    return {"success": True, "id": reply_id}


@app.get("/api/home")
async def get_home():
    cur = conn.execute(f"SELECT {_POST_COLS} FROM posts ORDER BY created_at DESC LIMIT 5")
    latest = [_post_dict(r) for r in cur.fetchall()]
    gcur = conn.execute(f"SELECT {_POST_COLS} FROM posts WHERE category = '攻略心得' ORDER BY likes DESC, created_at DESC LIMIT 3")
    top_guides = [_post_dict(r) for r in gcur.fetchall()]
    total = conn.execute("SELECT COUNT(*) FROM posts").fetchone()[0]
    return {"success": True, "data": {
        "latest": latest, "topGuides": top_guides,
        "stats": {"posts": total, "rooms": len(active_rooms), "categories": FORUM_CATEGORY_ORDER},
    }}


# --- 新增：个人中心聚合接口（一次性返回我的帖子、别人回复我的、我的评论、我的插件） ---
@app.get("/api/profile")
async def get_profile(request: Request):
    username = _auth_user(request)
    if not username:
        raise HTTPException(status_code=401, detail="请先登录")

    # 1. 我发布的帖子
    pcur = conn.execute(
        f"SELECT {_POST_COLS} FROM posts WHERE author = ? ORDER BY created_at DESC LIMIT 50",
        (username,)
    )
    my_posts = [_post_dict(r) for r in pcur.fetchall()]

    # 2. 别人回复我的（在我发布的帖子下，且回帖人不是我自己）
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
        {
            "id": r[0],
            "postId": r[1],
            "postTitle": r[2],
            "fromAuthor": r[3],
            "content": r[4],
            "createdAt": r[5],
            "images": _parse_images(r[6]),
        }
        for r in icur.fetchall()
    ]

    # 3. 我发表的评论（含所属帖子标题）
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
        {
            "id": r[0],
            "postId": r[1],
            "postTitle": r[2],
            "content": r[3],
            "createdAt": r[4],
            "images": _parse_images(r[5]),
        }
        for r in rcur.fetchall()
    ]

    # 4. 我分享的插件
    acur = conn.execute(
        """
        SELECT id, name, version, game_version, author, description, download_url, likes, created_at, downloads
        FROM addons
        WHERE author = ?
        ORDER BY created_at DESC
        LIMIT 50
        """,
        (username,)
    )
    my_addons = [_addon_dict(r) for r in acur.fetchall()]

    return {
        "success": True,
        "data": {
            "username": username,
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
    conn.execute(
        "INSERT INTO posts (id, category, title, author, content, images) VALUES (?, ?, ?, ?, ?, ?)",
        (post_id, category, title, author, content, json.dumps(images))
    )
    conn.commit()
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
    cur = conn.execute("INSERT OR IGNORE INTO post_likes (post_id, voter) VALUES (?, ?)", (post_id, voter))
    if cur.rowcount:  # 首次点赞才 +1，重复点不重复加
        conn.execute("UPDATE posts SET likes = likes + 1 WHERE id = ?", (post_id,))
        conn.commit()
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
    cur = conn.execute("DELETE FROM post_likes WHERE post_id = ? AND voter = ?", (post_id, voter))
    if cur.rowcount:  # 之前赞过才 -1
        conn.execute("UPDATE posts SET likes = MAX(likes - 1, 0) WHERE id = ?", (post_id,))
        conn.commit()
    return {"success": True, "liked": False, "likes": _like_count(post_id)}


# --- 账号登录：用户名+密码，开放注册；发帖/回帖必须登录，作者取登录账号 ---
class AuthReq(BaseModel):
    username: str = ""
    password: str = ""


_login_fails = {}


def _new_session(username):
    token = secrets.token_urlsafe(32)
    expires = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time() + 30 * 24 * 3600))
    conn.execute("INSERT INTO sessions (token, username, expires_at) VALUES (?, ?, ?)", (token, username, expires))
    conn.commit()
    return token


def _auth_user(request):
    """从 Authorization: Bearer 取 token，返回用户名；未登录/过期返回 None。"""
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
        conn.execute("DELETE FROM sessions WHERE token = ?", (token,))
        conn.commit()
        return None
    return row[0]


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


@app.post("/api/auth/register")
async def register(req: AuthReq):
    username = (req.username or "").strip()
    password = req.password or ""
    if not re.fullmatch(r"[\w\u4e00-\u9fa5]{2,12}", username):
        raise HTTPException(status_code=400, detail="用户名 2-12 位，仅限字母、数字、中文、下划线")
    if not (4 <= len(password) <= 32):
        raise HTTPException(status_code=400, detail="密码 4-32 位")
    salt = secrets.token_hex(8)
    cur = conn.execute("INSERT OR IGNORE INTO users (username, password_hash, password_salt) VALUES (?, ?, ?)",
                       (username, _hash_password(password, salt), salt))
    if not cur.rowcount:
        raise HTTPException(status_code=400, detail="用户名已被注册")
    conn.commit()
    return {"success": True, "data": {"username": username, "token": _new_session(username)}}


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
    return {"success": True, "data": {"username": username, "token": _new_session(username)}}


@app.post("/api/auth/logout")
async def logout(request: Request):
    auth = request.headers.get("authorization") or ""
    if auth.startswith("Bearer "):
        conn.execute("DELETE FROM sessions WHERE token = ?", (auth[7:].strip(),))
        conn.commit()
    return {"success": True}


@app.get("/api/auth/me")
async def me(request: Request):
    username = _auth_user(request)
    if not username:
        raise HTTPException(status_code=401, detail="未登录")
    return {"success": True, "data": {"username": username}}


# --- 多源魔兽快讯聚合：国服官网 + NGA/178 + Wowhead RSS + MMO-Champion RSS ---
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

# 预置多源初始资讯种子（保证首次启动或外网源受限时各分类均有丰富内容）
_SEED_NEWS = [
    (
        "https://wow.blizzard.cn/news/seed-official-1",
        "官网",
        "《魔兽世界》每周例行维护与在线修正汇总公告",
        "本周例行维护已完成，多项史诗钥石地下城首领技能数值与职业套装触发率已实装在线热修。",
        time.strftime("%Y-%m-%d"),
        "<p><strong>【官方蓝贴汇总】</strong> 本周各服务器已完成例行维护，主要调整如下：</p><ul><li>优化了高延迟环境下部分引导类法术的施法队列判定。</li><li>下调了泽风岛精英区域部分法系怪物的奥术冲击伤害（降低 12%）。</li><li>修复了部分专精在切换天赋配置后精通加成未能立即刷新的问题。</li></ul>",
    ),
    (
        "https://bbs.nga.cn/read.php?tid=42450001",
        "NGA社区",
        "【NGA前线周报】全专精大秘境与团本最新梯队排行及配装风向标",
        "汇总本周国服与亚服高层钥石冲层数据、WCL 全首领秒伤中位数统计及热门饰品模拟排行。",
        time.strftime("%Y-%m-%d"),
        "<p><strong>【艾泽拉斯国家地理 · 前线数据周报】</strong></p><p>结合本周 WCL 95 分位实战日志与高层大秘境限时样本，当前版本各职责表现如下：</p><blockquote><p><strong>坦克职责：</strong>防骑与复仇恶魔猎手在聚怪控制链上依然稳居 T0，血DK在高压尖刺伤害BOSS战表现突出。</p><p><strong>治疗职责：</strong>戒律牧与奶萨凭借强大的团队减伤覆盖率成为开荒首选。</p></blockquote><p>建议各位开荒队友优先凑齐 4 件套效果后再堆叠核心绿字属性。</p>",
    ),
    (
        "https://bbs.nga.cn/read.php?tid=42450002",
        "NGA社区",
        "【地精商会】本周拍卖行开荒合剂、食物与附魔材料价格波动分析",
        "周四CD更新前后主流爆发药水、三星附魔与公函材料价格走势一览，教你错峰采购省下数万金币。",
        time.strftime("%Y-%m-%d"),
        "<p><strong>【地精研究院周报】</strong> 每周四晚 19:00-22:00 为各服务器活动高峰期，合剂与爆发药水价格普遍上浮 25%~40%。建议在周日或周一下午补齐下周开荒物资。</p>",
    ),
    (
        "https://www.wowhead.com/news/seed-wowhead-1",
        "Wowhead",
        "[Wowhead 数据挖掘] 下阶段新ビルド补丁职业天赋改动与新增坐骑预览",
        "最新测试服 Build 数据挖掘出炉：包含多个专精英雄天赋树节点微调、全新商栈幻化与虚空主题坐骑。",
        time.strftime("%Y-%m-%d"),
        "<p><strong>【Wowhead PTR 数据挖掘快讯】</strong></p><p>在今日推送的最新测试服构建版本中，开发人员对多项过冷门的天赋节点进行了数值补偿（Buff 幅度在 6%~15% 之间），同时新增了 3 只造型独特的飞行坐骑与多套职业主题武器幻化。</p>",
    ),
    (
        "https://www.mmo-champion.com/content/seed-mmoc-1",
        "国际服快讯",
        "[MMO-Champion] 设计师蓝贴访谈：关于地下城词缀体验与追赶机制的未来计划",
        "开发团队在最新社区访谈中回应了小号装备追赶、纹章获取上限解锁以及大秘境词缀轮换的后续改动方向。",
        time.strftime("%Y-%m-%d"),
        "<p><strong>【开发者访谈纪要】</strong></p><p>针对社区广泛反馈的“多角色小号纹章与神勇石获取肝度过高”问题，设计师表示将在接下来的小版本中进一步提高账号战团绑定装备的掉落率，并提供更便捷的战团货币转移折扣。</p>",
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
    """抓取暴雪国服官网新闻列表页 (wow.blizzard.cn/news/)。"""
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
            "url": url,
            "source": "官网",
            "title": title[:65],
            "summary": summary,
            "published_at": _normalize_pub_date(tm.group(1) if tm else ""),
            "content": "",
        })
        if len(items) >= 12:
            break
    return items


def _fetch_rss_feed(feed_url, source_label, max_items=8):
    """通用 RSS/Atom 解析器：用于抓取 Wowhead、MMO-Champion、NGA 等 XML 源。"""
    req = urllib.request.Request(feed_url, headers=_UA_HEADERS)
    with urllib.request.urlopen(req, timeout=10) as resp:
        raw_bytes = resp.read()
    text = raw_bytes.decode("utf-8", "ignore")
    if "encoding=\"gbk\"" in text.lower() or "encoding='gbk'" in text.lower():
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
        # 清洗一份带段落排版的正文缓存，便于站内直接阅读
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
            "url": link,
            "source": source_label,
            "title": title[:70],
            "summary": summary or title,
            "published_at": _normalize_pub_date(pub),
            "content": clean_body,
        })
    return items


def _fetch_all_news_sources():
    """多渠道聚合抓取：任一渠道超时或失败不影响其他渠道入库。"""
    all_items = []
    fetch_jobs = [
        ("官网", _fetch_official_news),
        ("Wowhead", lambda: _fetch_rss_feed(NEWS_WOWHEAD_RSS, "Wowhead", 8)),
        ("国际服快讯", lambda: _fetch_rss_feed(NEWS_MMOC_RSS, "国际服快讯", 6)),
        ("NGA社区", lambda: _fetch_rss_feed(NEWS_NGA_URL, "NGA社区", 8)),
    ]
    for label, fn in fetch_jobs:
        try:
            res = fn()
            if res:
                all_items.extend(res)
        except Exception as e:
            log.info("资讯源 [%s] 本次跳过: %s", label, e)
    return all_items


def _store_news(items):
    new_count = 0
    for it in items:
        cur = conn.execute(
            "INSERT OR IGNORE INTO news (url, source, title, summary, published_at, content) VALUES (?, ?, ?, ?, ?, ?)",
            (it["url"], it["source"], it["title"], it["summary"], it["published_at"], it.get("content") or ""),
        )
        new_count += cur.rowcount
    conn.commit()
    return new_count


async def _refresh_news_if_stale(force: bool = False):
    """缓存 30 分钟；支持 force 强制立即刷新多源快讯。"""
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


# --- 资讯正文站内阅读：支持官网详情抓包清洗 + 外部多源 RSS 摘要/正文展示 ---
NEWS_ARTICLE_HOST = "https://wow.blizzard.cn/news/"
_ARTICLE_ALLOWED_TAGS = {
    "p", "h1", "h2", "h3", "h4", "ul", "ol", "li", "blockquote",
    "img", "a", "strong", "em", "b", "i", "u", "s", "span", "div",
    "hr", "br", "table", "thead", "tbody", "tr", "td", "th", "pre", "code",
}
_ARTICLE_DROP_TAGS = {"script", "style", "iframe", "form", "object", "embed", "link", "meta", "noscript"}


def _extract_detail_html(page):
    """取 <div class="detail"> 内的 HTML；按 div 层级配平找结束位置，结构变了返回空。"""
    m = re.search(r'<div class="detail">', page)
    if not m:
        return ""
    start = m.end()
    depth = 1
    tag_re = re.compile(r'<(/?)div\b[^>]*>', re.I)
    for t in tag_re.finditer(page, start):
        if t.group(1):
            depth -= 1
            if depth == 0:
                return page[start:t.start()]
        else:
            depth += 1
    return ""


class _ArticleSanitizer(HTMLParser):
    """白名单重建：只留正文标签与安全属性，其余标签去标记留文本，脚本类整段丢弃。"""

    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.out = []
        self.drop_depth = 0

    def handle_starttag(self, tag, attrs):
        if tag in _ARTICLE_DROP_TAGS:
            self.drop_depth += 1
            return
        if self.drop_depth:
            return
        if tag not in _ARTICLE_ALLOWED_TAGS:
            return
        kept = []
        for k, v in attrs:
            if k.startswith("on"):
                continue
            if tag == "img" and k in ("src", "alt", "title"):
                if k == "src" and not (v.startswith("https://wow.res.netease.com/") or v.startswith("https://wow.blizzard.cn/")):
                    continue
                kept.append((k, v))
            elif tag == "a" and k == "href":
                if v.startswith("http://") or v.startswith("https://"):
                    kept.append((k, v))
            elif tag in ("span", "div") and k == "style":
                low = v.lower()
                if "expression" in low or "javascript" in low or "url(" in low:
                    continue
                kept.append((k, v))
        attr = "".join(f' {k}="{htmlmod.escape(v, quote=True)}"' for k, v in kept)
        if tag == "img":
            self.out.append(f"<img{attr} loading=\"lazy\">")
        elif tag == "a":
            self.out.append(f"<a{attr} target=\"_blank\" rel=\"noopener\">")
        elif tag in ("hr", "br"):
            self.out.append(f"<{tag}>")
        else:
            self.out.append(f"<{tag}{attr}>")

    def handle_startendtag(self, tag, attrs):
        self.handle_starttag(tag, attrs)
        if tag in _ARTICLE_ALLOWED_TAGS and tag not in _ARTICLE_DROP_TAGS and not self.drop_depth:
            if tag not in ("hr", "br", "img"):
                self.out.append(f"</{tag}>")

    def handle_endtag(self, tag):
        if tag in _ARTICLE_DROP_TAGS:
            self.drop_depth = max(0, self.drop_depth - 1)
            return
        if self.drop_depth:
            return
        if tag in _ARTICLE_ALLOWED_TAGS and tag not in ("hr", "br", "img"):
            self.out.append(f"</{tag}>")

    def handle_data(self, data):
        if self.drop_depth:
            return
        self.out.append(htmlmod.escape(data))

    def get_html(self):
        return "".join(self.out)


def _fetch_article_body(url):
    """同步抓文章页并返回清洗后的正文 HTML；结构对不上返回空串。"""
    req = urllib.request.Request(url, headers={
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
    })
    with urllib.request.urlopen(req, timeout=15) as resp:
        page = resp.read().decode("utf-8", "ignore")
    inner = _extract_detail_html(page)
    if not inner.strip():
        return ""
    s = _ArticleSanitizer()
    s.feed(inner)
    s.close()
    return s.get_html().strip()


@app.get("/api/news/article")
async def get_news_article(url: str = ""):
    """站内读资讯正文：只允许库里已有的链接（防 SSRF），支持官网全文抓包与外站 RSS 内容回退。"""
    row = conn.execute(
        "SELECT title, source, published_at, summary, content FROM news WHERE url = ?", (url,)).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="not in news list")
    title, source, published_at, summary, content = row
    if content:
        return {"success": True, "data": {"url": url, "title": title, "source": source,
                                           "publishedAt": published_at, "content": content, "cached": True}}
    body = ""
    if url.startswith(NEWS_ARTICLE_HOST):
        try:
            body = await asyncio.to_thread(_fetch_article_body, url)
        except Exception as e:
            log.warning("资讯正文抓取失败 %s: %s", url, e)
    if not body:
        safe_sum = htmlmod.escape(summary or title or "")
        safe_url = htmlmod.escape(url, quote=True)
        body = (
            f"<blockquote><p>📡 <strong>来源频道：{htmlmod.escape(source or '外站快讯')}</strong></p></blockquote>"
            f"<p>{safe_sum}</p>"
            f"<p>该快讯来自外部资讯源，你可以点击下方 <strong>「🌐 前往源站查看完整原文」</strong> 查阅完整图文与数据表格。</p>"
        )
    conn.execute("UPDATE news SET content = ? WHERE url = ?", (body, url))
    conn.commit()
    return {"success": True, "data": {"url": url, "title": title, "source": source,
                                       "publishedAt": published_at, "content": body, "cached": False}}


# --- 插件分享 ---
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
        raise HTTPException(status_code=400, detail="插件名和下载链接（http/https 开头）必填")
    addon_id = "addon_" + "".join(random.choices(string.ascii_lowercase + string.digits, k=6))
    conn.execute(
        "INSERT INTO addons (id, name, version, game_version, author, description, download_url) VALUES (?, ?, ?, ?, ?, ?, ?)",
        (addon_id, name, (req.version or "").strip()[:20], (req.game_version or "").strip()[:20],
         username, (req.description or "").strip()[:200], url))
    conn.commit()
    return {"success": True, "id": addon_id}


def _addon_like_count(addon_id):
    cur = conn.execute("SELECT likes FROM addons WHERE id = ?", (addon_id,))
    row = cur.fetchone()
    return row[0] if row else None


@app.post("/api/addons/{addon_id}/download")
async def download_addon(addon_id: str):
    cur = conn.execute("SELECT id FROM addons WHERE id = ?", (addon_id,))
    if not cur.fetchone():
        raise HTTPException(status_code=404, detail="插件不存在")
    conn.execute("UPDATE addons SET downloads = downloads + 1 WHERE id = ?", (addon_id,))
    conn.commit()
    cur = conn.execute("SELECT downloads FROM addons WHERE id = ?", (addon_id,))
    return {"success": True, "downloads": cur.fetchone()[0]}


@app.post("/api/addons/{addon_id}/like")
async def like_addon(addon_id: str, req: LikeReq, request: Request):
    if not _auth_user(request):
        raise HTTPException(status_code=401, detail="点赞需要先登录")
    voter = (req.voter or "").strip()[:36]
    if not voter:
        raise HTTPException(status_code=400, detail="缺少 voter")
    if _addon_like_count(addon_id) is None:
        raise HTTPException(status_code=404, detail="插件不存在")
    cur = conn.execute("INSERT OR IGNORE INTO addon_likes (addon_id, voter) VALUES (?, ?)", (addon_id, voter))
    if cur.rowcount:
        conn.execute("UPDATE addons SET likes = likes + 1 WHERE id = ?", (addon_id,))
        conn.commit()
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
    cur = conn.execute("DELETE FROM addon_likes WHERE addon_id = ? AND voter = ?", (addon_id, voter))
    if cur.rowcount:
        conn.execute("UPDATE addons SET likes = MAX(likes - 1, 0) WHERE id = ?", (addon_id,))
    if cur.rowcount:
        conn.commit()
    return {"success": True, "liked": False, "likes": _addon_like_count(addon_id)}


# --- 配装模拟云端方案保存与读取 API ---
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


def _build_dict(row):
    return {
        "id": row[0],
        "title": row[1],
        "author": row[2],
        "specKey": row[3],
        "specName": row[4],
        "ilvl": row[5],
        "mainStat": row[6],
        "crit": row[7],
        "haste": row[8],
        "mastery": row[9],
        "vers": row[10],
        "dps": row[11],
        "createdAt": row[12],
    }


@app.get("/api/sim/builds")
async def get_sim_builds(spec_key: str = ""):
    if spec_key:
        cur = conn.execute(
            "SELECT id, title, author, spec_key, spec_name, ilvl, main_stat, crit, haste, mastery, vers, dps, created_at FROM sim_builds WHERE spec_key = ? ORDER BY dps DESC, created_at DESC LIMIT 20",
            (spec_key.strip()[:30],),
        )
    else:
        cur = conn.execute(
            "SELECT id, title, author, spec_key, spec_name, ilvl, main_stat, crit, haste, mastery, vers, dps, created_at FROM sim_builds ORDER BY created_at DESC LIMIT 20"
        )
    return {"success": True, "data": [_build_dict(r) for r in cur.fetchall()]}


@app.post("/api/sim/builds")
async def save_sim_build(req: SimBuildReq, request: Request):
    username = _auth_user(request)
    if not username:
        raise HTTPException(status_code=401, detail="保存配装方案需要先登录")
    title = (req.title or "").strip()[:36]
    if not title:
        raise HTTPException(status_code=400, detail="请填写配装方案名称")
    build_id = "build_" + "".join(random.choices(string.ascii_lowercase + string.digits, k=6))
    ilvl = max(550, min(700, int(req.ilvl or 626)))
    main_stat = max(100, min(9999, int(req.main_stat or 1200)))
    crit = round(max(0.0, min(100.0, float(req.crit or 0))), 1)
    haste = round(max(0.0, min(100.0, float(req.haste or 0))), 1)
    mastery = round(max(0.0, min(120.0, float(req.mastery or 0))), 1)
    vers = round(max(0.0, min(80.0, float(req.vers or 0))), 1)
    dps = max(0, min(9_999_999, int(req.dps or 0)))
    conn.execute(
        """
        INSERT INTO sim_builds (id, title, author, spec_key, spec_name, ilvl, main_stat, crit, haste, mastery, vers, dps)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (build_id, title, username, (req.spec_key or "ret_paladin")[:30], (req.spec_name or "专精")[:24], ilvl, main_stat, crit, haste, mastery, vers, dps),
    )
    conn.commit()
    return {"success": True, "id": build_id}


# --- Socket.io 营火大厅与语音 ---
@sio.event
async def connect(sid, environ):
    await sio.emit("lobby:list", get_public_rooms(), to=sid)


@sio.on("room:create")
async def handle_create_room(sid, data):
    data = _data(data)
    if not _check_rate(sid, "room:create"):
        await sio.emit("room:error", "⚠️ 创建太频繁了，稍后再试！", to=sid)
        return
    if len(active_rooms) >= MAX_ROOMS:
        await sio.emit("room:error", "⚠️ 房间数量已达上限，请稍后再试！", to=sid)
        return
    cid = _client_id(sid, data)
    await leave_current_room(sid, force=True)
    room_id = "".join(random.choices(string.ascii_lowercase + string.digits, k=6))
    while room_id in active_rooms:
        room_id = "".join(random.choices(string.ascii_lowercase + string.digits, k=6))

    nickname = (data.get("nickname") or "神秘队长").strip()[:12]
    title = (data.get("title") or "泽风岛开荒求组").strip()[:24]
    password = (data.get("password") or "").strip()
    salt = secrets.token_hex(8) if password else ""
    is_hidden = bool(data.get("isHidden", False))

    new_room = {
        "id": room_id, "title": title, "faction": data.get("faction", "horde"),
        "leader": nickname, "leaderId": cid,
        "password_hash": _hash_password(password, salt) if password else "",
        "password_salt": salt,
        "isHidden": is_hidden,
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
    if not _check_rate(sid, "room:join"):
        await sio.emit("room:error", "⚠️ 操作太频繁了，稍后再试！", to=sid)
        return
    cid = _client_id(sid, data)
    room_id = (data.get("roomId") or "").strip().lower()
    room = active_rooms.get(room_id)
    if not room:
        await sio.emit("room:error", "❌ 未找到该房间号，可能已解散！", to=sid)
        return

    fail = _join_failures.get(room_id)
    if fail and time.time() < fail.get("locked_until", 0):
        await sio.emit("room:error", "🔒 该房间密码错误次数过多，已暂时锁定 5 分钟！", to=sid)
        return

    # 断线重连：同一 clientId 已在房内时直接恢复身份，不重复加人
    existing = next((m for m in room["members"] if m["id"] == cid), None)
    if existing:
        existing["sid"] = sid
        client_to_room[cid] = room_id
        await sio.enter_room(sid, room_id)
        await sio.emit("room:joined", get_room_detail(room), to=sid)
        return

    async with _room_lock:
        if len(room["members"]) >= room["maxMembers"]:
            await sio.emit("room:error", f"队伍人数已满 ({room['maxMembers']}/{room['maxMembers']})", to=sid)
            return
        if room.get("password_hash"):
            input_pwd = (data.get("password") or "").strip()
            if not _check_password(room, input_pwd):
                fail = _join_failures.setdefault(room_id, {"count": 0, "locked_until": 0})
                fail["count"] += 1
                if fail["count"] >= JOIN_LOCK_MAX_FAILS:
                    fail["locked_until"] = time.time() + JOIN_LOCK_MINUTES * 60
                    fail["count"] = 0
                await sio.emit("room:error", "🔒 房间密码错误！", to=sid)
                return
        _join_failures.pop(room_id, None)

        await leave_current_room(sid, force=True)
        nickname = (data.get("nickname") or "热心队友").strip()[:12]
        role = data.get("role", "⚔️ 输出")
        room["members"].append({"id": cid, "sid": sid, "name": nickname, "role": role})
        client_to_room[cid] = room_id

    await sio.enter_room(sid, room_id)
    await sio.emit("room:updated", get_room_detail(room), room=room_id)
    await sio.emit("room:joined", get_room_detail(room), to=sid)
    await sio.emit("room:chat", {"sender": "系统", "text": f"🎉 {nickname} ({role}) 加入了小队！", "isSystem": True}, room=room_id)
    await sio.emit("lobby:list", get_public_rooms())


@sio.on("room:kick")
async def handle_kick_member(sid, data):
    data = _data(data)
    cid = sid_to_client.get(sid)
    room = _room_of_client(cid) if cid else None
    if not room:
        return
    if room["leaderId"] != cid:
        await sio.emit("room:error", "⚠️ 只有小队队长才能踢出成员！", to=sid)
        return
    target_id = data.get("targetId")
    if not target_id or target_id == cid:
        return
    target_member = next((m for m in room["members"] if m["id"] == target_id), None)
    if not target_member:
        return
    room["members"] = [m for m in room["members"] if m["id"] != target_id]
    client_to_room.pop(target_id, None)
    target_sid = client_to_sid.pop(target_id, None)
    if target_sid:
        await sio.leave_room(target_sid, room["id"])
    if target_sid:
        await sio.emit("room:kicked", "⚠️ 你已被队长移出小队。", to=target_sid)
    await sio.emit("room:chat", {"sender": "系统", "text": f"👢 {target_member['name']} 已被队长移出小队。", "isSystem": True}, room=room["id"])
    await sio.emit("room:updated", get_room_detail(room), room=room["id"])
    await sio.emit("lobby:list", get_public_rooms())


@sio.on("room:chat")
async def handle_room_chat(sid, data):
    data = _data(data)
    if not _check_rate(sid, "room:chat"):
        return
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
    room = _room_of_client(cid)
    if room and not force:
        _m = next((m for m in room["members"] if m["id"] == cid), None)
        if _m and _m.get("sid") and _m.get("sid") != sid:
            sid_to_client.pop(sid, None)
            return
    room_id = client_to_room.pop(cid, None)
    if room_id and room_id in active_rooms:
        room = active_rooms[room_id]
        leaving_member = next((m for m in room["members"] if m["id"] == cid), None)
        room["members"] = [m for m in room["members"] if m["id"] != cid]
        await sio.leave_room(sid, room_id)
        if len(room["members"]) == 0:
            del active_rooms[room_id]
            _join_failures.pop(room_id, None)
        else:
            if room["leaderId"] == cid:
                new_leader = room["members"][0]
                room["leaderId"] = new_leader["id"]
                room["leader"] = new_leader["name"]
                await sio.emit("room:chat", {"sender": "系统", "text": f"👑 原队长离开，{new_leader['name']} 成为了新的小队队长！", "isSystem": True}, room=room_id)
            elif leaving_member:
                await sio.emit("room:chat", {"sender": "系统", "text": f"👋 {leaving_member['name']} 离开了营火小队。", "isSystem": True}, room=room_id)
            await sio.emit("room:updated", get_room_detail(room), room=room_id)
        await sio.emit("lobby:list", get_public_rooms())


@sio.event
async def disconnect(sid):
    await leave_current_room(sid)
    sid_to_client.pop(sid, None)


app.mount("/", StaticFiles(directory="public", html=True), name="static")

if __name__ == "__main__":
    uvicorn.run(socket_app, host=os.environ.get("HOST", "0.0.0.0"), port=int(os.environ.get("PORT", "3000")))