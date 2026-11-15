import asyncio
import hashlib
import html as htmlmod
from html.parser import HTMLParser
import json
import re
import urllib.request
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


# --- 官网资讯：后端定时抓 wow.blizzard.cn/news，存库；有新文章站内推送 ---
NEWS_OFFICIAL_URL = "https://wow.blizzard.cn/news/"
NEWS_CACHE_SECONDS = 1800
_news_state = {"last_fetch": 0.0, "fetching": False}


def _fetch_official_news():
    """同步抓取官网新闻列表页；只解析 HTML，不登录不调 API。"""
    req = urllib.request.Request(NEWS_OFFICIAL_URL, headers={
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
    })
    with urllib.request.urlopen(req, timeout=15) as resp:
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
        title = htmlmod.unescape(re.sub(r"<[^>]+>", "", t.group(1))).strip()
        if not title:
            continue
        d = re.search(r'class="list-desc"[^>]*>(.*?)</div>', inner, re.S)
        summary = htmlmod.unescape(re.sub(r"<[^>]+>", "", d.group(1))).strip()[:120] if d else ""
        tm = re.search(r'class="list-time"[^>]*data-time="([^"]+)"', inner)
        seen.add(url)
        items.append({"url": url, "source": "官网", "title": title[:60],
                      "summary": summary, "published_at": tm.group(1) if tm else ""})
        if len(items) >= 20:
            break
    return items


def _store_news(items):
    new_count = 0
    for it in items:
        cur = conn.execute(
            "INSERT OR IGNORE INTO news (url, source, title, summary, published_at) VALUES (?, ?, ?, ?, ?)",
            (it["url"], it["source"], it["title"], it["summary"], it["published_at"]))
        new_count += cur.rowcount
    conn.commit()
    return new_count


async def _refresh_news_if_stale():
    """缓存 30 分钟；过期才抓一次，抓失败只记日志、不影响接口返回旧数据。"""
    now = time.time()
    if now - _news_state["last_fetch"] < NEWS_CACHE_SECONDS or _news_state["fetching"]:
        return 0
    _news_state["fetching"] = True
    try:
        items = await asyncio.to_thread(_fetch_official_news)
        new_count = _store_news(items)
        _news_state["last_fetch"] = time.time()
        if new_count:
            await sio.emit("news:new", {"count": new_count, "source": "官网"})
        return new_count
    except Exception as e:
        log.warning("官网资讯抓取失败: %s", e)
        return 0
    finally:
        _news_state["fetching"] = False


@app.on_event("startup")
async def _startup_news_refresh():
    await _refresh_news_if_stale()


@app.get("/api/news")
async def get_news(source: str = ""):
    await _refresh_news_if_stale()
    if source:
        cur = conn.execute(
            "SELECT url, source, title, summary, published_at FROM news WHERE source = ? ORDER BY published_at DESC, fetched_at DESC LIMIT 20",
            (source,))
    else:
        cur = conn.execute(
            "SELECT url, source, title, summary, published_at FROM news ORDER BY published_at DESC, fetched_at DESC LIMIT 20")
    return {"success": True, "data": [
        {"url": r[0], "source": r[1], "title": r[2], "summary": r[3], "publishedAt": r[4]}
        for r in cur.fetchall()]}


# --- 资讯正文站内阅读：后端抓官网文章页，只取 <div class="detail"> 正文，白名单清洗后返回 ---
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
    """站内读资讯正文：只允许库里已有的官网链接（防 SSRF），正文抓一次后存库。"""
    if not url.startswith(NEWS_ARTICLE_HOST):
        raise HTTPException(status_code=400, detail="bad url")
    row = conn.execute(
        "SELECT title, source, published_at, content FROM news WHERE url = ?", (url,)).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="not in news list")
    title, source, published_at, content = row
    if content:
        return {"success": True, "data": {"url": url, "title": title, "source": source,
                                           "publishedAt": published_at, "content": content, "cached": True}}
    try:
        body = await asyncio.to_thread(_fetch_article_body, url)
    except Exception as e:
        log.warning("资讯正文抓取失败 %s: %s", url, e)
        raise HTTPException(status_code=502, detail="fetch failed")
    if not body:
        raise HTTPException(status_code=502, detail="empty body")
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
        conn.commit()
    return {"success": True, "liked": False, "likes": _addon_like_count(addon_id)}


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