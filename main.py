import asyncio
import hashlib
import html as htmlmod
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
from fastapi import FastAPI, HTTPException
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

# 初始化 SQLite 数据库（支持营火、论坛、配装等）
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
# 老库迁移：posts 补浏览/回复数字段（没有才加），新建回帖表
_post_cols = {r[1] for r in conn.execute("PRAGMA table_info(posts)")}
for _col, _ddl in [
    ("views", "ALTER TABLE posts ADD COLUMN views INTEGER DEFAULT 0"),
    ("reply_count", "ALTER TABLE posts ADD COLUMN reply_count INTEGER DEFAULT 0"),
    ("last_reply_at", "ALTER TABLE posts ADD COLUMN last_reply_at DATETIME"),
]:
    if _col not in _post_cols:
        conn.execute(_ddl)
conn.execute("""
             CREATE TABLE IF NOT EXISTS replies (
                                                  id TEXT PRIMARY KEY,
                                                  post_id TEXT,
                                                  author TEXT,
                                                  content TEXT,
                                                  created_at DATETIME DEFAULT CURRENT_TIMESTAMP
             )
             """)
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
# 收紧 CORS：只允许本站与本地开发来源；局域网 IP 访问如需放行再加对应来源
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
    author: str
    content: str


class LikeReq(BaseModel):
    voter: str = ""


FORUM_CATEGORIES = {"综合讨论", "开荒组队", "欧皇晒图", "Bug反馈", "攻略心得", "开荒战报"}
FORUM_CATEGORY_ORDER = ["综合讨论", "开荒组队", "攻略心得", "开荒战报", "欧皇晒图", "Bug反馈"]


_POST_COLS = "id, category, title, author, content, likes, created_at, views, reply_count, last_reply_at"


def _post_dict(row):
    return {
        "id": row[0], "category": row[1], "title": row[2], "author": row[3],
        "content": row[4], "likes": row[5], "createdAt": row[6],
        "views": row[7] or 0, "replyCount": row[8] or 0, "lastReplyAt": row[9],
    }


def _reply_dict(row):
    return {"id": row[0], "author": row[1], "content": row[2], "createdAt": row[3]}


@app.get("/api/posts")
async def get_posts(category: str = ""):
    if category and category in FORUM_CATEGORIES:
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
    rcur = conn.execute("SELECT id, author, content, created_at FROM replies WHERE post_id = ? ORDER BY created_at ASC LIMIT 100", (post_id,))
    data["replies"] = [_reply_dict(r) for r in rcur.fetchall()]
    return {"success": True, "data": data}


class ReplyReq(BaseModel):
    author: str = ""
    content: str = ""


@app.post("/api/posts/{post_id}/replies")
async def add_reply(post_id: str, req: ReplyReq):
    if _like_count(post_id) is None:
        raise HTTPException(status_code=404, detail="帖子不存在")
    author = (req.author or "").strip()[:12] or "无名冒险者"
    content = (req.content or "").strip()[:300]
    if not content:
        raise HTTPException(status_code=400, detail="回帖内容不能为空")
    reply_id = "reply_" + "".join(random.choices(string.ascii_lowercase + string.digits, k=6))
    conn.execute("INSERT INTO replies (id, post_id, author, content) VALUES (?, ?, ?, ?)",
                 (reply_id, post_id, author, content))
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


@app.post("/api/posts")
async def create_post(req: PostReq):
    if req.category not in FORUM_CATEGORIES:
        raise HTTPException(status_code=400, detail="请选择版块")
    category = req.category
    title = (req.title or "").strip()[:40]
    author = (req.author or "").strip()[:12] or "无名冒险者"
    content = (req.content or "").strip()[:300]
    if not title or not content:
        raise HTTPException(status_code=400, detail="标题和内容不能为空")
    post_id = "post_" + "".join(random.choices(string.ascii_lowercase + string.digits, k=6))
    conn.execute(
        "INSERT INTO posts (id, category, title, author, content) VALUES (?, ?, ?, ?, ?)",
        (post_id, category, title, author, content)
    )
    conn.commit()
    return {"success": True, "id": post_id}


def _like_count(post_id):
    cur = conn.execute("SELECT likes FROM posts WHERE id = ?", (post_id,))
    row = cur.fetchone()
    return row[0] if row else None


@app.post("/api/posts/{post_id}/like")
async def like_post(post_id: str, req: LikeReq):
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
async def unlike_post(post_id: str, req: LikeReq):
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
async def create_addon(req: AddonReq):
    name = (req.name or "").strip()[:40]
    url = (req.download_url or "").strip()[:300]
    if not name or not (url.startswith("http://") or url.startswith("https://")):
        raise HTTPException(status_code=400, detail="插件名和下载链接（http/https 开头）必填")
    addon_id = "addon_" + "".join(random.choices(string.ascii_lowercase + string.digits, k=6))
    conn.execute(
        "INSERT INTO addons (id, name, version, game_version, author, description, download_url) VALUES (?, ?, ?, ?, ?, ?, ?)",
        (addon_id, name, (req.version or "").strip()[:20], (req.game_version or "").strip()[:20],
         (req.author or "").strip()[:12] or "无名冒险者", (req.description or "").strip()[:200], url))
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
async def like_addon(addon_id: str, req: LikeReq):
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
async def unlike_addon(addon_id: str, req: LikeReq):
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
    # 重连保护（只对断开连接生效）：该 clientId 已用新 sid 重进房时，旧连接的
    # 断开不踢人。建房/加房/主动离开是有意移动，必须清掉旧房成员——否则下面
    # 这支会把刚建好的新 sid 映射弹掉，之后 room:chat 等按 sid 查身份的
    # handler 全部静默失效，旧房还一直残留（/health 的 rooms 只涨不跌）。
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
