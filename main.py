import random
import string
import sqlite3
from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
import socketio
import uvicorn

# 初始化 SQLite 数据库（支持营火、论坛、配装等）
conn = sqlite3.connect("gamemate.db", check_same_thread=False)
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
conn.commit()

app = FastAPI(title="GameMate.fun Backend")
sio = socketio.AsyncServer(async_mode="asgi", cors_allowed_origins="*")
socket_app = socketio.ASGIApp(sio, other_asgi_app=app)

active_rooms = {}
sid_to_room = {}

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
            "isLocked": bool(r.get("password")),
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
        "isLocked": bool(room.get("password")),
        "isHidden": bool(room.get("isHidden")),
        "members": room["members"],
    }

# --- 论坛 API ---
class PostReq(BaseModel):
    category: str
    title: str
    author: str
    content: str

@app.get("/api/posts")
async def get_posts():
    cur = conn.execute("SELECT id, category, title, author, content, likes, created_at FROM posts ORDER BY created_at DESC LIMIT 50")
    rows = cur.fetchall()
    posts = []
    for row in rows:
        posts.append({
            "id": row[0],
            "category": row[1],
            "title": row[2],
            "author": row[3],
            "content": row[4],
            "likes": row[5],
            "createdAt": row[6]
        })
    return {"success": True, "data": posts}

@app.post("/api/posts")
async def create_post(req: PostReq):
    post_id = "post_" + "".join(random.choices(string.ascii_lowercase + string.digits, k=6))
    conn.execute(
        "INSERT INTO posts (id, category, title, author, content) VALUES (?, ?, ?, ?, ?)",
        (post_id, req.category, req.title, req.author, req.content)
    )
    conn.commit()
    return {"success": True, "id": post_id}

@app.post("/api/posts/{post_id}/like")
async def like_post(post_id: str):
    conn.execute("UPDATE posts SET likes = likes + 1 WHERE id = ?", (post_id,))
    conn.commit()
    return {"success": True}

# --- Socket.io 营火大厅与语音 ---
@sio.event
async def connect(sid, environ):
    await sio.emit("lobby:list", get_public_rooms(), to=sid)

@sio.on("room:create")
async def handle_create_room(sid, data):
    await leave_current_room(sid)
    room_id = "".join(random.choices(string.ascii_lowercase + string.digits, k=6))
    while room_id in active_rooms:
        room_id = "".join(random.choices(string.ascii_lowercase + string.digits, k=6))

    nickname = (data.get("nickname") or "神秘队长").strip()[:12]
    title = (data.get("title") or "泽风岛开荒求组").strip()[:24]
    password = (data.get("password") or "").strip()
    is_hidden = bool(data.get("isHidden", False))

    new_room = {
        "id": room_id, "title": title, "faction": data.get("faction", "horde"),
        "leader": nickname, "leaderId": sid, "password": password, "isHidden": is_hidden,
        "maxMembers": 5, "members": [{"id": sid, "name": nickname, "role": data.get("role", "⚔️ 输出"), "inVoice": False}]
    }
    active_rooms[room_id] = new_room
    sid_to_room[sid] = room_id
    await sio.enter_room(sid, room_id)
    await sio.emit("lobby:list", get_public_rooms())
    await sio.emit("room:joined", get_room_detail(new_room), to=sid)

@sio.on("room:join")
async def handle_join_room(sid, data):
    room_id = (data.get("roomId") or "").strip().lower()
    room = active_rooms.get(room_id)
    if not room:
        await sio.emit("room:error", "❌ 未找到该房间号，可能已解散！", to=sid)
        return
    if len(room["members"]) >= room["maxMembers"]:
        await sio.emit("room:error", "队伍人数已满 (5/5)", to=sid)
        return
    if room.get("password") and data.get("password", "").strip() != room["password"]:
        await sio.emit("room:error", "🔒 房间密码错误！", to=sid)
        return

    await leave_current_room(sid)
    nickname = (data.get("nickname") or "热心队友").strip()[:12]
    role = data.get("role", "⚔️ 输出")
    room["members"].append({"id": sid, "name": nickname, "role": role, "inVoice": False})
    sid_to_room[sid] = room_id

    await sio.enter_room(sid, room_id)
    await sio.emit("room:updated", get_room_detail(room), room=room_id)
    await sio.emit("room:joined", get_room_detail(room), to=sid)
    await sio.emit("room:chat", {"sender": "系统", "text": f"🎉 {nickname} ({role}) 加入了小队！", "isSystem": True}, room=room_id)
    await sio.emit("lobby:list", get_public_rooms())

@sio.on("room:kick")
async def handle_kick_member(sid, data):
    room_id = sid_to_room.get(sid)
    if not room_id or room_id not in active_rooms: return
    room = active_rooms[room_id]
    if room["leaderId"] != sid: return
    target_id = data.get("targetId")
    room["members"] = [m for m in room["members"] if m["id"] != target_id]
    sid_to_room.pop(target_id, None)
    await sio.leave_room(target_id, room_id)
    await sio.emit("room:kicked", "⚠️ 你已被队长移出小队。", to=target_id)
    await sio.emit("room:updated", get_room_detail(room), room=room_id)
    await sio.emit("lobby:list", get_public_rooms())

@sio.on("voice:join")
async def handle_voice_join(sid):
    room_id = sid_to_room.get(sid)
    if not room_id or room_id not in active_rooms: return
    room = active_rooms[room_id]
    for m in room["members"]:
        if m["id"] == sid: m["inVoice"] = True
    await sio.emit("room:updated", get_room_detail(room), room=room_id)
    await sio.emit("voice:peer-joined", {"peerId": sid}, room=room_id, skip_sid=sid)

@sio.on("voice:leave")
async def handle_voice_leave(sid):
    room_id = sid_to_room.get(sid)
    if not room_id or room_id not in active_rooms: return
    room = active_rooms[room_id]
    for m in room["members"]:
        if m["id"] == sid: m["inVoice"] = False
    await sio.emit("room:updated", get_room_detail(room), room=room_id)
    await sio.emit("voice:peer-left", {"peerId": sid}, room=room_id, skip_sid=sid)

@sio.on("voice:signal")
async def handle_voice_signal(sid, data):
    if data.get("toPeerId"):
        await sio.emit("voice:signal", {"fromPeerId": sid, "signal": data.get("signal")}, to=data.get("toPeerId"))

@sio.on("room:chat")
async def handle_room_chat(sid, data):
    room_id = sid_to_room.get(sid)
    if not room_id or room_id not in active_rooms: return
    room = active_rooms[room_id]
    member = next((m for m in room["members"] if m["id"] == sid), None)
    text = (data.get("text") or "").strip()[:200]
    if text:
        await sio.emit("room:chat", {"sender": member["name"] if member else "队友", "text": text, "isSystem": False}, room=room_id)

@sio.on("room:leave")
async def handle_leave_room(sid):
    await leave_current_room(sid)
    await sio.emit("room:left", to=sid)

async def leave_current_room(sid):
    room_id = sid_to_room.pop(sid, None)
    if room_id and room_id in active_rooms:
        room = active_rooms[room_id]
        room["members"] = [m for m in room["members"] if m["id"] != sid]
        await sio.leave_room(sid, room_id)
        await sio.emit("voice:peer-left", {"peerId": sid}, room=room_id)
        if len(room["members"]) == 0:
            del active_rooms[room_id]
        else:
            if room["leaderId"] == sid:
                new_leader = room["members"][0]
                room["leaderId"] = new_leader["id"]
                room["leader"] = new_leader["name"]
            await sio.emit("room:updated", get_room_detail(room), room=room_id)
        await sio.emit("lobby:list", get_public_rooms())

@sio.event
async def disconnect(sid):
    await leave_current_room(sid)

app.mount("/", StaticFiles(directory="public", html=True), name="static")

if __name__ == "__main__":
    uvicorn.run(socket_app, host="0.0.0.0", port=3000)