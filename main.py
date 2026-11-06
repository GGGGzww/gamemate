import random
import string
import sqlite3
from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
import socketio
import uvicorn

# 初始化 SQLite 数据库（存配装短链）
conn = sqlite3.connect("gamemate.db", check_same_thread=False)
conn.execute("PRAGMA journal_mode=WAL;")
conn.execute("""
             CREATE TABLE IF NOT EXISTS builds (
                                                   id TEXT PRIMARY KEY,
                                                   class_name TEXT,
                                                   talent_code TEXT,
                                                   created_at DATETIME DEFAULT CURRENT_TIMESTAMP
             )
             """)
conn.commit()

# 创建 FastAPI 与 Socket.io 服务
app = FastAPI(title="GameMate.fun Backend")
sio = socketio.AsyncServer(async_mode="asgi", cors_allowed_origins="*")
socket_app = socketio.ASGIApp(sio, other_asgi_app=app)


class BuildReq(BaseModel):
    className: str
    talentCode: str


@app.post("/api/build")
async def create_build(req: BuildReq):
    short_id = "".join(
        random.choices(string.ascii_lowercase + string.digits, k=6)
    )
    conn.execute(
        "INSERT INTO builds (id, class_name, talent_code) VALUES (?, ?, ?)",
        (short_id, req.className, req.talentCode),
    )
    conn.commit()
    return {
        "success": True,
        "shortUrl": f"https://gamemate.fun/?b={short_id}",
        "id": short_id,
    }


@app.get("/api/build/{build_id}")
async def get_build(build_id: str):
    cur = conn.execute(
        "SELECT id, class_name, talent_code FROM builds WHERE id = ?", (build_id,)
    )
    row = cur.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="未找到该配装")
    return {
        "success": True,
        "data": {"id": row[0], "className": row[1], "talentCode": row[2]},
    }


# 实时营火大厅与 WebRTC 语音信令
active_rooms = {}
sid_to_room = {}


@sio.event
async def connect(sid, environ):
    await sio.emit("lobby:list", list(active_rooms.values()), to=sid)


@sio.on("room:create")
async def handle_create_room(sid, data):
    room_id = "room_" + "".join(random.choices(string.ascii_lowercase, k=5))
    new_room = {
        "id": room_id,
        "title": data.get("title", "泽风岛开荒求组"),
        "faction": data.get("faction", "horde"),
        "leader": data.get("nickname", "神秘队长"),
        "maxMembers": 5,
        "members": [{
            "id": sid,
            "name": data.get("nickname", "队长"),
            "role": data.get("role", "DPS"),
        }],
    }
    active_rooms[room_id] = new_room
    sid_to_room[sid] = room_id
    await sio.enter_room(sid, room_id)
    await sio.emit("lobby:list", list(active_rooms.values()))
    await sio.emit("room:joined", new_room, to=sid)


@sio.on("room:join")
async def handle_join_room(sid, data):
    room_id = data.get("roomId")
    room = active_rooms.get(room_id)
    if not room or len(room["members"]) >= room["maxMembers"]:
        await sio.emit("room:error", "队伍已满或已解散", to=sid)
        return
    nickname = data.get("nickname", "队友")
    room["members"].append(
        {"id": sid, "name": nickname, "role": data.get("role", "DPS")}
    )
    sid_to_room[sid] = room_id
    await sio.enter_room(sid, room_id)
    await sio.emit(
        "voice:peer-joined",
        {"peerId": sid, "name": nickname},
        room=room_id,
        skip_sid=sid,
    )
    await sio.emit("room:joined", room, to=sid)
    await sio.emit("lobby:list", list(active_rooms.values()))


@sio.on("voice:signal")
async def handle_voice_signal(sid, data):
    target_sid = data.get("toPeerId")
    if target_sid:
        await sio.emit(
            "voice:signal",
            {"fromPeerId": sid, "signalData": data.get("signalData")},
            to=target_sid,
        )


@sio.event
async def disconnect(sid):
    room_id = sid_to_room.pop(sid, None)
    if room_id and room_id in active_rooms:
        room = active_rooms[room_id]
        room["members"] = [m for m in room["members"] if m["id"] != sid]
        await sio.emit("voice:peer-left", {"peerId": sid}, room=room_id)
        if len(room["members"]) == 0:
            del active_rooms[room_id]
        await sio.emit("lobby:list", list(active_rooms.values()))


# 挂载前端静态页面
app.mount("/", StaticFiles(directory="public", html=True), name="static")

if __name__ == "__main__":
    uvicorn.run(socket_app, host="0.0.0.0", port=3000)