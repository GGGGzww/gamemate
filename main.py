import os
import random
import string
import sqlite3
from datetime import datetime
from typing import Optional
from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import socketio
import uvicorn

DB_PATH = os.environ.get("GAMEMATE_DB", "gamemate.db")

# 1. 初始化 SQLite 数据库（开启 WAL 高并发模式，与爬虫/AI共用同一库）
conn = sqlite3.connect(DB_PATH, check_same_thread=False)
conn.row_factory = sqlite3.Row
conn.execute("PRAGMA journal_mode=WAL;")
conn.execute("""
CREATE TABLE IF NOT EXISTS builds (
    id TEXT PRIMARY KEY,
    class_name TEXT NOT NULL,
    race_name TEXT DEFAULT '亡灵',
    spec_summary TEXT DEFAULT '',
    talent_code TEXT NOT NULL,
    author TEXT DEFAULT '匿名开荒者',
    likes INTEGER DEFAULT 1,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
)
""")
conn.commit()

# 初始化几条默认热门配装（若表为空）
cur = conn.execute("SELECT COUNT(*) as cnt FROM builds")
if cur.fetchone()["cnt"] == 0:
    seed_builds = [
        ("pal88a", "圣骑士 (亡灵骑)", "亡灵", "神圣 31 / 防护 20 / 惩戒 0", "paladin|horde|亡灵|31-20-0|552103-530201-000000", "灰烬使者", 128),
        ("sha72b", "萨满祭司 (矮人萨)", "矮人", "增强 30 / 恢复 21 / 元素 0", "shaman|alliance|矮人|30-21-0|550320-523102-000000", "铜须雷锤", 96),
        ("ast99c", "星术师 (天裔)", "天裔 (新种族)", "星界 35 / 秩序 16 / 虚空 0", "astrologer|alliance|天裔 (新种族)|35-16-0|555320-531000-000000", "星穹观测者", 84),
        ("war61d", "战士", "兽人", "狂暴 34 / 武器 17 / 防护 0", "warrior|horde|兽人|34-17-0|554320-532000-000000", "洛克塔尔", 73),
    ]
    conn.executemany(
        "INSERT INTO builds (id, class_name, race_name, spec_summary, talent_code, author, likes) VALUES (?, ?, ?, ?, ?, ?, ?)",
        seed_builds
    )
    conn.commit()

# 2. 创建 FastAPI 实例与 Socket.io 异步服务
app = FastAPI(title="GameMate.fun Backend", version="2.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

sio = socketio.AsyncServer(
    async_mode="asgi",
    cors_allowed_origins="*",
    ping_interval=25,
    ping_timeout=60
)
socket_app = socketio.ASGIApp(sio, other_asgi_app=app)


# 3. 数据模型定义
class BuildReq(BaseModel):
    className: str
    raceName: Optional[str] = "亡灵"
    specSummary: Optional[str] = ""
    talentCode: str
    author: Optional[str] = "匿名开荒者"


class AIQuestionReq(BaseModel):
    question: str
    contextClass: Optional[str] = ""


# 4. REST API：保存、读取与排行天赋配装短链
@app.post("/api/build")
async def create_build(req: BuildReq):
    short_id = "".join(random.choices(string.ascii_lowercase + string.digits, k=6))
    conn.execute(
        "INSERT INTO builds (id, class_name, race_name, spec_summary, talent_code, author) VALUES (?, ?, ?, ?, ?, ?)",
        (short_id, req.className, req.raceName or "亡灵", req.specSummary or "", req.talentCode, req.author or "匿名开荒者"),
    )
    conn.commit()
    return {
        "success": True,
        "shortUrl": f"https://gamemate.fun/?b={short_id}",
        "id": short_id,
    }


@app.get("/api/builds/hot")
async def get_hot_builds():
    cur = conn.execute(
        "SELECT id, class_name, race_name, spec_summary, talent_code, author, likes, created_at FROM builds ORDER BY likes DESC, created_at DESC LIMIT 12"
    )
    rows = cur.fetchall()
    return {
        "success": True,
        "items": [
            {
                "id": r["id"],
                "className": r["class_name"],
                "raceName": r["race_name"],
                "specSummary": r["spec_summary"],
                "talentCode": r["talent_code"],
                "author": r["author"],
                "likes": r["likes"],
                "createdAt": r["created_at"],
            }
            for r in rows
        ],
    }


@app.get("/api/build/{build_id}")
async def get_build(build_id: str):
    cur = conn.execute(
        "SELECT id, class_name, race_name, spec_summary, talent_code, author, likes, created_at FROM builds WHERE id = ?",
        (build_id,),
    )
    row = cur.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="未找到该配装")
    return {
        "success": True,
        "data": {
            "id": row["id"],
            "className": row["class_name"],
            "raceName": row["race_name"],
            "specSummary": row["spec_summary"],
            "talentCode": row["talent_code"],
            "author": row["author"],
            "likes": row["likes"],
        },
    }


@app.post("/api/build/{build_id}/like")
async def like_build(build_id: str):
    conn.execute("UPDATE builds SET likes = likes + 1 WHERE id = ?", (build_id,))
    conn.commit()
    cur = conn.execute("SELECT likes FROM builds WHERE id = ?", (build_id,))
    row = cur.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="未找到该配装")
    return {"success": True, "likes": row["likes"]}


# 5. AI 副本攻略问答接口（内置《魔兽世界：无限》高频知识库 + 支持扩展 DeepSeek/OpenAI）
KNOWLEDGE_BASE = {
    "亡灵": "【亡灵圣骑开荒指南】在《魔兽世界：无限》中，亡灵圣骑拥有专属被动「凋零圣印」（神圣法术暴击后附加暗影持续恢复）。海加尔山与风暴神龛开荒推荐主修神圣31/防护20，优先堆叠法术暴击至22%与五回属性，饰品首选泽风岛声望崇敬「潮汐圣契」。",
    "矮人": "【矮人萨满开荒指南】矮人萨满凭借「石像形态」免疫流血毒药，在河林新五人本「翡翠梦境裂隙」担任近战增强萨或团刷恢复萨极佳。增强流推荐30/21/0点出风怒图腾强化，武器附魔十字军。",
    "天裔": "【天裔新种族与星术师指南】天裔自带「星界跃迁」（1.5分钟CD短距瞬移）与1%奥术/自然急速。新职业星术师建议点满「星界35/秩序16」，在团本海加尔峰1号Boss卡兹洛加注意预留星落爆发清理地狱火。",
    "泽风岛": "【泽风岛探索路线】登岛先去暴风港/锈水码头解锁飞行点（坐标 42, 68）➔ 清理珊瑚海湾精英鱼人拿「风暴钥匙」碎片 ➔ 前往西北角雷鸣高地开启3个符文宝箱，可直接获得16格开荒背包与装绑蓝武。",
    "海加尔": "【海加尔峰团本开门任务链】需先达到60级并完成河林「翡翠之眼」前置 ➔ 击败风暴神龛尾王拾取「永恒水瓶印记」➔ 护送海加尔山先知麦迪文残影抵御4波燃烧军团突袭即可解锁团本入口。",
    "语音": "【营火大厅语音提示】网页内置 WebRTC 5人小队P2P直连语音（需通过 HTTPS 或 localhost 访问以授权麦克风），音频流在队友浏览器间点对点加密传输，零服务器带宽占用；10人以上团本建议同时附上 OOPZ 或 YY 频道链接。",
}


@app.post("/api/ai-guide")
async def ask_ai_guide(req: AIQuestionReq):
    q = req.question.strip()
    matched = []
    for kw, ans in KNOWLEDGE_BASE.items():
        if kw in q or (req.contextClass and kw in req.contextClass):
            matched.append(ans)
    if matched:
        reply = "\n\n".join(matched)
    else:
        reply = (
            f"【AI开荒战术官】收到关于「{q}」的提问：\n"
            "1. 开荒期建议优先在【开荒雷达】完成泽风岛与河林的飞行点及3把钥匙前置；\n"
            "2. 队伍配置推荐携带至少1名亡灵圣骑（提供暗影抗性光环与双祝福）或矮人萨满（风怒/清毒图腾）；\n"
            "3. 你可以在【天赋与配装】保存当前流派短链，并直接一键同步到【英雄名片】生成招募海报。"
        )
    return {"success": True, "answer": reply, "timestamp": datetime.now().strftime("%H:%M:%S")}


# 6. Socket.io 实时营火大厅 + WebRTC P2P 语音信令服务
active_rooms = {}  # room_id -> room dict
sid_to_room = {}   # sid -> room_id


@sio.event
async def connect(sid, environ):
    await sio.emit("lobby:list", list(active_rooms.values()), to=sid)


@sio.on("room:create")
async def handle_create_room(sid, data):
    # 若已在旧房间，先安全退出
    await _leave_current_room(sid)

    room_id = "room_" + "".join(random.choices(string.ascii_lowercase + string.digits, k=5))
    nickname = (data.get("nickname") or "神秘队长")[:16]
    role = data.get("role") or "TANK"
    class_name = data.get("className") or "圣骑士"

    new_room = {
        "id": room_id,
        "title": (data.get("title") or "泽风岛精英任务求组")[:32],
        "activity": data.get("activity") or "五人本开荒",
        "faction": data.get("faction") or "horde",
        "leader": nickname,
        "leaderSid": sid,
        "maxMembers": int(data.get("maxMembers") or 5),
        "voiceType": data.get("voiceType") or "webrtc",
        "voiceUrl": (data.get("voiceUrl") or "")[:120],
        "buildId": (data.get("buildId") or "")[:12],
        "createdAt": datetime.now().strftime("%H:%M"),
        "members": [
            {
                "id": sid,
                "name": nickname,
                "role": role,
                "className": class_name,
                "muted": False,
            }
        ],
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

    if sid_to_room.get(sid) == room_id:
        await sio.emit("room:joined", room, to=sid)
        return

    await _leave_current_room(sid)

    nickname = (data.get("nickname") or "热心队友")[:16]
    role = data.get("role") or "DPS"
    class_name = data.get("className") or "法师"

    member_obj = {
        "id": sid,
        "name": nickname,
        "role": role,
        "className": class_name,
        "muted": False,
    }
    room["members"].append(member_obj)
    sid_to_room[sid] = room_id
    await sio.enter_room(sid, room_id)

    # 通知房间内其他人发起 WebRTC 语音连接
    await sio.emit(
        "voice:peer-joined",
        {"peerId": sid, "name": nickname, "role": role, "className": class_name},
        room=room_id,
        skip_sid=sid,
    )
    await sio.emit("room:joined", room, to=sid)
    await sio.emit("room:updated", room, room=room_id)
    await sio.emit("lobby:list", list(active_rooms.values()))


@sio.on("room:leave")
async def handle_leave_room(sid, data=None):
    await _leave_current_room(sid)
    await sio.emit("room:left", {"ok": True}, to=sid)


@sio.on("voice:mute-status")
async def handle_mute_status(sid, data):
    room_id = sid_to_room.get(sid)
    if room_id and room_id in active_rooms:
        room = active_rooms[room_id]
        muted = bool(data.get("muted", False))
        for m in room["members"]:
            if m["id"] == sid:
                m["muted"] = muted
                break
        await sio.emit("room:updated", room, room=room_id)


@sio.on("room:chat")
async def handle_room_chat(sid, data):
    room_id = sid_to_room.get(sid)
    if room_id and room_id in active_rooms:
        room = active_rooms[room_id]
        sender = next((m["name"] for m in room["members"] if m["id"] == sid), "队友")
        text = (data.get("text") or "").strip()[:200]
        if text:
            await sio.emit(
                "room:chat-msg",
                {
                    "sender": sender,
                    "text": text,
                    "time": datetime.now().strftime("%H:%M:%S"),
                },
                room=room_id,
            )


@sio.on("voice:signal")
async def handle_voice_signal(sid, data):
    target_sid = data.get("toPeerId")
    if target_sid:
        await sio.emit(
            "voice:signal",
            {"fromPeerId": sid, "signalData": data.get("signalData")},
            to=target_sid,
        )


async def _leave_current_room(sid):
    room_id = sid_to_room.pop(sid, None)
    if room_id and room_id in active_rooms:
        room = active_rooms[room_id]
        room["members"] = [m for m in room["members"] if m["id"] != sid]
        await sio.leave_room(sid, room_id)
        await sio.emit("voice:peer-left", {"peerId": sid}, room=room_id)

        if len(room["members"]) == 0:
            del active_rooms[room_id]
        else:
            if room.get("leaderSid") == sid:
                room["leaderSid"] = room["members"][0]["id"]
                room["leader"] = room["members"][0]["name"]
            await sio.emit("room:updated", room, room=room_id)
        await sio.emit("lobby:list", list(active_rooms.values()))


@sio.event
async def disconnect(sid):
    await _leave_current_room(sid)


# 7. 挂载前端静态网页（放在最后，将 public 文件夹下的 index.html 映射为首页）
if os.path.exists("public"):
    app.mount("/", StaticFiles(directory="public", html=True), name="static")

if __name__ == "__main__":
    uvicorn.run(socket_app, host="0.0.0.0", port=3000)
