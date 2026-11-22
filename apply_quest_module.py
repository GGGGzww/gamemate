#!/usr/bin/env python3
"""一键将 1-60 级全地图任务图鉴整合进 main.py 与 public/index.html（自动备份 + 防重复）"""

import py_compile
import shutil
import sys
from pathlib import Path

MAIN_PATH = Path("/opt/gamemate/main.py")
HTML_PATH = Path("/opt/gamemate/public/index.html")

# ==================== 1. 后端补丁内容 ====================
MAIN_ANCHOR = '''# --- 秘典与宠物图鉴 API ---
@app.get("/api/codex")
async def get_codex():
    return {"success": True, "data": {"items": [], "companions": []}}'''

MAIN_PATCH = '''# --- 全地图任务图鉴 API (1-60级 pfQuest 数据) ---
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
    return {"success": True, "screenshots": imgs}'''

# ==================== 2. 前端补丁内容 ====================
NAV_ANCHOR = '''      <button onclick="switchTab('guide')" id="tab-guide" class="gm-nav-btn">
        <span class="nav-icon-box">📜</span>
        <span class="min-w-0">
          <span class="block nav-label">秘典·装备·宠物</span>
          <span class="block nav-sub">模拟配装·神兵与宝宝库</span>
        </span>
      </button>'''

NAV_PATCH = '''
      <button onclick="switchTab('quests')" id="tab-quests" class="gm-nav-btn">
        <span class="nav-icon-box">🗺️</span>
        <span class="min-w-0">
          <span class="block nav-label">任务图鉴</span>
          <span class="block nav-sub">1-60全图坐标 · 前置链</span>
        </span>
      </button>'''

SEC_ANCHOR = '  <!-- 4. 逐风酒馆论坛模块 -->'
SEC_PATCH = '''  <!-- 3.5 全地图任务图鉴模块 (1-60级 4400+任务) -->
  <section id="sec-quests" class="hidden flex-1 flex flex-col">
    <div class="epic-panel p-5 sm:p-6 space-y-5 flex-1 flex flex-col">
      <div class="flex flex-col md:flex-row md:items-center justify-between gap-3 border-b border-amber-500/20 pb-4">
        <div>
          <div class="flex flex-wrap items-center gap-2">
            <h2 class="wow-serif text-xl text-amber-300 font-bold">🗺️ 艾泽拉斯 1-60 级全地图任务图鉴</h2>
            <span id="quest-total-badge" class="text-[11px] font-mono px-2.5 py-0.5 rounded-full q-rare font-bold">载入中…</span>
          </div>
          <p class="text-xs text-gray-400 mt-1">支持按地图/阵营/等级/关键词检索，查看前置与后续任务链、TomTom 坐标一键复制及真实游戏截图上传</p>
        </div>
        <button onclick="openRxpModal()" class="px-4 py-2 text-xs gold-btn cursor-pointer shrink-0">📥 导入 RXP / 升级路线</button>
      </div>

      <div class="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-12 gap-2.5 bg-[#080b12] p-3.5 rounded-xl border border-amber-500/25 text-xs">
        <div class="lg:col-span-3">
          <select id="q-filter-zone" onchange="searchQuests(1)" class="w-full px-3 py-2 font-semibold">
            <option value="">🌍 全部地图区域</option>
          </select>
        </div>
        <div class="lg:col-span-2">
          <select id="q-filter-faction" onchange="searchQuests(1)" class="w-full px-3 py-2 font-semibold">
            <option value="">⚔️ 全部阵营</option>
            <option value="alliance">🔵 联盟</option>
            <option value="horde">🔴 部落</option>
            <option value="both">⚖️ 通用 / 中立</option>
          </select>
        </div>
        <div class="lg:col-span-2">
          <select id="q-filter-lvl" onchange="searchQuests(1)" class="w-full px-3 py-2 font-semibold">
            <option value="1-60">📊 全部等级 (1-60)</option>
            <option value="1-20">🌱 1 - 20 级</option>
            <option value="21-40">⚔️ 21 - 40 级</option>
            <option value="41-55">🛡️ 41 - 55 级</option>
            <option value="56-60">👑 56 - 60 级满级</option>
          </select>
        </div>
        <div class="lg:col-span-5 flex gap-2">
          <input type="text" id="q-filter-keyword" placeholder="搜索任务名、NPC、地图或任务 ID…" onkeydown="if(event.key==='Enter') searchQuests(1)" class="flex-1 min-w-0 px-3 py-2">
          <button onclick="searchQuests(1)" class="px-4 py-2 gold-btn cursor-pointer shrink-0">🔍 搜索</button>
        </div>
      </div>

      <div class="grid grid-cols-1 lg:grid-cols-12 gap-5 flex-1 items-start">
        <div class="lg:col-span-5 bg-[#080b12] rounded-xl border border-amber-500/25 p-3.5 flex flex-col gap-3">
          <div id="quest-list-box" class="space-y-2 max-h-[620px] overflow-y-auto pr-1"></div>
          <div class="flex items-center justify-between pt-2.5 border-t border-amber-500/15 text-xs">
            <button id="q-prev-btn" onclick="changeQuestPage(-1)" class="px-3 py-1.5 ghost-btn cursor-pointer disabled:opacity-40">← 上一页</button>
            <span id="q-page-info" class="font-mono text-gray-400">第 1 / 1 页</span>
            <button id="q-next-btn" onclick="changeQuestPage(1)" class="px-3 py-1.5 ghost-btn cursor-pointer disabled:opacity-40">下一页 →</button>
          </div>
        </div>

        <div id="quest-detail-pane" class="lg:col-span-7 bg-[#080b12] rounded-xl border border-amber-500/25 p-5 min-h-[540px] flex flex-col justify-between space-y-5">
          <div class="text-center py-20 text-gray-400 text-sm">👈 点击左侧任意任务，查看前置链、精准坐标与实景截图</div>
        </div>
      </div>

      <p class="text-[11px] text-gray-500 text-center">基础 1-60 级任务与坐标数据参考开源项目 pfQuest (MIT)，魔兽世界相关游戏内容版权归暴雪娱乐所有。</p>
    </div>
  </section>

  <!-- RXP / 路线指南解析弹窗 -->
  <div id="rxp-modal" onclick="if(event.target.id==='rxp-modal') closeRxpModal()" class="hidden fixed inset-0 z-[82] flex items-center justify-center bg-black/80 backdrop-blur-md p-4">
    <div class="gm-modal-panel max-w-2xl w-full p-6 space-y-4 max-h-[88vh] flex flex-col">
      <div class="flex items-start justify-between gap-3 border-b border-amber-500/20 pb-3">
        <div>
          <h3 class="wow-serif text-lg text-amber-300 font-bold">📥 导入 RestedXP (RXP) 开源升级路线</h3>
          <p class="text-xs text-gray-400 mt-0.5">支持解析标准 RXP 语法（step / .goto / .accept / .complete / .turnin / >>提示），自动生成可勾选清单与 /way 坐标</p>
        </div>
        <button onclick="closeRxpModal()" class="w-8 h-8 rounded-lg text-gray-400 hover:text-amber-200 cursor-pointer">✕</button>
      </div>
      <textarea id="rxp-input" rows="6" placeholder="粘贴 RXP 指南脚本，或点击下方「载入示例」体验…" class="w-full p-3 text-xs font-mono"></textarea>
      <div class="flex flex-wrap items-center gap-2">
        <button onclick="parseRxpScript()" class="px-4 py-2 text-xs gold-btn cursor-pointer">⚡ 解析路线</button>
        <button onclick="loadRxpDemo()" class="px-3.5 py-2 text-xs ghost-btn cursor-pointer">📝 载入示例脚本</button>
      </div>
      <div id="rxp-result-box" class="flex-1 overflow-y-auto space-y-2 pr-1"></div>
    </div>
  </div>

'''

JS_ANCHOR = '  window.onload = async () => {'
JS_PATCH = '''  // =========================================================================
  // 🗺️ 全地图任务图鉴 (1-60级 SQLite 分页 + 任务链 + 实景截图 + RXP 解析)
  // =========================================================================
  let questModuleInited = false;
  let questPage = 1;
  let questTotalPages = 1;
  let currentQuestId = null;
  let currentQuestData = null;

  async function initQuestsModule() {
    if (!questModuleInited) {
      questModuleInited = true;
      await loadQuestZones();
      await searchQuests(1);
    }
  }

  async function loadQuestZones() {
    try {
      const res = await apiFetch('/api/quests/zones');
      const json = await res.json();
      const sel = document.getElementById('q-filter-zone');
      if (!sel || !json.success) return;
      sel.innerHTML = '<option value="">🌍 全部地图区域</option>' +
        json.data.map(z => `<option value="${escapeHtml(z.zone)}">${escapeHtml(z.zone)} (${z.count})</option>`).join('');
    } catch (e) { console.error(e); }
  }

  async function searchQuests(page = 1) {
    questPage = Math.max(1, page);
    const zone = document.getElementById('q-filter-zone')?.value || '';
    const faction = document.getElementById('q-filter-faction')?.value || '';
    const lvlRange = (document.getElementById('q-filter-lvl')?.value || '1-60').split('-');
    const q = (document.getElementById('q-filter-keyword')?.value || '').trim();
    const qs = new URLSearchParams({
      zone, faction, q,
      min_level: lvlRange[0] || '1',
      max_level: lvlRange[1] || '60',
      page: String(questPage),
      page_size: '30',
    });

    const box = document.getElementById('quest-list-box');
    if (box) box.innerHTML = '<div class="text-center py-10 text-xs text-gray-400">正在检索任务库…</div>';
    try {
      const res = await apiFetch('/api/quests?' + qs.toString());
      const json = await res.json();
      if (!json.success) return;
      const { items, total, totalPages } = json.data;
      questTotalPages = totalPages || 1;
      const badge = document.getElementById('quest-total-badge');
      if (badge) badge.textContent = `共 ${total.toLocaleString()} 条任务`;
      document.getElementById('q-page-info').textContent = `第 ${questPage} / ${questTotalPages} 页`;
      document.getElementById('q-prev-btn').disabled = questPage <= 1;
      document.getElementById('q-next-btn').disabled = questPage >= questTotalPages;

      if (!items.length) {
        box.innerHTML = '<div class="text-center py-12 text-xs text-gray-500">未找到匹配的任务，试试放宽筛选条件</div>';
        return;
      }
      box.innerHTML = items.map(it => {
        const fTag = it.faction === 'alliance'
          ? '<span class="px-1.5 py-0.5 rounded bg-blue-500/20 text-blue-300 text-[10px]">联盟</span>'
          : it.faction === 'horde'
            ? '<span class="px-1.5 py-0.5 rounded bg-red-500/20 text-red-300 text-[10px]">部落</span>'
            : '<span class="px-1.5 py-0.5 rounded bg-amber-500/15 text-amber-300 text-[10px]">通用</span>';
        const active = it.id === currentQuestId ? 'border-amber-400 bg-amber-500/15' : '';
        return `
          <div onclick="loadQuestDetail(${it.id})" class="loot-card p-3 cursor-pointer space-y-1 transition ${active}">
            <div class="flex items-center justify-between gap-2">
              <div class="flex items-center gap-1.5 min-w-0">
                <span class="font-mono text-[11px] font-bold text-amber-300 shrink-0">[${it.level}]</span>
                <span class="text-sm font-bold text-amber-100 truncate">${escapeHtml(it.title)}</span>
              </div>
              ${fTag}
            </div>
            <div class="flex items-center justify-between text-[11px] text-gray-400">
              <span class="truncate">📍 ${escapeHtml(it.zone)}${it.giverCoord ? ` (${escapeHtml(it.giverCoord)})` : ''}</span>
              <span class="shrink-0 space-x-1">
                ${it.hasPrereq ? '<span title="有前置任务">⛓️</span>' : ''}
                ${it.shotCount ? `<span title="含实景截图">🖼️${it.shotCount}</span>` : ''}
              </span>
            </div>
          </div>`;
      }).join('');

      if (!currentQuestId && items.length) {
        loadQuestDetail(items[0].id);
      }
    } catch (e) {
      if (box) box.innerHTML = '<div class="text-center py-10 text-xs text-red-300">任务列表加载失败</div>';
    }
  }

  function changeQuestPage(delta) {
    const next = questPage + delta;
    if (next < 1 || next > questTotalPages) return;
    searchQuests(next);
  }

  async function loadQuestDetail(questId) {
    currentQuestId = questId;
    const pane = document.getElementById('quest-detail-pane');
    if (pane) pane.innerHTML = '<div class="text-center py-20 text-xs text-gray-400">正在加载任务详情…</div>';
    try {
      const res = await apiFetch(`/api/quests/${questId}`);
      const json = await res.json();
      if (!json.success) return;
      currentQuestData = json.data;
      renderQuestDetail(json.data);
    } catch (e) {
      if (pane) pane.innerHTML = '<div class="text-center py-20 text-xs text-red-300">任务详情加载失败</div>';
    }
  }

  function copyWayMacro(zone, coord, npc) {
    if (!coord) { showToast('该节点暂无坐标数据', 'info'); return; }
    const cmd = `/way ${zone} ${coord} ${npc || ''}`.trim();
    if (navigator.clipboard && navigator.clipboard.writeText) {
      navigator.clipboard.writeText(cmd).then(() => showToast(`✅ 已复制坐标宏：${cmd}`, 'success')).catch(() => {});
    } else {
      showToast(`坐标宏：${cmd}`, 'info');
    }
  }

  function copyQuestWay(side) {
    if (!currentQuestData) return;
    const q = currentQuestData;
    if (side === 'turnin') {
      copyWayMacro(q.zone, q.turninCoord || q.giverCoord, q.turninNpc || q.giverNpc);
    } else {
      copyWayMacro(q.zone, q.giverCoord, q.giverNpc);
    }
  }

  function renderQuestDetail(q) {
    const pane = document.getElementById('quest-detail-pane');
    if (!pane) return;
    const fLabel = q.faction === 'alliance' ? '🔵 联盟专属' : q.faction === 'horde' ? '🔴 部落专属' : '⚖️ 联盟 / 部落通用';

    const chainBtn = (n, dir) => `
      <button onclick="loadQuestDetail(${n.id})" class="px-2.5 py-1.5 rounded-lg ghost-btn text-xs text-left cursor-pointer flex items-center gap-1.5">
        <span>${dir === 'pre' ? '⬅️' : '➡️'}</span>
        <span class="font-mono text-amber-300">[${n.level}]</span>
        <span class="font-bold text-amber-100">${escapeHtml(n.title)}</span>
      </button>`;

    const prereqHtml = q.prereqs.length
      ? q.prereqs.map(n => chainBtn(n, 'pre')).join('')
      : '<span class="text-xs text-gray-500">无前置任务（可直接接取）</span>';

    const nextHtml = q.nextQuests.length
      ? q.nextQuests.map(n => chainBtn(n, 'next')).join('')
      : '<span class="text-xs text-gray-500">本任务为当前任务链终点</span>';

    const objsHtml = q.objectives.length
      ? q.objectives.map(o => `
          <label class="flex items-start gap-2 text-xs text-gray-200 cursor-pointer">
            <input type="checkbox" class="mt-0.5 accent-amber-500">
            <span>${escapeHtml(o)}</span>
          </label>`).join('')
      : '<p class="text-xs text-gray-400">与目标 NPC 对话或按任务描述完成即可。</p>';

    const shotsHtml = q.screenshots.length
      ? `<div class="grid grid-cols-2 sm:grid-cols-3 gap-2.5">${q.screenshots.map((src, idx) => `
          <div class="relative group rounded-xl overflow-hidden border border-amber-500/30 bg-black/40 h-28">
            <img src="${escapeHtml(src)}" onclick="openImageLightbox('${escapeHtml(src)}')" class="w-full h-full object-cover cursor-zoom-in" loading="lazy">
            ${currentUser ? `<button onclick="deleteQuestShot(${q.id}, ${idx})" class="absolute top-1 right-1 w-6 h-6 rounded-full bg-black/80 hover:bg-red-600 text-white text-xs cursor-pointer" title="删除截图">✕</button>` : ''}
          </div>`).join('')}</div>`
      : '<div class="text-xs text-gray-500 py-3">暂无实景截图，登录冒险者可点击右侧上传或直接 Ctrl+V 粘贴游戏截图。</div>';

    pane.innerHTML = `
      <div class="space-y-5">
        <div class="border-b border-amber-500/20 pb-3.5 space-y-1.5">
          <div class="flex flex-wrap items-center gap-2 text-xs">
            <span class="px-2.5 py-0.5 rounded-full q-legendary font-bold">Lv.${q.level} (需 ${q.requiredLevel} 级)</span>
            <span class="px-2.5 py-0.5 rounded-full q-rare font-semibold">📍 ${escapeHtml(q.zone)}</span>
            <span class="px-2.5 py-0.5 rounded-full bg-amber-500/15 text-amber-200">${fLabel}</span>
            <span class="font-mono text-gray-500 ml-auto">Quest ID: #${q.id}</span>
          </div>
          <h3 class="wow-serif text-xl sm:text-2xl font-black text-amber-100">${escapeHtml(q.title)}</h3>
          ${q.titleEn && q.titleEn !== q.title ? `<div class="text-xs text-gray-500 font-mono">${escapeHtml(q.titleEn)}</div>` : ''}
        </div>

        <div class="grid grid-cols-1 sm:grid-cols-2 gap-3">
          <div class="loot-card p-3.5 space-y-1.5">
            <div class="text-[11px] text-amber-300 font-bold">❗ 接取 NPC 与坐标</div>
            <div class="text-sm font-bold text-amber-100">${escapeHtml(q.giverNpc || '任务物品/自动触发')}</div>
            <div class="flex items-center justify-between gap-2 pt-1">
              <span class="text-xs font-mono text-emerald-400">${escapeHtml(q.giverCoord || '无固定坐标')}</span>
              ${q.giverCoord ? `<button onclick="copyQuestWay('giver')" class="px-2 py-0.5 rounded ghost-btn text-[11px] cursor-pointer">📋 复制 /way</button>` : ''}
            </div>
          </div>
          <div class="loot-card p-3.5 space-y-1.5">
            <div class="text-[11px] text-emerald-400 font-bold">❓ 交还 NPC 与坐标</div>
            <div class="text-sm font-bold text-amber-100">${escapeHtml(q.turninNpc || q.giverNpc || '同接取点')}</div>
            <div class="flex items-center justify-between gap-2 pt-1">
              <span class="text-xs font-mono text-emerald-400">${escapeHtml(q.turninCoord || q.giverCoord || '无固定坐标')}</span>
              ${(q.turninCoord || q.giverCoord) ? `<button onclick="copyQuestWay('turnin')" class="px-2 py-0.5 rounded ghost-btn text-[11px] cursor-pointer">📋 复制 /way</button>` : ''}
            </div>
          </div>
        </div>

        <div class="loot-card p-4 space-y-3">
          <div class="text-xs font-bold text-amber-300">⛓️ 任务链关系（点击可直接跳转）</div>
          <div class="space-y-1.5">
            <div class="text-[11px] text-gray-400">前置任务（需先完成）：</div>
            <div class="flex flex-wrap gap-2">${prereqHtml}</div>
          </div>
          <div class="space-y-1.5 pt-2 border-t border-amber-500/15">
            <div class="text-[11px] text-gray-400">后续解锁（它是以下任务的前置）：</div>
            <div class="flex flex-wrap gap-2">${nextHtml}</div>
          </div>
        </div>

        <div class="loot-card p-4 space-y-3">
          <div class="text-xs font-bold text-amber-300">📋 任务要求与说明</div>
          <div class="space-y-1.5">${objsHtml}</div>
          ${q.description ? `<p class="text-xs text-gray-300 leading-relaxed pt-2 border-t border-amber-500/15 whitespace-pre-line">${escapeHtml(q.description)}</p>` : ''}
        </div>

        <div class="loot-card p-4 space-y-3">
          <div class="flex items-center justify-between gap-2">
            <span class="text-xs font-bold text-amber-300">📸 任务点位实景截图 (${q.screenshots.length}/6)</span>
            <label class="px-3 py-1 rounded-lg ghost-btn text-xs cursor-pointer">
              ➕ 上传游戏截图
              <input type="file" accept="image/*" multiple class="hidden" onchange="uploadQuestShots(${q.id}, this.files)">
            </label>
          </div>
          ${shotsHtml}
        </div>
      </div>
    `;
  }

  async function uploadQuestShots(questId, fileList) {
    if (!currentUser) { openAuthModal('login'); return; }
    const files = Array.from(fileList || []).slice(0, 6);
    if (!files.length) return;
    const imgs = [];
    for (const f of files) {
      try { imgs.push(await compressImageToDataUrl(f)); } catch (e) {}
    }
    if (!imgs.length) return;
    try {
      const res = await apiFetch(`/api/quests/${questId}/screenshots`, {
        method: 'POST', headers: authHeaders(), body: JSON.stringify({ images: imgs })
      });
      const json = await res.json();
      if (!res.ok || !json.success) { showToast(json.detail || '上传失败', 'error'); return; }
      showToast('✅ 实景截图已保存！', 'success');
      loadQuestDetail(questId);
    } catch (e) { showToast('上传失败，请检查网络', 'error'); }
  }

  async function deleteQuestShot(questId, idx) {
    if (!currentUser) { openAuthModal('login'); return; }
    try {
      const res = await apiFetch(`/api/quests/${questId}/screenshots/${idx}`, {
        method: 'DELETE', headers: authHeaders()
      });
      if (res.ok) { showToast('🗑️ 截图已删除', 'success'); loadQuestDetail(questId); }
    } catch (e) {}
  }

  document.addEventListener('paste', (e) => {
    const sec = document.getElementById('sec-quests');
    if (!sec || sec.classList.contains('hidden') || !currentQuestId) return;
    if (['INPUT', 'TEXTAREA'].includes(document.activeElement?.tagName)) return;
    const items = (e.clipboardData && e.clipboardData.items) || [];
    const files = [];
    for (const it of items) {
      if (it.type && it.type.startsWith('image/')) {
        const f = it.getAsFile();
        if (f) files.push(f);
      }
    }
    if (files.length) uploadQuestShots(currentQuestId, files);
  });

  function openRxpModal() { document.getElementById('rxp-modal').classList.remove('hidden'); }
  function closeRxpModal() { document.getElementById('rxp-modal').classList.add('hidden'); }
  function loadRxpDemo() {
    document.getElementById('rxp-input').value = `step
    .goto Elwynn Forest,48.9,41.6
    .accept 783 >> 接取《身边的威胁》
step
    .goto Elwynn Forest,47.5,35.8
    .complete 783,1 >> 击杀 10 只森林狼
step
    .goto Elwynn Forest,48.9,41.6
    .turnin 783 >> 交还《身边的威胁》并接取后续`;
    parseRxpScript();
  }
  function parseRxpScript() {
    const raw = (document.getElementById('rxp-input')?.value || '').trim();
    const box = document.getElementById('rxp-result-box');
    if (!box) return;
    if (!raw) { box.innerHTML = '<div class="text-xs text-gray-500">请先粘贴 RXP 路线文本</div>'; return; }
    const steps = raw.split(/\\bstep\\b/i).map(s => s.trim()).filter(Boolean);
    box.innerHTML = steps.map((blk, i) => {
      const gotoM = blk.match(/\\.goto\\s+([^,\\n]+),\\s*([\\d.]+)\\s*,\\s*([\\d.]+)/i);
      const zone = gotoM ? gotoM[1].trim() : '';
      const coord = gotoM ? `${gotoM[2]}, ${gotoM[3]}` : '';
      const lines = blk.split('\\n').map(l => l.trim()).filter(l => l && !l.startsWith('.goto'));
      return `
        <div class="loot-card p-3 space-y-1.5 text-xs">
          <div class="flex items-center justify-between gap-2">
            <label class="flex items-center gap-2 font-bold text-amber-200 cursor-pointer">
              <input type="checkbox" class="accent-amber-500">
              <span>步骤 ${i + 1}${zone ? ` · ${escapeHtml(zone)}` : ''}</span>
            </label>
            ${coord ? `<button data-zone="${escapeHtml(zone)}" data-coord="${coord}" onclick="copyWayMacro(this.dataset.zone, this.dataset.coord, '')" class="px-2 py-0.5 rounded ghost-btn text-[11px] cursor-pointer">📋 /way ${coord}</button>` : ''}
          </div>
          <div class="space-y-1 pl-5 text-gray-300">${lines.map(l => `<div>${escapeHtml(l.replace(/^>>\\s*/, ''))}</div>`).join('')}</div>
        </div>`;
    }).join('');
  }

'''

def patch_main():
    text = MAIN_PATH.read_text(encoding="utf-8")
    if "/api/quests" in text:
        print("ℹ️ main.py 已包含任务图鉴接口，跳过写入。")
        return
    if MAIN_ANCHOR not in text:
        print("❌ 未在 main.py 中找到 /api/codex 锚点，已中止。")
        sys.exit(1)
    shutil.copy2(MAIN_PATH, MAIN_PATH.with_suffix(".py.bak"))
    updated = text.replace(MAIN_ANCHOR, MAIN_ANCHOR + "\n\n\n" + MAIN_PATCH, 1)
    MAIN_PATH.write_text(updated, encoding="utf-8")
    py_compile.compile(str(MAIN_PATH), doraise=True)
    print("✅ main.py 已自动备份（main.py.bak）并整合完成，Python 语法校验通过！")

def patch_html():
    text = HTML_PATH.read_text(encoding="utf-8")
    if 'id="sec-quests"' in text:
        print("ℹ️ public/index.html 已包含任务图鉴模块，跳过写入。")
        return
    checks = [
        ("导航栏锚点", NAV_ANCHOR),
        ("页面区块锚点", SEC_ANCHOR),
        ("ALL_TABS", "const ALL_TABS = ['home', 'campfire', 'guide', 'forum', 'addons', 'games', 'profile'];"),
        ("switchTab", "if (id === 'profile') loadProfile();"),
        ("JS 锚点", JS_ANCHOR),
    ]
    for name, anchor in checks:
        if anchor not in text:
            print(f"❌ 未在 public/index.html 中找到 {name}，已中止。")
            sys.exit(1)

    shutil.copy2(HTML_PATH, HTML_PATH.with_suffix(".html.bak"))
    updated = text
    updated = updated.replace(NAV_ANCHOR, NAV_ANCHOR + NAV_PATCH, 1)
    updated = updated.replace(SEC_ANCHOR, SEC_PATCH + SEC_ANCHOR, 1)
    updated = updated.replace(
        "const ALL_TABS = ['home', 'campfire', 'guide', 'forum', 'addons', 'games', 'profile'];",
        "const ALL_TABS = ['home', 'campfire', 'guide', 'quests', 'forum', 'addons', 'games', 'profile'];",
        1,
    )
    updated = updated.replace(
        "if (id === 'profile') loadProfile();",
        "if (id === 'profile') loadProfile();\n    if (id === 'quests') initQuestsModule();",
        1,
    )
    updated = updated.replace("closeNewsModal();", "closeNewsModal(); closeRxpModal();", 1)
    updated = updated.replace(JS_ANCHOR, JS_PATCH + JS_ANCHOR, 1)
    HTML_PATH.write_text(updated, encoding="utf-8")
    print("✅ public/index.html 已自动备份（index.html.bak）并整合完成！")

if __name__ == "__main__":
    patch_main()
    patch_html()
    print("🎉 全部整合完成！请执行 ./restart.sh 重启服务。")
