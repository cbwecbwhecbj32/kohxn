# ═══════════════════════════════════════════════════════════════════════
# nodes.py — منطق سیستم نود (Node System)
# ═══════════════════════════════════════════════════════════════════════
#
# این فایل مسئول ارتباط پنل مستر با نودهاست.
# وقتی مستر یه کاربر می‌سازه، از توابع این فایل استفاده می‌کنه تا کاربر
# رو روی همه‌ی نودها هم بسازه.
#
# ═══════════════════════════════════════════════════════════════════════

import asyncio
import json
import time
from datetime import datetime, timezone

import httpx

from main import (
    logger,
    get_db,
    CONFIG,
    generate_node_token,
    COUNTRIES,
    MAX_NODES,
)


# ═══════════════════════════════════════════════════════════════════════
# ۵ اسلات پیش‌فرض با پرچم (طبق خواسته‌ی کاربر)
# ═══════════════════════════════════════════════════════════════════════

DEFAULT_SLOTS = [
    {"slot": 1, "flag": "🇺🇸", "country_code": "us", "label": "America"},
    {"slot": 2, "flag": "🇸🇬", "country_code": "sg", "label": "Singapore"},
    {"slot": 3, "flag": "🇳🇱", "country_code": "nl", "label": "Netherlands"},
    {"slot": 4, "flag": "🇫🇮", "country_code": "fi", "label": "Finland"},
    {"slot": 5, "flag": "🌐", "country_code": "xx", "label": "Variable"},
    {"slot": 6, "flag": "🌐", "country_code": "xx", "label": "Variable"},
    {"slot": 7, "flag": "🌐", "country_code": "xx", "label": "Variable"},
]

def migrate_nodes_table_for_7_slots():
    """
    جدول nodes رو از CHECK(1-5) به CHECK(1-7) مهاجرت می‌ده.
    
    چون SQLite اجازه‌ی تغییر CHECK رو نمی‌ده، جدول رو از نو می‌سازیم.
    """
    conn = get_db()
    try:
        # چک کن جدول فعلی، CHECK قدیمی داره یا نه
        cur = conn.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='nodes'")
        row = cur.fetchone()
        if not row:
            return  # جدول وجود نداره، بعداً ساخته می‌شه
        
        current_sql = row["sql"] or ""
        if "BETWEEN 1 AND 7" in current_sql:
            return  # از قبل درسته
        
        logger.warning("[NODE] Migrating nodes table from CHECK(1-5) to CHECK(1-7)...")
        
        # ۱. جدول قدیمی رو rename کن
        conn.execute("ALTER TABLE nodes RENAME TO nodes_old")
        
        # ۲. جدول جدید بساز
        conn.execute("""
            CREATE TABLE nodes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                slot INTEGER UNIQUE CHECK(slot BETWEEN 1 AND 7),
                name TEXT NOT NULL,
                country_code TEXT NOT NULL,
                flag TEXT NOT NULL,
                address TEXT NOT NULL,
                api_token TEXT NOT NULL,
                status TEXT DEFAULT 'unknown',
                enabled INTEGER DEFAULT 1,
                last_check REAL,
                last_stats_json TEXT,
                created_at TEXT NOT NULL
            )
        """)
        
        # ۳. داده‌های قدیمی رو کپی کن
        conn.execute("""
            INSERT INTO nodes (id, slot, name, country_code, flag, address, api_token, status, enabled, last_check, last_stats_json, created_at)
            SELECT id, slot, name, country_code, flag, address, api_token, status, enabled, last_check, last_stats_json, created_at
            FROM nodes_old
        """)
        
        # ۴. جدول قدیمی رو پاک کن
        conn.execute("DROP TABLE nodes_old")
        
        conn.commit()
        logger.info("[NODE] Successfully migrated nodes table to CHECK(1-7)")
    except Exception as e:
        logger.error(f"[NODE] Migration failed: {e}")
        conn.rollback()
    finally:
        conn.close()


def init_default_slots():
    """
    اسلات‌های پیش‌فرض رو توی جدول nodes می‌سازه.
    
    - اگه جدول خالیه، همه ۷ اسلات رو می‌سازه
    - اگه جدول پره، فقط اسلات‌های جدید (که نیستن) رو اضافه می‌کنه
    """
    conn = get_db()
    migrate_nodes_table_for_7_slots()
    try:
        # چک کن کدوم اسلات‌ها هستن
        cur = conn.execute("SELECT slot FROM nodes")
        existing_slots = {row["slot"] for row in cur.fetchall()}
        
        now = datetime.now(timezone.utc).isoformat()
        added = 0
        for s in DEFAULT_SLOTS:
            if s["slot"] in existing_slots:
                continue  # از قبل هست
            conn.execute("""
                INSERT INTO nodes (slot, name, country_code, flag, address, api_token, status, enabled, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                s["slot"],
                s["label"],
                s["country_code"],
                s["flag"],
                "",
                "",
                "empty",
                1,
                now,
            ))
            added += 1
        conn.commit()
        
        if added > 0:
            logger.info(f"[NODE] Initialized {added} new node slot(s)")
        else:
            logger.info(f"[NODE] All {len(existing_slots)} node slots already exist")
    except Exception as e:
        logger.error(f"[NODE] Error initializing default slots: {e}")
    finally:
        conn.close()

# ═══════════════════════════════════════════════════════════════════════
# توابع اصلی مدیریت نودها
# ═══════════════════════════════════════════════════════════════════════

def get_all_nodes() -> list[dict]:
    """لیست همه‌ی اسلات‌ها (چه خالی چه پر) رو برمی‌گردونه."""
    conn = get_db()
    try:
        cur = conn.execute("""
            SELECT id, slot, name, country_code, flag, address, api_token,
                   status, enabled, last_check, last_stats_json, created_at
            FROM nodes
            ORDER BY slot ASC
        """)
        nodes = []
        for row in cur.fetchall():
            node = dict(row)
            # api_token رو توی خروجی نذار (امنیت)
            node["has_token"] = bool(node.pop("api_token", None))
            # آدرس رو کامل برنگردون
            node["address"] = node.get("address", "")
            nodes.append(node)
        return nodes
    finally:
        conn.close()


def get_node_by_slot(slot: int) -> dict | None:
    """یه نود رو بر اساس شماره اسلات برمی‌گردونه (با توکن کامل — فقط برای استفاده داخلی)."""
    conn = get_db()
    try:
        cur = conn.execute("""
            SELECT id, slot, name, country_code, flag, address, api_token,
                   status, enabled, last_check, last_stats_json, created_at
            FROM nodes
            WHERE slot = ?
        """, (slot,))
        row = cur.fetchone()
        if row is None:
            return None
        return dict(row)
    finally:
        conn.close()


def update_node(slot: int, name: str, address: str, api_token: str) -> bool:
    """اطلاعات یه نود رو آپدیت می‌کنه (وقتی ادمین آدرس و توکن وارد می‌کنه)."""
    conn = get_db()
    try:
        # چک کن اسلات وجود داره
        cur = conn.execute("SELECT id FROM nodes WHERE slot = ?", (slot,))
        if cur.fetchone() is None:
            logger.warning(f"[NODE] Slot {slot} not found")
            return False
        
        conn.execute("""
            UPDATE nodes
            SET name = ?, address = ?, api_token = ?, status = 'unknown'
            WHERE slot = ?
        """, (name, address, api_token, slot))
        conn.commit()
        logger.info(f"[NODE] Slot {slot} updated: name='{name}', address='{address}'")
        return True
    finally:
        conn.close()


def clear_node(slot: int) -> bool:
    """اطلاعات یه نود رو پاک می‌کنه (اسلات خالی می‌شه)."""
    conn = get_db()
    try:
        # اول اطلاعات اسلات رو از دست نده (فقط آدرس و توکن رو پاک کن)
        cur = conn.execute("SELECT country_code, flag, name FROM nodes WHERE slot = ?", (slot,))
        row = cur.fetchone()
        if row is None:
            return False
        
        default = next((s for s in DEFAULT_SLOTS if s["slot"] == slot), None)
        if default:
            conn.execute("""
                UPDATE nodes
                SET name = ?, address = '', api_token = '', status = 'empty'
                WHERE slot = ?
            """, (default["label"], slot))
        else:
            conn.execute("""
                UPDATE nodes
                SET address = '', api_token = '', status = 'empty'
                WHERE slot = ?
            """, (slot,))
        
        conn.commit()
        logger.info(f"[NODE] Slot {slot} cleared")
        return True
    finally:
        conn.close()


def update_node_status(slot: int, status: str, stats_json: str | None = None):
    """وضعیت یه نود رو آپدیت می‌کنه (online/offline/error)."""
    conn = get_db()
    try:
        conn.execute("""
            UPDATE nodes
            SET status = ?, last_check = ?, last_stats_json = ?
            WHERE slot = ?
        """, (status, time.time(), stats_json, slot))
        conn.commit()
    finally:
        conn.close()


# ═══════════════════════════════════════════════════════════════════════
# تست اتصال با نود (Handshake)
# ═══════════════════════════════════════════════════════════════════════

async def test_node_connection(slot: int) -> dict:
    """
    با یه نود تماس می‌گیره و چک می‌کنه در دسترسه یا نه.
    
    Returns:
        {
            "ok": bool,
            "status": str,  # "online" / "offline" / "error"
            "message": str,
            "stats": dict | None
        }
    """
    node = get_node_by_slot(slot)
    if not node:
        return {"ok": False, "status": "error", "message": f"Slot {slot} not found"}
    
    address = (node.get("address") or "").strip()
    token = (node.get("api_token") or "").strip()
    
    if not address:
        update_node_status(slot, "empty")
        return {"ok": False, "status": "empty", "message": "Address not set"}
    if not token:
        update_node_status(slot, "error")
        return {"ok": False, "status": "error", "message": "Token not set"}
    
    # آدرس رو نرمال کن
    if not address.startswith("http"):
        address = "https://" + address
    address = address.rstrip("/")
    
    try:
        async with httpx.AsyncClient(timeout=10.0, follow_redirects=True) as client:
            r = await client.get(
                f"{address}/api/node/handshake",
                headers={"X-Node-Token": token},
            )
            if r.status_code == 200:
                data = r.json()
                stats = data.get("stats", {})
                update_node_status(slot, "online", json.dumps(stats))
                logger.info(f"[NODE] Slot {slot} ({node['name']}) is ONLINE")
                return {
                    "ok": True,
                    "status": "online",
                    "message": "Connected successfully",
                    "stats": stats,
                }
            elif r.status_code == 401:
                update_node_status(slot, "error")
                return {"ok": False, "status": "error", "message": "Invalid token (401)"}
            else:
                update_node_status(slot, "error")
                return {"ok": False, "status": "error", "message": f"HTTP {r.status_code}"}
    except httpx.TimeoutException:
        update_node_status(slot, "offline")
        return {"ok": False, "status": "offline", "message": "Connection timeout"}
    except Exception as e:
        update_node_status(slot, "offline")
        logger.warning(f"[NODE] Slot {slot} connection failed: {e}")
        return {"ok": False, "status": "offline", "message": str(e)}


async def test_all_nodes() -> list[dict]:
    """همه‌ی نودهای پر رو تست می‌کنه."""
    results = []
    for s in DEFAULT_SLOTS:
        slot = s["slot"]
        node = get_node_by_slot(slot)
        if not node or not node.get("address"):
            continue
        result = await test_node_connection(slot)
        result["slot"] = slot
        result["name"] = node.get("name", "")
        results.append(result)
    return results


# ═══════════════════════════════════════════════════════════════════════
# ارسال کاربر به نودها
# ═══════════════════════════════════════════════════════════════════════

async def push_user_to_node(slot: int, user_data: dict) -> dict:
    """
    یه کاربر رو به یه نود می‌فرسته.
    
    Args:
        slot: شماره اسلات نود
        user_data: اطلاعات کاربر شامل:
            - uuid
            - label
            - limit_bytes
            - used_bytes
            - expires_at
            - variants
            - port
    
    Returns:
        {"ok": bool, "message": str}
    """
    node = get_node_by_slot(slot)
    if not node:
        return {"ok": False, "message": f"Slot {slot} not found"}
    
    address = (node.get("address") or "").strip()
    token = (node.get("api_token") or "").strip()
    if not address or not token:
        return {"ok": False, "message": "Node not configured"}
    
    if not address.startswith("http"):
        address = "https://" + address
    address = address.rstrip("/")
    
    try:
        async with httpx.AsyncClient(timeout=15.0, follow_redirects=True) as client:
            r = await client.post(
                f"{address}/api/node/receive-user",
                headers={
                    "X-Node-Token": token,
                    "Content-Type": "application/json",
                },
                json=user_data,
            )
            if r.status_code == 200:
                return {"ok": True, "message": "User pushed successfully"}
            else:
                return {"ok": False, "message": f"HTTP {r.status_code}: {r.text[:200]}"}
    except Exception as e:
        logger.warning(f"[NODE] Push to slot {slot} failed: {e}")
        return {"ok": False, "message": str(e)}


async def push_user_to_all_nodes(user_data: dict) -> dict:
    """یه کاربر رو به همه‌ی نودهای فعال می‌فرسته."""
    results = {}
    for s in DEFAULT_SLOTS:
        slot = s["slot"]
        node = get_node_by_slot(slot)
        if not node or not node.get("address"):
            continue
        result = await push_user_to_node(slot, user_data)
        results[slot] = result
        if result["ok"]:
            logger.info(f"[NODE] User '{user_data.get('label')}' pushed to slot {slot} ✅")
        else:
            logger.warning(f"[NODE] User '{user_data.get('label')}' push to slot {slot} failed: {result['message']}")
    return results


# ═══════════════════════════════════════════════════════════════════════
# Health Check خودکار (هر ۶۰ ثانیه)
# ═══════════════════════════════════════════════════════════════════════

async def node_health_check_loop():
    """هر ۶۰ ثانیه همه‌ی نودها رو چک می‌کنه."""
    await asyncio.sleep(30)  # تاخیر اولیه
    while True:
        try:
            await test_all_nodes()
        except Exception as e:
            logger.error(f"[NODE] Health check loop error: {e}")
        await asyncio.sleep(60)


# ═══════════════════════════════════════════════════════════════════════
# توابع اضافی برای sync تغییرات کاربر با نودها
# ═══════════════════════════════════════════════════════════════════════

async def delete_user_from_all_nodes(uid: str) -> dict:
    """کاربر رو از همه نودهای فعال حذف می‌کنه."""
    results = {}
    for s in DEFAULT_SLOTS:
        slot = s["slot"]
        node = get_node_by_slot(slot)
        if not node or not node.get("address"):
            continue
        
        address = node["address"].rstrip("/")
        if not address.startswith("http"):
            address = "https://" + address
        
        try:
            async with httpx.AsyncClient(timeout=15.0, follow_redirects=True) as client:
                r = await client.post(
                    f"{address}/api/node/delete-user",
                    headers={
                        "X-Node-Token": node["api_token"],
                        "Content-Type": "application/json",
                    },
                    json={"uuid": uid},
                )
                results[slot] = {"ok": r.status_code == 200}
                if r.status_code == 200:
                    logger.info(f"[NODE] User {uid[:8]} deleted from slot {slot} ✅")
        except Exception as e:
            results[slot] = {"ok": False, "message": str(e)}
            logger.warning(f"[NODE] Delete from slot {slot} failed: {e}")
    return results


async def sync_user_to_all_nodes(user_data: dict) -> dict:
    """اطلاعات کاربر رو روی همه نودها sync می‌کنه."""
    results = {}
    for s in DEFAULT_SLOTS:
        slot = s["slot"]
        node = get_node_by_slot(slot)
        if not node or not node.get("address"):
            continue
        result = await push_user_to_node(slot, user_data)
        results[slot] = result
    return results


async def reset_usage_on_all_nodes(uid: str) -> dict:
    """مصرف کاربر رو روی همه نودها صفر می‌کنه."""
    results = {}
    for s in DEFAULT_SLOTS:
        slot = s["slot"]
        node = get_node_by_slot(slot)
        if not node or not node.get("address"):
            continue
        
        address = node["address"].rstrip("/")
        if not address.startswith("http"):
            address = "https://" + address
        
        try:
            async with httpx.AsyncClient(timeout=15.0, follow_redirects=True) as client:
                r = await client.post(
                    f"{address}/api/node/reset-usage",
                    headers={
                        "X-Node-Token": node["api_token"],
                        "Content-Type": "application/json",
                    },
                    json={"uuid": uid},
                )
                results[slot] = {"ok": r.status_code == 200}
        except Exception as e:
            results[slot] = {"ok": False, "message": str(e)}
    return results

async def get_config_from_node(slot: int, uid: str) -> str | None:
    """
    از یه نود می‌پرسه کانفیگ کاربر چیه.
    
    Returns:
        کانفیگ (vless://... یا trojan://...) یا None اگه نود آفلاین/خطا بود
    """
    node = get_node_by_slot(slot)
    if not node:
        return None
    
    address = (node.get("address") or "").strip()
    token = (node.get("api_token") or "").strip()
    if not address or not token:
        return None
    
    if not address.startswith("http"):
        address = "https://" + address
    address = address.rstrip("/")
    
    try:
        async with httpx.AsyncClient(timeout=10.0, follow_redirects=True) as client:
            r = await client.get(
                f"{address}/api/node/get-config",
                headers={"X-Node-Token": token},
                params={"uuid": uid},
            )
            if r.status_code == 200:
                data = r.json()
                return data.get("config")
            else:
                logger.warning(f"[NODE] get-config from slot {slot} returned {r.status_code}")
                return None
    except Exception as e:
        logger.warning(f"[NODE] get-config from slot {slot} failed: {e}")
        return None


# ═══════════════════════════════════════════════════════════════════════
# گزارش‌دهی خودکار مصرف به مستر (روی نودها اجرا می‌شه)
# ═══════════════════════════════════════════════════════════════════════

async def report_usage_to_master_loop():
    """
    هر ۳۰ ثانیه، مصرف همه کاربرا رو به مستر گزارش می‌ده.
    
    فقط روی پنل‌هایی اجرا می‌شه که role=slave هستن.
    """
    await asyncio.sleep(15)  # تاخیر اولیه
    
    while True:
        try:
            # اگه master هستیم، کاری نکن
            role = CONFIG.get("panel_role", "master")
            if role != "slave":
                await asyncio.sleep(30)
                continue
            
            # master_url و master_token رو بگیر
            master_url = (CONFIG.get("master_url") or "").strip()
            master_token = (CONFIG.get("master_token") or "").strip()
            
            if not master_url or not master_token:
                await asyncio.sleep(30)
                continue
            
            # آدرس رو نرمال کن
            if not master_url.startswith("http"):
                master_url = "https://" + master_url
            master_url = master_url.rstrip("/")
            
            # slot این پنل رو از مستر بپرس (یا از CONFIG)
            my_slot = int(CONFIG.get("panel_slot") or 0)
            if my_slot < 1 or my_slot > MAX_NODES:
                logger.warning(f"[NODE] Invalid panel_slot: {my_slot}, skipping report")
                await asyncio.sleep(30)
                continue
            
            # مصرف همه کاربرا رو جمع کن
            reports = []
            async with LINKS_LOCK:
                for uid, link in LINKS.items():
                    reports.append({
                        "uuid": uid,
                        "used_bytes": int(link.get("used_bytes", 0)),
                    })
            
            if not reports:
                await asyncio.sleep(30)
                continue
            
            # بفرست به مستر
            try:
                async with httpx.AsyncClient(timeout=15.0, follow_redirects=True) as client:
                    r = await client.post(
                        f"{master_url}/api/node/report-usage",
                        headers={
                            "X-Node-Token": master_token,
                            "Content-Type": "application/json",
                        },
                        json={
                            "node_slot": my_slot,
                            "reports": reports,
                        },
                    )
                    if r.status_code == 200:
                        logger.debug(f"[NODE] Reported {len(reports)} users to master")
                    else:
                        logger.warning(f"[NODE] Report failed: HTTP {r.status_code}")
            except Exception as e:
                logger.warning(f"[NODE] Report to master failed: {e}")
        
        except Exception as e:
            logger.error(f"[NODE] report_usage loop error: {e}")
        
        await asyncio.sleep(30)  # ۳۰ ثانیه صبر
