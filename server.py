"""
agent-hub: 多设备 AI Agent 通信中枢 (MCP Server)
部署在 VPS 上，为家里的 Claude Code、工作的 Claude Code/Hermes、VPS 的 OpenClaw
提供消息信箱、MCP 配置共享、任务队列三类功能。
"""
import asyncio
import hashlib
import json
import sqlite3
import time
import os
import logging
import re
from contextvars import ContextVar
from datetime import datetime, timezone, timedelta
from urllib.parse import parse_qs

from mcp.server.fastmcp import FastMCP
from mcp.server.transport_security import TransportSecuritySettings
from mcp.types import TextContent
import uvicorn
from starlette.responses import JSONResponse
from starlette.routing import Route

# ---------------------------------------------------------------------------
# 配置
# ---------------------------------------------------------------------------
DB_PATH = os.environ.get("AGENT_HUB_DB", os.path.join(os.path.dirname(__file__), "hub.db"))
AGENTS_CONFIG = os.environ.get(
    "AGENT_HUB_AGENTS",
    os.path.join(os.path.dirname(__file__), "agents.json"),
)
_ALLOWED_HOSTS_DEFAULT = "127.0.0.1:*,localhost:*"
ALLOWED_HOSTS = os.environ.get("AGENT_HUB_ALLOWED_HOSTS", _ALLOWED_HOSTS_DEFAULT).split(",")
_TZ_OFFSET = os.environ.get("AGENT_HUB_TZ")
if _TZ_OFFSET is not None:
    TZ = timezone(timedelta(hours=int(_TZ_OFFSET)))
else:
    TZ = datetime.now().astimezone().tzinfo
BIND_ADDR = os.environ.get("AGENT_HUB_BIND_ADDR", "0.0.0.0")

# 输入大小限制（可通过环境变量调整）
MAX_SUBJECT_LEN = int(os.environ.get("AGENT_HUB_MAX_SUBJECT_LEN", "500"))
MAX_BODY_LEN = int(os.environ.get("AGENT_HUB_MAX_BODY_LEN", "50000"))
MAX_DESCRIPTION_LEN = int(os.environ.get("AGENT_HUB_MAX_DESCRIPTION_LEN", "2000"))
MAX_CONTEXT_LEN = int(os.environ.get("AGENT_HUB_MAX_CONTEXT_LEN", "5000"))

# 消息/任务保留天数（可通过环境变量调整，默认 30 天）
MESSAGE_RETENTION_DAYS = int(os.environ.get("AGENT_HUB_MESSAGE_RETENTION_DAYS", "30"))
TASK_RETENTION_DAYS = int(os.environ.get("AGENT_HUB_TASK_RETENTION_DAYS", "30"))
CLEANUP_INTERVAL_SEC = int(os.environ.get("AGENT_HUB_CLEANUP_INTERVAL_SEC", "3600"))
HEARTBEAT_INTERVAL_SEC = int(os.environ.get("AGENT_HUB_HEARTBEAT_INTERVAL_SEC", "300"))
AGENT_OFFLINE_THRESHOLD_SEC = int(os.environ.get("AGENT_HUB_OFFLINE_THRESHOLD_SEC", "600"))


async def cleanup_loop():
    """后台定期清理过期消息和已完成/取消的任务"""
    while True:
        await asyncio.sleep(CLEANUP_INTERVAL_SEC)
        try:
            conn = get_db()
            msg_cutoff = int(time.time()) - MESSAGE_RETENTION_DAYS * 86400
            task_cutoff = int(time.time()) - TASK_RETENTION_DAYS * 86400
            deleted_cutoff = int(time.time()) - 7 * 86400  # 软删除 7 天后彻底清除

            # 清理已读消息
            deleted_msgs = conn.execute(
                "DELETE FROM messages WHERE is_read=1 AND is_deleted=0 AND created_at < ?", (msg_cutoff,)
            ).rowcount

            # 清理软删除的消息（7天后彻底删除）
            deleted_msgs += conn.execute(
                "DELETE FROM messages WHERE is_deleted=1 AND created_at < ?", (deleted_cutoff,)
            ).rowcount

            # 清理已完成或已取消的旧任务
            deleted_tasks = conn.execute(
                "DELETE FROM tasks WHERE status IN ('done','cancelled') AND is_deleted=0 AND created_at < ?",
                (task_cutoff,),
            ).rowcount

            # 清理软删除的任务（7天后彻底删除）
            deleted_tasks += conn.execute(
                "DELETE FROM tasks WHERE is_deleted=1 AND created_at < ?", (deleted_cutoff,)
            ).rowcount

            conn.commit()
            conn.close()

            if deleted_msgs or deleted_tasks:
                logging.getLogger("agent-hub").info(
                    "cleanup: removed %d messages, %d tasks", deleted_msgs, deleted_tasks
                )
        except Exception:
            logging.getLogger("agent-hub").exception("cleanup error")


async def heartbeat_loop():
    """后台定期将长时间未活跃的 agent 标记为离线"""
    while True:
        await asyncio.sleep(HEARTBEAT_INTERVAL_SEC)
        try:
            conn = get_db()
            cutoff = int(time.time()) - AGENT_OFFLINE_THRESHOLD_SEC
            affected = conn.execute(
                "UPDATE agents SET is_online=0 WHERE is_online=1 AND last_seen < ?",
                (cutoff,),
            ).rowcount
            conn.commit()
            conn.close()
            if affected:
                logging.getLogger("agent-hub").info(
                    "heartbeat: marked %d agent(s) offline", affected
                )
        except Exception:
            logging.getLogger("agent-hub").exception("heartbeat error")

# ---------------------------------------------------------------------------
# Token 日志脱敏
# ---------------------------------------------------------------------------
class TokenSanitizer(logging.Filter):
    def filter(self, record):
        if record.args:
            if isinstance(record.args, tuple):
                record.args = tuple(
                    re.sub(r'token=[^&\s"\']+', 'token=***', a) if isinstance(a, str) else a
                    for a in record.args
                )
            elif isinstance(record.args, dict):
                for k in list(record.args.keys()):
                    if isinstance(record.args[k], str):
                        record.args[k] = re.sub(r'token=[^&\s"\']+', 'token=***', record.args[k])
        return True

# ---------------------------------------------------------------------------
# 上下文：当前请求的 agent 身份
# ---------------------------------------------------------------------------
current_agent_id: ContextVar[str] = ContextVar("current_agent_id", default="unknown")
current_agent_name: ContextVar[str] = ContextVar("current_agent_name", default="Unknown")

# ---------------------------------------------------------------------------
# 数据库
# ---------------------------------------------------------------------------
def get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db():
    conn = get_db()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS agents (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            device TEXT NOT NULL,
            last_seen INTEGER NOT NULL DEFAULT 0,
            is_online INTEGER NOT NULL DEFAULT 0,
            created_at INTEGER NOT NULL DEFAULT (strftime('%s','now'))
        );

        CREATE TABLE IF NOT EXISTS messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sender_id TEXT NOT NULL,
            recipient_id TEXT NOT NULL,
            subject TEXT NOT NULL DEFAULT '',
            body TEXT NOT NULL,
            priority TEXT NOT NULL DEFAULT 'normal',
            is_read INTEGER NOT NULL DEFAULT 0,
            created_at INTEGER NOT NULL DEFAULT (strftime('%s','now'))
        );

        CREATE TABLE IF NOT EXISTS mcp_configs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            config_json TEXT NOT NULL,
            description TEXT NOT NULL DEFAULT '',
            shared_by TEXT NOT NULL,
            created_at INTEGER NOT NULL DEFAULT (strftime('%s','now'))
        );

        CREATE TABLE IF NOT EXISTS tasks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_by TEXT NOT NULL,
            assigned_to TEXT NOT NULL,
            description TEXT NOT NULL,
            context TEXT NOT NULL DEFAULT '',
            status TEXT NOT NULL DEFAULT 'pending',
            claimed_by TEXT,
            deadline INTEGER,
            result_note TEXT NOT NULL DEFAULT '',
            created_at INTEGER NOT NULL DEFAULT (strftime('%s','now')),
            completed_at INTEGER
        );

        CREATE INDEX IF NOT EXISTS idx_msg_recipient ON messages(recipient_id, is_read);
        CREATE INDEX IF NOT EXISTS idx_msg_sender ON messages(sender_id, created_at);
        CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status, assigned_to);
        CREATE INDEX IF NOT EXISTS idx_tasks_created ON tasks(created_by, created_at);
    """)
    conn.commit()

    # 迁移：为旧数据库添加 is_deleted 列
    for table in ("messages", "tasks"):
        try:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN is_deleted INTEGER NOT NULL DEFAULT 0")
        except sqlite3.OperationalError:
            pass  # 列已存在

    conn.close()


def load_agents() -> dict[str, dict]:
    with open(AGENTS_CONFIG, "r", encoding="utf-8") as f:
        data = json.load(f)
    mapping = {}
    for token, info in data["agents"].items():
        token_hash = hashlib.sha256(token.encode()).hexdigest()
        mapping[token_hash] = info
    return mapping


# ---------------------------------------------------------------------------
# 初始化
# ---------------------------------------------------------------------------
init_db()
TOKEN_MAP = load_agents()
mcp = FastMCP(
    "agent-hub",
    host="0.0.0.0",
    transport_security=TransportSecuritySettings(
        enable_dns_rebinding_protection=True,
        allowed_hosts=ALLOWED_HOSTS,
    ),
)


# ---------------------------------------------------------------------------
# 认证中间件（原生 ASGI，不破坏 SSE 流）
# 支持两种传 token 方式：
#   1. URL query:  /sse?token=xxx
#   2. HTTP header: Authorization: Bearer xxx
# ---------------------------------------------------------------------------
class AuthMiddleware:
    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        token = self._extract_token(scope)
        token_hash = hashlib.sha256(token.encode()).hexdigest() if token else ""
        agent_info = TOKEN_MAP.get(token_hash)

        if agent_info is not None:
            agent_id = agent_info["id"]
            current_agent_id.set(agent_id)
            current_agent_name.set(agent_info["name"])

            conn = get_db()
            conn.execute(
                "INSERT INTO agents(id, name, device, last_seen, is_online) VALUES(?,?,?,?,1) "
                "ON CONFLICT(id) DO UPDATE SET last_seen=strftime('%s','now'), is_online=1",
                (agent_id, agent_info["name"], agent_info["device"], int(time.time())),
            )
            conn.commit()
            conn.close()

        await self.app(scope, receive, send)

    def _extract_token(self, scope) -> str:
        for header_name, header_value in scope.get("headers", []):
            if header_name.lower() == b"authorization":
                val = header_value.decode()
                if val.startswith("Bearer "):
                    return val[7:]

        query_bytes = scope.get("query_string", b"")
        params = parse_qs(query_bytes.decode())
        return params.get("token", [""])[0]


def _caller() -> str:
    return current_agent_id.get()


def _caller_name() -> str:
    return current_agent_name.get()


def _validate_input(**kwargs) -> str | None:
    """验证输入大小限制，返回错误消息或 None"""
    limits = {
        "subject": MAX_SUBJECT_LEN,
        "body": MAX_BODY_LEN,
        "description": MAX_DESCRIPTION_LEN,
        "context": MAX_CONTEXT_LEN,
    }
    for field, value in kwargs.items():
        limit = limits.get(field)
        if limit and isinstance(value, str) and len(value) > limit:
            return f"❌ {field} 超过最大长度限制（{limit} 字符）"
    return None


# ===================================================================
# 工具：消息信箱
# ===================================================================

@mcp.tool(name="send_message", description="向其他智能体发送消息。recipient 填 agent_id 或 'all' 广播")
async def send_message(recipient: str, subject: str, body: str, priority: str = "normal") -> str:
    if err := _validate_input(subject=subject, body=body):
        return err
    sender = _caller()
    if recipient == "all":
        conn = get_db()
        agents = [r["id"] for r in conn.execute("SELECT id FROM agents").fetchall()]
        conn.close()
        recipients = [a for a in agents if a != sender]
        if not recipients:
            return "❌ 没有其他已知 agent 可以接收广播"
    else:
        recipients = [r.strip() for r in recipient.split(",")]

    conn = get_db()
    ids = []
    now = int(time.time())
    for r in recipients:
        cur = conn.execute(
            "INSERT INTO messages(sender_id, recipient_id, subject, body, priority, created_at) "
            "VALUES(?,?,?,?,?,?)",
            (sender, r, subject, body, priority, now),
        )
        ids.append(str(cur.lastrowid))
    conn.commit()
    conn.close()
    return f"✅ 消息已发送给 {', '.join(recipients)}（ID: {', '.join(ids)}）"


@mcp.tool(name="check_mailbox", description="查看发给自己的未读消息")
async def check_mailbox(limit: int = 20) -> str:
    agent = _caller()
    conn = get_db()
    rows = conn.execute(
        "SELECT id, sender_id, subject, priority, datetime(created_at,'unixepoch','localtime') as ts "
        "FROM messages WHERE recipient_id=? AND is_read=0 AND is_deleted=0 ORDER BY created_at DESC LIMIT ?",
        (agent, limit),
    ).fetchall()
    conn.close()

    if not rows:
        return "📭 信箱为空"

    lines = [f"📬 {len(rows)} 条未读消息:"]
    for r in rows:
        flag = "🔴" if r["priority"] == "high" else ""
        lines.append(f"  [{r['id']}] {flag} 来自 {r['sender_id']} | {r['ts']}")
        lines.append(f"      主题: {r['subject']}")
    return "\n".join(lines)


@mcp.tool(name="read_message", description="读取指定消息的完整内容，并标记为已读")
async def read_message(message_id: int) -> str:
    agent = _caller()
    conn = get_db()
    row = conn.execute(
        "SELECT * FROM messages WHERE id=? AND recipient_id=? AND is_deleted=0", (message_id, agent)
    ).fetchone()
    if not row:
        conn.close()
        return "❌ 消息不存在或不是发给你的"

    conn.execute("UPDATE messages SET is_read=1 WHERE id=?", (message_id,))
    conn.commit()
    conn.close()

    return (
        f"📨 来自: {row['sender_id']}\n"
        f"主题: {row['subject']}\n"
        f"时间: {datetime.fromtimestamp(row['created_at'], TZ).strftime('%Y-%m-%d %H:%M:%S')}\n"
        f"优先级: {row['priority']}\n"
        f"---\n{row['body']}"
    )


@mcp.tool(name="reply_message", description="回复一条消息，自动设置 subject 为 Re: 原主题")
async def reply_message(message_id: int, body: str) -> str:
    agent = _caller()
    conn = get_db()
    original = conn.execute(
        "SELECT * FROM messages WHERE id=? AND recipient_id=? AND is_deleted=0", (message_id, agent)
    ).fetchone()
    if not original:
        conn.close()
        return "❌ 原消息不存在或不是发给你的"

    reply_subject = f"Re: {original['subject']}" if not original["subject"].startswith("Re:") else original["subject"]
    cur = conn.execute(
        "INSERT INTO messages(sender_id, recipient_id, subject, body, created_at) VALUES(?,?,?,?,?)",
        (agent, original["sender_id"], reply_subject, body, int(time.time())),
    )
    mid = cur.lastrowid
    conn.execute("UPDATE messages SET is_read=1 WHERE id=?", (message_id,))
    conn.commit()
    conn.close()
    return f"✅ 已回复 {original['sender_id']}（消息 ID: {mid}）"


@mcp.tool(name="delete_message", description="软删除一条发给自己的消息")
async def delete_message(message_id: int) -> str:
    agent = _caller()
    conn = get_db()
    row = conn.execute(
        "SELECT * FROM messages WHERE id=? AND recipient_id=? AND is_deleted=0",
        (message_id, agent),
    ).fetchone()
    if not row:
        conn.close()
        return "❌ 消息不存在、不是发给你的、或已被删除"

    conn.execute("UPDATE messages SET is_deleted=1 WHERE id=?", (message_id,))
    conn.commit()
    conn.close()
    return f"✅ 消息 [{message_id}] 已删除"


# ===================================================================
# 工具：MCP 配置共享
# ===================================================================

@mcp.tool(name="share_mcp_config", description="分享一个 MCP 服务器配置，供其他设备上的 agent 发现和使用")
async def share_mcp_config(name: str, config_json: str, description: str = "") -> str:
    agent = _caller()
    try:
        json.loads(config_json)
    except json.JSONDecodeError:
        return "❌ config_json 不是有效的 JSON"

    conn = get_db()
    conn.execute(
        "INSERT OR REPLACE INTO mcp_configs(name, config_json, description, shared_by, created_at) "
        "VALUES(?,?,?,?,?)",
        (name, config_json, description, agent, int(time.time())),
    )
    conn.commit()
    conn.close()
    return f"✅ MCP 配置 '{name}' 已共享"


@mcp.tool(name="discover_mcp_configs", description="查看所有共享的 MCP 配置，返回名称和描述")
async def discover_mcp_configs() -> str:
    conn = get_db()
    rows = conn.execute(
        "SELECT id, name, description, shared_by, datetime(created_at,'unixepoch','localtime') as ts "
        "FROM mcp_configs ORDER BY created_at DESC"
    ).fetchall()
    conn.close()

    if not rows:
        return "📭 没有共享的 MCP 配置"

    lines = ["📋 共享的 MCP 配置:"]
    for r in rows:
        lines.append(f"  [{r['id']}] {r['name']} （来自 {r['shared_by']}，{r['ts']}）")
        if r["description"]:
            lines.append(f"      {r['description']}")
    return "\n".join(lines)


@mcp.tool(name="get_mcp_config", description="获取指定 MCP 配置的完整 JSON，可直接复制到 mcpServers 中使用")
async def get_mcp_config(config_id: int) -> str:
    conn = get_db()
    row = conn.execute("SELECT * FROM mcp_configs WHERE id=?", (config_id,)).fetchone()
    conn.close()

    if not row:
        return "❌ 配置不存在"

    return (
        f"📋 配置名称: {row['name']}\n"
        f"共享者: {row['shared_by']}\n"
        f"描述: {row['description']}\n"
        f"---\n{row['config_json']}"
    )


# ===================================================================
# 工具：任务队列
# ===================================================================

@mcp.tool(name="add_task", description="创建一个任务分配给指定 agent。assigned_to 填 agent_id 或 'any' 表示谁都可以接")
async def add_task(description: str, assigned_to: str = "any", deadline: str = "", context: str = "") -> str:
    if err := _validate_input(description=description, context=context):
        return err
    agent = _caller()
    deadline_ts = None
    if deadline:
        try:
            deadline_ts = int(datetime.strptime(deadline, "%Y-%m-%d %H:%M").timestamp())
        except ValueError:
            return "❌ deadline 格式错误，请使用 'YYYY-MM-DD HH:MM'"

    conn = get_db()
    cur = conn.execute(
        "INSERT INTO tasks(created_by, assigned_to, description, context, deadline, created_at) "
        "VALUES(?,?,?,?,?,?)",
        (agent, assigned_to, description, context, deadline_ts, int(time.time())),
    )
    task_id = cur.lastrowid
    conn.commit()
    conn.close()
    return f"✅ 任务已创建（ID: {task_id}），分配给: {assigned_to}"


@mcp.tool(name="get_my_tasks", description="查看分配给我的待处理任务")
async def get_my_tasks(status: str = "pending") -> str:
    agent = _caller()
    conn = get_db()
    rows = conn.execute(
        "SELECT id, created_by, description, deadline, context, "
        "datetime(created_at,'unixepoch','localtime') as ts "
        "FROM tasks "
        "WHERE (assigned_to=? OR assigned_to='any' OR (assigned_to='*' AND status='pending')) "
        "AND status=? AND is_deleted=0 "
        "ORDER BY created_at DESC",
        (agent, status),
    ).fetchall()
    conn.close()

    if not rows:
        return f"📭 没有状态为 '{status}' 的任务"

    lines = [f"📋 {len(rows)} 个任务（状态: {status}）:"]
    for r in rows:
        dl = f" ⏰ 截止: {datetime.fromtimestamp(r['deadline'], TZ).strftime('%m-%d %H:%M')}" if r["deadline"] else ""
        lines.append(f"  [{r['id']}] {r['description']}{dl}")
        lines.append(f"      来自 {r['created_by']} | {r['ts']}")
        if r["context"]:
            lines.append(f"      上下文: {r['context'][:120]}")
    return "\n".join(lines)


@mcp.tool(name="update_task", description="更新任务状态。status: claimed/in_progress/done/cancelled")
async def update_task(task_id: int, status: str, result_note: str = "") -> str:
    if err := _validate_input(description=result_note):
        return err
    agent = _caller()
    valid = {"claimed", "in_progress", "done", "cancelled"}
    if status not in valid:
        return f"❌ 无效状态，可选: {', '.join(valid)}"

    conn = get_db()
    row = conn.execute("SELECT * FROM tasks WHERE id=?", (task_id,)).fetchone()
    if not row:
        conn.close()
        return "❌ 任务不存在"

    # 授权检查：仅创建者、被分配者、或认领者可修改
    allowed = {row["created_by"], row["assigned_to"], row["claimed_by"]}
    if agent not in allowed and row["assigned_to"] not in ("any", "*") and agent != "system":
        conn.close()
        return "❌ 你没有权限修改此任务"

    if status == "claimed":
        conn.execute(
            "UPDATE tasks SET status='in_progress', claimed_by=? WHERE id=?",
            (agent, task_id),
        )
    elif status == "done" or status == "cancelled":
        conn.execute(
            "UPDATE tasks SET status=?, result_note=?, completed_at=strftime('%s','now') WHERE id=?",
            (status, result_note, task_id),
        )
    else:
        conn.execute(
            "UPDATE tasks SET status=?, result_note=? WHERE id=?",
            (status, result_note, task_id),
        )

    if status in ("done", "cancelled"):
        notify = (
            f"任务 [{task_id}] '{row['description']}' 已被 {_caller_name()} 标记为 {status}。"
            + (f" 备注: {result_note}" if result_note else "")
        )
        conn.execute(
            "INSERT INTO messages(sender_id, recipient_id, subject, body, priority, created_at) "
            "VALUES(?,?,?,?,?,?)",
            ("system", row["created_by"], f"任务状态更新: {status}", notify, "normal", int(time.time())),
        )

    conn.commit()
    conn.close()
    return f"✅ 任务 [{task_id}] 状态已更新为 {status}"


@mcp.tool(name="delete_task", description="软删除一个自己创建且尚未开始的任务")
async def delete_task(task_id: int) -> str:
    agent = _caller()
    conn = get_db()
    row = conn.execute(
        "SELECT * FROM tasks WHERE id=? AND created_by=? AND is_deleted=0",
        (task_id, agent),
    ).fetchone()
    if not row:
        conn.close()
        return "❌ 任务不存在、不是你创建的、或已被删除"
    if row["status"] not in ("pending",):
        conn.close()
        return "❌ 只能删除状态为 pending 的任务"

    conn.execute("UPDATE tasks SET is_deleted=1 WHERE id=?", (task_id,))
    conn.commit()
    conn.close()
    return f"✅ 任务 [{task_id}] 已删除"


@mcp.tool(name="list_all_tasks", description="查看所有任务（不限分配对象），方便了解全局任务状况")
async def list_all_tasks(status: str = "") -> str:
    conn = get_db()
    if status:
        rows = conn.execute(
            "SELECT * FROM tasks WHERE status=? AND is_deleted=0 ORDER BY created_at DESC", (status,)
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM tasks WHERE is_deleted=0 ORDER BY created_at DESC LIMIT 50"
        ).fetchall()
    conn.close()

    if not rows:
        return "📭 没有任务"

    lines = ["📋 全部任务:"]
    for r in rows:
        st = r["status"]
        emoji = {"pending": "⏳", "in_progress": "🔄", "done": "✅", "cancelled": "❌"}.get(st, "")
        dl = f" ⏰{datetime.fromtimestamp(r['deadline'], TZ).strftime('%m-%d %H:%M')}" if r["deadline"] else ""
        claimed = f" [{r['claimed_by']}]" if r["claimed_by"] else ""
        lines.append(
            f"  [{r['id']}] {emoji}{st} →{r['assigned_to']}{claimed} {r['description'][:60]}{dl}"
        )
    return "\n".join(lines)


# ===================================================================
# 工具：在线状态
# ===================================================================

@mcp.tool(name="list_agents", description="列出所有已知 agent 及其在线状态和最后活跃时间")
async def list_agents() -> str:
    conn = get_db()
    rows = conn.execute(
        "SELECT id, name, device, is_online, last_seen, "
        "datetime(last_seen,'unixepoch','localtime') as last_seen_str "
        "FROM agents ORDER BY device, id"
    ).fetchall()
    conn.close()

    if not rows:
        return "📭 还没有 agent 连接过"

    lines = ["🤖 已知 Agent:"]
    for r in rows:
        status = "🟢 在线" if r["is_online"] else "🔴 离线"
        seen = r["last_seen_str"] if r["last_seen"] else "从未"
        lines.append(f"  {r['id']} ({r['name']}) [{r['device']}] {status} 最后活跃: {seen}")
    return "\n".join(lines)


# ===================================================================
# 健康检查端点
# ===================================================================

async def health_endpoint(request):
    try:
        conn = get_db()
        conn.execute("SELECT 1")
        conn.close()
        db_ok = True
    except Exception:
        db_ok = False

    try:
        conn = get_db()
        online = conn.execute("SELECT COUNT(*) as cnt FROM agents WHERE is_online=1").fetchone()["cnt"]
        total = conn.execute("SELECT COUNT(*) as cnt FROM agents").fetchone()["cnt"]
        conn.close()
    except Exception:
        online = 0
        total = 0

    return JSONResponse({
        "status": "ok" if db_ok else "degraded",
        "database": "ok" if db_ok else "error",
        "agents_online": online,
        "agents_total": total,
    })


# ===================================================================
# 启动
# ===================================================================

def create_app():
    app = mcp.sse_app()
    app.routes.insert(0, Route("/health", health_endpoint, methods=["GET"]))
    for i, item in enumerate(app.user_middleware):
        if item[0] == AuthMiddleware:
            break
    else:
        app.user_middleware.insert(0, (AuthMiddleware, (), {}))
        app.middleware_stack = app.build_middleware_stack()
    return app


app = create_app()

if __name__ == "__main__":
    port = int(os.environ.get("AGENT_HUB_PORT", "9020"))
    uvicorn_logger = logging.getLogger("uvicorn.access")
    uvicorn_logger.addFilter(TokenSanitizer())
    print(f"🚀 agent-hub 启动在 http://{BIND_ADDR}:{port}")
    print(f"   allowed_hosts: {ALLOWED_HOSTS}")

    async def main():
        cleanup_task = asyncio.create_task(cleanup_loop())
        heartbeat_task = asyncio.create_task(heartbeat_loop())
        config = uvicorn.Config(app, host=BIND_ADDR, port=port)
        server = uvicorn.Server(config)
        try:
            await server.serve()
        finally:
            cleanup_task.cancel()
            heartbeat_task.cancel()
            try:
                await asyncio.gather(cleanup_task, heartbeat_task)
            except asyncio.CancelledError:
                pass
            print("👋 agent-hub 已安全关闭")

    asyncio.run(main())
