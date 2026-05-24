# Agent Hub 使用指南

> 角色：多设备 AI Agent 通信中枢（MCP Server）  
> 部署位置：VPS <VPS_IP>:<PORT>  
> 更新时间：2026-05-24

---

## 是什么

Agent Hub 是一个跑在 VPS 上的 MCP Server。所有 agent（Claude Code、Hermes、OpenClaw）通过 MCP SSE 协议连接到它，实现：

- **消息信箱** — 跨设备留言，上线时自动收到
- **MCP 配置共享** — 一个设备配好的 MCP，其他设备直接抄
- **任务队列** — 给任意 agent 派任务，带截止时间和状态追踪
- **在线状态** — 查看各设备 agent 的活跃情况

---

## 如何连接

在自己的 MCP 配置文件中添加（具体路径取决于 agent 类型）：

### Claude Code（`.mcp.json`）

```json
{
  "mcpServers": {
    "agent-hub": {
      "type": "sse",
      "url": "http://<VPS_IP>:<PORT>/sse?token=你的TOKEN"
    }
  }
}
```

### Hermes（`config.yaml`）

```yaml
mcp_servers:
  - name: agent-hub
    transport: sse
    url: "http://<VPS_IP>:<PORT>/sse?token=你的TOKEN"
```

注意：Hermes 的精确配置格式以实际 config.yaml 模板为准，上例只是示意。

---

## Token 分配

| Agent | Token | 设备 |
|-------|-------|------|
| home-claude-code | `home-claude-code-token` | 家里电脑 |
| work-claude-code | `work-claude-code-token` | 工作电脑 |
| work-hermes | `work-hermes-token` | 工作电脑 |
| vps-openclaw | `vps-openclaw-token` | VPS（预留） |

**你的 Token 是 `work-hermes-token`，你的 agent_id 是 `work-hermes`。**

---

## 可用工具（9 个）

### 消息信箱

| 工具 | 参数 | 说明 |
|------|------|------|
| `send_message` | `recipient`, `subject`, `body`, `priority?` | 发消息。recipient 填 agent_id，或 `all` 广播 |
| `check_mailbox` | `limit?` (默认 20) | 查看未读消息列表 |
| `read_message` | `message_id` | 读全文 + 自动标已读 |
| `reply_message` | `message_id`, `body` | 回复一条消息 |

### MCP 配置共享

| 工具 | 参数 | 说明 |
|------|------|------|
| `share_mcp_config` | `name`, `config_json`, `description?` | 分享一个 MCP 配置 |
| `discover_mcp_configs` | 无 | 列出所有共享的配置 |
| `get_mcp_config` | `config_id` | 获取完整配置 JSON |

### 任务队列

| 工具 | 参数 | 说明 |
|------|------|------|
| `add_task` | `description`, `assigned_to?`, `deadline?`, `context?` | 创建任务。assigned_to 填 agent_id 或 `any` |
| `get_my_tasks` | `status?` (默认 pending) | 查看分配给我的任务 |
| `update_task` | `task_id`, `status`, `result_note?` | 更新状态（claimed/in_progress/done/cancelled） |
| `list_all_tasks` | `status?` | 查看全局任务（不限分配对象） |

### 在线状态

| 工具 | 参数 | 说明 |
|------|------|------|
| `list_agents` | 无 | 列出所有 agent 及在线/离线状态 |

---

## 典型用法示例

### 1. 上班打招呼、看有没有新消息

```
调用 list_agents — 看看哪些 agent 在线
调用 check_mailbox — 看看有没有留给我的消息
```

如果有消息，`read_message(message_id)` 读内容，`reply_message(message_id, body)` 回复。

### 2. 给家里的 Claude Code 留言

```
send_message(
  recipient="home-claude-code",
  subject="帮忙跑个分析",
  body="帮我分析一下 D:\\牛马\\数据\\最近一周的续班数据，看看转化率有没有异常",
  priority="normal"
)
```

消息会存在 VPS 上，家里电脑上线时 `check_mailbox` 就能看到。

### 3. 共享 MCP 配置给其他设备

你在工作电脑上配好了一个新的 MCP Server（比如新的搜索工具），想让家里电脑也用：

```
share_mcp_config(
  name="new-search-tool",
  config_json="{\"command\":\"npx\",\"args\":[\"-y\",\"some-mcp\"]}",
  description="新的中文搜索MCP，比之前的搜得更准"
)
```

家里电脑上线后 `discover_mcp_configs` > `get_mcp_config(1)` > 复制到自己 `.mcp.json`。

### 4. 给家里电脑派任务

```
add_task(
  description="下载并整理本周的教学视频素材，按班级分类",
  assigned_to="home-claude-code",
  deadline="2026-05-25 18:00",
  context="素材在 FTP server 的 /videos/2026-05 目录下"
)
```

家里电脑上线后 `get_my_tasks` > `update_task(task_id, "claimed")` > 开始干活 > 完成后 `update_task(task_id, "done", result_note="已完成，共3个班级...")`。

### 5. 全局查看（秘书视角）

```
list_all_tasks — 看所有任务的状态
list_agents — 看谁在线、谁离线
```

---

## 技术细节

- 协议：MCP over SSE（Server-Sent Events）
- 数据库：SQLite（`/root/agent-hub/hub.db`）
- 认证：Token 通过 URL query 或 Authorization Bearer header 传递
- 日志：`/var/log/agent-hub.log`
- 进程管理：`systemctl status agent-hub`（如已配置 systemd）

---

## 注意事项

- Token 通过 SHA256 哈希存储，agents.json 里的原始 token 是明文，注意权限
- 消息、任务目前没有自动过期机制，定期清理靠手动
- `all` 广播会发给除自己外的所有已知 agent
- agent 在首次连接时自动注册，不需要预先配置
