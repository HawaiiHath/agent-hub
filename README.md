# Agent Hub

> 多设备 AI Agent 通信中枢（MCP Server）

让分布在不同设备上的 AI Agent（Claude Code、Hermes 等）能够互相留言、派任务、共享工具配置、查看在线状态。

## 快速开始

```bash
pip install -r requirements.txt
cp agents.example.json agents.json   # 编辑 agents.json，填入真实 token
python3 server.py                     # 默认监听 0.0.0.0:9020
```

## 客户端连接

在 Claude Code 中添加 MCP 服务器：

```bash
claude mcp add --transport sse --scope user agent-hub http://<VPS_IP>:9020/sse?token=你的TOKEN
```

详细使用说明见 [docs/Agent-Hub-使用指南.md](docs/Agent-Hub-使用指南.md)

## 功能

- **消息信箱** — 跨设备留言，上线自动收到
- **任务队列** — 给任意 agent 派任务，带截止时间和状态追踪
- **MCP 配置共享** — 一个设备配好的 MCP，其他设备直接抄
- **在线状态** — 查看各设备 agent 的活跃情况

## 安全

- `agents.json` 包含真实 token，已在 `.gitignore` 中排除
- 使用 `agents.example.json` 作为模板
- Token 通过 SHA256 哈希存储于服务端数据库
