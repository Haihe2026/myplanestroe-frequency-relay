# 频次线沟通仓（myplanestroe-frequency-relay）

频次流水线（仓库 `Haihe2026/myplanestroe-frequency`，SOP
`docs/sop/sop-frequency-skill-auto.md`）专用的消息中转仓，与生图线
`myplanestroe` 内 `pipeline/relay/` 完全分开（LI 2026-11-16 定）。
结构照抄生图 relay；消费者轮询目标与白名单指向本仓，不碰生图 relay。

- `events/`：待 Muse 认领的事件（Codex 发来）。
- `requests/`：待对方认领的请求（msg + relay 请求成对）。
- `receipts/`：认领与回执（认领前双键查：request_id 与 event id 各查一次）。
- `consumers/`：轮询消费程序（Muse 侧每 5 分钟、生图线独立）。
- `registrations/`：消费者登记。
- `reports/`：交接报告。
- `transport/`：传输层说明。
