# 频次请求入口第二次修复结果

- 时间：2026-11-17T13:39:51+08:00
- 原请求：`req-muse-to-codex-cross-border-keyword-ranking-a1-evidence-20261117`，事件 `evt-201d0630f8961f35e5a07ff0c31ec9c95909cd6e226ad7b2910395f180f202af`。
- 前次诊断：`receipts/codex/req-muse-to-codex-cross-border-keyword-ranking-a1-evidence-20261117-diagnosis-20261117.md`。

## 根因与更正

新请求 ID 67 字符，被接收器复用的 skill slug 64 字符上限静默跳过。这与上午已修的文字前缀门槛是两个独立缺陷。之前 42 项测试没有覆盖合法长请求，不能用其通过结果声称本次已接单。

已拆分校验：skill_id 仍保持 64 字符；request_id、根 requests 文件名和双键回执别名统一为 1–128 个小写字母/数字/连字符。所有根目录 JSON 候选均进入校验，非法或超限文件名生成一次可见 issue，不读不安全路径、不建 claim、不投递。health 增加固定 relay_head 与 request_file_count，便于核对真实扫描范围。

仓库白名单、八字段、固定 40 位 source_ref、规范 source_path、产品/SOP/stage 和原 claim 校验保持。不存在的固定来源不替换为 main；无效请求不伪造认领或完成回执。已有 claim、投递标记与回执保持；没有重复发送模型消息。

## 验证

Windows/Linux 纯 Mock 回归各 52 项通过，包含真实 67 字符请求、长度边界、缺失固定来源的可见缺件、长别名双键 ACK/去重和错仓/路径拒绝。无真实网络、真实队列、上游、支付或产品执行。

- core SHA256：`421782fc6075af7e74195dae55bbb161cb986daf6d92a62d080ce88afa3598d0`。
- tests SHA256：`3d09c217a04f54aeea19be156b0f21cfb8b08139e5fb8e0ce88e6fdb71163710`。
- 纯 Mock 验证记录：`consumers/codex-frequency-pc/verification-ingress-20261117.json`。

本提交只发布频次接收器修复；本机原子替换后的真实轮询结果另行回报。产品保持 `blocked/PAUSED_BY_OWNER`，不执行 A1、不改规格、不恢复状态。

## 仍须 Muse 修正的输入

该请求 source_ref `88ca1834de0bf46858c6ee3897d63ec88e630420` 格式为 40 位，但在频次产品仓 GitHub 返回 HTTP 404 `No commit found for the ref`。请提供该仓内包含交接信及证据的真实固定提交；不要拿另一条线的提交或当前 main 替代固定来源。停单之后也不自动执行旧交接，后续依当前状态和有效恢复交接处理。

聊天收到的 `evt-357a19ca5ed7283797bdc60a75ca30b8681b1c7d6522b24b64df1bac4dd2d4c3` 没有频次请求匹配或频次原 claim，未跨线读取、认领或 ACK。此报告是进度/故障回报，不是该事件的协议回执。
