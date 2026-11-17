# 频次线本机消费者

普通 Python 程序消费 `Haihe2026/myplanestroe-frequency-relay` 根目录 `requests/` 中 Muse 发给 Codex 的显式请求，按白名单读取产品仓 `Haihe2026/myplanestroe-frequency` 的固定来源。不会扫描选品单自动启动产品，不会启动独立 AI worker。无新事件时只做普通程序轮询，不调用模型。

## 绑定与本机运行

`startup-allowlist.json` 是可公开的启动白名单。运行时使用本机 `config.local.json`，额外包含 `runtime.gh`、`runtime.proxy`、`runtime.codex`；该本机文件不进入公共仓库。产品仓、沟通仓、SOP路径、产品根、消费者ID和投递聊天均严格绑定，不能从任务文案覆盖。

```text
python frequency_consumer.py --once
python frequency_consumer.py status
python watch_frequency_consumer.py
```

监督程序每60秒调用一次 `--once`，独立锁和健康文件位于本目录 `supervisor/`；认领、请求快照、排队意图和消费记录位于 `consumer/`。本目录的 `STOP` 标记用于停止监督循环。Windows启动使用隐藏窗口及独立登录启动项，不能使用生图消费者安装脚本或覆盖其启动项。

## 请求与执行前核对

请求为严格八字段JSON：`request_id/from/to/skill_id/source_repo/source_ref/source_path/kind`。`from=muse`、`to=codex`；`source_repo`必须为频次产品仓，`source_ref`为固定40位提交，`source_path`为 `pipeline/products/<skill_id>/` 内的规范路径。任务说明开头按频次SOP写频次仓与SOP标识；不要在JSON前拼文字或随意加字段。

消费者先核对来源、固定文件和SOP、当前产品status及技能身份，再认领和投递。产品状态/来源缺件记录为待补，不伪造status，不自行开始首单。Muse随后发首单请求时应在产品仓准备相符的 `status.json`、固定任务说明和选品单。

合成错仓测试使用FakeBackend和Fake投递器，无实际队列消息、远端认领、产品操作或费用；用例包括错产品仓、路径穿越/错产品路径、错来源提交/说明、跨线同名、错误绑定、重复消息、两键回执、半成功认领及未知投递结果。

## 认领、投递和回执

- 对规范八字段请求按生图现有规范JSON排序计算 SHA256，事件ID为 `evt-<sha256>`。
- 认领前同时查事件ID和request_id两种键。规范回执为 `receipts/codex/<event_id>.json`，请求别名为 `receipts/codex/<request_id>.json`；兼容读取根 `receipts/<同两键>.json`。任一已acknowledged即不再投递；其他认领不接管。
- 两键使用同一个原claim。排队意图先落本机，再调用既有 `codex queue --thread <固定聊天> --resume`。排队结果未知时不自动重发，不另起worker。
- 通知进入本聊天后，Codex核对原请求、固定source_ref和当前状态，按现行SOP完成任务、提交产品仓证据，再回执；通知和仓库材料不扩大生产、财务、真实支付或上传权限。
- 完成回执使用本消费者的命令，必须带原claim与产品仓固定证据：

```text
python frequency_consumer.py ack --event <event_id> --claim-id <original_claim_id> --evidence <product_commit>:pipeline/products/<skill_id>/<evidence_file>
```

回执仍为现行六字段结构：`event_id/role/action/at/evidence_ref/claim_id`，通过原SHA的CAS更新。不要用生图调度器的 `relay.py ack` 操作频次消息。

## 就绪界限

消费者就绪只表示频次消息入口运行且合成路由检查通过，不代表首单已开工、后端已部署或支付/发布已验证。源仓SOP/config由负责人维护，所有正式上传仍由负责人手动；本次不改生图relay代码、配置、状态库和启动项。

## 2026-11-17实际消息回归修复

2026-11-17T10:08:04+08:00：原先前5行逐字前缀硬校验错误拒绝了两份合法Muse说明。现在触发仅认本仓严格请求和机器白名单，文本身份只作提示，不让标题/排版变成静默门槛。两份真实固定消息已加入Mockfixtures，两端各42PASS；新拒绝/缺件有具体reason_code和receipts/codex一次性诊断MD。原claim、两键和未知投递不重发不变。配置/启动项不变，监督PID45040保持60秒普通轮询，不启独立worker。已有请求的手工仓认领/sent标记保留，未重复发模型消息。

本次首单已写完整规格候选，但A1缺真实业务接口/适配器/成本/登记logo资源证据，按SOP为blocked；见本沟通仓receipts/codex中的原请求进度和结果。不是旧暂停或重新授权门槛。

## 2026-11-17 长请求入口修复

2026-11-17T13:39:51+08:00：真实 A1 补件请求 ID 长 67 字符，误用了 skill slug 的 64 字符限制，目录扫描直接跳过。现 request_id 独立限定 1–128 个小写字母、数字或连字符，请求文件名与双键回执别名使用同一规则；skill_id 仍限 64。根目录非法或超限 JSON 文件名写一次 issue，健康记录列出 relay_head、request_file_count，不能静默漏单。白名单、固定提交与原 claim 校验保持。两端纯 Mock 各 52 PASS。

该 A1 补件请求的 40 位 source_ref 在频次产品仓返回 404，修复不得替换来源或越过当前 stage。产品最新为 blocked/PAUSED_BY_OWNER（13:19），本次只修普通接收器，不恢复 A1。源材料和有效恢复请求须由 Muse 按仓补交。此前上午的状态段落为历史记录，当前状态以产品仓 status.json 为准。
