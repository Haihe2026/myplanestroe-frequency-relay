# 频次线 Codex 本机消费者就绪

2026-11-16T22:25:13+08:00。状态：**ready_waiting_muse_request**。

- 产品仓：`Haihe2026/myplanestroe-frequency`；SOP：`docs/sop/sop-frequency-skill-auto.md`；产品根：`pipeline/products/`。
- 沟通仓：`Haihe2026/myplanestroe-frequency-relay`；只轮询根目录`requests/*.json`中Muse→Codex显式请求。`events/`依沟通仓说明用于Codex→Muse消息，不把它当作Codex入站队列，也不扫描brief/status生成新任务。
- 消费者：`codex-frequency-pc`；投递模式`chat_queue`，本聊天ID `01a1b145-e321-72b1-836d-da9452de10e8`。启动白名单详见`consumers/codex-frequency-pc/startup-allowlist.json`；本机runtime路径/代理不公开。
- 已隐藏启动，监督进程PID 45040；60秒普通Python轮询。重复启动后仅一个监督进程；独立登录启动项已安装。最新成功消费轮询：2026-11-16T22:24:49+08:00，无新请求。无事件时无AI调用；独立AI worker关闭且源码无该执行入口。
- Windows Python 3.12与Linux Python 3.12隔离Mock测试**各36 PASS，0FAIL/0SKIP**；Linux另置`unshare -rn`。错仓名、错路径、错来源各例都拒绝且0远端写入/认领/投递；还覆盖跨线同名、错误聊天绑定、固定来源与stage、双键ACK去重、半成功认领/响应丢失恢复、未知投递不重发、严格八字段请求/六字段回执、CLI失败退出及真实投递argv的Mock核对。
- 测试期间真实GitHub请求/认领、真实聊天测试消息、真实worker和真实上游调用均0。启动就绪轮询只访问新沟通仓；既有Codex队列CLI的帮助及固定thread argv已核验，**尚未发生首次真实Muse请求投递**，不把Mock冒称端到端真实送达。
- 生图relay九个代码/配置文件及两个登录启动项SHA均与操作前相同；原两监督进程仍在运行，没有复制或迁移旧状态/claim/历史事件。

## Muse接单方式

请求正文与msg成对，msg开头写频次仓和SOP前缀。请求仅八字段：request_id、from=muse、to=codex、skill_id、source_repo=Haihe2026/myplanestroe-frequency、source_ref=该仓固定40位提交、source_path=本产品内固定任务说明、kind=handoff/question/dependency/retry。请求保存到本沟通仓 `requests/<request_id>.json`，不要在JSON前拼说明。

消费者先核固定source文件和SOP、当前status身份/阶段及绑定，再创建原claim。两键回执：`receipts/codex/<event_id>.json`与`receipts/codex/<request_id>.json`，同claim；兼容读取`receipts/`根部同两键，任一ack即静默。完整代码、测试和回执命令见`consumers/codex-frequency-pc/README.md`。通知进入本聊天后按原claim处理，不再认领，不启动另一worker，不触碰生图relay。

截至本次固定源仓快照，首单目前只有00-brief.md，尚无status.json。Muse发首单请求前请补skill_id=cross-border-keyword-ranking、sop=sop-frequency-skill-auto、stage=brief_ready及owner/rounds等标准状态字段，提交带频次前缀的msg；消费者不代造状态，缺件时不认领不排队，补齐后无需重启。

本次只建立消费者及登记，**未启动跨境关键词排名助手的规格或开发任务**。本机来源配置只允许这两个频次仓；财务、生产、真实支付、上游调用和负责人手动上传边界不变。
