# Astrbot_Plugin_MP
一个运行于astrbot平台的MP调用小工具
使用方法请使用命令"/mp菜单"查看

# 大模型自动调用（LLM 函数工具）
除指令方式外，本插件已可选注册 LLM 函数工具，支持模型在对话中自动调用：

| 工具名 | 说明 |
| --- | --- |
| mp_search_media | 搜索影视资源 |
| mp_subscribe_media | 订阅影视（电视剧需指定季，未指定时返回季列表） |
| mp_list_subscriptions | 查看订阅与进度 |
| mp_search_subscription | 立即触发订阅搜索（可选订阅 ID） |
| mp_enable_notification / mp_disable_notification | 开关本会话的 MP 通知推送 |

直接说“帮我搜一下黑暗荣耀”“订阅黑暗荣耀第二季”“看看我订阅的剧更新到哪了”即可，无需发送 `/mp` 指令。
若模型未调用工具，请在 WebUI 中确认该模型支持工具调用，且当前人格未禁用工具。

如需启用http转发，请在MP中添加聚合消息通知插件，选择非Get请求，并添加以下内容（亦可根据需要自行定义）：

请求头：
```
Content-Type:application/json
```
请求体
```
{
  "title": "${title}",
  "message": "${text}",
  "type": "${type}"
}
```
感谢KoWming大佬提供思路！

# 注意事项
订阅功能当前仅支持默认设置的订阅操作，如需取消，请前往MP后台处理。
# 支持

[帮助文档](https://astrbot.app)
