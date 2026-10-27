import os
import json
import aiohttp
import asyncio
import datetime
from datetime import timedelta
from collections import defaultdict
import threading
from aiohttp import web
import socket
from urllib.parse import urlparse, parse_qs
import sys
from queue import Queue
from pathlib import Path
import re
import codecs
import time
from astrbot.api.event import filter, AstrMessageEvent, MessageChain
from astrbot.api.star import Context, Star, register, StarTools
import astrbot.api
from astrbot.api import logger

# --- 插件介绍代码内容 ---
@register("astrbot_plugin_mp", "EWEDL", "MoviePilot小工具", "1.6.0")
class MediaSearchPlugin(Star):
    """集成媒体搜索、订阅管理以及基于HTTP的分类消息通知功能。
    支持持久化订阅和文件日志。"""
    def __init__(self, context: Context, config: dict = None):
        super().__init__(context)
        self.config = config or {}
        self._init_paths()
        self._init_logging()
        self._load_api_config()
        self._load_display_notification_config()
        self.access_token = None
        self.token_refresh_count = 0
        self.user_search_results = {}
        self.notification_subscriptions = defaultdict(set)
        self.message_queue = Queue()
        self.httpd = None
        self.server_thread = None
        self.message_processor_task = None
        self.server_stop_event = threading.Event()
        self.user_states = {}  # 多轮交互状态
        self.pending_subscription = {}  # 记录待处理的多季订阅信息
        self.logger.info(f"Subscription persistence file path: {self.subscriptions_file}", extra={"plugin": "mp"})

    def _init_paths(self):
        """初始化插件所需的路径配置"""
        try:
            self.data_dir = StarTools.get_data_dir()
            self.plugin_dir = self.data_dir
            self.subscriptions_file = self.data_dir / "mp_sub.json"
            self.log_file_path = self.data_dir / "http.log"
        except Exception:
            self.plugin_dir = Path(".")
            self.subscriptions_file = Path("./mp_sub.json")
            self.log_file_path = Path("./http.log")

    def _init_logging(self):
        """初始化日志系统，使用框架logger，并额外写入详细日志文件（仅记录本插件日志）"""
        self.logger = logger
        self.logger.info(f"详细日志系统初始化完成，保存在框架日志中", extra={"plugin": "mp"})
        # 额外写入详细日志文件，仅记录本插件日志
        try:
            import logging
            class PluginOnlyFilter(logging.Filter):
                def filter(self, record):
                    return getattr(record, "plugin", None) == "mp"
            detailed_log_path = self.data_dir / "detailed.log"
            file_handler = logging.FileHandler(str(detailed_log_path), mode='a', encoding='utf-8')
            file_handler.setLevel(logging.DEBUG)
            formatter = logging.Formatter('[%(asctime)s][%(levelname)s][%(funcName)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
            file_handler.setFormatter(formatter)
            file_handler.addFilter(PluginOnlyFilter())
            # 为避免重复添加，先移除同类 handler
            for h in list(self.logger.handlers):
                if isinstance(h, logging.FileHandler) and getattr(h, 'baseFilename', None) == str(detailed_log_path):
                    self.logger.removeHandler(h)
            self.logger.addHandler(file_handler)
            self.logger.info("详细日志也写入: %s", str(detailed_log_path), extra={"plugin": "mp"})
        except Exception as e:
            self.logger.warning(f"详细日志文件配置失败: {e}", extra={"plugin": "mp"})

    def _check_and_clear_daily_log(self):
        """已弃用：每日清空日志逻辑，交由框架日志管理"""
        pass

    def _log_and_check_daily(self, level, msg, *args, **kwargs):
        """写日志，直接调用框架logger"""
        if level == 'debug':
            self.logger.debug(msg, *args, **kwargs)
        elif level == 'info':
            self.logger.info(msg, *args, **kwargs)
        elif level == 'warning':
            self.logger.warning(msg, *args, **kwargs)
        elif level == 'error':
            self.logger.error(msg, *args, **kwargs)
        elif level == 'exception':
            self.logger.exception(msg, *args, **kwargs)
        else:
            self.logger.info(msg, *args, **kwargs)

    def _load_api_config(self):
        """加载API配置"""
        api_config = self.config.get("api_config", {})
        self.username = api_config.get("username", "")
        self.password = api_config.get("password", "")
        self.base_url = api_config.get("base_url", "").rstrip('/')
        
        if not self.base_url:
            self.logger.warning("MP base_url未配置", extra={"plugin": "mp"})
            
        self.token_url = f"{self.base_url}/api/v1/login/access-token" if self.base_url else ""
        self.subscribe_url = f"{self.base_url}/api/v1/subscribe/" if self.base_url else ""

    def _load_display_notification_config(self):
        """加载显示和通知配置"""
        # 加载搜索结果设置
        display_config = self.config.get("display_config", {})
        self.max_results = display_config.get("max_results", 5)
        
        # 加载通知设置
        notification_config = self.config.get("notification_config", {})
        
        # 从notification_config加载HTTP转发功能开关
        self.http_forward_enabled = notification_config.get("enabled", False)
        
        # 加载HTTP监听端口
        self.listen_port = notification_config.get("listen_port", 8080)
        
        try:
            port_num = int(self.listen_port)
            if not (1024 <= port_num <= 65535):
                raise ValueError("Port out of range")
            self.listen_port = port_num
        except (ValueError, TypeError):
            self.logger.warning(f"无效的端口号 ('{self.listen_port}')，使用默认值8080", extra={"plugin": "mp"})
            self.listen_port = 8080

    async def initialize(self):
        """初始化插件，加载订阅、获取令牌并启动HTTP服务器"""
        self.logger.info("初始化MediaSearchPlugin...", extra={"plugin": "mp"})
        self.logger.info(f"插件目录: {self.plugin_dir}", extra={"plugin": "mp"})
        self.logger.info(f"日志文件路径: {self.log_file_path}", extra={"plugin": "mp"})
        self.logger.info(f"订阅文件路径: {self.subscriptions_file}", extra={"plugin": "mp"})
        # 加载订阅
        if self.subscriptions_file:
            self._load_subscriptions()
        else:
            self.logger.warning("订阅持久化已禁用", extra={"plugin": "mp"})
        # 初始化API令牌
        if not self.base_url or not self.username or not self.password:
            self.logger.error("API配置不完整", extra={"plugin": "mp"})
        else:
            self.access_token = await self.get_access_token(self.username, self.password, self.token_url)
            if not self.access_token:
                self.logger.warning("初始令牌获取失败", extra={"plugin": "mp"})
        # 初始化通知系统
        if not self.http_forward_enabled:
            self.logger.info("HTTP转发功能已禁用，不启动HTTP服务器", extra={"plugin": "mp"})
            return
        self.logger.info("正在初始化通知系统...", extra={"plugin": "mp"})
        try:
            self.http_app = web.Application()
            self.http_app.add_routes([
                web.post('/', self.aiohttp_notification_handler),
                web.put('/', self.aiohttp_notification_handler),
            ])
            self.http_runner = web.AppRunner(self.http_app)
            await self.http_runner.setup()
            self.http_site = web.TCPSite(self.http_runner, LISTEN_ADDRESS, self.listen_port)
            await self.http_site.start()
            self.logger.info(f"HTTP服务器启动在 {LISTEN_ADDRESS}:{self.listen_port}", extra={"plugin": "mp"})
            self.message_processor_task = asyncio.create_task(self.process_message_queue())
            self.logger.info("消息处理任务已启动", extra={"plugin": "mp"})
        except OSError as e:
            self.logger.error(f"HTTP服务器在端口 {self.listen_port} 启动失败: {e}", extra={"plugin": "mp"})
            self.http_app = None
            self.http_runner = None
            self.http_site = None
        except Exception as e:
            self.logger.error(f"通知系统初始化失败: {e}", exc_info=True, extra={"plugin": "mp"})
            self.http_app = None
            self.http_runner = None
            self.http_site = None

    async def aiohttp_notification_handler(self, request: web.Request) -> web.Response:
        """
        异步处理收到的HTTP通知。
        此函数负责解码请求体，并从中提取消息类型，然后将原始消息体和类型放入队列。
        """
        timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        client_ip = request.remote
        log_entry = [f"\n----- HTTP Log Start -----"]
        log_entry.append(f"Timestamp: {timestamp}")
        log_entry.append(f"Client: {client_ip}")
        log_entry.append(f"Method: {request.method}")
        log_entry.append(f"Path: {request.path}")

        msg_type = "未知"
        body_str = ""
        body_description = "No Body"

        try:
            if request.has_body:
                body_bytes = await request.read()
                try:
                    # 解码请求体
                    body_str = body_bytes.decode('utf-8')
                    body_description = "Text" # 初始假定为文本
                    # 无论JSON是否有效，都尝试用正则提取type
                    type_match = re.search(r'"type":\s*"(.*?)"', body_str)
                    if type_match:
                        msg_type = type_match.group(1)
                        body_description = f"Text (Type found by Regex: {msg_type})"
                    else:
                        msg_type = "文本" # 如果连type字段都找不到
                except UnicodeDecodeError:
                    body_str = f"(Undecodable bytes: {body_bytes!r})"
                    msg_type = "原始数据"
                    body_description = "Undecodable"
            
            log_entry.append(f"Body Type: {body_description}")
            log_entry.append(f"Raw Body Content: {body_str if body_str else '(No body)'}")

            if body_str:
                # 将原始字符串和提取出的类型放入队列
                self.message_queue.put((timestamp, msg_type, body_str))
                log_entry.append(f"Message Queued: Yes (Final Type: {msg_type})")
            else:
                log_entry.append("Message Queued: No (Empty content)")

            log_entry.append("----- HTTP Log End -----\n")
            self.logger.info("\n".join(log_entry), extra={"plugin": "mp"})
            
            return web.Response(status=200, text="Notification received.")

        except Exception as e:
            self.logger.error(f"aiohttp通知处理异常: {e}", exc_info=True, extra={"plugin": "mp"})
            return web.Response(status=500, text="Internal Server Error.")

    def _load_subscriptions(self):
        """从文件加载通知订阅"""
        if not self.subscriptions_file:
            return
            
        if self.subscriptions_file.exists() and self.subscriptions_file.is_file():
            try:
                with open(self.subscriptions_file, 'r', encoding='utf-8') as f:
                    loaded_data = json.load(f)
                    
                temp_subscriptions = defaultdict(set)
                for chat_id, categories in loaded_data.items():
                    if isinstance(categories, list):
                        valid_cats = {cat for cat in categories if cat in ALLOWED_CATEGORIES}
                        if valid_cats:
                            temp_subscriptions[str(chat_id)] = valid_cats
                        else:
                            self.logger.warning(f"聊天ID {chat_id} 的订阅只包含无效类别，已跳过", extra={"plugin": "mp"})
                    else:
                        self.logger.warning(f"聊天ID {chat_id} 的订阅格式无效，已跳过", extra={"plugin": "mp"})
                        
                self.notification_subscriptions = temp_subscriptions
                self.logger.info(f"从 {self.subscriptions_file} 加载了 {len(self.notification_subscriptions)} 个聊天订阅", extra={"plugin": "mp"})
            except Exception as e:
                self.logger.error(f"加载订阅失败: {e}", exc_info=True, extra={"plugin": "mp"})
                self.notification_subscriptions = defaultdict(set)
        else:
            self.logger.info("订阅文件不存在，使用空订阅", extra={"plugin": "mp"})
            self.notification_subscriptions = defaultdict(set)

    def _save_subscriptions(self):
        """保存通知订阅到文件"""
        if not self.subscriptions_file:
            self.logger.warning("无法保存订阅：未设置文件路径", extra={"plugin": "mp"})
            return
            
        try:
            self.subscriptions_file.parent.mkdir(parents=True, exist_ok=True)
            data_to_save = {chat_id: list(categories) for chat_id, categories in self.notification_subscriptions.items()}
            
            with open(self.subscriptions_file, 'w', encoding='utf-8') as f:
                json.dump(data_to_save, f, ensure_ascii=False, indent=4)
                
            self.logger.info(f"已保存 {len(data_to_save)} 个聊天订阅到 {self.subscriptions_file}", extra={"plugin": "mp"})
        except Exception as e:
            self.logger.error(f"保存订阅失败: {e}", exc_info=True, extra={"plugin": "mp"})

    def run_http_server(self):
        """运行HTTP服务器的线程函数"""
        if not self.httpd:
            self.logger.error("HTTP服务器实例为None", extra={"plugin": "mp"})
            return
            
        self.logger.info("HTTP服务器线程已启动", extra={"plugin": "mp"})
        self.httpd.timeout = 1.0
        
        while not self.server_stop_event.is_set():
            try:
                self.httpd.handle_request()
            except socket.timeout:
                continue
            except Exception as e:
                self.logger.error(f"HTTP服务器循环出错: {e}", exc_info=True, extra={"plugin": "mp"})
                
        self.logger.info("HTTP服务器收到停止信号", extra={"plugin": "mp"})
        
        try:
            self.httpd.server_close()
        except Exception as e:
            self.logger.error(f"关闭HTTP socket时出错: {e}", extra={"plugin": "mp"})
            
        self.logger.info("HTTP服务器socket已关闭", extra={"plugin": "mp"})

    async def process_message_queue(self):
        """
        异步处理消息队列，解析通知并发送到订阅者。
        此版本简化了消息解析逻辑，优先处理JSON，否则作为纯文本。
        """
        self.logger.info("消息处理循环已启动 (JSON优先模式)", extra={"plugin": "mp"})
        
        # 正则表达式用于从可能无效的JSON字符串中提取字段
        title_pattern = re.compile(r'"title":\s*"(.*?)"', re.DOTALL)
        message_pattern = re.compile(r'"message":\s*"((?:.|\n)*?)"\s*(?:,|}|\Z)', re.DOTALL)

        while True:
            try:
                if self.message_queue.empty():
                    await asyncio.sleep(0.5)
                    continue
                    
                timestamp, msg_type, content_str = self.message_queue.get_nowait()
                self.logger.info(f"处理消息. 类型='{msg_type}'", extra={"plugin": "mp"})
                
                formatted_message = ""
                
                try:
                    # 优先尝试将内容解析为标准JSON
                    data = json.loads(content_str)
                    title = data.get("title")
                    message = data.get("message")

                    # 过滤掉 None 或 "None"
                    valid_title = title if title is not None and title != "None" else None
                    valid_message = message if message is not None and message != "None" else None
                    
                    if valid_title and valid_message:
                        formatted_message = f"{valid_title}\n{valid_message}"
                    elif valid_title:
                        formatted_message = valid_title
                    elif valid_message:
                        formatted_message = valid_message
                    else:
                        # 如果两者都无效，则不发送任何内容
                        formatted_message = ""
                except json.JSONDecodeError:
                    # 如果JSON解析失败，回退到正则提取
                    self.logger.warning(f"JSON解析失败，回退到正则提取. 内容: {content_str[:100]}...", extra={"plugin": "mp"})
                    title_match = title_pattern.search(content_str)
                    message_match = message_pattern.search(content_str)
                    
                    title = title_match.group(1) if title_match else None
                    message = message_match.group(1) if message_match else None

                    # 清理可能存在的转义符
                    if title: title = title.replace('\\n', '\n').replace('\\"', '"')
                    if message: message = message.replace('\\n', '\n').replace('\\"', '"')

                    # 过滤掉 None 或 "None"
                    valid_title = title if title is not None and title != "None" else None
                    valid_message = message if message is not None and message != "None" else None

                    if valid_title and valid_message:
                        formatted_message = f"{valid_title}\n{valid_message}"
                    elif valid_title:
                        formatted_message = valid_title
                    elif valid_message:
                        formatted_message = valid_message
                    else:
                        # 如果两者都无效，则不发送任何内容
                        formatted_message = ""
                
                # 发送消息给订阅者
                if formatted_message:
                    subs_copy = dict(self.notification_subscriptions)
                    sent_to_any = False
                    
                    for cid, cats in subs_copy.items():
                        is_subscribed_to_all = "所有" in cats
                        is_subscribed_to_type = msg_type in cats and msg_type in ALLOWED_CATEGORIES
                        
                        if is_subscribed_to_all or is_subscribed_to_type:
                            try:
                                await self.context.send_message(cid, MessageChain().message(formatted_message))
                                self.logger.info(f"消息已发送 (类型: {msg_type}) 到: {cid}", extra={"plugin": "mp"})
                                sent_to_any = True
                            except Exception as e:
                                self.logger.error(f"发送失败 到 {cid}: {e}", extra={"plugin": "mp"})
                    
                    if not sent_to_any:
                        self.logger.debug(f"没有类型为 '{msg_type}' 的订阅", extra={"plugin": "mp"})
                else:
                    self.logger.warning(f"格式化消息为空 (类型: {msg_type})，不发送", extra={"plugin": "mp"})
                
                self.message_queue.task_done()

            except asyncio.CancelledError:
                self.logger.info("处理任务被取消", extra={"plugin": "mp"})
                break
            except Exception as e:
                self.logger.error(f"消息处理循环错误: {e}", exc_info=True, extra={"plugin": "mp"})
                await asyncio.sleep(1)

    # --- 命令组和命令实现 ---
    @filter.command_group("mp")
    def mp(self): 
        """mp命令组"""
        pass
        
    @mp.command("菜单")
    async def menu_command(self, event: AstrMessageEvent):
        """显示插件功能菜单"""
        # 记录收到菜单命令
        self.logger.info(f"收到用户菜单命令, 用户: {getattr(event, 'unified_msg_origin', None)}", extra={"plugin": "mp"})
        # 确定HTTP服务器状态
        http_status = "未启用 ⛔"
        if self.http_forward_enabled:
            # 新的aiohttp判断逻辑
            if getattr(self, 'http_runner', None) and getattr(self, 'http_site', None):
                ip_addr = get_local_ip() if LISTEN_ADDRESS == '0.0.0.0' else LISTEN_ADDRESS
                http_status = f"运行中 ✅ - http://{ip_addr}:{self.listen_port}"
            else:
                http_status = f"未运行 ❌ - 端口 {self.listen_port}"
        menu_text = f"""📺 mp 功能菜单 v1.6.0 📺
---------------------------------
【媒体管理】
  mp 搜索 [关键词] > 搜索媒体。 例: mp 搜索 黑暗荣耀
  mp 新增订阅 [序号] > 订阅搜索结果。 例: mp 新增订阅 1
  mp 查看订阅 > 显示已订阅内容。
  mp 搜索订阅 [ID] > 检查订阅任务(可选ID)。 例: mp 搜索订阅 123
【消息通知】
  mp 启用通知 [所有/类别|类别]
  mp 取消通知 [所有/类别|类别]
  多个类别用|分割
  mp 菜单 > 显示此菜单。
---------------------------------
可用类别: 资源下载,整理入库,订阅,媒体服务器,手动处理,插件,其他,站点,所有
HTTP服务: {http_status}
---------------------------------
提示: 也可以直接用自然语言对话，大模型会自动调用搜索、订阅、查看订阅、通知等能力
注意: 所有命令仅支持小写形式(mp)"""
        yield event.plain_result(menu_text.strip())
        
    @mp.command("搜索")
    async def search_command(self, event: AstrMessageEvent, keyword: str):
        """搜索媒体内容"""
        userid = str(event.unified_msg_origin)
        if not self.access_token:
            self.access_token = await self.get_access_token(self.username, self.password, self.token_url)
            if not self.access_token:
                yield event.plain_result("⚠️ Token获取失败。")
                return
        self.token_refresh_count = 0
        media_data = await self.search_media(self.access_token, keyword)
        if media_data:
            cleaned_data = self.remove_empty_keys(media_data)
            if not cleaned_data:
                yield event.plain_result("无匹配内容。")
                return
            self.user_search_results[userid] = cleaned_data
            result_text = self.format_search_results(cleaned_data) + "\n\n👉 可用 `mp 新增订阅 序号` 订阅"
            yield event.plain_result(result_text)
        else:
            yield event.plain_result("⚠️ 搜索失败或无结果。")
        
    @mp.command("新增订阅")
    async def add_subscription_command(self, event: AstrMessageEvent, index: str):
        """将搜索结果添加到订阅（支持TMDB多季选择）"""
        userid = str(event.unified_msg_origin)
        if userid not in self.user_search_results or not self.user_search_results[userid]:
            yield event.plain_result("⚠️ 请先搜索。")
            return
        try:
            idx = int(index) - 1
            search_results = self.user_search_results[userid]
            if not (0 <= idx < len(search_results)):
                yield event.plain_result(f"⚠️ 无效序号 {index} (范围 1-{len(search_results)})")
                return
        except ValueError:
            yield event.plain_result(f"⚠️ 无效序号 {index} (请输入数字)。")
            return
        media_item = search_results[idx]
        media_title = media_item.get("title", "未知")
        source = media_item.get("source") or media_item.get("media_source", "")
        tmdbid = media_item.get("tmdb_id")
        media_type = media_item.get("type", "")
        # 只有电视剧才查季
        if source == "themoviedb" and tmdbid and media_type == "电视剧":
            if not self.access_token:
                self.access_token = await self.get_access_token(self.username, self.password, self.token_url)
                if not self.access_token:
                    yield event.plain_result("⚠️ Token获取失败。")
                    return
            # 查询季信息
            tmdb_url = f"{self.base_url}/api/v1/tmdb/seasons/{tmdbid}"
            try:
                headers = {"accept": "application/json", "Authorization": f"Bearer {self.access_token}"}
                timeout = aiohttp.ClientTimeout(total=10)
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    async with session.get(tmdb_url, headers=headers) as resp:
                        if resp.status == 200:
                            seasons = await resp.json()
                            if isinstance(seasons, dict):  # 兼容 {"success":..., "data": [...]} 包装
                                seasons = seasons.get("data")
                            if isinstance(seasons, list) and len(seasons) > 1:
                                # 多季，提示用户选择
                                season_list = []
                                for i, s in enumerate(seasons, 1):
                                    sn = s.get("season_number", "?")
                                    name = s.get("name", "?")
                                    season_list.append(f"{i}、第{sn}季 {name}")
                                msg = "该剧集有多季，请回复所有或序号选择要订阅的季：\n" + "\n".join(season_list) + "\n（输入退出可取消）"
                                self.user_states[userid] = "waiting_tmdb_season"
                                self.pending_subscription[userid] = {
                                    "media_item": media_item,
                                    "seasons": seasons
                                }
                                yield event.plain_result(msg)
                                return
                            elif isinstance(seasons, list) and len(seasons) == 1:
                                # 只有一季，直接订阅
                                media_item = dict(media_item)
                                media_item["season"] = seasons[0].get("season_number", 1)
                                transformed_data = self.transform_data(media_item)
                                response = await self.add_subscription(self.access_token, transformed_data)
                                if response and response.get("success") == True:
                                    yield event.plain_result(f"✅ `{media_title}` 订阅成功。")
                                else:
                                    error_msg = (response.get("msg") or response.get("message") or "看日志。") if isinstance(response, dict) else "看日志。"
                                    yield event.plain_result(f"⚠️ 订阅 `{media_title}` 失败: {error_msg}")
                                return
                            else:
                                yield event.plain_result("⚠️ 未获取到季信息，无法订阅。")
                                return
                        else:
                            yield event.plain_result(f"⚠️ 查询季信息失败: {resp.status}")
                            return
            except Exception as e:
                yield event.plain_result(f"⚠️ 查询季信息异常: {e}")
                return
        # 其他来源或无tmdbid或非电视剧，直接订阅
        if not self.access_token:
            self.access_token = await self.get_access_token(self.username, self.password, self.token_url)
            if not self.access_token:
                yield event.plain_result("⚠️ Token获取失败。")
                return
        transformed_data = self.transform_data(media_item)
        response = await self.add_subscription(self.access_token, transformed_data)
        if response and response.get("success") == True:
            yield event.plain_result(f"✅ `{media_title}` 订阅成功。")
        else:
            error_msg = (response.get("msg") or response.get("message") or "看日志。") if isinstance(response, dict) else "看日志。"
            yield event.plain_result(f"⚠️ 订阅 `{media_title}` 失败: {error_msg}")
        
    @mp.command("查看订阅")
    async def view_subscriptions_command(self, event: AstrMessageEvent):
        """查看当前订阅"""
        if not self.access_token:
            self.access_token = await self.get_access_token(self.username, self.password, self.token_url)
            if not self.access_token:
                yield event.plain_result("⚠️ Token获取失败。")
                return
        self.token_refresh_count = 0
        subscription_data = await self.get_subscription_data(self.access_token)
        if subscription_data:
            yield event.plain_result(self.format_subscription_data(subscription_data))
        else:
            yield event.plain_result("⚠️ 获取订阅数据失败或无订阅。")
        
    @mp.command("搜索订阅")
    async def search_subscription_command(self, event: AstrMessageEvent, subscription_id: str = ""):
        """执行订阅搜索"""
        if not self.access_token:
            self.access_token = await self.get_access_token(self.username, self.password, self.token_url)
            if not self.access_token:
                yield event.plain_result("⚠️ Token获取失败。")
                return
        self.token_refresh_count = 0
        search_result = await self.search_subscription(self.access_token, subscription_id.strip())
        if search_result:
            if search_result.get("success"):
                yield event.plain_result(f"✅ 执行订阅搜索{' (ID: ' + subscription_id + ')' if subscription_id else ''}。看后台任务。")
            else:
                msg = search_result.get("msg") or search_result.get("message") or "?"
                yield event.plain_result(f"⚠️ 订阅搜索API失败: {msg}")
        else:
            yield event.plain_result("⚠️ 无法获取订阅搜索结果。")
        
    @mp.command("启用通知")
    async def enable_notification_command(self, event: AstrMessageEvent, categories_str: str):
        """启用指定类别的通知"""
        chat_id = str(event.unified_msg_origin)
        result_text = self._enable_notification_categories(chat_id, self._parse_categories(categories_str))
        yield event.plain_result(result_text)

    @mp.command("取消通知")
    async def disable_notification_command(self, event: AstrMessageEvent, categories_str: str):
        """取消指定类别的通知"""
        chat_id = str(event.unified_msg_origin)
        result_text = self._disable_notification_categories(chat_id, self._parse_categories(categories_str))
        yield event.plain_result(result_text)

    # --- LLM 函数工具：大模型可在对话中自动调用，无需用户发送指令 ---

    async def _get_valid_token(self):
        """获取可用的访问令牌，失败时返回 None"""
        if self.access_token:
            return self.access_token
        self.access_token = await self.get_access_token(self.username, self.password, self.token_url)
        return self.access_token

    def _parse_categories(self, categories_str: str) -> set:
        """解析通知类别字符串，支持 | , ， 、 和空格分隔"""
        return {c.strip() for c in re.split(r"[|,，、\s]+", categories_str or "") if c.strip()}

    def _enable_notification_categories(self, chat_id: str, categories: set) -> str:
        """启用指定类别的通知，返回结果文本"""
        if not categories:
            return f"⚠️ 请指定类别. 可用: {', '.join(sorted(ALLOWED_CATEGORIES))}"

        valid_cats = categories & ALLOWED_CATEGORIES
        invalid_cats = categories - ALLOWED_CATEGORIES

        if not valid_cats:
            return f"⚠️ 类别无效. 可用: {', '.join(sorted(ALLOWED_CATEGORIES - {'所有'}))} 或 '所有'"

        added = set()
        current = self.notification_subscriptions.setdefault(chat_id, set())
        changed = False

        if "所有" in valid_cats:
            if "所有" not in current or len(current) > 1:
                current.clear()
                current.add("所有")
                added.add("所有")
                changed = True
        else:
            if "所有" in current:
                current.remove("所有")
                changed = True
            for cat in valid_cats:
                if cat not in current:
                    current.add(cat)
                    added.add(cat)
                    changed = True

        if changed:
            self._save_subscriptions()

        parts = []
        if added:
            parts.append(f"✅ 已启用: {', '.join(sorted(added))}。")
        elif valid_cats:
            parts.append(f"ℹ️ 已启用状态: {', '.join(sorted(valid_cats))}。")
        if invalid_cats:
            parts.append(f"⚠️ 忽略无效: {', '.join(sorted(invalid_cats))}。")
        final = self.notification_subscriptions.get(chat_id, set())
        parts.append(f"当前启用: {', '.join(sorted(final)) if final else '无'}")
        return "\n".join(parts)

    def _disable_notification_categories(self, chat_id: str, categories: set) -> str:
        """取消指定类别的通知，返回结果文本"""
        if not categories:
            return f"⚠️ 请指定类别. 可用: {', '.join(sorted(ALLOWED_CATEGORIES))}"

        current = self.notification_subscriptions.get(chat_id)
        if not current:
            return "ℹ️ 当前无启用通知。"

        if "所有" in categories:
            del self.notification_subscriptions[chat_id]
            self._save_subscriptions()
            return "✅ 已取消所有通知。"

        valid_rem = categories & ALLOWED_CATEGORIES
        invalid_cats = categories - ALLOWED_CATEGORIES

        if not valid_rem:
            return f"⚠️ 类别无效. 可用: {', '.join(sorted(ALLOWED_CATEGORIES - {'所有'}))} 或 '所有'"

        removed = set()
        changed = False
        for cat in valid_rem:
            if cat in current:
                current.remove(cat)
                removed.add(cat)
                changed = True

        if not current:
            self.notification_subscriptions.pop(chat_id, None)
        if changed:
            self._save_subscriptions()

        parts = []
        if removed:
            parts.append(f"✅ 已取消: {', '.join(sorted(removed))}。")
        elif valid_rem:
            parts.append(f"ℹ️ 指定类别未启用: {', '.join(sorted(valid_rem))}。")
        if invalid_cats:
            parts.append(f"⚠️ 忽略无效: {', '.join(sorted(invalid_cats))}。")
        final = self.notification_subscriptions.get(chat_id, set())
        parts.append(f"当前剩余: {', '.join(sorted(final)) if final else '无'}")
        return "\n".join(parts)

    def _pick_best_media(self, candidates: list, title: str, year: str = "", media_type: str = ""):
        """从搜索结果中挑选与用户意图最匹配的一条记录"""
        target = (title or "").strip().lower()
        best_item, best_score = None, 0
        for item in candidates:
            name = str(item.get("title") or "").strip().lower()
            if not name:
                continue
            score = 0
            if name == target:
                score += 100
            elif name.startswith(target) or target.startswith(name):
                score += 50
            elif target in name or name in target:
                score += 30
            if year and str(item.get("year") or "").strip() == str(year).strip():
                score += 20
            if media_type and item.get("type") == str(media_type).strip():
                score += 20
            if score > best_score:
                best_item, best_score = item, score
        return best_item

    async def _get_tmdb_seasons(self, token: str, tmdb_id):
        """获取 TMDB 剧集的季列表，请求失败返回 None"""
        url = f"{self.base_url}/api/v1/tmdb/seasons/{tmdb_id}"
        headers = {"accept": "application/json", "Authorization": f"Bearer {token}"}
        try:
            timeout = aiohttp.ClientTimeout(total=10)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(url, headers=headers) as resp:
                    if resp.status != 200:
                        self.logger.warning(f"查询季信息失败: {resp.status}", extra={"plugin": "mp"})
                        return None
                    seasons = await resp.json()
                    # 兼容 {"success":..., "data": [...]} 包装
                    if isinstance(seasons, dict):
                        seasons = seasons.get("data")
                    return seasons if isinstance(seasons, list) else []
        except Exception as e:
            self.logger.error(f"查询季信息异常: {e}", exc_info=True, extra={"plugin": "mp"})
            return None

    async def _subscribe_one(self, token: str, media_item: dict, display_name: str) -> str:
        """提交单条订阅，返回结果文本"""
        response = await self.add_subscription(token, self.transform_data(media_item))
        if response and response.get("success") is True:
            return f"✅ 已成功订阅{display_name}。"
        error_msg = (response.get("msg") or response.get("message") or "未知错误") if isinstance(response, dict) else "未知错误"
        return f"⚠️ 订阅{display_name}失败: {error_msg}"

    @filter.llm_tool(name="mp_search_media")
    async def llm_search_media(self, event: AstrMessageEvent, keyword: str) -> str:
        """在 MoviePilot 媒体库中搜索电影、电视剧等影视资源。当用户想找片、搜片、查询某部影视能否下载或有哪些版本时调用。

        Args:
            keyword(string): 影视作品名称关键词，例如“黑暗荣耀”
        """
        token = await self._get_valid_token()
        if not token:
            return "错误：MoviePilot 登录失败，请检查插件配置中的用户名、密码与基础地址。"

        keyword = (keyword or "").strip()
        if not keyword:
            return "请提供要搜索的影视名称。"

        media_data = await self.search_media(token, keyword)
        cleaned = self.remove_empty_keys(media_data) if media_data else None
        if not cleaned:
            return f"未搜索到与“{keyword}”相关的影视资源。"

        # 同时缓存给命令式订阅使用
        self.user_search_results[str(event.unified_msg_origin)] = cleaned

        lines = [f"共搜索到 {len(cleaned)} 条与“{keyword}”相关的结果："]
        for i, item in enumerate(cleaned[: self.max_results], 1):
            year = item.get("year")
            year_str = f"({year})" if year else ""
            tmdb_id = item.get("tmdb_id")
            tmdb_str = f", TMDB ID: {tmdb_id}" if tmdb_id else ""
            lines.append(
                f"{i}. 《{item.get('title', '?')}》{year_str} 类型: {item.get('type', '?')} "
                f"来源: {item.get('source') or item.get('media_source', '?')}{tmdb_str}"
            )
        lines.append("需要订阅时调用 mp_subscribe_media 工具；电视剧有多季时需先向用户确认季数。")
        return "\n".join(lines)

    @filter.llm_tool(name="mp_subscribe_media")
    async def llm_subscribe_media(
        self,
        event: AstrMessageEvent,
        title: str,
        year: str = "",
        season: str = "",
        media_type: str = "",
    ) -> str:
        """在 MoviePilot 中新增订阅（自动追剧并下载）。当用户要求订阅、收藏、追某部电影或电视剧时调用。

        Args:
            title(string): 影视作品名称，建议使用搜索结果中的完整名称
            year(string): 发行年份，用于区分同名作品，不确定时留空
            season(string): 季数，如 "2"；用户要求全部季时传 "所有"；电视剧有多季且用户未指定季时留空，本工具会返回可选季列表
            media_type(string): 媒体类型，取值为 "电影" 或 "电视剧"，不确定时留空
        """
        token = await self._get_valid_token()
        if not token:
            return "错误：MoviePilot 登录失败，请检查插件配置中的用户名、密码与基础地址。"

        title = (title or "").strip()
        if not title:
            return "请提供要订阅的影视名称。"

        candidates = self.remove_empty_keys(await self.search_media(token, title)) or []
        if not candidates:
            return f"未搜索到《{title}》，无法订阅，请先确认作品名称。"
        item = self._pick_best_media(candidates, title, year, media_type)
        if not item:
            return f"未找到与《{title}》匹配的作品，请先调用 mp_search_media 确认准确名称。"

        media_title = item.get("title") or title
        season_arg = str(season).strip()
        season_numbers = []

        # 仅 TMDB 来源的电视剧需要选择季
        if (item.get("source") or item.get("media_source")) == "themoviedb" and item.get("tmdb_id") and item.get("type") == "电视剧":
            seasons = await self._get_tmdb_seasons(token, item.get("tmdb_id"))
            if seasons is None:
                return f"查询《{media_title}》的季信息失败，暂无法订阅。"
            # 过滤掉第 0 季（特别篇），避免默认订阅或选季时错位
            season_numbers = [s.get("season_number") for s in seasons if s.get("season_number")]
            if len(season_numbers) > 1 and not season_arg:
                lines = [f"《{media_title}》共有 {len(season_numbers)} 季，请先向用户确认要订阅哪一季："]
                lines.extend(
                    f"{i}. 第{s.get('season_number', '?')}季 {s.get('name', '')}".rstrip()
                    for i, s in enumerate(seasons, 1)
                )
                lines.append("确认后再次调用本工具并传入 season（季数）；若用户要全部订阅，season 传“所有”。")
                return "\n".join(lines)

        if season_arg in ("所有", "全部", "all") and season_numbers:
            targets = season_numbers
        elif season_arg and season_numbers:
            if not any(str(n) == season_arg for n in season_numbers):
                return f"《{media_title}》不存在第 {season_arg} 季，可选季数: {', '.join(str(n) for n in season_numbers)}。"
            targets = [int(season_arg)]
        elif season_numbers:
            targets = [season_numbers[0]]
        else:
            targets = [None]

        results = []
        for season_number in targets:
            media_item = dict(item)
            display_name = f"《{media_title}》"
            if season_number is not None:
                media_item["season"] = season_number
                if item.get("type") == "电视剧":
                    display_name = f"《{media_title}》第{season_number}季"
            results.append(await self._subscribe_one(token, media_item, display_name))
        return "\n".join(results)

    @filter.llm_tool(name="mp_list_subscriptions")
    async def llm_list_subscriptions(self, event: AstrMessageEvent) -> str:
        """查看 MoviePilot 中当前的订阅列表与下载进度。用户询问已订阅了哪些影视、追剧进度如何时调用。"""
        token = await self._get_valid_token()
        if not token:
            return "错误：MoviePilot 登录失败，请检查插件配置中的用户名、密码与基础地址。"

        subscription_data = await self.get_subscription_data(token)
        if not subscription_data:
            return "获取订阅列表失败，或当前没有任何订阅。"
        return self.format_subscription_data(subscription_data)

    @filter.llm_tool(name="mp_search_subscription")
    async def llm_search_subscription(
        self, event: AstrMessageEvent, subscription_id: str = ""
    ) -> str:
        """立即让 MoviePilot 对订阅执行一次资源搜索（刷新、补种、重试下载）。用户要求刷新订阅或立即搜索资源时调用。

        Args:
            subscription_id(string): 订阅 ID；留空表示对所有订阅执行搜索
        """
        token = await self._get_valid_token()
        if not token:
            return "错误：MoviePilot 登录失败，请检查插件配置中的用户名、密码与基础地址。"

        sub_id = (subscription_id or "").strip()
        search_result = await self.search_subscription(token, sub_id)
        if not search_result:
            return "触发订阅搜索失败，请检查 MoviePilot 服务状态。"
        if search_result.get("success"):
            return f"✅ 已触发订阅搜索{f'（ID: {sub_id}）' if sub_id else ''}，资源搜索在 MoviePilot 后台执行。"
        return f"⚠️ 订阅搜索失败: {search_result.get('msg') or search_result.get('message') or '未知错误'}"

    @filter.llm_tool(name="mp_enable_notification")
    async def llm_enable_notification(self, event: AstrMessageEvent, categories: str) -> str:
        """在当前会话开启 MoviePilot 事件通知推送。当用户希望接收下载、整理入库、订阅等消息通知时调用。

        Args:
            categories(string): 通知类别，多个用 | 或逗号分隔。可选值: 资源下载、整理入库、订阅、媒体服务器、手动处理、插件、其他、站点、所有
        """
        return self._enable_notification_categories(
            str(event.unified_msg_origin), self._parse_categories(categories)
        )

    @filter.llm_tool(name="mp_disable_notification")
    async def llm_disable_notification(self, event: AstrMessageEvent, categories: str) -> str:
        """关闭当前会话的 MoviePilot 事件通知推送。当用户不想再接收下载、入库、订阅等通知时调用。

        Args:
            categories(string): 要关闭的通知类别，多个用 | 或逗号分隔。可选值: 资源下载、整理入库、订阅、媒体服务器、手动处理、插件、其他、站点、所有
        """
        return self._disable_notification_categories(
            str(event.unified_msg_origin), self._parse_categories(categories)
        )

    @filter.regex(r"^[\s\S]+")
    async def handle_tmdb_season_input(self, event: AstrMessageEvent):
        userid = str(getattr(event, "unified_msg_origin", None))
        if userid not in self.user_states:
            return
        state = self.user_states[userid]
        msg = event.message_str.strip()
        if state == "waiting_tmdb_season":
            if msg == "退出":
                self.logger.info(f"[TMDB多季订阅] 用户{userid} 退出流程", extra={"plugin": "mp"})
                self.user_states.pop(userid, None)
                self.pending_subscription.pop(userid, None)
                yield event.plain_result("已退出多季订阅流程。")
                return
            if msg == "所有":
                pending = self.pending_subscription.get(userid)
                if not pending:
                    self.user_states.pop(userid, None)
                    yield event.plain_result("⚠️ 状态已失效，请重新发起订阅。")
                    return
                seasons = pending["seasons"]
                media_item = dict(pending["media_item"])
                results = []
                for s in seasons:
                    season_number = s.get("season_number", 1)
                    media_item["season"] = season_number
                    media_title = media_item.get("title", "未知")
                    if not self.access_token:
                        self.access_token = await self.get_access_token(self.username, self.password, self.token_url)
                        if not self.access_token:
                            results.append(f"❌ `{media_title}` 第{season_number}季 Token获取失败。")
                            continue
                    transformed_data = self.transform_data(media_item)
                    response = await self.add_subscription(self.access_token, transformed_data)
                    if response and response.get("success") == True:
                        results.append(f"✅ `{media_title}` 第{season_number}季 订阅成功。")
                    else:
                        error_msg = (response.get("msg") or response.get("message") or "看日志。") if isinstance(response, dict) else "看日志。"
                        results.append(f"⚠️ 订阅 `{media_title}` 第{season_number}季 失败: {error_msg}")
                self.user_states.pop(userid, None)
                self.pending_subscription.pop(userid, None)
                yield event.plain_result("\n".join(results))
                return
            if msg.isdigit():
                idx = int(msg) - 1
                pending = self.pending_subscription.get(userid)
                if not pending:
                    self.user_states.pop(userid, None)
                    yield event.plain_result("⚠️ 状态已失效，请重新发起订阅。")
                    return
                seasons = pending["seasons"]
                media_item = dict(pending["media_item"])
                if 0 <= idx < len(seasons):
                    season_number = seasons[idx].get("season_number", 1)
                    media_item["season"] = season_number
                    media_title = media_item.get("title", "未知")
                    if not self.access_token:
                        self.access_token = await self.get_access_token(self.username, self.password, self.token_url)
                        if not self.access_token:
                            yield event.plain_result("⚠️ Token获取失败。")
                            return
                    transformed_data = self.transform_data(media_item)
                    response = await self.add_subscription(self.access_token, transformed_data)
                    if response and response.get("success") == True:
                        yield event.plain_result(f"✅ `{media_title}` 第{season_number}季 订阅成功。")
                    else:
                        error_msg = (response.get("msg") or response.get("message") or "看日志。") if isinstance(response, dict) else "看日志。"
                        yield event.plain_result(f"⚠️ 订阅 `{media_title}` 第{season_number}季 失败: {error_msg}")
                    self.user_states.pop(userid, None)
                    self.pending_subscription.pop(userid, None)
                    return
                else:
                    yield event.plain_result("⚠️ 序号无效，请重新输入或输入退出。")
                    return
            # 非数字输入直接忽略
            return

    # --- terminate, _ensure_token, API methods (保持 v1.3.5 的状态) ---
    async def terminate(self):
        """清理资源并终止插件"""
        self.logger.info("终止MediaSearchPlugin...", extra={"plugin": "mp"})
        if not self.http_forward_enabled:
            self.logger.info("HTTP转发功能未启用，跳过HTTP资源清理", extra={"plugin": "mp"})
            return
        # 停止HTTP服务器
        if hasattr(self, 'http_runner') and self.http_runner:
            self.logger.info("正在停止HTTP服务器...", extra={"plugin": "mp"})
            try:
                await self.http_runner.cleanup()
                self.logger.info("HTTP服务器已停止", extra={"plugin": "mp"})
            except Exception as e:
                self.logger.warning(f"HTTP服务器关闭异常: {e}", extra={"plugin": "mp"})
            self.http_runner = None
            self.http_site = None
            self.http_app = None
        # 取消消息处理任务
        if self.message_processor_task and not self.message_processor_task.done():
            self.logger.info("正在取消消息处理任务...", extra={"plugin": "mp"})
            self.message_processor_task.cancel()
            try:
                await self.message_processor_task
            except asyncio.CancelledError:
                self.logger.info("消息处理任务已取消", extra={"plugin": "mp"})
            except Exception as e:
                self.logger.error(f"取消任务时出错: {e}", extra={"plugin": "mp"})
        self.logger.info("MediaSearchPlugin已终止", extra={"plugin": "mp"})

    def _ensure_token(self) -> bool:
        """确保访问令牌有效，必要时获取新令牌"""
        if not self.access_token:
            self.logger.info("令牌缺失，获取新令牌", extra={"plugin": "mp"})
            return False
        return True
        
    async def get_access_token(self, username, password, token_url):
        """获取API访问令牌 (aiohttp)"""
        self._log_and_check_daily('info', f"请求令牌: {token_url}, 用户名: {username}", extra={"plugin": "mp"})
        if not token_url: 
            self._log_and_check_daily('error', "令牌URL未配置", extra={"plugin": "mp"})
            return None
        data = {"username": username, "password": password}
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        try:
            timeout = aiohttp.ClientTimeout(total=10)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.post(token_url, data=data, headers=headers) as response:
                    self._log_and_check_daily('debug', f"令牌请求响应状态: {response.status}", extra={"plugin": "mp"})
                    response.raise_for_status()
                    token_data = await response.json()
                    self._log_and_check_daily('debug', f"令牌响应内容: {token_data}", extra={"plugin": "mp"})
                    token = token_data.get("access_token")
                    if token: 
                        self._log_and_check_daily('info', "令牌获取成功", extra={"plugin": "mp"})
                        return token
                    else: 
                        self._log_and_check_daily('warning', f"响应中无令牌: {await response.text()}", extra={"plugin": "mp"})
                        return None
        except aiohttp.ClientError as e:
            self._log_and_check_daily('error', f"令牌请求错误: {e}", extra={"plugin": "mp"})
            return None
        except Exception as e:
            self._log_and_check_daily('exception', f"令牌获取未知错误: {e}", extra={"plugin": "mp"})
            return None
            
    async def search_media(self, access_token, title):
        """搜索媒体内容 (aiohttp)"""
        self._log_and_check_daily('info', f"搜索媒体: {title}", extra={"plugin": "mp"})
        if not self.base_url: 
            self._log_and_check_daily('warning', "base_url未配置", extra={"plugin": "mp"})
            return None
        search_url = f"{self.base_url}/api/v1/media/search"
        params = {'title': title, 'type': 'media', 'page': 1, 'count': self.max_results * 2}
        headers = {"accept": "application/json", "Authorization": f"Bearer {access_token}"}
        try:
            timeout = aiohttp.ClientTimeout(total=15)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(search_url, headers=headers, params=params) as response:
                    self._log_and_check_daily('debug', f"媒体搜索响应状态: {response.status}", extra={"plugin": "mp"})
                    if response.status == 200:
                        data = await response.json()
                        self._log_and_check_daily('debug', f"媒体搜索响应内容: {data}", extra={"plugin": "mp"})
                        # MoviePilot 返回 {"success":..., "data": [...]}，取出 data 字段
                        return data.get("data") if isinstance(data, dict) else data
                    elif response.status == 401 and self.token_refresh_count < 1:
                        self._log_and_check_daily('warning', "搜索需要更新令牌", extra={"plugin": "mp"})
                        self.token_refresh_count += 1
                        return None
                    else:
                        self._log_and_check_daily('error', f"搜索失败: {response.status} - {await response.text()}", extra={"plugin": "mp"})
                        return None
        except aiohttp.ClientError as e:
            self._log_and_check_daily('error', f"搜索请求错误: {e}", extra={"plugin": "mp"})
            return None
        except Exception as e:
            self._log_and_check_daily('exception', f"搜索未知错误: {e}", extra={"plugin": "mp"})
            return None
            
    async def get_subscription_data(self, access_token):
        """获取订阅数据 (aiohttp)"""
        self._log_and_check_daily('info', "获取订阅数据", extra={"plugin": "mp"})
        if not self.subscribe_url: 
            self._log_and_check_daily('warning', "subscribe_url未配置", extra={"plugin": "mp"})
            return None
        headers = {"accept": "application/json", "Authorization": f"Bearer {access_token}"}
        try:
            timeout = aiohttp.ClientTimeout(total=15)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(self.subscribe_url, headers=headers) as response:
                    self._log_and_check_daily('debug', f"订阅数据响应状态: {response.status}", extra={"plugin": "mp"})
                    if response.status == 200:
                        data = await response.json()
                        self._log_and_check_daily('debug', f"订阅数据响应内容: {data}", extra={"plugin": "mp"})
                        # MoviePilot 返回 {"success":..., "data": [...]}，取出 data 字段
                        return data.get("data") if isinstance(data, dict) else data
                    elif response.status == 401 and self.token_refresh_count < 1:
                        self._log_and_check_daily('warning', "获取订阅需要更新令牌", extra={"plugin": "mp"})
                        self.token_refresh_count += 1
                        return None
                    else:
                        self._log_and_check_daily('error', f"获取订阅失败: {response.status} - {await response.text()}", extra={"plugin": "mp"})
                        return None
        except aiohttp.ClientError as e:
            self._log_and_check_daily('error', f"获取订阅请求错误: {e}", extra={"plugin": "mp"})
            return None
        except Exception as e:
            self._log_and_check_daily('exception', f"获取订阅未知错误: {e}", extra={"plugin": "mp"})
            return None
            
    def remove_empty_keys(self, data):
        """递归移除空值键"""
        if isinstance(data, dict): 
            return {k: v_clean for k, v in data.items() if (v_clean := self.remove_empty_keys(v)) is not None} or None
        elif isinstance(data, list): 
            return [item_clean for item in data if (item_clean := self.remove_empty_keys(item)) is not None] or None
        elif isinstance(data, str): 
            return data if data.strip() else None
        elif data in (0, False): 
            return data
        else: 
            return data
            
    def format_search_results(self, data):
        """格式化搜索结果"""
        if not data: 
            return "没有找到匹配的媒体内容。"
            
        text = "【搜索结果】\n"
        count = 0
        
        for i, item in enumerate(data):
            item = self.remove_empty_keys(item) or {}
            if not item or count >= self.max_results:
                if count >= self.max_results: 
                    text += f"\n... (最多显示 {self.max_results} 条)"
                break
                
            title = item.get("title", "?")
            year = item.get("year")
            mtype = item.get("type", "?")
            src = item.get("source") or item.get("media_source", "?")
            detail_link = item.get("detail_link", "")
            
            year_str = f" ({year})" if year else ""
            text += f"{i+1}. {title}{year_str} [{mtype}]\n   来源: {src}\n"
            if detail_link:
                text += f"   地址: {detail_link}\n"
            
            count += 1
            
        return text.strip() if count > 0 else "没有找到有效的媒体内容。"
        
    def format_subscription_data(self, data):
        """格式化订阅数据"""
        if not data or not (cleaned := self.remove_empty_keys(data)): 
            return "当前无有效订阅数据。"
            
        tv = sorted([i for i in cleaned if i.get("type") == "电视剧"], key=lambda x: (x.get('name', ''), x.get('season', 0)))
        mv = sorted([i for i in cleaned if i.get("type") == "电影"], key=lambda x: (x.get('name', ''), x.get('year', '')))
        
        text = "💎 当前订阅详情 💎\n---------------------\n【📺 电视系列】\n"
        
        if tv:
            for i, s in enumerate(tv):
                n = s.get('name','?')
                seas = s.get('season','?')
                yr = s.get('year','')
                te = s.get('total_episode')
                le = s.get('lack_episode')
                sid = s.get('id','?')
                
                yr_s = f"({yr})" if yr else ""
                text += f"{i+1}. 《{n}》 第 {seas} 季 {yr_s}\n"
                
                if isinstance(te, int) and isinstance(le, int) and te > 0:
                    p = te-le
                    pct = int((p/te)*100)
                    text += f"   进度: {p}/{te} ({pct}%) | ID: {sid}\n"
                elif isinstance(te, int) and te == 0:
                    text += f"   进度: 未开播 | ID: {sid}\n"
                else:
                    text += f"   进度: 信息不详 | ID: {sid}\n"
        else:
            text += "  (无)\n"
            
        text += "\n【🎬 电影】\n"
        
        if mv:
            for i, m in enumerate(mv):
                n = m.get('name','?')
                yr = m.get('year','')
                mid = m.get('id','?')
                yr_s = f"({yr})" if yr else ""
                text += f"{i+1}. 《{n}》 {yr_s} | ID: {mid}\n"
        else:
            text += "  (无)\n"
            
        return text.strip()
        
    def transform_data(self, data):
        """转换媒体数据为订阅格式"""
        cleaned = self.remove_empty_keys(data) or {}
        # 处理季数
        season = cleaned.get("season")
        if season is None:
            s_years = cleaned.get("season_years")
            num_s = cleaned.get("number_of_seasons")
            if isinstance(s_years, dict) and s_years:
                try:
                    nums = [int(s) for s in s_years if s.isdigit()]
                    season = max(nums) if nums else 1
                except:
                    season = 1
            elif isinstance(num_s, int) and num_s > 0:
                season = num_s
            else:
                season = 1
        else:
            try:
                season = int(season)
            except:
                season = 1
        t = {
            k: cleaned.get(m, d) for k, m, d in [
                ("n", "title", ""),
                ("y", "year", ""),
                ("t", "type", ""),
                ("tm", "tmdb_id", 0),
                ("d", "douban_id", ""),
                ("b", "bangumi_id", 0),
                ("p", "poster_path", ""),
                ("bp", "backdrop_path", ""),
                ("v", "vote_average", 0.0),
                ("desc", "overview", ""),
                ("lu", "release_date", ""),
                ("dt", "release_date", "")
            ]
        }
        tx = {
            "name": t['n'],
            "year": str(t['y'] or ""),
            "type": t['t'],
            "keyword": "",
            "tmdbid": int(t['tm'] or 0),
            "doubanid": str(t['d'] or ""),
            "bangumiid": int(t['b'] or 0),
            "mediaid": "",
            "season": season,
            "poster": t['p'],
            "backdrop": t['bp'],
            "vote": float(t['v'] or 0.0),
            "description": t['desc'],
            "filter": "",
            "include": "",
            "exclude": "",
            "quality": "",
            "resolution": "",
            "effect": "",
            "total_episode": 0,
            "start_episode": 0,
            "lack_episode": 0,
            "note": "",
            "state": "",
            "last_update": t['lu'],
            "username": self.username,
            "sites": [],
            "downloader": "",
            "best_version": 0,
            "current_priority": 0,
            "save_path": "",
            "search_imdbid": 0,
            "date": t['dt'],
            "custom_words": "",
            "media_category": "",
            "filter_groups": []
        }
        tx.pop('id', None)
        return tx
        
    async def add_subscription(self, access_token, sub_data):
        """添加订阅 (aiohttp)"""
        self._log_and_check_daily('info', f"添加订阅: {sub_data}", extra={"plugin": "mp"})
        if not self.subscribe_url:
            self._log_and_check_daily('warning', "订阅URL未配置", extra={"plugin": "mp"})
            return {"success": False, "msg": "订阅URL未配置"}
        headers = {"accept": "application/json", "Authorization": f"Bearer {access_token}"}
        try:
            timeout = aiohttp.ClientTimeout(total=20)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.post(self.subscribe_url, headers=headers, json=sub_data) as response:
                    self._log_and_check_daily('debug', f"添加订阅响应状态: {response.status}", extra={"plugin": "mp"})
                    try:
                        data = await response.json()
                        self._log_and_check_daily('debug', f"添加订阅响应内容: {data}", extra={"plugin": "mp"})
                        return data
                    except Exception:
                        self._log_and_check_daily('error', f"添加订阅响应JSON解析错误: {await response.text()}", extra={"plugin": "mp"})
                        return {"success": False, "msg": "服务器响应格式错误"}
        except aiohttp.ClientResponseError as e:
            self._log_and_check_daily('error', f"添加订阅HTTP错误: {e.status} - {e.message}", extra={"plugin": "mp"})
            return {"success": False, "msg": f"HTTP {e.status}"}
        except aiohttp.ClientError as e:
            self._log_and_check_daily('error', f"添加订阅请求错误: {e}", extra={"plugin": "mp"})
            return {"success": False, "msg": "网络错误"}
        except Exception as e:
            self._log_and_check_daily('exception', f"添加订阅未知错误: {e}", extra={"plugin": "mp"})
            return {"success": False, "msg": "内部错误"}
            
    async def search_subscription(self, access_token, sub_id=""):
        """搜索订阅 (aiohttp)"""
        self._log_and_check_daily('info', f"搜索订阅: {sub_id}", extra={"plugin": "mp"})
        if not self.base_url:
            self._log_and_check_daily('warning', "base_url未配置", extra={"plugin": "mp"})
            return None
        headers = {"accept": "application/json", "Authorization": f"Bearer {access_token}"}
        url = f"{self.base_url}/api/v1/subscribe/search/{sub_id}" if sub_id else f"{self.base_url}/api/v1/subscribe/search"
        try:
            timeout = aiohttp.ClientTimeout(total=15)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.post(url, headers=headers) as response:
                    self._log_and_check_daily('debug', f"搜索订阅响应状态: {response.status}", extra={"plugin": "mp"})
                    if response.status == 200:
                        data = await response.json()
                        self._log_and_check_daily('debug', f"搜索订阅响应内容: {data}", extra={"plugin": "mp"})
                        return data
                    elif response.status == 401 and self.token_refresh_count < 1:
                        self._log_and_check_daily('warning', "搜索订阅需要更新令牌", extra={"plugin": "mp"})
                        self.token_refresh_count += 1
                        return None
                    elif response.status == 404:
                        self._log_and_check_daily('warning', f"搜索订阅404错误: ID '{sub_id}'", extra={"plugin": "mp"})
                        return {"success": False, "msg": f"ID {sub_id} 未找到"} if sub_id else {"success": True, "data": {"list": []}}
                    else:
                        self._log_and_check_daily('error', f"搜索订阅失败: {response.status} - {await response.text()}", extra={"plugin": "mp"})
                        return None
        except aiohttp.ClientError as e:
            self._log_and_check_daily('error', f"搜索订阅请求错误: {e}", extra={"plugin": "mp"})
            return None
        except Exception as e:
            self._log_and_check_daily('exception', f"搜索订阅未知错误: {e}", extra={"plugin": "mp"})
            return None

# 允许的通知类别
ALLOWED_CATEGORIES = {
    "资源下载", "整理入库", "订阅", "媒体服务器",
    "手动处理", "插件", "其他", "站点", "所有"
}

# HTTP 服务器基础配置
LISTEN_ADDRESS = '0.0.0.0'

# 获取本机 IP 地址
def get_local_ip():
    """安全地获取本机的主要IPv4地址，不进行外部网络连接"""
    try:
        hostname = socket.gethostname()
        ip = socket.gethostbyname(hostname)
        if ip and not ip.startswith("127."):
            return ip
        # 进一步遍历所有网卡
        for info in socket.getaddrinfo(hostname, None):
            family, _, _, _, sockaddr = info
            if family == socket.AF_INET:
                candidate = sockaddr[0]
                if not candidate.startswith("127."):
                    return candidate
    except Exception:
        pass
    return "127.0.0.1"
