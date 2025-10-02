"""
统一日志系统
提供统一的日志格式和级别控制，支持彩色输出和进度显示
"""

import sys
from datetime import datetime
from enum import Enum


class LogLevel(Enum):
    """日志级别枚举"""

    DEBUG = "DEBUG"
    INFO = "INFO"
    SUCCESS = "SUCCESS"
    WARNING = "WARNING"
    ERROR = "ERROR"

    @classmethod
    def from_string(cls, level_str: str) -> "LogLevel":
        """从字符串创建日志级别"""
        level_str = level_str.upper()
        for level in cls:
            if level.value == level_str:
                return level
        return cls.INFO  # 默认返回INFO级别


class Logger:
    """统一日志记录器"""

    # ANSI 颜色代码
    COLORS = {
        LogLevel.DEBUG: "\033[37m",  # 白色
        LogLevel.INFO: "\033[36m",  # 青色
        LogLevel.SUCCESS: "\033[32m",  # 绿色
        LogLevel.WARNING: "\033[33m",  # 黄色
        LogLevel.ERROR: "\033[31m",  # 红色
    }

    # 日志级别对应的图标
    ICONS = {
        LogLevel.DEBUG: "🔍",
        LogLevel.INFO: "ℹ️",
        LogLevel.SUCCESS: "✅",
        LogLevel.WARNING: "⚠️",
        LogLevel.ERROR: "❌",
    }

    # 日志级别优先级
    LEVEL_PRIORITY = {
        LogLevel.DEBUG: 0,
        LogLevel.INFO: 1,
        LogLevel.SUCCESS: 2,
        LogLevel.WARNING: 3,
        LogLevel.ERROR: 4,
    }

    RESET = "\033[0m"  # 重置颜色

    def __init__(
        self,
        enable_color: bool = True,
        log_level: LogLevel = LogLevel.INFO,
        show_progress: bool = True,
    ):
        """
        初始化日志记录器

        Args:
            enable_color: 是否启用彩色输出，默认为 True
            log_level: 日志级别，默认为 INFO
            show_progress: 是否显示进度条，默认为 True
        """
        self.enable_color = enable_color and sys.stdout.isatty()
        self.log_level = log_level
        self.show_progress = show_progress

    def _format_message(self, level: LogLevel, message: str, icon: bool = True) -> str:
        """
        格式化日志消息

        Args:
            level: 日志级别
            message: 消息内容
            icon: 是否显示图标

        Returns:
            格式化后的消息
        """
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # 构建基础消息
        if icon:
            icon_str = self.ICONS.get(level, "")
            base_message = f"[{timestamp}] [{level.value}] {icon_str} {message}"
        else:
            base_message = f"[{timestamp}] [{level.value}] {message}"

        # 添加颜色
        if self.enable_color:
            color = self.COLORS.get(level, "")
            return f"{color}{base_message}{self.RESET}"
        else:
            return base_message

    def _should_log(self, level: LogLevel) -> bool:
        """
        检查是否应该输出该级别的日志

        Args:
            level: 日志级别

        Returns:
            是否应该输出
        """
        return self.LEVEL_PRIORITY[level] >= self.LEVEL_PRIORITY[self.log_level]

    def _print(
        self, level: LogLevel, message: str, icon: bool = True, file=None
    ) -> None:
        """
        打印日志消息

        Args:
            level: 日志级别
            message: 消息内容
            icon: 是否显示图标
            file: 输出文件，默认为 stdout（ERROR 级别默认为 stderr）
        """
        if not self._should_log(level):
            return

        formatted_message = self._format_message(level, message, icon)

        if file is None:
            file = sys.stderr if level == LogLevel.ERROR else sys.stdout

        print(formatted_message, file=file)
        file.flush()

    def debug(self, message: str) -> None:
        """
        输出调试级别日志

        Args:
            message: 消息内容
        """
        self._print(LogLevel.DEBUG, message)

    def info(self, message: str) -> None:
        """
        输出信息级别日志

        Args:
            message: 消息内容
        """
        self._print(LogLevel.INFO, message)

    def success(self, message: str) -> None:
        """
        输出成功级别日志

        Args:
            message: 消息内容
        """
        self._print(LogLevel.SUCCESS, message)

    def warning(self, message: str) -> None:
        """
        输出警告级别日志

        Args:
            message: 消息内容
        """
        self._print(LogLevel.WARNING, message)

    def error(self, message: str) -> None:
        """
        输出错误级别日志

        Args:
            message: 消息内容
        """
        self._print(LogLevel.ERROR, message)

    def progress(self, current: int, total: int, message: str = "") -> None:
        """
        显示进度信息

        Args:
            current: 当前进度
            total: 总数
            message: 附加消息
        """
        if not self.show_progress or total <= 0:
            return

        percentage = min(100, max(0, (current * 100) // total))

        # 构建进度条
        bar_length = 20
        filled_length = (percentage * bar_length) // 100
        bar = "█" * filled_length + "░" * (bar_length - filled_length)

        # 构建进度消息
        progress_text = f"进度: [{bar}] {percentage}% ({current}/{total})"
        if message:
            progress_text += f" - {message}"

        # 使用 \r 实现同行更新
        if self.enable_color:
            color = self.COLORS[LogLevel.INFO]
            formatted = f"\r{color}{progress_text}{self.RESET}"
        else:
            formatted = f"\r{progress_text}"

        print(formatted, end="", flush=True)

        # 如果完成，换行
        if current >= total:
            print()

    def set_level(self, level: LogLevel) -> None:
        """
        设置日志级别

        Args:
            level: 新的日志级别
        """
        self.log_level = level

    def set_color_enabled(self, enabled: bool) -> None:
        """
        设置是否启用彩色输出

        Args:
            enabled: 是否启用
        """
        self.enable_color = enabled and sys.stdout.isatty()

    def set_progress_enabled(self, enabled: bool) -> None:
        """
        设置是否显示进度条

        Args:
            enabled: 是否显示
        """
        self.show_progress = enabled

    def step(self, step_name: str, current: int, total: int) -> None:
        """
        显示步骤进度

        Args:
            step_name: 步骤名称
            current: 当前步骤
            total: 总步骤数
        """
        self.info(f"🚀 {step_name} ({current}/{total})")

    def separator(self, title: str = "") -> None:
        """
        打印分隔线

        Args:
            title: 分隔线标题
        """
        line = "=" * 50
        if title:
            title_line = f" {title} "
            padding = (len(line) - len(title_line)) // 2
            line = (
                "=" * padding
                + title_line
                + "=" * (len(line) - padding - len(title_line))
            )

        self.info(line)

    def header(self, title: str) -> None:
        """
        打印标题头部

        Args:
            title: 标题内容
        """
        self.separator()
        self.info(f"🌏 {title}")
        self.separator()


# 创建全局日志实例
logger = Logger()
