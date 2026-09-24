"""异常层级。

所有对用户可见的失败都应是 SrsBoxError 的子类，携带可直接展示的中文消息；
其余异常一律视为 bug，不做兜底捕获。
"""


class SrsBoxError(Exception):
    """所有可预期失败的基类。"""


class ConfigError(SrsBoxError):
    """配置文件结构或取值有误。"""


class ParseError(SrsBoxError):
    """源内容无法按声明的格式解析。"""


class InvalidValue(SrsBoxError):
    """单个规则值非法（不合法的 CIDR、端口、域名等）。"""


class CompileError(SrsBoxError):
    """sing-box 获取或编译失败。"""
