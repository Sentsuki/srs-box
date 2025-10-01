"""
配置管理工具
提供统一的配置文件加载和访问接口
"""

import json
import os
from typing import Dict, List, Any, Optional


class ConfigManager:
    """配置管理器，负责加载和验证配置文件"""
    
    def __init__(self, config_path: str = "config.json"):
        """
        初始化配置管理器
        
        Args:
            config_path: 配置文件路径，默认为 config.json
        """
        self.config_path = config_path
        self._config: Optional[Dict[str, Any]] = None
        
    def load_config(self) -> Dict[str, Any]:
        """
        加载配置文件
        
        Returns:
            配置字典
            
        Raises:
            FileNotFoundError: 配置文件不存在
            json.JSONDecodeError: 配置文件格式错误
            ValueError: 配置文件内容验证失败
        """
        if not os.path.exists(self.config_path):
            raise FileNotFoundError(f"配置文件不存在: {self.config_path}")
            
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                self._config = json.load(f)
        except json.JSONDecodeError as e:
            raise json.JSONDecodeError(
                f"配置文件格式错误: {e.msg}",
                e.doc,
                e.pos
            )
            
        # 验证配置文件结构
        self._validate_config()
        return self._config
    
    def _validate_config(self) -> None:
        """
        验证配置文件结构
        
        Raises:
            ValueError: 配置文件结构不正确
        """
        if not isinstance(self._config, dict):
            raise ValueError("配置文件根节点必须是对象")
            
        # 检查必需的字段
        required_fields = ["rulesets", "sing_box", "version"]
        for field in required_fields:
            if field not in self._config:
                raise ValueError(f"配置文件缺少必需字段: {field}")
        
        # 验证 rulesets 字段
        rulesets = self._config.get("rulesets")
        if not isinstance(rulesets, dict):
            raise ValueError("rulesets 字段必须是对象")
            
        for name, urls in rulesets.items():
            if not isinstance(name, str):
                raise ValueError(f"规则集名称必须是字符串: {name}")
            if not isinstance(urls, list):
                raise ValueError(f"规则集 {name} 的 URL 列表必须是数组")
            if not urls:
                raise ValueError(f"规则集 {name} 的 URL 列表不能为空")
            for url in urls:
                if not isinstance(url, str):
                    raise ValueError(f"规则集 {name} 中的 URL 必须是字符串: {url}")
        
        # 验证 sing_box 字段
        sing_box = self._config.get("sing_box")
        if not isinstance(sing_box, dict):
            raise ValueError("sing_box 字段必须是对象")
            
        required_sing_box_fields = ["version", "platform"]
        for field in required_sing_box_fields:
            if field not in sing_box:
                raise ValueError(f"sing_box 配置缺少必需字段: {field}")
            if not isinstance(sing_box[field], str):
                raise ValueError(f"sing_box.{field} 必须是字符串")
        
        # 验证 version 字段
        version = self._config.get("version")
        if not isinstance(version, int):
            raise ValueError("version 字段必须是整数")
        
        # 验证可选的 logging 字段
        if "logging" in self._config:
            logging_config = self._config["logging"]
            if not isinstance(logging_config, dict):
                raise ValueError("logging 字段必须是对象")
        
        # 验证可选的 output 字段
        if "output" in self._config:
            output_config = self._config["output"]
            if not isinstance(output_config, dict):
                raise ValueError("output 字段必须是对象")
    
    def get_rulesets(self) -> Dict[str, List[str]]:
        """
        获取规则集配置
        
        Returns:
            规则集字典，键为规则集名称，值为 URL 列表
        """
        if self._config is None:
            self.load_config()
        return self._config["rulesets"]
    
    def get_sing_box_config(self) -> Dict[str, str]:
        """
        获取 sing-box 配置
        
        Returns:
            sing-box 配置字典
        """
        if self._config is None:
            self.load_config()
        return self._config["sing_box"]
    
    def get_version(self) -> int:
        """
        获取配置版本号
        
        Returns:
            配置版本号
        """
        if self._config is None:
            self.load_config()
        return self._config["version"]
    
    def get_ruleset_urls(self, ruleset_name: str) -> List[str]:
        """
        获取指定规则集的 URL 列表
        
        Args:
            ruleset_name: 规则集名称
            
        Returns:
            URL 列表
            
        Raises:
            KeyError: 规则集不存在
        """
        rulesets = self.get_rulesets()
        if ruleset_name not in rulesets:
            raise KeyError(f"规则集不存在: {ruleset_name}")
        return rulesets[ruleset_name]
    
    def get_ruleset_names(self) -> List[str]:
        """
        获取所有规则集名称
        
        Returns:
            规则集名称列表
        """
        return list(self.get_rulesets().keys())
    
    def get_sing_box_version(self) -> str:
        """
        获取 sing-box 版本
        
        Returns:
            sing-box 版本字符串
        """
        return self.get_sing_box_config()["version"]
    
    def get_sing_box_platform(self) -> str:
        """
        获取 sing-box 平台
        
        Returns:
            sing-box 平台字符串
        """
        return self.get_sing_box_config()["platform"]
    
    def get_logging_config(self) -> Dict[str, Any]:
        """
        获取日志配置
        
        Returns:
            日志配置字典，包含默认值
        """
        if self._config is None:
            self.load_config()
        
        # 默认日志配置
        default_logging = {
            "level": "INFO",
            "enable_color": True,
            "show_progress": True
        }
        
        # 合并用户配置
        user_logging = self._config.get("logging", {})
        default_logging.update(user_logging)
        
        return default_logging
    
    def get_output_config(self) -> Dict[str, str]:
        """
        获取输出配置
        
        Returns:
            输出配置字典，包含默认值
        """
        if self._config is None:
            self.load_config()
        
        # 默认输出配置
        default_output = {
            "json_dir": "output/json",
            "srs_dir": "output/srs"
        }
        
        # 合并用户配置
        user_output = self._config.get("output", {})
        default_output.update(user_output)
        
        return default_output
    
    def get_convert_config(self) -> Dict[str, List[str]]:
        """
        获取convert配置
        
        Returns:
            convert配置字典，键为规则集名称，值为 URL 列表
        """
        if self._config is None:
            self.load_config()
        return self._config.get("convert", {})