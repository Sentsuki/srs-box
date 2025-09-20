"""
文件操作工具
提供目录创建、文件读写、文件合并等通用功能
"""

import os
import json
import shutil
from pathlib import Path
from typing import List, Dict, Any, Optional, Union


class FileUtils:
    """文件操作工具类"""
    
    @staticmethod
    def ensure_dir(path: Union[str, Path]) -> None:
        """
        确保目录存在，如果不存在则创建
        
        Args:
            path: 目录路径
        """
        path = Path(path)
        path.mkdir(parents=True, exist_ok=True)
    
    @staticmethod
    def read_text_file(path: Union[str, Path], encoding: str = 'utf-8') -> List[str]:
        """
        读取文本文件，返回行列表
        
        Args:
            path: 文件路径
            encoding: 文件编码，默认为 utf-8
            
        Returns:
            文件行列表，已去除换行符
            
        Raises:
            FileNotFoundError: 文件不存在
            UnicodeDecodeError: 文件编码错误
        """
        path = Path(path)
        if not path.exists():
            raise FileNotFoundError(f"文件不存在: {path}")
        
        try:
            with open(path, 'r', encoding=encoding) as f:
                return [line.rstrip('\n\r') for line in f]
        except UnicodeDecodeError as e:
            raise UnicodeDecodeError(
                e.encoding,
                e.object,
                e.start,
                e.end,
                f"文件编码错误: {path}"
            )
    
    @staticmethod
    def write_text_file(path: Union[str, Path], lines: List[str], encoding: str = 'utf-8') -> None:
        """
        写入文本文件
        
        Args:
            path: 文件路径
            lines: 要写入的行列表
            encoding: 文件编码，默认为 utf-8
        """
        path = Path(path)
        FileUtils.ensure_dir(path.parent)
        
        with open(path, 'w', encoding=encoding) as f:
            for line in lines:
                f.write(line + '\n')
    
    @staticmethod
    def read_json_file(path: Union[str, Path], encoding: str = 'utf-8') -> Dict[str, Any]:
        """
        读取 JSON 文件
        
        Args:
            path: 文件路径
            encoding: 文件编码，默认为 utf-8
            
        Returns:
            JSON 数据字典
            
        Raises:
            FileNotFoundError: 文件不存在
            json.JSONDecodeError: JSON 格式错误
        """
        path = Path(path)
        if not path.exists():
            raise FileNotFoundError(f"文件不存在: {path}")
        
        try:
            with open(path, 'r', encoding=encoding) as f:
                return json.load(f)
        except json.JSONDecodeError as e:
            raise json.JSONDecodeError(
                f"JSON 格式错误 ({path}): {e.msg}",
                e.doc,
                e.pos
            )
    
    @staticmethod
    def write_json_file(path: Union[str, Path], data: Dict[str, Any], 
                       encoding: str = 'utf-8', indent: int = 2) -> None:
        """
        写入 JSON 文件
        
        Args:
            path: 文件路径
            data: 要写入的数据
            encoding: 文件编码，默认为 utf-8
            indent: JSON 缩进，默认为 2
        """
        path = Path(path)
        FileUtils.ensure_dir(path.parent)
        
        with open(path, 'w', encoding=encoding) as f:
            json.dump(data, f, ensure_ascii=False, indent=indent)
    
    @staticmethod
    def merge_text_files(file_paths: List[Union[str, Path]], 
                        output_path: Union[str, Path],
                        remove_duplicates: bool = True,
                        remove_empty_lines: bool = True) -> int:
        """
        合并多个文本文件
        
        Args:
            file_paths: 要合并的文件路径列表
            output_path: 输出文件路径
            remove_duplicates: 是否去除重复行，默认为 True
            remove_empty_lines: 是否去除空行，默认为 True
            
        Returns:
            合并后的总行数
        """
        all_lines = []
        seen_lines = set() if remove_duplicates else None
        
        for file_path in file_paths:
            try:
                lines = FileUtils.read_text_file(file_path)
                for line in lines:
                    # 去除空行
                    if remove_empty_lines and not line.strip():
                        continue
                    
                    # 去除重复行
                    if remove_duplicates:
                        if line in seen_lines:
                            continue
                        seen_lines.add(line)
                    
                    all_lines.append(line)
            except FileNotFoundError:
                # 跳过不存在的文件
                continue
        
        # 写入合并后的文件
        FileUtils.write_text_file(output_path, all_lines)
        return len(all_lines)
    
    @staticmethod
    def merge_json_files(file_paths: List[Union[str, Path]], 
                        output_path: Union[str, Path],
                        merge_strategy: str = 'combine') -> Dict[str, Any]:
        """
        合并多个 JSON 文件
        
        Args:
            file_paths: 要合并的文件路径列表
            output_path: 输出文件路径
            merge_strategy: 合并策略，'combine' 或 'override'
            
        Returns:
            合并后的 JSON 数据
        """
        if merge_strategy == 'combine':
            # 合并策略：将所有 JSON 的内容合并到一个对象中
            merged_data = {}
            for file_path in file_paths:
                try:
                    data = FileUtils.read_json_file(file_path)
                    if isinstance(data, dict):
                        merged_data.update(data)
                except FileNotFoundError:
                    continue
        else:
            # 覆盖策略：后面的文件覆盖前面的文件
            merged_data = {}
            for file_path in file_paths:
                try:
                    data = FileUtils.read_json_file(file_path)
                    if isinstance(data, dict):
                        merged_data = {**merged_data, **data}
                except FileNotFoundError:
                    continue
        
        # 写入合并后的文件
        FileUtils.write_json_file(output_path, merged_data)
        return merged_data
    
    @staticmethod
    def cleanup_temp_files(temp_dir: Union[str, Path], 
                          keep_patterns: Optional[List[str]] = None) -> int:
        """
        清理临时文件
        
        Args:
            temp_dir: 临时目录路径
            keep_patterns: 要保留的文件模式列表（glob 模式）
            
        Returns:
            删除的文件数量
        """
        temp_dir = Path(temp_dir)
        if not temp_dir.exists():
            return 0
        
        deleted_count = 0
        keep_patterns = keep_patterns or []
        
        for item in temp_dir.iterdir():
            should_keep = False
            
            # 检查是否匹配保留模式
            for pattern in keep_patterns:
                if item.match(pattern):
                    should_keep = True
                    break
            
            if should_keep:
                continue
            
            try:
                if item.is_file():
                    item.unlink()
                    deleted_count += 1
                elif item.is_dir():
                    shutil.rmtree(item)
                    deleted_count += 1
            except (OSError, PermissionError):
                # 忽略删除失败的文件
                continue
        
        return deleted_count
    
    @staticmethod
    def get_file_size(path: Union[str, Path]) -> int:
        """
        获取文件大小（字节）
        
        Args:
            path: 文件路径
            
        Returns:
            文件大小（字节）
            
        Raises:
            FileNotFoundError: 文件不存在
        """
        path = Path(path)
        if not path.exists():
            raise FileNotFoundError(f"文件不存在: {path}")
        return path.stat().st_size
    
    @staticmethod
    def format_file_size(size_bytes: int) -> str:
        """
        格式化文件大小为人类可读格式
        
        Args:
            size_bytes: 文件大小（字节）
            
        Returns:
            格式化后的文件大小字符串
        """
        if size_bytes == 0:
            return "0 B"
        
        size_names = ["B", "KB", "MB", "GB", "TB"]
        i = 0
        size = float(size_bytes)
        
        while size >= 1024.0 and i < len(size_names) - 1:
            size /= 1024.0
            i += 1
        
        return f"{size:.1f} {size_names[i]}"
    
    @staticmethod
    def copy_file(src: Union[str, Path], dst: Union[str, Path]) -> None:
        """
        复制文件
        
        Args:
            src: 源文件路径
            dst: 目标文件路径
        """
        src = Path(src)
        dst = Path(dst)
        
        if not src.exists():
            raise FileNotFoundError(f"源文件不存在: {src}")
        
        FileUtils.ensure_dir(dst.parent)
        shutil.copy2(src, dst)
    
    @staticmethod
    def move_file(src: Union[str, Path], dst: Union[str, Path]) -> None:
        """
        移动文件
        
        Args:
            src: 源文件路径
            dst: 目标文件路径
        """
        src = Path(src)
        dst = Path(dst)
        
        if not src.exists():
            raise FileNotFoundError(f"源文件不存在: {src}")
        
        FileUtils.ensure_dir(dst.parent)
        shutil.move(str(src), str(dst))
    
    @staticmethod
    def file_exists(path: Union[str, Path]) -> bool:
        """
        检查文件是否存在
        
        Args:
            path: 文件路径
            
        Returns:
            文件是否存在
        """
        return Path(path).exists()
    
    @staticmethod
    def list_files(directory: Union[str, Path], 
                  pattern: str = "*",
                  recursive: bool = False) -> List[Path]:
        """
        列出目录中的文件
        
        Args:
            directory: 目录路径
            pattern: 文件模式（glob 格式），默认为 "*"
            recursive: 是否递归搜索，默认为 False
            
        Returns:
            文件路径列表
        """
        directory = Path(directory)
        if not directory.exists():
            return []
        
        if recursive:
            return list(directory.rglob(pattern))
        else:
            return list(directory.glob(pattern))