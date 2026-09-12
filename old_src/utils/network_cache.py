"""
网络缓存管理
负责缓存文件的读写、有效性检查和清理
"""

import hashlib
import shutil
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Optional

from .file_utils import FileUtils


class NetworkCache:
    """网络请求缓存管理器"""

    def __init__(self, cache_dir: str = "temp/cache", cache_ttl_hours: int = 24):
        """
        初始化缓存管理器

        Args:
            cache_dir: 缓存目录，默认为 temp/cache
            cache_ttl_hours: 缓存有效期（小时），默认为 24
        """
        self.cache_dir = Path(cache_dir)
        self.cache_ttl = timedelta(hours=cache_ttl_hours)
        FileUtils.ensure_dir(self.cache_dir)

    def get_cache_path(self, url: str) -> Path:
        """
        获取 URL 对应的缓存文件路径

        Args:
            url: URL 字符串

        Returns:
            缓存文件路径
        """
        url_hash = hashlib.md5(url.encode("utf-8")).hexdigest()
        return self.cache_dir / f"{url_hash}.cache"

    def is_valid(self, cache_path: Path) -> bool:
        """
        检查缓存文件是否有效（存在、非空、未过期）

        Args:
            cache_path: 缓存文件路径

        Returns:
            缓存是否有效
        """
        if not cache_path.exists() or cache_path.stat().st_size == 0:
            return False
        file_time = datetime.fromtimestamp(cache_path.stat().st_mtime)
        return datetime.now() - file_time < self.cache_ttl

    def copy_to(self, url: str, destination: Path) -> bool:
        """
        将缓存文件复制到目标路径

        Args:
            url: 原始 URL
            destination: 目标路径

        Returns:
            是否成功复制
        """
        cache_path = self.get_cache_path(url)
        if not self.is_valid(cache_path):
            return False
        try:
            FileUtils.ensure_dir(destination.parent)
            shutil.copy2(cache_path, destination)
            return True
        except OSError:
            return False

    def save_from(self, url: str, source: Path) -> None:
        """
        将下载完成的文件保存到缓存

        Args:
            url: 原始 URL
            source: 已下载的文件路径
        """
        if not source.exists():
            return
        cache_path = self.get_cache_path(url)
        try:
            shutil.copy2(source, cache_path)
        except OSError:
            pass  # 缓存保存失败不影响主要功能

    def clear(self, older_than_hours: Optional[int] = None) -> int:
        """
        清理缓存文件

        Args:
            older_than_hours: 清理多少小时前的缓存，None 表示清理全部

        Returns:
            清理的文件数量
        """
        if not self.cache_dir.exists():
            return 0

        cleared = 0
        cutoff_time = (
            datetime.now() - timedelta(hours=older_than_hours)
            if older_than_hours is not None
            else None
        )

        for cache_file in self.cache_dir.glob("*.cache"):
            try:
                if cutoff_time is None:
                    cache_file.unlink()
                    cleared += 1
                else:
                    file_time = datetime.fromtimestamp(cache_file.stat().st_mtime)
                    if file_time < cutoff_time:
                        cache_file.unlink()
                        cleared += 1
            except OSError:
                continue

        return cleared

    def get_info(self) -> Dict[str, Any]:
        """
        获取缓存统计信息

        Returns:
            缓存信息字典
        """
        if not self.cache_dir.exists():
            return {
                "cache_dir": str(self.cache_dir),
                "total_files": 0,
                "total_size_mb": 0.0,
                "oldest_file": None,
                "newest_file": None,
                "ttl_hours": self.cache_ttl.total_seconds() / 3600,
            }

        cache_files = [f for f in self.cache_dir.glob("*.cache") if f.exists()]
        total_size = sum(f.stat().st_size for f in cache_files)

        oldest_time = newest_time = None
        if cache_files:
            file_times = [f.stat().st_mtime for f in cache_files]
            oldest_time = datetime.fromtimestamp(min(file_times))
            newest_time = datetime.fromtimestamp(max(file_times))

        return {
            "cache_dir": str(self.cache_dir),
            "total_files": len(cache_files),
            "total_size_mb": total_size / (1024 * 1024),
            "oldest_file": oldest_time.isoformat() if oldest_time else None,
            "newest_file": newest_time.isoformat() if newest_time else None,
            "ttl_hours": self.cache_ttl.total_seconds() / 3600,
        }
