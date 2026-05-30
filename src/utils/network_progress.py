"""
下载进度与结果数据类
"""

import threading
import time
from typing import Dict, Optional, Tuple


class DownloadResult:
    """下载结果类"""

    def __init__(
        self,
        url: str,
        success: bool,
        file_path: Optional[str] = None,
        error: Optional[str] = None,
        size: int = 0,
        duration: float = 0.0,
    ):
        self.url = url
        self.success = success
        self.file_path = file_path
        self.error = error
        self.size = size
        self.duration = duration

    @property
    def speed_mbps(self) -> float:
        """计算下载速度（MB/s）"""
        if self.duration > 0 and self.size > 0:
            return (self.size / (1024 * 1024)) / self.duration
        return 0.0


class DownloadProgress:
    """下载进度跟踪类"""

    def __init__(self, total_files: int):
        self.total_files = total_files
        self.completed_files = 0
        self.start_time = time.time()
        self.lock = threading.Lock()
        self.file_progresses: Dict[str, Tuple[int, int]] = {}

    def update_file_progress(self, file_id: str, downloaded: int, total: int) -> None:
        """更新单个文件的进度"""
        with self.lock:
            self.file_progresses[file_id] = (downloaded, total)

    def complete_file(self, file_id: str, size: int) -> None:
        """标记文件下载完成"""
        with self.lock:
            self.completed_files += 1
            self.file_progresses.pop(file_id, None)

    def get_overall_progress(self) -> Tuple[int, int, float, float]:
        """
        获取整体进度信息

        Returns:
            (已完成文件数, 总文件数, 速度MB/s, 已用时间秒)
        """
        with self.lock:
            current_downloaded = sum(prog[0] for prog in self.file_progresses.values())
            elapsed_time = time.time() - self.start_time
            speed_mbps = 0.0
            if elapsed_time > 0 and current_downloaded > 0:
                speed_mbps = (current_downloaded / (1024 * 1024)) / elapsed_time
            return self.completed_files, self.total_files, speed_mbps, elapsed_time
