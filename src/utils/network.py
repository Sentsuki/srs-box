"""
网络工具
支持文件下载、JSON下载、重试机制和并发下载功能
"""

import json
import time
import urllib.request
import urllib.error
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple, Union, Callable
from urllib.parse import urlparse

from .file_utils import FileUtils


class DownloadResult:
    """下载结果类"""
    
    def __init__(self, url: str, success: bool, file_path: Optional[str] = None, 
                 error: Optional[str] = None, size: int = 0):
        self.url = url
        self.success = success
        self.file_path = file_path
        self.error = error
        self.size = size


class NetworkUtils:
    """网络工具类"""
    
    def __init__(self, timeout: int = 30, user_agent: str = None):
        """
        初始化网络工具
        
        Args:
            timeout: 请求超时时间（秒），默认为 30
            user_agent: 用户代理字符串
        """
        self.timeout = timeout
        self.user_agent = user_agent or (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        )
    
    def _create_request(self, url: str) -> urllib.request.Request:
        """
        创建 HTTP 请求对象
        
        Args:
            url: 请求 URL
            
        Returns:
            请求对象
        """
        request = urllib.request.Request(url)
        request.add_header('User-Agent', self.user_agent)
        return request
    
    def _download_with_progress(self, url: str, output_path: Union[str, Path],
                               progress_callback: Optional[Callable[[int, int], None]] = None) -> bool:
        """
        下载文件并显示进度
        
        Args:
            url: 下载 URL
            output_path: 输出文件路径
            progress_callback: 进度回调函数，参数为 (已下载字节数, 总字节数)
            
        Returns:
            是否下载成功
        """
        try:
            request = self._create_request(url)
            
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                # 获取文件大小
                content_length = response.headers.get('Content-Length')
                total_size = int(content_length) if content_length else 0
                
                # 确保输出目录存在
                output_path = Path(output_path)
                FileUtils.ensure_dir(output_path.parent)
                
                # 下载文件
                downloaded_size = 0
                chunk_size = 8192  # 8KB chunks
                
                with open(output_path, 'wb') as f:
                    while True:
                        chunk = response.read(chunk_size)
                        if not chunk:
                            break
                        
                        f.write(chunk)
                        downloaded_size += len(chunk)
                        
                        # 调用进度回调
                        if progress_callback and total_size > 0:
                            progress_callback(downloaded_size, total_size)
                
                return True
                
        except (urllib.error.URLError, urllib.error.HTTPError, OSError) as e:
            # 删除部分下载的文件
            if output_path.exists():
                try:
                    output_path.unlink()
                except OSError:
                    pass
            raise e
    
    def download_file(self, url: str, output_path: Union[str, Path], 
                     max_retries: int = 3, retry_delay: float = 1.0,
                     progress_callback: Optional[Callable[[int, int], None]] = None) -> bool:
        """
        下载文件到指定路径
        
        Args:
            url: 下载 URL
            output_path: 输出文件路径
            max_retries: 最大重试次数，默认为 3
            retry_delay: 重试延迟（秒），默认为 1.0
            progress_callback: 进度回调函数
            
        Returns:
            是否下载成功
        """
        output_path = Path(output_path)
        
        # 检查文件是否已存在且不为空
        if output_path.exists() and output_path.stat().st_size > 0:
            return True
        
        last_error = None
        
        for attempt in range(max_retries + 1):
            try:
                success = self._download_with_progress(url, output_path, progress_callback)
                if success:
                    return True
                    
            except Exception as e:
                last_error = str(e)
                
                if attempt < max_retries:
                    time.sleep(retry_delay * (attempt + 1))  # 递增延迟
                    continue
                else:
                    break
        
        return False
    
    def download_json(self, url: str, max_retries: int = 3, 
                     retry_delay: float = 1.0) -> Optional[Dict[str, Any]]:
        """
        下载并解析 JSON 数据
        
        Args:
            url: JSON URL
            max_retries: 最大重试次数，默认为 3
            retry_delay: 重试延迟（秒），默认为 1.0
            
        Returns:
            JSON 数据字典，失败时返回 None
        """
        last_error = None
        
        for attempt in range(max_retries + 1):
            try:
                request = self._create_request(url)
                
                with urllib.request.urlopen(request, timeout=self.timeout) as response:
                    data = response.read().decode('utf-8')
                    return json.loads(data)
                    
            except (urllib.error.URLError, urllib.error.HTTPError, 
                   json.JSONDecodeError, UnicodeDecodeError) as e:
                last_error = str(e)
                
                if attempt < max_retries:
                    time.sleep(retry_delay * (attempt + 1))
                    continue
                else:
                    break
        
        return None
    
    def download_text(self, url: str, max_retries: int = 3, 
                     retry_delay: float = 1.0) -> Optional[List[str]]:
        """
        下载文本内容并返回行列表
        
        Args:
            url: 文本 URL
            max_retries: 最大重试次数，默认为 3
            retry_delay: 重试延迟（秒），默认为 1.0
            
        Returns:
            文本行列表，失败时返回 None
        """
        last_error = None
        
        for attempt in range(max_retries + 1):
            try:
                request = self._create_request(url)
                
                with urllib.request.urlopen(request, timeout=self.timeout) as response:
                    data = response.read().decode('utf-8')
                    return [line.rstrip('\n\r') for line in data.splitlines()]
                    
            except (urllib.error.URLError, urllib.error.HTTPError, 
                   UnicodeDecodeError) as e:
                last_error = str(e)
                
                if attempt < max_retries:
                    time.sleep(retry_delay * (attempt + 1))
                    continue
                else:
                    break
        
        return None
    
    def download_multiple(self, download_tasks: List[Tuple[str, Union[str, Path]]], 
                         max_workers: int = 5, 
                         progress_callback: Optional[Callable[[int, int, str], None]] = None) -> List[DownloadResult]:
        """
        并发下载多个文件
        
        Args:
            download_tasks: 下载任务列表，每个元素为 (url, output_path) 元组
            max_workers: 最大并发数，默认为 5
            progress_callback: 进度回调函数，参数为 (已完成数, 总数, 当前文件名)
            
        Returns:
            下载结果列表
        """
        results = []
        completed_count = 0
        total_count = len(download_tasks)
        
        def download_single(task: Tuple[str, Union[str, Path]]) -> DownloadResult:
            """下载单个文件的内部函数"""
            url, output_path = task
            output_path = Path(output_path)
            
            try:
                success = self.download_file(url, output_path)
                if success:
                    size = FileUtils.get_file_size(output_path) if output_path.exists() else 0
                    return DownloadResult(url, True, str(output_path), None, size)
                else:
                    return DownloadResult(url, False, None, "下载失败")
                    
            except Exception as e:
                return DownloadResult(url, False, None, str(e))
        
        # 使用线程池并发下载
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # 提交所有任务
            future_to_task = {
                executor.submit(download_single, task): task 
                for task in download_tasks
            }
            
            # 处理完成的任务
            for future in as_completed(future_to_task):
                task = future_to_task[future]
                url, output_path = task
                
                try:
                    result = future.result()
                    results.append(result)
                    
                    completed_count += 1
                    
                    # 调用进度回调
                    if progress_callback:
                        filename = Path(output_path).name
                        progress_callback(completed_count, total_count, filename)
                        
                except Exception as e:
                    # 处理异常
                    result = DownloadResult(url, False, None, str(e))
                    results.append(result)
                    completed_count += 1
                    
                    if progress_callback:
                        filename = Path(output_path).name
                        progress_callback(completed_count, total_count, filename)
        
        return results
    
    def get_file_info(self, url: str) -> Optional[Dict[str, Any]]:
        """
        获取远程文件信息（不下载文件内容）
        
        Args:
            url: 文件 URL
            
        Returns:
            文件信息字典，包含 size, last_modified 等，失败时返回 None
        """
        try:
            request = self._create_request(url)
            request.get_method = lambda: 'HEAD'  # 使用 HEAD 请求
            
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                headers = response.headers
                
                info = {
                    'url': url,
                    'status_code': response.getcode(),
                    'content_type': headers.get('Content-Type', ''),
                    'content_length': int(headers.get('Content-Length', 0)) or None,
                    'last_modified': headers.get('Last-Modified', ''),
                    'etag': headers.get('ETag', ''),
                }
                
                return info
                
        except (urllib.error.URLError, urllib.error.HTTPError) as e:
            return None
    
    def is_url_accessible(self, url: str) -> bool:
        """
        检查 URL 是否可访问
        
        Args:
            url: 要检查的 URL
            
        Returns:
            URL 是否可访问
        """
        try:
            request = self._create_request(url)
            request.get_method = lambda: 'HEAD'
            
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                return response.getcode() == 200
                
        except (urllib.error.URLError, urllib.error.HTTPError):
            return False
    
    def get_filename_from_url(self, url: str) -> str:
        """
        从 URL 中提取文件名
        
        Args:
            url: URL 字符串
            
        Returns:
            文件名
        """
        parsed_url = urlparse(url)
        filename = Path(parsed_url.path).name
        
        # 如果没有文件名，使用默认名称
        if not filename or '.' not in filename:
            filename = f"download_{hash(url) % 10000}"
        
        return filename
    
    @staticmethod
    def is_json_url(url: str) -> bool:
        """
        判断 URL 是否指向 JSON 文件
        
        Args:
            url: URL 字符串
            
        Returns:
            是否为 JSON URL
        """
        url_lower = url.lower()
        return (url_lower.endswith('.json') or 
                'json' in url_lower or
                url_lower.endswith('.jsonl'))
    
    @staticmethod
    def is_text_url(url: str) -> bool:
        """
        判断 URL 是否指向文本文件
        
        Args:
            url: URL 字符串
            
        Returns:
            是否为文本 URL
        """
        url_lower = url.lower()
        text_extensions = ['.txt', '.list', '.conf', '.cfg', '.ini']
        return any(url_lower.endswith(ext) for ext in text_extensions)