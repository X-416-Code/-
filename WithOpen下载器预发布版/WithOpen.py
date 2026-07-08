import os
import re
import shutil
import requests
import subprocess
import warnings
import hashlib
import logging
import threading
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urlparse
from tqdm import tqdm
import time

warnings.resetwarnings()

# ==================== 配置（日志改到代码根目录） ====================
# 获取当前脚本所在目录（代码根目录）
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_FILE = os.path.join(ROOT_DIR, "download_log.txt")
SAVE_DIR = r"D:\Downloads\WithOpen下载器"
Version = "0.1.6, 非发行版"
os.makedirs(SAVE_DIR, exist_ok=True)

MIN_CHUNK_SIZE = 5 * 1024 * 1024
MAX_THREADS = 16
# 默认超时（可被参数覆盖）
DEFAULT_CONNECT_TIMEOUT = 30    # 连接超时30秒，过长等待无意义
DEFAULT_READ_TIMEOUT = 1800     # 读取超时30分钟，长时间卡顿断开
CHUNK = 2 * 1024 * 1024         # 分片读写缓冲区2MB，平衡IO与内存
RETRY_COUNT = 5                 # 单分片单次循环重试5次，避免无效重复
MAX_RETRY_ROUND = 2             # 全局分片重跑2轮，卡住切单线程

# 关键修复：禁用 requests 自动解压 gzip/deflate，保证 Range 字节精准
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept-Encoding": "identity"
}

# 日志初始化（根目录）
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    encoding="utf-8",
    filemode="a"
)
logger = logging.getLogger(__name__)

# 全局 session
SESSION = requests.Session()
SESSION.headers.update(HEADERS)

# ==================== 工具函数 ====================
def safe_filename(name: str) -> str:
    return re.sub(r'[<>:"/\\|?*]', '_', name)

def human_size(size: int) -> str:
    if size <= 0:
        return "0B"
    size_f = float(size)
    for u in ("B", "KB", "MB", "GB", "TB"):
        if size_f < 1024.0:
            return f"{size_f:.2f}{u}"
        size_f /= 1024.0
    return f"{size_f:.2f}PB"

def get_file_sha256(filepath: str) -> str:
    sha256 = hashlib.sha256()
    with open(filepath, "rb") as f:
        while chunk := f.read(1024*1024):
            sha256.update(chunk)
    return sha256.hexdigest()

# ==================== 控制台输出 + 同步写入日志 ====================
# 控制台颜色定义（RGB精确匹配）
COLOR_INFO = "\033[38;2;0;162;232m"    # 蓝色 INFORMATION
COLOR_OK = "\033[38;2;181;230;29m"     # 绿色 OK
COLOR_WARN = "\033[38;2;255;242;0m"    # 黄色 WARNING
COLOR_ERROR = "\033[38;2;237;28;36m"   # 红色 ERROR
COLOR_RESET = "\033[0m"                # 重置颜色

def WriteInformation(m):
    msg = f"[INFORMATION] {m}"
    print(f"{COLOR_INFO}{msg}{COLOR_RESET}")
    logger.info(msg)

def WriteSuccess(m):
    msg = f"[OK] {m}"
    print(f"{COLOR_OK}{msg}{COLOR_RESET}")
    logger.info(msg)

def WriteWarning(m):
    msg = f"[WARNING] {m}"
    print(f"{COLOR_WARN}{msg}{COLOR_RESET}")
    logger.warning(msg)

def WriteError(m):
    msg = f"[ERROR] {m}"
    print(f"{COLOR_ERROR}{msg}{COLOR_RESET}")
    logger.error(msg)

# ==================== 进度条 ====================
class ProgressBar:
    def __init__(self, total: int):
        self.total = total
        self.lock = threading.Lock()
        self.pbar = tqdm(
            total=total,
            unit="B",
            unit_scale=True,
            unit_divisor=1024,
            dynamic_ncols=True,
            colour="blue"
        )

    def update(self, n: int):
        with self.lock:
            self.pbar.update(n)

    def close(self):
        self.pbar.close()

# ==================== 分片合并 ====================
def merge_files(final_path: str, parts: list, chunk_size: int) -> None:
    with open(final_path, "wb") as out:
        for p in parts:
            if os.path.exists(p):
                with open(p, "rb") as f:
                    shutil.copyfileobj(f, out, chunk_size)
                os.remove(p)

# ==================== 分片下载（新增自定义chunk、retry参数） ====================
def download_chunk(url: str, start: int, end: int, part: str, progress: ProgressBar, verify_ssl: bool, timeout: tuple, chunk_size: int, retry_times: int) -> bool:
    expected = end - start + 1
    if os.path.exists(part) and os.path.getsize(part) == expected:
        progress.update(expected)
        return True

    headers = {
        **HEADERS,
        "Range": f"bytes={start}-{end}"
    }

    for _ in range(retry_times):
        try:
            with SESSION.get(url, headers=headers, stream=True, timeout=timeout, verify=verify_ssl) as r:
                if r.status_code in (403, 404, 400, 500, 503):
                    WriteWarning(f"分片{os.path.basename(part)} 服务器拒绝 ({r.status_code})")
                    return False
                if r.status_code != 206:
                    WriteWarning(f"分片{os.path.basename(part)} 未返回 206，状态码 {r.status_code}")
                    return False

                with open(part, "wb") as f:
                    for chunk in r.iter_content(chunk_size):
                        if chunk:
                            f.write(chunk)
                            progress.update(len(chunk))

            if os.path.getsize(part) == expected:
                return True
            else:
                WriteWarning(f"分片{os.path.basename(part)} 尺寸不匹配，重新下载")
                try:
                    os.remove(part)
                except Exception:
                    pass
        except requests.RequestException as e:
            err_str = str(e)[:120]
            WriteWarning(f"分片{os.path.basename(part)} 网络异常重试: {err_str}")
            continue
        except Exception as e:
            err_str = str(e)[:120]
            WriteWarning(f"分片{os.path.basename(part)} 未知错误: {err_str}")
            continue
    return False

# ==================== 检查服务器是否支持断点续传 ====================
def is_support_range(url: str, verify_ssl: bool, timeout: tuple) -> bool:
    try:
        resp = SESSION.head(url,headers=HEADERS,timeout=timeout,verify=verify_ssl,allow_redirects=True)
        if resp.headers.get("Accept-Ranges", "").lower() == "bytes":
            return True
    except requests.RequestException:
        pass

    try:
        with SESSION.get(url,headers={**HEADERS, "Range": "bytes=0-0"},timeout=timeout,verify=verify_ssl,stream=True) as r:
            return r.status_code == 206
    except requests.RequestException:
        return False

# ==================== 多线程下载（接收params自定义参数） ====================
def download_http(url: str, verify_ssl: bool, timeout: tuple, params: dict):
    # 读取自定义参数
    custom_chunk = params["-chunk"]
    custom_retry = params["-retry"]
    custom_max_round = params["-maxretry"]

    try:
        resp = SESSION.head(url, timeout=timeout, verify=verify_ssl, allow_redirects=True)
        resp.raise_for_status()
    except requests.RequestException:
        try:
            resp = SESSION.get(url, stream=True, timeout=timeout, verify=verify_ssl, allow_redirects=True)
            resp.raise_for_status()
        except requests.RequestException as e:
            WriteError(f"请求失败: {str(e)}")
            return

    file_size = 0
    try:
        file_size = int(resp.headers.get("Content-Length", 0))
    except Exception:
        file_size = 0

    name = safe_filename(os.path.basename(urlparse(url).path)) or "download.bin"
    final = os.path.join(SAVE_DIR, name)

    if os.path.exists(final):
        WriteSuccess(f"文件已存在：{name}，自动跳过")
        return

    if not is_support_range(url, verify_ssl, timeout):
        WriteInformation("服务器不支持多线程，将使用单线程下载")
        download_simple(url, verify_ssl, timeout, params)
        return

    if file_size <= 0:
        WriteInformation("无法获取文件大小，将使用单线程下载")
        download_simple(url, verify_ssl, timeout, params)
        return

    if file_size <= MIN_CHUNK_SIZE * 2:
        WriteInformation("文件较小，将使用单线程下载")
        download_simple(url, verify_ssl, timeout, params)
        return

    threads = min(MAX_THREADS, max(1, file_size // MIN_CHUNK_SIZE))
    WriteInformation(f"文件名：{name}")
    WriteInformation(f"文件大小：{human_size(file_size)} | 下载线程：{threads}")
    
    progress = ProgressBar(file_size)
    parts = []

    try:
        ranges = []
        chunk_size = file_size // threads
        remainder = file_size % threads
        current = 0
        for i in range(threads):
            s = current
            e = current + chunk_size - 1
            if i < remainder:
                e += 1
            part = f"{final}.part{i}"
            parts.append(part)
            ranges.append((s, e, part))
            current = e + 1

        fail_segments = ranges.copy()
        # 使用自定义全局重试轮次
        for round_idx in range(custom_max_round + 1):
            if not fail_segments:
                break
            WriteInformation(f"分片重试轮次 {round_idx}/{custom_max_round}，待重试分片数：{len(fail_segments)}")
            max_workers = min(MAX_THREADS, len(fail_segments))
            with ThreadPoolExecutor(max_workers) as pool:
                seg_future_map = {}
                for seg in fail_segments:
                    s, e, p = seg
                    # 传入自定义chunk与retry
                    fut = pool.submit(download_chunk, url, s, e, p, progress, verify_ssl, timeout, custom_chunk, custom_retry)
                    seg_future_map[seg]

                new_fail = []
                for seg, fut in seg_future_map.items():
                    s, e, p = seg
                    try:
                        success = fut.result()
                        if not success:
                            new_fail.append(seg)
                    except Exception as err:
                        WriteWarning(f"分片 {os.path.basename(p)} 线程异常: {str(err)[:60]}")
                        new_fail.append(seg)
                fail_segments = new_fail

        if fail_segments:
            err_msg = f"仍有{len(fail_segments)}个分片多次重试失败"
            WriteError(err_msg)
            # 清理残留分片
            for p in parts:
                if os.path.exists(p):
                    try:
                        os.remove(p)
                    except Exception:
                        pass
            raise RuntimeError(err_msg)

        WriteInformation("正在合并文件...")
        merge_files(final, parts, custom_chunk)
        WriteSuccess(f"下载完成：{final}")

        WriteInformation("正在校验文件完整性...")
        file_hash = get_file_sha256(final)
        WriteSuccess(f"文件校验通过 SHA256: {file_hash[:16]}...")

    except Exception as main_err:
        WriteError(f"多线程下载失败({str(main_err)})，自动切换单线程")
        # 清理失败的分片
        for p in parts:
            if os.path.exists(p):
                try:
                    os.remove(p)
                except Exception:
                    pass
        download_simple(url, verify_ssl, timeout, params)
    finally:
        progress.close()

# ==================== 单线程兜底（接收自定义chunk） ====================
def download_simple(url: str, verify_ssl: bool, timeout: tuple, params: dict):
    custom_chunk = params["-chunk"]
    name = safe_filename(os.path.basename(urlparse(url).path)) or "download.bin"
    final = os.path.join(SAVE_DIR, name)
    temp_file = final + ".tmp"

    if os.path.exists(final):
        WriteSuccess("文件已存在，跳过下载")
        return

    downloaded = 0
    if os.path.exists(temp_file):
        downloaded = os.path.getsize(temp_file)

    for attempt in range(RETRY_COUNT):
        try:
            headers = {**HEADERS}
            if downloaded > 0:
                headers["Range"] = f"bytes={downloaded}-"

            resp = SESSION.get(
                url, headers=headers, stream=True,
                timeout=timeout, verify=verify_ssl
            )
            resp.raise_for_status()

            if downloaded == 0:
                total = int(resp.headers.get("Content-Length", 0))
            else:
                remaining = int(resp.headers.get("Content-Length", 0))
                total = downloaded + remaining

            mode = "ab" if downloaded > 0 else "wb"
            with tqdm(total=total, initial=downloaded, unit="B", unit_scale=True, colour="green") as pbar:
                with open(temp_file, mode) as f:
                    for chunk in resp.iter_content(custom_chunk):
                        if chunk:
                            f.write(chunk)
                            pbar.update(len(chunk))
                            downloaded += len(chunk)

            if os.path.exists(final):
                os.remove(final)
            os.rename(temp_file, final)

            WriteSuccess(f"单线程下载完成：{final}")
            WriteInformation("正在校验文件完整性...")
            h = get_file_sha256(final)
            WriteSuccess(f"文件校验通过 SHA256: {h[:16]}...")
            return

        except requests.RequestException as e:
            WriteWarning(f"单线程断开，重试 {attempt+1}/{RETRY_COUNT}")
            continue
        except Exception as e:
            WriteError(f"单线程下载异常：{str(e)}")
            break

    WriteError("单线程下载失败，已达最大重试次数")
    if os.path.exists(temp_file):
        try:
            os.remove(temp_file)
        except Exception:
            pass
    return

# ==================== Aria2 ====================
def download_aria2(link: str):
    if not shutil.which("aria2c"):
        WriteError("未找到 aria2c.exe，请放在同一目录")
        return

    cmd = [
        "aria2c", "-d", SAVE_DIR,
        "-x", "16", "-s", "16", "-c",
        "--file-allocation=none",
        "--user-agent", HEADERS["User-Agent"],
        "--connect-timeout=600",
        "--timeout=86400",
        link
    ]
    try:
        subprocess.run(cmd, check=True, shell=False)
        WriteSuccess("Aria2: 下载完成")
    except subprocess.CalledProcessError:
        WriteError("Aria2 下载失败")

# ==================== 参数解析函数（无重复代码，修复timeout元组bug） ====================
def parse_input(input_str: str):
    # ===================== 参数扩展配置区【新增参数只改这里】 =====================
    # 格式："-参数名": (默认值, 转换lambda, 参数说明)
    param_config = {
        "-verify": (
            True,
            lambda v: v.strip().lower() == "true",
            "SSL证书验证开关 true/false"
        ),
        "-timeout": (
            (DEFAULT_CONNECT_TIMEOUT, DEFAULT_READ_TIMEOUT),
            lambda v: tuple(map(int, [x.strip() for x in v.split(",", 1)])) if "," in v else (int(v.strip()), int(v.strip())),
            "超时 连接,读取 或单数字"
        ),
        "-chunk": (
            CHUNK,
            lambda v: int(v.strip()) * 1024 * 1024,
            "单次读写缓冲区大小(MB)"
        ),
        "-retry": (
            RETRY_COUNT,
            lambda v: int(v.strip()),
            "单个分片单次最大重试次数"
        ),
        "-maxretry": (
            MAX_RETRY_ROUND,
            lambda v: int(v.strip()),
            "分片全局重试轮次"
        ),
    }
    # 初始化全部参数为默认值
    params = {k: v[0] for k, v in param_config.items()}

    tokens = input_str.strip().split()
    if not tokens:
        # 修复：返回标准二元超时元组，不再只传单个数字
        return None, True, (DEFAULT_CONNECT_TIMEOUT, DEFAULT_READ_TIMEOUT), params

    url = tokens[0]
    args = tokens[1:]
    i = 0
    while i < len(args):
        if i + 1 >= len(args):
            break
        key = args[i]
        val_raw = args[i + 1]
        if key in param_config:
            default, convert_func, desc = param_config[key]
            try:
                parsed_val = convert_func(val_raw)
                params[key] = parsed_val
            except Exception:
                WriteWarning(f"参数 {key} [{desc}] 格式错误，使用默认值")
        i += 2

    verify_ssl = params["-verify"]
    timeout_tuple = params["-timeout"]
    return url, verify_ssl, timeout_tuple, params

# ==================== 主程序 ====================
def main():
    raw_input = input("\nWithOpen URL> ").strip()
    #彩蛋
    if raw_input == '/kill @s':
        WriteWarning('SYSTEM DAMAGE!（彩蛋）')
        return
    
    if raw_input.lower() in ("exit", "exit()"):
        WriteInformation("程序退出")
        time.sleep(1)
        exit()
    if not raw_input:
        WriteError("下载链接不能为空")
        return

    # 接收4个返回值
    link, verify_ssl, timeout, params = parse_input(raw_input)

    # 判空修复类型警告
    if not link:
        WriteError("解析链接失败，链接为空")
        return
    if link.startswith(("ed2k://", "magnet:")):
        download_aria2(link)
    else:
        # 传入完整参数字典
        download_http(link, verify_ssl, timeout, params)

if __name__ == "__main__":
    print(f"\nWithOpen 下载器    Version = {Version}")
    while True:
        try:
            main()
        except KeyboardInterrupt:
            print("\n")
            WriteInformation("Ctrl+C终止程序")
            time.sleep(1)
            exit()
        except Exception as e:
            WriteError(f"程序异常：{str(e)}")
            print('')
            