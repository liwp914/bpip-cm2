import os
import configparser
import requests
import pandas as pd
from tqdm import tqdm
import time
import concurrent.futures
import logging
import socket
import subprocess
import platform
import threading
import glob
from collections import defaultdict

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('ip_info_collector.log', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

# 读取配置文件
config = configparser.ConfigParser()
try:
    with open('config.ini', 'r', encoding='utf-8') as f:
        config.read_file(f)
except FileNotFoundError:
    logger.error("配置文件 config.ini 不存在")
    exit(1)
except Exception as e:
    logger.error(f"读取配置文件时出错: {e}")
    exit(1)

def get_config_value(section, option, default=None):
    """安全获取配置值"""
    try:
        return config.get(section, option)
    except (configparser.NoSectionError, configparser.NoOptionError):
        return default

# 从配置获取常量
INPUT_DIR = get_config_value('directories', 'input_dir', './ips/{port}/')
OUTPUT_DIR = get_config_value('directories', 'output_dir', './ip-info/{port}/')
BATCH_SIZE = int(get_config_value('api', 'batch_size', 100))
MAX_RETRIES = int(get_config_value('api', 'max_retries', 3))
MAX_WORKERS = int(get_config_value('api', 'max_workers', 5))
COUNTRIES = [c.strip() for c in get_config_value('processing', 'countries', 'HK,JP,KR,SG,US').split(',')]
MAX_RECORDS = int(get_config_value('processing', 'max_records_per_country', 10))

# IP检测相关配置
ENABLE_IP_CHECK = get_config_value('ip_check', 'enable_ip_check', 'true').lower() == 'true'
CHECK_TIMEOUT = float(get_config_value('ip_check', 'check_timeout', 2))
CHECK_METHOD = get_config_value('ip_check', 'check_method', 'port')
CHECK_PORT = int(get_config_value('ip_check', 'check_port', 443))
CHECK_THREADS = int(get_config_value('ip_check', 'check_threads', 50))

# 合并配置
PORTS_TO_MERGE = [p.strip() for p in get_config_value('merging', 'ports_to_merge', '443,8443,2053,2083,2087,2096').split(',')]
MERGE_OUTPUT_DIR = get_config_value('merging', 'merge_output_dir', './ip-info/mrked/')
MAX_RECORDS_PER_MERGE = int(get_config_value('merging', 'max_records_per_merge', 10))

# API速率限制相关
API_RATE_LIMIT = 15  # ip-api.com每分钟最多15个请求
API_WINDOW = 60      # 60秒窗口

def get_ip_from_file(filename):
    """从文本文件读取IP地址"""
    try:
        with open(filename, "r", encoding="utf-8") as f:
            ips = [line.strip() for line in f if line.strip()]
        logger.info(f"从 {filename} 读取了 {len(ips)} 个IP")
        return ips
    except Exception as e:
        logger.error(f"读取文件 {filename} 时出错: {e}")
        return []

def ipinfoapi_batch(ips: list, session, retries=MAX_RETRIES):
    """批量查询IP信息（带重试机制）"""
    if not ips:
        return []
        
    url = 'http://ip-api.com/batch'
    ips_dict = [{'query': ip, "fields": "city,country,countryCode,isp,org,as,query"} for ip in ips]
    
    for attempt in range(retries):
        try:
            response = session.post(url, json=ips_dict)
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 429:  # 速率限制
                wait_time = min(60, 10 * (attempt + 1))  # 指数退避
                logger.warning(f"API速率受限，等待 {wait_time} 秒后重试...")
                time.sleep(wait_time)
            else:
                logger.error(f'获取IP信息失败: {response.status_code}, {response.reason}')
                return []
        except Exception as e:
            logger.error(f'请求错误: {e}')
            time.sleep(2)
    
    logger.error(f"经过 {retries} 次重试后仍失败")
    return []

def get_ip_info(ips):
    """获取IP信息（带速率限制控制）"""
    if not ips:
        logger.warning("IP列表为空，跳过信息获取")
        return []
        
    # 创建批次
    batches = [ips[i:i + BATCH_SIZE] for i in range(0, len(ips), BATCH_SIZE)]
    results = []
    
    # 计算最小请求间隔（遵守API速率限制）
    min_interval = API_WINDOW / API_RATE_LIMIT
    logger.info(f"API速率限制: {API_RATE_LIMIT} 请求/分钟, 最小间隔: {min_interval:.2f} 秒")
    
    with tqdm(total=len(batches), desc="处理IP批次") as pbar:
        with requests.Session() as session:
            last_request_time = 0
            
            for batch in batches:
                # 确保遵守API速率限制
                current_time = time.time()
                elapsed = current_time - last_request_time
                
                if elapsed < min_interval:
                    wait_time = min_interval - elapsed
                    logger.debug(f"等待 {wait_time:.2f} 秒以遵守API速率限制")
                    time.sleep(wait_time)
                
                # 记录请求时间
                last_request_time = time.time()
                
                # 处理批次
                batch_result = ipinfoapi_batch(batch, session)
                if batch_result:
                    results.extend(batch_result)
                
                pbar.update(1)
    
    return results

def gather_ip_addresses(port):
    """收集指定端口的IP地址"""
    port_dir = INPUT_DIR.format(port=port)
    
    if not os.path.exists(port_dir):
        logger.warning(f"端口 {port} 目录不存在: {port_dir}")
        return []
    
    logger.info(f"扫描端口 {port} 目录: {port_dir}")
    all_ips = []
    
    for file in os.listdir(port_dir):
        if file.endswith('.txt'):
            file_path = os.path.join(port_dir, file)
            all_ips.extend(get_ip_from_file(file_path))
    
    unique_ips = list(set(all_ips))
    logger.info(f"收集到 {len(all_ips)} 个IP地址，去重后: {len(unique_ips)}")
    return unique_ips

def check_ip_port(ip, port, timeout=CHECK_TIMEOUT):
    """检测IP端口是否开放"""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(timeout)
            result = s.connect_ex((ip, port))
            return result == 0
    except Exception:
        return False

def check_ip_ping(ip, timeout=CHECK_TIMEOUT):
    """检测IP是否可ping通"""
    param = '-n' if platform.system().lower() == 'windows' else '-c'
    command = ['ping', param, '1', '-w', str(int(timeout * 1000)), ip]
    
    try:
        with subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE) as process:
            _, _ = process.communicate()
            return process.returncode == 0
    except Exception:
        return False

def check_ip(ip):
    """根据配置检测IP"""
    if CHECK_METHOD == 'port':
        return check_ip_port(ip, CHECK_PORT, CHECK_TIMEOUT)
    elif CHECK_METHOD == 'ping':
        return check_ip_ping(ip, CHECK_TIMEOUT)
    else:
        logger.warning(f"未知的检测方法: {CHECK_METHOD}, 默认使用端口检测")
        return check_ip_port(ip, CHECK_PORT, CHECK_TIMEOUT)

def check_ips(ips):
    """批量检测IP可用性"""
    if not ENABLE_IP_CHECK:
        logger.info("IP检测已禁用，跳过检测")
        return [True] * len(ips)
    
    logger.info(f"开始检测 {len(ips)} 个IP的可用性 (方法: {CHECK_METHOD})")
    
    valid_ips = []
    lock = threading.Lock()
    
    def check_and_record(ip):
        result = check_ip(ip)
        with lock:
            valid_ips.append((ip, result))
    
    with tqdm(total=len(ips), desc="检测IP可用性") as pbar:
        with concurrent.futures.ThreadPoolExecutor(max_workers=CHECK_THREADS) as executor:
            futures = {executor.submit(check_and_record, ip): ip for ip in ips}
            
            for future in concurrent.futures.as_completed(futures):
                pbar.update(1)
    
    # 按原始顺序返回结果
    result_map = {ip: result for ip, result in valid_ips}
    return [result_map.get(ip, False) for ip in ips]

def process_ip_info(ip_info, port):
    """处理IP信息并按国家保存"""
    if not ip_info:
        logger.warning(f"没有获取到IP信息，跳过处理端口 {port}")
        return
        
    save_dir = OUTPUT_DIR.format(port=port)
    os.makedirs(save_dir, exist_ok=True)

    try:
        df = pd.DataFrame(ip_info)
        
        # 检测IP可用性
        if ENABLE_IP_CHECK:
            ips = df['query'].tolist()
            valid_results = check_ips(ips)
            df['valid'] = valid_results
            logger.info(f"有效IP数量: {sum(valid_results)}/{len(valid_results)}")
        else:
            df['valid'] = True
            logger.info("IP检测已禁用，跳过有效性检查")
        
        # 只保留有效的IP
        valid_df = df[df['valid']]
        
        grouped = valid_df.groupby('countryCode')
        
        for country_code, group in grouped:
            unique_ips = group['query'].drop_duplicates()
            output_file = os.path.join(save_dir, f"{country_code}.txt")
            unique_ips.to_csv(output_file, header=None, index=False)
            logger.info(f"保存 {country_code} 的 {len(unique_ips)} 个有效IP到 {output_file}")
            
            # 保存包含所有信息的CSV文件
            info_file = os.path.join(save_dir, f"{country_code}_info.csv")
            group.to_csv(info_file, index=False)
            logger.info(f"保存 {country_code} 的详细信息到 {info_file}")
    except Exception as e:
        logger.error(f"处理IP信息时出错: {e}")
        if ip_info:
            logger.debug(f"数据结构示例: {ip_info[:1]}")

def create_country_marker_files(port):
    """为指定国家创建带标记的文件（在IP后添加端口号）"""
    base_dir = OUTPUT_DIR.format(port=port)
    
    for country in COUNTRIES:
        source_path = os.path.join(base_dir, f"{country}.txt")
        target_path = os.path.join(base_dir, f"{country}_marked.txt")
        
        if not os.path.exists(source_path):
            logger.warning(f"国家文件不存在: {source_path}")
            continue
            
        try:
            # 读取源文件，只取前MAX_RECORDS行
            with open(source_path, "r", encoding="utf-8") as src:
                lines = [line.strip() for line in src.readlines()[:MAX_RECORDS]]
            
            # 写入目标文件，添加端口号和标记
            with open(target_path, "w", encoding="utf-8") as tgt:
                for line in lines:
                    if line:  # 跳过空行
                        # 在IP地址后添加端口号和标记
                        tgt.write(f"{line}:{port}#{country}☮\n")
            
            logger.info(f"已创建标记文件: {target_path} (包含 {len(lines)} 条记录，端口: {port})")
        except Exception as e:
            logger.error(f"处理 {country} 文件时出错: {e}")

def merge_marked_files():
    """合并所有端口的标记文件到指定目录（新增的文件合并功能）"""
    logger.info("开始合并标记文件")
    
    # 创建合并输出目录
    os.makedirs(MERGE_OUTPUT_DIR, exist_ok=True)
    
    # 使用字典存储每个国家的所有记录
    country_records = defaultdict(list)
    total_files_found = 0
    
    # 收集所有端口的标记文件内容
    for port in PORTS_TO_MERGE:
        port_dir = OUTPUT_DIR.format(port=port)
        
        if not os.path.exists(port_dir):
            logger.warning(f"端口目录不存在: {port_dir}")
            continue
            
        for country in COUNTRIES:
            marked_file_path = os.path.join(port_dir, f"{country}_marked.txt")
            
            if os.path.exists(marked_file_path):
                try:
                    with open(marked_file_path, "r", encoding="utf-8") as f:
                        lines = [line.strip() for line in f if line.strip()]
                        country_records[country].extend(lines)
                    total_files_found += 1
                    logger.info(f"从端口 {port} 读取了 {len(lines)} 条 {country} 记录")
                except Exception as e:
                    logger.error(f"读取文件 {marked_file_path} 时出错: {e}")
    
    logger.info(f"总共找到 {total_files_found} 个标记文件，涉及 {len(country_records)} 个国家")
    
    # 为每个国家创建合并文件
    merged_count = 0
    for country, records in country_records.items():
        if not records:
            logger.warning(f"国家 {country} 没有记录，跳过")
            continue
            
        # 去重并限制记录数量
        unique_records = list(set(records))
        if len(unique_records) > MAX_RECORDS_PER_MERGE:
            unique_records = unique_records[:MAX_RECORDS_PER_MERGE]
            logger.info(f"国家 {country} 记录数超过限制，保留前 {MAX_RECORDS_PER_MERGE} 条")
        
        # 写入合并文件
        output_file = os.path.join(MERGE_OUTPUT_DIR, f"{country}2_mrked.txt")
        try:
            with open(output_file, "w", encoding="utf-8") as f:
                for record in unique_records:
                    f.write(f"{record}\n")
            
            merged_count += 1
            logger.info(f"合并完成: {output_file} (包含 {len(unique_records)} 条唯一记录)")
        except Exception as e:
            logger.error(f"写入合并文件 {output_file} 时出错: {e}")
    
    logger.info(f"标记文件合并完成，共生成 {merged_count} 个合并文件")

def process_single_port(port):
    """处理单个端口的完整流程"""
    logger.info(f"开始处理端口 {port}")
    
    # 收集IP地址
    ips = gather_ip_addresses(port)
    
    if not ips:
        logger.warning(f"端口 {port} 没有找到IP地址，跳过API调用")
        # 确保输出目录存在，即使没有数据也创建目录结构
        save_dir = OUTPUT_DIR.format(port=port)
        os.makedirs(save_dir, exist_ok=True)
        return
    
    # 获取IP信息
    ip_info = get_ip_info(ips)
    
    # 处理IP信息并保存
    process_ip_info(ip_info, port)
    
    # 创建标记文件
    create_country_marker_files(port)
    
    logger.info(f"端口 {port} 处理完成")

def main(ports_config):
    """主处理函数，支持多端口"""
    if isinstance(ports_config, str):
        ports = [p.strip() for p in ports_config.split(',')]
    else:
        ports = [ports_config] if ports_config else ['443']  # 默认端口
    
    logger.info(f"配置处理的端口列表: {ports}")
    
    # 循环处理每个端口
    for port in ports:
        try:
            process_single_port(port)
        except Exception as e:
            logger.error(f"处理端口 {port} 时发生错误: {e}")
            continue
    
    # 合并所有标记文件
    merge_marked_files()
    
    logger.info(f"所有端口处理完成: {ports}")

if __name__ == "__main__":
    # 从配置获取要处理的端口
    ports_config = get_config_value('settings', 'ports', '443')
    
    logger.info("=" * 50)
    logger.info(f"IP信息收集器启动")
    logger.info(f"配置参数:")
    logger.info(f"  处理端口: {ports_config}")
    logger.info(f"  合并端口: {', '.join(PORTS_TO_MERGE)}")
    logger.info(f"  合并输出目录: {MERGE_OUTPUT_DIR}")
    logger.info(f"  每个合并文件最大记录数: {MAX_RECORDS_PER_MERGE}")
    logger.info("=" * 50)
    
    try:
        main(ports_config)
        logger.info("所有处理完成!")
    except Exception as e:
        logger.exception("处理过程中发生严重错误")