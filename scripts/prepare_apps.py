import csv
import os
from datetime import datetime

# 定义输出CSV的字段
FIELDNAMES = [
    "package_name", "sha256", "apk_name", "apk_path", "app_output_dir",
    "year", "size", "vercode", "markets", "vt_detection", "contain_ad", 
    "is_downloaded", "is_tested", "issue", "sensor_test_done", "timestamp", 
    "device_serial"
]

def read_root_log(root_log_path):
    """读取root_log.csv，提取apk_name(去除.apk)和sha256(转大写)"""
    tested_sha256 = set()
    tested_apk_names = set()
    
    print(f"正在读取 {root_log_path}...")
    
    with open(root_log_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            # 提取sha256并转大写
            if 'sha256' in row and row['sha256']:
                tested_sha256.add(row['sha256'].upper())
            
            # 提取apk_name并去除.apk后缀
            if 'apk_name' in row and row['apk_name']:
                apk_name = row['apk_name']
                if apk_name.endswith('.apk'):
                    apk_name = apk_name[:-4]
                tested_apk_names.add(apk_name)
    
    print(f"已记录 {len(tested_sha256)} 个已测试的SHA256")
    return tested_sha256, tested_apk_names

def filter_and_generate_csvs(latest_csv_path, tested_sha256, output_dir='output', 
                             max_apks=5000, batch_size=500, max_size_mb=50,
                             start_year=2025):
    """
    从latest.csv中筛选未测试的APK并生成批次CSV文件
    
    参数:
    - latest_csv_path: AndroZoo的latest.csv路径
    - tested_sha256: 已测试的SHA256集合
    - output_dir: 输出目录
    - max_apks: 最大APK数量（默认5000）
    - batch_size: 每个CSV的APK数量（默认500）
    - max_size_mb: 最大APK大小（MB）（默认50MB）
    - start_year: 起始年份（默认2025）
    """
    
    # 创建输出目录
    os.makedirs(output_dir, exist_ok=True)
    
    print(f"\n正在读取 {latest_csv_path}...")
    print(f"筛选条件: 年份>={start_year}, 大小<={max_size_mb}MB")
    
    selected_apks = []
    skipped_count = 0
    processed_count = 0
    
    max_size_bytes = max_size_mb * 1024 * 1024  # 转换为字节
    
    with open(latest_csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        
        for row in reader:
            processed_count += 1
            
            if processed_count % 10000 == 0:
                print(f"已处理 {processed_count} 行，已选择 {len(selected_apks)} 个APK")
            
            # 检查是否已达到目标数量
            if len(selected_apks) >= max_apks:
                break
            
            try:
                # 提取并转换SHA256为大写
                sha256 = row.get('sha256', '').upper()
                if not sha256 or sha256 in tested_sha256:
                    skipped_count += 1
                    continue
                
                # 提取年份（dex_date字段）
                dex_date = row.get('dex_date', '')
                if dex_date:
                    year = int(dex_date.split('-')[0])
                    if year < start_year:
                        continue
                else:
                    continue
                
                # 提取大小（apk_size字段）
                apk_size = int(row.get('apk_size', 0))
                if apk_size > max_size_bytes or apk_size == 0:
                    continue
                
                # 提取其他字段
                pkg_name = row.get('pkg_name', '')
                vercode = row.get('vercode', '')
                markets = row.get('markets', '')
                vt_detection = row.get('vt_detection', '')
                
                # 构建新记录
                new_record = {
                    'package_name': pkg_name,
                    'sha256': sha256,
                    'apk_name': f"{pkg_name}_{vercode}" if pkg_name and vercode else f"{sha256}",
                    'apk_path': '',  # 下载后填写
                    'app_output_dir': '',  # 测试时填写
                    'year': year,
                    'size': apk_size,
                    'vercode': vercode,
                    'markets': markets,
                    'vt_detection': vt_detection,
                    'contain_ad': '',
                    'is_downloaded': 'False',
                    'is_tested': 'False',
                    'issue': '',
                    'sensor_test_done': 'False',
                    'timestamp': '',
                    'device_serial': ''
                }
                
                selected_apks.append(new_record)
                
            except Exception as e:
                print(f"处理行时出错: {e}")
                continue
    
    print(f"\n总共处理 {processed_count} 行")
    print(f"跳过 {skipped_count} 个已测试的APK")
    print(f"选择了 {len(selected_apks)} 个新APK")
    
    # 分批写入CSV文件
    num_batches = (len(selected_apks) + batch_size - 1) // batch_size
    
    for i in range(num_batches):
        start_idx = i * batch_size
        end_idx = min((i + 1) * batch_size, len(selected_apks))
        batch_apks = selected_apks[start_idx:end_idx]
        
        output_file = os.path.join(output_dir, f'batch_{i+1:02d}.csv')
        
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
            writer.writeheader()
            writer.writerows(batch_apks)
        
        print(f"已生成 {output_file} ({len(batch_apks)} 个APK)")
    
    print(f"\n完成！共生成 {num_batches} 个CSV文件")
    return len(selected_apks)

def main():
    # 配置文件路径
    ROOT_LOG_PATH = '/Users/noname/noname/nku/intrusiveads/results/round2/root_log.csv'
    LATEST_CSV_PATH = '/Users/noname/noname/SysBOM/latest.csv'
    OUTPUT_DIR = '/Users/noname/noname/nku/intrusiveads/test_apks/'
    
    # 配置参数
    MAX_APKS = 5000  # 总共需要的APK数量
    BATCH_SIZE = 500  # 每个CSV文件的APK数量
    MAX_SIZE_MB = 1000  # 最大APK大小（MB）
    START_YEAR = 2025  # 起始年份
    
    print("=" * 60)
    print("APK筛选与CSV生成工具")
    print("=" * 60)
    
    # 读取已测试的APK
    tested_sha256, tested_apk_names = read_root_log(ROOT_LOG_PATH)
    
    # 筛选并生成CSV文件
    total_selected = filter_and_generate_csvs(
        latest_csv_path=LATEST_CSV_PATH,
        tested_sha256=tested_sha256,
        output_dir=OUTPUT_DIR,
        max_apks=MAX_APKS,
        batch_size=BATCH_SIZE,
        max_size_mb=MAX_SIZE_MB,
        start_year=START_YEAR
    )
    
    print("\n处理完成！")
    print(f"输出目录: {OUTPUT_DIR}")
    print(f"总计选择: {total_selected} 个APK")

if __name__ == '__main__':
    main()