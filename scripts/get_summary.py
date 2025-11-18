import os
import pandas as pd
import json
from datetime import datetime

# 全局文件路径
GLOBAL_CSV = "all_apps_master.csv"
GLOBAL_TXT = "checked_apps.txt"

def deduplicate_and_analyze_master_csv():
    """
    对全局CSV文件进行去重和统计分析
    
    Returns:
        dict: 包含详细统计信息的字典
    """
    try:
        if not os.path.exists(GLOBAL_CSV):
            print(f"[!] 全局CSV文件不存在: {GLOBAL_CSV}")
            return None
        
        # 读取CSV文件
        df = pd.read_csv(GLOBAL_CSV, dtype=str).fillna("")
        original_count = len(df)
        print(f"[+] 读取全局CSV: 共 {original_count} 条记录")
        
        if original_count == 0:
            print("[!] CSV文件为空")
            return None
        
        # 去重处理 - 基于app_name，保留最新的记录
        df['test_date'] = pd.to_datetime(df['test_date'], errors='coerce')
        df_sorted = df.sort_values('test_date', ascending=False)
        df_dedup = df_sorted.drop_duplicates('app_name', keep='first')
        dedup_count = len(df_dedup)
        
        print(f"[+] 去重完成: 从 {original_count} 条记录去重到 {dedup_count} 条")
        
        # 如果有重复，保存去重后的文件
        if dedup_count < original_count:
            backup_file = GLOBAL_CSV.replace(".csv", f"_backup_{int(datetime.now().timestamp())}.csv")
            df.to_csv(backup_file, index=False)
            print(f"[+] 已备份原文件: {backup_file}")
            
            df_dedup.to_csv(GLOBAL_CSV, index=False)
            print(f"[+] 已保存去重后的文件: {GLOBAL_CSV}")
        
        # 计算统计信息
        stats = calculate_detailed_stats(df_dedup)
        
        # 保存统计报告
        save_statistics_report(stats, df_dedup)
        
        return stats
        
    except Exception as e:
        print(f"[!] 处理全局CSV失败: {e}")
        import traceback
        traceback.print_exc()
        return None

def calculate_detailed_stats(df):
    """
    计算详细的统计信息
    
    Args:
        df: 去重后的DataFrame
        
    Returns:
        dict: 详细统计信息
    """
    # 基本统计
    total_apps = len(df)
    tested_apps = len(df[df['is_tested'].str.upper().isin(['TRUE', 'T', '1', 'YES', 'Y'])])
    utg_exists = len(df[df['utg_exists'].str.upper().isin(['TRUE', 'T', '1', 'YES', 'Y'])])
    has_ad = len(df[df['has_ad'].str.upper().isin(['TRUE', 'T', '1', 'YES', 'Y'])])
    
    # 广告类型统计
    type2_count = len(df[df['type2_detected'].str.upper().isin(['TRUE', 'T', '1', 'YES', 'Y'])])
    type3_count = len(df[df['type3_detected'].str.upper().isin(['TRUE', 'T', '1', 'YES', 'Y'])])
    type4_count = len(df[df['type4_detected'].str.upper().isin(['TRUE', 'T', '1', 'YES', 'Y'])])
    type5_count = len(df[df['type5_detected'].str.upper().isin(['TRUE', 'T', '1', 'YES', 'Y'])])
    type6_count = len(df[df['type6_detected'].str.upper().isin(['TRUE', 'T', '1', 'YES', 'Y'])])
    
    # 计算百分比
    ad_percentage = (has_ad / total_apps * 100) if total_apps > 0 else 0
    tested_percentage = (tested_apps / total_apps * 100) if total_apps > 0 else 0
    utg_percentage = (utg_exists / total_apps * 100) if total_apps > 0 else 0
    
    # 在广告应用中的类型分布
    type2_in_ad = (type2_count / has_ad * 100) if has_ad > 0 else 0
    type3_in_ad = (type3_count / has_ad * 100) if has_ad > 0 else 0
    type4_in_ad = (type4_count / has_ad * 100) if has_ad > 0 else 0
    type5_in_ad = (type5_count / has_ad * 100) if has_ad > 0 else 0
    type6_in_ad = (type6_count / has_ad * 100) if has_ad > 0 else 0
    
    # 时间范围统计
    if 'test_date' in df.columns and not df['test_date'].isnull().all():
        df['test_date'] = pd.to_datetime(df['test_date'], errors='coerce')
        date_range = df['test_date'].dropna()
        if len(date_range) > 0:
            earliest_date = date_range.min().strftime('%Y-%m-%d')
            latest_date = date_range.max().strftime('%Y-%m-%d')
        else:
            earliest_date = latest_date = "N/A"
    else:
        earliest_date = latest_date = "N/A"
    
    stats = {
        "total_apps": total_apps,
        "tested_apps": tested_apps,
        "utg_exists": utg_exists,
        "has_ad": has_ad,
        "type2_count": type2_count,
        "type3_count": type3_count,
        "type4_count": type4_count,
        "type5_count": type5_count,
        "type6_count": type6_count,
        "ad_percentage": ad_percentage,
        "tested_percentage": tested_percentage,
        "utg_percentage": utg_percentage,
        "type2_in_ad": type2_in_ad,
        "type3_in_ad": type3_in_ad,
        "type4_in_ad": type4_in_ad,
        "type5_in_ad": type5_in_ad,
        "type6_in_ad": type6_in_ad,
        "earliest_test_date": earliest_date,
        "latest_test_date": latest_date,
        "analysis_timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }
    
    return stats

def save_statistics_report(stats, df):
    """
    保存统计报告到文件
    
    Args:
        stats: 统计信息字典
        df: 数据DataFrame
    """
    try:
        report_file = GLOBAL_CSV.replace(".csv", "_statistics_report.txt")
        
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write("=" * 60 + "\n")
            f.write("           APK分析统计报告\n")
            f.write("=" * 60 + "\n\n")
            
            f.write(f"报告生成时间: {stats['analysis_timestamp']}\n")
            f.write(f"数据文件: {GLOBAL_CSV}\n")
            f.write(f"测试时间范围: {stats['earliest_test_date']} 至 {stats['latest_test_date']}\n\n")
            
            f.write("📊 总体统计:\n")
            f.write("-" * 40 + "\n")
            f.write(f"总应用数: {stats['total_apps']}\n")
            f.write(f"已测试应用: {stats['tested_apps']} ({stats['tested_percentage']:.1f}%)\n")
            f.write(f"有UTG文件: {stats['utg_exists']} ({stats['utg_percentage']:.1f}%)\n")
            f.write(f"包含广告: {stats['has_ad']} ({stats['ad_percentage']:.1f}%)\n\n")
            
            f.write("🎯 广告类型分布:\n")
            f.write("-" * 40 + "\n")
            f.write(f"Type2 (功能性中断): {stats['type2_count']} ({stats['type2_in_ad']:.1f}% of ad apps)\n")
            f.write(f"Type3 (返回键问题): {stats['type3_count']} ({stats['type3_in_ad']:.1f}% of ad apps)\n")
            f.write(f"Type4 (重定向): {stats['type4_count']} ({stats['type4_in_ad']:.1f}% of ad apps)\n")
            f.write(f"Type5 (外部应用广告): {stats['type5_count']} ({stats['type5_in_ad']:.1f}% of ad apps)\n")
            f.write(f"Type6 (广告频率): {stats['type6_count']} ({stats['type6_in_ad']:.1f}% of ad apps)\n\n")
            
            f.write("📈 广告应用详情:\n")
            f.write("-" * 40 + "\n")
            
            # 列出包含广告的应用
            ad_apps = df[df['has_ad'].str.upper().isin(['TRUE', 'T', '1', 'YES', 'Y'])]
            if len(ad_apps) > 0:
                f.write("包含广告的应用列表:\n")
                for _, app in ad_apps.iterrows():
                    app_name = app.get('app_name', 'Unknown')
                    types = []
                    if app.get('type2_detected', '').upper() in ['TRUE', 'T', '1', 'YES', 'Y']:
                        types.append("Type2")
                    if app.get('type3_detected', '').upper() in ['TRUE', 'T', '1', 'YES', 'Y']:
                        types.append("Type3")
                    if app.get('type4_detected', '').upper() in ['TRUE', 'T', '1', 'YES', 'Y']:
                        types.append("Type4")
                    if app.get('type5_detected', '').upper() in ['TRUE', 'T', '1', 'YES', 'Y']:
                        types.append("Type5")
                    if app.get('type6_detected', '').upper() in ['TRUE', 'T', '1', 'YES', 'Y']:
                        types.append("Type6")
                    
                    type_str = ", ".join(types) if types else "无具体类型"
                    f.write(f"  - {app_name}: {type_str}\n")
            else:
                f.write("未发现包含广告的应用\n")
        
        print(f"[+] 统计报告已保存: {report_file}")
        return report_file
        
    except Exception as e:
        print(f"[!] 保存统计报告失败: {e}")
        return None

def print_detailed_stats():
    """打印详细的统计信息"""
    try:
        if not os.path.exists(GLOBAL_CSV):
            print(f"[!] 全局CSV文件不存在: {GLOBAL_CSV}")
            return
        
        # 读取数据
        df = pd.read_csv(GLOBAL_CSV, dtype=str).fillna("")
        
        if len(df) == 0:
            print("[!] CSV文件为空")
            return
        
        # 计算统计信息
        stats = calculate_detailed_stats(df)
        
        # 打印统计信息
        print("\n" + "=" * 60)
        print("           APK分析详细统计")
        print("=" * 60)
        
        print(f"\n📊 总体统计:")
        print(f"   总应用数: {stats['total_apps']}")
        print(f"   已测试应用: {stats['tested_apps']} ({stats['tested_percentage']:.1f}%)")
        print(f"   有UTG文件: {stats['utg_exists']} ({stats['utg_percentage']:.1f}%)")
        print(f"   包含广告: {stats['has_ad']} ({stats['ad_percentage']:.1f}%)")
        
        print(f"\n🎯 广告类型分布 (在{stats['has_ad']}个广告应用中):")
        print(f"   Type2 (功能性中断): {stats['type2_count']} ({stats['type2_in_ad']:.1f}%)")
        print(f"   Type3 (返回键问题): {stats['type3_count']} ({stats['type3_in_ad']:.1f}%)")
        print(f"   Type4 (重定向): {stats['type4_count']} ({stats['type4_in_ad']:.1f}%)")
        print(f"   Type5 (外部应用广告): {stats['type5_count']} ({stats['type5_in_ad']:.1f}%)")
        print(f"   Type6 (广告频率): {stats['type6_count']} ({stats['type6_in_ad']:.1f}%)")
        
        print(f"\n📅 测试时间范围:")
        print(f"   最早测试: {stats['earliest_test_date']}")
        print(f"   最新测试: {stats['latest_test_date']}")
        
        print(f"\n💾 数据文件:")
        print(f"   全局CSV: {GLOBAL_CSV}")
        print(f"   全局TXT: {GLOBAL_TXT}")
        
    except Exception as e:
        print(f"[!] 打印统计信息失败: {e}")

def get_master_csv_info():
    """
    获取主CSV文件的基本信息
    
    Returns:
        dict: 包含文件信息的字典
    """
    try:
        if not os.path.exists(GLOBAL_CSV):
            return {"exists": False, "size": 0, "record_count": 0}
        
        file_size = os.path.getsize(GLOBAL_CSV)
        
        df = pd.read_csv(GLOBAL_CSV, dtype=str).fillna("")
        record_count = len(df)
        
        return {
            "exists": True,
            "size": file_size,
            "record_count": record_count,
            "file_path": GLOBAL_CSV
        }
    except Exception as e:
        print(f"[!] 获取CSV信息失败: {e}")
        return {"exists": False, "size": 0, "record_count": 0}

# 使用示例
if __name__ == "__main__":
    # 检查CSV文件信息
    csv_info = get_master_csv_info()
    if csv_info["exists"]:
        print(f"[+] 主CSV文件: {csv_info['file_path']}")
        print(f"[+] 文件大小: {csv_info['size']} 字节")
        print(f"[+] 记录数量: {csv_info['record_count']}")
        
        # 去重和统计分析
        stats = deduplicate_and_analyze_master_csv()
        
        if stats:
            # 打印统计信息
            print_detailed_stats()
    else:
        print(f"[!] 主CSV文件不存在: {GLOBAL_CSV}")