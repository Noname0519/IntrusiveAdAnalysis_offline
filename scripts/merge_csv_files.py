import pandas as pd
import os
from collections import defaultdict

def merge_csv_files(csv_files, output_file=None, deduplicate=True, key_columns=None):
    """
    合并多个CSV文件，保留最多字段并去除重复数据
    
    参数:
    - csv_files: CSV文件路径列表
    - output_file: 输出文件路径 (可选)
    - deduplicate: 是否去重 (默认True)
    - key_columns: 去重依据的列名列表 (默认使用所有列)
    
    返回:
    - merged_df: 合并后的DataFrame
    - stats: 统计信息字典
    """
    
    if not csv_files:
        print("错误: 没有提供CSV文件")
        return None, {}
    
    # 读取所有CSV文件
    dataframes = []
    file_stats = {}
    
    print("正在读取CSV文件...")
    for file_path in csv_files:
        if not os.path.exists(file_path):
            print(f"警告: 文件不存在 - {file_path}")
            continue
            
        try:
            df = pd.read_csv(file_path, encoding='utf-8')
            dataframes.append(df)
            file_stats[file_path] = {
                'rows': len(df),
                'columns': len(df.columns),
                'columns_list': list(df.columns)
            }
            print(f"✓ 已读取: {file_path} ({len(df)} 行, {len(df.columns)} 列)")
        except Exception as e:
            print(f"✗ 读取失败: {file_path} - {e}")
            continue
    
    if not dataframes:
        print("错误: 没有成功读取任何CSV文件")
        return None, {}
    
    # 合并所有DataFrame
    print("\n正在合并数据...")
    merged_df = pd.concat(dataframes, ignore_index=True, sort=False)
    
    original_rows = len(merged_df)
    print(f"合并后总行数: {original_rows}")
    
    # 统计列信息
    all_columns = set()
    for df in dataframes:
        all_columns.update(df.columns)
    
    print(f"所有字段总数: {len(all_columns)}")
    
    # 重新排列列，保持原始顺序（尽可能）
    final_columns = []
    # 先添加第一个文件的列顺序
    if dataframes:
        first_df_columns = list(dataframes[0].columns)
        final_columns.extend(first_df_columns)
    
    # 添加其他文件中新出现的列
    for column in all_columns:
        if column not in final_columns:
            final_columns.append(column)
    
    merged_df = merged_df[final_columns]
    
    # 去重
    if deduplicate:
        print("\n正在去除重复数据...")
        if key_columns:
            # 检查关键列是否存在
            available_key_columns = [col for col in key_columns if col in merged_df.columns]
            if available_key_columns:
                duplicates_before = merged_df.duplicated(subset=available_key_columns).sum()
                merged_df = merged_df.drop_duplicates(subset=available_key_columns, keep='first')
                print(f"基于列 {available_key_columns} 去重，移除 {duplicates_before} 个重复行")
            else:
                print("警告: 指定的关键列不存在，使用所有列去重")
                duplicates_before = merged_df.duplicated().sum()
                merged_df = merged_df.drop_duplicates()
                print(f"使用所有列去重，移除 {duplicates_before} 个重复行")
        else:
            duplicates_before = merged_df.duplicated().sum()
            merged_df = merged_df.drop_duplicates()
            print(f"使用所有列去重，移除 {duplicates_before} 个重复行")
    
    final_rows = len(merged_df)
    
    # 生成统计信息
    stats = {
        'input_files': len(csv_files),
        'successful_files': len(dataframes),
        'original_rows': original_rows,
        'final_rows': final_rows,
        'removed_duplicates': original_rows - final_rows if deduplicate else 0,
        'total_columns': len(all_columns),
        'file_stats': file_stats,
        'column_analysis': {}
    }
    
    # 列分析
    for column in merged_df.columns:
        non_null_count = merged_df[column].notna().sum()
        unique_count = merged_df[column].nunique()
        stats['column_analysis'][column] = {
            'non_null_count': non_null_count,
            'null_count': len(merged_df) - non_null_count,
            'unique_values': unique_count,
            'data_type': str(merged_df[column].dtype)
        }
    
    # 输出到文件
    if output_file:
        try:
            merged_df.to_csv(output_file, index=False, encoding='utf-8')
            print(f"\n✓ 合并后的数据已保存到: {output_file}")
            stats['output_file'] = output_file
        except Exception as e:
            print(f"✗ 保存失败: {e}")
    
    # 打印汇总统计
    print("\n" + "="*50)
    print("数据汇总统计:")
    print(f"输入文件数: {stats['input_files']}")
    print(f"成功读取文件数: {stats['successful_files']}")
    print(f"原始总行数: {stats['original_rows']}")
    print(f"去重后行数: {stats['final_rows']}")
    print(f"移除重复行: {stats['removed_duplicates']}")
    print(f"总字段数: {stats['total_columns']}")
    print("="*50)
    
    return merged_df, stats

def find_csv_files(directory, pattern=None):
    """
    在目录中查找CSV文件
    
    参数:
    - directory: 目录路径
    - pattern: 文件名模式 (可选)
    
    返回:
    - csv_files: CSV文件路径列表
    """
    if not os.path.exists(directory):
        print(f"错误: 目录不存在 - {directory}")
        return []
    
    csv_files = []
    for file in os.listdir(directory):
        if file.endswith('.csv'):
            if pattern is None or pattern in file:
                csv_files.append(os.path.join(directory, file))
    
    print(f"在 {directory} 中找到 {len(csv_files)} 个CSV文件")
    return csv_files

# 使用示例
if __name__ == "__main__":
    # 方法1: 直接指定CSV文件列表
    # csv_files = [
    #     "F:\\test\\untested_simulator2.csv",
    #     "F:\\test\\sensor_test_input_batch_1.csv" 
    #     # "D:\\NKU\\Work\\Work2\\appchina_output\\log.csv",
    #     # "D:\\NKU\\Work\\Work2\\datasets\\aligned_log.csv"
    # ]
    csv_files = [
        r"/Users/noname/noname/nku/intrusiveads/results/round2/root_log.csv",
        r"/Users/noname/noname/nku/intrusiveads/results/round2/log_device.csv",
        r"/Users/noname/noname/nku/intrusiveads/results/round2/log_remote.csv",
    ]
    
    merged_df, stats = merge_csv_files(
        csv_files, 
        "merged_output.csv", 
        key_columns=["app_name"]
    )


    # 方法2: 从目录中查找CSV文件
    # csv_files = find_csv_files("D:\\NKU\\Work\\Work2\\results")
    
    # if csv_files:
    #     # 合并CSV文件
    #     merged_df, stats = merge_csv_files(
    #         csv_files=csv_files,
    #         output_file="D:\\NKU\\Work\\Work2\\merged_results.csv",
    #         deduplicate=True,
    #         key_columns=["app_name", "activity"]  # 可选: 指定去重依据的列
    #     )
        
    #     if merged_df is not None:
    #         # 打印详细的列统计
    #         print("\n字段详细统计:")
    #         for col, info in stats['column_analysis'].items():
    #             print(f"- {col}: {info['non_null_count']} 非空值, {info['unique_values']} 唯一值, 类型: {info['data_type']}")
            
    #         # 保存统计信息到JSON
    #         import json
    #         with open("D:\\NKU\\Work\\Work2\\merge_stats.json", "w", encoding='utf-8') as f:
    #             json.dump(stats, f, ensure_ascii=False, indent=2)
    #         print(f"\n统计信息已保存到: D:\\NKU\\Work\\Work2\\merge_stats.json")