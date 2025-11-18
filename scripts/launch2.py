import os
import csv
import shutil
import threading
import queue
import time
from datetime import datetime

import pandas as pd
import logging

from droidbot import DroidBot
from droidbot.APKDownloader import APKDownloader

root_dir = "F:\\test\\new\\"
INPUT_CSV = os.path.join(root_dir, "sensor_test_input_batch_1.csv")  # list of pending apps
TESTED_LOG_CSV = "D:\\NKU\\Work\\Work2\\merged_validity_results_checked.csv"
LOG_CSV = os.path.join(root_dir, "log2.csv")  # list of tested apps
RUN_LOG_FILE = os.path.join(root_dir, "run2.log")  # log_file_path
# log_file_path = os.path.join(root_dir, "run2.log")


# download_dir = "F:\\test\\apks\\"
cur_download_dir = "F:\\test\\new\\apks\\"
new_download_dir = "F:\\test\\apks\\"
download_dir = "D:\\NKU\\Work\\Work2\\datasets\\androzoo\\androzoo_apks"
device_serial = "emulator-5554"  # 改成你自己的设备号
OUTPUT_DIR = "F:\\test\\new\\output\\"

proxies = {
    "http": "http://127.0.0.1:7890",  # clash 默认的 HTTP 代理端口
    "https": "http://127.0.0.1:7890",  # clash 默认的 HTTPS 代理端口
}

FIELDNAMES = [
    "package_name", "sha256", "apk_name", "apk_path", "app_output_dir",
    "year", "size", "contain_ad", "is_downloaded", "is_tested", "issue", "sensor_test_done", "timestamp",
    "device_serial"
]


# def get_tested_apps():
#     """获取已经测试过的应用列表"""
#     tested_apps = set()
#     if os.path.exists(log_file_path):
#         try:
#             with open(log_file_path, 'r', encoding='utf-8') as f:
#                 for line in f:
#                     app_name = line.strip()
#                     if app_name:
#                         tested_apps.add(app_name)
#             print(f"[+] Found {len(tested_apps)} tested apps in run.log")
#         except Exception as e:
#             logging.error(f"Error reading run.log: {e}")
#     return tested_apps
#
# def get_pending_apps(input_csv=INPUT_CSV, download_dir=download_dir):
#     """
#     从输入CSV中读取 is_tested == FALSE 的应用列表
#     返回 [{app_record}, ...] 的列表
#     """
#     if not os.path.exists(input_csv):
#         print(f"[!] Input CSV {input_csv} not found")
#         return []
#
#     df = pd.read_csv(input_csv, dtype=str).fillna("")
#
#     if "is_tested" in df.columns:
#         pending_list = df[df["is_tested"] == "FALSE"].to_dict("records")
#     else:
#         pending_list = df.to_dict("records")
#
#     normalized_list = []
#     for app in pending_list:
#         sha256 = app["sha256"]
#         pkg = app["pkg_name"]
#
#         # apk_path 可能是绝对路径，也可能是文件名
#         apk_path = app.get("apk_path", "")
#         if apk_path and os.path.isabs(apk_path) and os.path.exists(apk_path):
#             real_apk_path = apk_path
#         else:
#             real_apk_path = os.path.join(download_dir, f"{sha256}.apk")
#
#         app["apk_path"] = real_apk_path
#         normalized_list.append(app)
#
#     print(f"[+] Found {len(normalized_list)} pending apps")
#     return normalized_list

def run_droidbot(apk_path, apk_name, device_serial, output_dir, log_file_path):
    """单次运行 DroidBot 并写日志"""
    print(f"[+] Processing {apk_name}: {apk_path}")
    try:
        droidbot = DroidBot(
            app_path=apk_path,
            device_serial=device_serial,
            is_emulator=True,
            output_dir=output_dir,
            env_policy=None,
            policy_name="dfs_ad",
            random_input=False,
            script_path=None,
            event_count=50,
            event_interval=10,
            timeout=750,
            keep_app=False,
            keep_env=True,
            cv_mode=False,
            debug_mode=False,
            profiling_method=None,
            grant_perm=True,
        )
        droidbot.start()
        with open(log_file_path, 'a') as f:
            f.write(apk_name + '\n')
        logging.info(f"Finished processing {apk_name} successfully.")
    except ValueError as ve:
        # 这里能抓到 "EOCD signature not found" 暂时多此一举，是在APP类初始化时报错
        if "EOCD" in str(ve):
            print(f"[!] APK文件损坏: {apk_path}")
            return (False, "bad_apk")
        else:
            print(f"[!] ValueError: {ve}")
        return (False, "")

    except Exception as e:
        print("eeeee")
        print(str(e))
        if "uninstall" in str(e):
            return (False, "bad_apk")
        
        if 'droidbot' in locals().keys():
            droidbot.stop()
        logging.error(f"Failed to process {apk_name}: {e}")
        print(apk_name + " can not use.")

        with open("break.txt", "a") as af:
            af.write(apk_path + " " + apk_path + '\n')
        import traceback
        logging.error(traceback.format_exc())
        traceback.print_exc()
        return (False, str(e))

    return (True, "")


def init_log_csv():
    """
    init_log
    """
    if not os.path.exists(LOG_CSV):
        with open(LOG_CSV, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
            writer.writeheader()
        print("[+] Initialized log.csv")


def write_csv_back(log_file, record, success=None):
    """追加写回 log.csv"""
    # record["timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    # record["device_serial"] = device_serial
    file_exists = os.path.exists(log_file)

    if success is not None:
        record["is_tested"] = success

    with open(log_file, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)

        if not file_exists:
            writer.writeheader()
            print(f"[+] 新建日志文件并写入表头: {log_file}")

        writer.writerow(record)
        print(f"[✔] 记录追加写入 {log_file}: {record.get('apk_name', record.get('package_name', 'unknown'))}")


def update_csv_record(record, csv_file=LOG_CSV):
    import pandas as pd

    # 读取整个 csv
    # if os.path.exists(csv_file):
    #     df = pd.read_csv(csv_file, dtype=str, on_bad_lines='skip').fillna("")
    # else:
    #     df = pd.DataFrame(columns=FIELDNAMES)
    # 检查文件是否存在且不为空
    if os.path.exists(csv_file) and os.path.getsize(csv_file) > 0:
        try:
            df = pd.read_csv(csv_file, dtype=str, on_bad_lines='skip').fillna("")
        except (pd.errors.EmptyDataError, pd.errors.ParserError):
            df = pd.DataFrame(columns=record.keys())
    else:
        df = pd.DataFrame(columns=record.keys())

    # 用 sha256 或 package_name 作为唯一键来查找
    mask = (df["sha256"] == record["sha256"]) | (df["apk_name"] == record["apk_name"])
    if mask.any():
        # 更新已有记录
        for key, value in record.items():
            df.loc[mask, key] = str(value)
    else:
        # 没有就新增
        df = pd.concat([df, pd.DataFrame([record])], ignore_index=True)

    # 写回 csv（覆盖式，但保留所有数据）
    df.to_csv(csv_file, index=False, encoding="utf-8")


def check_utg_validity(app_output_dir):
    """
    检查应用输出目录中是否包含utg.js文件

    Args:
        app_output_dir: 应用输出目录路径

    Returns:
        bool: 如果存在utg.js则返回True，否则返回False
    """
    if not app_output_dir or not os.path.exists(app_output_dir):
        return False

    utg_path = os.path.join(app_output_dir, "utg.js")
    return os.path.exists(utg_path)


def update_input_csv_from_log_robust(input_csv=INPUT_CSV, log_csv=LOG_CSV, backup=True, add_missing_records=True):
    """
    从LOG_CSV中查找已测试的应用，并更新INPUT_CSV中的is_tested字段
    包含备份、更多检查和自动添加缺失记录功能
    """
    try:
        # 备份原文件
        if backup and os.path.exists(input_csv):
            backup_file = input_csv.replace(".csv", f"_backup_{int(time.time())}.csv")
            shutil.copy2(input_csv, backup_file)
            print(f"[+] 已备份原文件: {backup_file}")

        # 读取INPUT_CSV
        if not os.path.exists(input_csv):
            print(f"[!] 输入CSV文件不存在: {input_csv}")
            return False

        if os.path.getsize(input_csv) == 0:
            print(f"[!] 输入CSV文件为空: {input_csv}")
            return False

        input_df = pd.read_csv(input_csv, dtype=str).fillna("")
        print(f"[+] 读取输入CSV: {input_csv}, 共 {len(input_df)} 条记录")

        # 确保必要的列存在
        required_columns = ["is_tested", "apk_path", "sha256", "package_name", "app_name", "app_output_dir"]
        for col in required_columns:
            if col not in input_df.columns:
                input_df[col] = ""

        # 读取LOG_CSV
        if not os.path.exists(log_csv):
            print(f"[!] 日志CSV文件不存在: {log_csv}")
            return False

        if os.path.getsize(log_csv) == 0:
            print(f"[!] 日志CSV文件为空: {log_csv}")
            return False

        log_df = pd.read_csv(log_csv, dtype=str).fillna("")
        print(f"[+] 读取日志CSV: {log_csv}, 共 {len(log_df)} 条记录")

        # 确保日志CSV有必要的列
        if "is_tested" not in log_df.columns:
            print(f"[!] 日志CSV中没有is_tested列")
            return False

        # 筛选已测试的应用
        tested_mask = log_df["is_tested"].str.upper().isin(["TRUE", "T", "1", "YES", "Y"])
        tested_df = log_df[tested_mask]

        if tested_df.empty:
            print(f"[!] 日志CSV中没有已测试的应用")
            return False

        print(f"[+] 在日志CSV中找到 {len(tested_df)} 个已测试应用")

        # 统计变量
        stats = {
            "updated": 0,
            "already_updated": 0,
            "no_match": 0,
            "added": 0,
            "invalid_utg": 0
        }

        # 用于跟踪已处理的标识符，避免重复
        processed_identifiers = set()

        # 更新INPUT_CSV中的记录
        for idx, row in tested_df.iterrows():
            apk_path = row.get("apk_path", "")
            sha256 = row.get("sha256", "")
            package_name = row.get("package_name", "")
            app_output_dir = row.get("app_output_dir", "")

            # 生成唯一标识符
            identifier = f"{apk_path}|{sha256}|{package_name}"

            # 跳过没有有效标识符的记录
            if not apk_path and not sha256 and not package_name:
                continue

            # 检查是否已处理过（避免重复）
            if identifier in processed_identifiers:
                continue

            processed_identifiers.add(identifier)

            # 检查UTG.js有效性
            if app_output_dir and not check_utg_validity(app_output_dir):
                stats["invalid_utg"] += 1
                print(f"[!] 无效的测试结果: {app_output_dir} 中缺少utg.js")
                continue

            # 在INPUT_CSV中查找匹配的记录
            match_mask = pd.Series([False] * len(input_df))

            if apk_path:
                match_mask = match_mask | (input_df["apk_path"] == apk_path)

            if sha256:
                match_mask = match_mask | (input_df["sha256"] == sha256)

            if package_name:
                match_mask = match_mask | (input_df["package_name"] == package_name)

            # 更新匹配的记录
            if match_mask.any():
                for match_idx in input_df[match_mask].index:
                    current_is_tested = input_df.loc[match_idx, "is_tested"]
                    if current_is_tested.upper() in ["TRUE", "T", "1", "YES", "Y"]:
                        stats["already_updated"] += 1
                    else:
                        input_df.loc[match_idx, "is_tested"] = "TRUE"

                        # 可选：更新其他字段
                        update_columns = ["apk_path", "sha256", "apk_name", "app_output_dir",
                                          "year", "size", "contain_ad", "sensor_test_done", "timestamp"]
                        for col in update_columns:
                            if col in row.index and col in input_df.columns and pd.notna(row[col]) and row[col] != "":
                                input_df.loc[match_idx, col] = row[col]

                        stats["updated"] += 1
                        print(f"[+] 更新记录: {input_df.loc[match_idx, 'package_name']}")
            else:
                stats["no_match"] += 1
                print(f"[!] 未找到匹配记录: {package_name} (apk_path: {apk_path}, sha256: {sha256})")

                # 如果启用了添加缺失记录功能，且测试结果有效
                if add_missing_records and app_output_dir and check_utg_validity(app_output_dir):
                    # 创建新记录
                    new_record = {}

                    # 从现有行复制可用字段
                    for col in input_df.columns:
                        if col in row.index and pd.notna(row[col]) and row[col] != "":
                            new_record[col] = row[col]
                        else:
                            new_record[col] = ""

                    # 设置基本字段
                    new_record["is_tested"] = "TRUE"

                    # 如果app_name为空，使用文件夹名
                    if not new_record.get("app_name") and app_output_dir:
                        folder_name = os.path.basename(app_output_dir.rstrip(os.sep))
                        new_record["app_name"] = folder_name

                    # 确保必要的字段有值
                    if not new_record.get("apk_path") and apk_path:
                        new_record["apk_path"] = apk_path
                    if not new_record.get("sha256") and sha256:
                        new_record["sha256"] = sha256
                    if not new_record.get("package_name") and package_name:
                        new_record["package_name"] = package_name
                    if not new_record.get("app_output_dir") and app_output_dir:
                        new_record["app_output_dir"] = app_output_dir

                    # 添加新记录到DataFrame
                    input_df = pd.concat([input_df, pd.DataFrame([new_record])], ignore_index=True)
                    stats["added"] += 1
                    print(
                        f"[+] 添加新记录: {new_record.get('package_name', 'Unknown')} (app_name: {new_record.get('app_name', 'Unknown')})")
                else:
                    print(f"[!] 跳过添加记录: {package_name} - 测试结果无效或功能未启用")

        # 保存更新后的INPUT_CSV
        input_df.to_csv(input_csv, index=False)

        print(f"\n[+] 更新完成统计:")
        print(f"    - 新标记为已测试: {stats['updated']} 个应用")
        print(f"    - 已经是已测试状态: {stats['already_updated']} 个应用")
        print(f"    - 未找到匹配记录: {stats['no_match']} 个应用")
        print(f"    - 新增记录: {stats['added']} 个应用")
        print(f"    - 无效测试结果(缺少utg.js): {stats['invalid_utg']} 个应用")
        print(f"    - 已保存更新到: {input_csv}")

        return True

    except Exception as e:
        print(f"[!] 更新INPUT_CSV失败: {e}")
        import traceback
        traceback.print_exc()
        return False


# def update_input_csv_from_log_robust(input_csv=INPUT_CSV, log_csv=LOG_CSV, backup=True):
#     """
#     从LOG_CSV中查找已测试的应用，并更新INPUT_CSV中的is_tested字段
#     包含备份和更多检查
#     """
#     try:
#         # 备份原文件
#         if backup and os.path.exists(input_csv):
#             backup_file = input_csv.replace(".csv", f"_backup_{int(time.time())}.csv")
#             shutil.copy2(input_csv, backup_file)
#             print(f"[+] 已备份原文件: {backup_file}")
#
#         # 读取INPUT_CSV
#         if not os.path.exists(input_csv):
#             print(f"[!] 输入CSV文件不存在: {input_csv}")
#             return False
#
#         if os.path.getsize(input_csv) == 0:
#             print(f"[!] 输入CSV文件为空: {input_csv}")
#             return False
#
#         input_df = pd.read_csv(input_csv, dtype=str).fillna("")
#         print(f"[+] 读取输入CSV: {input_csv}, 共 {len(input_df)} 条记录")
#
#         # 确保必要的列存在
#         required_columns = ["is_tested", "apk_path", "sha256", "package_name"]
#         for col in required_columns:
#             if col not in input_df.columns:
#                 input_df[col] = ""
#
#         # 读取LOG_CSV
#         if not os.path.exists(log_csv):
#             print(f"[!] 日志CSV文件不存在: {log_csv}")
#             return False
#
#         if os.path.getsize(log_csv) == 0:
#             print(f"[!] 日志CSV文件为空: {log_csv}")
#             return False
#
#         log_df = pd.read_csv(log_csv, dtype=str).fillna("")
#         print(f"[+] 读取日志CSV: {log_csv}, 共 {len(log_df)} 条记录")
#
#         # 确保日志CSV有必要的列
#         if "is_tested" not in log_df.columns:
#             print(f"[!] 日志CSV中没有is_tested列")
#             return False
#
#         # 筛选已测试的应用
#         tested_mask = log_df["is_tested"].str.upper().isin(["TRUE", "T", "1", "YES", "Y"])
#         tested_df = log_df[tested_mask]
#
#         if tested_df.empty:
#             print(f"[!] 日志CSV中没有已测试的应用")
#             return False
#
#         print(f"[+] 在日志CSV中找到 {len(tested_df)} 个已测试应用")
#
#         # 统计变量
#         stats = {
#             "updated": 0,
#             "already_updated": 0,
#             "no_match": 0
#         }
#
#         # 用于跟踪已处理的标识符，避免重复
#         processed_identifiers = set()
#
#         # 更新INPUT_CSV中的记录
#         for idx, row in tested_df.iterrows():
#             apk_path = row.get("apk_path", "")
#             sha256 = row.get("sha256", "")
#             package_name = row.get("package_name", "")
#
#             # 生成唯一标识符
#             identifier = f"{apk_path}|{sha256}|{package_name}"
#
#             # 跳过没有有效标识符的记录
#             if not apk_path and not sha256 and not package_name:
#                 continue
#
#             # 检查是否已处理过（避免重复）
#             if identifier in processed_identifiers:
#                 continue
#
#             processed_identifiers.add(identifier)
#
#             # 在INPUT_CSV中查找匹配的记录
#             match_mask = pd.Series([False] * len(input_df))
#
#             if apk_path:
#                 match_mask = match_mask | (input_df["apk_path"] == apk_path)
#
#             if sha256:
#                 match_mask = match_mask | (input_df["sha256"] == sha256)
#
#             if package_name:
#                 match_mask = match_mask | (input_df["package_name"] == package_name)
#
#             # 更新匹配的记录
#             if match_mask.any():
#                 for match_idx in input_df[match_mask].index:
#                     current_is_tested = input_df.loc[match_idx, "is_tested"]
#                     if current_is_tested.upper() in ["TRUE", "T", "1", "YES", "Y"]:
#                         stats["already_updated"] += 1
#                     else:
#                         input_df.loc[match_idx, "is_tested"] = "TRUE"
#
#                         # 可选：更新其他字段
#                         update_columns = ["apk_path", "sha256", "apk_name", "app_output_dir",
#                                           "year", "size", "contain_ad", "sensor_test_done", "timestamp"]
#                         for col in update_columns:
#                             if col in row.index and col in input_df.columns and pd.notna(row[col]) and row[col] != "":
#                                 input_df.loc[match_idx, col] = row[col]
#
#                         stats["updated"] += 1
#                         print(f"[+] 更新记录: {input_df.loc[match_idx, 'package_name']}")
#             else:
#                 stats["no_match"] += 1
#                 print(f"[!] 未找到匹配记录: {package_name} (apk_path: {apk_path}, sha256: {sha256})")
#
#         # 保存更新后的INPUT_CSV
#         input_df.to_csv(input_csv, index=False)
#
#         print(f"\n[+] 更新完成统计:")
#         print(f"    - 新标记为已测试: {stats['updated']} 个应用")
#         print(f"    - 已经是已测试状态: {stats['already_updated']} 个应用")
#         print(f"    - 未找到匹配记录: {stats['no_match']} 个应用")
#         print(f"    - 已保存更新到: {input_csv}")
#
#         return True
#
#     except Exception as e:
#         print(f"[!] 更新INPUT_CSV失败: {e}")
#         import traceback
#         traceback.print_exc()
#         return False

def get_tested_apps(run_log, csv_file):
    """
    获取已测试应用列表
    从run.log或其他日志文件中读取已测试的包名
    """
    tested_apps = set()

    # 从run.log读取
    if os.path.exists(run_log):
        with open(run_log, "r", encoding="utf-8") as f:
            for line in f:
                if "package_name" in line:
                    # 解析包名，根据实际日志格式调整
                    parts = line.strip().split()
                    for part in parts:
                        if "package_name" in part:
                            pkg = part.split(":")[1] if ":" in part else part.split("=")[1]
                            tested_apps.add(pkg)
                            break

    # 从CSV文件读取已测试的应用
    # tested_csv_files = ["tested_apps.csv", "tested_apps_final.csv"]
    # tested_csv_files = ["log2.csv"]
    # for csv_file in tested_csv_files:
    if os.path.exists(csv_file):
        try:
            df = pd.read_csv(csv_file, dtype=str).fillna("")
            if "package_name" in df.columns:
                tested_apps.update(df["package_name"].tolist())
            if "sha256" in df.columns:
                tested_apps.update(df["sha256"].tolist())
        except Exception as e:
            print(f"[!] 读取已测试CSV失败 {csv_file}: {e}")

    print(f"[+] 从日志和CSV中读取到 {len(tested_apps)} 个已测试应用")
    return tested_apps


def get_pending_apps(input_csv, tested_apps):
    """
    get list of pending apps from input csv
    """
    if not os.path.exists(input_csv):
        print(f"[!] 输入CSV不存在: {input_csv}")
        return []

    try:
        df = pd.read_csv(input_csv, dtype=str).fillna("")
        print(f"[+] 从 {input_csv} 读取到 {len(df)} 条记录")
    except Exception as e:
        print(f"[+] Failed to read csv. {e}")
        return []

    # 筛选未测试的应用
    if "is_tested" in df.columns:
        pending_mask = ~df["is_tested"].str.upper().isin(["TRUE", "T", "1", "YES", "Y"])
        pending_df = df[pending_mask]
        print(f"[+] 筛选出 {len(pending_df)} 个未测试应用")
    else:
        # pending_df = df
        # print(f"[+] CSV中没有is_tested列，使用所有 {len(pending_df)} 条记录")
        pending_df = df.copy()

    print(f"[+] 初步筛选出 {len(pending_df)} 个未测试应用")

    # pending_list = pending_df.to_dict("records")
    # for app in pending_list:
    # 去除 tested_apps 中已存在的记录
    before_filter = len(pending_df)
    if "package_name" in pending_df.columns:
        pending_df = pending_df[~pending_df["package_name"].isin(tested_apps)]
    if "sha256" in pending_df.columns:
        pending_df = pending_df[~pending_df["sha256"].isin(tested_apps)]

    after_filter = len(pending_df)
    removed = before_filter - after_filter
    print(f"[✔] 从 pending 列表中剔除了 {removed} 个已测试应用，剩余 {after_filter} 个待测试应用")

    pending_list = pending_df.to_dict("records")

    return pending_list


def get_existing_apk_path(pkg, sha256, download_dir, new_download_dir):
    """
    检查APK是否已存在于任意下载目录，若存在有效文件则返回其路径，否则返回None。
    """
    candidate_paths = [
        os.path.join(download_dir, f"{sha256}.apk"),
        os.path.join(download_dir, pkg),
        os.path.join(new_download_dir, f"{sha256}.apk"),
        os.path.join(new_download_dir, pkg)
    ]

    for file_path in candidate_paths:
        if os.path.exists(file_path):
            size = os.path.getsize(file_path)
            if size > 0:
                print(f"[✔] 文件已存在且有效，跳过下载: {os.path.basename(file_path)} ({size / 1024:.1f} KB)")
                return file_path
            else:
                print(f"[!] 检测到空文件，忽略并重新下载: {file_path}")

    return None


# def get_pending_apps(input_csv=INPUT_CSV, download_dir="D:\\NKU\\Work\\Work2\\datasets\\androzoo\\androzoo_apks"):
#     """
#     从输入CSV中读取待测试的应用列表
#     修复输出目录为空的问题
#     """
#     if not os.path.exists(input_csv):
#         print(f"[!] 输入CSV不存在: {input_csv}")
#         return []

#     try:
#         df = pd.read_csv(input_csv, dtype=str).fillna("")
#         print(f"[+] 从 {input_csv} 读取到 {len(df)} 条记录")
#     except Exception as e:
#         print(f"[!] 读取CSV失败: {e}")
#         return []

#     # 筛选未测试的应用
#     if "is_tested" in df.columns:
#         pending_mask = ~df["is_tested"].str.upper().isin(["TRUE", "T", "1", "YES", "Y"])
#         pending_df = df[pending_mask]
#         print(f"[+] 筛选出 {len(pending_df)} 个未测试应用")
#     else:
#         pending_df = df
#         print(f"[+] CSV中没有is_tested列，使用所有 {len(pending_df)} 条记录")

#     pending_list = pending_df.to_dict("records")

#     # 标准化APK路径和输出目录
#     normalized_list = []
#     for app in pending_list:
#         sha256 = app.get("sha256", "")
#         package_name = app.get("package_name", "")
#         apk_name = app.get("apk_name", "")

#         # 标准化APK路径
#         apk_path = app.get("apk_path", "")
#         normalized_path = normalize_apk_path(apk_path, download_dir, sha256, apk_name)
#         app["apk_path"] = normalized_path

#         # 确保输出目录不为空
#         app_output_dir = app.get("app_output_dir", "")
#         if not app_output_dir or app_output_dir.strip() == "":
#             app_output_dir = apk_name if apk_name else f"app_{sha256[:8]}"
#             app["app_output_dir"] = app_output_dir

#         normalized_list.append(app)

#     print(f"[+] 处理完成，共 {len(normalized_list)} 个待测试应用")
#     return normalized_list

def normalize_apk_path(apk_path, download_dir, sha256, apk_name):
    """
    标准化APK路径
    """
    # 如果已经是绝对路径且文件存在
    if apk_path and os.path.isabs(apk_path) and os.path.exists(apk_path):
        return apk_path

    # 如果路径是相对路径，尝试在download_dir中查找
    if apk_path and not os.path.isabs(apk_path):
        potential_path = os.path.join(download_dir, apk_path)
        if os.path.exists(potential_path):
            return potential_path

    # 尝试基于sha256构建路径
    if sha256:
        potential_path = os.path.join(download_dir, f"{sha256}.apk")
        if os.path.exists(potential_path):
            return potential_path

    # 尝试基于apk_name构建路径
    if apk_name:
        potential_path = os.path.join(download_dir, apk_name)
        if os.path.exists(potential_path):
            return potential_path

    # 如果都找不到，返回基于sha256的预期路径
    if sha256:
        return os.path.join(download_dir, f"{sha256}.apk")

    return apk_path  # 返回原始路径（可能不存在）


def is_valid_apk(apk_path):
    """
    检查APK文件是否有效
    """
    try:
        if not os.path.exists(apk_path):
            return False

        # 检查文件大小是否合理
        file_size = os.path.getsize(apk_path)
        if file_size < 1024:  # 小于1KB的文件很可能是损坏的
            return False

        # 简单检查：文件头是否为ZIP格式
        with open(apk_path, 'rb') as f:
            header = f.read(4)
            if header != b'PK\x03\x04':
                return False

        return True
    except Exception:
        return False


'''
test for downloaded apps

def main(max_retries=2, num_downloaders=1, num_testers=1):
    """
    轻量级运行单个浏览器测试已下载的APK
    优先处理已经下载过的APK，使用单线程
    """
    # 初始化日志和CSV
    init_log_csv()

    # 获取已测试和待测试应用列表
    tested_apps = get_tested_apps()
    pending_list = get_pending_apps()

    print(f"[+] 已测试应用: {len(tested_apps)} 个")
    print(f"[+] 待测试应用: {len(pending_list)} 个")

    # 创建测试队列和下载队列
    test_queue = queue.Queue()
    download_queue = queue.Queue()

    # 统计变量
    already_tested_count = 0
    ready_to_test_count = 0
    need_download_count = 0

    # 分类处理应用
    for app in pending_list:
        package_name = app.get("package_name", "")
        sha256 = app.get("sha256", "")
        apk_path = app.get("apk_path", "")

        # 检查是否已经测试过
        if package_name in tested_apps or sha256 in tested_apps:
            already_tested_count += 1
            print(f"[!] 跳过 {package_name} - 已经测试过")
            continue

        # 检查APK是否已下载且有效
        if apk_path and os.path.exists(apk_path) and os.path.getsize(apk_path) > 1 * 1024 * 1024:
            # 验证APK文件完整性
            if is_valid_apk(apk_path):
                # 确保 app_output_dir 不为空
                app_output_dir = app.get("app_output_dir", "")
                if not app_output_dir:
                    # 如果为空，使用包名作为目录名
                    app_output_dir = package_name if package_name else f"app_{sha256[:8]}"
                # 创建测试记录
                record = {
                    "package_name": package_name,
                    "sha256": sha256,
                    "apk_name": app.get("apk_name", ""),
                    "apk_path": apk_path,
                    "app_output_dir": app.get("app_output_dir", package_name),
                    "year": app.get("year", ""),
                    "size": app.get("size", ""),
                    "contain_ad": app.get("contain_ad", "FALSE"),
                    "is_downloaded": "TRUE",
                    "is_tested": "FALSE",
                    "issue": "",
                    "sensor_test_done": app.get("sensor_test_done", "FALSE"),
                    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "device_serial": device_serial
                }
                test_queue.put((package_name, apk_path, record))
                ready_to_test_count += 1
                print(f"[+] 准备测试: {apk_path}")
            else:
                print(f"[!] APK文件损坏: {apk_path}")
                download_queue.put(app)
                need_download_count += 1
        else:
            print(f"[!] APK文件不存在或过小: {apk_path}")
            download_queue.put(app)
            need_download_count += 1

    print(f"\n[+] 分类统计:")
    print(f"    - 已测试跳过: {already_tested_count}")
    print(f"    - 准备测试: {ready_to_test_count}")
    print(f"    - 需要下载: {need_download_count}")

    # 测试工作函数
    def test_worker():
        test_count = 0
        success_count = 0
        fail_count = 0

        while not test_queue.empty():
            try:
                package_name, apk_path, record = test_queue.get_nowait()
            except queue.Empty:
                break

            test_count += 1
            print(f"\n[+] 开始测试 ({test_count}/{ready_to_test_count}): {package_name}")

            success = False
            try:
                # 运行droidbot测试
                output_dir = os.path.join(OUTPUT_DIR, record["app_output_dir"])
                ret = run_droidbot(apk_path, package_name, device_serial, output_dir, log_file_path)
                if ret:
                    success = True
                    success_count += 1
                    print(f"[✓] 测试成功: {apk_path}")
                else:
                    print(f"[X] 测试失败: {apk_path}")
                    logging.error(f"测试失败 {apk_path}")
            except Exception as e:
                logging.error(f"测试失败 {apk_path}: {e}")
                record["issue"] = f"test_failed: {str(e)}"
                fail_count += 1
                print(f"[✗] 测试失败: {apk_path} - {e}")

            # 更新记录状态
            record["is_tested"] = "TRUE" if success else "FALSE"
            record["timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            update_csv_record(record)

            test_queue.task_done()

        print(f"\n[+] 测试完成统计:")
        print(f"    - 总测试: {test_count}")
        print(f"    - 成功: {success_count}")
        print(f"    - 失败: {fail_count}")

    # 下载工作函数
    def download_worker():
        if download_queue.empty():
            print("[+] 没有需要下载的APK")
            return

        download_count = 0
        success_download_count = 0

        print(f"\n[+] 开始下载缺失的APK, 共 {download_queue.qsize()} 个")

        downloader = APKDownloader(
            api_key="89d3f75d8e8117ba89f8b6aba744cf9e62fdc7214c2f2a67a369d463f5c64314",
            download_dir=new_download_dir,
            proxies=proxies
        )

        while not download_queue.empty():
            try:
                app = download_queue.get_nowait()
            except queue.Empty:
                break

            download_count += 1
            sha256 = app.get("sha256", "")
            package_name = app.get("package_name", "")

            print(f"[+] 下载 ({download_count}/{need_download_count}): {package_name} ({sha256})")

            try:
                file_path = downloader.download_apk(sha256)
                if file_path and os.path.exists(file_path) and os.path.getsize(file_path) > 1 * 1024 * 1024:
                    if is_valid_apk(file_path):
                        success_download_count += 1
                        print(f"[✓] 下载成功: {package_name}")

                        # 创建记录并加入测试队列
                        record = {
                            "package_name": package_name,
                            "sha256": sha256,
                            "apk_name": app.get("apk_name", ""),
                            "apk_path": file_path,
                            "app_output_dir": app.get("app_output_dir", package_name),
                            "year": app.get("year", ""),
                            "size": app.get("size", ""),
                            "contain_ad": app.get("contain_ad", "FALSE"),
                            "is_downloaded": "TRUE",
                            "is_tested": "FALSE",
                            "issue": "",
                            "sensor_test_done": app.get("sensor_test_done", "FALSE"),
                            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            "device_serial": device_serial
                        }

                        # 立即测试下载的APK
                        print(f"[+] 立即测试下载的APK: {package_name}")
                        try:
                            output_dir = os.path.join(OUTPUT_DIR, record["app_output_dir"])
                            ret = run_droidbot(file_path, package_name, device_serial, output_dir, log_file_path)
                            if ret:
                                record["is_tested"] = "TRUE"
                                print(f"[✓] 测试成功: {package_name}")
                            else:
                                print(f"[X] 测试失败: {package_name}")
                                logging.error(f"测试失败 {package_name}: {e}")
                        except Exception as e:
                            logging.error(f"测试失败 {package_name}: {e}")
                            record["issue"] = f"test_failed: {str(e)}"
                            record["is_tested"] = "FALSE"
                            print(f"[✗] 测试失败: {package_name} - {e}")

                        update_csv_record(record)
                    else:
                        print(f"[!] 下载的APK损坏: {file_path}")
                else:
                    print(f"[!] 下载失败: {package_name}")
            except Exception as e:
                logging.error(f"下载失败 {sha256}: {e}")
                print(f"[!] 下载异常: {package_name} - {e}")

            download_queue.task_done()

        print(f"\n[+] 下载完成统计:")
        print(f"    - 总下载: {download_count}")
        print(f"    - 成功: {success_download_count}")
        print(f"    - 失败: {download_count - success_download_count}")

    # 单线程执行：先测试已下载的APK，然后下载并测试缺失的APK
    print("\n" + "=" * 50)
    print("阶段1: 测试已下载的APK")
    print("=" * 50)

    test_worker()

    print("\n" + "=" * 50)
    print("阶段2: 下载并测试缺失的APK")
    print("=" * 50)

    download_worker()

    print("\n[+] 所有任务完成!")
'''


# def main(max_retries=2, num_downloaders=1, num_testers=1):
#     init_log_csv()
#     tested_apps = get_tested_apps()
#     pending_list = get_pending_apps()
#
#     downloader = APKDownloader(
#         api_key="",
#         download_dir=download_dir,
#         proxies=proxies
#     )
#
#     test_queue = queue.Queue()
#     download_queue = queue.Queue()
#
#     for app in pending_list:
#         sha256 = app["sha256"]
#         pkg = app["pkg_name"]
#         apk_path = os.path.join(download_dir, f"{sha256}.apk")
#
#         # 检查是否已经在run.log中测试过
#         if pkg in tested_apps:
#             print(f"[!] Skipping {pkg} - already tested in run.log")
#             continue
#
#         if os.path.exists(apk_path) and os.path.getsize(apk_path) > 1 * 1024 * 1024:
#             record = {
#                 "package_name": app["pkg_name"],
#                 "sha256": app["sha256"],
#                 "apk_name": app["pkg_name"],
#                 "apk_path": apk_path,
#                 "app_output_dir": app["pkg_name"],
#                 "year": app.get("year", ""),
#                 "size": app.get("apk_size", ""),
#                 "contain_ad": False,
#                 "is_downloaded": True,
#                 "is_tested": False,
#                 "issue": "",
#                 "sensor_test_done": False,
#                 "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
#                 "device_serial": device_serial
#             }
#             test_queue.put((app["pkg_name"], apk_path, record))
#         else:
#             print(f"[!] Skipping {pkg} - file not exist")
#             download_queue.put((app))
#
#     def test_worker():
#         while not test_queue.empty():
#             apk_name, apk_path, record = test_queue.get()
#             success = False
#             try:
#                 run_droidbot(apk_path, apk_name, device_serial, output_dir, log_file_path)
#                 success = True
#             except Exception as e:
#                 logging.error(f"Test failed for {apk_name}: {e}")
#                 record["issue"] = "test_failed"
#             record["is_tested"] = "TRUE"
#             update_csv_record(record)
#             # write_csv_back(record)
#             test_queue.task_done()
#
#     # 启动一个测试线程（也可以开多个）
#     t = threading.Thread(target=test_worker, daemon=True)
#     t.start()
#     test_queue.join()
#     t.join()
#
#     # Step 3: 测试结束后再处理下载
#     if not download_queue.empty():
#         print(f"[+] Need to download {download_queue.qsize()} missing/broken apks")
#         downloader = APKDownloader(
#             api_key="",
#             download_dir=download_dir,
#             proxies=proxies
#         )
#         while not download_queue.empty():
#             app = download_queue.get()
#             sha256 = app["sha256"]
#             try:
#                 file_path = downloader.download_apk(sha256)
#                 if file_path and os.path.exists(file_path):
#                     print(f"[+] Re-downloaded {sha256}")
#                 else:
#                     print(f"[!] Failed to download {sha256}")
#             except Exception as e:
#                 logging.error(f"Download failed for {sha256}: {e}")
#             download_queue.task_done()

def main_single_thread(max_retries=2):
    """
    单线程版本：按顺序执行下载、安装、测试并更新 log.csv
    """
    init_log_csv()  # read and wait for updating?
    tested_apps = get_tested_apps(RUN_LOG_FILE, TESTED_LOG_CSV)
    pending_list = get_pending_apps(INPUT_CSV, tested_apps)

    downloader = APKDownloader(
        api_key="89d3f75d8e8117ba89f8b6aba744cf9e62fdc7214c2f2a67a369d463f5c64314",
        download_dir=cur_download_dir,
        proxies=proxies
    )

    for app in pending_list:
        pkg = app["package_name"]
        sha256 = app["sha256"]
        existing_apk_path = app.get("apk_path", "")
        apk_path = os.path.join(cur_download_dir, f"{sha256}.apk")
        is_downloaded = app["is_downloaded"]
        app_output_dir = os.path.join(OUTPUT_DIR, pkg)

        # 跳过已测试
        if pkg in tested_apps:
            print(f"[!] Skipping {pkg} - already tested")
            continue

        # 跳过已下载
        # existing_path = get_existing_apk_path(pkg, sha256, download_dir, new_download_dir)

        # if existing_path:
        #     apk_path = existing_path  # 跳过下载
        #     is_downloaded = True
        # else:
            # apk_path = os.path.join(download_dir, f"{sha256}.apk")

        record = {
            "package_name": pkg,
            "sha256": sha256,
            "apk_name": pkg,
            "apk_path": apk_path,
            "app_output_dir": pkg,
            "year": app.get("year", ""),
            "size": app.get("apk_size", ""),
            "contain_ad": False,
            "is_downloaded": "0",
            "is_tested": "0",
            "issue": "",
            "timestamp": None,
            "device_serial": device_serial,
            "sensor_test_done": False,
        }

        # =============== 1️⃣ 下载阶段 ===============
        if not is_downloaded:
            try:
                print(f"[*] Downloading {pkg} ...")
                file_path = downloader.download_apk(sha256)
                if not file_path or not os.path.exists(file_path):
                    record["issue"] = "download_failed"
                    write_csv_back(LOG_CSV, record)
                    continue
                record["is_downloaded"] = "1"
                record["apk_path"] = file_path
                print(f"[+] Downloaded: {pkg} ({os.path.getsize(file_path) / 1024 / 1024:.2f} MB)")
            except Exception as e:
                record["issue"] = f"download_error: {e}"
                write_csv_back(LOG_CSV, record)
                continue
        else:
            record["is_downloaded"] = "1"
            record["apk_path"] = existing_apk_path
            file_path = existing_apk_path
            print(f"[+] Existing downloaded: {pkg} ({os.path.getsize(file_path) / 1024 / 1024:.2f} MB)")

        # =============== 2️⃣ 测试阶段 ===============
        success = "0"
        for attempt in range(max_retries):
            try:
                print(f"[*] Installing and testing {pkg} (attempt {attempt + 1})...")
                # output_dir = os.path.join(OUTPUT_DIR, record["app_output_dir"])
                os.makedirs(app_output_dir, exist_ok=True)

                # 确保设备可用
                if not device_serial:
                    raise RuntimeError("Device serial not set or device not connected")

                # 调用测试
                test_result, info = run_droidbot(file_path, pkg, device_serial, app_output_dir, RUN_LOG_FILE)
                if test_result and info == "bad_apk":
                    success = "0"
                    print(f"[!] APK 文件损坏，删除并重新下载: {file_path}")
                    record["is_downloaded"] = "0"
                    try:
                        if os.path.exists(file_path):
                            os.remove(file_path)
                    except Exception as del_err:
                        print(f"删除损坏文件失败: {del_err}")
                    
                    # 重新下载APK
                    file_path = downloader.download_apk(sha256)
                    if not file_path:
                        print(f"[x] {pkg} 重新下载失败，跳过。")
                        break
                    time.sleep(2)  # 稍等一下再测试
                    record["is_downloaded"] = "1"
                elif test_result:
                    success = "1"
                    print(f"[✓] Test finished for {pkg}")
                    break
                else:
                    record["issue"] = "test_failed"
            except Exception as e:
                record["issue"] = f"test_failed: {e}"
                print(f"[×] Test failed for {pkg}: {e}")
                time.sleep(3)

        record["is_tested"] = success
        record["timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        write_csv_back(LOG_CSV, record)

        # 可选：测试成功后释放存储空间
        # if success and os.path.exists(file_path):
        #     os.remove(file_path)


def main(max_retries=2, num_downloaders=1, num_testers=1):
    init_log_csv()
    tested_apps = get_tested_apps()
    pending_list = get_pending_apps(INPUT_CSV)

    downloader = APKDownloader(
        api_key="89d3f75d8e8117ba89f8b6aba744cf9e62fdc7214c2f2a67a369d463f5c64314",
        download_dir=cur_download_dir,
        proxies=proxies
    )

    test_queue = queue.Queue()
    download_queue = queue.Queue()

    # df = pd.read_csv(INPUT_CSV, dtype=str).fillna("")
    # if "is_tested" in df.columns:
    #     pending_list = df[df["is_tested"] == "FALSE"].to_dict("records")
    # else:
    #     pending_list = df.to_dict("records")

    stop_flag = threading.Event()

    need_download, downloaded = [], []
    for app in pending_list:
        sha256 = app["sha256"]
        pkg = app["package_name"]
        apk_path = os.path.join(download_dir, f"{sha256}.apk")

        # 检查是否已经在run.log中测试过
        if pkg in tested_apps:
            print(f"[!] Skipping {pkg} - already tested in run.log")
            continue

        if os.path.exists(apk_path) and os.path.getsize(apk_path) > 1 * 1024 * 1024:
            downloaded.append((app, apk_path))
        else:
            need_download.append((app, apk_path))

    for app, apk_path in downloaded:
        record = {
            "package_name": app["package_name"],
            "sha256": app["sha256"],
            "apk_name": app["package_name"],
            "apk_path": apk_path,
            "app_output_dir": app["package_name"],
            "year": app.get("year", ""),
            "size": app.get("apk_size", ""),
            "contain_ad": False,
            "is_downloaded": True,
            "is_tested": False,
            "issue": "",
            "sensor_test_done": False,
        }
        test_queue.put((app["package_name"], apk_path, record))

    def download_worker_loop():
        while True:
            item = download_queue.get()
            if item is None:
                download_queue.task_done()
                break
            apk_info, apk_path = item
            sha256 = apk_info["sha256"]
            pkg = apk_info["package_name"]

            # 再次检查是否已经在run.log中测试过（防止在下载过程中被其他线程测试）
            current_tested_apps = get_tested_apps()

            if pkg in current_tested_apps:
                print(f"[!] Skipping {pkg} - already tested during download process")
                download_queue.task_done()
                continue

            record = {
                "package_name": pkg,
                "sha256": sha256,
                "apk_name": pkg,
                "apk_path": apk_path,
                "app_output_dir": pkg,
                "year": apk_info.get("year", ""),
                "size": apk_info.get("apk_size", ""),
                "contain_ad": False,
                "is_downloaded": False,
                "is_tested": False,
                "issue": "",
                "sensor_test_done": False
            }
            try:
                file_path = downloader.download_apk(sha256)
                if file_path and os.path.exists(file_path):
                    record["is_downloaded"] = True
                    write_csv_back(LOG_CSV, record)
                    test_queue.put((pkg, file_path, record))
                else:
                    record["issue"] = "download_failed"
                    write_csv_back(LOG_CSV, record)
            except Exception as e:
                logging.error(f"Download failed for {pkg}: {e}")
            finally:
                download_queue.task_done()

    def test_worker():
        while True:
            item = test_queue.get()
            if item is None:
                test_queue.task_done()
                break
            apk_name, apk_path, record = item

            # 最终检查是否已经在run.log中测试过
            current_tested_apps = get_tested_apps()
            if apk_name in current_tested_apps:
                print(f"[!] Skipping {apk_name} - already tested before execution")
                test_queue.task_done()
                continue

            success = False
            for attempt in range(max_retries):
                try:
                    output_dir = os.path.join(OUTPUT_DIR, record["app_output_dir"])
                    run_droidbot(apk_path, apk_name, device_serial, output_dir, log_file_path)
                    success = True
                    break
                except Exception as e:
                    logging.error(f"Test failed for {apk_name} attempt {attempt + 1}: {e}")

            record["is_tested"] = success

            if not success:
                record["issue"] = "test_failed"

            write_csv_back(record, success)
            test_queue.task_done()

    for app, apk_path in need_download:
        download_queue.put((app, apk_path))

    download_threads = []
    for _ in range(num_downloaders):
        t = threading.Thread(target=download_worker_loop, daemon=True)
        t.start()
        download_threads.append(t)

    test_threads = []
    for _ in range(num_testers):
        t = threading.Thread(target=test_worker, daemon=True)
        t.start()
        test_threads.append(t)

    download_queue.join()
    for _ in range(num_downloaders):
        download_queue.put(None)
    for t in download_threads:
        t.join()

    test_queue.join()
    for _ in range(num_testers):
        test_queue.put(None)
    for t in test_threads:
        t.join()


if __name__ == "__main__":
    if ':' in device_serial:
        os.system("adb connect " + device_serial)

    # main()
    main_single_thread(max_retries=2)
    # update_input_csv_from_log_robust(
    #      input_csv=INPUT_CSV,
    #      log_csv=LOG_CSV,
    #      backup=True
    # )