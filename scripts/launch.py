import csv
import glob
import os
import logging
import json
import queue
import shutil
import tempfile
import time
import re
import sys
import threading
from copy import deepcopy
from pathlib import Path
from queue import Queue
from typing import List, Optional, Dict

import pandas as pd
import requests

from droidbot.APKDownloader import logger, APKDownloader
from droidbot.droidbot import DroidBot
from droidbot.config import PathConfig

from concurrent.futures import ThreadPoolExecutor, as_completed

from droidbot.utils import read_csv_as_dicts, find_paths_json, ensure_dir, write_csv, write_csv_back

# basic configuration
config = PathConfig()

# app info
# apk_name = "125_66a1751ab8e27f6dcfbe3d5eb1b7b780"
# apk_name = "1A31DF27649623C8E46396BD02B58619E0901581FA42E39B01CF0145850C8599.apk"
# apk_name = "3_1DBA386071F68874AE26CC95CEAD657BF57F9CD4F33D7CF4DC43328E4027035A.apk"
# apk_name = "7_Colorful-Widget.apk"
# apk_name = "4_BC681FF7B2E05C9FD63ED69874979E8F580F2E367E2011465D9F76A78DB0C18F.apk"
# apk_name = "8_com.cssq.sbrowser.apk"
# apk_name = "com.sh.feichang.remote.control"

# apk_name = "com.robinvanrodeman.bald_eagle"
# if apk_name.endswith(".apk"):
#    apk_name = apk_name[:-4]

# environment info
# app_dir = "D:\\NKU\\Work\\Work2\\datasets\\manual_analysis\\"
app_dir = config.APP_DIR
# output_dir = "D:\\NKU\\Work\\Work2\\datasets\\manual_analysis\\test_output\\"
output_dir = config.OUTPUT_DIR

# output_dir = "D:\\NKU\\Work\\Work2\\datasets\\chin\\output\\"
# 配置日志文件
log_file_path = os.path.join(output_dir, "androzoo_droidbot_process.log")
fraudulent_log_file_path = os.path.join(output_dir, 'fraudulent_processed_apks.txt')

# if not os.path.exists(log_file_path):
#     # 创建日志文件
#     open(log_file_path, 'w').close()
#
# logging.basicConfig(
#     filename=log_file_path,
#     filemode='a',
#     level=logging.INFO,
#     format='%(asctime)s [%(levelname)s] %(message)s',
#     datefmt='%Y-%m-%d %H:%M:%S'
# )

# device info
# device_serial = "emulator-5554"
device_serial = "adb-352fb9d8-Crugp0._adb-tls-connect._tcp"
# device_serial = "192.168.31.162:1111"
# flags
bunch_test = False
proxies = {
    "http": "http://127.0.0.1:7890",  # clash 默认的 HTTP 代理端口
    "https": "http://127.0.0.1:7890",  # clash 默认的 HTTPS 代理端口
}

# download_dir = "D:\\NKU\Work\Work2\datasets\\ad_dataset\\chin"

download_dir = config.DOWNLOAD_DIR

# def main():
#
#     try:
#         droidbot = DroidBot(
#             app_path=os.path.join(app_dir, apk_name + ".apk"),
#             #app_path=apk_path,
#             device_serial=device_serial,
#             is_emulator=True,
#             output_dir=os.path.join(output_dir, apk_name),
#             env_policy=None,
#             policy_name="dfs_ad",
#             # policy_name="dfs_greedy",
#             random_input=False,
#             script_path=None,
#             event_count=50,
#             event_interval=10,
#             timeout=750,
#             keep_app=False,
#             keep_env=True,
#             cv_mode=False,
#             debug_mode=False,
#             profiling_method=None,
#             grant_perm=True,
#         )
#         droidbot.start()
#     except:
#         if 'droidbot' in locals().keys():
#             droidbot.stop()
#         print(apk_name + " can not use.")
#         import traceback
#         traceback.print_exc()

import random


def deduplicate_by_pkg(apk_list):
    result = {}
    for apk in apk_list:
        pkg = apk["pkg_name"]
        if pkg not in result:
            result[pkg] = []
        result[pkg].append(apk)
    # 每个 pkg 只保留一个（随机）
    deduped = [random.choice(v) for v in result.values()]
    return deduped


"""
def main():
    # 设置日志
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler("droidbot_test.log"),
            logging.StreamHandler()
        ]
    )

    os.makedirs(output_dir, exist_ok=True)

    # 读取已测试的APK记录
    tested_apks = set()
    if os.path.exists(log_file_path):
        with open(log_file_path, 'r', encoding='utf-8') as f:
            tested_apks = set(line.strip() for line in f if line.strip())

    # 获取所有APK文件
    apk_files = []
    for filename in os.listdir(download_dir):
        if filename.endswith(".apk"):
            apk_path = os.path.join(download_dir, filename)
            # 这里可以添加获取SHA256的逻辑，如果需要的话
            apk_files.append({
                "path": apk_path,
                "name": filename[:-4]  # 移除.apk后缀
            })


    # 从日志中提取已成功处理的 apk_name
    tested_apks_by_log = set()
    if os.path.exists(log_file_path):
        with open(log_file_path, 'r', encoding='utf-8') as f:
            # tested_apks_by_log = set(f.read().splitlines())
            tested_apks_by_log = set(line.strip() for line in f if line.strip())

    new_apks = [apk for apk in apk_files if apk["name"] not in tested_apks]
    logging.info(f"Found {len(new_apks)} new APKs to test.")

    # 测试每个新APK
    for apk in new_apks:
        apk_path = apk["path"]
        apk_name = apk["name"]

        try:
            logging.info(f"Testing APK: {apk_name}")

            droidbot = DroidBot(
                app_path=apk_path,
                device_serial=device_serial,
                is_emulator=True,
                output_dir=os.path.join(output_dir, apk_name),
                env_policy=None,
                policy_name="dfs_ad",
                random_input=False,
                event_count=20,
                event_interval=10,
                timeout=750,
                keep_app=False,
                keep_env=True,
                cv_mode=False,
                debug_mode=False,
                grant_perm=True,
            )

            droidbot.start()
            droidbot.stop()

            # 记录成功测试的APK
            with open(log_file_path, 'a', encoding='utf-8') as f:
                f.write(apk_name + '\n')

            logging.info(f"Successfully tested: {apk_name}")

        except Exception as e:
            logging.error(f"Failed to test {apk_name}: {str(e)}")
            # 确保在异常时停止droidbot
            if 'droidbot' in locals() and droidbot:
                droidbot.stop()
            continue

"""

def run_droidbot(apk_path, apk_name, device_serial, output_dir, log_file_path):
    """单次运行 DroidBot 并写日志"""
    print(f"[+] Processing {apk_name}: {apk_path}")
    try:
        droidbot = DroidBot(
            app_path=apk_path,
            device_serial=device_serial,
            is_emulator=True,
            output_dir=os.path.join(output_dir, apk_name),
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
    except Exception as e:
        if 'droidbot' in locals().keys():
            droidbot.stop()
        logging.error(f"Failed to process {apk_name}: {e}")
        print(apk_name + " can not use.")
        with open("break.txt", "a") as af:
            af.write(apk_path + " " + apk_path + '\n')
        import traceback
        logging.error(traceback.format_exc())
        traceback.print_exc()


def run_sensor_test(
        apk_path,
        apk_name,
        app_output_dir,
        device_serial,
        output_dir,
        path_file,
        log_csv_path,
        policy_name="sensor_policy",
        inject_method="adb",
        inject_point="before_ad",
        event_interval=5,
        event_count_extra=10,
        timeout=600,
        max_paths=None,
        env_overrides=None):
    """
    完整的 run_sensor_test 主流程：
    - paths: List[List[Dict]]，来自 extract_paths_to_ads
    - 该函数会把 paths 写成 JSON，设置环境变量，然后启动 DroidBot（policy_name 指向 SensorPolicy）
    - 记录 summary CSV 到 log_csv_path

    运行 DroidBot 测试 sensor 相关 API
    - apk_path: 待测 APK 文件路径
    - apk_name: APK 名称
    - device_serial: adb device 序列号（如 emulator-5554）
    - output_dir: 输出目录
    - path_sequence: 广告路径 (list of states / events)
    - log_csv_path: CSV 日志文件
    """
    print(f"[+] Starting sensor test for {apk_name}: {apk_path}")

    # 1) prepare output dir and paths.json
    #apk_output_dir = os.path.join(output_dir, apk_name)
    os.makedirs(app_output_dir, exist_ok=True)

    # if max_paths is not None:
    #     paths_to_write = path_sequence[:max_paths]
    # else:
    #     paths_to_write = path_sequence

    # paths_file = os.path.join(apk_output_dir, f"{apk_name}_paths.json")
    # with open(paths_file, "w", encoding="utf-8") as f:
    #     json.dump(paths_to_write, f, ensure_ascii=False, indent=2)
    # 2) load path_file
    with open(path_file, "r", encoding="utf-8") as f:
        path_sequence = json.load(f)

    if max_paths is not None:
        paths_to_write = path_sequence[:max_paths]
    else:
        paths_to_write = path_sequence

    # 把路径写入 apk_output_dir 下，供 SensorPolicy 读取
    paths_file = os.path.join(app_output_dir, f"{apk_name}_paths.json")
    with open(paths_file, "w", encoding="utf-8") as f:
        json.dump(paths_to_write, f, ensure_ascii=False, indent=2)

    # 2) 设置环境变量，供 SensorPolicy 读取
    # 主要 env:
    #   SENSOR_PATHS_FILE -> paths_file
    #   SENSOR_INJECT_METHOD -> inject_method
    #   SENSOR_INJECT_POINT -> inject_point
    os.environ["SENSOR_PATHS_FILE"] = paths_file
    os.environ["SENSOR_INJECT_METHOD"] = inject_method
    os.environ["SENSOR_INJECT_POINT"] = inject_point
    os.environ["SENSOR_EVENT_INTERVAL"] = str(event_interval)

    if env_overrides:
        for k, v in env_overrides.items():
            os.environ[k] = v

    # 3) 计算 event_count: paths 总事件数 + buffer
    total_event_count = sum(len(p) for p in paths_to_write)
    # 加入额外事件数量以保证 sensor 注入有机会运行
    total_event_count += event_count_extra
    # 最低限制
    total_event_count = max(total_event_count, 20)

    # 4) 启动 DroidBot
    print(f"[+] Running sensor test for {apk_name}, paths_file={paths_file}, inject_method={inject_method}")
    start_time = time.time()
    try:
        # DroidBot 配置
        droidbot = DroidBot(
            app_path=apk_path,
            device_serial=device_serial,
            is_emulator=True,
            output_dir=app_output_dir,
            env_policy=None,
            policy_name="sensor_policy",  # 你要新写的 InputPolicy 名称
            random_input=False,
            script_path=None,
            event_count=total_event_count,  # 事件数：路径长度 + sensor 测试
            event_interval=5,
            timeout=600,
            keep_app=False,
            keep_env=True,
            cv_mode=False,
            debug_mode=False,
            profiling_method=None,
            grant_perm=True,
            custom_path=path_sequence
        )

        # 在这里你需要在 input_policy 中利用 path_sequence 重放 UI 动作，
        # 到广告节点时再注入 SensorEvent（policy_violation）
        droidbot.start()

        # 5) 写入 summary CSV 日志
        duration = time.time() - start_time
        file_exists = os.path.isfile(log_csv_path)
        with open(log_csv_path, "a", newline="") as csvfile:
            writer = csv.writer(csvfile)
            if not file_exists:
                writer.writerow(
                    ["timestamp", "apk_name", "apk_path", "device_serial", "status", "paths_used", "inject_method",
                     "inject_point", "duration_s", "paths_file"])
            writer.writerow(
                [time.time(), apk_name, apk_path, device_serial, "success", len(paths_to_write), inject_method,
                 inject_point, f"{duration:.1f}", paths_file])

        logging.info(f"[+] Finished sensor test for {apk_name} successfully.")

    except Exception as e:
        # 若出现异常，确保停止 droidbot
        try:
            if 'droidbot' in locals() and droidbot is not None:
                droidbot.stop()
        except Exception:
            pass

        duration = time.time() - start_time
        file_exists = os.path.isfile(log_csv_path)
        with open(log_csv_path, "a", newline="") as csvfile:
            writer = csv.writer(csvfile)
            if not file_exists:
                writer.writerow(
                    ["timestamp", "apk_name", "apk_path", "device_serial", "status", "paths_used", "inject_method",
                     "inject_point", "duration_s", "paths_file"])
            writer.writerow(
                [time.time(), apk_name, apk_path, device_serial, f"failed: {e}", len(paths_to_write), inject_method,
                 inject_point, f"{duration:.1f}", paths_file])

        logging.error(f"[-] Failed sensor test for {apk_name}: {e}")
        import traceback
        traceback.print_exc()
    finally:
        # 额外提示：SensorPolicy 会在 device.output_dir 下生成更细粒度日志（sensor_policy_log.csv）
        print(f"[+] SensorPolicy paths file saved to: {paths_file}")
        print(f"[+] Summary logged to: {log_csv_path}")


INPUT_CSV = os.path.join(output_dir, "apk_fixed.csv")
OUTPUT_CSV = os.path.join(output_dir, "output_apks.csv")
new_apk_csv = os.path.join(output_dir, "new_apks.csv")
pending_csv = os.path.join(output_dir, "pending_fixed.csv")

'''
Test for existing CSV

def main():
    log_file_path = os.path.join(output_dir, "output_log.txt")
    records = []
    if not os.path.exists(INPUT_CSV):
        logging.error(f"{INPUT_CSV} 文件不存在")
        return

    logs = []

    with open(log_file_path, 'r') as file:
        lines = file.readlines()
        logs = [line.strip() for line in lines]

    with open(INPUT_CSV, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames or []

        # 确保有 is_tested 字段
        if "is_tested" not in fieldnames:
            fieldnames.append("is_tested")

        for row in reader:
            row = deepcopy(row)  # 确保 row 是 dict，不被引用修改
            apk_name = row.get("package_name")
            sha = row.get("sha256") + ".apk"
            apk_path = os.path.abspath(os.path.join(download_dir, sha))

            if apk_name in logs:

                print("[+] 已测试" + apk_name)
                continue

            print(apk_name, " ", apk_path)
            if not apk_name or not apk_path or not os.path.exists(apk_path):
                logging.warning(f"跳过无效记录: {row}")
                row["is_tested"] = False
                records.append(row)
                continue

            logging.info(f"开始测试: {apk_name}")
            run_droidbot(apk_path, apk_name, device_serial, output_dir, log_file_path)

            row["is_tested"] = check_utg(apk_name)
            records.append(row)

    # 写回新的 CSV
    with open(OUTPUT_CSV, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=records[0].keys())
        writer.writeheader()
        writer.writerows(records)

    logging.info(f"所有测试完成，结果已写入 {OUTPUT_CSV}")
'''

"""
def main(mode="multi", max_apks=500, source="androzoo", stop_on_fail=True, max_retries=2):
    downloader = APKDownloader(
        api_key="24851dac0234f240cdd1220c6be78c9233502532b83cb98ef28f9d0c870306d7",
        download_dir=download_dir,
        proxies=proxies
    )

    # get the list of apks

    # 从日志中提取已成功处理的 apk
    # tested_apks_by_log = set()
    # if os.path.exists(log_file_path):
    #     with open(log_file_path, 'r', encoding='utf-8') as f:
    #         tested_apks_by_log = set(line.strip() for line in f if line.strip())
    tested_apks = set()
    output_log_file_path = os.path.join(output_dir, "output_log.txt")
    csv_log_file_path = os.path.join(output_dir, "apks.csv")
    logs = []

    if os.path.exists(output_log_file_path):
        with open(output_log_file_path, 'r') as file:
            for line in file:
                pkg = line.strip()
                if pkg:
                    tested_apks.add(pkg)
            # lines = file.readlines()
            # logs = [line.strip() for line in lines]
            # tested_apks.update(line.strip() for line in lines if line.strip())

    if os.path.exists(log_file_path):
        with open(log_file_path, 'r') as file:
            lines = file.readlines()
            logs = [line.strip() for line in lines]
            tested_apks.update(line.strip() for line in lines if line.strip())

    # 读取历史 CSV 日志
    if os.path.exists(csv_log_file_path):
        with open(csv_log_file_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                pkg = row.get("package_name")
                if pkg:
                    tested_apks.add(pkg)

    logging.info(f"Loaded {len(tested_apks)} tested packages from logs and csv.")

    if source == "csv":
        apk_files = downloader.get_apks_from_csv(config.CSV_FILE_PATH)
        apk_files = deduplicate_by_pkg(apk_files)
    elif source == "androzoo":
        if not config.ANDROZOO_CSV_PATH:
            raise ValueError("请在 config.ANDROZOO_CSV_PATH 指定 AndroZoo latest.csv 的路径")

        # apk_files = get_random_apks_from_androzoo(
        #     config.ANDROZOO_CSV_PATH,
        #     num=max_apks or 150,
        #     min_year=2024,
        #     max_size_mb=80,
        #     log_list = tested_apks
        # )

        apk_files = get_random_apks_from_androzoo(
            config.ANDROZOO_CSV_PATH,
            num=max_apks or 500,
            min_year=2024,
            max_size_mb=50,
            log_list=tested_apks,
            new_apks_csv=new_apk_csv,
            pending_csv=pending_csv,
            append_to_new=True
        )

    elif source == "androzoo_csv":
        if not os.path.exists("apk.csv"):
            raise ValueError("apk.csv 文件不存在，请检查路径")
        with open("apk.csv", "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            apk_files = [row for row in reader]

    else:
        logging.error(f"Invalid source: {source}")
        return

    if not apk_files:
        logging.error("No apk files found.")
        return

    new_apks = apk_files[:max_apks] if max_apks else apk_files

    # new_apks = [
    #     apk for apk in apk_files
    # ]

    print(new_apks)

    # new_apks = [
    #     apk for apk in apk_files
    #     if apk["package_name"] not in tested_apks
    # ]
    # logging.info(f"Found {len(new_apks)} new apks.")

    # 限制处理数量
    # if max_apks is not None:
    #     new_apks = new_apks[:max_apks]

    logging.info(f"Found {len(new_apks)} new apks (after applying max_apks limit).")

    if mode == "multi":
        # -----------------------
        # 多线程模式
        # -----------------------
        test_queue = Queue()
        stop_flag = threading.Event()
        csv_path = os.path.join(output_dir, "apks.csv")

        def download_worker(apk_info):
            if stop_flag.is_set():
                return
            pkg_name = apk_info["package_name"]
            sha256 = apk_info["sha256"]
            year = apk_info.get("year", "")
            size = apk_info.get("apk_size", "")
            record = {
                "package_name": pkg_name,
                "sha256": sha256,
                "year": year,
                "size": size,
                "contain_ad": False,
                "is_downloaded": False,
                "is_tested": False
            }

            try:
                if sha256:
                    file_path = downloader.download_apk(sha256)
                else:
                    file_path = apk_info["path"]
                if file_path:
                    record["is_downloaded"] = True
                    logging.info(f"[Download Success] {pkg_name} ({sha256}) -> {file_path}")
                    # contain_ad = analyze_with_libradar(file_path)
                    contain_ad = False
                    record["contain_ad"] = contain_ad

                    # if contain_ad:a
                    #     logging.info(f"[Ad Found] {pkg_name}, enqueue for testing")
                    #     test_queue.put((pkg_name, file_path, record))
                    # else:
                    #     logging.info(f"[No Ad] {pkg_name}, removing file")
                    #     os.remove(file_path)
                    write_csv(csv_path, record)

                    test_queue.put((pkg_name, file_path, record))
            except Exception as e:
                logging.error(f"Failed to download {pkg_name}: {e}")

        def test_worker():
            while True:
                item = test_queue.get()
                if item is None:
                    break

                apk_name, apk_path, record = item
                success = False
                for attempt in range(max_retries):
                    try:
                        run_droidbot(apk_path, apk_name, device_serial, output_dir, log_file_path)
                        success = True
                        break
                    except Exception as e:
                        logging.error(f"Test failed for {apk_name} (attempt {attempt + 1}): {e}")

                if success:
                    record["is_tested"] = success

                write_csv(csv_path, record)

                if not success:
                    logging.error(f"APK {apk_name} failed after {max_retries} retries.")
                    if stop_on_fail:
                        stop_flag.set()
                        # 清空队列避免卡死
                        while not test_queue.empty():
                            test_queue.get_nowait()
                            test_queue.task_done()
                        break
                test_queue.task_done()

        # 启动消费者线程
        test_threads = []
        test_workers = 1  # 这里可以改大，但注意设备资源

        for _ in range(test_workers):
            t = threading.Thread(target=test_worker, daemon=True)
            t.start()
            test_threads.append(t)

        # 启动下载线程池
        with ThreadPoolExecutor(max_workers=2) as executor:
            executor.map(download_worker, new_apks)

        # 等待队列完成
        test_queue.join()

        # 停止消费者
        for _ in range(test_workers):
            test_queue.put(None)

        for t in test_threads:
            t.join()

        logging.info("All apks processed (multi-thread).")

    else:
        # -----------------------
        # 单线程模式
        # -----------------------
        for apk_info in new_apks:
            apk_path = apk_info["path"] if apk_info.get("path") else downloader.download_apk(apk_info["sha256"])
            apk_name = apk_info["apk_name"]

            success = False
            for attempt in range(max_retries):
                try:
                    run_droidbot(apk_path, apk_name, device_serial, output_dir, log_file_path)
                    success = True
                    break
                except Exception as e:
                    logging.error(f"Test failed for {apk_name} (attempt {attempt + 1}): {e}")

            if not success:
                logging.error(f"APK {apk_name} failed after {max_retries} retries.")
                if stop_on_fail:
                    break

        logging.info("All apks processed (single-thraead).")
"""

def get_random_apks_from_androzoo(
        androzoo_csv,
        num=10,
        min_year=2024,
        max_size_mb=50,
        log_list=None,
        apks_csv="new_apks.csv",
        append_to_new=True,
):
    if log_list is None:
        log_list = set()
    else:
        log_list = set(log_list)

    # ---------- Step 1: 优先读取 pending_csv ----------
    # if os.path.exists(pending_csv):
    #     with open(pending_csv, "r", encoding="utf-8") as f:
    #         reader = csv.DictReader(f)
    #         pending = [row for row in reader if row.get("package_name")]

    #     if pending:
    #         print(f"[INFO] 检测到 {len(pending)} 条未完成的 pending 任务，直接返回，不重新采样")
    #         return pending

    # ---------- Step 1: 读取 apks.csv 中已有的包，避免重复 ----------
    existing_pkgs = set()
    existing_shas = set()
    existing_fieldnames = None
    if os.path.exists(apks_csv):
        try:
            with open(apks_csv, "r", encoding="utf-8", errors="replace") as f:
                reader = csv.DictReader(f)
                existing_fieldnames = reader.fieldnames or []
                for r in reader:
                    pkg = (r.get("package_name") or "").strip()
                    sha = (r.get("sha256") or "").strip()
                    if pkg:
                        existing_pkgs.add(pkg)
                    if sha:
                        existing_shas.add(sha)
        except Exception as e:
            print(f"[WARN] 读取 existing new_apks.csv 失败: {e} — 将按空集合处理")

    # ---------- Step 2: reservoir sampling ----------
    reservoir = []
    matched = 0

    with open(androzoo_csv, "rt", encoding="utf-8", errors="replace") as f:
        reader = csv.reader(f)
        for row in reader:
            if not row:
                continue
            try:
                sha256 = row[0].strip()
                if sha256.lower().startswith("sha256"):
                    continue  # 跳过 header
                apk_size = int(row[4])
                dex_date = row[3].strip()
                pkg_name = row[5].strip()
            except Exception:
                continue

            # 去重：跳过已存在的
            if (pkg_name and pkg_name in existing_pkgs) or (sha256 and sha256 in existing_shas):
                continue
            if pkg_name in log_list or sha256 in log_list:
                continue

            # 年份筛选
            try:
                year = int(dex_date[:4])
            except Exception:
                continue
            if year < min_year:
                continue

            # 大小筛选
            if apk_size > max_size_mb * 1024 * 1024:
                continue

            # reservoir 采样
            matched += 1
            item = {
                "sha256": sha256,
                "package_name": pkg_name,
                "size": apk_size,
                "year": year
            }
            if len(reservoir) < num:
                reservoir.append(item)
            else:
                r = random.randint(0, matched - 1)
                if r < num:
                    reservoir[r] = item

    # ---------- Step 3: 追加到 apks.csv ----------
    if append_to_new and reservoir:
        # default_fields = ["sha256", "package_name", "apk_size", "year", "is_tested", "reason", "", process_status_col]
        default_fields = config.fieldnames
        if existing_fieldnames:
            header_out = list(existing_fieldnames)
            for col in default_fields:
                if col not in header_out:
                    header_out.append(col)
        else:
            header_out = default_fields

        write_header = not os.path.exists(apks_csv)
        try:
            with open(apks_csv, "a", encoding="utf-8", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=header_out)
                if write_header:
                    writer.writeheader()
                for it in reservoir:
                    row = {k: "" for k in header_out}
                    row["package_name"] = it["package_name"]
                    row["sha256"] = it["sha256"]
                    row["apk_name"] = it["package_name"]
                    row["size"] = it["size"]
                    row["year"] = it["year"]
                    row["apk_path"] = it["sha256"]
                    row["app_output_dir"] = it["package_name"]
                    row["contain_ad"] = "FALSE"
                    row["is_downloaded"] = "FALSE"
                    row["is_tested"] = "FALSE"
                    row["issue"] = ""
                    row["sensor_test_done"] = "FALSE"
                    # row[process_status_col] = "pending"
                    writer.writerow(row)
            print(
                f"[DONE] 已把 {len(reservoir)} 条待处理追加到 {apks_csv} (is_tested=FALSE)")
        except Exception as e:
            print(f"[ERROR] 追加到 new_apks.csv 失败: {e}")

    return reservoir


def main(max_apks=1000,
         stop_on_fail=True,
         max_retries=2,
         num_downloaders=2,
         num_testers=1):
    downloader = APKDownloader(
        api_key="24851dac0234f240cdd1220c6be78c9233502532b83cb98ef28f9d0c870306d7",
        download_dir=download_dir,
        proxies=proxies
    )

    # get the list of apks through read the apks.csv
    print("[+] Get the list of apks from apks_fixed.csv")
    df_apk = pd.read_csv(INPUT_CSV, dtype=str).fillna("")
    # df_apk["is_tested"] = df_apk["is_tested"].astype(str).str.lower() == "true"
    # df_apk["is_downloaded"] = df_apk["is_downloaded"].astype(str).str.lower() == "true"
    df_apk["is_tested"] = df_apk["is_tested"].astype(str).str.upper() == "TRUE"
    df_apk["is_downloaded"] = df_apk["is_downloaded"].astype(str).str.upper() == "TRUE"

    # 去除重复的APK记录，基于sha256字段
    df_apk = df_apk.drop_duplicates(subset=["sha256"], keep="last")

    #pending_list = df_apk[df_apk["is_tested"] == False].to_dict("records")
    pending_list = df_apk[(df_apk["is_tested"] == False) & (df_apk["issue"] == "")].to_dict("records")

    print("[+] Number of pending apks: {}".format(len(pending_list)))

    # read apks from pending.csv
    # if os.path.exists(pending_csv):
    #     print("read from pending")
    #     df_pending = pd.read_csv(pending_csv, dtype=str).fillna("")
    #     df_pending["is_tested"] = df_pending["is_tested"].astype(str).str.lower() == "true"
    #     not_done = df_pending[df_pending["is_tested"] == False].to_dict("records")
    #     pending_list.extend(not_done)

    # sample from androzoo
    if config.ANDROZOO_CSV_PATH and len(pending_list) < max_apks:
        print("sample")
        need_num = max_apks - len(pending_list)
        sampled = get_random_apks_from_androzoo(
            androzoo_csv=config.ANDROZOO_CSV_PATH,
            num=need_num,
            min_year=2024,
            max_size_mb=500,
            log_list=[a["sha256"] for a in pending_list],
            apks_csv=INPUT_CSV,
            append_to_new=True
        )
        pending_list.extend(sampled)

    pending_list = pending_list[:max_apks]
    print(f"[INFO] 本轮待测试 APK 数量: {len(pending_list)}")

    test_queue = queue.Queue()
    download_queue = queue.Queue()
    need_download = []
    downloaded = []
    stop_flag = threading.Event()

    for app in pending_list:
        sha256 = app["sha256"]
        pkg = app["package_name"]
        apk_path = os.path.join(download_dir, f"{sha256}.apk")

        # if app.get("is_downloaded", False) == "True":
        #     if not os.path.exists(apk_path) or os.path.getsize(apk_path) < 1024:
        #         print(f"[!] {sha256} 文件缺失或损坏，重新下载...")
        #         app["is_downloaded"] = False
        #
        # if not app.get("is_downloaded", False):
        #     need_download.append((app, apk_path))
        # else:
        #     downloaded.append((app, apk_path))

        # if app.get("is_downloaded", False) and isinstance(app.get("is_downloaded", False), str):
        #     app["is_downloaded"] = app["is_downloaded"].lower() == "true"
        # 检查is_downloaded字段，确保它是布尔类型
        is_downloaded = app.get("is_downloaded", False)

        # 如果字段是字符串类型（从CSV读取的），需要转换为布尔值
        if isinstance(is_downloaded, str):
            is_downloaded = is_downloaded.upper() == "TRUE"
            app["is_downloaded"] = is_downloaded  # 更新为布尔值

        if app.get("is_downloaded", False):
            if not os.path.exists(apk_path) or os.path.getsize(apk_path) < 1024:
                print(f"[!] {sha256} 文件缺失或损坏，重新下载...")
                app["is_downloaded"] = False
                need_download.append((app, apk_path))
            else:
                downloaded.append((app, apk_path))
        else:
            need_download.append((app, apk_path))

        for app, apk_path in downloaded:
            # test_queue.put((app, apk_path))
            record = {
                "package_name": app["package_name"],
                "sha256": app["sha256"],
                "apk_name": app["package_name"],
                "apk_path": apk_path,
                "app_output_dir": app["package_name"],
                "year": app.get("year", ""),
                "size": app.get("apk_size", ""),
                "contain_ad": app.get("contain_ad", False),
                "is_downloaded": True,
                "is_tested": False
            }
            test_queue.put((app["package_name"], apk_path, record))

        # fieldnames = [
        #     "package_name", "sha256", "year", "size",
        #     "contain_ad", "is_downloaded", "apk_path", "app_output_dir", "is_tested", "issue", "sensor_test_done"
        # ]

    def download_worker_loop():
        while True:
            item = download_queue.get()
            if item is None:
                # 收到哨兵，标记完成并退出
                download_queue.task_done()
                break

            apk_info, _ = item
            print("[+] Downloading APK...]")
            if stop_flag.is_set():
                download_queue.task_done()
                continue

            pkg_name = apk_info["package_name"]
            sha256 = apk_info["sha256"]
            year = apk_info.get("year", "")
            size = apk_info.get("apk_size", "")
            record = {
                "package_name": pkg_name,
                "sha256": sha256,
                "apk_name": pkg_name,
                "apk_path": sha256,
                "app_output_dir": pkg_name,
                "year": year,
                "size": size,
                "contain_ad": False,
                "is_downloaded": False,
                "is_tested": False
            }

            try:
                if sha256:
                    file_path = downloader.download_apk(sha256)
                else:
                    file_path = apk_info.get("path")
                if file_path and os.path.exists(file_path):
                    record["is_downloaded"] = True
                    logging.info(f"[Download Success] {pkg_name} ({sha256}) -> {file_path}")
                    record["contain_ad"] = False
                    write_csv_back(INPUT_CSV, record, config.fieldnames)
                    # 推入测试队列
                    test_queue.put((pkg_name, file_path, record))
                else:
                    logging.error(f"[Download Fail] {pkg_name} ({sha256}) -> {file_path}")
            except Exception as e:
                logging.error(f"Failed to download {pkg_name}: {e}")
            finally:
                download_queue.task_done()
    def download_worker(apk_info):
        print("[+] Downloading APK...]")
        if stop_flag.is_set():
            return

        pkg_name = apk_info["package_name"]
        sha256 = apk_info["sha256"]
        year = apk_info.get("year", "")
        size = apk_info.get("apk_size", "")
        # apk_path = os.path.join(download_dir, sha256+".apk")
        # apk_output_dir = os.path.join(output_dir, sha256+".apk")
        record = {
            "package_name": pkg_name,
            "sha256": sha256,
            "apk_name": pkg_name,
            "apk_path": sha256,
            "app_output_dir": pkg_name,
            "year": year,
            "size": size,
            "contain_ad": False,
            "is_downloaded": False,
            "is_tested": False
        }

        try:
            if sha256:
                file_path = downloader.download_apk(sha256)
            else:
                file_path = apk_info["path"]
            if file_path:
                record["is_downloaded"] = True
                logging.info(f"[Download Success] {pkg_name} ({sha256}) -> {file_path}")
                # contain_ad = analyze_with_libradar(file_path)
                contain_ad = False
                record["contain_ad"] = contain_ad

                # if contain_ad:
                #     logging.info(f"[Ad Found] {pkg_name}, enqueue for testing")
                #     test_queue.put((pkg_name, file_path, record))
                # else:
                #     logging.info(f"[No Ad] {pkg_name}, removing file")
                #     os.remove(file_path)
                write_csv_back(INPUT_CSV, record, config.fieldnames)

                test_queue.put((pkg_name, file_path, record))
        except Exception as e:
            logging.error(f"Failed to download {pkg_name}: {e}")

    def is_already_tested(sha256):
        """检查指定SHA256的APK是否已经测试过"""
        # 这里可以从CSV文件或内存中的数据结构检查
        df = pd.read_csv(INPUT_CSV, dtype=str).fillna("")
        tested = df[df["sha256"] == sha256]["is_tested"].astype(str).str.upper() == "TRUE"
        return len(tested) > 0 and tested.iloc[0]

    def update_memory_record(updated_record):
        """更新内存中的记录"""
        # 找到并更新pending_list中的对应记录
        sha256 = updated_record.get("sha256", "")
        for i, app in enumerate(pending_list):
            if app.get("sha256", "") == sha256:
                pending_list[i] = updated_record
                break

    def test_worker():
        print("[+] Testing APK...")
        while True:
            try:
                item = test_queue.get()
            except queue.Empty:
                if stop_flag.is_set():
                    break
                else:
                    continue

            if item is None:  # 哨兵
                test_queue.task_done()
                break

            apk_name, apk_path, record = item

            # 检查是否已经测试过（防止重复测试）
            sha256 = record.get("sha256", "")
            if is_already_tested(sha256):  # 需要实现这个函数
                print(f"[!] APK {apk_name} ({sha256}) 已经测试过，跳过")
                test_queue.task_done()
                continue

            print(apk_name)
            success = False
            for attempt in range(max_retries):
                try:
                    run_droidbot(apk_path, apk_name, device_serial, output_dir, log_file_path)
                    success = True
                    break
                except Exception as e:
                    logging.error(f"Test failed for {apk_name} (attempt {attempt + 1}): {e}")

            if success:
                record["is_tested"] = success

            write_csv_back(INPUT_CSV, record, config.fieldnames)
            update_memory_record(record)

            if not success:
                logging.error(f"APK {apk_name} failed after {max_retries} retries.")
                if stop_on_fail:
                    stop_flag.set()
                    # 清空队列避免卡死
                    while not test_queue.empty():
                        try:
                            test_queue.get_nowait()
                            test_queue.task_done()
                        except queue.Empty:
                            break
                    break
            test_queue.task_done()

    # 启动固定数量的下载线程
    # download_threads = []
    # for _ in range(num_downloaders):
    #     t = threading.Thread(target=download_worker, args=(app,))
    #     t.daemon = True  # 设置为守护线程，主线程退出时自动结束
    #     t.start()
    #     download_threads.append(t)


    # 把需要下载的任务放到 download_queue 中
    for app, apk_path in need_download:
        download_queue.put((app, apk_path))

    download_threads = []
    for _ in range(num_downloaders):
        t = threading.Thread(target=download_worker_loop)
        t.daemon = True
        t.start()
        download_threads.append(t)

    test_threads = []
    for _ in range(num_testers):
        t = threading.Thread(target=test_worker)
        t.daemon = True
        t.start()
        test_threads.append(t)

    # ---------- 等待下载队列完成 ----------
    download_queue.join()  # 等待所有 download_queue.put 的任务都被处理并 task_done()

    # 发送下载 worker 的哨兵让下载线程结束
    for _ in range(num_downloaders):
        download_queue.put(None)

    # 等待下载线程真正退出（可选）
    for t in download_threads:
        t.join()

    # ---------- 此时所有下载已完成并把下载成功的项 put 到 test_queue ----------
    # 等待测试队列处理完已入队的所有任务
    test_queue.join()

    # 给每个测试线程发送哨兵，通知退出
    for _ in range(num_testers):
        test_queue.put(None)

    # 等待测试线程退出
    for t in test_threads:
        t.join()


def main_sensor_test(
        sensor_csv: str,
        root_output_dirs: List[str],
        max_paths_per_app: Optional[int] = None):
    # [todo] read lit from csv and get a list of tasks
    print("[+] process " + sensor_csv)
    print("[+] process " + root_output_dirs)
    if not os.path.exists(sensor_csv):
        raise FileNotFoundError(sensor_csv)

    rows = read_csv_as_dicts(sensor_csv)
    if not rows:
        logger.info("No tasks in sensor CSV.")
        return

    fieldnames = list(rows[0].keys())
    required_cols = ["package_name", "apk_name", "apk_path", "app_output_dir", "sensor_test_done"]
    for c in required_cols:
        if c not in fieldnames:
            fieldnames.append(c)

    updated_rows = []  # for writing back ole csv
    pending_tasks = {}

    for row in rows:
        done = str(row.get("sensor_test_done")).strip().lower()
        if done in ("true", "1", "yes", "y"):
            logger.info(f"Sensor test {row.get('apk_name')} done.")
            continue

        package_name = row.get("package_name")
        apk_name = row.get("apk_name")
        apk_path = row.get("apk_path")
        app_output_dir = row.get("app_output_dir")

        if not apk_path or not os.path.exists(apk_path):
            logger.warning(
                f"APK not found for task (package={package_name}, apk_name={apk_name}) in {apk_path}. Skipping and leaving task as not done.")
            updated_rows.append(row)
            continue

        # 放入待执行任务池
        pending_tasks[package_name] = {
            "row": row,
            "package_name": package_name,
            "apk_name": apk_name,
            "apk_path": apk_path,
            "app_output_dir": app_output_dir,
        }

    new_results = []

    for output_root in root_output_dirs:

        # [todo] if the package_name (the pkg_name is the output_root name) is not `sensor_test_done`, continue to test.

        if not os.path.isdir(output_root):
            logger.warning(f"Output root not found or not dir: {output_root}")
            continue

        #folders = sorted(os.listdir(output_root))
        #logger.info(f"Processing output_root={output_root}, found {len(folders)} entries")

        with os.scandir(output_root) as it:
            for entry in it:

                if not entry.is_dir():
                    logger.debug(f"Skipping non-dir entry: {entry.name}")
                    continue

                app_dir = entry.path
                app_dir_name = entry.name
                logger.info(f"Processing app_dir: {app_dir_name} ({app_dir})")

                if app_dir_name not in pending_tasks:
                    logger.debug(f"{app_dir_name} not in pending tasks, skip.")
                    continue

                task = pending_tasks[app_dir_name]
                row = task["row"]
                apk_name = task["apk_name"]
                apk_path = task["apk_path"]

                # 1) find apk (reuse your helper or implement inline)
                # need to check the label
                # apk_path = find_apk_from_csv_or_folder(app_dir, download_dir)
                # if not apk_path or not os.path.exists(apk_path):
                #     logger.warning(f"APK not found for {app_dir_name}, skipping")
                #     continue

                path_json_path = find_paths_json(app_dir)
                if not path_json_path or not os.path.exists(path_json_path):
                    logger.warning(f"paths json not found in {app_dir}; skipping sensor test for this app")
                    continue

                sensor_test_dir = os.path.join(app_dir, "sensor_test")
                ensure_dir(sensor_test_dir)

                # prepare log files
                sensor_log_txt = os.path.join(sensor_test_dir, "sensor_log.txt")
                sensor_log_csv = os.path.join(sensor_test_dir, "sensor_log.csv")

                app_output_dir = sensor_test_dir  # where run_sensor_test will write outputs for this run
                # determine apk_name (use package name if present in a metadata file, else filename)
                apk_name = os.path.splitext(os.path.basename(apk_path))[0]

                # call run_sensor_test (use named args to avoid order mismatch)
                logger.info(f"Starting run_sensor_test for apk_name={apk_name}")
                output_dir = os.path.join(sensor_test_dir, "./sensor_log.txt")
                log_csv_path = os.path.join(sensor_test_dir, "./sensor_log.csv")
                try:
                    run_sensor_test(
                        apk_path,
                        apk_name,
                        app_output_dir,
                        device_serial,
                        output_dir=app_output_dir,
                        log_csv_path=sensor_log_csv,
                        policy_name="sensor_policy",
                        inject_method="adb",
                        inject_point="before_ad",
                        path_file=path_json_path,
                        event_interval=5,
                        event_count_extra=10,
                        timeout=300,
                        max_paths=None,
                        env_overrides=None
                    )
                    # [todo] finish and write the sensot_test_down in old csv, and write a new csv for recording
                    row["sensor_test_done"] = "true"
                    new_results.append({
                        "package_name": task["package_name"],
                        "apk_name": apk_name,
                        "apk_path": apk_path,
                        "app_output_dir": app_dir,
                        "sensor_test_dir": sensor_test_dir,
                        "sensor_log_txt": sensor_log_txt,
                        "sensor_log_csv": sensor_log_csv,
                        "sensor_test_done": "true"
                    })
                except Exception as e:
                    logger.error(f"run_sensor_test failed for {apk_name}: {e}", exc_info=True)
                    row["sensor_test_done"] = "false"

                updated_rows.append(row)

    # 先做一个备份 copy
    backup_csv = sensor_csv.replace(".csv", "_backup.csv")
    shutil.copy(sensor_csv, backup_csv)
    logger.info(f"Backup of old CSV written to {backup_csv}")

    # 再覆盖原 CSV
    with open(sensor_csv, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(updated_rows)

    # 新结果 CSV
    result_csv = sensor_csv.replace(".csv", "_sensor_results.csv")
    with open(result_csv, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "package_name", "apk_name", "apk_path", "app_output_dir",
            "sensor_test_dir", "sensor_log_txt", "sensor_log_csv", "sensor_test_done"
        ])
        writer.writeheader()
        writer.writerows(new_results)

    logger.info(f"Sensor test finished. Updated tasks written to {sensor_csv}, "
                f"results to {result_csv}, backup to {backup_csv}.")


if __name__ == '__main__':
    # adb over network
    if ':' in device_serial:
        os.system("adb connect " + device_serial)

    #main_sensor_test(INPUT_CSV, output_dir)
    main()
