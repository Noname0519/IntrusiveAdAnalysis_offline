import os
import csv
import threading
import queue
import pandas as pd
import logging

root_dir = "E:\\test\\"
INPUT_CSV = os.path.join(root_dir, "split_1.csv")
LOG_CSV = os.path.join(root_dir, "log.csv")
download_dir = "E:\\test\\apks\\"
device_serial = "13.209.173.201:44055"  # 改成你自己的设备号
output_dir = "E:\\test\\output\\"
log_file_path = os.path.join(root_dir, "run.log")

FIELDNAMES = [
    "package_name", "sha256", "apk_name", "apk_path", "app_output_dir",
    "year", "size", "contain_ad", "is_downloaded", "is_tested", "issue"
]

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


def init_log_csv():
    if not os.path.exists(LOG_CSV):
        with open(LOG_CSV, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
            writer.writeheader()
        print("[+] Initialized log.csv")

def write_csv_back(record):
    """追加写回 log.csv"""
    with open(LOG_CSV, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writerow(record)

def main(max_retries=2, num_downloaders=1, num_testers=1):
    init_log_csv()

    df = pd.read_csv(INPUT_CSV, dtype=str).fillna("")
    pending_list = df.to_dict("records")

    test_queue = queue.Queue()
    download_queue = queue.Queue()
    stop_flag = threading.Event()

    need_download, downloaded = [], []
    for app in pending_list:
        sha256 = app["sha256"]
        pkg = app["pkg_name"]
        apk_path = os.path.join(download_dir, f"{sha256}.apk")

        if os.path.exists(apk_path) and os.path.getsize(apk_path) > 1 * 1024 * 1024:
            downloaded.append((app, apk_path))
        else:
            need_download.append((app, apk_path))

    for app, apk_path in downloaded:
        record = {
            "package_name": app["pkg_name"],
            "sha256": app["sha256"],
            "apk_name": app["pkg_name"],
            "apk_path": apk_path,
            "app_output_dir": app["pkg_name"],
            "year": app.get("year", ""),
            "size": app.get("apk_size", ""),
            "contain_ad": False,
            "is_downloaded": True,
            "is_tested": False,
            "issue": ""
        }
        test_queue.put((app["pkg_name"], apk_path, record))

    def download_worker_loop():
        while True:
            item = download_queue.get()
            if item is None:
                download_queue.task_done()
                break
            apk_info, apk_path = item
            sha256 = apk_info["sha256"]
            pkg = apk_info["pkg_name"]
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
                "issue": ""
            }
            try:
                file_path = downloader.download_apk(sha256)
                if file_path and os.path.exists(file_path):
                    record["is_downloaded"] = True
                    write_csv_back(record)
                    test_queue.put((pkg, file_path, record))
                else:
                    record["issue"] = "download_failed"
                    write_csv_back(record)
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
            success = False
            for attempt in range(max_retries):
                try:
                    run_droidbot(apk_path, apk_name, device_serial, output_dir, log_file_path)
                    success = True
                    break
                except Exception as e:
                    logging.error(f"Test failed for {apk_name} attempt {attempt+1}: {e}")
            record["is_tested"] = success
            if not success:
                record["issue"] = "test_failed"
            write_csv_back(record)
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

    main()
