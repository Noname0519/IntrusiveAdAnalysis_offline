
import os
import pandas as pd
import shutil

# input an csv and the output file path, to clean the folders


def clean_folders(log_csv, root_dir):
    """
    清理 root_dir 下的文件夹：
      1. 不在 log.csv 的 apk_name 中的文件夹将被删除；
      2. 文件夹中没有 utg.js 的也将被删除。
    """
    # 读取 CSV
    try:
        df = pd.read_csv(log_csv, dtype=str)
    except Exception as e:
        print(f"[!] 读取 {log_csv} 失败: {e}")
        return

    valid_apks = set(df["apk_name"].dropna().unique())
    print(f"[+] 从 {log_csv} 中读取 {len(valid_apks)} 个有效 APK 名称。")

    # 遍历文件夹
    if not os.path.exists(root_dir):
        print(f"[!] 目录不存在: {root_dir}")
        return

    folders = [f for f in os.listdir(root_dir) if os.path.isdir(os.path.join(root_dir, f))]
    print(f"[+] 检查 {len(folders)} 个文件夹...")

    removed_count = 0
    for folder in folders:
        folder_path = os.path.join(root_dir, folder)

        # 1. 不在CSV中则删除
        # if folder not in valid_apks:
        #     print(f"[-] 删除无记录文件夹: {folder}")
        #     shutil.rmtree(folder_path, ignore_errors=True)
        #     removed_count += 1
        #     continue

        # 2. 不含utg.js也删除
        utg_path = os.path.join(folder_path, "utg.js")
        if not os.path.exists(utg_path):
            print(f"[-] 删除无 utg.js 文件夹: {folder}")
            shutil.rmtree(folder_path, ignore_errors=True)
            removed_count += 1
            continue

    print(f"\n[✓] 清理完成，共删除 {removed_count} 个无效文件夹。")


if __name__ == "__main__":
    # === 手动配置部分 ===
    LOG_CSV = "untested_simulator1.csv"           # 你的 log.csv 路径
    ROOT_DIR = "output"  # 存放APK测试结果的目录（每个APK一个文件夹）

    clean_folders(LOG_CSV, ROOT_DIR)

#if __name__ == "__main__":
    #dir_path = "D:\\NKU\\Work\\Work2\\datasets\\androzoo\\androzoo_apks"
    #clean_apk_folder(apk_dir=dir_path, csv_path="D:\\NKU\\Work\\Work2\\datasets\\androzoo\\androzoo_output\\apk_fixed.csv")


