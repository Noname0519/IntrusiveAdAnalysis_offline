import os
import pandas as pd
import csv

def check_consistency(log_csv, output_dir, apk_col="apk_name"):
    """
    检查 log.csv 与 output_dir 下文件夹是否一一对应。

    参数:
        log_csv: CSV 文件路径（包含 apk_name 字段）
        output_dir: 输出文件夹路径
        apk_col: CSV 中 APK 名称对应的列名（默认为 "apk_name"）

    输出:
        打印出缺失项和多余项
    """
    if not os.path.exists(log_csv):
        print(f"[!] CSV 文件不存在: {log_csv}")
        return
    if not os.path.exists(output_dir):
        print(f"[!] 输出目录不存在: {output_dir}")
        return

    # === 读取 CSV ===
    df = pd.read_csv(log_csv, dtype=str).fillna("")
    if apk_col not in df.columns:
        print(f"[!] CSV 不包含列 '{apk_col}'")
        return

    csv_apks = set(df[apk_col].dropna().unique())
    print(f"[+] 从 {log_csv} 读取 {len(csv_apks)} 个 APK 记录")

    # === 读取输出目录 ===
    dir_apks = {d for d in os.listdir(output_dir) if os.path.isdir(os.path.join(output_dir, d))}
    print(f"[+] 输出目录中检测到 {len(dir_apks)} 个文件夹")

    # === 差集分析 ===
    missing_in_dir = csv_apks - dir_apks
    extra_in_dir = dir_apks - csv_apks

    print("\n===== 差异报告 =====")
    print(f"🟡 CSV 中存在但文件夹缺失: {len(missing_in_dir)}")
    if missing_in_dir:
        for item in sorted(list(missing_in_dir))[:20]:
            print(f"  - {item}")
        if len(missing_in_dir) > 20:
            print(f"  ... 共 {len(missing_in_dir)} 项")

    print(f"\n🔵 文件夹存在但 CSV 中无记录: {len(extra_in_dir)}")
    if extra_in_dir:
        for item in sorted(list(extra_in_dir))[:20]:
            print(f"  - {item}")
        if len(extra_in_dir) > 20:
            print(f"  ... 共 {len(extra_in_dir)} 项")

    # === 返回结果（方便程序化调用）===
    return {
        "missing_in_dir": missing_in_dir,
        "extra_in_dir": extra_in_dir
    }


def align_csv_with_output(output_dir: str, csv_path: str, output_csv: str = "aligned_log.csv"):
    """
    检查csv与文件夹是否一一对应，输出一个对齐后的csv：
    - 如果文件夹存在，则is_tested=True；
    - 如果不存在，则is_tested=False；
    - 对于存在文件夹但csv中缺少的apk_name，也会补充记录。
    """

    FIELDNAMES = [
        "package_name", "sha256", "apk_name", "size", "year",
        "apk_path", "app_output_dir", "contain_ad", "is_downloaded",
        "is_tested", "issue", "sensor_test_done",  "timestamp", "device_serial"
    ]

    # 读取已有 CSV
    if os.path.exists(csv_path):
        df = pd.read_csv(csv_path, dtype=str).fillna("")
    else:
        print(f"[!] CSV文件不存在：{csv_path}")
        df = pd.DataFrame(columns=FIELDNAMES)

    # 记录所有 output_dir 中的文件夹
    folder_names = {name for name in os.listdir(output_dir)
                    if os.path.isdir(os.path.join(output_dir, name))}

    existing_apks = set(df["apk_name"]) if "apk_name" in df.columns else set()

    updated_records = []

    # 更新已有记录的 is_tested 状态
    for _, row in df.iterrows():
        apk_name = row["apk_name"]
        if apk_name in folder_names:
            row["is_tested"] = "TRUE"
        else:
            row["is_tested"] = "FALSE"
        updated_records.append(row.to_dict())

    # 检查 output_dir 中多出来的文件夹（不在CSV中）
    extra_folders = folder_names - existing_apks
    if extra_folders:
        print(f"[+] 检测到 {len(extra_folders)} 个未记录的文件夹，将自动添加。")

    for folder in extra_folders:
        record = {f: "" for f in FIELDNAMES}
        record["apk_name"] = folder
        record["app_output_dir"] = folder
        record["is_tested"] = "TRUE"
        updated_records.append(record)

    # 保存为新的 CSV
    with open(output_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(updated_records)

    print(f"[✔] 已生成对齐后的CSV：{output_csv}")
    print(f"共有记录数：{len(updated_records)}")


if __name__ == "__main__":
    LOG_CSV = "untested_simulator1.csv"
    OUTPUT_DIR = "output"
    OUTPUT_CSV = "aligned_log.csv"
    # check_consistency(LOG_CSV, OUTPUT_DIR)

    align_csv_with_output(OUTPUT_DIR, LOG_CSV, OUTPUT_CSV)
