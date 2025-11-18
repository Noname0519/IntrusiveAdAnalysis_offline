import csv
import os

def merge_csv_based_on_validity(validity_txt, csv_paths, output_csv="merged_validity_results.csv", invalid_csv="invalid_is_tested.csv"):
    """
    基于 folder_validity.txt 中的 apk_name 与 valid 值，从多个 CSV 文件中匹配行并合并。
    若 valid=1，则将 is_tested 设置为 1，并在 app_output_dir 中写入实际路径。
    同时输出一个记录 is_tested 非 0/1 的 CSV。
    """

    # === 读取 folder_validity.txt ===
    validity_map = {}  # {apk_name: (root_dir, valid)}
    with open(validity_txt, "r", encoding="utf-8") as f:
        for line in f:
            parts = line.strip().split("\t")
            if len(parts) < 3:
                continue
            root_dir, apk_name, valid = parts
            validity_map[apk_name] = (root_dir, int(valid))

    print(f"[+] 从 {validity_txt} 读取到 {len(validity_map)} 条记录")

    # === 目标字段顺序 ===
    fieldnames = [
        "package_name", "sha256", "apk_name", "size", "year",
        "apk_path", "app_output_dir", "contain_ad", "is_downloaded",
        "is_tested", "issue", "sensor_test_done", "timestamp", "device_serial"
    ]

    merged_rows = []
    invalid_rows = []
    seen = set()

    # === 遍历所有 CSV ===
    for csv_path in csv_paths:
        if not os.path.exists(csv_path):
            print(f"[!] 跳过不存在的 CSV: {csv_path}")
            continue

        print(f"[+] 正在读取: {csv_path}")
        with open(csv_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                apk_name = row.get("apk_name", "").strip()
                if not apk_name:
                    continue

                if apk_name in validity_map:
                    root_dir, valid = validity_map[apk_name]
                    row["app_output_dir"] = os.path.join(root_dir, apk_name)
                    # 若 valid=1，则强制设置 is_tested=1
                    if valid == 1:
                        row["is_tested"] = "1"
                    else:
                        # 若 CSV 原本有值就保留，没有则默认 0
                        row["is_tested"] = row.get("is_tested", "0")

                    # 检查 is_tested 是否异常
                    if row["is_tested"] not in ("0", "1"):
                        invalid_rows.append({fn: row.get(fn, "") for fn in fieldnames})

                    # 去重
                    key = (apk_name, row.get("sha256", ""))
                    if key not in seen:
                        seen.add(key)
                        clean_row = {fn: row.get(fn, "") for fn in fieldnames}
                        merged_rows.append(clean_row)

    # === 写出合并结果 ===
    with open(output_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(merged_rows)

    # === 写出 is_tested 异常记录 ===
    if invalid_rows:
        with open(invalid_csv, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(invalid_rows)
        print(f"[!] 发现 {len(invalid_rows)} 条 is_tested 异常记录，已写入 {invalid_csv}")
    else:
        print("[✔] 所有 is_tested 字段均有效 (0 或 1)")

    print(f"[✔] 合并完成，共匹配 {len(merged_rows)} 条记录。结果写入: {output_csv}")

# =============================
# 主程序入口
# =============================
if __name__ == "__main__":
    validity_txt = "/Users/noname/noname/nku/Work/IntrusiveAdAnalysis_offline/files/folder_validity.txt"
    csv_paths = [
        r"/Users/noname/noname/nku/Work/IntrusiveAdAnalysis_offline/files/merged_output_e.csv",
        r"/Users/noname/noname/nku/Work/IntrusiveAdAnalysis_offline/files/merged_output_f.csv",
        r"/Users/noname/noname/nku/Work/IntrusiveAdAnalysis_offline/files/log2.csv",
        r"/Users/noname/noname/nku/Work/IntrusiveAdAnalysis_offline/files/log.csv",
        r"/Users/noname/noname/nku/Work/IntrusiveAdAnalysis_offline/files/aligned_log.csv",

    ]
    merge_csv_based_on_validity(validity_txt, csv_paths,
                                output_csv="/Users/noname/noname/nku/Work/IntrusiveAdAnalysis_offline/files/merged_validity_results1.csv",
                                invalid_csv="/Users/noname/noname/nku/Work/IntrusiveAdAnalysis_offline/files/invalid_is_tested1.csv")