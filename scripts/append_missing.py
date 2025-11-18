import os
import csv
import shutil

"""
添加散落的结果
"""

def append_missing_apks(merged_csv, root_dirs, output_csv=None):
    """
    遍历多个根文件夹，将未在 merged_csv 中记录的有效 APK 文件夹添加为新记录：
      - 检查文件夹名是否已在 CSV 的 package_name/sha256/apk_name 中出现；
      - 如果不存在，且文件夹下存在 utg.js，则记录一条新记录；
      - apk_name = 文件夹名
      - app_output_dir = os.path.join(根文件夹, 文件夹名)
      - is_tested = 1
      - 其他字段为空
    """

    if not output_csv:
        output_csv = merged_csv.replace(".csv", "_updated.csv")

    # 读取已有 CSV
    existing_keys = set()  # 用 apk_name/sha256/package_name 去重
    rows = []
    if os.path.exists(merged_csv):
        with open(merged_csv, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            fieldnames = reader.fieldnames
            for row in reader:
                rows.append(row)
                key = (
                    row.get("package_name", "").strip(),
                    row.get("sha256", "").strip(),
                    row.get("apk_name", "").strip()
                )
                existing_keys.add(key)
    else:
        # 若 CSV 不存在，则创建默认字段
        fieldnames = [
            "package_name", "sha256", "apk_name", "size", "year",
            "apk_path", "app_output_dir", "contain_ad", "is_downloaded",
            "is_tested", "issue", "sensor_test_done", "timestamp", "device_serial"
        ]

    new_count = 0
    for root_dir in root_dirs:
        if not os.path.exists(root_dir):
            print(f"[!] 根文件夹不存在，跳过: {root_dir}")
            continue

        for folder_name in os.listdir(root_dir):
            folder_path = os.path.join(root_dir, folder_name)
            if not os.path.isdir(folder_path):
                continue

            # 构建 key，默认 sha256/package_name 空，apk_name = folder_name
            key = ("", "", folder_name)
            if key in existing_keys:
                continue  # 已存在，跳过

            utg_path = os.path.join(folder_path, "utg.js")
            if os.path.exists(utg_path):
                new_row = {fn: "" for fn in fieldnames}
                new_row["apk_name"] = folder_name
                new_row["app_output_dir"] = folder_path
                new_row["is_tested"] = "1"

                rows.append(new_row)
                existing_keys.add(key)
                new_count += 1

    # 写出更新后的 CSV
    with open(output_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"[✔] 补充 {new_count} 条新记录。更新后的 CSV 写入: {output_csv}")


def main():
    
    # 假设以下路径
    merged_csv = "merged_validity_results.csv"
    root_dirs = [
        "D:\\NKU\\Work\\Work2\\fraudulent_output",
        "D:\\NKU\\Work\\Work2\\datasets\\manual_analysis\\output", 
        "D:\\NKU\\Work\\Work2\\datasets\\chin\\output",
        "D:\\NKU\\Work\\Work2\\datasets\\manual_analysis\\test_adgpe_test"
    ]
    # 如果需要自定义输出文件名
    output_csv = "merged_validity_results_final.csv"
    append_missing_apks(merged_csv, root_dirs, output_csv=output_csv)

    

if __name__ == "__main__":
    main()