import os
import pandas as pd
import random

FIELDNAMES = [
    "package_name", "sha256", "apk_name", "size", "year",
    "apk_path", "app_output_dir", "contain_ad", "is_downloaded",
    "is_tested", "issue", "sensor_test_done"
]

def prepare_sensor_test_inputs(history_csvs, latest_csv, output_dir, total_needed=3000, num_splits=3):
    os.makedirs(output_dir, exist_ok=True)

    # 1️⃣ 读取历史 CSV 并收集 sha256
    all_sha256 = set()
    for csv_path in history_csvs:
        try:
            df = pd.read_csv(csv_path, dtype=str)
            if "sha256" in df.columns:
                all_sha256.update(df["sha256"].dropna().unique())
                print(f"[+] 加载 {csv_path}: {len(df)} 条, 收集 sha256 {len(all_sha256)} 个")
            else:
                print(f"[!] {csv_path} 不含 sha256 字段，跳过")
        except Exception as e:
            print(f"[!] 读取失败 {csv_path}: {e}")

    print(f"[✓] 历史中共收集 {len(all_sha256)} 个已测试 sha256")

    # 2️⃣ 读取 AndroZoo latest.csv
    print(f"[+] 正在加载 AndroZoo latest.csv (可能较慢)...")
    latest_df = pd.read_csv(latest_csv, dtype=str, usecols=["sha256", "pkg_name", "apk_size", "dex_date"], low_memory=False)
    print(f"[+] latest.csv 共 {len(latest_df)} 条记录")

    # 3️⃣ 过滤掉已出现的 sha256
    new_df = latest_df[~latest_df["sha256"].isin(all_sha256)]
    print(f"[✓] 去重后剩余 {len(new_df)} 个可用 APK")

    if len(new_df) == 0:
        print("[!] 没有可用新样本。退出。")
        return

    # 4️⃣ 随机抽样
    if len(new_df) < total_needed:
        print(f"[!] 可选APK不足 {total_needed} 个，仅 {len(new_df)} 个，将全部输出。")
        total_needed = len(new_df)

    selected = new_df.sample(n=total_needed, random_state=42).copy()

    # 5️⃣ 生成标准字段格式
    def extract_year(date_str):
        if pd.isna(date_str):
            return ""
        for sep in ["-", "/", " "]:
            if sep in date_str:
                return date_str.split(sep)[0]
        return date_str[:4] if len(date_str) >= 4 else ""

    selected["apk_name"] = selected["sha256"].astype(str) + ".apk"
    selected["year"] = selected["dex_date"].apply(extract_year)

    result = pd.DataFrame(columns=FIELDNAMES)
    result["package_name"] = selected["pkg_name"]
    result["sha256"] = selected["sha256"]
    result["apk_name"] = selected["apk_name"]
    result["size"] = selected["apk_size"]
    result["year"] = selected["year"]

    # 其余字段空置
    for col in ["apk_path", "app_output_dir", "contain_ad", "is_downloaded", "is_tested", "issue", "sensor_test_done"]:
        result[col] = ""

    # 6️⃣ 拆分输出为多个 CSV
    batch_size = total_needed // num_splits
    for i in range(num_splits):
        start = i * batch_size
        end = total_needed if i == num_splits - 1 else (i + 1) * batch_size
        subset = result.iloc[start:end]
        out_csv = os.path.join(output_dir, f"sensor_test_input_batch_{i+1}.csv")
        subset.to_csv(out_csv, index=False)
        print(f"[+] 输出 {len(subset)} 条到 {out_csv}")

    print(f"\n[✓] 已生成 {num_splits} 个 sensor_test_input.csv，共 {total_needed} 条。")


def prepare_new_apks(history_csvs, latest_csv, output_dir, total_needed=3000, num_splits=3):
    """
    从 AndroZoo latest.csv 中选出未测试过的 APK，并分成多个 CSV。
    """
    os.makedirs(output_dir, exist_ok=True)

    # 1️⃣ 读取并合并历史 CSV
    all_history = []
    for csv_path in history_csvs:
        try:
            df = pd.read_csv(csv_path, dtype=str)
            df["apk_name"] = df["apk_name"].str.replace(".apk", "", regex=False)
            all_history.append(df[["apk_name", "sha256"]])
            print(f"[+] 加载历史文件: {csv_path} ({len(df)} 条)")
        except Exception as e:
            print(f"[!] 读取失败 {csv_path}: {e}")

    if not all_history:
        print("[!] 没有有效的历史 CSV 文件。")
        return

    history_df = pd.concat(all_history, ignore_index=True).drop_duplicates(subset=["sha256", "apk_name"])
    known_sha256 = set(history_df["sha256"].dropna())
    known_apks = set(history_df["apk_name"].dropna())
    print(f"[✓] 历史记录总计: {len(history_df)} 个 (唯一 sha256={len(known_sha256)})")

    # 2️⃣ 读取 AndroZoo latest.csv
    # 格式: sha256, pkg_name, vercode, vername, size, markets, added, updated, dex_date, apk_name, ...
    print(f"[+] 正在读取 AndroZoo latest.csv（可能需要较长时间）...")
    latest_df = pd.read_csv(latest_csv, dtype=str)
    print(f"[+] latest.csv 共 {len(latest_df)} 条记录")

    latest_df["apk_name"] = latest_df["apk_name"].str.replace(".apk", "", regex=False)

    # 3️⃣ 过滤掉已知的 apk
    filtered = latest_df[~latest_df["sha256"].isin(known_sha256)]
    filtered = filtered[~filtered["apk_name"].isin(known_apks)]
    print(f"[✓] 去重后剩余 {len(filtered)} 个新 APK 可选")

    if len(filtered) < total_needed:
        print(f"[!] 可用APK不足 {total_needed} 个，仅 {len(filtered)} 个。将全部输出。")
        total_needed = len(filtered)

    # 4️⃣ 随机抽样
    selected = filtered.sample(n=total_needed, random_state=42)

    # 5️⃣ 输出字段
    selected = selected[["sha256", "apk_name", "pkg_name", "size", "dex_date"]].copy()

    # 6️⃣ 拆分输出为多个 CSV
    batch_size = total_needed // num_splits
    for i in range(num_splits):
        start = i * batch_size
        end = total_needed if i == num_splits - 1 else (i + 1) * batch_size
        subset = selected.iloc[start:end]
        out_csv = os.path.join(output_dir, f"batch_{i+1}.csv")
        subset.to_csv(out_csv, index=False)
        print(f"[+] 输出 {len(subset)} 条到 {out_csv}")

    print(f"\n[✓] 所有输出完成，共 {total_needed} 条 APK 分为 {num_splits} 批。")


if __name__ == "__main__":
    # === 配置区 ===
    HISTORY_CSVS = [
        "F:\\test\\untested_simulator2.csv",
        "E:\\test\\untested_simulator1.csv",
        "D:\\NKU\\Work\\Work2\\appchina_output\\log.csv",
        "D:\\NKU\\Work\\Work2\\datasets\\aligned_log.csv"
    ]
    LATEST_CSV = "D:\\NKU\\Work\\Work2\\datasets\\androzoo\\latest.csv\\latest.csv"  # AndroZoo 最新大文件路径
    OUTPUT_DIR = "D:\\NKU\\Work\\Work2\\datasets\\androzoo\\new_batches"

    # prepare_new_apks(HISTORY_CSVS, LATEST_CSV, OUTPUT_DIR, total_needed=3000, num_splits=3)
    prepare_sensor_test_inputs(HISTORY_CSVS, LATEST_CSV, OUTPUT_DIR, total_needed=3000, num_splits=3)
