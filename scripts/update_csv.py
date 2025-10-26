def update_input_csv_from_log_robust(input_csv=INPUT_CSV, log_csv=LOG_CSV, backup=True):
    """
    从LOG_CSV中查找已测试的应用，并更新INPUT_CSV中的is_tested字段
    包含备份和更多检查
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
        required_columns = ["is_tested", "apk_path", "sha256", "package_name"]
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
            "no_match": 0
        }

        # 用于跟踪已处理的标识符，避免重复
        processed_identifiers = set()

        # 更新INPUT_CSV中的记录
        for idx, row in tested_df.iterrows():
            apk_path = row.get("apk_path", "")
            sha256 = row.get("sha256", "")
            package_name = row.get("package_name", "")

            # 生成唯一标识符
            identifier = f"{apk_path}|{sha256}|{package_name}"

            # 跳过没有有效标识符的记录
            if not apk_path and not sha256 and not package_name:
                continue

            # 检查是否已处理过（避免重复）
            if identifier in processed_identifiers:
                continue

            processed_identifiers.add(identifier)

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

        # 保存更新后的INPUT_CSV
        input_df.to_csv(input_csv, index=False)

        print(f"\n[+] 更新完成统计:")
        print(f"    - 新标记为已测试: {stats['updated']} 个应用")
        print(f"    - 已经是已测试状态: {stats['already_updated']} 个应用")
        print(f"    - 未找到匹配记录: {stats['no_match']} 个应用")
        print(f"    - 已保存更新到: {input_csv}")

        return True

    except Exception as e:
        print(f"[!] 更新INPUT_CSV失败: {e}")
        import traceback
        traceback.print_exc()
        return False
