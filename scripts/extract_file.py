import os

def collect_subfolders_with_validity(root_dirs, output_txt="folder_validity.txt"):
    """
    遍历多个输出根文件夹，提取所有子文件夹名，并检查是否包含 utg.js。
    结果写入 txt，格式：
        根文件夹路径 子文件夹名 有效性(1或0)
    """
    results = []

    for root in root_dirs:
        if not os.path.exists(root):
            print(f"[!] 跳过不存在的目录: {root}")
            continue

        print(f"[+] 正在扫描: {root}")
        try:
            subfolders = [f for f in os.listdir(root) if os.path.isdir(os.path.join(root, f))]
        except PermissionError:
            print(f"[!] 无法访问目录: {root}")
            continue

        for folder in subfolders:
            folder_path = os.path.join(root, folder)
            utg_path = os.path.join(folder_path, "utg.js")
            validity = 1 if os.path.exists(utg_path) else 0
            results.append((root, folder, validity))

    # 写出到文件
    with open(output_txt, "w", encoding="utf-8") as f:
        for root, folder, valid in results:
            f.write(f"{root}\t{folder}\t{valid}\n")

    print(f"[✔] 结果已写入: {output_txt}")
    print(f"[✔] 共扫描 {len(results)} 个子文件夹")


# =============================
# 主程序入口
# =============================
if __name__ == "__main__":
    root_dirs = [
        r"F:\\test\\output",
        r"E:\\test\\output",
        r"D:\\NKU\Work\Work2\\appchina_output",
        r"D:\\NKU\Work\Work2\datasets\androzoo\androzoo_output"
    ]
    collect_subfolders_with_validity(root_dirs, output_txt="folder_validity.txt")
