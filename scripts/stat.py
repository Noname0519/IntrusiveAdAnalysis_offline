valid_count = 0
total_count = 0

validity_txt = "/Users/noname/noname/nku/Work/IntrusiveAdAnalysis_offline/files/folder_validity.txt"

with open(validity_txt, "r", encoding="utf-8") as f:
    for line in f:
        parts = line.strip().split("\t")
        if len(parts) < 3:
            continue
        root_dir, apk_name, valid = parts
        total_count += 1
        if valid.strip() == "1":
            valid_count += 1

print(f"[+] Total entries: {total_count}")
print(f"[+] Valid entries (valid=1): {valid_count}")
print(f"[+] Invalid entries (valid=0): {total_count - valid_count}")
