import subprocess
import os

def deploy_frida_server(adb_path="adb", frida_server_bin="frida-server"):
    """
    将 frida-server 推送到 /data/local/tmp 并启动
    """
    download_url = "https://github.com/frida/frida/releases/download/16.5.1/frida-server-16.5.1-android-x86_64.xz"
    
    device_serial = "http://13.209.173.201:44059/"

    # download and push
    
    if not os.path.exists(frida_server_bin):
        raise FileNotFoundError(f"未找到 frida-server 文件: {frida_server_bin}")

    target_path = "/data/local/tmp/frida-server"

    # Step 1: 推送
    subprocess.run([adb_path, "push", frida_server_bin, target_path], check=True)

    # Step 2: 修改权限
    subprocess.run([adb_path, "shell", "chmod", "755", target_path], check=True)

    # Step 3: 后台启动
    subprocess.run([adb_path, "shell", f"cd /data/local/tmp && ./frida-server &"], check=True)

    print("[DONE] frida-server 已启动")

if __name__ == "__main__":
    deploy_frida_server(
        adb_path="adb",  # 确保 adb 在 PATH 里
        frida_server_bin="./frida-server-16.4.5-android-arm64"  # 替换成你的文件路径
    )
