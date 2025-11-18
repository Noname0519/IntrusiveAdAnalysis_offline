import os
import pandas as pd
import json
import shutil
from datetime import datetime
import glob

class AppAnalyzer:
    def __init__(self, master_csv_path="master_apps.csv", master_txt_path="checked_apks.txt"):
        """
        初始化分析器
        
        Args:
            master_csv_path: 主CSV文件路径，包含所有应用信息
            master_txt_path: 主TXT文件路径，包含已检测的应用名称列表
        """
        self.master_csv_path = master_csv_path
        self.master_txt_path = master_txt_path
        
        # 初始化主CSV的列
        self.csv_columns = [
            'app_name', 'app_path', 'package_name', 'apk_path', 'sha256', 
            'is_tested', 'test_date', 'utg_exists', 'app_output_dir',
            'year', 'size', 'contain_ad', 'sensor_test_done', 'timestamp'
        ]
        
        # 确保主文件存在
        self._ensure_master_files()
    
    def _ensure_master_files(self):
        """确保主CSV和TXT文件存在"""
        # 确保CSV文件存在且有正确的列
        if not os.path.exists(self.master_csv_path):
            df = pd.DataFrame(columns=self.csv_columns)
            df.to_csv(self.master_csv_path, index=False)
            print(f"[+] 创建新的主CSV文件: {self.master_csv_path}")
        
        # 确保TXT文件存在
        if not os.path.exists(self.master_txt_path):
            with open(self.master_txt_path, 'w', encoding='utf-8') as f:
                f.write("# 已检测应用列表\n")
            print(f"[+] 创建新的主TXT文件: {self.master_txt_path}")
    
    def generate_master_files_from_analyze(self, analyzed_apps):
        """
        从分析结果生成主CSV和TXT文件
        
        Args:
            analyzed_apps: 分析过的应用列表，每个元素是包含应用信息的字典
        """
        try:
            # 读取现有的主CSV
            if os.path.exists(self.master_csv_path) and os.path.getsize(self.master_csv_path) > 0:
                master_df = pd.read_csv(self.master_csv_path, dtype=str).fillna("")
            else:
                master_df = pd.DataFrame(columns=self.csv_columns)
            
            # 读取现有的TXT文件
            existing_apps = set()
            if os.path.exists(self.master_txt_path) and os.path.getsize(self.master_txt_path) > 0:
                with open(self.master_txt_path, 'r', encoding='utf-8') as f:
                    for line in f:
                        line = line.strip()
                        if line and not line.startswith('#'):
                            existing_apps.add(line)
            
            added_count = 0
            updated_count = 0
            
            # 处理每个分析过的应用
            for app_info in analyzed_apps:
                app_name = app_info.get('app_name', '')
                if not app_name:
                    continue
                
                # 检查是否已存在
                existing_mask = master_df['app_name'] == app_name
                
                if existing_mask.any():
                    # 更新现有记录
                    for idx in master_df[existing_mask].index:
                        for col, value in app_info.items():
                            if col in master_df.columns and value:
                                master_df.loc[idx, col] = value
                        master_df.loc[idx, 'is_tested'] = 'TRUE'
                        master_df.loc[idx, 'test_date'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    updated_count += 1
                else:
                    # 添加新记录
                    new_row = {col: '' for col in self.csv_columns}
                    new_row.update(app_info)
                    new_row['is_tested'] = 'TRUE'
                    new_row['test_date'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    
                    master_df = pd.concat([master_df, pd.DataFrame([new_row])], ignore_index=True)
                    added_count += 1
                
                # 添加到TXT文件（如果不存在）
                if app_name not in existing_apps:
                    existing_apps.add(app_name)
            
            # 保存更新后的文件
            master_df.to_csv(self.master_csv_path, index=False)
            
            with open(self.master_txt_path, 'w', encoding='utf-8') as f:
                f.write("# 已检测应用列表\n")
                for app_name in sorted(existing_apps):
                    f.write(f"{app_name}\n")
            
            print(f"\n[+] 主文件更新完成:")
            print(f"    - 新增应用: {added_count}")
            print(f"    - 更新应用: {updated_count}")
            print(f"    - 总应用数: {len(master_df)}")
            print(f"    - CSV文件: {self.master_csv_path}")
            print(f"    - TXT文件: {self.master_txt_path}")
            
            return True
            
        except Exception as e:
            print(f"[!] 生成主文件失败: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def scan_multiple_folders(self, folder_paths, recursive=True):
        """
        扫描多个文件夹，查找新的应用测试结果
        
        Args:
            folder_paths: 文件夹路径列表
            recursive: 是否递归搜索子文件夹
            
        Returns:
            list: 新发现的应用信息列表
        """
        try:
            # 读取已检查的应用列表
            checked_apps = self._load_checked_apps()
            
            new_apps = []
            
            for folder_path in folder_paths:
                if not os.path.exists(folder_path):
                    print(f"[!] 文件夹不存在: {folder_path}")
                    continue
                
                print(f"[+] 扫描文件夹: {folder_path}")
                
                # 查找所有可能的应用结果文件夹
                app_folders = self._find_app_folders(folder_path, recursive)
                print(f"    找到 {len(app_folders)} 个可能的应用文件夹")
                
                for app_folder in app_folders:
                    app_name = os.path.basename(app_folder.rstrip(os.sep))
                    
                    # 检查是否已记录
                    if app_name in checked_apps:
                        continue
                    
                    # 检查是否存在utg.js
                    utg_path = os.path.join(app_folder, "utg.js")
                    if not os.path.exists(utg_path):
                        continue
                    
                    # 提取应用信息
                    app_info = self._extract_app_info(app_folder, app_name)
                    if app_info:
                        new_apps.append(app_info)
                        print(f"[+] 发现新应用: {app_name}")
            
            return new_apps
            
        except Exception as e:
            print(f"[!] 扫描文件夹失败: {e}")
            import traceback
            traceback.print_exc()
            return []
    
    def _load_checked_apps(self):
        """加载已检查的应用列表"""
        checked_apps = set()
        
        if os.path.exists(self.master_txt_path) and os.path.getsize(self.master_txt_path) > 0:
            with open(self.master_txt_path, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#'):
                        checked_apps.add(line)
        
        return checked_apps
    
    def _find_app_folders(self, root_path, recursive=True):
        """查找所有可能的应用文件夹"""
        app_folders = []
        
        if recursive:
            # 递归搜索所有子文件夹
            for root, dirs, files in os.walk(root_path):
                # 检查当前目录是否包含utg.js
                if "utg.js" in files:
                    app_folders.append(root)
        else:
            # 只搜索直接子文件夹
            for item in os.listdir(root_path):
                item_path = os.path.join(root_path, item)
                if os.path.isdir(item_path):
                    utg_path = os.path.join(item_path, "utg.js")
                    if os.path.exists(utg_path):
                        app_folders.append(item_path)
        
        return app_folders
    
    def _extract_app_info(self, app_folder, app_name):
        """从应用文件夹中提取信息"""
        try:
            app_info = {
                'app_name': app_name,
                'app_path': app_folder,
                'utg_exists': 'TRUE',
                'test_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            
            # 尝试从utg.js中提取更多信息
            utg_path = os.path.join(app_folder, "utg.js")
            if os.path.exists(utg_path):
                try:
                    with open(utg_path, 'r', encoding='utf-8') as f:
                        utg_content = f.read()
                        # 尝试解析JSON（utg.js通常是JSON格式）
                        if utg_content.strip().startswith('{'):
                            utg_data = json.loads(utg_content)
                            package_name = utg_data.get('packageName', '')
                            if package_name:
                                app_info['package_name'] = package_name
                except:
                    # 如果解析失败，跳过
                    pass
            
            # 尝试查找APK文件
            apk_files = glob.glob(os.path.join(app_folder, "*.apk"))
            if apk_files:
                app_info['apk_path'] = apk_files[0]
            
            # 尝试读取其他可能存在的元数据文件
            meta_files = ['app_info.json', 'metadata.json', 'analysis_result.json']
            for meta_file in meta_files:
                meta_path = os.path.join(app_folder, meta_file)
                if os.path.exists(meta_path):
                    try:
                        with open(meta_path, 'r', encoding='utf-8') as f:
                            meta_data = json.load(f)
                            # 提取有用的字段
                            for key in ['sha256', 'year', 'size', 'contain_ad', 'sensor_test_done', 'timestamp']:
                                if key in meta_data:
                                    app_info[key] = str(meta_data[key])
                    except:
                        pass
            
            return app_info
            
        except Exception as e:
            print(f"[!] 提取应用信息失败 {app_folder}: {e}")
            return None
    
    def add_new_apps_to_master(self, folder_paths, recursive=True, auto_save=True):
        """
        扫描文件夹并将新发现的应用添加到主文件
        
        Args:
            folder_paths: 文件夹路径列表
            recursive: 是否递归搜索
            auto_save: 是否自动保存到主文件
            
        Returns:
            tuple: (新应用列表, 是否成功)
        """
        try:
            new_apps = self.scan_multiple_folders(folder_paths, recursive)
            
            if not new_apps:
                print("[!] 未发现新的应用")
                return [], True
            
            print(f"\n[+] 发现 {len(new_apps)} 个新应用:")
            for app in new_apps:
                print(f"    - {app['app_name']} ({app.get('package_name', 'Unknown')})")
            
            if auto_save:
                success = self._add_apps_to_master_files(new_apps)
                return new_apps, success
            else:
                return new_apps, True
            
        except Exception as e:
            print(f"[!] 添加新应用失败: {e}")
            return [], False
    
    def _add_apps_to_master_files(self, new_apps):
        """将新应用添加到主文件"""
        try:
            # 读取现有CSV
            if os.path.exists(self.master_csv_path) and os.path.getsize(self.master_csv_path) > 0:
                master_df = pd.read_csv(self.master_csv_path, dtype=str).fillna("")
            else:
                master_df = pd.DataFrame(columns=self.csv_columns)
            
            # 读取现有TXT
            checked_apps = self._load_checked_apps()
            
            added_count = 0
            
            # 添加新应用到CSV
            for app_info in new_apps:
                app_name = app_info.get('app_name', '')
                if not app_name or app_name in checked_apps:
                    continue
                
                # 创建新行
                new_row = {col: '' for col in self.csv_columns}
                new_row.update(app_info)
                new_row['is_tested'] = 'TRUE'
                
                master_df = pd.concat([master_df, pd.DataFrame([new_row])], ignore_index=True)
                checked_apps.add(app_name)
                added_count += 1
            
            # 保存文件
            master_df.to_csv(self.master_csv_path, index=False)
            
            with open(self.master_txt_path, 'w', encoding='utf-8') as f:
                f.write("# 已检测应用列表\n")
                for app_name in sorted(checked_apps):
                    f.write(f"{app_name}\n")
            
            print(f"[+] 成功添加 {added_count} 个新应用到主文件")
            return True
            
        except Exception as e:
            print(f"[!] 添加到主文件失败: {e}")
            return False
    
    def get_master_stats(self):
        """获取主文件的统计信息"""
        try:
            if not os.path.exists(self.master_csv_path) or os.path.getsize(self.master_csv_path) == 0:
                return {"total_apps": 0, "tested_apps": 0}
            
            df = pd.read_csv(self.master_csv_path, dtype=str).fillna("")
            
            total_apps = len(df)
            tested_apps = len(df[df['is_tested'].str.upper().isin(['TRUE', 'T', '1', 'YES', 'Y'])])
            utg_exists = len(df[df['utg_exists'].str.upper().isin(['TRUE', 'T', '1', 'YES', 'Y'])])
            
            stats = {
                "total_apps": total_apps,
                "tested_apps": tested_apps,
                "utg_exists": utg_exists,
                "tested_percentage": (tested_apps / total_apps * 100) if total_apps > 0 else 0
            }
            
            return stats
            
        except Exception as e:
            print(f"[!] 获取统计信息失败: {e}")
            return {"total_apps": 0, "tested_apps": 0}
    
    def print_master_stats(self):
        """打印主文件统计信息"""
        stats = self.get_master_stats()
        
        print(f"\n📊 主文件统计信息:")
        print(f"   总应用数: {stats['total_apps']}")
        print(f"   已测试应用: {stats['tested_apps']}")
        print(f"   有UTG文件: {stats['utg_exists']}")
        print(f"   测试完成率: {stats['tested_percentage']:.1f}%")

# 使用示例
def main():
    # 初始化分析器
    analyzer = AppAnalyzer(
        master_csv_path="all_apps_master.csv",
        master_txt_path="checked_apps.txt"
    )
    
    # 示例1: 从分析结果生成主文件
    # analyzed_apps = ["F:\\test\\merge_output.csv",
    #                  "E:\\test\\untested_simulator1.csv",

    #                  ]  # 你的分析结果
    # analyzer.generate_master_files_from_analyze(analyzed_apps)
    
    # 示例2: 扫描多个文件夹并添加新应用
    folders_to_scan = [
        "D:\\NKU\\Work\\Work2\\fraudulent_output",
        "D:\\NKU\\Work\\Work2\\datasets\\manual_analysis\\output", 
        "D:\\NKU\\Work\\Work2\\datasets\\chin\\output",
        "D:\\NKU\\Work\\Work2\\datasets\\manual_analysis\\test_adgpe_test"
    ]
    
    new_apps, success = analyzer.add_new_apps_to_master(folders_to_scan, recursive=True)
    
    # 打印统计信息
    analyzer.print_master_stats()

if __name__ == "__main__":
    main()