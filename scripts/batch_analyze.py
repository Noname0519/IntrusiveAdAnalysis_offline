import os
import json
import shutil
import csv
import pandas as pd
from datetime import datetime
import time

class dynamic_graph():
    def __init__(self, js_path=None, json_path=None):

        self.ad_screenshots_dir = "F:\\test\\ad_screenshots"
        self.ad_ui_files_dir = "F:\\test\\ad_ui_files"

        if json_path is None:
            self.json_path = js_path.replace("js", "json")
        else:
            self.json_path = json_path
        if js_path is not None:
            ret = self.change_js_to_json(js_path, self.json_path)
            
        self.state = {}
        self.state_edge = []
        self.state_edge_json = []
        self.activity = {}
        self.edge = []
        try:
            with open(self.json_path, encoding='utf-8') as f:
                j = json.load(f)
        except Exception as e:
            print(f"加载UTG失败: {e}")
            return

        self.raw_utg = j  # 保存原始utg，方便后续增强

        for js_node in j.setdefault('nodes', []):
            id = js_node['id']
            act = js_node['activity']
            self.state[id] = js_node
            self.activity.setdefault(act, {}).setdefault('state', []).append(id)

        for js_transition in j.setdefault('edges', []):
            src = js_transition['from']
            dst = js_transition['to']
            trigger = js_transition['events']

            src_ad_related = self.state[src].get("is_ad_related", False)
            src_ad_feature = self.state[src].get("ad_feature", {})
            src_is_external = self.state[src].get("is_external_site", False)

            dst_ad_related = self.state[dst].get("is_ad_related", False)
            dst_ad_feature = self.state[dst].get("ad_feature", {})
            dst_is_external = self.state[dst].get("is_external_site", False)
 
            # self.state[src].setdefault('src', {})[dst] = {'is_ad_related': is_ad_related, 'events': trigger}
            # self.state[dst].setdefault('dst', {})[src] = {'is_ad_related': is_ad_related, 'events': trigger}

            # 在 src 节点里记录 dst 邻居，同时写入广告标记
            self.state[src].setdefault('dst', {})[dst] = {
                'is_ad_related': dst_ad_related, 
                'is_external_site': dst_is_external,
                'events': trigger,
                'ad_feature': dst_ad_feature
            }

            # 在 dst 节点里记录 src 邻居，同时写入广告标记
            self.state[dst].setdefault('src', {})[src] = {
                'is_ad_related': src_ad_related, 
                'is_external_site': src_is_external,
                'events': trigger,
                'ad_feature': src_ad_feature
            }

            self.state_edge.append([src, dst])
            self.state_edge_json.append(js_transition)
            src_act = self.state[src]['activity']
            dst_act = self.state[dst]['activity']
            if src_act != dst_act:
                edge = [src_act, dst_act]
                if edge not in self.edge:
                    self.edge.append(edge)
                    self.activity[src_act].setdefault('src', {})[dst_act] = {'trigger': trigger, 'weight': 1}
                    self.activity[dst_act].setdefault('dst', {})[src_act] = {'trigger': trigger, 'weight': 1}
                else:
                    self.activity[src_act]['src'][dst_act]['weight'] += 1
                    self.activity[dst_act]['dst'][src_act]['weight'] += 1
                    self.activity[src_act]['src'][dst_act]['trigger'] += trigger
                    self.activity[dst_act]['dst'][src_act]['trigger'] += trigger

    def change_js_to_json(self, js_path, json_path, encoding="utf-8"):
        
        with open(js_path, encoding=encoding) as f:
            js_content = f.read().strip()
        

        # 去掉前缀 "var utg ="
        if js_content.startswith("var utg"):
            js_content = js_content[js_content.index("{"):]  

        # 去掉结尾的分号
        if js_content.endswith(";"):
            js_content = js_content[:-1]

        # 确保目标目录存在
        os.makedirs(os.path.dirname(json_path), exist_ok=True)

        with open(json_path, "w", encoding=encoding) as f:
            f.write(js_content)
        print(f"[+] 转换完成: {json_path}")

    def _load_false_positive_keywords(self, file_path):
        """
        加载误识别关键词文件
        """
        false_positive_keywords = set()
        
        if os.path.exists(file_path):
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    for line in f:
                        line = line.strip()
                        # 跳过空行和注释
                        if line and not line.startswith('#'):
                            false_positive_keywords.add(line)
                print(f"[+] 已加载 {len(false_positive_keywords)} 个误识别关键词")
            except Exception as e:
                print(f"[-] 加载误识别关键词文件失败: {e}")
                # 使用默认的误识别关键词
                false_positive_keywords = set([
                    "loading", "leading", "headline", "header", "footer", "bottom", 
                    "top", "progress", "loader", "indicator", "status", "bar", "banner_generic"
                ])
        else:
            print(f"[-] 误识别关键词文件不存在: {file_path}")
            # 使用默认的误识别关键词
            false_positive_keywords = set([
                "loading", "leading", "headline", "header", "footer", "bottom", 
                "top", "progress", "loader", "indicator", "status", "bar", "banner_generic"
            ])
        
        return false_positive_keywords

    # def enhance_utg(self, root_dir, keywords=None, save_back=True):
    #     """
    #     遍历UTG节点，根据对应的 state.json 检测广告相关视图，增强节点信息，并记录日志
    #     """
    #     log_file=os.path.join(root_dir, "enhanced_log.txt")

    #     false_positive_file = os.path.join(root_dir, "false_positive_keywords.txt")
    #     false_positive_keywords = self.

    #     if keywords is None:
    #         keywords = ["ad_contain", "ad_view", "advertisement", "广告", "ad_icon", "ad_title", "adView"]
        
    #     false_positive_keywords = ["load", "loading", "lead", "leading", "adapter", "adopt", "adapt"]
    #     false_positive_nodes = []

    #     nodes = self.raw_utg.get("nodes", [])
    #     log_entries = []

    #     false_positive_nodes = []
    #     # check false_positive
    #     for node in nodes:
    #         if node.get("is_ad_related", False):
    #             ad_features = node.get("ad_feature", [])
    #             is_false_positive = False

    #             for feature in ad_features:
    #                 if isinstance(feature, dict):
    #                     for value in feature.values():
    #                         if any(fp_kw in str(value).lower() for fp_kw in false_positive_keywords):
    #                             is_false_positive = True
    #                             break
    #                 elif any(fp_kw in str(feature).lower() for fp_kw in false_positive_keywords):
    #                     is_false_positive = True

    #             if is_false_positive:
    #                 false_positive_nodes.append(node["id"])

    #                 node["is_ad_related"] = False
    #                 if "ad_feature" in node:
    #                     node["ad_feature"] = []
    #                 if "ad_format" in node:
    #                     node["ad_format"] = []

    #                 log_line = f"⚠️  Node {node['id']} 被修正：原广告识别为误识别（包含过滤关键词）"
    #                 print("[-] " + log_line)
    #                 log_entries.append(log_line)

    #     for node in nodes:
    #         #if not node.get("is_ad_related", False): # 暂时注释，有些ad_feature & ad_format存不下来

    #         if node["id"] in false_positive_nodes:
    #             continue    
            
    #         image_path = node.get("image")
            
    #         if not image_path:
    #             continue

    #         # 生成对应的 state.json 文件路径
    #         # state_json_name = os.path.basename(image_path).replace("screen", "state").replace(".png", ".json")
    #         # state_json_path = os.path.join(root_dir, state_json_name)

    #         image_path = image_path.replace("\\", "/")
    #         state_json_name = os.path.basename(image_path).replace("screen", "state").replace(".png", ".json")
            
    #         state_json_path = os.path.normpath(os.path.join(root_dir,"states", state_json_name))

    #         if not os.path.exists(state_json_path):
    #             continue

    #         try:
    #             with open(state_json_path, "r", encoding="utf-8") as sf:
    #                 state_data = json.load(sf)
    #         except Exception as e:
    #             print(f"读取 {state_json_path} 失败: {e}")
    #             continue

    #         views = state_data.get("views", [])
    #         ad_features = []
    #         ad_formats = set()

    #         for view in views:

    #             # skip modified nodes:
    #             has_false_positive = False
    #             # if node["id"] in false_positive_nodes:
    #             #     continue

    #             # if view.get("ad_feature"):
    #             #     ad_features.append(view["ad_feature"])

    #             #     if view.get("ad_format") is not None:
    #             #         node["ad_format"] = view["ad_format"]
    #             #         print(node["ad_format"])
    #             #         continue

    #             for field in ["resource_id", "text", "class"]:
                    
    #                 if field in view and view[field]:
    #                     if field in view and view[field]:
    #                         field_value = str(view[field]).lower()
    #                         if any(fp_kw in field_value for fp_kw in false_positive_keywords):
    #                             has_false_positive = True
    #                             break

    #                     # for kw in keywords:
                            
    #                     #     if kw.lower() in str(view[field]).lower():
    #                     #         #print(str(view[field]))
    #                     #         ad_features.append({field: view[field]})
    #                     #         #print(node["state_str"])

    #             if has_false_positive:
    #                 continue

    #             if view.get("ad_feature"):
    #                 ad_features.append(view["ad_feature"])

    #                 if view.get("ad_format") is not None:
    #                     ad_formats.add(view["ad_format"])
                
    #             # 关键词匹配（排除误识别后）
    #             for field in ["resource_id", "text"]:
    #                 if field in view and view[field]:
    #                     for kw in keywords:
    #                         field_value = str(view[field]).lower()
    #                         kw_lower = kw.lower()
                            
    #                         # 检查是否包含目标关键词但不包含误识别关键词
    #                         if (kw_lower in field_value and 
    #                             not any(fp_kw in field_value for fp_kw in false_positive_keywords)):
    #                             feature_entry = {field: view[field], "matched_keyword": kw}
    #                             if feature_entry not in ad_features:
    #                                 ad_features.append(feature_entry)

    #         # 处理广告格式
    #         if ad_formats:
    #             node["ad_format"] = list(ad_formats)
    #             if len(ad_formats) == 1:
    #                 node["ad_format"] = ad_formats.pop()

    #         if ad_features:
    #             was_ad_related = node.get("is_ad_related", False)
    #             node["is_ad_related"] = True
    #             node["ad_feature"] = ad_features

    #             if was_ad_related:
    #                 # 记录日志
    #                 log_line = f"Node {node['id']} ({image_path}) 被标记为广告相关, 特征: {ad_features}"
    #                 print("[+] " + log_line)
    #                 log_entries.append(log_line)

    #     # 更新 self.state 里的节点
    #     for node in nodes:
    #         self.state[node["id"]] = node

    #     # 保存增强后的UTG
    #     if save_back:
    #         enhanced_utg_path = os.path.join(root_dir, "enhanced_utg.json")
    #         with open(enhanced_utg_path, "w", encoding="utf-8") as f:
    #             json.dump(self.raw_utg, f, indent=2, ensure_ascii=False)
    #         print("[+] 增强UTG已保存到 enhanced_utg.json")

    #     # 保存日志
    #     if log_entries:
    #         with open(log_file, "a", encoding="utf-8") as lf:
    #             lf.write("\n".join(log_entries) + "\n")
    #         print(f"[+] 日志已写入 {log_file}")

    def enhance_utg(self, root_dir, keywords=None, save_back=True):
        """
        遍历UTG节点，根据对应的 state.json 检测广告相关视图，增强节点信息，并记录日志
        增加误识别过滤和现有节点检查逻辑
        """
        log_file = os.path.join(root_dir, "enhanced_log.txt")

        # 全局变量定义（可以在类初始化时定义）
        if not hasattr(self, 'ad_screenshots_dir'):
            self.ad_screenshots_dir = os.path.join(root_dir, "ad_screenshots")
        if not hasattr(self, 'ad_ui_files_dir'):
            self.ad_ui_files_dir = os.path.join(root_dir, "ad_ui_files")
        
        # 创建输出目录
        os.makedirs(self.ad_screenshots_dir, exist_ok=True)
        os.makedirs(self.ad_ui_files_dir, exist_ok=True)

        # if keywords is None:
        #     keywords = ["ad_contain", "ad_view", "advertisement", "广告", "ad_icon", "ad_title", "adView"]
        
        # # 误识别关键词过滤列表
        # false_positive_keywords = ["loading", "leading", "headline", "header", "footer", "bottom", "top", 
        #                         "progress", "loader", "indicator", "status", "bar", "banner_generic"]

        # false_positive_file = os.path.join(root_dir, "false_positive_keywords.txt")
        false_positive_file = "D:\\NKU\\Work\\Work2\\datasets\\androzoo\\false_positive_keywords.txt"
        false_positive_keywords = self._load_false_positive_keywords(false_positive_file)
        if keywords is None:
            keywords = ["ad_contain", "ad_view", "advertisement", "广告", "ad_icon", "ad_title", "adView"]
        
        
        nodes = self.raw_utg.get("nodes", [])
        log_entries = []

        # 初始化统计计数器
        stats = {
            "total_nodes": len(nodes),
            "original_ad_nodes": 0,  # 原始广告节点数
            "false_positive_corrected": 0,  # 误识别修正数
            "new_ad_nodes": 0,  # 新标记广告节点数
            "confirmed_ad_nodes": 0,  # 确认广告节点数
            "cleared_ad_nodes": 0,  # 清除广告标记节点数
            "ad_nodes_with_screenshots": 0,  # 成功复制截图的广告节点数
            "ad_nodes_with_ui_files": 0,  # 成功复制UI文件的广告节点数
            "ad_features_found": 0,  # 发现的广告特征总数
        }

        # 首先统计原始广告节点数
        stats["original_ad_nodes"] = sum(1 for node in nodes if node.get("is_ad_related", False))
        
        # 首先检查现有被识别为ad_related的节点是否存在误识别
        false_positive_nodes = []
        for node in nodes:
            if node.get("is_ad_related", False):
                ad_features = node.get("ad_feature", [])
                is_false_positive = False
                
                # 检查ad_feature中是否包含误识别关键词
                for feature in ad_features:
                    feature_str = str(feature).lower()
                    if any(fp_kw.lower() in feature_str for fp_kw in false_positive_keywords):
                        is_false_positive = True
                        break
                
                if is_false_positive:
                    false_positive_nodes.append(node["id"])
                    # 修正误识别
                    node["is_ad_related"] = False
                    if "ad_feature" in node:
                        del node["ad_feature"]
                    if "ad_format" in node:
                        del node["ad_format"]
                    
                    log_line = f"⚠️  Node {node['id']} 被修正：原广告识别为误识别（包含过滤关键词）"
                    print("[-] " + log_line)
                    log_entries.append(log_line)
                    stats["false_positive_corrected"] += 1

        # 处理每个节点
        for node in nodes:
            # 跳过已经被修正的误识别节点
            if node["id"] in false_positive_nodes:
                continue
                
            image_path = node.get("image") # states\screen_2025-10-15_110502.png
            
            if not image_path:
                continue

            image_path = image_path.replace("\\", "/")
            state_image_name = os.path.basename(image_path)
            state_image_path = os.path.normpath(os.path.join(root_dir, "states", state_image_name))
            state_json_name = os.path.basename(image_path).replace("screen", "state").replace(".png", ".json")
            state_json_path = os.path.normpath(os.path.join(root_dir, "states", state_json_name))

            if not os.path.exists(state_json_path):
                continue

            try:
                with open(state_json_path, "r", encoding="utf-8") as sf:
                    state_data = json.load(sf)
            except Exception as e:
                print(f"读取 {state_json_path} 失败: {e}")
                continue

            views = state_data.get("views", [])
            ad_features = []
            ad_formats = set()

            for view in views:
                # 检查是否包含误识别关键词
                has_false_positive = False
                for field in ["resource_id", "text", "class", "content_description"]:
                    if field in view and view[field]:
                        field_value = str(view[field]).lower()
                        if any(fp_kw in field_value for fp_kw in false_positive_keywords):
                            has_false_positive = True
                            break
                
                # 如果包含误识别关键词，跳过该view
                if has_false_positive:
                    continue
                    
                # if view.get("ad_feature"):
                #     ad_features.append(view["ad_feature"])

                #     if view.get("ad_format") is not None:
                #         ad_formats.add(view["ad_format"])
                if view.get("ad_feature"):
                    # 再次检查广告特征本身是否包含误识别词
                    ad_feature_str = str(view["ad_feature"]).lower()
                    if not any(fp_kw.lower() in ad_feature_str for fp_kw in false_positive_keywords):
                        ad_features.append(view["ad_feature"])
                        stats["ad_features_found"] += 1

                    if view.get("ad_format") is not None:
                        ad_formats.add(view["ad_format"])

                # 关键词匹配（排除误识别后）
                for field in ["resource_id", "text"]:
                    if field in view and view[field]:

                        field_value = str(view[field]).lower()
                        has_fp_in_field = any(fp_kw.lower() in field_value for fp_kw in false_positive_keywords)
                        if has_fp_in_field:
                            continue

                        for kw in keywords:
                            # field_value = str(view[field]).lower()
                            kw_lower = kw.lower()
                            if kw_lower in field_value:
                                feature_entry = {
                                    field: view[field],
                                    "matched_keyword": kw,
                                    "view_class": view.get("class", "Unknown")
                                }
                            
                                # 检查是否包含目标关键词但不包含误识别关键词
                                # if (kw_lower in field_value and 
                                #     not any(fp_kw in field_value for fp_kw in false_positive_keywords)):
                                #     feature_entry = {field: view[field], "matched_keyword": kw}
                                #     if feature_entry not in ad_features:
                                #         ad_features.append(feature_entry)
                                if not any(fe.get(field) == view[field] and fe.get("matched_keyword") == kw 
                                        for fe in ad_features):
                                    ad_features.append(feature_entry)
                                    stats["ad_features_found"] += 1

            # 处理广告格式
            if ad_formats:
                node["ad_format"] = list(ad_formats)
                if len(ad_formats) == 1:
                    node["ad_format"] = ad_formats.pop()

            # 只有在有有效广告特征时才标记为广告相关
            if ad_features:
                # 检查是否是新发现的广告节点
                was_ad_related = node.get("is_ad_related", False)
                node["is_ad_related"] = True
                node["ad_feature"] = ad_features

                if was_ad_related:
                    log_line = f"✅ Node {node['id']} ({state_image_name}) 确认广告相关, 特征: {len(ad_features)}个"
                    stats["confirmed_ad_nodes"] += 1
                else:
                    log_line = f"🎯 Node {node['id']} ({state_image_name}) 新标记为广告相关, 特征: {len(ad_features)}个"
                    stats["new_ad_nodes"] += 1

                print("[+] " + log_line)
                log_entries.append(log_line)
                
                # 详细特征日志
                for i, feature in enumerate(ad_features, 1):
                    detail_log = f"   特征{i}: {feature}"
                    log_entries.append(detail_log)

                # 复制截图和UI文件
                try:
                    # 复制截图
                    if os.path.exists(state_image_path):
                        screenshot_dest = os.path.join(self.ad_screenshots_dir, state_image_name)
                        shutil.copy2(state_image_path, screenshot_dest)
                        stats["ad_nodes_with_screenshots"] += 1
                    
                    # 复制UI文件（state.json）
                    if os.path.exists(state_json_path):
                        # ui_file_dest = os.path.join(self.ad_ui_files_dir, f"node_{node['id']}.json")
                        ui_file_dest = os.path.join(self.ad_ui_files_dir, state_json_name)
                        shutil.copy2(state_json_path, ui_file_dest)
                        stats["ad_nodes_with_ui_files"] += 1
                        
                except Exception as e:
                    print(f"[!] 复制广告节点文件失败: {e}")

            else:
                # 如果没有广告特征但之前被标记为广告相关，进行清理
                if node.get("is_ad_related", False):
                    node["is_ad_related"] = False
                    if "ad_feature" in node:
                        del node["ad_feature"]
                    if "ad_format" in node:
                        del node["ad_format"]
                    
                    log_line = f"🧹 Node {node['id']} 广告标记被清除：未发现有效广告特征"
                    print("[-] " + log_line)
                    log_entries.append(log_line)
                    stats["cleared_ad_nodes"] += 1

        # 更新 self.state 里的节点
        for node in nodes:
            self.state[node["id"]] = node

        # 保存增强后的UTG
        if save_back:
            enhanced_utg_path = os.path.join(root_dir, "enhanced_utg.json")
            with open(enhanced_utg_path, "w", encoding="utf-8") as f:
                json.dump(self.raw_utg, f, indent=2, ensure_ascii=False)
            print("[+] 增强UTG已保存到 enhanced_utg.json")

        # 保存日志
        if log_entries:
            with open(log_file, "a", encoding="utf-8") as lf:
                lf.write("\n" + "="*50 + "\n")
                lf.write(f"增强日志 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                lf.write("="*50 + "\n")
                lf.write("\n".join(log_entries) + "\n")
            print(f"[+] 日志已写入 {log_file}")
        
        # 保存广告节点信息到单独文件
        self._save_ad_nodes_info(root_dir, nodes)
            
        # 输出统计信息
        final_ad_nodes = sum(1 for node in nodes if node.get("is_ad_related", False))
        ad_edges_count = sum(1 for edge in self.state_edge_json if edge.get("is_ad_related", False))

        print(f"\n📊 增强UTG统计信息:")
        print(f"   总节点数: {stats['total_nodes']}")
        print(f"   原始广告节点: {stats['original_ad_nodes']}")
        print(f"   误识别修正: {stats['false_positive_corrected']}")
        print(f"   新标记广告节点: {stats['new_ad_nodes']}")
        print(f"   确认广告节点: {stats['confirmed_ad_nodes']}")
        print(f"   清除广告标记: {stats['cleared_ad_nodes']}")
        print(f"   最终广告节点: {final_ad_nodes}")
        print(f"   广告特征发现: {stats['ad_features_found']}")
        print(f"   复制截图成功: {stats['ad_nodes_with_screenshots']}")
        print(f"   复制UI文件成功: {stats['ad_nodes_with_ui_files']}")
        print(f"   广告相关边: {ad_edges_count}")

    def _save_ad_nodes_info(self, root_dir, nodes):
        """
        保存广告节点信息到单独文件
        """
        ad_nodes_info = []
        
        for node in nodes:
            if node.get("is_ad_related", False):
                node_info = {
                    "node_id": node["id"],
                    "image_path": node.get("image", ""),
                    "package_name": node.get("package_name", ""),
                    "is_ad_related": True,
                    "ad_features": node.get("ad_feature", []),
                    "ad_format": node.get("ad_format", ""),
                    "screenshot_path": os.path.join(self.ad_screenshots_dir, f"node_{node['id']}.png"),
                    "ui_file_path": os.path.join(self.ad_ui_files_dir, f"node_{node['id']}.json"),
                    "timestamp": datetime.now().isoformat()
                }
                ad_nodes_info.append(node_info)
        
        # 保存到JSON文件
        ad_info_file = os.path.join(root_dir, "ad_nodes_info.json")
        with open(ad_info_file, "w", encoding="utf-8") as f:
            json.dump(ad_nodes_info, f, indent=2, ensure_ascii=False, default=str)
        
        print(f"[+] 广告节点信息已保存到: {ad_info_file}")

    def _rebuild_edges(self):
        """
        重新构建边信息，基于增强后的节点属性更新边的广告相关标记
        """
        # 清空现有的边信息
        self.state_edge = []
        self.state_edge_json = []
        
        # 清空节点中的邻居信息
        for node_id in self.state:
            if 'src' in self.state[node_id]:
                self.state[node_id]['src'] = {}
            if 'dst' in self.state[node_id]:
                self.state[node_id]['dst'] = {}
        
        # 重新构建边
        for js_transition in self.raw_utg.setdefault('edges', []):
            src = js_transition['from']
            dst = js_transition['to']
            trigger = js_transition['events']

            # 使用增强后的节点属性
            src_ad_related = self.state[src].get("is_ad_related", False)
            src_ad_feature = self.state[src].get("ad_feature", {})
            src_is_external = self.state[src].get("is_external_site", False)

            dst_ad_related = self.state[dst].get("is_ad_related", False)
            dst_ad_feature = self.state[dst].get("ad_feature", {})
            dst_is_external = self.state[dst].get("is_external_site", False)

            # 更新边的广告相关标记
            js_transition['src_ad_related'] = src_ad_related
            js_transition['dst_ad_related'] = dst_ad_related
            js_transition['is_ad_related'] = src_ad_related or dst_ad_related

            # 在 src 节点里记录 dst 邻居，使用更新后的广告属性
            self.state[src].setdefault('dst', {})[dst] = {
                'is_ad_related': dst_ad_related, 
                'is_external_site': dst_is_external,
                'events': trigger,
                'ad_feature': dst_ad_feature
            }

            # 在 dst 节点里记录 src 邻居，使用更新后的广告属性
            self.state[dst].setdefault('src', {})[src] = {
                'is_ad_related': src_ad_related, 
                'is_external_site': src_is_external,
                'events': trigger,
                'ad_feature': src_ad_feature
            }

            self.state_edge.append([src, dst])
            self.state_edge_json.append(js_transition)
            
            # 更新活动级别的边
            src_act = self.state[src]['activity']
            dst_act = self.state[dst]['activity']
            if src_act != dst_act:
                edge = [src_act, dst_act]
                if edge not in self.edge:
                    self.edge.append(edge)
                    self.activity[src_act].setdefault('src', {})[dst_act] = {
                        'trigger': trigger, 
                        'weight': 1,
                        'is_ad_related': src_ad_related or dst_ad_related
                    }
                    self.activity[dst_act].setdefault('dst', {})[src_act] = {
                        'trigger': trigger, 
                        'weight': 1,
                        'is_ad_related': src_ad_related or dst_ad_related
                    }
                else:
                    self.activity[src_act]['src'][dst_act]['weight'] += 1
                    self.activity[dst_act]['dst'][src_act]['weight'] += 1
                    self.activity[src_act]['src'][dst_act]['trigger'] += trigger
                    self.activity[dst_act]['dst'][src_act]['trigger'] += trigger


        # def enhance_utg(self, root_dir, keywords=None, save_back=True):
        #     """
        #     遍历UTG节点，根据对应的 state.json 检测广告相关视图，增强节点信息
        #     """
        #     if keywords is None:
        #         keywords = ["ad_contain", "ad_view", "ad_banner", "advertisement"]

        #     nodes = self.raw_utg.get("nodes", [])
        #     for node in nodes:
        #         if not node.get("is_ad_related", False):
        #             image_path = node.get("image")
        #             if not image_path:
        #                 continue

        #             # 生成对应的 state.json 文件路径
        #             state_json_name = os.path.basename(image_path).replace("screen", "state").replace(".png", ".json")
        #             state_json_path = os.path.join(root_dir, state_json_name)

        #             if not os.path.exists(state_json_path):
        #                 continue

        #             try:
        #                 with open(state_json_path, "r", encoding="utf-8") as sf:
        #                     state_data = json.load(sf)
        #             except Exception as e:
        #                 print(f"读取 {state_json_path} 失败: {e}")
        #                 continue

        #             views = state_data.get("views", [])
        #             ad_features = []

        #             for view in views:
        #                 for field in ["resource_id", "text"]:
        #                     if field in view and view[field]:
        #                         for kw in keywords:
        #                             if kw.lower() in str(view[field]).lower():
        #                                 ad_features.append({field: view[field]})

        #             if ad_features:
        #                 node["is_ad_related"] = True
        #                 node["ad_feature"] = ad_features

        #     # 更新 self.state 里的节点
        #     for node in nodes:
        #         self.state[node["id"]] = node

        #     if save_back:
        #         with open("enhanced_utg.json", "w", encoding="utf-8") as f:
        #             json.dump(self.raw_utg, f, indent=2, ensure_ascii=False)
        #         print("[+] 增强UTG已保存到 enhanced_utg.json")


"""
def analyze(path):
    print("[+] test ... " + path)
    utg_fail_count = 0
    ad_count = 0
    summary = 0
    type2 = {}
    type3 = {}
    type4 = {}
    type5 = {}
    type6 = {}

    type2_list = []
    type3_list = []
    type4_list = []
    type5_list = []
    type6_list = []

    results = {
        'total_apk': 0,
        'apks_with_ad_states': 0,
        'apks_with_frida_logs': 0,
        'ad_state_counts': 0,

    }

    result = {
        "package_name": None,
        "sha": None,
        "has_ad": False
    }

    folders = os.listdir(path)
    for app_dir in folders:
        
        apk_path = os.path.join(path, app_dir)
        if not os.path.isdir(apk_path):
            continue
        
        utg_path = os.path.join(apk_path, "utg.js")
        print("utg_path: " + utg_path)
        if not os.path.exists(utg_path):
            print(f"[!] utg.js 不存在: {utg_path}")
            utg_fail_count = utg_fail_count + 1
            continue
        
        # utg_json_path = os.path.join(apk_path, "utg.json")
        # if not os.path.exists(utg_json_path):
        #     print(f"[!] utg.json 不存在: {utg_json_path}")
        #     continue

        utg = dynamic_graph(js_path=utg_path)
        utg.enhance_utg(apk_path)
        enhance_utg_path = os.path.join(apk_path, "enhanced_utg.json")
        enhanced_utg = dynamic_graph(json_path=enhance_utg_path)
        summary = summary + 1

        unique_data = getAdStatus(apk_path)
        if unique_data:
            ad_count += 1

            type2 = check_type2(enhanced_utg)
            type3 = check_type3(enhanced_utg)
            type4 = check_type4(enhanced_utg)
            type5 = check_type5(enhanced_utg)
            type6 = check_type6(enhanced_utg)

            if type2 != []:
                type2_list.append(type2)
            
            if type3 != []:
                type3_list.append(type3)

            if type4 != []:
                type4_list.append(type4)

            if type5 != []:
                type5_list.append(type5)

            if type5 != []:
                type6_list.append(type6)

    print("type2: ", str(len(type2_list)))
    print("type3: ", str(len(type3_list)))
    print("type4: ", str(len(type4_list)))
    print("type5: ", str(len(type5_list)))
    print("type6: ", str(len(type6_list)))
    print("summary: ", str(summary))
    print("failed count: ", str(utg_fail_count))

    paths_json = os.path.join(path, "path.json")
    results = extract_paths_to_ads(enhanced_utg, None, 20, paths_json)
    

    has_ad = False
    has_frida_logs = False

    #rets = check_type2(enhanced_utg)
    # for ret in rets:
    #     f"{ret['src_node']} -> {ret['event_type']} -> {ret['dst_ad_node']}"
    
    # ret3 = check_type3(enhanced_utg)
    # ret = check_type5(enhanced_utg)


    # check if the ad_states.json exist and store the ad state
    # data = getAdStates(path, enhance_utg_path, results)
    # if data:
    #     has_ad = True
        # print("Has ads")
        # get the list of ad states
        # print(data)


    # check if the frida_log.json exist.

    # stats for app with ads

    pass
"""

def analyze(path, output_csv="apk_analysis_results.csv"):
    """
    分析指定目录下的所有APK，并将结果保存到CSV文件中
    
    Args:
        path: 包含APK分析结果的根目录
        output_csv: 输出CSV文件的路径
    """
    print(f"[+] Starting analysis of directory: {path}")
    
    # 准备CSV文件
    fieldnames = [
        "app_name", 
        "has_ad", 
        "type2_detected", "type2_features",
        "type3_detected", "type3_features", 
        "type4_detected", "type4_features",
        "type5_detected", "type5_features", 
        "type6_detected", "type6_features",
        "analysis_date"
    ]
    
    # 创建或清空CSV文件并写入表头
    with open(output_csv, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
    
    # 获取所有APK目录
    folders = [f for f in os.listdir(path) if os.path.isdir(os.path.join(path, f))]
    print(f"[+] Found {len(folders)} APK directories")
    
    # 初始化统计计数器
    stats = {
        "total_apks": len(folders),
        "apks_with_ads": 0,
        "type2_count": 0,
        "type3_count": 0,
        "type4_count": 0,
        "type5_count": 0,
        "type6_count": 0,
        "failed_analysis": 0
    }
    
    # 分析每个APK
    for app_dir in folders:
        apk_path = os.path.join(path, app_dir)
        result = analyze_single_apk(apk_path, app_dir)
        
        # 更新统计信息
        if result:
            if result["has_ad"]:
                stats["apks_with_ads"] += 1
            
            if result["type2_detected"]:
                stats["type2_count"] += 1
                
            if result["type3_detected"]:
                stats["type3_count"] += 1
                
            if result["type4_detected"]:
                stats["type4_count"] += 1
                
            if result["type5_detected"]:
                stats["type5_count"] += 1
                
            if result["type6_detected"]:
                stats["type6_count"] += 1
            
            # 将结果写入CSV
            with open(output_csv, 'a', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writerow(result)
            
            print(f"[+] Results for {app_dir} saved to {output_csv}")
        else:
            stats["failed_analysis"] += 1
    
    # 生成并输出总体统计报告
    generate_summary_report(stats, output_csv)
    
    return stats
    
def generate_summary_report(stats, output_csv):
    """
    生成并输出总体统计报告
    
    Args:
        stats: 包含统计信息的字典
        output_csv: 输出CSV文件的路径
    """
    # 创建报告文件名
    report_file = output_csv.replace(".csv", "_summary.txt")
    
    # 计算百分比
    if stats["total_apks"] > 0:
        ad_percentage = (stats["apks_with_ads"] / stats["total_apks"]) * 100
        type2_percentage = (stats["type2_count"] / stats["apks_with_ads"]) * 100 if stats["apks_with_ads"] > 0 else 0
        type3_percentage = (stats["type3_count"] / stats["apks_with_ads"]) * 100 if stats["apks_with_ads"] > 0 else 0
        type4_percentage = (stats["type4_count"] / stats["apks_with_ads"]) * 100 if stats["apks_with_ads"] > 0 else 0
        type5_percentage = (stats["type5_count"] / stats["apks_with_ads"]) * 100 if stats["apks_with_ads"] > 0 else 0
        type6_percentage = (stats["type6_count"] / stats["apks_with_ads"]) * 100 if stats["apks_with_ads"] > 0 else 0
    else:
        ad_percentage = type2_percentage = type3_percentage = type4_percentage = type5_percentage = type6_percentage = 0
    
    # 创建报告内容
    report_content = f"""
    APK 分析总体统计报告
    生成时间: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
    ==================================================

    总体统计:
    - 分析的APK总数: {stats["total_apks"]}
    - 包含广告的APK数量: {stats["apks_with_ads"]} ({ad_percentage:.2f}%)
    - 分析失败的APK数量: {stats["failed_analysis"]}

    广告类型检测统计:
    - 检测到Type2的APK数量: {stats["type2_count"]} ({type2_percentage:.2f}% of ad-containing APKs)
    - 检测到Type3的APK数量: {stats["type3_count"]} ({type3_percentage:.2f}% of ad-containing APKs)
    - 检测到Type4的APK数量: {stats["type4_count"]} ({type4_percentage:.2f}% of ad-containing APKs)
    - 检测到Type5的APK数量: {stats["type5_count"]} ({type5_percentage:.2f}% of ad-containing APKs)
    - 检测到Type6的APK数量: {stats["type6_count"]} ({type6_percentage:.2f}% of ad-containing APKs)

    详细结果已保存至: {output_csv}
    """
    
    # 写入报告文件
    with open(report_file, 'w', encoding='utf-8') as f:
        f.write(report_content)
    
    # 打印报告到控制台
    print(report_content)
    
    # 生成可视化图表
    #generate_visualizations(stats, output_csv)
    
    return report_content

def analyze_single_apk(apk_path, app_dir):
    """
    分析单个APK目录
    
    Args:
        apk_path: APK目录的完整路径
        app_dir: APK目录名称（用作app_name）
    
    Returns:
        dict: 包含分析结果的字典
    """
    print(f"[+] Analyzing APK: {app_dir}")
    app_info = extract_app_info(apk_path, app_dir)
    if not app_info:
        app_info = {
            'app_name': app_dir,
            'app_path': apk_path,
            'utg_exists': 'TRUE',
            'test_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'is_tested': 'TRUE'
        }


    # 初始化结果字典
    result = {
        "app_name": app_dir,
        "has_ad": False,
        "type2_detected": False, "type2_features": "",
        "type3_detected": False, "type3_features": "",
        "type4_detected": False, "type4_features": "",
        "type5_detected": False, "type5_features": "",
        "type6_detected": False, "type6_features": "",
        "analysis_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    
    # 检查UTG文件是否存在
    utg_path = os.path.join(apk_path, "utg.js")
    if not os.path.exists(utg_path):
        print(f"[!] utg.js does not exist: {utg_path}")
        # 仍然更新全局文件，标记为已测试但无UTG
        app_info['utg_exists'] = 'FALSE'
        update_global_files(app_info, result)
        return result
    
    # 增强UTG
    try:
        utg = dynamic_graph(js_path=utg_path)
        utg.enhance_utg(apk_path)
        enhance_utg_path = os.path.join(apk_path, "enhanced_utg.json")
        enhanced_utg = dynamic_graph(json_path=enhance_utg_path)
    except Exception as e:
        print(f"[!] Failed to enhance UTG for {app_dir}: {e}")
        # 更新全局文件，标记分析失败
        update_global_files(app_info, result)
        return result
    
    # 检查广告状态
    unique_data = getAdStatus(apk_path)
    if unique_data:
        result["has_ad"] = True
        
        # 检查各种类型
        type2_results = check_type2(enhanced_utg)
        type3_results = check_type3(enhanced_utg)
        type4_results = check_type4(enhanced_utg)
        type5_results = check_type5(enhanced_utg)
        type6_results = check_type6(enhanced_utg)
        
        # 更新结果
        if type2_results:
            result["type2_detected"] = True
            result["type2_features"] = json.dumps(type2_results, ensure_ascii=False)
        
        if type3_results:
            result["type3_detected"] = True
            result["type3_features"] = json.dumps(type3_results, ensure_ascii=False)
        
        if type4_results:
            result["type4_detected"] = True
            result["type4_features"] = json.dumps(type4_results, ensure_ascii=False)
        
        if type5_results:
            result["type5_detected"] = True
            result["type5_features"] = json.dumps(type5_results, ensure_ascii=False)
        
        if type6_results:
            result["type6_detected"] = True
            result["type6_features"] = json.dumps(type6_results, ensure_ascii=False)
    
    # 更新全局文件
    update_global_files(app_info, result)

    return result

# single test
def analyze_test(path):

    results = {
        'total_apk': 0,
        'apks_with_ad_states': 0,
        'apks_with_frida_logs': 0,
        'ad_state_counts': 0,

    }

    result = {
        "package_name": None,
        "sha": None,
        "has_ad": False
    }

    utg_path = os.path.join(path, "utg.js")
    utg = dynamic_graph(js_path=utg_path)
    utg.enhance_utg(path)

    enhance_utg_path = os.path.join(path, "enhanced_utg.json")
    enhanced_utg = dynamic_graph(json_path=enhance_utg_path)

    has_ad = False
    has_frida_logs = False

    # result_list = extract_paths_to_ads(enhanced_utg)
    paths_json = os.path.join(path, "path.json")
    results = extract_paths_to_ads(enhanced_utg, None, 20, paths_json)
    print(results)

    rets = check_type2(enhanced_utg)
    for ret in rets:
        f"{ret['src_node']} -> {ret['event_type']} -> {ret['dst_ad_node']}"
    
    rets3 = check_type3(enhanced_utg)
    ret = check_type5(enhanced_utg)


    # check if the ad_states.json exist and store the ad state
    # data = getAdStates(path, enhance_utg_path, results)
    # if data:
    #     has_ad = True
        # print("Has ads")
        # get the list of ad states
        # print(data)


    # check if the frida_log.json exist.

    # stats for app with ads

    pass

def getAdStatus(app_path):
    # unique_data = None
    # print(f"[+] Start analyze ad states " + app_path)

    # ad_state = False
    # if not os.path.isdir(path):
    #     print(f"Error! The dir path is not exist -- {path}.")
    #     return None

    # for file_name in os.listdir(app_path):
    #     if file_name.endswith("ad_states.json"):
    #         ad_state_path = os.path.join(app_path, file_name)
    #         unique_data = get_unique_ad_states(ad_state_path)
    #         break
    
    # return unique_data
    """检查APK是否包含广告"""
    unique_data = None
    print(f"[+] Checking ad status for: {app_path}")

    if not os.path.isdir(app_path):
        print(f"Error! The dir path does not exist: {app_path}.")
        return None

    for file_name in os.listdir(app_path):
        if file_name.endswith("ad_states.json"):
            ad_state_path = os.path.join(app_path, file_name)
            unique_data = get_unique_ad_states(ad_state_path)
            break
    
    return unique_data

def getAdStatics(path, enhanced_utg_path, results):
    print("[+] start analyzing ad states")
    ad_states_path = None
    if not os.path.isdir(path):
        print(f"Error! The dir path is not exist -- {path}.")
        return None
    
    for apk_folder in os.listdir(path):
        apk_path = os.path.join(path, apk_folder)

        if not os.path.isdir(apk_path):
            continue
        
        results['total_apk'] += 1
        
        for file_name in os.listdir(path):
            #print(file_name)
            if file_name.endswith("ad_states.json"):
                # check if the ad_states.json exist and store the ad state
                ad_states_path = os.path.join(path, file_name)
                break
            
        unique_data = []
        seen_items = set() 
            
        if ad_states_path and os.path.isfile(ad_states_path):
            with open(ad_states_path, "r", encoding="utf-8") as f:
                try:
                    data = json.load(f)
                    for item in data:
                        state_id = item.get("state_str")
                        screenshot_path = item.get("screenshot_path")
                        if state_id not in seen_items and path in screenshot_path:
                            seen_items.add(state_id)
                            unique_data.append(item)
                except json.JSONDecodeError:
                    print("JSON format error: ", ad_states_path)
            
            # 存回去（覆盖原文件）
            # with open(ad_states_path, "w", encoding="utf-8") as f:
            #     json.dump(unique_data, f, indent=2, ensure_ascii=False)
            # 保存为副本
            dedup_path = ad_states_path.replace(".json", "_dedup.json")
            with open(dedup_path, "w", encoding="utf-8") as f:
                json.dump(unique_data, f, indent=2, ensure_ascii=False)

            print(f"去重后保存成功，共 {len(unique_data)} 条记录。")
        else:
            print("未找到 ad_states.json 文件")
            return None

    return unique_data

# 全局变量定义
GLOBAL_MASTER_CSV = "all_apps_master.csv"
GLOBAL_CHECKED_TXT = "checked_apps.txt"

def ensure_global_files():
    """确保全局CSV和TXT文件存在"""
    # 确保CSV文件存在且有正确的列
    csv_columns = [
        'app_name', 'app_path', 'package_name', 'apk_path', 'sha256', 
        'is_tested', 'test_date', 'utg_exists', 'app_output_dir',
        'year', 'size', 'contain_ad', 'sensor_test_done', 'timestamp',
        'has_ad', 'type2_detected', 'type3_detected', 'type4_detected',
        'type5_detected', 'type6_detected', 'analysis_date'
    ]
    
    if not os.path.exists(GLOBAL_MASTER_CSV):
        df = pd.DataFrame(columns=csv_columns)
        df.to_csv(GLOBAL_MASTER_CSV, index=False)
        print(f"[+] 创建全局主CSV文件: {GLOBAL_MASTER_CSV}")
    
    # 确保TXT文件存在
    if not os.path.exists(GLOBAL_CHECKED_TXT):
        with open(GLOBAL_CHECKED_TXT, 'w', encoding='utf-8') as f:
            f.write("# 已检测应用列表\n")
        print(f"[+] 创建全局TXT文件: {GLOBAL_CHECKED_TXT}")

def load_checked_apps():
    """加载已检查的应用列表"""
    checked_apps = set()
    
    if os.path.exists(GLOBAL_CHECKED_TXT) and os.path.getsize(GLOBAL_CHECKED_TXT) > 0:
        with open(GLOBAL_CHECKED_TXT, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#'):
                    checked_apps.add(line)
    
    return checked_apps

def update_global_files(app_info, analysis_result=None):
    """
    更新全局CSV和TXT文件
    
    Args:
        app_info: 应用基本信息字典
        analysis_result: 分析结果字典（可选）
    """
    try:
        # 读取现有CSV
        if os.path.exists(GLOBAL_MASTER_CSV) and os.path.getsize(GLOBAL_MASTER_CSV) > 0:
            master_df = pd.read_csv(GLOBAL_MASTER_CSV, dtype=str).fillna("")
        else:
            master_df = pd.DataFrame(columns=[
                'app_name', 'app_path', 'package_name', 'apk_path', 'sha256', 
                'is_tested', 'test_date', 'utg_exists', 'app_output_dir',
                'year', 'size', 'contain_ad', 'sensor_test_done', 'timestamp',
                'has_ad', 'type2_detected', 'type3_detected', 'type4_detected',
                'type5_detected', 'type6_detected', 'analysis_date'
            ])
        
        # 读取现有TXT
        checked_apps = load_checked_apps()
        
        app_name = app_info.get('app_name', '')
        if not app_name:
            return False
        
        # 合并应用信息和分析结果
        merged_info = app_info.copy()
        if analysis_result:
            merged_info.update(analysis_result)
        
        # 设置默认值
        merged_info.setdefault('is_tested', 'TRUE')
        merged_info.setdefault('test_date', datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        merged_info.setdefault('utg_exists', 'TRUE')
        merged_info.setdefault('analysis_date', datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        
        # 检查是否已存在
        existing_mask = master_df['app_name'] == app_name
        
        if existing_mask.any():
            # 更新现有记录
            for idx in master_df[existing_mask].index:
                for col, value in merged_info.items():
                    if col in master_df.columns and value:
                        master_df.loc[idx, col] = value
            print(f"[+] 更新全局记录: {app_name}")
        else:
            # 添加新记录
            new_row = {col: '' for col in master_df.columns}
            new_row.update(merged_info)
            master_df = pd.concat([master_df, pd.DataFrame([new_row])], ignore_index=True)
            checked_apps.add(app_name)
            print(f"[+] 添加全局记录: {app_name}")
        
        # 保存文件
        master_df.to_csv(GLOBAL_MASTER_CSV, index=False)
        
        with open(GLOBAL_CHECKED_TXT, 'w', encoding='utf-8') as f:
            f.write("# 已检测应用列表\n")
            for app_name in sorted(checked_apps):
                f.write(f"{app_name}\n")
        
        return True
        
    except Exception as e:
        print(f"[!] 更新全局文件失败: {e}")
        return False

def scan_and_update_multiple_folders(folder_paths, recursive=True):
    """
    扫描多个文件夹，将新的应用添加到全局文件
    
    Args:
        folder_paths: 文件夹路径列表
        recursive: 是否递归搜索子文件夹
        
    Returns:
        tuple: (新发现的应用数量, 总应用数量)
    """
    try:
        # 确保全局文件存在
        ensure_global_files()
        
        # 读取已检查的应用列表
        checked_apps = load_checked_apps()
        
        new_apps_count = 0
        total_apps_count = 0
        
        for folder_path in folder_paths:
            if not os.path.exists(folder_path):
                print(f"[!] 文件夹不存在: {folder_path}")
                continue
            
            print(f"[+] 扫描文件夹: {folder_path}")
            
            # 查找所有可能的应用结果文件夹
            app_folders = find_app_folders(folder_path, recursive)
            print(f"    找到 {len(app_folders)} 个可能的应用文件夹")
            
            for app_folder in app_folders:
                total_apps_count += 1
                app_name = os.path.basename(app_folder.rstrip(os.sep))
                
                # 检查是否已记录
                if app_name in checked_apps:
                    continue
                
                # 检查是否存在utg.js
                utg_path = os.path.join(app_folder, "utg.js")
                if not os.path.exists(utg_path):
                    continue
                
                # 提取应用信息
                app_info = extract_app_info(app_folder, app_name)
                if app_info:
                    # 更新全局文件
                    if update_global_files(app_info):
                        new_apps_count += 1
                        print(f"[+] 发现并记录新应用: {app_name}")
        
        print(f"\n[+] 扫描完成:")
        print(f"    - 总应用文件夹: {total_apps_count}")
        print(f"    - 新发现应用: {new_apps_count}")
        print(f"    - 全局CSV: {GLOBAL_MASTER_CSV}")
        print(f"    - 全局TXT: {GLOBAL_CHECKED_TXT}")
        
        return new_apps_count, total_apps_count
        
    except Exception as e:
        print(f"[!] 扫描文件夹失败: {e}")
        return 0, 0

def find_app_folders(root_path, recursive=True):
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

def extract_app_info(app_folder, app_name):
    """从应用文件夹中提取信息"""
    try:
        app_info = {
            'app_name': app_name,
            'app_path': app_folder,
            'utg_exists': 'TRUE',
            'test_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'is_tested': 'TRUE'
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
        import glob
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

def get_global_stats():
    """获取全局文件的统计信息"""
    try:
        if not os.path.exists(GLOBAL_MASTER_CSV) or os.path.getsize(GLOBAL_MASTER_CSV) == 0:
            return {"total_apps": 0, "tested_apps": 0}
        
        df = pd.read_csv(GLOBAL_MASTER_CSV, dtype=str).fillna("")
        
        total_apps = len(df)
        tested_apps = len(df[df['is_tested'].str.upper().isin(['TRUE', 'T', '1', 'YES', 'Y'])])
        utg_exists = len(df[df['utg_exists'].str.upper().isin(['TRUE', 'T', '1', 'YES', 'Y'])])
        has_ad = len(df[df['has_ad'].str.upper().isin(['TRUE', 'T', '1', 'YES', 'Y'])])
        
        stats = {
            "total_apps": total_apps,
            "tested_apps": tested_apps,
            "utg_exists": utg_exists,
            "has_ad": has_ad,
            "tested_percentage": (tested_apps / total_apps * 100) if total_apps > 0 else 0
        }
        
        return stats
        
    except Exception as e:
        print(f"[!] 获取全局统计信息失败: {e}")
        return {"total_apps": 0, "tested_apps": 0}

def print_global_stats():
    """打印全局统计信息"""
    stats = get_global_stats()
    
    print(f"\n📊 全局文件统计信息:")
    print(f"   总应用数: {stats['total_apps']}")
    print(f"   已测试应用: {stats['tested_apps']}")
    print(f"   有UTG文件: {stats['utg_exists']}")
    print(f"   包含广告: {stats['has_ad']}")
    print(f"   测试完成率: {stats['tested_percentage']:.1f}%")
    print(f"   全局CSV: {GLOBAL_MASTER_CSV}")
    print(f"   全局TXT: {GLOBAL_CHECKED_TXT}")

# def get_unique_ad_states(path):
#     print("[+] get unique ad status: " + path)
#     unique_data = []
#     seen_items = set() 
            
#     if path and os.path.isfile(path):
#         with open(path, "r", encoding="utf-8") as f:
#             try:
#                 data = json.load(f)
#                 if data is None:
#                     return None
                
#                 for item in data:
                    
#                     state_id = item.get("state_str")
#                     screenshot_path = item.get("screenshot_path")
#                     if state_id not in seen_items:
#                         seen_items.add(state_id)
#                         unique_data.append(item)
#             except json.JSONDecodeError:
#                 print("JSON format error: ", path)
#         print("unique_data: ", unique_data)
#         # 存回去（覆盖原文件）
#         # with open(ad_states_path, "w", encoding="utf-8") as f:
#         #     json.dump(unique_data, f, indent=2, ensure_ascii=False)
#         # 保存为副本
#         dedup_path = path.replace(".json", "_dedup.json")
#         with open(dedup_path, "w", encoding="utf-8") as f:
#             json.dump(unique_data, f, indent=2, ensure_ascii=False)

#         print(f"去重后保存成功，共 {len(unique_data)} 条记录。")
#     else:
#         print("未找到 ad_states.json 文件")
#         return None

#     return unique_data

def get_unique_ad_states(path):
    """获取唯一的广告状态"""
    print(f"[+] Getting unique ad states: {path}")
    unique_data = []
    seen_items = set()
    
    if path and os.path.isfile(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
                if data is None:
                    return None
                
                for item in data:
                    state_id = item.get("state_str")
                    if state_id and state_id not in seen_items:
                        seen_items.add(state_id)
                        unique_data.append(item)
        except json.JSONDecodeError:
            print(f"JSON format error: {path}")
            return None
        
        # 保存去重后的副本
        dedup_path = path.replace(".json", "_dedup.json")
        try:
            with open(dedup_path, "w", encoding="utf-8") as f:
                json.dump(unique_data, f, indent=2, ensure_ascii=False)
            print(f"Saved deduplicated ad states to {dedup_path}, total records: {len(unique_data)}")
        except Exception as e:
            print(f"Failed to save deduplicated ad states: {e}")
    else:
        print("ad_states.json file not found")
        return None

    return unique_data

def detect_misleading_UI(image_path):
    pass

def check_type2(graph):
    """
        遍历所有 ad-related 状态，检查其来源 src：
        - 如果 src 不是 ad-related / 是ad-related但是banner format
        - 且 event 不是 touchad_event
        - 且边的类型不是 adcomposite
        则认为是可疑的功能性中断
        """
    print("[+] Checking type2 ...")
    results = []

    for node_id, node in graph.state.items():
        
        if not node.get("is_ad_related", False):
            continue
        
        for src, edge_info in node.get("src", {}).items():
            src_node = graph.state[src]

            if src_node.get("is_ad_related", False) and src_node.get("ad_format") != "banner":
                continue
            
            for e in edge_info.get("events", []):
                etype = e.get("event_type", "").lower()

                detail = e.get("event_str").lower()
                if etype == "key" and "back" in detail:
                    etype = "key_back"

                if "adcomposite" in etype:
                    continue

                # 如果不是 touchad_event，则记录
                dst_activity = node.get("activity", "unknown")
                src_activity = src_node.get("activity", "unknown")
                if "touch_ad" not in etype:

                    if src_node.get("ad_format") == "banner":
                        results.append({
                            "dst_ad_node": node_id,
                            "src_node": src,
                            #"activity": node.get("activity"),
                            "dst_activity": node.get("activity", "unknown"),
                            "src_activity": src_node.get("activity", "unknown"),
                            "event_type": etype,
                            "edge_info": e,
                            "pattern": f"{src}(banner ad) --[{etype}]--> {node_id}(ad)"
                        })
                        print(f"  Found: {src}(banner ad){src_activity} --[{etype}]--> {node_id}(ad){dst_activity}")
                    else:
                        results.append({
                            "dst_ad_node": node_id,
                            "src_node": src,
                            #"activity": node.get("activity"),
                            "dst_activity": node.get("activity", "unknown"),
                            "src_activity": src_node.get("activity", "unknown"),
                            "event_type": etype,
                            "edge_info": e,
                            "pattern": f"{src}(non-ad) --[{etype}]--> {node_id}(ad)"
                        })

                        # 打印简洁的输出格式
                        print(f"  Found: {src}(non-ad){src_activity} --[{etype}]--> {node_id}(ad){dst_activity}")
    return results

def check_type3(graph):
    """
        两种情况：
        1. ad-related 节点 -> 按 back -> 仍然 ad-related (后续可做页面相似度检测)
        2. ad-related 节点，且 is_external = True -> back -> 仍然 external
        """
    
    print("[+] Checking type3")
    results = []
    for node_id, node in graph.state.items():
        
        if not node.get("is_ad_related", False):
            continue
        
        # 遍历src边，即进入这个广告的来源
        for src, edge_info in node.get("src", {}).items():
            # if src == "50b318375205e8450a077c6b68cfb85e":
            #     print("11111+", node_id)
            #     print(edge_info.get("is_ad_related", False))
            #     print(node.get("is_ad_related", False))
            #     print(edge_info)
            for e in edge_info.get("events", []):
                
                etype = e.get("event_type", "").lower()
                detail = e.get("event_str").lower()
                if etype == "key" and "back" in detail:
                    etype = "key_back"
                # if src == "cd2bdebcfacf84ca65b85335dd96d131":
                #     print(etype)

                if etype == "key_back": # no banner
                    # ignore case that external_site (ad-content) back to aad
                    if edge_info.get("is_external_site", False):
                        continue
                    # if src == "cd2bdebcfacf84ca65b85335dd96d131":
                    #     print(etype)
                    # cd2bdebcfacf84ca65b85335dd96d131-->72832348da8e84a26b234ef8a63fc5a7
                    if edge_info.get("is_ad_related", False):
                        
                        ret = {
                            "node": node_id,
                            "activity": node.get("activity"),
                            "case": "back_still_ad",
                            "event": etype,
                            "edge_info": e,
                            "pattern": f"{src}(ad-related) --[{etype}]--> {node_id}(ad-related)"
                        }
                        if ret not in results:
                            results.append(ret)
                        print(f"  Found: {src}(ad) --[{etype}]--> {node_id}(ad)")
                    
                    # 情况2: external 节点 back 仍然 external
                    if node.get("is_external", False) and edge_info.get("is_external", False):
                        print("both external")
                        results.append({
                            "node": node_id,
                            "activity": node.get("activity"),
                            "case": "back_still_external",
                            "event": etype,
                            "edge_info": e,
                                "pattern": f"{src}(is_external) --[{etype}]--> {node_id}(ad)"
                        })
                        print(f"  Found: {src}(is_external) --[{etype}]--> {node_id}(is_external)")
        return results


        # src_nodes = node.get("src", {})
        # #if src_nodes.items():
        # for src_id, edge_info in src_nodes.items():
        #     src_node = graph.state.get(src_id, {})
        #     #for src_node in src_nodes:

        #     if not src_node.get("is_ad_related", False):
        #         events = edge_info.get("events", [])
        #         for e in events:
        #             if e.get("event_type") != "touch_ad":
        #                 print(f"[Type2] Ad node {node_id} "
        #                   f"triggered from non-ad node {src_id} "
        #                   f"via event {e}")
        #         # print(src_node)
        #         # continue
    
def check_type4(graph):

    """
    4. aggressive redirection: state A (is_ad_related) -> wait -> state B (is_external)
    """

    results = []

    for node_id, node in graph.state.items():
        
        if not node.get("is_ad_related", False):
            continue
        
        # 遍历src边，即进入这个广告的来源
        for src, edge_info in node.get("src", {}).items():
            
            for e in edge_info.get("events", []):
                etype = e.get("event_type", "").lower()
                detail = e.get("event_str").lower()
                if etype == "wait":
                    # ignore the non is_external node
                    if edge_info.get("is_external_site", False):
                        continue
                    
                    results.append({
                            "node": node_id,
                            "activity": node.get("activity"),
                            "case": "type4",
                            "event": etype,
                            "edge_info": e,
                            "pattern": f"{src}(ad) --[{etype}]--> {node_id}(external)"
                        })
                    print(f"  Found: {src}(ad) --[{etype}]--> {node_id}(external)")
        return results

def check_type5(graph):
    """
    5. Outside-App Ads: - UI activities do not belong to the app
    """
    print("[+] checking type5 outside-app ads ...")
    results = []
    for node_id, node in graph.state.items():
        # not to resolve the non-ad-related nodes
        # if there is no "is_ad_related", return the False
        if not node.get("is_ad_related", False):
            continue
        
        if "Browser" in node.get("activity", ""):
            continue
        
        #if "launcher" in node.get("package") or node.get("package").startswith("com.android"):
        if "launcher" in node.get("package", "") or node.get("package", "").startswith("com.android"):    
            results.append({
                "node": node_id,
                "activity": node.get("activity"),
                "case": "outside-app ads",
                "pattern": f"{node_id}(ad) -- out of app"
            })
            print("[+] {node_id}(ad) -- out of app", node_id)

    return results

def check_type6(graph, ad_ratio_threshold=0.3, consecutive_threshold=3, gap_threshold=2):
    """
    6. Frequency
    """

    print("[+] checking type6 frequency")
    results = []

    ad_nodes = [nid for nid, n in graph.state.items() if n.get("is_ad_related", False)]
    total_nodes = len(graph.state)
    ad_count = len(ad_nodes)

    if total_nodes == 0:
        return results

    # 规则1: 广告比例过高
    ad_ratio = ad_count / total_nodes
    if ad_ratio > ad_ratio_threshold:
        ret = {
            "case": "high_ad_ratio",
            "ad_ratio": ad_ratio,
            "ad_count": ad_count,
            "total_nodes": total_nodes,
            "pattern": f"Ad ratio {ad_ratio:.2f} exceeds threshold {ad_ratio_threshold}"
        }
        results.append(ret)
        print(f"  Found high ad ratio: {ad_ratio:.2f} ({ad_count}/{total_nodes})")

    # 规则2: 广告频繁出现 (允许一定的非广告间隔)
    for nid, node in graph.state.items():
        if not node.get("is_ad_related", False):
            continue

        chain = [nid]  # 当前链
        current = nid
        visited = set()

        while True:
            visited.add(current)
            next_nodes = list(graph.state[current].get("dst", {}).keys())
            if not next_nodes:
                break

            found_next_ad = False
            for dst in next_nodes:
                if dst in visited:
                    continue

                # 检查 dst 到下一个广告的距离 (gap)
                path = [dst]
                gap = 0
                while gap <= gap_threshold:
                    if graph.state[path[-1]].get("is_ad_related", False):
                        # 找到下一个广告
                        chain.append(path[-1])
                        current = path[-1]
                        found_next_ad = True
                        break
                    # 继续往下找
                    next_dst = list(graph.state[path[-1]].get("dst", {}).keys())
                    if not next_dst:
                        break
                    path.append(next_dst[0])  # 取第一个
                    gap += 1

                if found_next_ad:
                    break

            if not found_next_ad:
                break

            if len(chain) >= consecutive_threshold:
                ret = {
                    "case": "frequent_ads",
                    "start_node": nid,
                    "chain_length": len(chain),
                    "gap_threshold": gap_threshold,
                    "pattern": " -> ".join(chain)
                }
                if ret not in results:
                    results.append(ret)
                print(f"  Found frequent ads chain: {' -> '.join(chain)} (len={len(chain)})")
                break

    return results

# def extract_paths_to_ads(graph, max_depth=20):
#     """
#     提取从入口节点到广告页面的所有动作序列
#     结果可供 DroidBot 生成 input events 使用

#     :param graph: UTG 图对象 (包含 state, src, dst, edges)
#     :param max_depth: 最大搜索深度，避免死循环
#     :return: List[List[Dict]]，每个子列表是一条事件序列
#     """
#     print("[+] Extracting paths to ads")

#     results = []

#     # 入口节点（假设有 root 字段，否则取 state 中第一个）
#     if hasattr(graph, "root"):
#         start_node = graph.root
#     else:
#         start_node = list(graph.state.keys())[0]

#     # DFS 搜索路径
#     def dfs(current, path, events, depth):
#         if depth > max_depth:
#             return

#         node = graph.state[current]

#         # 如果到达广告页面，保存路径
#         if node.get("is_ad_related", False):
#             results.append(events.copy())
#             print(f"  Found ad path: {[e['event_str'] for e in events]}")
#             return

#         # 遍历 outgoing edges
#         for dst, edge_info in node.get("dst", {}).items():
#             for e in edge_info.get("events", []):
#                 new_events = events + [e]
#                 dfs(dst, path + [dst], new_events, depth + 1)

#     dfs(start_node, [start_node], [], 0)
#     return results
        
# def check_type5(graph):
#     """
#     5. Outside-App Ads: - UI activities do not belong to the app
#     """
#     print("[+] checking type5 outside-app ads ...")
#     results = []
    
#     # 首先收集所有作为广告节点目标的非广告节点
#     ad_targets = set()
#     for edge in graph.edge:  # 遍历边列表
#         # 直接访问边的属性，而不是使用 get 方法
#         src_node_id = edge.src if hasattr(edge, 'src') else None
#         dst_node_id = edge.dst if hasattr(edge, 'dst') else None
        
#         if src_node_id and dst_node_id:
#             src_node = graph.state.get(src_node_id)
#             if src_node and src_node.get("is_ad_related", False):
#                 ad_targets.add(dst_node_id)
    
#     for node_id, node in graph.state.items():
#         # 检查节点是否来自外部应用
#         package_name = node.get("package", "")
#         if ("launcher" in package_name or package_name.startswith("com.android")):
#             # 检查节点是否是广告相关，或者是广告节点的目标
#             if node.get("is_ad_related", False) or node_id in ad_targets:
#                 results.append({
#                     "node": node_id,
#                     "activity": node.get("activity"),
#                     "case": "outside-app ads",
#                     "pattern": f"{node_id}(ad) -- out of app"
#                 })
#                 print(f"[+] {node_id}(ad) -- out of app")

#     return results

def extract_paths_to_ads(graph, entry_state=None, max_depth=10, output_file=None):
    """
    从入口状态出发，提取到广告页面的所有路径
    :param graph: dynamic_graph 对象
    :param entry_state: 指定入口节点ID（可选，不传则取第一个）
    :param max_depth: 限制搜索深度，避免环路
    :param output_file: 如果指定，将结果保存到 JSON 文件
    :return: 所有路径和事件的列表
    """
    results = []
    visited = set()

    # 入口节点
    if entry_state is None:
        if not graph.state:
            return []
        entry_state = list(graph.state.keys())[0]

    def dfs(current, path, events, depth):
        if depth > max_depth:
            return
        visited.add(current)
        path.append(current)

        node = graph.state[current]
        if node.get("is_ad_related") or node.get("is_external_site"):
            results.append({
                "path": path.copy(),
                "events": events.copy()
            })
            path.pop()
            visited.remove(current)
            return

        for neighbor, edge_info in node.get("dst", {}).items():
            if neighbor not in visited:
                new_events = events + edge_info.get("events", [])
                dfs(neighbor, path, new_events, depth + 1)

        path.pop()
        visited.remove(current)

    dfs(entry_state, [], [], 0)

    # 保存到文件
    if output_file:
        import json
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(results, f, indent=2, ensure_ascii=False)
        print(f"[+] Saved extracted paths to {output_file}")

    return results

def batch_analyze(output_dirs_with_csv, global_summary="global_summary.csv", sensor_input_csv="sensor_test_input_csv"):
    print("batch analysis")
    """
    批量分析多个输出目录。

    参数:
        output_dirs_with_csv: list[tuple[str, str]]
            例如: [("/data/output_dir1", "log1.csv"), ("/data/output_dir2", "log2.csv")]
        global_summary: str
            最终汇总的CSV文件名
    """
    global_stats = []
    sensor_entries = []

    for (output_dir, csv_path) in output_dirs_with_csv:
        print(f"\n[+] 开始分析: {output_dir}")
        if not os.path.exists(output_dir):
            print(f"[!] 跳过不存在的目录: {output_dir}")
            continue

        output_csv = os.path.join(output_dir, "apk_analysis_results.csv")
        result_stats = analyze(output_dir, output_csv)

        # ---- 生成 JSON 输出 ----
        json_output = os.path.join(output_dir, "apk_analysis_results.json")
        convert_csv_to_json(output_csv, json_output)
        print(f"[✔] JSON结果写入: {json_output}")

        # ---- 添加一条全局统计信息 ----
        global_stats.append({
            "output_dir": output_dir,
            "csv_file": csv_path,
            "total_apks": result_stats.get("total_apks", 0),
            "apks_with_ads": result_stats.get("apks_with_ads", 0),
            "type2_count": result_stats.get("type2_count", 0),
            "type3_count": result_stats.get("type3_count", 0),
            "type4_count": result_stats.get("type4_count", 0),
            "type5_count": result_stats.get("type5_count", 0),
            "type6_count": result_stats.get("type6_count", 0),
            "failed_analysis": result_stats.get("failed_analysis", 0),
            "analyzed_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })

        # ---- 解析当前目录的 has_ad 应用 ----
        with open(output_csv, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row.get("has_ad", "").lower() in ("true", "1", "yes"):
                    sensor_entries.append({
                        "package_name": row.get("app_name", ""),
                        "apk_name": row.get("app_name", ""),
                        "output_dir": output_dir,
                        "has_ad": True,
                        "analyzed_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    })

    # ---- 汇总所有分析结果 ----
    if global_stats:
        write_global_summary(global_stats, global_summary)
        print(f"[✔] 全部任务完成，汇总结果写入：{global_summary}")
    else:
        print("[!] 没有成功分析任何目录。")
    
    # ---- 打印全局统计 ----
    print_global_stats()

    # ---- 写出 sensor_test_input.csv ----
    if sensor_entries:
        write_sensor_input(sensor_entries, sensor_input_csv)
        print(f"[✔] 已生成 sensor_test_input.csv，记录 {len(sensor_entries)} 个广告相关应用")
    else:
        print("[!] 没有检测到广告应用，未生成 sensor_test_input.csv")

def write_sensor_input(entries, output_csv):
    """写出 sensor 测试输入列表"""
    fieldnames = ["package_name", "apk_name", "output_dir", "has_ad", "analyzed_at"]
    with open(output_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(entries)

def convert_csv_to_json(csv_file, json_file):
    """将分析结果CSV转换为JSON文件"""
    if not os.path.exists(csv_file):
        print(f"[!] CSV文件不存在: {csv_file}")
        return
    data = []
    with open(csv_file, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            data.append(row)
    with open(json_file, "w", encoding="utf-8") as jf:
        json.dump(data, jf, ensure_ascii=False, indent=2)

def write_global_summary(records, output_csv):
    """写出汇总的全局统计结果"""
    fieldnames = [
        "output_dir", "csv_file",
        "total_apks", "apks_with_ads",
        "type2_count", "type3_count",
        "type4_count", "type5_count",
        "type6_count", "failed_analysis",
        "analyzed_at"
    ]
    with open(output_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)

# if __name__=='__main__':
    #path = os.path.join('examples/C07B41EB38A4AA087A9B2883AA8F3679C035441AD4470F2A23')
    # root_directory = "D:\\NKU\\Work\\Work2\\appchina_output"
    
    #path = os.path.join('examples/com.dz.law')
    #analyze(path)
    #analyze_test(path)
    
    # 指定包含APK分析结果的根目录
    #root_directory = "D:\\NKU\\Work\\Work2\\datasets\\androzoo\\androzoo_output"
    
    # 指定输出CSV文件路径
    # output_csv = "apk_analysis_results.csv"
    
    # 运行分析
    # analyze(root_directory, output_csv)

    # read a list of root

    '''
    dirs_to_analyze = [
        ("F:\\test\\output", "F:\\test\\merge_output.csv"),
        ("E:\\test\\output", "E:\\test\\untested_simulator1.csv"),
        ("D:\\NKU\\Work\\Work2\\appchina_output", "D:\\NKU\\Work\\Work2\\appchina_output\\log.csv"),
        ("D:\\NKU\\Work\\Work2\\datasets\\androzoo\\androzoo_output", "D:\\NKU\\Work\\Work2\\datasets\\aligned_log.csv")
    ]

    batch_analyze(dirs_to_analyze, global_summary="global_offline_summary.csv", sensor_input_csv="sensor_test_input.csv")
    '''

# 全局文件路径
GLOBAL_CSV = "all_apps_master.csv"
GLOBAL_TXT = "checked_apps.txt"

def ensure_global_files():
    """确保全局CSV和TXT文件存在"""
    # CSV列定义 - 与原有分析结果保持一致
    csv_columns = [
        'app_name', 'has_ad', 'type2_detected', 'type2_features',
        'type3_detected', 'type3_features', 'type4_detected', 'type4_features',
        'type5_detected', 'type5_features', 'type6_detected', 'type6_features',
        'analysis_date', 'app_path', 'is_tested', 'test_date', 'utg_exists'
    ]
    
    if not os.path.exists(GLOBAL_CSV):
        df = pd.DataFrame(columns=csv_columns)
        df.to_csv(GLOBAL_CSV, index=False)
        print(f"[+] 创建全局CSV: {GLOBAL_CSV}")
    
    if not os.path.exists(GLOBAL_TXT):
        with open(GLOBAL_TXT, 'w', encoding='utf-8') as f:
            f.write("# 已检测应用列表\n")
        print(f"[+] 创建全局TXT: {GLOBAL_TXT}")

def load_checked_apps():
    """加载已检查的应用列表"""
    checked_apps = set()
    if os.path.exists(GLOBAL_TXT):
        with open(GLOBAL_TXT, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#'):
                    checked_apps.add(line)
    return checked_apps

def incremental_analysis_with_full_detection(folders_to_scan):
    """
    增量分析主函数 - 使用完整的广告检测流程
    
    Args:
        folders_to_scan: 要扫描的文件夹路径列表
    """
    print("🚀 开始增量分析（完整检测流程）...")
    
    # 确保全局文件存在
    ensure_global_files()
    
    # 加载已检查的应用
    checked_apps = load_checked_apps()
    print(f"[+] 已加载 {len(checked_apps)} 个已检查应用")
    
    # 统计
    stats = {
        'scanned_folders': 0,
        'new_apps': 0,
        'skipped_apps': 0,
        'failed_apps': 0
    }
    
    # 遍历每个大文件夹
    for folder_path in folders_to_scan:
        if not os.path.exists(folder_path):
            print(f"[!] 文件夹不存在: {folder_path}")
            continue
            
        print(f"\n[+] 扫描文件夹: {folder_path}")
        stats['scanned_folders'] += 1
        
        # 获取所有子文件夹
        try:
            sub_folders = [f for f in os.listdir(folder_path) 
                          if os.path.isdir(os.path.join(folder_path, f))]
        except Exception as e:
            print(f"[!] 读取文件夹失败: {e}")
            continue
            
        print(f"    找到 {len(sub_folders)} 个子文件夹")
        
        # 处理每个子文件夹
        for app_name in sub_folders:
            app_folder = os.path.join(folder_path, app_name)
            
            # 检查是否已分析过
            if app_name in checked_apps:
                stats['skipped_apps'] += 1
                continue
                
            # 检查是否存在utg.js
            utg_path = os.path.join(app_folder, "utg.js")
            if not os.path.exists(utg_path):
                stats['skipped_apps'] += 1
                continue
                
            print(f"\n[+] 分析新应用: {app_name}")
            
            # 使用完整的分析流程
            try:
                # 调用原有的完整分析函数
                result = analyze_single_apk(app_folder, app_name)
                
                if result:
                    # 添加额外信息
                    result['app_path'] = app_folder
                    result['is_tested'] = 'TRUE'
                    result['test_date'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    result['utg_exists'] = 'TRUE'
                    
                    # 更新全局文件
                    if update_global_files_with_full_result(result):
                        stats['new_apps'] += 1
                        print(f"[+] 成功分析并记录: {app_name}")
                    else:
                        stats['failed_apps'] += 1
                        print(f"[!] 记录失败: {app_name}")
                else:
                    stats['failed_apps'] += 1
                    print(f"[!] 分析失败: {app_name}")
                    
            except Exception as e:
                stats['failed_apps'] += 1
                print(f"[!] 分析异常 {app_name}: {e}")
    
    # 输出统计
    print(f"\n📊 分析完成统计:")
    print(f"   扫描文件夹: {stats['scanned_folders']}")
    print(f"   新增应用: {stats['new_apps']}")
    print(f"   跳过应用: {stats['skipped_apps']}")
    print(f"   失败应用: {stats['failed_apps']}")
    print(f"   全局CSV: {GLOBAL_CSV}")
    print(f"   全局TXT: {GLOBAL_TXT}")
    
    return stats

def update_global_files_with_full_result(result):
    """使用完整分析结果更新全局文件"""
    try:
        # 读取现有CSV
        if os.path.exists(GLOBAL_CSV) and os.path.getsize(GLOBAL_CSV) > 0:
            df = pd.read_csv(GLOBAL_CSV, dtype=str).fillna("")
        else:
            df = pd.DataFrame(columns=[
                'app_name', 'has_ad', 'type2_detected', 'type2_features',
                'type3_detected', 'type3_features', 'type4_detected', 'type4_features',
                'type5_detected', 'type5_features', 'type6_detected', 'type6_features',
                'analysis_date', 'app_path', 'is_tested', 'test_date', 'utg_exists'
            ])
        
        app_name = result['app_name']
        
        # 检查是否已存在
        existing_mask = df['app_name'] == app_name
        
        if existing_mask.any():
            # 更新现有记录
            for idx in df[existing_mask].index:
                for col, value in result.items():
                    if col in df.columns and value:
                        df.loc[idx, col] = value
            print(f"   更新记录: {app_name}")
        else:
            # 添加新记录
            new_row = {col: '' for col in df.columns}
            new_row.update(result)
            df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
            print(f"   新增记录: {app_name}")
        
        # 保存CSV
        df.to_csv(GLOBAL_CSV, index=False)
        
        # 更新TXT文件
        checked_apps = load_checked_apps()
        checked_apps.add(app_name)
        with open(GLOBAL_TXT, 'w', encoding='utf-8') as f:
            f.write("# 已检测应用列表\n")
            for app in sorted(checked_apps):
                f.write(f"{app}\n")
        
        return True
        
    except Exception as e:
        print(f"[!] 更新全局文件失败: {e}")
        return False

def get_analysis_stats():
    """获取分析统计信息"""
    try:
        if not os.path.exists(GLOBAL_CSV):
            return {"total_apps": 0, "tested_apps": 0}
        
        df = pd.read_csv(GLOBAL_CSV, dtype=str).fillna("")
        
        total_apps = len(df)
        tested_apps = len(df[df['is_tested'] == 'TRUE'])
        has_ad = len(df[df['has_ad'] == 'TRUE'])
        
        # 统计各种广告类型
        type2_count = len(df[df['type2_detected'] == 'TRUE'])
        type3_count = len(df[df['type3_detected'] == 'TRUE'])
        type4_count = len(df[df['type4_detected'] == 'TRUE'])
        type5_count = len(df[df['type5_detected'] == 'TRUE'])
        type6_count = len(df[df['type6_detected'] == 'TRUE'])
        
        return {
            "total_apps": total_apps,
            "tested_apps": tested_apps,
            "has_ad": has_ad,
            "type2_count": type2_count,
            "type3_count": type3_count,
            "type4_count": type4_count,
            "type5_count": type5_count,
            "type6_count": type6_count,
            "ad_percentage": (has_ad / total_apps * 100) if total_apps > 0 else 0
        }
    except Exception as e:
        print(f"[!] 获取统计失败: {e}")
        return {"total_apps": 0, "tested_apps": 0}

def print_detailed_stats():
    """打印详细统计信息"""
    stats = get_analysis_stats()
    
    print(f"\n📈 详细统计信息:")
    print(f"   总应用数: {stats['total_apps']}")
    print(f"   已测试应用: {stats['tested_apps']}")
    print(f"   包含广告: {stats['has_ad']} ({stats['ad_percentage']:.1f}%)")
    print(f"   广告类型分布:")
    print(f"     - Type2: {stats['type2_count']}")
    print(f"     - Type3: {stats['type3_count']}")
    print(f"     - Type4: {stats['type4_count']}")
    print(f"     - Type5: {stats['type5_count']}")
    print(f"     - Type6: {stats['type6_count']}")
    print(f"   全局CSV: {GLOBAL_CSV}")
    print(f"   全局TXT: {GLOBAL_TXT}")

# 使用示例
if __name__ == "__main__":
    folders_to_scan = [
        "D:\\NKU\\Work\\Work2\\fraudulent_output",
        "D:\\NKU\\Work\\Work2\\datasets\\manual_analysis\\output", 
        "D:\\NKU\\Work\\Work2\\datasets\\chin\\output",
        "D:\\NKU\\Work\\Work2\\datasets\\manual_analysis\\test_adgpe_test"
    ]
    
    # 执行增量分析（使用完整检测流程）
    stats = incremental_analysis_with_full_detection(folders_to_scan)
    
    # 显示详细统计信息
    print_detailed_stats()
