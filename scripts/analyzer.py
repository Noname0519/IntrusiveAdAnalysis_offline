import os
import csv
import json
import traceback
from datetime import datetime
from multiprocessing import Pool, cpu_count
from tqdm import tqdm
from functools import partial

class dynamic_graph():
    def __init__(self, js_path=None, json_path=None):
        if json_path is None:
            self.json_path = js_path.replace("js", "json")
        else:
            self.json_path = json_path
        if js_path is not None:
            ret = self.change_js_to_json(js_path, self.json_path)
        

        self.state = {}
        self.ad_nodes = []
        self.ad_nodes_info = {}  # 新增：记录广告节点信息
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

        print(f"[debug！！] graph.state size: {len(self.state)}")

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

    def enhance_utg(self, apk_dir):
        """
        模拟增强UTG，检测广告节点（假设以 is_ad_related 标记）
        自动记录广告节点截图/UI路径
        """

        log_file = os.path.join(apk_dir, "enhanced_log.txt")
        new_ad_node_count = 0

        false_positive_file = "D:\\NKU\\Work\\Work2\\datasets\\androzoo\\false_positive_keywords.txt"
        false_positive_keywords = self._load_false_positive_keywords(false_positive_file)
        keywords = ["ad_contain", "ad_view", "advertisement", "广告", "ad_icon", "ad_title", "adView", "AD", "ad"]
        # print("11111")
        nodes = self.raw_utg.get("nodes", [])
        # print("222222")
        log_entries = []

        false_positive_nodes = []
        for node in nodes:
            if node.get("is_ad_related", False):
                # print("33333")
                ad_features = node.get("ad_feature", [])
                # print("44444")
                is_false_positive = False
                
                # 检查ad_feature中是否包含误识别关键词
                for feature in ad_features:
                    feature_str = str(feature).lower()
                    if any(fp_kw.lower() in feature_str for fp_kw in false_positive_keywords):
                        is_false_positive = True
                        break
                    # if isinstance(feature, dict):
                    #     for value in feature.values():
                    #         if any(fp_kw in str(value).lower() for fp_kw in false_positive_keywords):
                    #             is_false_positive = True
                    #             break
                    # elif any(fp_kw in str(feature).lower() for fp_kw in false_positive_keywords):
                    #is_false_positive = True
                
                if is_false_positive:
                    node["is_ad_related"] = False
                    node["was_corrected_fp"] = True  # ✅ 新增标记
                    
                    # 修正误识别
                    node["is_ad_related"] = False
                    if "ad_feature" in node:
                        del node["ad_feature"]
                    if "ad_format" in node:
                        del node["ad_format"]

                    false_positive_nodes.append(node["id"])
                    log_line = f"⚠️  Node {node['id']} 被修正：原广告识别为误识别（包含过滤关键词）"
                    print("[-] " + log_line)
                    log_entries.append(log_line)

        for node in nodes:
            # 跳过已经被修正的误识别节点
            if node["id"] in false_positive_nodes:
                continue
            
            # print("55555")
            image_path = node.get("image")
            # print("66666")
            
            if not image_path:
                continue

            image_path = image_path.replace("\\", "/")
            state_json_name = os.path.basename(image_path).replace("screen", "state").replace(".png", ".json")
            state_json_path = os.path.normpath(os.path.join(apk_dir, "states", state_json_name))

            if not os.path.exists(state_json_path):
                continue

            try:
                with open(state_json_path, "r", encoding="utf-8") as sf:
                    state_data = json.load(sf)
            except Exception as e:
                print(f"读取 {state_json_path} 失败: {e}")
                continue

            ad_path_dict = {
                "screenshot_path": image_path,
                "ui_layout_path": state_json_path,
                "ui_layout": state_data if state_data else ""
            }

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
                            
                                if not any(fe.get(field) == view[field] and fe.get("matched_keyword") == kw 
                                        for fe in ad_features):
                                    ad_features.append(feature_entry)

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
                node["ad_nodes_info"] = ad_path_dict
                self.ad_nodes_info[node["id"]] = ad_path_dict

                if was_ad_related:
                    log_line = f"✅ Node {node['id']} ({os.path.basename(image_path)}) 确认广告相关, 特征: {len(ad_features)}个"
                else:
                    log_line = f"🎯 Node {node['id']} ({os.path.basename(image_path)}) 新标记为广告相关, 特征: {len(ad_features)}个"
                    new_ad_node_count += 1
                
                print("[+] " + log_line)
                log_entries.append(log_line)
                
                # 详细特征日志
                for i, feature in enumerate(ad_features, 1):
                    detail_log = f"   特征{i}: {feature}"
                    log_entries.append(detail_log)
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

        # 更新 self.state 里的节点
        for node in nodes:
            self.state[node["id"]] = node

        self._rebuild_edges()
        
        

        # 导出增强后的UTG
        enhanced_utg_path = os.path.join(apk_dir, "enhanced_utg.json")
        # with open(enhanced_path, "w", encoding="utf-8") as f:
        #     json.dump({"state": self.state, "ad_nodes_info": self.ad_nodes_info}, f, ensure_ascii=False, indent=2)
        
        with open(enhanced_utg_path, "w", encoding="utf-8") as f:
            json.dump(self.raw_utg, f, indent=2, ensure_ascii=False)
        print(f"[+] Enhanced UTG saved: {enhanced_utg_path}")


        # print(f"[debug after rebuild] graph.state size: {len(self.graph.state)}")

        # 保存日志
        if log_entries:
            with open(log_file, "a", encoding="utf-8") as lf:
                lf.write("\n" + "="*50 + "\n")
                lf.write(f"增强日志 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                lf.write("="*50 + "\n")
                lf.write("\n".join(log_entries) + "\n")
            print(f"[+] 日志已写入 {log_file}")
            
        # 输出统计信息
        ad_nodes_count = sum(1 for node in nodes if node.get("is_ad_related", False))
        #corrected_count = len(false_positive_nodes)
        corrected_count = sum(1 for n in nodes if n.get("was_corrected_fp", False)) + new_ad_node_count

        # ad_edges_count = 0
        # for edge in self.state_edge_json:
        #     if edge.get("is_ad_related", False):
        #         ad_edges_count += 1
        ad_edges_count = sum(1 for edge in self.state_edge_json if edge.get("is_ad_related", False))
                
        print(f"\n📊 统计信息: 总节点{len(nodes)}, 广告节点{ad_nodes_count}, 修正误识别{corrected_count}个")
        print(f"📊 边信息: 总边{len(self.state_edge)}, 广告相关边{ad_edges_count}个")

    def _rebuild_edges(self):
        """
        重新构建边信息，基于增强后的节点属性更新边的广告相关标记
        调用时机：enhance_utg() 完成所有节点属性标注后调用
        """
        print("[*] Rebuilding enhanced edges ...")

        # 清空旧的边数据
        self.state_edge = []
        self.state_edge_json = []
        self.edge = []
        self.activity = {}

        # 清空节点中的邻居信息
        for node_id, node in self.state.items():
            node["src"] = {}
            node["dst"] = {}

        # 防御性检查
        edges = self.raw_utg.get("edges", [])
        if not edges:
            print("[warn] No edges found in raw_utg, skip rebuilding.")
            return

        # 重建所有边
        for js_transition in edges:
            src = js_transition.get("from")
            dst = js_transition.get("to")
            trigger = js_transition.get("events", [])

            # 防止非法节点引用
            if src not in self.state or dst not in self.state:
                print(f"[warn] Missing node in edge: {src} -> {dst}")
                continue

            src_node = self.state[src]
            dst_node = self.state[dst]

            # 获取广告属性
            src_ad_related = src_node.get("is_ad_related", False)
            src_ad_feature = src_node.get("ad_feature", {})
            src_is_external = src_node.get("is_external_site", False)

            dst_ad_related = dst_node.get("is_ad_related", False)
            dst_ad_feature = dst_node.get("ad_feature", {})
            dst_is_external = dst_node.get("is_external_site", False)

            # 更新 edge 的广告标记
            js_transition["src_ad_related"] = src_ad_related
            js_transition["dst_ad_related"] = dst_ad_related
            js_transition["is_ad_related"] = src_ad_related or dst_ad_related

            # === 建立节点的邻接信息 ===
            src_node.setdefault("dst", {})[dst] = {
                "is_ad_related": dst_ad_related,
                "is_external_site": dst_is_external,
                "events": trigger,
                "ad_feature": dst_ad_feature,
            }
            dst_node.setdefault("src", {})[src] = {
                "is_ad_related": src_ad_related,
                "is_external_site": src_is_external,
                "events": trigger,
                "ad_feature": src_ad_feature,
            }

            # === 添加到全局边表 ===
            self.state_edge.append([src, dst])
            self.state_edge_json.append(js_transition)

            # === 按 activity 级别构建映射 ===
            src_act = src_node.get("activity")
            dst_act = dst_node.get("activity")

            if src_act and dst_act:
                # 初始化 activity 映射
                self.activity.setdefault(src_act, {"src": {}, "dst": {}})
                self.activity.setdefault(dst_act, {"src": {}, "dst": {}})

                edge_info = {
                    "trigger": trigger,
                    "weight": 1,
                    "is_ad_related": src_ad_related or dst_ad_related,
                }

                # 添加 src -> dst
                if dst_act not in self.activity[src_act]["src"]:
                    self.activity[src_act]["src"][dst_act] = edge_info
                else:
                    self.activity[src_act]["src"][dst_act]["weight"] += 1
                    self.activity[src_act]["src"][dst_act]["trigger"] += trigger

                # 添加 dst -> src
                if src_act not in self.activity[dst_act]["dst"]:
                    self.activity[dst_act]["dst"][src_act] = edge_info
                else:
                    self.activity[dst_act]["dst"][src_act]["weight"] += 1
                    self.activity[dst_act]["dst"][src_act]["trigger"] += trigger

        print(f"[+] Rebuild done: {len(self.state_edge)} edges, "
            f"{sum(1 for n in self.state.values() if n.get('is_ad_related'))} ad-related nodes.")

    # def _rebuild_edges(self):
    #     """
    #     重新构建边信息，基于增强后的节点属性更新边的广告相关标记
    #     """
    #     # 清空现有的边信息
    #     self.state_edge = []
    #     self.state_edge_json = []
        
    #     # 清空节点中的邻居信息
    #     for node_id in self.state:
    #         if 'src' in self.state[node_id]:
    #             self.state[node_id]['src'] = {}
    #         if 'dst' in self.state[node_id]:
    #             self.state[node_id]['dst'] = {}
        
    #     # 重新构建边
    #     for js_transition in self.raw_utg.setdefault('edges', []):
    #         src = js_transition['from']
    #         dst = js_transition['to']
    #         trigger = js_transition['events']

    #         # 使用增强后的节点属性
    #         src_ad_related = self.state[src].get("is_ad_related", False)
    #         src_ad_feature = self.state[src].get("ad_feature", {})
    #         src_is_external = self.state[src].get("is_external_site", False)

    #         dst_ad_related = self.state[dst].get("is_ad_related", False)
    #         dst_ad_feature = self.state[dst].get("ad_feature", {})
    #         dst_is_external = self.state[dst].get("is_external_site", False)

    #         # 更新边的广告相关标记
    #         js_transition['src_ad_related'] = src_ad_related
    #         js_transition['dst_ad_related'] = dst_ad_related
    #         js_transition['is_ad_related'] = src_ad_related or dst_ad_related

    #         # 在 src 节点里记录 dst 邻居，使用更新后的广告属性
    #         self.state[src].setdefault('dst', {})[dst] = {
    #             'is_ad_related': dst_ad_related, 
    #             'is_external_site': dst_is_external,
    #             'events': trigger,
    #             'ad_feature': dst_ad_feature
    #         }

    #         # 在 dst 节点里记录 src 邻居，使用更新后的广告属性
    #         self.state[dst].setdefault('src', {})[src] = {
    #             'is_ad_related': src_ad_related, 
    #             'is_external_site': src_is_external,
    #             'events': trigger,
    #             'ad_feature': src_ad_feature
    #         }

    #         self.state_edge.append([src, dst])
    #         self.state_edge_json.append(js_transition)
            
    #         # 更新活动级别的边
    #         src_act = self.state[src]['activity']
    #         dst_act = self.state[dst]['activity']
    #         if src_act != dst_act:
    #             edge = [src_act, dst_act]
    #             if edge not in self.edge:
    #                 self.edge.append(edge)
    #                 self.activity[src_act].setdefault('src', {})[dst_act] = {
    #                     'trigger': trigger, 
    #                     'weight': 1,
    #                     'is_ad_related': src_ad_related or dst_ad_related
    #                 }
    #                 self.activity[dst_act].setdefault('dst', {})[src_act] = {
    #                     'trigger': trigger, 
    #                     'weight': 1,
    #                     'is_ad_related': src_ad_related or dst_ad_related
    #                 }
    #             else:
    #                 self.activity[src_act]['src'][dst_act]['weight'] += 1
    #                 self.activity[dst_act]['dst'][src_act]['weight'] += 1
    #                 self.activity[src_act]['src'][dst_act]['trigger'] += trigger
    #                 self.activity[dst_act]['dst'][src_act]['trigger'] += trigger
    
    def record_ad_node(self, node_id, activity, screenshot_path, ui_path):
        """
        记录广告节点的基础信息
        """
        self.ad_nodes_info.append({
            "node_id": node_id,
            "activity": activity,
            "screenshot_path": screenshot_path,
            "ui_path": ui_path,
            "intrusive_pattern": []
        })

    def record_intrusive_pattern(self, node_id, pattern_type):
        """
        记录广告节点的侵入性模式
        """
        for node in self.ad_nodes_info:
            if node["node_id"] == node_id:
                if pattern_type not in node["intrusive_pattern"]:
                    node["intrusive_pattern"].append(pattern_type)
                break

    def export_ad_nodes_info(self, output_path):
        """
        导出广告节点信息为独立JSON
        """
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(self.ad_nodes_info, f, ensure_ascii=False, indent=2)

def getAdStatus(app_path):
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

def _resolve_media_paths_for_node(apk_dir, node):
    """
    给定 node dict（来自 graph.state[node_id]），尝试返回 (screenshot_path, ui_json_path)
    处理 node.get("image") 的多种可能（相对/绝对/带反斜杠等）
    """
    screenshot = ""
    ui_json = ""
    image_path = node.get("image") or ""
    if not image_path:
        return screenshot, ui_json

    # 规范分隔符
    image_base = os.path.basename(image_path).replace("\\", "/")
    # screenshot 文件名通常像 screen_YYYY-... .png
    screenshot_candidate = os.path.join(apk_dir, "states", image_base)
    if os.path.exists(screenshot_candidate):
        screenshot = os.path.normpath(screenshot_candidate)
    else:
        # 如果 image_path 是绝对路径或相对其他路径，尝试直接使用并存在检查
        if os.path.isabs(image_path) and os.path.exists(image_path):
            screenshot = os.path.normpath(image_path)
        else:
            # 作为最后尝试，直接 join apk_dir + image_path
            try_path = os.path.normpath(os.path.join(apk_dir, image_path))
            if os.path.exists(try_path):
                screenshot = try_path

    # 对应的 UI JSON 名称通常从 screen_*.png -> state_*.json
    if screenshot:
        base = os.path.basename(screenshot)
        state_name = base.replace("screen", "state").rsplit(".", 1)[0] + ".json"
        ui_candidate = os.path.join(apk_dir, "states", state_name)
        if os.path.exists(ui_candidate):
            ui_json = os.path.normpath(ui_candidate)
    else:
        # fallback: try to find state_*.json with same timestamp from image_path
        base_img = os.path.basename(image_path)
        if base_img.startswith("screen_") and base_img.endswith(".png"):
            state_name = base_img.replace("screen_", "state_").replace(".png", ".json")
            ui_candidate = os.path.join(apk_dir, "states", state_name)
            if os.path.exists(ui_candidate):
                ui_json = os.path.normpath(ui_candidate)
    return screenshot, ui_json

def _update_graph_ad_node_info(graph, node_id, screenshot, ui_json, pattern_type=None):
    """
    如果 graph 有 ad_nodes_info (list of dict) 或 record_intrusive_pattern 方法，尝试更新它们。
    这是兼容性写法：若 graph 没有这些属性/方法则跳过，不抛异常。
    """
    try:
        # 如果有 ad_nodes_info（list），先查找是否存在node记录
        if hasattr(graph, "ad_nodes_info"):
            found = None
            for n in graph.ad_nodes_info:
                if n.get("node_id") == node_id:
                    found = n
                    break
            if not found:
                newrec = {
                    "node_id": node_id,
                    "activity": graph.state.get(node_id, {}).get("activity"),
                    "screenshot_path": screenshot or "",
                    "ui_path": ui_json or "",
                    "intrusive_pattern": []
                }
                graph.ad_nodes_info.append(newrec)
                found = newrec
            else:
                # 补充路径信息
                if screenshot and not found.get("screenshot_path"):
                    found["screenshot_path"] = screenshot
                if ui_json and not found.get("ui_path"):
                    found["ui_path"] = ui_json

            # 记录 pattern
            if pattern_type:
                if pattern_type not in found.get("intrusive_pattern", []):
                    found.setdefault("intrusive_pattern", []).append(pattern_type)

        # 如果 graph 有 record_intrusive_pattern 方法，调用它
        if pattern_type and hasattr(graph, "record_intrusive_pattern"):
            try:
                graph.record_intrusive_pattern(node_id, pattern_type)
            except Exception:
                pass
    except Exception:
        # 防御性容错，不要打断主流程
        pass

# -------------------------
# check_type2
# -------------------------
def check_type2(apk_dir, graph):
    """
    功能性中断：ad-related 节点由非 ad 或 banner 通过非 touch_ad 事件触发 -> 可疑
    返回 list of dict:
      {
        "dst_ad_node": node_id,
        "src_node": src,
        "dst_activity": ...,
        "src_activity": ...,
        "event_type": etype,
        "edge_info": {浅字典化 event },
        "pattern": "...",
        "screenshot": "...",
        "ui_path": "..."
      }
    """
    print("[+] Checking type2 ...")
    results = []
    print(f"[debug] graph.state size: {len(graph.state)}")

    for node_id, node in graph.state.items():
        print(node_id)

        if not node.get("is_ad_related", False):
            print("not ad related")
            continue

        srcs = node.get("src", {})

        for src, edge_info in srcs.items():
            src_node = graph.state.get(src, {})

            # 若来源本身是广告且不是 banner，则跳过（不是来自非广告的触发）
            if src_node.get("is_ad_related", False) and src_node.get("ad_format") != "banner":
                continue

            for e in edge_info.get("events", []):
                print("etype!")
                etype = (e.get("event_type") or "").lower()
                detail = (e.get("event_str") or "").lower()
                if etype == "key" and "back" in detail:
                    etype = "key_back"

                if "adcomposite" in etype:
                    continue

                if "touch_ad" in etype:
                    continue

                dst_activity = node.get("activity", "unknown")
                src_activity = src_node.get("activity", "unknown")
                if src_node.get("ad_format") == "banner":
                    pattern = f"{src}(banner ad) --[{etype}]--> {node_id}(ad)"
                else:
                    pattern = f"{src}(non-ad) --[{etype}]--> {node_id}(ad)"

                # 获取媒体路径
                # print("type2: screenshot")
                screenshot, ui_json = _resolve_media_paths_for_node(apk_dir, node)
                # print("type2: screenshot")

                result = {
                    "dst_ad_node": node_id,
                    "src_node": src,
                    "dst_activity": dst_activity,
                    "src_activity": src_activity,
                    "event_type": etype,
                    "edge_info": dict(e) if isinstance(e, dict) else {"event_str": str(e)},
                    "pattern": pattern,
                    "screenshot": screenshot,
                    "ui_path": ui_json
                }
                
                results.append(result)

                # 更新 graph 的 ad_nodes_info / intrusive pattern
                _update_graph_ad_node_info(graph, node_id, screenshot, ui_json, "type2")

                # 打印简洁信息
                print(f"  Found type2: {pattern}")

    return results

def check_type3(apk_dir, graph):
    """
    type3:
     - ad-related 节点按 back 后仍为 ad-related
     - 或者 ad-related 且 is_external -> back -> 仍是 external
    """
    print("[+] Checking type3 ...")
    results = []

    for node_id, node in graph.state.items():
        if not node.get("is_ad_related", False):
            continue

        for src, edge_info in node.get("src", {}).items():
            for e in edge_info.get("events", []):
                etype = (e.get("event_type") or "").lower()
                detail = (e.get("event_str") or "").lower()
                if etype == "key" and "back" in detail:
                    etype = "key_back"

                if etype != "key_back":
                    continue

                # 情形1：来源边本身是广告并且回到 ad-related（back_still_ad）
                if edge_info.get("is_ad_related", False):
                    pattern = f"{src}(ad-related) --[{etype}]--> {node_id}(ad-related)"
                    screenshot, ui_json = _resolve_media_paths_for_node(apk_dir, node)
                    results.append({
                        "node": node_id,
                        "activity": node.get("activity"),
                        "case": "back_still_ad",
                        "event": etype,
                        "edge_info": dict(e) if isinstance(e, dict) else {"event_str": str(e)},
                        "pattern": pattern,
                        "screenshot": screenshot,
                        "ui_path": ui_json
                    })
                    _update_graph_ad_node_info(graph, node_id, screenshot, ui_json, "type3")
                    print(f"  Found type3 back_still_ad: {pattern}")

                # 情形2：external 节点 back 仍然 external
                if node.get("is_external", False) and edge_info.get("is_external", False):
                    pattern = f"{src}(is_external) --[{etype}]--> {node_id}(is_external)"
                    screenshot, ui_json = _resolve_media_paths_for_node(apk_dir, node)
                    results.append({
                        "node": node_id,
                        "activity": node.get("activity"),
                        "case": "back_still_external",
                        "event": etype,
                        "edge_info": dict(e) if isinstance(e, dict) else {"event_str": str(e)},
                        "pattern": pattern,
                        "screenshot": screenshot,
                        "ui_path": ui_json
                    })
                    _update_graph_ad_node_info(graph, node_id, screenshot, ui_json, "type3")
                    print(f"  Found type3 back_still_external: {pattern}")

    return results

def check_type4(apk_dir, graph):
    """
    aggressive redirection: ad-related state A -> wait -> state B (is_external)
    """
    print("[+] Checking type4 ...")
    results = []

    for node_id, node in graph.state.items():
        if not node.get("is_ad_related", False):
            continue

        for src, edge_info in node.get("src", {}).items():
            for e in edge_info.get("events", []):
                etype = (e.get("event_type") or "").lower()
                if etype != "wait":
                    continue

                # 如果边本身标记为外部站点，忽略（非重定向）
                if edge_info.get("is_external_site", False):
                    continue

                # 目标 node 是否为 external？（node 或 edge_info 标记）
                if node.get("is_external", False) or edge_info.get("is_external", False):
                    pattern = f"{src}(ad) --[{etype}]--> {node_id}(external)"
                    screenshot, ui_json = _resolve_media_paths_for_node(apk_dir, node)
                    results.append({
                        "node": node_id,
                        "activity": node.get("activity"),
                        "case": "type4",
                        "event": etype,
                        "edge_info": dict(e) if isinstance(e, dict) else {"event_str": str(e)},
                        "pattern": pattern,
                        "screenshot": screenshot,
                        "ui_path": ui_json
                    })
                    _update_graph_ad_node_info(graph, node_id, screenshot, ui_json, "type4")
                    print(f"  Found type4: {pattern}")

    return results

def check_type5(apk_dir, graph):
    """
    Outside-App Ads: ad node 的 activity 不属于应用（launcher/com.android 或 包含 Browser）
    """
    print("[+] Checking type5 ...")
    results = []

    for node_id, node in graph.state.items():
        if not node.get("is_ad_related", False):
            continue

        activity = node.get("activity", "") or ""
        package = node.get("package", "") or ""

        # 排除浏览器类内嵌广告（你可以调整规则）
        if "browser" in activity.lower():
            continue

        if "launcher" in package.lower() or package.startswith("com.android"):
            pattern = f"{node_id}(ad) -- out of app"
            screenshot, ui_json = _resolve_media_paths_for_node(apk_dir, node)
            print("check check launcher")
            results.append({
                "node": node_id,
                "activity": activity,
                "case": "outside-app ads",
                "pattern": pattern,
                "screenshot": screenshot,
                "ui_path": ui_json
            })
            _update_graph_ad_node_info(graph, node_id, screenshot, ui_json, "type5")
            print(f"  Found type5: {pattern}")

    return results

def check_type6(apk_dir, graph, ad_ratio_threshold=0.3, consecutive_threshold=3, gap_threshold=2):
    """
    Frequency:
     - 高广告比例
     - 连续广告链（允许 gap_threshold 的非广告间隔）
    """
    print("[+] Checking type6 ...")
    results = []

    ad_nodes = [nid for nid, n in graph.state.items() if n.get("is_ad_related", False)]
    total_nodes = len(graph.state)
    ad_count = len(ad_nodes)

    if total_nodes == 0:
        return results

    # 规则1: 广告比例过高
    ad_ratio = ad_count / total_nodes
    if ad_ratio > ad_ratio_threshold:
        pattern = f"Ad ratio {ad_ratio:.2f} exceeds threshold {ad_ratio_threshold}"
        results.append({
            "case": "high_ad_ratio",
            "ad_ratio": ad_ratio,
            "ad_count": ad_count,
            "total_nodes": total_nodes,
            "pattern": pattern
        })
        print(f"  Found high ad ratio: {ad_ratio:.2f} ({ad_count}/{total_nodes})")

    # 规则2: 广告频繁出现（检测链）
    for nid in ad_nodes:
        chain = [nid]
        visited = set([nid])
        current = nid

        while True:
            next_nodes = list(graph.state.get(current, {}).get("dst", {}).keys())
            if not next_nodes:
                break

            found_next_ad = False
            for dst in next_nodes:
                if dst in visited:
                    continue
                # 查找从 dst 开始到下一个 ad 的距离
                path = [dst]
                gap = 0
                while gap <= gap_threshold:
                    if graph.state.get(path[-1], {}).get("is_ad_related", False):
                        # 找到下一个广告
                        chain.append(path[-1])
                        current = path[-1]
                        visited.add(path[-1])
                        found_next_ad = True
                        break
                    # 继续往下
                    next_dst = list(graph.state.get(path[-1], {}).get("dst", {}).keys())
                    if not next_dst:
                        break
                    path.append(next_dst[0])
                    gap += 1

                if found_next_ad:
                    break

            if not found_next_ad:
                break

            if len(chain) >= consecutive_threshold:
                pattern = " -> ".join(chain)
                results.append({
                    "case": "frequent_ads",
                    "start_node": nid,
                    "chain_length": len(chain),
                    "pattern": pattern
                })
                # 把 intrusive pattern 记录到所有链上的节点
                for node_in_chain in chain:
                    screenshot, ui_json = _resolve_media_paths_for_node(apk_dir, graph.state.get(node_in_chain, {}))
                    _update_graph_ad_node_info(graph, node_in_chain, screenshot, ui_json, "type6")
                print(f"  Found frequent ads chain: {pattern} (len={len(chain)})")
                break

    return results

def safe_json_dumps(obj):
    """确保 json.dumps 不报错"""
    try:
        return json.dumps(obj, ensure_ascii=False)
    except Exception as e:
        print(f"[!] JSON dump failed: {e}")
        return str(obj)

def safe_join_patterns(results):
    """安全地连接pattern，处理空列表和None值"""
    if not results:
        return ""
    patterns = []
    for r in results:
        pattern = r.get("pattern")
        if pattern and str(pattern).strip():  # 只添加非空且非纯空格的pattern
            patterns.append(str(pattern).strip())
    return "; ".join(patterns) if patterns else ""

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


# ==============================
# 核心分析逻辑
# ==============================

def detect_type(graph, func):
    """调用各类检测函数，返回 (bool, json_str)"""
    try:
        results = func(graph)
        return (len(results) > 0, safe_json_dumps(results))
    except Exception as e:
        print(f"[!] {func.__name__} failed: {e}")
        return (False, "")

# ==============================
# 主分析逻辑（单线程 + 并行）
# ==============================

# 全局函数：可被子进程调用
def analyze_worker(args):
    """Wrapper for multiprocessing"""
    path, folder = args
    return analyze_single_apk(os.path.join(path, folder), folder)

def analyze_single_apk(apk_dir, apk_name):
    all_results = []
    # 收集该app的所有状态文件
    states_dir = os.path.join(apk_dir, "states")
    if not os.path.exists(states_dir):
        print(f"[WARN] No states directory found for {apk_name}")
        return None

    state_files = [f for f in os.listdir(states_dir) if f.startswith("state_") and f.endswith(".json")]
    if not state_files:
        print(f"[WARN] No state JSONs found for {apk_name}")
        return None

    print(f"[+] Analyzing APK: {apk_name}")
    
    # 初始化结果字典
    result = {
        "app_name": apk_name,
        "issue": "",
        "has_ad": False,
        "type2_detected": False, "type2_features": "",
        "type3_detected": False, "type3_features": "",
        "type4_detected": False, "type4_features": "",
        "type5_detected": False, "type5_features": "",
        "type6_detected": False, "type6_features": "",
        "screenshots": [],  # 保存广告截图
        "ui_files": [],     # 保存对应 UI 文件
        "analysis_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }

    utg_path = os.path.join(apk_dir, "utg.js")
    print("utg_path: " + utg_path)
    if not os.path.exists(utg_path):
        print(f"[!] utg.js 不存在: {utg_path}")
        # utg_fail_count = utg_fail_count + 1
        result["issue"] = "utg.js not exists"
        return result
    
    utg_json_path = os.path.join(apk_dir, "utg.json")
    if not os.path.exists(utg_json_path):
        print(f"[!] utg.json 不存在: {utg_json_path}")
        result["issue"] = "utg.json not exists"
        return result
    
    try:
        utg = dynamic_graph(js_path=utg_path)
        utg.enhance_utg(apk_dir)
        enhance_utg_path = os.path.join(apk_dir, "enhanced_utg.json")
        print("enhanced_utg_path", enhance_utg_path) 
        enhanced_utg = dynamic_graph(json_path=enhance_utg_path)
    except Exception as e:
        print(f"[!] Failed to enhance UTG for {apk_dir}: {e}")
        return result

    # === 先判断是否有广告 ===
    # has_ad_nodes = any(n.get("is_ad_related", False) for n in enhanced_utg.state.values())
    # if not has_ad_nodes:
    #     print(f"[+] No ad-related nodes detected for {apk_name}, skipping detailed checks.")
    #     return result

    # result["has_ad"] = True

    # === 调用现有检测函数 ===
    # type2_result = check_type2(ui_data, png_path)
    unique_data = getAdStatus(apk_dir)
    if unique_data:
        result["has_ad"] = True
        type2_results = check_type2(apk_dir, enhanced_utg)
        type3_results = check_type3(apk_dir, enhanced_utg)
        type4_results = check_type4(apk_dir, enhanced_utg)
        type5_results = check_type5(apk_dir, enhanced_utg)
        type6_results = check_type6(apk_dir, enhanced_utg)

        # 汇总所有结果
        # result.update({
        #     "detection_results": {
        #         "type2": {
        #             "detected": bool(type2_result),
        #             "features": safe_join_patterns(type2_result),
        #             "details": type2_result
        #         },
        #         "type3": {
        #             "detected": bool(type3_result),
        #             "features": safe_join_patterns(type3_result),
        #             "details": type3_result
        #         },
        #         "type4": {
        #             "detected": bool(type4_result),
        #             "features": safe_join_patterns(type4_result),
        #             "details": type4_result
        #         },
        #         "type5": {
        #             "detected": bool(type5_result),
        #             "features": safe_join_patterns(type5_result),
        #             "details": type5_result
        #         },
        #         "type6": {
        #             "detected": bool(type6_result),
        #             "features": safe_join_patterns(type6_result),
        #             "details": type6_result
        #         }
        #     }
        # }
        # )
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

    # === 收集截图和 UI 文件 ===
    # for state_file in state_files:
    #     timestamp = state_file.replace("state_", "").replace(".json", "")
    #     screen_file = os.path.join(states_dir, f"screen_{timestamp}.png")
    #     ui_file = os.path.join(states_dir, f"state_{timestamp}.json")
    #     if os.path.exists(screen_file):
    #         result["screenshots"].append(screen_file)
    #     if os.path.exists(ui_file):
    #         result["ui_files"].append(ui_file)

    return result

def analyze_single_apk_in_dir(output_path, folder_name):
    """给定根目录和子文件夹名，分析单个 APK"""
    apk_dir = os.path.join(output_path, folder_name)
    if not os.path.isdir(apk_dir):
        return None
    try:
        return analyze_single_apk(apk_dir, folder_name)
    except Exception as e:
        print(f"[ERROR] Failed to analyze {folder_name}: {e}")
        return None

def analyze_dir(output_dir, output_csv="apk_analysis_results.csv"):
    # 准备CSV文件
    print(f"[+] Starting analysis of directory: {output_dir}")

    fieldnames = [
        "app_name", 
        "has_ad", 
        "type2_detected", "type2_features",
        "type3_detected", "type3_features", 
        "type4_detected", "type4_features",
        "type5_detected", "type5_features", 
        "type6_detected", "type6_features",
        "analysis_date",
        'screenshots', 'issue', 'detection_results', 'ui_files'
    ]

    # 创建或清空CSV文件并写入表头
    with open(output_csv, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
    
    # 获取所有APK目录
    folders = [f for f in os.listdir(output_dir) if os.path.isdir(os.path.join(output_dir, f))]
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

    for app_dir in folders:
        apk_path = os.path.join(output_dir, app_dir)
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

def analyze_all(dirs_to_analyze, global_summary="global_summary.csv", sensor_input_csv="sensor_test_input_csv"):

    global_stats = []
    sensor_entries = []

    for output_path, output_csv in dirs_to_analyze:
        print(f"[+] Starting analysis of directory: {output_path}")

        if not os.path.exists(output_path):
            print(f"[!] 跳过不存在的目录: {output_path}")
            continue

        output_csv = os.path.join(output_path, "apk_analysis_results.csv")
        result_stats = analyze_dir(output_path, output_csv)

        # ---- 生成 JSON 输出 ----
        json_output = os.path.join(output_path, "apk_analysis_results.json")
        convert_csv_to_json(output_csv, json_output)
        print(f"[✔] JSON结果写入: {json_output}")

        # ---- 添加一条全局统计信息 ----
        global_stats.append({
            "output_dir": output_path,
            "csv_file": output_csv,
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
                        "output_dir": output_path,
                        "has_ad": True,
                        "analyzed_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    })
    # ---- 汇总所有分析结果 ----
    if global_stats:
        write_global_summary(global_stats, global_summary)
        print(f"[✔] 全部任务完成，汇总结果写入：{global_summary}")
    else:
        print("[!] 没有成功分析任何目录。")

    # ---- 写出 sensor_test_input.csv ----
    if sensor_entries:
        write_sensor_input(sensor_entries, sensor_input_csv)
        print(f"[✔] 已生成 sensor_test_input.csv，记录 {len(sensor_entries)} 个广告相关应用")
    else:
        print("[!] 没有检测到广告应用，未生成 sensor_test_input.csv")

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


    # for output_path, output_csv in dirs_to_analyze:
    #     print(f"\n=== Analyzing {output_path} ===")

    #     if not os.path.exists(output_path):
    #         print(f"[ERROR] Path not found: {output_path}")
    #         continue

    #     folders = [f for f in os.listdir(output_path) if os.path.isdir(os.path.join(output_path, f))]
    #     print(f"[+] Found {len(folders)} APK directories in {output_path}")

    #     if parallel:
    #         n_workers = n_workers or max(1, cpu_count() - 1)
    #         print(f"[+] Running parallel analysis with {n_workers} workers...")

    #         with Pool(processes=n_workers) as pool:
    #             for res in tqdm(pool.imap_unordered(
    #                     partial(analyze_single_apk_in_dir, output_path),
    #                     folders),
    #                     total=len(folders)):
    #                 if res:
    #                     all_results.append(res)
    #     else:
    #         for d in tqdm(folders, desc="Analyzing (single-thread)"):
    #             res = analyze_single_apk(os.path.join(output_path, d), d)
    #             if res:
    #                 all_results.append(res)

    #     # 输出 CSV
    #     print_summary(all_results, output_csv)

    return all_results

# ==============================
# 报告与主函数
# ==============================

def print_summary(results, output_csv):
    """输出结果汇总并写入CSV"""
    if not results:
        print("[WARN] No analysis results.")
        return

    total = len(results)
    with_ads = sum(1 for r in results if any(r.get(k) for k in ["type1", "type2", "type3", "type4", "type5"]))
    type1 = sum(1 for r in results if r.get("type1"))
    type2 = sum(1 for r in results if r.get("type2"))
    type3 = sum(1 for r in results if r.get("type3"))
    type4 = sum(1 for r in results if r.get("type4"))
    type5 = sum(1 for r in results if r.get("type5"))

    print(f"""
=== Summary ===
Total APKs: {total}
With Ads: {with_ads}
Type1: {type1}
Type2: {type2}
Type3: {type3}
Type4: {type4}
Type5: {type5}
Output CSV: {output_csv}
""")

    # 写入 CSV 文件
    fieldnames = list(results[0].keys())
    with open(output_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)


# ==============================
# Main函数入口
# ==============================

if __name__ == "__main__":

    parallel = True
    workers = 3
    # results = analyze_all(dirs_to_analyze, parallel=parallel, n_workers=workers)

    # analyze_all(args.path, args.output, parallel=args.parallel, n_workers=args.workers)
    dirs_to_analyze = [
        ("F:\\test\\output", "F:\\test\\untested_simulator2.csv"),
        ("E:\\test\\output", "F:\\test\\untested_simulator1.csv"),
        ("D:\\NKU\\Work\\Work2\\appchina_output", "D:\\NKU\\Work\\Work2\\appchina_output\\log.csv"),
        ("D:\\NKU\\Work\\Work2\\datasets\\androzoo\\androzoo_output", "D:\\NKU\\Work\\Work2\\datasets\\aligned_log.csv")
    ]

    analyze_all(dirs_to_analyze, global_summary="global_offline_summary.csv", sensor_input_csv="sensor_test_input.csv")

    # all_results = []
    # for (path, csv_path) in dirs_to_analyze:
    #     print(f"\n=== Analyzing {path} ===")
    #     try:
    #         stats = analyze_all(path, parallel=parallel, n_workers=workers)
    #         all_results.append((path, stats))
    #     except Exception as e:
    #         print(f"[ERROR] Failed to analyze {path}: {e}")

    # print("\n=== Summary of All Analyses ===")
    # for path, stats in all_results:
    #     print(f"{path}: {stats['total_apks']} apps, {stats['apks_with_ads']} with ads, {stats['failed_analysis']} failed")