import os
import json

import json
import os

class dynamic_graph():
    def __init__(self, js_path):
        self.json_path = js_path.replace("js", "json")
        self.change_js_to_json(js_path, self.json_path)
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
            self.state[src].setdefault('src', {})[dst] = {'events': trigger}
            self.state[dst].setdefault('dst', {})[src] = {'events': trigger}
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

        with open(json_path, "w", encoding=encoding) as f:
            f.write(js_content)
        print(f"[+] 转换完成: {json_path}")

    def enhance_utg(self, root_dir, keywords=None, save_back=True):
        """
        遍历UTG节点，根据对应的 state.json 检测广告相关视图，增强节点信息，并记录日志
        """
        log_file=os.path.join(root_dir, "enhanced_log.txt")

        if keywords is None:
            keywords = ["ad_contain", "ad_view", "advertisement", "广告", "ad_icon", "ad_title"]

        nodes = self.raw_utg.get("nodes", [])
        log_entries = []

        for node in nodes:
            if not node.get("is_ad_related", False):
                
                image_path = node.get("image")
                
                if not image_path:
                    continue

                # 生成对应的 state.json 文件路径
                # state_json_name = os.path.basename(image_path).replace("screen", "state").replace(".png", ".json")
                # state_json_path = os.path.join(root_dir, state_json_name)

                image_path = image_path.replace("\\", "/")
                state_json_name = os.path.basename(image_path).replace("screen", "state").replace(".png", ".json")
                
                state_json_path = os.path.normpath(os.path.join(root_dir,"states", state_json_name))

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

                for view in views:
                    for field in ["resource_id", "text"]:
                        
                        if field in view and view[field]:
                            for kw in keywords:
                                
                                if kw.lower() in str(view[field]).lower():
                                    print(str(view[field]))
                                    ad_features.append({field: view[field]})
                                    print(node["state_str"])

                if ad_features:
                    node["is_ad_related"] = True
                    node["ad_feature"] = ad_features

                    # 记录日志
                    log_line = f"Node {node['id']} ({image_path}) 被标记为广告相关, 特征: {ad_features}"
                    print("[+] " + log_line)
                    log_entries.append(log_line)

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
                lf.write("\n".join(log_entries) + "\n")
            print(f"[+] 日志已写入 {log_file}")

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




def analyze(path):

    results = {
        'total_apk': 0,
        'apks_with_ad_states': 0,
        'apks_with_frida_logs': 0,
        'ad_state_counts': 0,

    }

    utg_path = os.path.join(path, "utg.js")
    utg = dynamic_graph(utg_path)
    utg.enhance_utg(path)

    has_ad_states = False
    has_frida_logs = False

    # check if the ad_states.json exist and store the ad state
    # data = analyzeAdStates(path, results)
    # if data:
    #     has_ad_states = True
        # print("Has ads")
        # get the list of ad states
        # print(data)

        # analyze the layout based

    # check if the frida_log.json exist.

    # stats for app with ads

    pass


def analyzeAdStates(path, results):
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



if __name__=='__main__':
    path = os.path.join('examples/C07B41EB38A4AA087A9B2883AA8F3679C035441AD4470F2A23')
    analyze(path)

