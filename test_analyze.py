import os
import json

import json
import os

class dynamic_graph():
    def __init__(self, js_path=None, json_path=None):
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

    def enhance_utg(self, root_dir, keywords=None, save_back=True):
        """
        遍历UTG节点，根据对应的 state.json 检测广告相关视图，增强节点信息，并记录日志
        """
        log_file=os.path.join(root_dir, "enhanced_log.txt")

        if keywords is None:
            keywords = ["ad_contain", "ad_view", "advertisement", "广告", "ad_icon", "ad_title", "adView"]

        nodes = self.raw_utg.get("nodes", [])
        log_entries = []

        for node in nodes:
            #if not node.get("is_ad_related", False): # 暂时注释，有些ad_feature & ad_format存不下来
                
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
                if view.get("ad_feature"):
                    ad_features.append(view["ad_feature"])

                    if view.get("ad_format") is not None:
                        node["ad_format"] = view["ad_format"]
                        print(node["ad_format"])
                        continue


                for field in ["resource_id", "text"]:
                    
                    if field in view and view[field]:
                        for kw in keywords:
                            
                            if kw.lower() in str(view[field]).lower():
                                #print(str(view[field]))
                                ad_features.append({field: view[field]})
                                #print(node["state_str"])

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
    print("[+] test ... " + path)
    utg_fail_count = 0
    ad_count = 0
    summary = 0
    type2 = {}
    type3 = {}
    type4 = {}
    type5 = {}

    type2_list = []
    type3_list = []
    type4_list = []
    type5_list = []

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

            if type2 != []:
                type2_list.append(type2)
            
            if type3 != []:
                type3_list.append(type3)

            if type4 != []:
                type4_list.append(type4)

            if type5 != []:
                type5_list.append(type5)


    print("type2: ", str(len(type2_list)))
    print("type3: ", str(len(type3_list)))
    print("type4: ", str(len(type4_list)))
    print("type5: ", str(len(type5_list)))
    print("summary: ", str(summary))
    print("failed count: ", str(utg_fail_count))


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

    #rets = check_type2(enhanced_utg)
    # for ret in rets:
    #     f"{ret['src_node']} -> {ret['event_type']} -> {ret['dst_ad_node']}"
    
    rets3 = check_type3(enhanced_utg)
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

def getAdStatus(app_path):
    unique_data = None
    print(f"[+] Start analyze ad states " + app_path)

    ad_state = False
    if not os.path.isdir(path):
        print(f"Error! The dir path is not exist -- {path}.")
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

def get_unique_ad_states(path):
    print("[+] get unique ad status: " + path)
    unique_data = []
    seen_items = set() 
            
    if path and os.path.isfile(path):
        with open(path, "r", encoding="utf-8") as f:
            try:
                data = json.load(f)
                if data is None:
                    return None
                
                for item in data:
                    
                    state_id = item.get("state_str")
                    screenshot_path = item.get("screenshot_path")
                    if state_id not in seen_items:
                        seen_items.add(state_id)
                        unique_data.append(item)
            except json.JSONDecodeError:
                print("JSON format error: ", path)
        print("unique_data: ", unique_data)
        # 存回去（覆盖原文件）
        # with open(ad_states_path, "w", encoding="utf-8") as f:
        #     json.dump(unique_data, f, indent=2, ensure_ascii=False)
        # 保存为副本
        dedup_path = path.replace(".json", "_dedup.json")
        with open(dedup_path, "w", encoding="utf-8") as f:
            json.dump(unique_data, f, indent=2, ensure_ascii=False)

        print(f"去重后保存成功，共 {len(unique_data)} 条记录。")
    else:
        print("未找到 ad_states.json 文件")
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



if __name__=='__main__':
    # path = os.path.join('examples/C07B41EB38A4AA087A9B2883AA8F3679C035441AD4470F2A23')
    path = "D:\\NKU\\Work\\Work2\\appchina_output"
    analyze(path)

