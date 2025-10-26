import requests
import base64
import json
import cv2
import numpy as np
from PIL import Image
import io
import os

def detect_ad_areas(image_path, api_key, ui_layout=None, output_path=None):
    """
    使用DeepSeek API检测图像中的广告区域
    
    参数:
    - image_path: 输入图片路径
    - api_key: DeepSeek API密钥
    - ui_layout: 可选的UI布局信息
    - output_path: 输出标记图片的路径，如果不提供则不保存
    
    返回:
    - dict: 包含检测结果的字典
    """
    
    # 读取并编码图片
    with open(image_path, "rb") as image_file:
        base64_image = base64.b64encode(image_file.read()).decode('utf-8')
    
    # 构建API请求
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }
    
    # 构建消息内容
    messages = []
    
    if ui_layout:
        # 如果提供了UI布局信息
        prompt = f"""
        分析这张截图和对应的UI布局，识别出所有广告区域。
        
        UI布局信息: {ui_layout}
        
        请返回:
        1. 广告区域的边界框坐标 (x1, y1, x2, y2格式)
        2. 每个广告的类型或内容描述
        3. 布局分析结果
        
        以JSON格式返回结果。
        """
    else:
        # 如果没有UI布局信息
        prompt = """
        分析这张截图，识别出所有广告区域。
        
        请返回:
        1. 广告区域的边界框坐标 (x1, y1, x2, y2格式)
        2. 每个广告的类型或内容描述
        
        以JSON格式返回结果，包含以下字段:
        - ad_boxes: 广告边界框列表
        - ad_descriptions: 广告描述列表
        - layout_analysis: 布局分析(如果有)
        """
    
    messages.append({
        "role": "user",
        "content": [
            {
                "type": "text",
                "text": prompt
            },
            {
                "type": "image_url",
                "image_url": {
                    "url": f"data:image/jpeg;base64,{base64_image}"
                }
            }
        ]
    })
    
    payload = {
        "model": "deepseek-reasoner",  # 根据实际模型名称调整
        "messages": messages,
        "max_tokens": 2000
    }
    
    try:
        # 调用DeepSeek API
        response = requests.post(
            "https://api.deepseek.com/v1/chat/completions",  # 根据实际API端点调整
            headers=headers,
            json=payload
        )
        
        if response.status_code == 200:
            result = response.json()
            content = result['choices'][0]['message']['content']
            
            # 解析API返回的JSON内容
            try:
                detection_result = json.loads(content)
            except json.JSONDecodeError:
                # 如果返回的不是标准JSON，尝试提取JSON部分
                import re
                json_match = re.search(r'\{.*\}', content, re.DOTALL)
                if json_match:
                    detection_result = json.loads(json_match.group())
                else:
                    print("无法解析API返回的JSON格式")
                    return None
            
            # 在原图上标记广告区域
            marked_image = mark_ad_areas_on_image(image_path, detection_result, output_path)
            
            return {
                'detection_result': detection_result,
                'marked_image': marked_image,
                'original_response': content
            }
        else:
            print(f"API调用失败: {response.status_code}")
            print(f"错误信息: {response.text}")
            return None
            
    except Exception as e:
        print(f"调用API时发生错误: {str(e)}")
        return None

def mark_ad_areas_on_image(image_path, detection_result, output_path=None):
    """
    在原图上用红框标记广告区域
    """
    # 读取图片
    image = cv2.imread(image_path)
    if image is None:
        print("无法读取图片")
        return None
    
    # 获取广告边界框
    ad_boxes = detection_result.get('ad_boxes', [])
    
    # 绘制红框
    for i, box in enumerate(ad_boxes):
        if len(box) == 4:  # [x1, y1, x2, y2]
            x1, y1, x2, y2 = map(int, box)
            # 绘制矩形框
            cv2.rectangle(image, (x1, y1), (x2, y2), (0, 0, 255), 3)
            # 添加标签
            label = f"Ad {i+1}"
            cv2.putText(image, label, (x1, y1-10), cv2.FONT_HERSHEY_SIMPLEX, 0.7, (0, 0, 255), 2)
    
    # 保存或显示结果
    if output_path:
        cv2.imwrite(output_path, image)
        print(f"标记后的图片已保存到: {output_path}")
    
    return image

def display_image(image):
    """
    显示图片 (在Jupyter notebook中)
    """
    if isinstance(image, np.ndarray):
        image_rgb = cv2.cvtColor(image, cv2.COLOR_BGR2RGB)
        return Image.fromarray(image_rgb)
    return image


import requests
import base64
import json
import cv2
import numpy as np
from PIL import Image
import io
import os

def expand_path(path):
    """展开包含~的路径到绝对路径"""
    return os.path.expanduser(path)

def detect_ads_from_ui_layout(ui_layout_path, api_key, screenshot_path=None, output_path=None):
    """
    从UI布局JSON文件中识别广告区域
    
    参数:
    - ui_layout_path: UI布局JSON文件路径
    - api_key: DeepSeek API密钥
    - screenshot_path: 可选的截图路径，用于可视化结果
    - output_path: 输出标记图片的路径
    
    返回:
    - dict: 包含检测结果的字典
    """
    
    # 展开路径
    ui_layout_path = expand_path(ui_layout_path)
    if screenshot_path:
        screenshot_path = expand_path(screenshot_path)
    if output_path:
        output_path = expand_path(output_path)
    
    # 检查文件是否存在
    if not os.path.exists(ui_layout_path):
        print(f"错误: UI布局文件不存在: {ui_layout_path}")
        return None
    
    # 读取UI布局JSON文件
    try:
        with open(ui_layout_path, 'r', encoding='utf-8') as f:
            ui_layout_data = json.load(f)
    except Exception as e:
        print(f"读取UI布局文件失败: {str(e)}")
        return None
    
    # 构建API请求
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }
    
    # 构建提示词
    prompt = """
    分析这个UI布局数据，识别出所有可能的广告区域。
    
    请重点关注以下元素:
    - 包含"ad", "banner", "sponsor", "promo"等关键词的组件
    - 通常位于页面顶部、底部或侧边的横幅区域
    - 与其他内容明显分隔的推荐或推广区域
    
    请返回JSON格式的结果，包含以下字段:
    - ad_boxes: 广告区域的边界框列表，格式为[[x1, y1, x2, y2], ...]
    - ad_elements: 被识别为广告的UI元素详细信息列表
    - ad_descriptions: 每个广告区域的描述
    - confidence: 识别置信度(0-1)
    """
    
    # 将UI布局数据转换为字符串，限制长度避免超过token限制
    ui_layout_str = json.dumps(ui_layout_data, ensure_ascii=False)
    if len(ui_layout_str) > 3000:  # 如果太长则截取
        ui_layout_str = ui_layout_str[:3000] + "...(数据截断)"
    
    payload = {
        "model": "deepseek-chat",
        "messages": [
            {
                "role": "user",
                "content": f"{prompt}\n\nUI布局数据:\n{ui_layout_str}"
            }
        ],
        "max_tokens": 2000,
        "temperature": 0.1
    }
    
    try:
        # 调用DeepSeek API
        response = requests.post(
            "https://api.deepseek.com/v1/chat/completions",
            headers=headers,
            json=payload
        )
        
        if response.status_code == 200:
            result = response.json()
            content = result['choices'][0]['message']['content']
            
            # 解析API返回的JSON内容
            try:
                detection_result = json.loads(content)
            except json.JSONDecodeError:
                # 如果返回的不是标准JSON，尝试提取JSON部分
                import re
                json_match = re.search(r'\{.*\}', content, re.DOTALL)
                if json_match:
                    detection_result = json.loads(json_match.group())
                else:
                    print("无法解析API返回的JSON格式")
                    print("原始响应:", content)
                    return None
            
            # 如果有截图路径，在截图上标记广告区域
            marked_image = None
            if screenshot_path and os.path.exists(screenshot_path):
                marked_image = mark_ad_areas_on_image(screenshot_path, detection_result, output_path)
            
            return {
                'detection_result': detection_result,
                'marked_image': marked_image,
                'original_response': content
            }
        else:
            print(f"API调用失败: {response.status_code}")
            print(f"错误信息: {response.text}")
            return None
            
    except Exception as e:
        print(f"调用API时发生错误: {str(e)}")
        return None

def mark_ad_areas_on_image(image_path, detection_result, output_path=None):
    """
    在原图上用红框标记广告区域
    """
    # 读取图片
    image = cv2.imread(image_path)
    if image is None:
        print("无法读取图片")
        return None
    
    # 获取广告边界框
    ad_boxes = detection_result.get('ad_boxes', [])
    
    # 绘制红框
    for i, box in enumerate(ad_boxes):
        if len(box) == 4:  # [x1, y1, x2, y2]
            x1, y1, x2, y2 = map(int, box)
            # 绘制矩形框
            cv2.rectangle(image, (x1, y1), (x2, y2), (0, 0, 255), 3)
            # 添加标签
            label = f"Ad {i+1}"
            cv2.putText(image, label, (x1, y1-10), cv2.FONT_HERSHEY_SIMPLEX, 0.7, (0, 0, 255), 2)
    
    # 保存或显示结果
    if output_path:
        # 确保输出目录存在
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        cv2.imwrite(output_path, image)
        print(f"标记后的图片已保存到: {output_path}")
    
    return image

def extract_bounds_from_layout(ui_layout_data):
    """
    从UI布局数据中提取所有元素的边界框
    这是一个辅助函数，用于分析UI布局结构
    """
    bounds_list = []
    
    def extract_bounds_recursive(element):
        if isinstance(element, dict):
            # 检查是否有bounds字段
            if 'bounds' in element:
                bounds_list.append({
                    'bounds': element['bounds'],
                    'class': element.get('class', ''),
                    'resource-id': element.get('resource-id', ''),
                    'text': element.get('text', '')
                })
            
            # 递归处理子元素
            for key, value in element.items():
                if isinstance(value, (dict, list)):
                    extract_bounds_recursive(value)
        
        elif isinstance(element, list):
            for item in element:
                extract_bounds_recursive(item)
    
    extract_bounds_recursive(ui_layout_data)
    return bounds_list

def display_image(image):
    """
    显示图片 (在Jupyter notebook中)
    """
    if isinstance(image, np.ndarray):
        image_rgb = cv2.cvtColor(image, cv2.COLOR_BGR2RGB)
        return Image.fromarray(image_rgb)
    return image

import requests
import json
import os

def expand_path(path):
    """展开包含~的路径到绝对路径"""
    return os.path.expanduser(path)

def detect_ads_from_ui_layout_only(ui_layout_path, api_key):
    """
    仅从UI布局JSON文件中识别广告区域，不需要截图
    
    参数:
    - ui_layout_path: UI布局JSON文件路径
    - api_key: DeepSeek API密钥
    
    返回:
    - dict: 包含检测结果的字典
    """
    
    # 展开路径
    ui_layout_path = expand_path(ui_layout_path)
    
    # 检查文件是否存在
    if not os.path.exists(ui_layout_path):
        print(f"错误: UI布局文件不存在: {ui_layout_path}")
        return None
    
    # 读取UI布局JSON文件
    try:
        with open(ui_layout_path, 'r', encoding='utf-8') as f:
            ui_layout_data = json.load(f)
    except Exception as e:
        print(f"读取UI布局文件失败: {str(e)}")
        return None
    
    # 构建API请求
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }
    
    # 构建提示词
    prompt = """
    分析这个UI布局数据，识别出所有可能的广告区域。
    
    请重点关注以下元素:
    - 包含"ad", "banner", "sponsor", "promo", "广告", "推荐"等关键词的组件
    - 通常位于页面顶部、底部或侧边的横幅区域
    - 与其他内容明显分隔的推荐或推广区域
    - 包含"install", "download", "点击下载"等推广类文本的元素
    - 资源ID或类名中包含广告相关词汇的元素
    
    请返回JSON格式的结果，包含以下字段:
    - ad_boxes: 广告区域的边界框列表，格式为[[x1, y1, x2, y2], ...]
    - ad_elements: 被识别为广告的UI元素详细信息列表
    - ad_descriptions: 每个广告区域的描述
    - confidence: 识别置信度(0-1)
    - layout_analysis: 对整个布局的分析总结
    """
    
    # 将UI布局数据转换为字符串
    ui_layout_str = json.dumps(ui_layout_data, ensure_ascii=False)
    
    # 如果UI布局数据太大，进行预处理
    if len(ui_layout_str) > 10000:
        print("UI布局数据较大，进行预处理...")
        # 提取关键信息
        simplified_layout = extract_key_elements(ui_layout_data)
        ui_layout_str = json.dumps(simplified_layout, ensure_ascii=False)
        print(f"预处理后数据长度: {len(ui_layout_str)}")
    
    payload = {
        "model": "deepseek-chat",
        "messages": [
            {
                "role": "user",
                "content": f"{prompt}\n\nUI布局数据:\n{ui_layout_str}"
            }
        ],
        "max_tokens": 3000,
        "temperature": 0.1
    }
    
    try:
        # 调用DeepSeek API
        response = requests.post(
            "https://api.deepseek.com/v1/chat/completions",
            headers=headers,
            json=payload
        )
        
        if response.status_code == 200:
            result = response.json()
            content = result['choices'][0]['message']['content']
            
            # 解析API返回的JSON内容
            try:
                detection_result = json.loads(content)
            except json.JSONDecodeError:
                # 如果返回的不是标准JSON，尝试提取JSON部分
                import re
                json_match = re.search(r'\{.*\}', content, re.DOTALL)
                if json_match:
                    detection_result = json.loads(json_match.group())
                else:
                    print("无法解析API返回的JSON格式")
                    print("原始响应:", content)
                    return {"raw_response": content}
            
            return {
                'detection_result': detection_result,
                'original_response': content
            }
        else:
            print(f"API调用失败: {response.status_code}")
            print(f"错误信息: {response.text}")
            return None
            
    except Exception as e:
        print(f"调用API时发生错误: {str(e)}")
        return None

def extract_key_elements(ui_layout_data, max_elements=50):
    """
    从UI布局数据中提取关键元素，减少数据量
    """
    key_elements = []
    
    def extract_elements_recursive(element, depth=0):
        if depth > 10:  # 防止递归过深
            return
            
        if isinstance(element, dict):
            # 提取关键字段
            element_info = {}
            
            # 添加边界信息
            if 'bounds' in element:
                element_info['bounds'] = element['bounds']
                
            # 添加文本和ID信息
            for key in ['text', 'resource-id', 'class', 'content-desc', 'package']:
                if key in element and element[key]:
                    element_info[key] = element[key]
            
            # 如果元素有文本或特定ID，认为是关键元素
            if any(key in element_info for key in ['text', 'resource-id']):
                key_elements.append(element_info)
                
                # 如果已经收集足够多的元素，停止收集
                if len(key_elements) >= max_elements:
                    return
            
            # 递归处理子元素
            for key, value in element.items():
                if isinstance(value, (dict, list)):
                    extract_elements_recursive(value, depth + 1)
        
        elif isinstance(element, list):
            for item in element:
                if len(key_elements) >= max_elements:
                    return
                extract_elements_recursive(item, depth + 1)
    
    extract_elements_recursive(ui_layout_data)
    return key_elements

def analyze_ui_layout_structure(ui_layout_path):
    """
    分析UI布局结构，提取有用的统计信息
    """
    ui_layout_path = expand_path(ui_layout_path)
    
    try:
        with open(ui_layout_path, 'r', encoding='utf-8') as f:
            ui_layout_data = json.load(f)
    except Exception as e:
        print(f"读取UI布局文件失败: {str(e)}")
        return None
    
    # 收集所有元素的边界框
    bounds_list = []
    text_elements = []
    ad_keywords = []
    
    ad_related_keywords = ['ad', 'banner', 'sponsor', 'promo', '广告', '推广', '推荐', 'download', 'install']
    
    def analyze_recursive(element, depth=0):
        if depth > 10:
            return
            
        if isinstance(element, dict):
            # 检查边界框
            if 'bounds' in element:
                bounds_list.append(element['bounds'])
                
            # 检查文本内容
            if 'text' in element and element['text']:
                text_elements.append(element['text'])
                
            # 检查是否包含广告关键词
            for key in ['text', 'resource-id', 'class', 'content-desc']:
                if key in element and element[key]:
                    value = str(element[key]).lower()
                    for keyword in ad_related_keywords:
                        if keyword in value:
                            ad_keywords.append({
                                'keyword': keyword,
                                'element': {k: element[k] for k in element if k in ['text', 'resource-id', 'class', 'bounds']}
                            })
                            break
            
            # 递归处理子元素
            for key, value in element.items():
                if isinstance(value, (dict, list)):
                    analyze_recursive(value, depth + 1)
        
        elif isinstance(element, list):
            for item in element:
                analyze_recursive(item, depth + 1)
    
    analyze_recursive(ui_layout_data)
    
    return {
        'total_elements': len(bounds_list),
        'text_elements_count': len(text_elements),
        'ad_keywords_found': ad_keywords,
        'bounds_sample': bounds_list[:10] if bounds_list else []  # 只返回前10个作为样本
    }

# 使用示例
if __name__ == "__main__":
    # 替换为你的API密钥
    API_KEY = "sk-e25bd66b4cb0456ba7c101796020194b"
    
    # 示例使用 - 仅使用UI布局JSON文件
    ui_layout_path = "/Users/noname/noname/nku/Work/IntrusiveAdAnalysis_offline/examples/C07B41EB38A4AA087A9B2883AA8F3679C035441AD4470F2A23/states/state_2025-08-10_175040.json"
    
    # 先分析UI布局结构
    structure_info = analyze_ui_layout_structure(ui_layout_path)
    if structure_info:
        print("UI布局结构分析:")
        print(f"总元素数量: {structure_info['total_elements']}")
        print(f"文本元素数量: {structure_info['text_elements_count']}")
        print(f"发现的广告关键词: {len(structure_info['ad_keywords_found'])}")
        for keyword_info in structure_info['ad_keywords_found'][:5]:  # 只显示前5个
            print(f"  - '{keyword_info['keyword']}': {keyword_info['element']}")
    
    # 检测广告区域
    result = detect_ads_from_ui_layout_only(
        ui_layout_path=ui_layout_path,
        api_key=API_KEY
    )
    
    if result:
        print("\n广告检测结果:")
        if 'detection_result' in result and isinstance(result['detection_result'], dict):
            print(json.dumps(result['detection_result'], indent=2, ensure_ascii=False))
        else:
            print("原始响应:", result.get('original_response', '无响应'))
    else:
        print("检测失败")
