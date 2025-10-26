def gen_walk_like_sequence(accel_threshold=15.0, rot_threshold_deg=35.0, duration_s=3.0):
    """
    模拟“走路”场景：周期性且方向多变的加速度脉冲
    返回一个 SensorEvent（payload 中 samples 会被 send() 消耗）
    """
    # 走路场景：在水平面上主要是 x/y 方向变化，z 方向有小幅波动
    # 保证最大合加速度 magnitude >= accel_threshold
    steps = int(duration_s * 4)  # 每秒 4 个步伐峰值
    # 生成峰值向量
    ax = accel_threshold * (0.8 + random.random() * 0.4)  # 稍高于阈值
    ay = accel_threshold * (0.2 + random.random() * 0.3)
    az = 1.0 + random.random() * 2.0
    payload = {
        "accel": (ax, ay, az),
        "rotation_deg": rot_threshold_deg * 0.6 + random.random() * (rot_threshold_deg * 0.8),
        "duration_s": duration_s,
        "samples": max(20, steps * 4),
    }
    return SensorEvent(sensor_type="accelerometer", payload=payload)


def gen_drive_like_sequence(accel_threshold=15.0, rot_threshold_deg=35.0, duration_s=3.0):
    """
    模拟“乘车/颠簸”场景：短时高振幅加速度，但方向相对稳定
    """
    ax = accel_threshold * (1.0 + random.random() * 0.6)
    ay = accel_threshold * (0.1 + random.random() * 0.2)
    az = 0.5 + random.random() * 1.5
    payload = {
        "accel": (ax, ay, az),
        "rotation_deg": rot_threshold_deg * (0.4 + random.random() * 0.8),
        "duration_s": duration_s,
        "samples": max(20, int(duration_s * 10)),
    }
    return SensorEvent(sensor_type="accelerometer", payload=payload)


def gen_pickup_putdown_sequence(accel_threshold=15.0, rot_threshold_deg=35.0, duration_s=3.0):
    """
    模拟“拾起 / 放下”动作：前后有一个较明显的角度变化和重力分量突变
    """
    ax = accel_threshold * (0.7 + random.random() * 0.6)
    ay = accel_threshold * (0.1 + random.random() * 0.3)
    az = accel_threshold * 0.05
    payload = {
        "accel": (ax, ay, az),
        "rotation_deg": rot_threshold_deg * (0.9 + random.random() * 0.6),
        "duration_s": duration_s,
        "samples": max(15, int(duration_s * 8)),
    }
    return SensorEvent(sensor_type="combined", payload=payload)


class SensorInputPolicy(object):
    """
    继承/替代你原来的 InputPolicy，用于按 paths 产生事件并在 ad-node 前注入 sensor events。
    paths: List[List[Dict]]  (每条 path 是一系列 event dict)
    sensor_mode: 可选 'walk', 'drive', 'pickup', or 'mixed'
    sensor_inject_point: 'before_ad' or 'on_ad' （在到达广告页面之前注入，或在进入广告页面后注入）
    """

    def __init__(self, device, app, paths, sensor_mode="mixed", sensor_inject_point="before_ad",
                 accel_threshold=15.0, rot_threshold_deg=35.0, duration_s=3.0):
        self.device = device
        self.current_status = None
        self.app = app
        self.paths = paths or []
        self.sensor_mode = sensor_mode
        self.sensor_inject_point = sensor_inject_point
        self.accel_threshold = accel_threshold
        self.rot_threshold_deg = rot_threshold_deg
        self.duration_s = duration_s
        self.action_count = 0
        self.logger = logging.getLogger(self.__class__.__name__)

    def start(self, input_manager):
        """
        遍历所有 paths，并按顺序发送事件；当到达广告节点（通常是路径的末尾）时注入 sensor events。
        """
        self.action_count = 0
        for path_idx, path in enumerate(self.paths):
            if not input_manager.enabled:
                break

            self.logger.info(f"[SensorInputPolicy] Running path {path_idx + 1}/{len(self.paths)}")
            # path 是 list of event dicts (来自 extract_paths_to_ads)
            for ev_idx, ev_dict in enumerate(path):
                # 有些 ev_dict 可能已经是 event 实例，或是标准 dict
                ev_instance = self._dict_to_event(ev_dict)

                # 若需要在进入 ad 时注入（on_ad），那我们必须判断当前 ev 表示到达 ad 的动作
                # 这里我们以 path 的最后一个 event 视为导致进入 ad 的动作（若你的 path 中有更明确标记，请替换判断）
                is_ad_event = (ev_idx == len(path) - 1)

                if self.sensor_inject_point == "before_ad" and is_ad_event:
                    # 在发送进入 ad 事件之前注入 sensor 序列
                    self._inject_sensor_sequence()

                # 发送该事件
                try:
                    self._send_event_instance(ev_instance)
                except Exception as e:
                    self.logger.warning(f"Failed to send event {ev_instance}: {e}")

                if self.sensor_inject_point == "on_ad" and is_ad_event:
                    # 在进入 ad 之后注入 sensor 序列
                    self._inject_sensor_sequence()

                if not input_manager.enabled:
                    break

                self.action_count += 1
                if hasattr(input_manager, "event_count") and self.action_count >= input_manager.event_count:
                    self.logger.info("Reached event_count limit")
                    return

            # small pause between different paths
            time.sleep(0.5)

    def _dict_to_event(self, ev_dict):
        """
        将 dict 转为对应的事件实例（支持 touch / key / sensor 原样封装）
        你可以扩展解析逻辑使其识别 view/info 并构造真实的 TouchEvent/KeyEvent 实例。
        """
        if isinstance(ev_dict, UIEvent):
            return ev_dict

        etype = (ev_dict.get("event_type") or "").lower()
        if "touch" in etype:
            # 尝试从 event_str 解析坐标，简单 heuristic：touch(x,y)
            s = ev_dict.get("event_str", "")
            x = y = None
            try:
                inside = s[s.find("(") + 1:s.rfind(")")]
                if "," in inside:
                    parts = inside.split(",")
                    x = float(parts[-2].strip())
                    y = float(parts[-1].strip())
            except Exception:
                pass
            # 生成一个轻触事件（假定你有 TouchEvent）
            evt = TouchEvent(x=x, y=y, view=None, event_dict=ev_dict)
            return evt
        elif "back" in (ev_dict.get("event_type") or "").lower() or "key" in (ev_dict.get("event_type") or "").lower():
            # 你应该有 KeyEvent 类：这里用一个简单封装
            return KeyEvent(event_dict=ev_dict)
        else:
            # 默认：将 dict 包装成一个 UIEvent，调用 send() 时尝试直接执行 device.shell(command)。
            return GenericWrappedEvent(ev_dict)

    def _send_event_instance(self, ev):
        """
        发送事件实例：优先调用 ev.send(device)，否则尝试按 event_str 发 shell 命令
        """
        if hasattr(ev, "send"):
            return ev.send(self.device)
        else:
            # fallback: 如果有 event_str，尝试以 shell 命令方式发送（视你的 event_str 而定）
            s = getattr(ev, "event_str", None) or getattr(ev, "get_event_str", lambda s=None: None)(None)
            if s:
                # 这是非常通用的 fallback，建议在工程中用具体事件类替换
                print(f"[SensorInputPolicy] fallback sending: {s}")
                return True
            return False

    def _inject_sensor_sequence(self):
        """
        根据 sensor_mode 选择注入序列（可以注入多个场景以覆盖更多触发条件）
        """
        modes = [self.sensor_mode] if self.sensor_mode != "mixed" else ["walk", "drive", "pickup"]
        for m in modes:
            if m == "walk":
                ev = gen_walk_like_sequence(self.accel_threshold, self.rot_threshold_deg, self.duration_s)
            elif m == "drive":
                ev = gen_drive_like_sequence(self.accel_threshold, self.rot_threshold_deg, self.duration_s)
            elif m == "pickup":
                ev = gen_pickup_putdown_sequence(self.accel_threshold, self.rot_threshold_deg, self.duration_s)
            else:
                ev = gen_walk_like_sequence(self.accel_threshold, self.rot_threshold_deg, self.duration_s)

            self.logger.info(f"[SensorInputPolicy] Injecting sensor event: {ev.get_event_str(None)}")
            try:
                ev.send(self.device)
            except Exception as e:
                self.logger.warning(f"Sensor injection failed: {e}")
