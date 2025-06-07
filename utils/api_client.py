import math
import time
import requests
import matplotlib.pyplot as plt
import networkx as nx
import numpy as np
import hashlib
from matplotlib.colors import LinearSegmentedColormap
import matplotlib.animation as animation
from datetime import datetime
import matplotlib.patches as mpatches
from matplotlib.gridspec import GridSpec

API_BASE_URL = "http://localhost:8911"
UPDATE_INTERVAL = 2.0
TRANSPARENCY_DECAY_TIME = 10.0
LAYOUT_ITERATIONS = 70  # Ещё больше итераций для лучшего распределения
LAYOUT_K = 0.5  # Значительно увеличиваем силу отталкивания
MIN_DISTANCE = 0.3  # Минимальное расстояние между узлами
DEBUG_API = True  # Флаг для включения/выключения отладочного вывода API

def log_api(message):
    """Функция для вывода отладочных сообщений по API"""
    if DEBUG_API:
        time_str = datetime.now().strftime("%H:%M:%S")
        print(f"[API {time_str}] {message}")

nodes = []
measurements = []
groups = []
goals = []
vote_requests = []  # Инициализируем как пустой список
votes = []
all_votes = []
leadership_resigns = []
last_data_time = None
api_available = True
node_colors = {}
group_colors = {}
latest_votes = {}  # Словарь для хранения последних голосов от каждой ноды

latency_cmap = LinearSegmentedColormap.from_list(
    "latency", [(0, "green"), (0.5, "yellow"), (1, "red")]
)
vote_cmap = LinearSegmentedColormap.from_list(
    "vote", [(0, "lightblue"), (0.5, "blue"), (1, "darkblue")]
)

def get_color_from_string(string_value):
    h = hashlib.md5(string_value.encode()).hexdigest()
    r = int(h[0:2], 16) / 255.0
    g = int(h[2:4], 16) / 255.0
    b = int(h[4:6], 16) / 255.0
    factor = 0.2
    r = min(1.0, r + (1.0 - r) * factor)
    g = min(1.0, g + (1.0 - g) * factor)
    b = min(1.0, b + (1.0 - b) * factor)
    return (r, g, b)

def get_data():
    global nodes, measurements, groups, goals, vote_requests, votes, all_votes, leadership_resigns, last_data_time, api_available, latest_votes
    try:
        nodes = []
        measurements = []
        groups = []
        goals = []
        vote_requests = []
        votes = []
        leadership_resigns = []

        log_api("Запрашиваем данные от API...")

        # Узлы
        resp = requests.get(f"{API_BASE_URL}/api/nodes", timeout=2)
        if resp.status_code != 200:
            log_api(f"Ошибка при получении узлов: статус {resp.status_code}")
            api_available = False
            return False
        json_response = resp.json()
        if json_response is not None:
            nodes = json_response
            log_api(f"Получено {len(nodes)} узлов")
            if nodes:
                log_api(f"Пример узла: {nodes[0]}")
        else:
            log_api("Ответ API для узлов пуст или равен None")
            nodes = []

        # Измерения
        resp = requests.get(f"{API_BASE_URL}/api/measurements", timeout=2)
        if resp.status_code != 200:
            log_api(f"Ошибка при получении измерений: статус {resp.status_code}")
            api_available = False
            return False
        json_response = resp.json()
        if json_response is not None:
            measurements = json_response
            log_api(f"Получено {len(measurements)} измерений")
            if measurements:
                log_api(f"Пример измерения: {measurements[-1]}")
        else:
            log_api("Ответ API для измерений пуст или равен None")
            measurements = []

        # Группы
        resp = requests.get(f"{API_BASE_URL}/api/groups", timeout=2)
        if resp.status_code != 200:
            log_api(f"Ошибка при получении групп: статус {resp.status_code}")
            api_available = False
            return False
        json_response = resp.json()
        if json_response is not None:
            groups = json_response
            log_api(f"Получено {len(groups)} групп")
            if groups:
                log_api(f"Пример группы: {groups[0]}")
        else:
            log_api("Ответ API для групп пуст или равен None")
            groups = []

        # Цели групп
        resp = requests.get(f"{API_BASE_URL}/api/groupgoals", timeout=2)
        if resp.status_code != 200:
            log_api(f"Ошибка при получении целей: статус {resp.status_code}")
            api_available = False
            return False
        json_response = resp.json()
        if json_response is not None:
            goals = json_response
            log_api(f"Получено {len(goals)} целей")
            if goals:
                log_api(f"Пример цели: {goals[0]}")
        else:
            log_api("Ответ API для целей пуст или равен None")
            goals = []

        # Запросы на голосование
        resp = requests.get(f"{API_BASE_URL}/api/voterequests", timeout=2)
        if resp.status_code == 200:
            json_response = resp.json()
            if json_response is not None:
                vote_requests = json_response
                log_api(f"Получено {len(vote_requests)} запросов на голосование")
                if vote_requests:
                    log_api("=== Детальная информация о запросах на голосование ===")
                    for i, vr in enumerate(vote_requests):
                        vr_id = vr.get("id", "н/д")
                        vr_kind = vr.get("kind", "н/д")
                        if vr_kind == "н/д":
                            vr_kind = vr.get("Kind", "н/д")
                        vr_target = vr.get("target", "н/д")
                        vr_deleted = vr.get("deleted", False)
                        vr_date = vr.get("dateUnix", vr.get("date_unix", vr.get("DateUnix", 0)))
                        vr_timeout = vr.get("timeoutSecs", vr.get("timeout_secs", vr.get("TimeoutSecs", 300)))
                        now = time.time()
                        is_active_by_time = now <= vr_date + vr_timeout
                        log_api(f"Запрос #{i+1}: ID={vr_id}, Тип={vr_kind}, Цель={vr_target}, "
                                f"Удален={vr_deleted}, Дата={vr_date}, Таймаут={vr_timeout}, "
                                f"Активен по времени={is_active_by_time}")
                    active_requests_local = [vr for vr in vote_requests if not vr.get("deleted", False)]
                    log_api(f"Активных неудаленных запросов: {len(active_requests_local)}")
                    now = time.time()
                    time_active_requests = [
                        vr for vr in active_requests_local
                        if now <= vr.get("dateUnix", vr.get("date_unix", vr.get("DateUnix", 0))) +
                           vr.get("timeoutSecs", vr.get("timeout_secs", vr.get("TimeoutSecs", 300)))
                    ]
                    log_api(f"Активных запросов с учетом таймаута: {len(time_active_requests)}")
                    if time_active_requests:
                        log_api(f"Пример активного запроса на голосование: {time_active_requests[0]}")
            else:
                log_api("Ответ API для запросов на голосование пуст или равен None")
                vote_requests = []
        else:
            log_api(f"Ошибка при получении запросов на голосование: статус {resp.status_code}")

        # Голоса
        resp = requests.get(f"{API_BASE_URL}/api/votes", timeout=2)
        if resp.status_code == 200:
            json_response = resp.json()
            if json_response is not None:
                votes = json_response
                log_api(f"Получено {len(votes)} голосов")
                if votes:
                    active_votes = [v for v in votes if not v.get("deleted", False)]
                    log_api(f"Активных голосов: {len(active_votes)}")
                    if active_votes:
                        log_api(f"Пример голоса: {active_votes[0]}")
                        log_api(f"Ключи первого активного голоса: {list(active_votes[0].keys())}")
            else:
                log_api("Ответ API для голосов пуст или равен None")
                votes = []
        else:
            log_api(f"Ошибка при получении голосов: статус {resp.status_code}")

        # Сложения полномочий
        resp = requests.get(f"{API_BASE_URL}/api/leadershipresigns", timeout=2)
        if resp.status_code == 200:
            json_response = resp.json()
            if json_response is not None:
                leadership_resigns = json_response
                log_api(f"Получено {len(leadership_resigns)} сложений полномочий")
                if leadership_resigns:
                    log_api(f"Пример сложения полномочий: {leadership_resigns[0]}")
            else:
                log_api("Ответ API для сложений полномочий пуст или равен None")
                leadership_resigns = []
        else:
            log_api(f"Ошибка при получении сложений полномочий: статус {resp.status_code}")

        last_data_time = time.time()
        api_available = True
        log_api("Обновление данных завершено успешно")
        return True
    except (requests.RequestException, ConnectionError, TimeoutError) as e:
        api_available = False
        log_api(f"Ошибка соединения с API: {str(e)}")
        return False
    except Exception as e:
        api_available = False
        log_api(f"Неожиданная ошибка при обработке данных API: {str(e)}")
        return False

def calc_edge_transparency(timestamp):
    now = time.time()
    age = now - timestamp
    if age < 0:
        return 1.0
    if age > TRANSPARENCY_DECAY_TIME:
        return 0.0
    return 1.0 - (age / TRANSPARENCY_DECAY_TIME)

def create_measurement_graph():
    global node_colors
    G = nx.DiGraph()
    for node in nodes or []:
        nid = node["id"]
        if nid not in node_colors:
            node_colors[nid] = get_color_from_string(nid)
        G.add_node(nid, color=node_colors[nid])

    for m in measurements or []:
        if m["type"] != "latency_class":
            continue
        owner = m["owner"]
        target = m["target"]
        value = m["value"]
        ts = m["dateUnix"]

        norm = value / 4.0
        color = latency_cmap(norm)
        alpha = calc_edge_transparency(ts)
        if alpha > 0:
            edge_color = (color[0], color[1], color[2], alpha)
            G.add_edge(owner, target, weight=value, color=edge_color, timestamp=ts)

    return G

def create_goal_graphs():
    global node_colors, group_colors
    graphs = {}

    if goals is None:
        log_api("Предупреждение: goals is None в create_goal_graphs")
        return graphs

    for goal in goals or []:
        G = nx.Graph()
        gid = goal.get("id", "unknown")

        if nodes is None:
            log_api("Предупреждение: nodes is None в create_goal_graphs")
            continue

        for node in nodes or []:
            nid = node.get("id", "unknown")
            if nid not in node_colors:
                node_colors[nid] = get_color_from_string(nid)
            G.add_node(nid, color=node_colors[nid])

        if groups is None:
            log_api("Предупреждение: groups is None в create_goal_graphs")
            graphs[gid] = G
            continue

        related = [g for g in groups or [] if g.get("goal") == gid]
        for grp in related:
            grpid = grp.get("id", "unknown")
            if grpid not in group_colors:
                group_colors[grpid] = get_color_from_string(grpid)
            col = group_colors[grpid]

            participants = grp.get("participants", [])
            if participants is None:
                log_api(f"Предупреждение: participants is None в группе {grpid}")
                participants = []

            members = [p.get("id", "unknown") for p in participants]
            for i in range(len(members)):
                for j in range(i + 1, len(members)):
                    G.add_edge(
                        members[i], members[j],
                        group_id=grpid,
                        color=(col[0], col[1], col[2], 0.7)
                    )
        graphs[gid] = G
    return graphs

def filter_votes_for_graph():
    """Фильтрует голоса перед построением графа, оставляя только последний голос от каждой ноды
    для каждого запроса. Результат записывается в глобальную переменную votes."""
    global votes
    global all_votes
    if votes is None or len(votes) == 0:
        return

    filtered_votes = {}
    sorted_votes = sorted(votes, key=lambda v: v.get("dateUnix", 0))
    for vote in sorted_votes:
        voter_id = str(vote.get("voter", ""))
        request_id = str(vote.get("requestId", ""))
        if not (voter_id and request_id):
            continue
        key = f"{voter_id}_{request_id}"
        filtered_votes[key] = vote

    all_votes = sorted_votes
    votes = list(filtered_votes.values())
    log_api(f"Голоса отфильтрованы: {len(filtered_votes)} уникальных из первоначальных {len(sorted_votes)}")

def create_vote_graph():
    """Создает граф голосования на основе запросов на голосование и голосов"""
    G = nx.DiGraph()

    if vote_requests is None:
        log_api("Предупреждение: vote_requests is None в create_vote_graph")
        return G, []

    if votes is None:
        log_api("Предупреждение: votes is None в create_vote_graph")
        return G, []

    # Первый проход: оставляем только последний голос от каждой ноды для каждого запроса
    filter_votes_for_graph()
    log_api(f"После первого фильтра осталось голосов: {len(votes)}")

    log_api("=== Определение активных запросов на голосование в create_vote_graph ===")
    active_requests = []
    for i, vr in enumerate(vote_requests):
        if not isinstance(vr, dict):
            log_api(f"Запрос #{i} не является словарем, пропускаем")
            continue

        kind = None
        for key in ["kind", "Kind", "KIND", "type", "Type", "TYPE"]:
            if key in vr:
                kind = vr[key]
                break

        is_leadership_type = False
        if kind is not None:
            leadership_variants = [
                "NetworkLeadership", "network_leadership", "NETWORK_LEADERSHIP",
                "networkleadership", "NetworkLeader", "network_leader"
            ]
            is_leadership_type = kind in leadership_variants

        is_deleted = False
        for key in ["deleted", "Deleted", "DELETED", "is_deleted", "IsDeleted"]:
            if key in vr and vr[key]:
                is_deleted = True
                break

        if is_leadership_type and not is_deleted:
            date_unix = 0
            for key in ["dateUnix", "date_unix", "DateUnix", "DATE_UNIX", "timestamp", "time", "date"]:
                if key in vr and vr[key]:
                    date_unix = int(vr[key])
                    break

            timeout_secs = 300
            for key in ["timeoutSecs", "timeout_secs", "TimeoutSecs", "TIMEOUT_SECS", "timeout"]:
                if key in vr and vr[key]:
                    timeout_secs = int(vr[key])
                    break

            now = time.time()
            is_active_by_time = now <= date_unix + timeout_secs

            log_api(f"Запрос #{i}: Тип={kind}, Удален={is_deleted}, "
                    f"Дата={date_unix}, Таймаут={timeout_secs}, "
                    f"Активен по времени={is_active_by_time}")

            if is_active_by_time:
                active_requests.append(vr)
                log_api(f"Запрос #{i} добавлен в активные запросы")

    log_api(f"Всего определено {len(active_requests)} активных запросов на голосование")
    active_requests.sort(key=lambda x: x.get("dateUnix", 0), reverse=True)

    # Если есть активные запросы, выбираем самый свежий
    last_request_id = None
    if active_requests:
        vr0 = active_requests[0]
        for key in ("id", "Id", "ID"):
            if key in vr0 and vr0[key]:
                last_request_id = vr0[key]
                break
    log_api(f"Последний активный запрос: {last_request_id}")

    # Оставляем только голоса по последнему запросу
    if last_request_id is not None:
        filtered_by_request = []
        for vote in votes:
            vote_req_id = None
            for key in ("requestId", "RequestId", "requestID", "REQUESTID"):
                if key in vote and vote[key]:
                    vote_req_id = vote[key]
                    break
            if vote_req_id == last_request_id:
                filtered_by_request.append(vote)
        votes[:] = filtered_by_request
        log_api(f"После фильтрации по последнему запросу осталось голосов: {len(votes)}")
    else:
        votes.clear()
        log_api("Нет активного запроса, очищаем список голосов")

    # Ещё раз оставляем только последний голос от каждой ноды (для безопасности)
    filter_votes_for_graph()
    log_api(f"После второго фильтра осталось уникальных голосов: {len(votes)}")

    # Добавляем узлы
    for node in nodes:
        if not isinstance(node, dict):
            continue
        nid = node.get("id")
        if not nid:
            continue
        if nid not in node_colors:
            node_colors[nid] = get_color_from_string(nid)
        is_candidate = False
        for vr in active_requests:
            if vr.get("target") == nid:
                is_candidate = True
                break
        G.add_node(nid, color=node_colors[nid], is_candidate=is_candidate)

    log_api(f"Обработка {len(votes)} голосов")
    for vote_idx, vote in enumerate(votes):
        if not isinstance(vote, dict):
            continue

        is_deleted = False
        for key in ["deleted", "Deleted", "DELETED", "is_deleted", "IsDeleted"]:
            if key in vote and vote[key]:
                is_deleted = True
                break
        if is_deleted:
            continue

        request_id = vote.get("requestId")
        if not request_id:
            log_api(f"Голос #{vote_idx} не имеет requestId, пропускаем.")
            continue

        matching_request = None
        for req_idx, req in enumerate(active_requests):
            req_id = None
            for key in ["id", "Id", "ID"]:
                if key in req and req[key]:
                    req_id = req[key]
                    break
            if req_id == request_id:
                matching_request = req
                log_api(f"Голос #{vote_idx} (request_id={request_id}) сопоставлен с запросом #{req_idx} (id={req_id}).")
                break

        if not matching_request:
            log_api(f"Голос #{vote_idx} (request_id={request_id}) не найден среди активных запросов. Пропускаем.")
            continue

        vote_target = None
        for key in ["voter", "Voter", "VOTER", "source", "Source", "SOURCE"]:
            if key in vote and vote[key]:
                vote_target = vote[key]
                break
        if not vote_target:
            for key in ["target", "Target", "TARGET"]:
                if key in vote and vote[key]:
                    vote_target = vote[key]
                    break

        vote_value = 1
        for key in ["value", "Value", "VALUE", "weight", "Weight", "WEIGHT"]:
            if key in vote and vote[key] is not None:
                try:
                    vote_value = int(vote[key])
                except (ValueError, TypeError):
                    pass
                break

        vote_request_target = matching_request.get("target")
        log_api(f"Голос #{vote_idx}: Запрос={request_id}, Голосующий={vote_target}, "
                f"Кандидат={vote_request_target}, Значение={vote_value}")

        if vote_target and vote_request_target:
            if vote_target not in G:
                if vote_target not in node_colors:
                    node_colors[vote_target] = get_color_from_string(vote_target)
                G.add_node(vote_target, color=node_colors[vote_target], is_candidate=False)
                log_api(f"Добавлен узел {vote_target} в граф голосования")

            if vote_request_target not in G:
                if vote_request_target not in node_colors:
                    node_colors[vote_request_target] = get_color_from_string(vote_request_target)
                G.add_node(vote_request_target, color=node_colors[vote_request_target], is_candidate=True)
                log_api(f"Добавлен узел-кандидат {vote_request_target} в граф голосования")

            norm_value = min(1.0, vote_value / 3.0)
            color = vote_cmap(norm_value)

            G.add_edge(
                vote_target,
                vote_request_target,
                vote_value=vote_value,
                request_id=request_id,
                color=(color[0], color[1], color[2], 0.8)
            )
            log_api(f"Добавлено ребро {vote_target} -> {vote_request_target} с весом {vote_value}")

    log_api(f"Граф голосования создан: {len(G.nodes)} узлов, {len(G.edges)} рёбер, {len(active_requests)} активных запросов")
    return G, active_requests

def post_process_layout(G, pos):
    """
    Постобработка layout для предотвращения наложений узлов.
    Гарантирует минимальное расстояние между всеми парами узлов.
    """
    if not pos or len(pos) <= 1:
        return pos

    new_pos = {n: np.array(p.copy()) for n, p in pos.items()}
    iterations = 5
    for _ in range(iterations):
        changes_made = False
        nodes_list = list(G.nodes())
        for i in range(len(nodes_list)):
            for j in range(i + 1, len(nodes_list)):
                ni, nj = nodes_list[i], nodes_list[j]
                pi, pj = new_pos[ni], new_pos[nj]
                dx, dy = pi[0] - pj[0], pi[1] - pj[1]
                distance = np.sqrt(dx * dx + dy * dy)
                if distance < MIN_DISTANCE:
                    changes_made = True
                    if distance > 0.0001:
                        push_factor = (MIN_DISTANCE - distance) / distance * 0.5
                        dx_push = dx * push_factor
                        dy_push = dy * push_factor
                    else:
                        angle = np.random.random() * 2 * np.pi
                        dx_push = MIN_DISTANCE * 0.5 * np.cos(angle)
                        dy_push = MIN_DISTANCE * 0.5 * np.sin(angle)
                    new_pos[ni][0] += dx_push
                    new_pos[ni][1] += dy_push
                    new_pos[nj][0] -= dx_push
                    new_pos[nj][1] -= dy_push
        if not changes_made:
            break

    x_values = [p[0] for p in new_pos.values()]
    y_values = [p[1] for p in new_pos.values()]
    min_x, max_x = min(x_values), max(x_values)
    min_y, max_y = min(y_values), max(y_values)
    center_x = (min_x + max_x) / 2
    center_y = (min_y + max_y) / 2
    scale = max(1.0, max(max_x - min_x, max_y - min_y)) * 1.1

    for node, p in new_pos.items():
        new_pos[node][0] = (p[0] - center_x) / scale
        new_pos[node][1] = (p[1] - center_y) / scale

    return new_pos

def update_layout(G, prev_layout=None, k=None, iterations=50):
    """
    Создает или обновляет layout графа, учитывая необходимость избегать наложений узлов.
    Включает дополнительную постобработку для гарантированного соблюдения минимальных расстояний.
    """
    if prev_layout is None or not prev_layout:
        k_value = (k or LAYOUT_K) * 1.5
        log_api(f"Создаем новый layout с силой отталкивания k={k_value}")
        more_iterations = iterations * 2
        layout = nx.spring_layout(G, seed=42, k=k_value, iterations=more_iterations)
        return post_process_layout(G, layout)

    existing = set(prev_layout.keys())
    current = set(G.nodes())
    if existing == current:
        pos = nx.spring_layout(
            G,
            pos=prev_layout,
            fixed=None,
            k=(k or LAYOUT_K) * 1.1,
            iterations=max(5, iterations // 10),
            seed=42,
            weight=None
        )
        return post_process_layout(G, pos)

    new_nodes = current - existing
    removed_nodes = existing - current
    if new_nodes:
        log_api(f"Обнаружены новые узлы в графе: {', '.join(str(n) for n in new_nodes)}")
    if removed_nodes:
        log_api(f"Удалены узлы из графа: {', '.join(str(n) for n in removed_nodes)}")

    new_nodes_percentage = len(new_nodes) / len(current) if current else 0
    removed_nodes_percentage = len(removed_nodes) / len(existing) if existing else 0
    if new_nodes_percentage > 0.2 or removed_nodes_percentage > 0.2:
        log_api(f"Значительное изменение в графе: {new_nodes_percentage*100:.1f}% новых, "
                f"{removed_nodes_percentage*100:.1f}% удаленных. Полный пересчет layout.")
        k_value = (k or LAYOUT_K) * 1.5
        log_api(f"Пересчитываем layout с k={k_value}, iterations={iterations*3}")
        layout = nx.spring_layout(G, seed=42, k=k_value, iterations=iterations*3)
        return post_process_layout(G, layout)

    pos = {n: prev_layout[n] for n in existing & current}
    if new_nodes:
        if pos:
            x_coords = [p[0] for p in pos.values()]
            y_coords = [p[1] for p in pos.values()]
            min_x, max_x = min(x_coords), max(x_coords)
            min_y, max_y = min(y_coords), max(y_coords)
            width = max(1.0, max_x - min_x)
            height = max(1.0, max_y - min_y)
            center = np.array([(min_x + max_x) / 2, (min_y + max_y) / 2])
            radius = max(1.0, max(width, height) * 0.75)
        else:
            center = np.array([0.0, 0.0])
            radius = 1.0

        for i, n in enumerate(new_nodes):
            angle = 2 * np.pi * i / len(new_nodes) + np.random.random() * 0.2 - 0.1
            r_jitter = radius * (0.8 + 0.4 * np.random.random())
            pos[n] = np.array([center[0] + r_jitter * np.cos(angle),
                               center[1] + r_jitter * np.sin(angle)])

        node_count = len(G.nodes())
        scale_factor = 1.2
        if node_count > 10:
            scale_factor = 1.2 + (node_count - 10) * 0.05
            scale_factor = min(scale_factor, 3.0)
        k_value = (k or LAYOUT_K) * scale_factor
        log_api(f"Обновляем layout для {len(new_nodes)} новых узлов из {node_count} с k={k_value} (масштаб: {scale_factor:.2f})")
        more_iterations = iterations * (1 + new_nodes_percentage * 2)
        pos = nx.spring_layout(
            G,
            pos=pos,
            fixed=None,
            iterations=int(more_iterations),
            k=k_value,
            seed=42,
            weight=None
        )

    return post_process_layout(G, pos)

def draw_graph(G, ax, title, layout=None):
    ax.clear()
    ax.set_title(title, fontsize=9)
    if len(G.nodes) == 0:
        ax.text(0.5, 0.5, "Нет данных", ha="center", va="center", fontsize=8)
        ax.axis("off")
        return layout

    if layout:
        prev_nodes = set(layout.keys())
        current_nodes = set(G.nodes())
        if prev_nodes != current_nodes:
            node_diff = len(prev_nodes.symmetric_difference(current_nodes))
            total_nodes = max(len(prev_nodes), len(current_nodes))
            if node_diff > 0 and ((node_diff / total_nodes > 0.2) if total_nodes else True) or len(current_nodes) > 15:
                log_api(f"Значительное изменение в составе узлов графа: {node_diff} из {total_nodes}. Сбрасываем layout.")
                layout = {}

    iterations = LAYOUT_ITERATIONS
    if not layout:
        iterations = LAYOUT_ITERATIONS * 3
        log_api(f"Создаем новый layout для графа с {iterations} итерациями")
    elif len(G.nodes) > 15:
        iterations = LAYOUT_ITERATIONS * 2
        log_api(f"Увеличиваем число итераций до {iterations} для большого графа ({len(G.nodes)} узлов)")

    nodes_count = len(G.nodes)
    k_multiplier = 1.0
    if nodes_count > 5:
        k_multiplier = 1.0 + min(2.0, (nodes_count - 5) * 0.1)
        log_api(f"Увеличиваем k в {k_multiplier:.2f} раз для {nodes_count} узлов")

    pos = update_layout(G, layout, k=LAYOUT_K * k_multiplier, iterations=iterations)

    if pos:
        x_coords = [p[0] for p in pos.values()]
        y_coords = [p[1] for p in pos.values()]
        if x_coords and y_coords:
            min_x, max_x = min(x_coords), max(x_coords)
            min_y, max_y = min(y_coords), max(y_coords)
            log_api(f"Координаты узлов: X:[{min_x:.2f}, {max_x:.2f}], Y:[{min_y:.2f}, {max_y:.2f}]")
            boundary = 2.0
            if min_x < -boundary or max_x > boundary or min_y < -boundary or max_y > boundary:
                log_api(f"Обнаружены узлы за пределами границ. Нормализуем координаты.")
                center_x = (min_x + max_x) / 2
                center_y = (min_y + max_y) / 2
                scale = max(1.0, max(max_x - min_x, max_y - min_y)) / (boundary * 1.8)
                for node in pos:
                    pos[node][0] = (pos[node][0] - center_x) / scale
                    pos[node][1] = (pos[node][1] - center_y) / scale

    node_cols = [G.nodes[n].get("color", (0.8, 0.8, 0.8)) for n in G.nodes()]
    nx.draw_networkx_nodes(
        G,
        pos,
        node_size=180,
        node_color=node_cols,
        alpha=0.85,
        ax=ax
    )

    for u, v, d in G.edges(data=True):
        col = d.get("color", (0.5, 0.5, 0.5, 0.5))
        nx.draw_networkx_edges(
            G,
            pos,
            edgelist=[(u, v)],
            width=0.6,
            edge_color=[col],
            arrows=True,
            arrowsize=6,
            alpha=0.8,
            ax=ax
        )

    node_labels = {}
    for n in G.nodes():
        try:
            label = n.split('-')[0] if isinstance(n, str) else str(n)
            if len(label) > 4:
                label = label[:4]
            node_labels[n] = label
        except:
            node_labels[n] = str(n)[:4]

    font_size = 6
    if len(node_labels) > 15:
        font_size = 5

    nx.draw_networkx_labels(
        G,
        pos,
        labels=node_labels,
        font_size=font_size,
        font_family='sans-serif',
        font_weight='bold',
        ax=ax
    )

    ax.set_aspect("equal")
    margin = 0.2
    if pos and len(pos) > 0:
        x_values = [p[0] for p in pos.values()]
        y_values = [p[1] for p in pos.values()]
        if x_values and y_values:
            x_min, x_max = min(x_values), max(x_values)
            y_min, y_max = min(y_values), max(y_values)
            node_factor = min(0.5, 0.1 + 0.02 * len(pos))
            width = max(0.5, x_max - x_min)
            height = max(0.5, y_max - y_min)
            buffer_x = width * (margin + node_factor)
            buffer_y = height * (margin + node_factor)
            ax.set_xlim(x_min - buffer_x, x_max + buffer_x)
            ax.set_ylim(y_min - buffer_y, y_max + buffer_y)
            log_api(f"Установлены границы графа: X:[{x_min - buffer_x:.2f}, {x_max + buffer_x:.2f}], "
                    f"Y:[{y_min - buffer_y:.2f}, {y_max + buffer_y:.2f}]")
    else:
        ax.set_xlim(-1.5, 1.5)
        ax.set_ylim(-1.5, 1.5)

    ax.axis("off")
    return pos

def draw_vote_graph(G, active_requests, ax, title, layout=None):
    ax.clear()
    ax.set_title(title)

    if G is None or not G.nodes or not active_requests:
        ax.text(0.5, 0.5, "Нет активных голосований", ha="center", va="center")
        ax.axis("off")
        return layout or {}

    log_api(f"Отрисовка графа голосования: {len(G.nodes)} узлов, {len(G.edges)} рёбер")

    if layout:
        prev_nodes = set(layout.keys())
        current_nodes = set(G.nodes())
        if prev_nodes != current_nodes:
            node_diff = len(prev_nodes.symmetric_difference(current_nodes))
            total_nodes = max(len(prev_nodes), len(current_nodes))
            if node_diff > 0 and (node_diff / total_nodes > 0.2 if total_nodes else True):
                log_api(f"Значительное изменение в составе узлов графа голосования: {node_diff} из {total_nodes}. Сбрасываем layout.")
                layout = {}

    iterations = LAYOUT_ITERATIONS
    if not layout:
        iterations = LAYOUT_ITERATIONS * 2
        log_api(f"Создаем новый layout для графа голосования с {iterations} итерациями")

    pos = update_layout(G, layout, k=LAYOUT_K, iterations=iterations)

    if pos:
        x_coords = [p[0] for p in pos.values()]
        y_coords = [p[1] for p in pos.values()]
        if x_coords and y_coords:
            min_x, max_x = min(x_coords), max(x_coords)
            min_y, max_y = min(y_coords), max(y_coords)
            log_api(f"Координаты графа голосования: X: [{min_x:.2f}, {max_x:.2f}], Y: [{min_y:.2f}, {max_y:.2f}]")

    regular_nodes = [n for n in G.nodes() if not G.nodes[n].get('is_candidate', False)]
    if regular_nodes:
        node_cols = [G.nodes[n].get("color", (0.8, 0.8, 0.8)) for n in regular_nodes]
        nx.draw_networkx_nodes(
            G, pos,
            nodelist=regular_nodes,
            node_size=180,
            node_color=node_cols,
            alpha=0.85,
            ax=ax
        )

    candidates = [n for n in G.nodes() if G.nodes[n].get('is_candidate', False)]
    if candidates:
        candidate_cols = [G.nodes[n].get("color", (0.8, 0.8, 0.8)) for n in candidates]
        nx.draw_networkx_nodes(
            G, pos,
            nodelist=candidates,
            node_size=200,
            node_color=candidate_cols,
            edgecolors='red',
            linewidths=1.0,
            ax=ax
        )

    for u, v, d in G.edges(data=True):
        col = d.get("color", (0.5, 0.5, 0.5, 0.5))
        width = 0.6 + d.get("vote_value", 1) * 0.2
        nx.draw_networkx_edges(
            G, pos,
            edgelist=[(u, v)],
            width=width,
            edge_color=[col],
            arrows=True,
            arrowsize=6,
            alpha=0.8,
            ax=ax
        )

    node_labels = {}
    for n in G.nodes():
        try:
            label = n.split('-')[0] if isinstance(n, str) else str(n)
            if len(label) > 6:
                label = label[:6]
            node_labels[n] = label
        except:
            node_labels[n] = str(n)[:6]

    nx.draw_networkx_labels(
        G, pos,
        labels=node_labels,
        font_size=6,
        font_family='sans-serif',
        ax=ax
    )

    ax.set_title(title, fontsize=9)
    vote_info = ""

    global votes
    if votes is None:
        votes = []

    for i, request in enumerate(active_requests[:3]):
        try:
            target = str(request.get("target", "неизвестно"))
            if "-" in target:
                target = target.split('-')[0]
            date = datetime.fromtimestamp(request.get("dateUnix", 0)).strftime("%H:%M:%S")
            timeout = request.get("timeoutSecs", 0)
            vote_count = 0
            for vote in votes:
                if not vote.get("deleted", False) and vote.get("requestId") == request.get("id"):
                    vote_count += 1
            vote_info += f"Голосование #{i+1}: за {target}, старт {date}, таймаут {timeout}с, голосов {vote_count}\n"
        except Exception as e:
            vote_info += f"Ошибка при обработке запроса: {str(e)}\n"

    diagnostics_info = ""
    if active_requests and votes:
        diagnostics_info += "\nСтатистика голосов по запросам:\n"
        for i, req in enumerate(active_requests[:3]):
            req_id = req.get("id", "unknown")
            target = req.get("target", "unknown")
            votes_for_this_request = []
            for v in votes:
                if v.get("requestId") == req_id and not v.get("deleted", False):
                    votes_for_this_request.append(v)
            diagnostics_info += f"Запрос {i+1}: ID={req_id[:8]}, Цель={target[:8]}, Голосов={len(votes_for_this_request)}\n"
            for j, v in enumerate(votes_for_this_request[:3]):
                voter = v.get("voter", "")
                if voter and len(voter) > 8:
                    voter = voter[:8] + "..."
                diagnostics_info += f"  Голос {j+1}: От={voter}\n"

    combined_info = vote_info.strip() + "\n" + diagnostics_info.strip()
    if combined_info.strip():
        ax.text(
            0.5, -0.1, combined_info.strip(),
            transform=ax.transAxes,
            horizontalalignment='center',
            verticalalignment='top',
            fontsize=7,
            bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5)
        )

    ax.set_aspect("equal")
    ax.set_xlim(-1.2, 1.2)
    ax.set_ylim(-1.3, 1.2)
    ax.axis("off")

    return pos

def update_plots(frame, fig_axes_layout):
    fig = fig_axes_layout["fig"]
    axes = fig_axes_layout["axes"]
    layout_cache = fig_axes_layout["layout"]
    vote_details = fig_axes_layout["vote_details"]

    ax_vote_graph = vote_details["graph_ax"]
    ax_vote_stats = vote_details["stats_ax"]
    ax_vote_history = vote_details["history_ax"]

    log_api("Начинаем обновление графиков...")

    prev_node_count = len(nodes) if nodes is not None else 0
    data_available = get_data()

    if nodes is not None:
        current_node_count = len(nodes)
        if current_node_count != prev_node_count:
            log_api(f"Изменение количества узлов: было {prev_node_count}, стало {current_node_count}")
            if abs(current_node_count - prev_node_count) > max(1, prev_node_count * 0.1):
                log_api("Значительное изменение количества узлов. Сбрасываем кэши позиций.")
                layout_cache.clear()
                log_api("Кэши позиций сброшены.")
    if last_data_time:
        t_str = datetime.fromtimestamp(last_data_time).strftime("%H:%M:%S")
        fig.suptitle(f"Состояние сети (обновлено: {t_str})", fontsize=11)

    if not api_available or not data_available:
        for ax in axes:
            ax.clear()
            ax.text(0.5, 0.5, "API недоступен", ha="center", va="center", color="red", fontsize=9)
            ax.axis("off")
        return

    # Граф измерений
    measurement_graph = create_measurement_graph()
    prev = layout_cache.get("measurements", {})
    if prev:
        prev_nodes = set(prev.keys())
        current_nodes = set(measurement_graph.nodes())
        if prev_nodes != current_nodes:
            node_diff = len(prev_nodes.symmetric_difference(current_nodes))
            total_nodes = max(len(prev_nodes), len(current_nodes))
            if node_diff > 0 and (node_diff / total_nodes > 0.2 if total_nodes else True):
                log_api(f"Значительное изменение в графе измерений: {node_diff} из {total_nodes} узлов. Пересчитываем layout.")
                prev = {}
                layout_cache["measurements"] = {}
    iterations = LAYOUT_ITERATIONS if prev else LAYOUT_ITERATIONS * 2
    pos_meas = update_layout(measurement_graph, prev, k=LAYOUT_K, iterations=iterations)
    layout_cache["measurements"] = pos_meas
    draw_graph(measurement_graph, axes[0], "Измерения задержки", pos_meas)

    # Граф голосования
    vote_graph, active_requests = create_vote_graph()
    prev_vote_layout = layout_cache.get("vote_graph", {})
    if prev_vote_layout and vote_graph:
        prev_nodes = set(prev_vote_layout.keys())
        current_nodes = set(vote_graph.nodes())
        if prev_nodes != current_nodes:
            node_diff = len(prev_nodes.symmetric_difference(current_nodes))
            total_nodes = max(len(prev_nodes), len(current_nodes))
            if node_diff > 0 and (node_diff / total_nodes > 0.2 if total_nodes else True):
                log_api(f"Значительное изменение в графе голосования: {node_diff} из {total_nodes} узлов. Пересчитываем layout.")
                prev_vote_layout = {}
                layout_cache["vote_graph"] = {}
    vote_iterations = LAYOUT_ITERATIONS if prev_vote_layout else LAYOUT_ITERATIONS * 2
    pos_vote = draw_vote_graph(vote_graph, active_requests, ax_vote_graph, "Голосование за лидера", layout=prev_vote_layout)
    layout_cache["vote_graph"] = pos_vote

    # Статистика голосования
    ax_vote_stats.clear()
    ax_vote_stats.set_title("Статистика голосования")
    try:
        stats_text = ""
        active_nodes_count = len([n for n in nodes if not n.get("deleted", False)]) if nodes is not None else 0
        if active_requests:
            for i, req in enumerate(active_requests[:3]):
                req_id = str(req.get("id", ""))
                if "-" in req_id:
                    req_id = req_id.split("-")[0]
                target_id = str(req.get("target", ""))
                if "-" in target_id:
                    target_id = target_id.split("-")[0]
                creation_time = datetime.fromtimestamp(req.get("dateUnix", 0)).strftime("%H:%M:%S")
                timeout = req.get("timeoutSecs", 0)
                vote_count = 0
                for v in all_votes:
                    if v.get("requestId") == req.get("id", "") and not v.get("deleted", False):
                        vote_count += 1
                quorum = max(1, int(active_nodes_count * 2/3))
                stats_text += f"Запрос #{i+1} ({req_id}):\n"
                stats_text += f"- Цель: {target_id}\n"
                stats_text += f"- Создан: {creation_time}, таймаут: {timeout}с\n"
                stats_text += f"- Голоса: {vote_count}/{active_nodes_count} (кворум: {quorum})\n"
                stats_text += f"- Статус: {'Принят' if vote_count >= quorum else 'Отклонен'}\n\n"
        else:
            stats_text = "Нет активных запросов на голосование"
    except Exception as e:
        stats_text = f"Ошибка при обработке статистики: {str(e)}"
    ax_vote_stats.text(0.05, 0.95, stats_text, transform=ax_vote_stats.transAxes, verticalalignment='top', fontsize=7)
    ax_vote_stats.axis("off")

    # История голосования
    ax_vote_history.clear()
    ax_vote_history.set_title("История голосований")
    try:
        timeline_text = "Последние изменения:\n"
        events = []
        if vote_requests is not None:
            for vr in vote_requests[:5]:
                if vr.get("kind") == "NetworkLeadership":
                    target_id = str(vr.get("target", ""))
                    if "-" in target_id:
                        target_id = target_id.split("-")[0]
                    req_id = str(vr.get("id", ""))
                    if "-" in req_id:
                        req_id = req_id.split("-")[0]
                    events.append({
                        "type": "request",
                        "time": vr.get("dateUnix", 0),
                        "target": target_id,
                        "id": req_id
                    })
        if votes is not None:
            filtered_votes = {}
            sorted_votes = sorted(votes, key=lambda v: v.get("dateUnix", 0))
            for vote in sorted_votes:
                voter_id = str(vote.get("voter", ""))
                if "-" in voter_id:
                    voter_id = voter_id.split("-")[0]
                target_id = str(vote.get("target", ""))
                if "-" in target_id:
                    target_id = target_id.split("-")[0]
                request_id = str(vote.get("requestId", ""))
                if "-" in request_id:
                    request_id = request_id.split("-")[0]
                key = f"{voter_id}_{target_id}_{request_id}"
                filtered_votes[key] = vote
            votes[:] = list(filtered_votes.values())
            for vote in votes:
                voter_id = str(vote.get("voter", ""))
                if "-" in voter_id:
                    voter_id = voter_id.split("-")[0]
                target_id = str(vote.get("target", ""))
                if "-" in target_id:
                    target_id = target_id.split("-")[0]
                vote_id = str(vote.get("id", ""))
                if "-" in vote_id:
                    vote_id = vote_id.split("-")[0]
                events.append({
                    "type": "vote",
                    "time": vote.get("dateUnix", 0),
                    "source": voter_id,
                    "target": target_id,
                    "id": vote_id
                })
                if voter_id:
                    latest_votes[voter_id] = vote
        if leadership_resigns is not None:
            for resign in leadership_resigns[:5]:
                leader_id = str(resign.get("owner", ""))
                if "-" in leader_id:
                    leader_id = leader_id.split("-")[0]
                req_id = str(resign.get("voteRequestID", ""))
                if "-" in req_id:
                    req_id = req_id.split("-")[0]
                events.append({
                    "type": "resign",
                    "time": resign.get("dateUnix", 0),
                    "leader": leader_id,
                    "vote_request_id": req_id
                })
        events.sort(key=lambda x: x.get("time", 0), reverse=True)
        if events:
            for i, event in enumerate(events[:8]):
                event_time = datetime.fromtimestamp(event.get("time", 0)).strftime("%H:%M:%S")
                if event["type"] == "request":
                    timeline_text += f"{event_time}: Запрос #{event['id']} за {event['target']}\n"
                elif event["type"] == "vote":
                    if "voter" in event and "request_id" in event:
                        timeline_text += f"{event_time}: {event['voter']} голосует за #{event['request_id']}\n"
                    elif "source" in event and "target" in event:
                        timeline_text += f"{event_time}: {event['source']} голосует за {event['target']}\n"
                    else:
                        timeline_text += f"{event_time}: Голосование (детали недоступны)\n"
                elif event["type"] == "resign":
                    timeline_text += f"{event_time}: {event['leader']} слагает полномочия #{event['vote_request_id']}\n"
        else:
            timeline_text += "Нет данных об истории голосований"
    except Exception as e:
        timeline_text = f"Ошибка при обработке истории: {str(e)}"
    ax_vote_history.text(0.05, 0.95, timeline_text, transform=ax_vote_history.transAxes, verticalalignment='top', fontsize=7)
    ax_vote_history.axis("off")

    # Графы целей
    goal_graphs = create_goal_graphs()
    if goal_graphs is None:
        log_api("Предупреждение: goal_graphs is None в update_plots")
        goal_graphs = {}
    log_api(f"Создано {len(goal_graphs)} графов целей")
    for goal_id, G in goal_graphs.items():
        log_api(f"Граф цели {goal_id}: {len(G.nodes)} узлов, {len(G.edges)} рёбер")
        if goal_id in layout_cache:
            prev_nodes = set(layout_cache.get(goal_id, {}).keys())
            current_nodes = set(G.nodes())
            if prev_nodes != current_nodes:
                node_diff = len(prev_nodes.symmetric_difference(current_nodes))
                total_nodes = max(len(prev_nodes), len(current_nodes))
                if node_diff > 0 and (node_diff / total_nodes > 0.2 if total_nodes else True):
                    log_api(f"Значительное изменение в составе узлов для цели {goal_id}. Сбрасываем layout.")
                    layout_cache[goal_id] = {}

    goal_start_idx = 2
    for i, (goal_id, G) in enumerate(goal_graphs.items()):
        ax_index = i + goal_start_idx
        if ax_index < len(axes):
            ax = axes[ax_index]
            goal_info = next((g for g in goals if g.get("id") == goal_id), None) if goals else None
            if goal_info:
                title = f"Цель: {goal_info.get('minGroupSize', '?')}-{goal_info.get('maxGroupSize', '?')} узлов"
            else:
                title = f"Цель: {goal_id}"
            prev_layout = layout_cache.get(goal_id, {})
            iterations = LAYOUT_ITERATIONS if prev_layout else LAYOUT_ITERATIONS * 2
            if not prev_layout:
                log_api(f"Полностью пересчитываем layout для цели {goal_id} с {iterations} итерациями")
            pos_goal = update_layout(G, prev_layout, k=LAYOUT_K, iterations=iterations)
            layout_cache[goal_id] = pos_goal
            draw_graph(G, ax, title, pos_goal)
            log_api(f"Отрисован граф цели {goal_id} в оси {ax_index}")

    log_api("Обновление графиков завершено")

def main():
    data_available = get_data()
    goal_graphs = create_goal_graphs()
    goal_count = len(goal_graphs)
    log_api(f"Найдено {goal_count} графов целей")

    total_subplots = 2 + goal_count
    main_graphs_count = 2 + goal_count
    max_per_row = 2
    main_rows = math.ceil(main_graphs_count / max_per_row)
    main_cols = 2
    total_cols = main_cols + 1

    fig = plt.figure(figsize=(7.5 * total_cols, 4.5 * main_rows))
    gs = GridSpec(
        main_rows,
        total_cols,
        figure=fig,
        width_ratios=[1, 1, 0.8],
        wspace=0.5,
        hspace=0.6
    )
    log_api(f"Создана сетка GridSpec: {main_rows} строк, {total_cols} столбцов")
    plt.rcParams.update({'font.size': 8})

    axes = []
    ax_measurements = fig.add_subplot(gs[0, 0])
    ax_measurements.set_title("Измерения задержки", fontsize=9)
    axes.append(ax_measurements)

    ax_vote_graph = fig.add_subplot(gs[0, 1])
    ax_vote_graph.set_title("Граф голосования за лидера", fontsize=9)
    axes.append(ax_vote_graph)

    for i in range(goal_count):
        row = (i + 2) // main_cols
        col = (i + 2) % main_cols
        ax = fig.add_subplot(gs[row, col])
        ax.set_title(f"Цель #{i+1}", fontsize=9)
        axes.append(ax)
        log_api(f"Создана ось для графа цели #{i+1} в позиции [{row}, {col}]")

    ax_vote_stats = fig.add_subplot(gs[0: main_rows // 2, -1])
    ax_vote_stats.set_title("Статистика голосования", fontsize=9)
    ax_vote_history = fig.add_subplot(gs[main_rows // 2 :, -1])
    ax_vote_history.set_title("История голосований", fontsize=9)
    log_api(f"Оси для статистики и истории созданы в столбце {total_cols - 1}")

    fig.suptitle("Состояние сети", fontsize=14)
    plt.subplots_adjust(hspace=0.4, wspace=0.3)

    layout_cache = {}
    if data_available and api_available:
        mg = create_measurement_graph()
        if mg and mg.nodes:
            layout_cache["measurements"] = nx.spring_layout(mg, seed=42, k=LAYOUT_K, iterations=100)
        else:
            layout_cache["measurements"] = {}

        vote_graph, _ = create_vote_graph()
        if vote_graph and vote_graph.nodes:
            layout_cache["vote_graph"] = nx.spring_layout(vote_graph, seed=42, k=LAYOUT_K, iterations=100)
        else:
            layout_cache["vote_graph"] = {}

        for gid, G in goal_graphs.items():
            if G and G.nodes:
                layout_cache[gid] = nx.spring_layout(G, seed=42, k=LAYOUT_K, iterations=100)
            else:
                layout_cache[gid] = {}

    fig_axes_layout = {
        "fig": fig,
        "axes": axes,
        "layout": layout_cache,
        "vote_details": {
            "graph_ax": ax_vote_graph,
            "stats_ax": ax_vote_stats,
            "history_ax": ax_vote_history
        }
    }
    log_api(f"Инициализирована структура данных с {len(axes)} осями для графиков")

    ani = animation.FuncAnimation(
        fig,
        update_plots,
        fargs=(fig_axes_layout,),
        interval=UPDATE_INTERVAL * 1000,
        blit=False,
        cache_frame_data=False
    )

    plt.tight_layout(rect=[0, 0, 1, 0.93])
    plt.subplots_adjust(
        hspace=0.8,
        wspace=0.7,
        top=0.93,
        bottom=0.07,
        left=0.07,
        right=0.93
    )
    plt.show()

def create_consensus_status_view():
    """Создает новое окно с информацией о состоянии консенсуса"""
    fig = plt.figure(figsize=(6.67, 4))
    fig.suptitle("Состояние консенсуса", fontsize=11)
    plt.rcParams.update({'font.size': 8})
    gs = GridSpec(3, 2, figure=fig)

    ax_graph = fig.add_subplot(gs[:2, :])
    ax_graph.set_title("Граф голосования")

    ax_stats = fig.add_subplot(gs[2, 0])
    ax_stats.set_title("Статистика голосов")
    ax_stats.axis("off")

    ax_timeline = fig.add_subplot(gs[2, 1])
    ax_timeline.set_title("История голосований")
    ax_timeline.axis("off")

    def update(frame):
        data_available = get_data()
        if not data_available or not api_available:
            for ax in [ax_graph, ax_stats, ax_timeline]:
                ax.clear()
                ax.text(0.5, 0.5, "API недоступен",
                        ha="center", va="center", color="red", fontsize=14)
                ax.axis("off")
            return

        vote_graph, active_requests = create_vote_graph()

        ax_graph.clear()
        ax_graph.set_title("Граф голосования за лидера")
        if not vote_graph or not vote_graph.nodes or not active_requests:
            ax_graph.text(0.5, 0.5, "Нет активных голосований",
                          ha="center", va="center", fontsize=12)
            ax_graph.axis("off")
        else:
            try:
                pos = nx.spring_layout(vote_graph, seed=42, k=0.3)
                regular_nodes = [n for n in vote_graph.nodes() if not vote_graph.nodes[n].get('is_candidate', False)]
                if regular_nodes:
                    node_cols = [vote_graph.nodes[n].get("color", (0.8, 0.8, 0.8)) for n in regular_nodes]
                    nx.draw_networkx_nodes(
                        vote_graph, pos,
                        nodelist=regular_nodes,
                        node_size=250,
                        node_color=node_cols,
                        alpha=0.85,
                        ax=ax_graph
                    )

                candidates = [n for n in vote_graph.nodes() if vote_graph.nodes[n].get('is_candidate', False)]
                if candidates:
                    candidate_cols = [vote_graph.nodes[n].get("color", (0.8, 0.8, 0.8)) for n in candidates]
                    nx.draw_networkx_nodes(
                        vote_graph, pos,
                        nodelist=candidates,
                        node_size=280,
                        node_color=candidate_cols,
                        edgecolors='red',
                        linewidths=1.0,
                        ax=ax_graph
                    )

                for u, v, d in vote_graph.edges(data=True):
                    col = d.get("color", (0.5, 0.5, 0.5, 0.7))
                    width = 1.0 + d.get("vote_value", 1) * 0.5
                    nx.draw_networkx_edges(
                        vote_graph, pos,
                        edgelist=[(u, v)],
                        width=width,
                        edge_color=[col],
                        arrows=True,
                        arrowsize=12,
                        ax=ax_graph
                    )

                node_labels = {}
                for n in vote_graph.nodes():
                    try:
                        label = n.split('-')[0] if isinstance(n, str) else str(n)
                        if len(label) > 6:
                            label = label[:6]
                        node_labels[n] = label
                    except:
                        node_labels[n] = str(n)[:6]

                nx.draw_networkx_labels(
                    vote_graph, pos,
                    labels=node_labels,
                    font_size=8,
                    font_family='sans-serif',
                    ax=ax_graph
                )

                ax_graph.set_aspect("equal")
                ax_graph.axis("off")
            except Exception as e:
                ax_graph.clear()
                ax_graph.text(0.5, 0.5, f"Ошибка отрисовки: {str(e)}",
                              ha="center", va="center", color="red", fontsize=10)
                ax_graph.axis("off")

        ax_stats.clear()
        ax_stats.set_title("Статистика голосования")
        try:
            stats_text = ""
            active_nodes_count = len([n for n in nodes if not n.get("deleted", False)]) if nodes is not None else 0
            if active_requests:
                for i, req in enumerate(active_requests[:3]):
                    req_id = str(req.get("id", ""))
                    if "-" in req_id:
                        req_id = req_id.split("-")[0]
                    target_id = str(req.get("target", ""))
                    if "-" in target_id:
                        target_id = target_id.split("-")[0]
                    creation_time = datetime.fromtimestamp(req.get("dateUnix", 0)).strftime("%H:%M:%S")
                    timeout = req.get("timeoutSecs", 0)
                    vote_count = 0
                    for v in votes:
                        if v.get("requestId") == req_id and not v.get("deleted", False):
                            vote_count += 1
                    log_api(f"Найдено {vote_count} голосов для запроса {req_id}")
                    quorum = max(1, int(active_nodes_count * 2/3))
                    stats_text += f"Запрос #{i+1} ({req_id}):\n"
                    stats_text += f"- Цель: {target_id}\n"
                    stats_text += f"- Создан: {creation_time}, таймаут: {timeout}с\n"
                    stats_text += f"- Голоса: {vote_count}/{active_nodes_count} (кворум: {quorum})\n"
                    stats_text += f"- Статус: {'Принят' if vote_count >= quorum else 'Ожидание'}\n\n"
            else:
                stats_text = "Нет активных запросов на голосование"
        except Exception as e:
            stats_text = f"Ошибка при обработке статистики: {str(e)}"
        ax_stats.text(0.05, 0.95, stats_text, transform=ax_stats.transAxes, verticalalignment='top', fontsize=10)
        ax_stats.axis("off")

        ax_timeline.clear()
        ax_timeline.set_title("История голосований")
        try:
            timeline_text = "Последние изменения:\n"
            events = []
            if vote_requests is not None:
                for vr in vote_requests[:5]:
                    if vr.get("kind") == "NetworkLeadership":
                        target_id = str(vr.get("target", ""))
                        if "-" in target_id:
                            target_id = target_id.split("-")[0]
                        req_id = str(vr.get("id", ""))
                        if "-" in req_id:
                            req_id = req_id.split("-")[0]
                        events.append({
                            "type": "request",
                            "time": vr.get("dateUnix", 0),
                            "target": target_id,
                            "id": req_id
                        })
            if votes is not None:
                for vote in votes[:10]:
                    voter_id = str(vote.get("voter", ""))
                    if "-" in voter_id:
                        voter_id = voter_id.split("-")[0]
                    req_id = str(vote.get("requestId", ""))
                    if "-" in req_id:
                        req_id = req_id.split("-")[0]
                    events.append({
                        "type": "vote",
                        "time": vote.get("dateUnix", 0),
                        "voter": voter_id,
                        "request_id": req_id
                    })
            if leadership_resigns is not None:
                for resign in leadership_resigns[:5]:
                    leader_id = str(resign.get("owner", ""))
                    if "-" in leader_id:
                        leader_id = leader_id.split("-")[0]
                    req_id = str(resign.get("voteRequestID", ""))
                    if "-" in req_id:
                        req_id = req_id.split("-")[0]
                    events.append({
                        "type": "resign",
                        "time": resign.get("dateUnix", 0),
                        "leader": leader_id,
                        "vote_request_id": req_id
                    })
            events.sort(key=lambda x: x.get("time", 0), reverse=True)
            if events:
                for i, event in enumerate(events[:8]):
                    event_time = datetime.fromtimestamp(event.get("time", 0)).strftime("%H:%M:%S")
                    if event["type"] == "request":
                        timeline_text += f"{event_time}: Запрос #{event['id']} за {event['target']}\n"
                    elif event["type"] == "vote":
                        timeline_text += f"{event_time}: {event['voter']} голосует за #{event['request_id']}\n"
                    elif event["type"] == "resign":
                        timeline_text += f"{event_time}: {event['leader']} слагает полномочия #{event['vote_request_id']}\n"
            else:
                timeline_text += "Нет данных об истории голосований"
        except Exception as e:
            timeline_text = f"Ошибка при обработке истории: {str(e)}"
        ax_timeline.text(0.05, 0.95, timeline_text, transform=ax_timeline.transAxes, verticalalignment='top', fontsize=10)
        ax_timeline.axis("off")

    ani = animation.FuncAnimation(
        fig,
        update,
        interval=UPDATE_INTERVAL * 1000,
        blit=False,
        cache_frame_data=False
    )

    plt.tight_layout(rect=[0, 0, 1, 0.95])
    plt.subplots_adjust(hspace=0.5)
    plt.show()

if __name__ == "__main__":
    main()
