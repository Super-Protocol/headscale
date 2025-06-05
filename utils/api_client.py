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

API_BASE_URL = "http://localhost:8911"
UPDATE_INTERVAL = 2.0
TRANSPARENCY_DECAY_TIME = 10.0
LAYOUT_ITERATIONS = 30
LAYOUT_K = 0.2

nodes = []
measurements = []
groups = []
goals = []
last_data_time = None
api_available = True
node_colors = {}
group_colors = {}

latency_cmap = LinearSegmentedColormap.from_list(
    "latency", [(0, "green"), (0.5, "yellow"), (1, "red")]
)

def get_color_from_string(string_value):
    h = hashlib.md5(string_value.encode()).hexdigest()
    r = int(h[0:2], 16) / 255.0
    g = int(h[2:4], 16) / 255.0
    b = int(h[4:6], 16) / 255.0
    factor = 0.7
    r = min(1.0, r + (1.0 - r) * factor)
    g = min(1.0, g + (1.0 - g) * factor)
    b = min(1.0, b + (1.0 - b) * factor)
    return (r, g, b)

def get_data():
    global nodes, measurements, groups, goals, last_data_time, api_available
    try:
        nodes = []
        measurements = []
        groups = []
        goals = []

        resp = requests.get(f"{API_BASE_URL}/api/nodes", timeout=2)
        if resp.status_code != 200:
            api_available = False
            return False
        nodes = resp.json()

        resp = requests.get(f"{API_BASE_URL}/api/measurements", timeout=2)
        if resp.status_code != 200:
            api_available = False
            return False
        measurements = resp.json()

        resp = requests.get(f"{API_BASE_URL}/api/groups", timeout=2)
        if resp.status_code != 200:
            api_available = False
            return False
        groups = resp.json()

        resp = requests.get(f"{API_BASE_URL}/api/groupgoals", timeout=2)
        if resp.status_code != 200:
            api_available = False
            return False
        goals = resp.json()

        last_data_time = time.time()
        api_available = True
        return True
    except (requests.RequestException, ConnectionError, TimeoutError):
        api_available = False
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
    for goal in goals or []:
        G = nx.Graph()
        gid = goal["id"]
        for node in nodes or []:
            nid = node["id"]
            if nid not in node_colors:
                node_colors[nid] = get_color_from_string(nid)
            G.add_node(nid, color=node_colors[nid])

        related = [g for g in groups or [] if g["goal"] == gid]
        for grp in related:
            grpid = grp["id"]
            if grpid not in group_colors:
                group_colors[grpid] = get_color_from_string(grpid)
            col = group_colors[grpid]
            members = [p["id"] for p in grp["participants"]]
            for i in range(len(members)):
                for j in range(i+1, len(members)):
                    G.add_edge(
                        members[i], members[j],
                        group_id=grpid,
                        color=(col[0], col[1], col[2], 0.7)
                    )
        graphs[gid] = G
    return graphs

def update_layout(G, prev_layout=None, k=None, iterations=50):
    if prev_layout is None or not prev_layout:
        return nx.spring_layout(G, seed=42, k=k or LAYOUT_K)

    existing = set(prev_layout.keys())
    current = set(G.nodes())
    if existing == current:
        return prev_layout

    new_nodes = current - existing
    pos = {n: prev_layout[n] for n in existing & current}

    if new_nodes:
        center = np.mean(list(pos.values()), axis=0) if pos else np.array([0.0, 0.0])
        radius = 0.5
        for i, n in enumerate(new_nodes):
            angle = 2 * np.pi * i / len(new_nodes)
            pos[n] = np.array([center[0] + radius * np.cos(angle),
                               center[1] + radius * np.sin(angle)])
        pos = nx.spring_layout(
            G,
            pos=pos,
            fixed=list(existing & current),
            iterations=iterations,
            k=k or LAYOUT_K,
            seed=42
        )
    return pos

def draw_graph(G, ax, title, layout=None):
    ax.clear()
    ax.set_title(title)
    if len(G.nodes) == 0:
        ax.text(0.5, 0.5, "Нет данных", ha="center", va="center")
        ax.axis("off")
        return layout

    pos = layout or {}
    node_cols = [G.nodes[n].get("color", (0.8,0.8,0.8)) for n in G.nodes()]
    nx.draw_networkx_nodes(G, pos, node_size=500, node_color=node_cols, ax=ax)

    for u, v, d in G.edges(data=True):
        col = d.get("color", (0.5, 0.5, 0.5, 0.5))
        nx.draw_networkx_edges(G, pos, edgelist=[(u, v)], width=2.0, edge_color=[col], arrows=True, ax=ax)

    ax.set_aspect("equal")
    ax.set_xlim(-1.2, 1.2)
    ax.set_ylim(-1.2, 1.2)
    ax.axis("off")
    return pos

def update_plots(frame, fig_axes_layout):
    fig, axes, layout_cache = fig_axes_layout["fig"], fig_axes_layout["axes"], fig_axes_layout["layout"]

    get_data()
    if last_data_time:
        t_str = datetime.fromtimestamp(last_data_time).strftime("%H:%M:%S")
        fig.suptitle(f"Состояние сети (обновлено: {t_str})", fontsize=14)

    if not api_available:
        for ax in axes:
            ax.clear()
            ax.text(0.5, 0.5, "API недоступен", ha="center", va="center", color="red", fontsize=14)
            ax.axis("off")
        return

    measurement_graph = create_measurement_graph()
    prev = layout_cache.get("measurements", None)
    pos_meas = update_layout(measurement_graph, prev, k=LAYOUT_K, iterations=LAYOUT_ITERATIONS)
    layout_cache["measurements"] = pos_meas
    draw_graph(measurement_graph, axes[0], "Измерения задержки", pos_meas)

    goal_graphs = create_goal_graphs()
    for i, (goal_id, G) in enumerate(goal_graphs.items()):
        ax = axes[i+1]
        goal_info = next((g for g in goals if g["id"] == goal_id), None)
        title = f"Цель: {goal_info['minGroupSize']}-{goal_info['maxGroupSize']} узлов"
        prev_layout = layout_cache.get(goal_id, None)
        pos_goal = update_layout(G, prev_layout, k=LAYOUT_K, iterations=LAYOUT_ITERATIONS)
        layout_cache[goal_id] = pos_goal
        draw_graph(G, ax, title, pos_goal)

def main():
    # Шаг 1. Получаем начальные данные, чтобы узнать число целей
    get_data()
    goal_graphs = create_goal_graphs()
    goal_count = len(goal_graphs)  # сколько у нас целей

    # Шаг 2. Решаем, сколько всего субплотов: 1 (измерения) + goal_count
    total_subplots = 1 + goal_count

    # Шаг 3. Подбираем размер сетки: возьмём 2 графа в строке
    max_per_row = 2
    rows = math.ceil(total_subplots / max_per_row)
    cols = min(total_subplots, max_per_row)

    # Шаг 4. Создаём фигуру
    fig, axes = plt.subplots(rows, cols, figsize=(8*cols, 6*rows))
    # Приводим axes к плоскому списку
    if isinstance(axes, np.ndarray):
        axes = axes.flatten()
    else:
        axes = [axes]

    # Если вдруг создано больше осей, чем нужно, удалим лишние:
    if len(axes) > total_subplots:
        for idx in range(total_subplots, len(axes)):
            fig.delaxes(axes[idx])
    axes = axes[:total_subplots]

    fig.suptitle("Состояние сети", fontsize=14)
    plt.subplots_adjust(hspace=0.3, wspace=0.3)

    layout_cache = {}
    if api_available:
        # сразу заполняем layout_cache начальными координатами
        mg = create_measurement_graph()
        layout_cache["measurements"] = nx.spring_layout(mg, seed=42, k=LAYOUT_K, iterations=100)
        for gid, G in goal_graphs.items():
            layout_cache[gid] = nx.spring_layout(G, seed=42, k=LAYOUT_K, iterations=100)

    fig_axes_layout = {
        "fig": fig,
        "axes": axes,
        "layout": layout_cache
    }

    ani = animation.FuncAnimation(
        fig,
        update_plots,
        fargs=(fig_axes_layout,),
        interval=UPDATE_INTERVAL * 1000,
        blit=False,
        cache_frame_data=False
    )

    plt.tight_layout(rect=[0, 0, 1, 0.96])
    plt.show()

if __name__ == "__main__":
    main()
