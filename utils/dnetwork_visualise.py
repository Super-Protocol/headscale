#!/usr/bin/env python3
import os
import sys
import glob
import re
import math
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.animation as animation
import networkx as nx
from networkx.drawing.nx_agraph import read_dot
from matplotlib.widgets import Button
import hashlib

# Размер кружков (нод) на графике
default_node_size = 50
# Размер шрифта
default_font_size = 6

# Словарь для хранения цвета каждого узла
node_color_map = {}

def get_color_for_node(name):
    """
    Берём первые 3 байта MD5(name) и превращаем их в RGB-кортеж из [0..1].
    """
    h = hashlib.md5(str(name).encode("utf-8")).digest()
    return (h[0]/255.0, h[1]/255.0, h[2]/255.0)

# Получаем путь к экспортированным .dot файлам из переменной окружения.
export_base = os.getenv("HEADSCALE_TEST_DNETWORK_EXPORT_DOT_PATH")
if not export_base:
    print("Environment variable HEADSCALE_TEST_DNETWORK_EXPORT_DOT_PATH is not set.")
    sys.exit(1)

# Получаем список директорий серверов
server_dirs = [d for d in os.listdir(export_base) if os.path.isdir(os.path.join(export_base, d))]
if not server_dirs:
    print("No server directories found in the export base folder.")
    sys.exit(1)

# Опционально: ограничение количества серверов для отображения
max_servers = 6
server_dirs = server_dirs[:max_servers]

# Разрезы (префиксы) для визуализации
prefixes = ["group:kubernetes", "latency_class"]

# Собираем файлы .dot по серверам и префиксам
server_frames = {}
for server in server_dirs:
    server_path = os.path.join(export_base, server)
    files = glob.glob(os.path.join(server_path, "*.dot"))
    cut_dict = {}
    for prefix in prefixes:
        pattern = re.compile(re.escape(prefix) + r"\.(\d+)\.dot$")
        frames = []
        for f in files:
            base = os.path.basename(f)
            m = pattern.match(base)
            if m:
                frames.append((int(m.group(1)), f))
        frames.sort(key=lambda x: x[0])
        cut_dict[prefix] = [f for _, f in frames]
    server_frames[server] = cut_dict

# Определяем максимальное число кадров
max_frames = 0
for svc in server_frames.values():
    for lst in svc.values():
        max_frames = max(max_frames, len(lst))
print("Maximum number of frames:", max_frames)

# Настройка сетки подграфиков
num_servers = len(server_dirs)
servers_per_row = 2
num_rows = math.ceil(num_servers / servers_per_row)
num_cuts = len(prefixes)
total_cols = servers_per_row * num_cuts

# Размер фигуры
scale_x, scale_y = 3, 3
fig, axes = plt.subplots(num_rows, total_cols,
                         figsize=(scale_x * total_cols, scale_y * num_rows))

# Приводим axes к 2D-массиву
if num_rows == 1 and total_cols == 1:
    axes = [[axes]]
elif num_rows == 1:
    axes = [axes]
elif total_cols == 1:
    axes = [[ax] for ax in axes]

# Сопоставляем каждому серверу его оси
server_axes = {}
sorted_servers = sorted(server_frames.keys())
for idx, server in enumerate(sorted_servers):
    row = idx // servers_per_row
    col_off = (idx % servers_per_row) * num_cuts
    if num_rows == 1:
        server_axes[server] = axes[col_off:col_off + num_cuts]
    else:
        server_axes[server] = axes[row][col_off:col_off + num_cuts]

# Для хранения позиций и предыдущих рёбер
prev_positions = {}
window_size = 0
prev_edges = {}

def average_positions(pos_list, nodes):
    avg = {}
    for node in nodes:
        coords = []
        for pos in pos_list:
            if node in pos:
                coords.append(np.array(pos[node]))
        if coords:
            avg[node] = np.mean(coords, axis=0).tolist()
    return avg


def load_graph(dot_file):
    try:
        return nx.Graph(read_dot(dot_file))
    except Exception as e:
        print(f"Error reading {dot_file}: {e}")
        return nx.Graph()


def update(frame):
    global prev_positions, prev_edges
    for server in sorted(server_frames.keys()):
        axes_list = server_axes[server]
        for j, prefix in enumerate(prefixes):
            ax = axes_list[j]
            ax.clear()
            lst = server_frames[server][prefix]
            if not lst:
                ax.set_title(f"{server} - {prefix}\nNo files")
                ax.axis('off')
                continue
            idx = frame if frame < len(lst) else len(lst) - 1
            dot_f = lst[idx]
            G = load_graph(dot_f)

            curr_edges = set(G.edges())
            key = (server, prefix)

            if key in prev_edges and curr_edges == prev_edges[key]:
                if key in prev_positions and prev_positions[key]:
                    pos = prev_positions[key][-1]
                else:
                    pos = nx.spring_layout(G, iterations=10, seed=42)
                    prev_positions[key] = [pos]
            else:
                window = prev_positions.get(key, [])
                avg = average_positions(window, G.nodes()) if window else {}
                pos = (nx.spring_layout(G, pos=avg, iterations=10, seed=42)
                       if avg else nx.spring_layout(G, iterations=10, seed=42))
                window.append(pos)
                if len(window) > window_size:
                    window.pop(0)
                prev_positions[key] = window
                prev_edges[key] = curr_edges

            # Раскраска узлов
            nodes = list(G.nodes())
            colors = []
            for n in nodes:
                if n not in node_color_map:
                    node_color_map[n] = get_color_for_node(n)
                colors.append(node_color_map[n])

            # Рисуем рёбра и узлы отдельно
            nx.draw_networkx_edges(G, pos, ax=ax, edge_color='black', arrows=False)
            for node in G.nodes():
                if node not in pos:
                    pos[node] = nx.spring_layout({node: None})[node]


            nx.draw_networkx_nodes(
                G, pos, ax=ax,
                nodelist=nodes,
                node_size=default_node_size,
                node_color=colors
            )
            ax.set_title(f"{server} - {prefix}", fontsize=default_font_size)
            ax.axis('off')
    fig.suptitle(f"Graph Visualization, frame {frame + 1}/{max_frames}")

# Анимация
ani = animation.FuncAnimation(fig, update, frames=max_frames, interval=500, repeat=False)

# Управление анимацией
current_frame = 0
running = True
fig.subplots_adjust(bottom=0.15)

ax_prev  = fig.add_axes([0.10, 0.05, 0.10, 0.05])
ax_pause = fig.add_axes([0.25, 0.05, 0.10, 0.05])
ax_next  = fig.add_axes([0.40, 0.05, 0.10, 0.05])

btn_prev  = Button(ax_prev,  '<< Prev')
btn_pause = Button(ax_pause, 'Pause')
btn_next  = Button(ax_next,  'Next >>')

def on_pause(event):
    global running
    if running:
        ani.event_source.stop()
        btn_pause.label.set_text('Play')
    else:
        ani.event_source.start()
        btn_pause.label.set_text('Pause')
    running = not running


def on_next(event):
    global current_frame, running
    if running:
        ani.event_source.stop()
        running = False
        btn_pause.label.set_text('Play')
    current_frame = min(current_frame + 1, max_frames - 1)
    update(current_frame)
    fig.canvas.draw_idle()


def on_prev(event):
    global current_frame, running
    if running:
        ani.event_source.stop()
        running = False
        btn_pause.label.set_text('Play')
    current_frame = max(current_frame - 1, 0)
    update(current_frame)
    fig.canvas.draw_idle()

btn_pause.on_clicked(on_pause)
btn_next .on_clicked(on_next)
btn_prev .on_clicked(on_prev)

plt.show()