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
from networkx.drawing.nx_pydot import read_dot

default_node_size = 50  # Размер кружков (нод) на графике
default_font_size = 6    # Размер шрифта

# Получаем путь к экспортированным .dot файлам из переменной окружения.
export_base = os.getenv("HEADSCALE_TEST_DNETWORK_EXPORT_DOT_PATH")
if not export_base:
    print("Environment variable HEADSCALE_TEST_DNETWORK_EXPORT_DOT_PATH is not set.")
    sys.exit(1)

# Получаем список директорий серверов (имя директории должно иметь вид 'host:port').
server_dirs = [d for d in os.listdir(export_base) if os.path.isdir(os.path.join(export_base, d))]
if not server_dirs:
    print("No server directories found in the export base folder.")
    sys.exit(1)

# Опционально: ограничение количества серверов для отображения
max_servers = 6  # Задайте нужное значение
server_dirs = server_dirs[:max_servers]

# ===== НОВОЕ: Задаем список разрезов (префиксов) для визуализации =====
prefixes = ["group:kubernetes", "latency_class"]

# ===== НОВОЕ: Для каждого сервера собираем файлы для каждого разреза =====
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
            match = pattern.match(base)
            if match:
                frames.append((int(match.group(1)), f))
        frames.sort(key=lambda x: x[0])
        cut_dict[prefix] = [f for _, f in frames]
    server_frames[server] = cut_dict

# ===== НОВОЕ: Определяем максимальное количество кадров среди всех серверов и разрезов. =====
max_frames = 0
for server in server_frames:
    for prefix in prefixes:
        num_frames = len(server_frames[server][prefix])
        if num_frames > max_frames:
            max_frames = num_frames
print("Maximum number of frames:", max_frames)

# ===== НОВОЕ: Создание подграфиков с ограничением по количеству серверов в ряду и масштабом =====
num_servers = len(server_dirs)
servers_per_row = 2  # Выводить по 2 сервера в ряд
num_rows = math.ceil(num_servers / servers_per_row)
num_cuts = len(prefixes)
# Общая ширина = (servers_per_row * num_cuts) и высота = num_rows
total_cols = servers_per_row * num_cuts

# Задаем масштаб: scale_x - ширина одного столбца, scale_y - высота одной строки
scale_x = 3
scale_y = 3
fig, axes = plt.subplots(num_rows, total_cols, figsize=(scale_x * total_cols, scale_y * num_rows))

# Приводим axes к 2D списку в зависимости от формы
if num_rows == 1 and total_cols == 1:
    axes = [[axes]]
elif num_rows == 1:
    axes = [axes]
elif total_cols == 1:
    axes = [[ax] for ax in axes]

# Создаем словарь, сопоставляющий каждому серверу список осей для его префиксов
server_axes = {}
sorted_servers = sorted(server_frames.keys())
for idx, server in enumerate(sorted_servers):
    row = idx // servers_per_row
    col_offset = (idx % servers_per_row) * num_cuts
    # Если num_rows == 1, axes — это одномерный список
    if num_rows == 1:
        server_axes[server] = axes[col_offset:col_offset + num_cuts]
    else:
        server_axes[server] = axes[row][col_offset:col_offset + num_cuts]

# Сохраняем для каждого (сервер, префикс) последние n позиций (скользящее окно) и набор рёбер предыдущего кадра.
prev_positions = {}   # ключи: (server, prefix)
window_size = 0
prev_edges = {}       # ключи: (server, prefix)

def average_positions(pos_list, nodes):
    """
    Вычисляет средние координаты для узлов, используя список словарей позиций.
    Рассчитываются только координаты для узлов, присутствующих в текущем графе.
    """
    avg = {}
    for node in nodes:
        coords = []
        for pos_dict in pos_list:
            if node in pos_dict:
                coords.append(np.array(pos_dict[node]))
        if coords:
            avg[node] = np.mean(coords, axis=0).tolist()
    return avg

def load_graph(dot_file):
    """Читает DOT файл и возвращает объект графа NetworkX."""
    try:
        G = read_dot(dot_file)
        return G
    except Exception as e:
        print(f"Error reading {dot_file}: {e}")
        return nx.Graph()

def update(frame):
    global prev_positions, prev_edges
    for server in sorted(server_frames.keys()):
        ax_list = server_axes[server]
        for j, prefix in enumerate(prefixes):
            ax = ax_list[j]
            ax.clear()
            frames_list = server_frames[server][prefix]
            if not frames_list:
                ax.set_title(f"{server} - {prefix}\nNo files")
                ax.axis('off')
                continue
            file_index = frame if frame < len(frames_list) else len(frames_list) - 1
            dot_file = frames_list[file_index]
            G = load_graph(dot_file)

            # Определяем набор рёбер текущего графа
            current_edges = set(G.edges())
            key = (server, prefix)

            if key in prev_edges and current_edges == prev_edges[key]:
                if key in prev_positions and prev_positions[key]:
                    new_pos = prev_positions[key][-1]
                else:
                    new_pos = nx.spring_layout(G, iterations=10, seed=42)
                    prev_positions[key] = [new_pos]
            else:
                window = prev_positions.get(key, [])
                computed_avg = average_positions(window, G.nodes()) if window else {}
                if computed_avg:
                    new_pos = nx.spring_layout(G, pos=computed_avg, iterations=10, seed=42)
                else:
                    new_pos = nx.spring_layout(G, iterations=10, seed=42)
                window.append(new_pos)
                if len(window) > window_size:
                    window.pop(0)
                prev_positions[key] = window
                prev_edges[key] = current_edges

            nx.draw_networkx(G, new_pos, ax=ax, with_labels=False, arrows=False,
                 node_size=default_node_size, font_size=default_font_size)
            ax.set_title(f"{server} - {prefix}", fontsize=default_font_size)
            ax.axis('off')
    fig.suptitle(f"Graph Visualization, frame {frame + 1}/{max_frames}")

# Создаем анимацию с интервалом 500 мс.
ani = animation.FuncAnimation(fig, update, frames=max_frames, interval=500, repeat=False)

plt.tight_layout()
plt.show()