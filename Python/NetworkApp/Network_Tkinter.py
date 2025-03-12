import pandas as pd
import networkx as nx
import matplotlib.pyplot as plt
from collections import defaultdict
import numpy as np
import tkinter as tk
from tkinter import ttk
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg

# URL cruda del archivo en GitHub
URL_CSV = "https://raw.githubusercontent.com/ringoquimico/DATA_SOURCES/main/Network_Full.csv"

# Función para mostrar ventana de carga
def show_loading_window(root):
    loading_window = tk.Toplevel(root)
    loading_window.title("Cargando")
    loading_window.geometry("300x100")
    loading_window.transient(root)  # Vincula la ventana de carga al root
    loading_window.grab_set()  # Bloquea interacciones con otras ventanas
    loading_window.overrideredirect(True)  # Elimina bordes de la ventana
    
    # Centrar la ventana de carga
    loading_window.update_idletasks()
    x = root.winfo_x() + (root.winfo_width() // 2) - (300 // 2)
    y = root.winfo_y() + (root.winfo_height() // 2) - (100 // 2)
    loading_window.geometry(f"300x100+{x}+{y}")
    
    label = tk.Label(loading_window, text="Cargando modelo, por favor espera...", font=("Arial", 12))
    label.pack(expand=True)
    
    return loading_window

# Crear la ventana principal
root = tk.Tk()
root.title("Análisis de Redes de Interacciones entre Grupos")
root.geometry("1200x800")

# Mostrar ventana de carga
loading_window = show_loading_window(root)

# Cargar datos inicialmente para obtener los grupos únicos
data = pd.read_csv(URL_CSV, quotechar='"', sep=';', encoding='utf-8')
rated_groups = sorted(data["CSAT Rated Group Name"].unique().tolist())

# Frame para controles
control_frame = ttk.Frame(root)
control_frame.pack(side=tk.LEFT, fill=tk.Y, padx=10, pady=10)

# Variables de tkinter
rated_group_var = tk.StringVar(value="ES Support T1")
k_value_var = tk.DoubleVar(value=5.0)
seed_var = tk.IntVar(value=2025)
min_size_var = tk.IntVar(value=200)
max_size_var = tk.IntVar(value=4000)
dark_mode_var = tk.BooleanVar(value=False)

# Funciones para actualizar etiquetas
def update_k_label(*args):
    k_label.config(text=f"Valor: {k_value_var.get():.1f}")

def update_seed_label(*args):
    seed_label.config(text=f"Valor: {seed_var.get()}")

def update_min_size_label(*args):
    min_size_label.config(text=f"Valor: {min_size_var.get()}")

def update_max_size_label(*args):
    max_size_label.config(text=f"Valor: {max_size_var.get()}")

# Configurar estilos
style = ttk.Style()
style.configure("Dark.TFrame", background="#1E1E1E")
style.configure("Dark.TMenubutton", background="#2F5597", foreground="white")
style.configure("Light.TMenubutton", background="SystemButtonFace", foreground="black")

# Función para aplicar modo oscuro
def toggle_dark_mode(*args):
    if dark_mode_var.get():
        root.configure(bg="#1E1E1E")
        control_frame.configure(style="Dark.TFrame")
        style.configure("TLabel", background="#1E1E1E", foreground="white")
        style.configure("TCheckbutton", background="#1E1E1E", foreground="white")
        style.configure("Horizontal.TScale", background="#1E1E1E")
        style.configure("TMenubutton", background="#2F5597", foreground="white")
        group_menu.configure(style="Dark.TMenubutton")
        update_button.configure(bg="#2F5597", fg="white")
    else:
        root.configure(bg="SystemButtonFace")
        control_frame.configure(style="TFrame")
        style.configure("TLabel", background="SystemButtonFace", foreground="black")
        style.configure("TCheckbutton", background="SystemButtonFace", foreground="black")
        style.configure("Horizontal.TScale", background="SystemButtonFace")
        style.configure("TMenubutton", background="SystemButtonFace", foreground="black")
        group_menu.configure(style="Light.TMenubutton")
        update_button.configure(bg="SystemButtonFace", fg="black")
    update_plots()

# Controles
ttk.Label(control_frame, text="Rated Group Name").pack(pady=5)
group_menu = ttk.OptionMenu(control_frame, rated_group_var, "ES Support T1", *rated_groups)
group_menu.pack(pady=5)

ttk.Label(control_frame, text="Valor de K (Distancia entre nodos)").pack(pady=5)
k_slider = ttk.Scale(control_frame, from_=0.1, to=10.0, variable=k_value_var, orient=tk.HORIZONTAL)
k_slider.pack(pady=5)
k_label = ttk.Label(control_frame, text=f"Valor: {k_value_var.get():.1f}")
k_label.pack(pady=2)
k_value_var.trace("w", update_k_label)

ttk.Label(control_frame, text="Semilla de Aleatoriedad").pack(pady=5)
seed_slider = ttk.Scale(control_frame, from_=1, to=9999, variable=seed_var, orient=tk.HORIZONTAL)
seed_slider.pack(pady=5)
seed_label = ttk.Label(control_frame, text=f"Valor: {seed_var.get()}")
seed_label.pack(pady=2)
seed_var.trace("w", update_seed_label)

ttk.Label(control_frame, text="Tamaño Mínimo de Nodos").pack(pady=5)
min_size_slider = ttk.Scale(control_frame, from_=50, to=1000, variable=min_size_var, orient=tk.HORIZONTAL)
min_size_slider.pack(pady=5)
min_size_label = ttk.Label(control_frame, text=f"Valor: {min_size_var.get()}")
min_size_label.pack(pady=2)
min_size_var.trace("w", update_min_size_label)

ttk.Label(control_frame, text="Tamaño Máximo de Nodos").pack(pady=5)
max_size_slider = ttk.Scale(control_frame, from_=500, to=8000, variable=max_size_var, orient=tk.HORIZONTAL)
max_size_slider.pack(pady=5)
max_size_label = ttk.Label(control_frame, text=f"Valor: {max_size_var.get()}")
max_size_label.pack(pady=2)
max_size_var.trace("w", update_max_size_label)

dark_mode_check = ttk.Checkbutton(control_frame, text="Modo Oscuro", variable=dark_mode_var, command=toggle_dark_mode)
dark_mode_check.pack(pady=10)
dark_mode_var.trace("w", toggle_dark_mode)

# Frame para gráficos
plot_frame = ttk.Frame(root)
plot_frame.pack(side=tk.RIGHT, fill=tk.BOTH, expand=True)

# Funciones de procesamiento de datos
def processed_data():
    data = pd.read_csv(URL_CSV, quotechar='"', sep=';', encoding='utf-8')
    df = pd.DataFrame(data)
    df = df[df["CSAT Rated Group Name"] == rated_group_var.get()]
    df = df.drop(columns=["CSAT Rated Group Name"])
    df["Group Name History"] = df["Group Name History"].str.replace(",", ", ")
    df["Group Name History"] = df["Group Name History"].apply(lambda x: x.split(", "))
    return df

def adjacency_and_interactions():
    df = processed_data()
    adjacency_matrix = defaultdict(lambda: defaultdict(int))
    interactions = defaultdict(int)
    for _, row in df.iterrows():
        groups = row["Group Name History"]
        for i in range(len(groups) - 1):
            source = groups[i]
            target = groups[i + 1]
            adjacency_matrix[source][target] += 1
            pair = f"{source} -> {target}"
            interactions[pair] += 1
    return adjacency_matrix, interactions

# Funciones para generar gráficos
def network_plot():
    adjacency_matrix, _ = adjacency_and_interactions()
    G = nx.DiGraph()
    
    for source, targets in adjacency_matrix.items():
        for target, weight in targets.items():
            G.add_edge(source, target, weight=weight)
            
    node_occurrences = defaultdict(int)
    for groups in processed_data()["Group Name History"]:
        for group in groups:
            node_occurrences[group] += 1
            
    min_size = min_size_var.get()
    max_size = max_size_var.get()
    occurrences = np.array(list(node_occurrences.values()))
    normalized_sizes = (occurrences - occurrences.min()) / (occurrences.max() - occurrences.min())
    node_sizes = {k: min_size + (max_size - min_size) * v for k, v in zip(node_occurrences.keys(), normalized_sizes)}
    node_colors = [normalized_sizes[list(node_occurrences.keys()).index(node)] for node in G.nodes()]
    
    bg_color = "#1E1E1E" if dark_mode_var.get() else "#C7C7C7"
    text_color = "white" if dark_mode_var.get() else "black"
    
    fig, ax = plt.subplots(figsize=(5, 4))
    fig.patch.set_facecolor(bg_color)
    ax.set_facecolor(bg_color)
    
    pos = nx.spring_layout(G, seed=seed_var.get(), k=k_value_var.get())
    nx.draw_networkx_nodes(G, pos, node_size=[node_sizes[node] for node in G.nodes()],
                          node_color=node_colors, cmap=plt.cm.viridis, alpha=0.5, linewidths=0, ax=ax)
    nx.draw_networkx_edges(G, pos, width=[d['weight'] for u, v, d in G.edges(data=True)],
                          edge_color='gray', alpha=0.5, arrowstyle='<-', arrowsize=10, ax=ax)
    nx.draw_networkx_labels(G, pos, font_size=8, font_color=text_color, ax=ax)
    
    sm = plt.cm.ScalarMappable(cmap=plt.cm.viridis, norm=plt.Normalize(vmin=occurrences.min(), vmax=occurrences.max()))
    sm.set_array([])
    cbar = plt.colorbar(sm, orientation="horizontal", label="Ocurrencias por grupo", ax=ax, shrink=0.4, aspect=25, pad=0.01)
    cbar.ax.tick_params(labelsize=8, labelcolor=text_color)
    cbar.set_label("Ocurrencias por grupo", color=text_color)
    
    ax.set_frame_on(False)
    plt.title("Análisis de Redes", fontsize=12, fontweight='bold', pad=10, color=text_color)
    plt.tight_layout(pad=0.5)
    return fig

def top_interactions():
    _, interactions = adjacency_and_interactions()
    top_10 = sorted(interactions.items(), key=lambda x: x[1], reverse=True)[:10]
    pairs, counts = zip(*top_10)
    
    bg_color = "#1E1E1E" if dark_mode_var.get() else "#C7C7C7"
    text_color = "white" if dark_mode_var.get() else "black"
    
    fig, ax = plt.subplots(figsize=(5, 2))
    fig.patch.set_facecolor(bg_color)
    ax.set_facecolor(bg_color)
    
    y_pos = np.arange(len(pairs))[::-1]
    bars = ax.barh(y_pos, counts, color=plt.cm.viridis(np.linspace(0, 1, len(counts))))
    
    for bar in bars:
        width = bar.get_width()
        ax.text(x=width + 0.5, y=bar.get_y() + bar.get_height()/2, s=f'{int(width)}',
                ha='left', va='center', fontsize=8, color=text_color)
    
    ax.set_yticks(y_pos)
    ax.set_yticklabels(pairs, fontsize=7, color=text_color)
    ax.xaxis.set_visible(False)
    ax.set_title("Top 10 Interacciones entre Grupos", fontsize=10, fontweight='bold', color=text_color)
    ax.set_frame_on(False)
    plt.tight_layout()
    return fig

# Función para actualizar los gráficos
def update_plots():
    for widget in plot_frame.winfo_children():
        widget.destroy()
    
    fig1 = network_plot()
    canvas1 = FigureCanvasTkAgg(fig1, master=plot_frame)
    canvas1.draw()
    canvas1.get_tk_widget().pack(side=tk.TOP, fill=tk.BOTH, expand=True)
    
    fig2 = top_interactions()
    canvas2 = FigureCanvasTkAgg(fig2, master=plot_frame)
    canvas2.draw()
    canvas2.get_tk_widget().pack(side=tk.TOP, fill=tk.BOTH, expand=True)

# Botón para actualizar
update_button = tk.Button(control_frame, text="Actualizar Gráficos", command=update_plots)
update_button.pack(pady=10)

# Función para inicializar la interfaz y cerrar la ventana de carga
def initialize_app():
    toggle_dark_mode()
    update_plots()
    loading_window.destroy()  # Cierra la ventana de carga
    root.deiconify()  # Muestra la ventana principal

# Ocultar la ventana principal temporalmente y programar la inicialización
root.withdraw()
root.after(2000, initialize_app)  # 2000 ms = 2 segundos de retraso (ajustable)

# Iniciar la aplicación
root.mainloop()