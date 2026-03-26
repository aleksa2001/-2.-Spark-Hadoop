import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

# =============================================
# ЗАГРУЗКА РЕАЛЬНЫХ ДАННЫХ ИЗ CSV
# =============================================

# Загружаем данные из экспериментов (уже есть в CSV)
regions_df = pd.read_csv("regions_data.csv")
devices_df = pd.read_csv("devices_data.csv")
customers_df = pd.read_csv("customers_data.csv")
channels_df = pd.read_csv("channels_data.csv")

# Результаты экспериментов (из проведенных тестов)
experiments_full = ['1 DataNode\nBaseline', '1 DataNode\nOptimized', '3 DataNodes\nBaseline', '3 DataNodes\nOptimized']
time_seconds = [9.30, 9.98, 13.38, 12.63]
memory_mb = [47, 47, 46, 42]
speedup = [1.00, 0.93, 0.69, 0.74]

# Цветовые схемы
colors_perf = ['#3498db', '#2ecc71', '#e74c3c', '#f39c12']
colors_region = plt.cm.Set3(np.linspace(0, 1, len(regions_df)))
colors_device = ['#3498db', '#2ecc71', '#f39c12']
colors_customer = ['#27ae60', '#e67e22', '#e74c3c']
colors_channel = plt.cm.Paired(np.linspace(0, 1, len(channels_df)))

# =============================================
# СОЗДАНИЕ ГРАФИКОВ
# =============================================

fig = plt.figure(figsize=(18, 14))
fig.suptitle('Spark Performance Analysis - Ecommerce Dataset (150,000 records)', 
             fontsize=18, fontweight='bold', y=0.98)

# 1. Время выполнения
ax1 = plt.subplot(3, 3, 1)
bars1 = ax1.bar(experiments_full, time_seconds, color=colors_perf, edgecolor='black', linewidth=1.5)
ax1.set_title('Execution Time', fontsize=12, fontweight='bold')
ax1.set_ylabel('Time (seconds)', fontsize=10)
ax1.set_ylim(0, max(time_seconds) * 1.2)
ax1.tick_params(axis='x', labelsize=8)
for bar, val in zip(bars1, time_seconds):
    ax1.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.2, f'{val:.2f}s', ha='center', va='bottom', fontsize=9)

# 2. Использование памяти
ax2 = plt.subplot(3, 3, 2)
bars2 = ax2.bar(experiments_full, memory_mb, color=colors_perf, edgecolor='black', linewidth=1.5)
ax2.set_title('Memory Usage', fontsize=12, fontweight='bold')
ax2.set_ylabel('Memory (MB)', fontsize=10)
ax2.set_ylim(0, max(memory_mb) * 1.2)
ax2.tick_params(axis='x', labelsize=8)
for bar, val in zip(bars2, memory_mb):
    ax2.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 1, f'{val:.0f} MB', ha='center', va='bottom', fontsize=9)

# 3. Ускорение
ax3 = plt.subplot(3, 3, 3)
bars3 = ax3.bar(experiments_full, speedup, color=colors_perf, edgecolor='black', linewidth=1.5)
ax3.set_title('Speedup Factor (vs Exp1)', fontsize=12, fontweight='bold')
ax3.set_ylabel('Speedup (x times)', fontsize=10)
ax3.axhline(y=1.0, color='red', linestyle='--', linewidth=2, alpha=0.7, label='Baseline')
ax3.set_ylim(0, max(speedup) * 1.2)
ax3.tick_params(axis='x', labelsize=8)
ax3.legend(loc='upper right', fontsize=8)
for bar, val in zip(bars3, speedup):
    ax3.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.02, f'{val:.2f}x', ha='center', va='bottom', fontsize=9)

# 4. Регионы (ВСЕ 15, горизонтальная диаграмма)
ax4 = plt.subplot(3, 3, 4)
regions_sorted = regions_df.sort_values('count', ascending=True)
y_pos = np.arange(len(regions_sorted))
bars4 = ax4.barh(y_pos, regions_sorted['count'], color=colors_region, edgecolor='black', linewidth=0.5)
ax4.set_yticks(y_pos)
ax4.set_yticklabels(regions_sorted['region'], fontsize=7)
ax4.set_xlabel('Number of Transactions', fontsize=10)
ax4.set_title(f'Transactions by Region (All {len(regions_sorted)} regions)', fontsize=12, fontweight='bold')
for bar, val in zip(bars4, regions_sorted['count']):
    ax4.text(bar.get_width() + 200, bar.get_y() + bar.get_height()/2, f'{val:,}', ha='left', va='center', fontsize=6)

# 5. Типы устройств (круговая)
ax5 = plt.subplot(3, 3, 5)
device_names = devices_df['device_type'].tolist()
device_counts = devices_df['count'].tolist()
device_pct = [f'{c/150000*100:.1f}%' for c in device_counts]
wedges, texts, autotexts = ax5.pie(device_counts, labels=device_names, autopct='%1.1f%%',
                                     colors=colors_device[:len(device_names)], explode=(0.05, 0.05, 0.05), startangle=90)
ax5.set_title('Transactions by Device Type', fontsize=12, fontweight='bold')
for autotext in autotexts:
    autotext.set_color('white')
    autotext.set_fontweight('bold')

# 6. Типы клиентов (круговая)
ax6 = plt.subplot(3, 3, 6)
customer_names = customers_df['customer_type'].tolist()
customer_counts = customers_df['count'].tolist()
wedges, texts, autotexts = ax6.pie(customer_counts, labels=customer_names, autopct='%1.1f%%',
                                     colors=colors_customer, explode=(0.05, 0, 0), startangle=90)
ax6.set_title('Transactions by Customer Type', fontsize=12, fontweight='bold')
for autotext in autotexts:
    autotext.set_color('white')
    autotext.set_fontweight('bold')

# 7. Каналы привлечения
ax7 = plt.subplot(3, 3, 7)
channel_names = channels_df['channel'].tolist()
channel_counts = channels_df['count'].tolist()
bars7 = ax7.bar(channel_names, channel_counts, color=colors_channel, edgecolor='black', linewidth=1)
ax7.set_title('Transactions by Channel', fontsize=12, fontweight='bold')
ax7.set_ylabel('Number of Transactions', fontsize=10)
ax7.tick_params(axis='x', rotation=45, labelsize=8)
for bar, val in zip(bars7, channel_counts):
    ax7.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 300, f'{val:,}', ha='center', va='bottom', fontsize=8)

# 8. Сравнение оптимизаций (группированная диаграмма)
ax8 = plt.subplot(3, 3, 8)
x = np.arange(2)
width = 0.35
baseline_times = [time_seconds[0], time_seconds[2]]
optimized_times = [time_seconds[1], time_seconds[3]]
bars8_1 = ax8.bar(x - width/2, baseline_times, width, label='Without Optimization', color='#e74c3c', edgecolor='black')
bars8_2 = ax8.bar(x + width/2, optimized_times, width, label='With Optimization', color='#2ecc71', edgecolor='black')
ax8.set_title('Optimization Impact by Configuration', fontsize=12, fontweight='bold')
ax8.set_ylabel('Time (seconds)', fontsize=10)
ax8.set_xticks(x)
ax8.set_xticklabels(['1 DataNode', '3 DataNodes'])
ax8.legend(loc='upper left', fontsize=8)
for bars in [bars8_1, bars8_2]:
    for bar in bars:
        ax8.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.15, f'{bar.get_height():.2f}s', ha='center', va='bottom', fontsize=8)

# 9. Сводная таблица
ax9 = plt.subplot(3, 3, 9)
ax9.axis('tight')
ax9.axis('off')
table_data = [
    ['Experiment', 'Configuration', 'Time (s)', 'Memory (MB)', 'Speedup'],
    ['Exp1', '1 DN, Baseline', '9.30', '47', '1.00x'],
    ['Exp2', '1 DN, Optimized', '9.98', '47', '0.93x'],
    ['Exp3', '3 DN, Baseline', '13.38', '46', '0.69x'],
    ['Exp4', '3 DN, Optimized', '12.63', '42', '0.74x'],
]
table = ax9.table(cellText=table_data, loc='center', cellLoc='center', colWidths=[0.15, 0.3, 0.12, 0.12, 0.12])
table.auto_set_font_size(False)
table.set_fontsize(9)
table.scale(1.2, 1.5)
for i in range(len(table_data)):
    if i == 0:
        for j in range(len(table_data[0])):
            table[(i, j)].set_facecolor('#34495e')
            table[(i, j)].set_text_props(weight='bold', color='white')
    else:
        for j in range(len(table_data[0])):
            table[(i, j)].set_facecolor('#ecf0f1')
ax9.set_title('Summary of Results', fontsize=12, fontweight='bold', pad=20)

# Настройка отступов
plt.tight_layout()

# Сохранение графиков
plt.savefig('experiment_results_with_real_data.png', dpi=300, bbox_inches='tight', facecolor='white')
plt.savefig('experiment_results_with_real_data.pdf', bbox_inches='tight', facecolor='white')
print("\n[OK] Graphics saved:")
print("   - experiment_results_with_real_data.png")
print("   - experiment_results_with_real_data.pdf")

plt.show()