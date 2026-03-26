import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from datetime import datetime
import matplotlib.dates as mdates
from matplotlib.gridspec import GridSpec
import warnings
warnings.filterwarnings('ignore')

# Настройка стиля
plt.style.use('seaborn-v0_8-darkgrid')
plt.rcParams['font.family'] = 'sans-serif'
plt.rcParams['font.sans-serif'] = ['Arial']

print("Загрузка данных...")

# Загрузка данных с правильным разделителем (точка с запятой)
# Важно: sep=';' - это разделитель столбцов
df = pd.read_csv("C:/Users/ki200/Downloads/ecommerce_dataset_150k_fixed.csv", 
                 sep=';', 
                 encoding='utf-8')

print(f"Загружено {len(df):,} записей")
print(f"Столбцы: {list(df.columns)}")

# Проверяем, что столбцы правильно разделились
# Если всё ещё один столбец - значит файл с другим разделителем
if len(df.columns) == 1:
    print("Обнаружен один столбец, пробуем другой разделитель...")
    # Пробуем другой разделитель
    df = pd.read_csv("C:/Users/ki200/Downloads/ecommerce_dataset_150k_fixed.csv", 
                     sep=',', 
                     encoding='utf-8')
    print(f"Столбцы после повторной загрузки: {list(df.columns)}")

# Теперь должны быть правильные столбцы
# Преобразование числовых столбцов
# purchase_amount содержит запятые вместо точек
df['purchase_amount'] = df['purchase_amount'].astype(str).str.replace(',', '.').astype(float)
df['date'] = pd.to_datetime(df['date'], format='%d.%m.%Y')
df['revenue'] = df['purchase_amount']

# Добавим день недели для агрегации
df['weekday_num'] = df['date'].dt.dayofweek
df['weekday_name'] = df['date'].dt.day_name()
df['month_num'] = df['date'].dt.month
df['day_of_month'] = df['date'].dt.day

print("Данные успешно загружены и обработаны!")

# =============================================
# ДАННЫЕ ЭКСПЕРИМЕНТОВ
# =============================================
experiments_full = ['1 DataNode\nBaseline', '1 DataNode\nOptimized', 
                    '3 DataNodes\nBaseline', '3 DataNodes\nOptimized']
time_seconds = [9.30, 9.98, 13.38, 12.63]
memory_mb = [47, 47, 46, 42]
speedup = [1.00, 0.93, 0.69, 0.74]

# =============================================
# АГРЕГАЦИЯ ДАННЫХ ДЛЯ ГРАФИКОВ
# =============================================

# Агрегация по дням
daily_agg = df.groupby('date').agg({
    'transaction_id': 'count',
    'revenue': 'sum',
    'session_duration_sec': 'mean',
    'pages_visited': 'mean'
}).reset_index()
daily_agg.columns = ['date', 'transactions', 'revenue', 'avg_session_duration', 'avg_pages_visited']

# Агрегация по дням недели
weekday_agg = df.groupby('weekday_num').agg({
    'transaction_id': 'count',
    'revenue': 'sum',
    'session_duration_sec': 'mean'
}).reset_index()
weekday_names = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
weekday_agg['weekday'] = [weekday_names[i] for i in weekday_agg['weekday_num']]

# Агрегация по часам
hourly_agg = df.groupby('hour').agg({
    'transaction_id': 'count',
    'revenue': 'sum',
    'session_duration_sec': 'mean'
}).reset_index()

# Агрегация по месяцам
monthly_agg = df.groupby('month_num').agg({
    'transaction_id': 'count',
    'revenue': 'sum'
}).reset_index()
month_names = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 
               'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']

# Регионы с выручкой
region_revenue = df.groupby('region')['revenue'].sum().sort_values(ascending=True)
region_transactions = df.groupby('region')['transaction_id'].count().sort_values(ascending=True)

# Типы устройств с выручкой
device_revenue = df.groupby('device_type')['revenue'].sum()
device_data = df.groupby('device_type')['transaction_id'].count()
customer_data = df.groupby('customer_type')['transaction_id'].count()
channel_data = df.groupby('channel')['transaction_id'].count()
channel_revenue = df.groupby('channel')['revenue'].sum()

print("Агрегация данных завершена!")

# =============================================
# СОЗДАНИЕ ФИГУРЫ С ВКЛАДКАМИ
# =============================================

# Создаем фигуру
fig = plt.figure(figsize=(20, 16))
fig.suptitle('Ecommerce Analytics Dashboard - 150,000 Transactions', 
             fontsize=18, fontweight='bold', y=0.98)

# Используем GridSpec для размещения
gs = GridSpec(3, 3, figure=fig, hspace=0.35, wspace=0.3)

# =============================================
# РЯД 1: Эксперименты
# =============================================

# 1. Время выполнения
ax1 = fig.add_subplot(gs[0, 0])
colors_perf = ['#3498db', '#2ecc71', '#e74c3c', '#f39c12']
bars1 = ax1.bar(experiments_full, time_seconds, color=colors_perf, edgecolor='black', linewidth=1.5)
ax1.set_title('⏱️ Execution Time', fontsize=12, fontweight='bold')
ax1.set_ylabel('Time (seconds)', fontsize=10)
ax1.tick_params(axis='x', labelsize=8)
for bar, val in zip(bars1, time_seconds):
    ax1.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.2,
             f'{val:.2f}s', ha='center', va='bottom', fontsize=8)

# 2. Использование памяти
ax2 = fig.add_subplot(gs[0, 1])
bars2 = ax2.bar(experiments_full, memory_mb, color=colors_perf, edgecolor='black', linewidth=1.5)
ax2.set_title('💾 Memory Usage', fontsize=12, fontweight='bold')
ax2.set_ylabel('Memory (MB)', fontsize=10)
ax2.tick_params(axis='x', labelsize=8)
for bar, val in zip(bars2, memory_mb):
    ax2.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.5,
             f'{val:.0f} MB', ha='center', va='bottom', fontsize=8)

# 3. Ускорение
ax3 = fig.add_subplot(gs[0, 2])
bars3 = ax3.bar(experiments_full, speedup, color=colors_perf, edgecolor='black', linewidth=1.5)
ax3.set_title('🚀 Speedup Factor', fontsize=12, fontweight='bold')
ax3.set_ylabel('Speedup (x times)', fontsize=10)
ax3.axhline(y=1.0, color='red', linestyle='--', linewidth=2, alpha=0.7)
ax3.tick_params(axis='x', labelsize=8)
for bar, val in zip(bars3, speedup):
    ax3.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.02,
             f'{val:.2f}x', ha='center', va='bottom', fontsize=8)

# =============================================
# РЯД 2: Динамические графики
# =============================================

# 4. Динамика транзакций и выручки по дням (линейный график)
ax4 = fig.add_subplot(gs[1, 0])
ax4_twin = ax4.twinx()

line1 = ax4.plot(daily_agg['date'], daily_agg['transactions'], 
                  color='#3498db', linewidth=2, marker='o', markersize=3, label='Transactions')
ax4.set_xlabel('Date', fontsize=10)
ax4.set_ylabel('Number of Transactions', fontsize=10, color='#3498db')
ax4.tick_params(axis='y', labelcolor='#3498db')
ax4.tick_params(axis='x', rotation=45)

line2 = ax4_twin.plot(daily_agg['date'], daily_agg['revenue'], 
                       color='#e74c3c', linewidth=2, marker='s', markersize=3, label='Revenue')
ax4_twin.set_ylabel('Revenue (currency)', fontsize=10, color='#e74c3c')
ax4_twin.tick_params(axis='y', labelcolor='#e74c3c')

lines = line1 + line2
labels = [l.get_label() for l in lines]
ax4.legend(lines, labels, loc='upper left', fontsize=8)
ax4.set_title('📈 Daily Transactions & Revenue Trend', fontsize=12, fontweight='bold')
ax4.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))
ax4.xaxis.set_major_locator(mdates.DayLocator(interval=10))

# 5. Динамика по дням недели (средняя длительность сессии)
ax5 = fig.add_subplot(gs[1, 1])
colors_weekday = ['#3498db', '#2ecc71', '#f39c12', '#e74c3c', '#9b59b6', '#1abc9c', '#e67e22']
bars5 = ax5.bar(weekday_agg['weekday'], weekday_agg['session_duration_sec'], 
                color=colors_weekday, edgecolor='black', linewidth=1)
ax5.set_title('⏰ Average Session Duration by Weekday', fontsize=12, fontweight='bold')
ax5.set_ylabel('Session Duration (seconds)', fontsize=10)
ax5.tick_params(axis='x', rotation=45, labelsize=8)
for bar, val in zip(bars5, weekday_agg['session_duration_sec']):
    ax5.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 5,
             f'{val:.0f}s', ha='center', va='bottom', fontsize=8)

# 6. Динамика по часам (транзакции и выручка)
ax6 = fig.add_subplot(gs[1, 2])
ax6_twin = ax6.twinx()

line3 = ax6.plot(hourly_agg['hour'], hourly_agg['transaction_id'], 
                  color='#3498db', linewidth=2, marker='o', markersize=4, label='Transactions')
ax6.set_xlabel('Hour of Day', fontsize=10)
ax6.set_ylabel('Number of Transactions', fontsize=10, color='#3498db')
ax6.tick_params(axis='y', labelcolor='#3498db')

line4 = ax6_twin.plot(hourly_agg['hour'], hourly_agg['revenue'], 
                       color='#e74c3c', linewidth=2, marker='s', markersize=4, label='Revenue')
ax6_twin.set_ylabel('Revenue (currency)', fontsize=10, color='#e74c3c')
ax6_twin.tick_params(axis='y', labelcolor='#e74c3c')

lines2 = line3 + line4
labels2 = [l.get_label() for l in lines2]
ax6.legend(lines2, labels2, loc='upper left', fontsize=8)
ax6.set_title('🕐 Hourly Transaction & Revenue Pattern', fontsize=12, fontweight='bold')

# =============================================
# РЯД 3: Аналитические графики
# =============================================

# 7. Выручка по регионам (горизонтальная диаграмма)
ax7 = fig.add_subplot(gs[2, 0])
region_revenue_sorted = region_revenue.sort_values()
y_pos = np.arange(len(region_revenue_sorted))
colors_region_rev = plt.cm.RdYlGn(np.linspace(0.2, 0.8, len(region_revenue_sorted)))
bars7 = ax7.barh(y_pos, region_revenue_sorted.values, color=colors_region_rev, edgecolor='black', linewidth=0.5)
ax7.set_yticks(y_pos)
ax7.set_yticklabels(region_revenue_sorted.index, fontsize=7)
ax7.set_xlabel('Total Revenue', fontsize=10)
ax7.set_title('💰 Revenue by Region (All 15 regions)', fontsize=12, fontweight='bold')
ax7.invert_yaxis()
for bar, val in zip(bars7, region_revenue_sorted.values):
    ax7.text(bar.get_width() + 1000, bar.get_y() + bar.get_height()/2,
             f'{val:,.0f}', ha='left', va='center', fontsize=7)

# 8. Количество транзакций по регионам
ax8 = fig.add_subplot(gs[2, 1])
region_trans_sorted = region_transactions.sort_values()
bars8 = ax8.barh(y_pos, region_trans_sorted.values, color=colors_region_rev, edgecolor='black', linewidth=0.5)
ax8.set_yticks(y_pos)
ax8.set_yticklabels(region_trans_sorted.index, fontsize=7)
ax8.set_xlabel('Number of Transactions', fontsize=10)
ax8.set_title('👥 Transactions by Region', fontsize=12, fontweight='bold')
ax8.invert_yaxis()
for bar, val in zip(bars8, region_trans_sorted.values):
    ax8.text(bar.get_width() + 200, bar.get_y() + bar.get_height()/2,
             f'{val:,}', ha='left', va='center', fontsize=7)

# 9. Комбинированный график: выручка по типам устройств и каналам
ax9 = fig.add_subplot(gs[2, 2])

x = np.arange(3)
width = 0.25
colors_dev = ['#3498db', '#2ecc71', '#f39c12']

device_bars = ax9.bar(x - width, device_revenue.values, width, 
                       label='Device Type', color=colors_dev, edgecolor='black')

x_ch = np.arange(len(channel_revenue))
colors_ch = plt.cm.Set3(np.linspace(0, 1, len(channel_revenue)))
channel_bars = ax9.bar(x_ch + width*2, channel_revenue.values, width*0.8,
                        label='Channel', color=colors_ch, edgecolor='black', alpha=0.7)

ax9.set_xticks(np.concatenate([x, x_ch + width*2]))
ax9.set_xticklabels(list(device_revenue.index) + list(channel_revenue.index), 
                    rotation=45, ha='right', fontsize=7)
ax9.set_ylabel('Revenue (currency)', fontsize=10)
ax9.set_title('💵 Revenue by Device Type & Channel', fontsize=12, fontweight='bold')
ax9.legend(loc='upper right', fontsize=8)

# =============================================
# ДОПОЛНИТЕЛЬНЫЙ ГРАФИК: Pages Visited vs Session Duration
# =============================================

# Создаем отдельную фигуру для дополнительного анализа
fig2, axes2 = plt.subplots(1, 2, figsize=(14, 5))
fig2.suptitle('User Behavior Analysis', fontsize=14, fontweight='bold')

# Scatter plot: Session Duration vs Pages Visited
ax_scatter = axes2[0]
sample_df = df.sample(n=5000, random_state=42)  # Выборка для читаемости
scatter = ax_scatter.scatter(sample_df['session_duration_sec'], 
                              sample_df['pages_visited'],
                              c=sample_df['revenue'], cmap='RdYlGn', 
                              alpha=0.5, s=20)
ax_scatter.set_xlabel('Session Duration (seconds)', fontsize=10)
ax_scatter.set_ylabel('Pages Visited', fontsize=10)
ax_scatter.set_title('📊 Session Duration vs Pages Visited (colored by Revenue)', fontsize=12)
plt.colorbar(scatter, ax=ax_scatter, label='Revenue')

# Distribution: Pages Visited
ax_hist = axes2[1]
ax_hist.hist(df['pages_visited'], bins=20, color='#3498db', edgecolor='black', alpha=0.7)
ax_hist.set_xlabel('Pages Visited', fontsize=10)
ax_hist.set_ylabel('Number of Transactions', fontsize=10)
ax_hist.set_title('📚 Pages Visited Distribution', fontsize=12)
ax_hist.axvline(df['pages_visited'].mean(), color='red', linestyle='--', 
                label=f"Mean: {df['pages_visited'].mean():.1f}")
ax_hist.legend()

plt.tight_layout()
plt.savefig('user_behavior_analysis.png', dpi=300, bbox_inches='tight')
print("✅ user_behavior_analysis.png сохранен")

# =============================================
# СОХРАНЕНИЕ ОСНОВНОГО ДАШБОРДА
# =============================================

plt.figure(fig)
plt.tight_layout()
plt.savefig('ecommerce_full_dashboard.png', dpi=300, bbox_inches='tight')
plt.savefig('ecommerce_full_dashboard.pdf', bbox_inches='tight')
print("✅ ecommerce_full_dashboard.png сохранен")
print("✅ ecommerce_full_dashboard.pdf сохранен")

print("\n" + "=" * 60)
print("✅ ВСЕ ГРАФИКИ УСПЕШНО СОЗДАНЫ!")
print("=" * 60)
print("Сохраненные файлы:")
print("  1. ecommerce_full_dashboard.png - Основная панель с 9 графиками")
print("  2. ecommerce_full_dashboard.pdf - Векторная версия")
print("  3. user_behavior_analysis.png - Анализ поведения пользователей")
print("=" * 60)

plt.show()