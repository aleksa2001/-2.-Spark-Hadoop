# Lab-2.-Spark-Hadoop
Данный репозиторий содержит результаты лабораторной работы по исследованию производительности Apache Spark в кластере Hadoop. В работе проведено 4 эксперимента с различными конфигурациями: 1 и 3 DataNode, а также с применением оптимизаций Spark.
#  Отчет по лабораторной работе №2: Spark/Hadoop
##  Цель работы

Исследовать влияние масштабирования кластера Hadoop (1 и 3 DataNode) и оптимизаций Spark (кэширование, репартицирование) на производительность обработки данных в распределенной среде.


##  Датасет

### 1.1. Характеристики датасета

| Параметр | Значение |
|----------|----------|
| **Количество строк** | **150,000** |
| **Количество признаков** | **13** | 
| **Типы данных** | 4 числовых, 9 категориальных |  
| **Категориальные признаки** | region, device_type, customer_type, channel, weekday и др. | 

### 1.2. Структура датасета

| Столбец | Тип | Описание |
|---------|-----|----------|
| transaction_id | **категориальный** | Уникальный ID транзакции |
| customer_id | **категориальный** | ID клиента |
| date | **категориальный** | Дата транзакции |
| weekday | **категориальный** | День недели (7 значений) |
| month | **числовой** | Месяц (1-12) |
| hour | **числовой** | Час (0-23) |
| channel | **категориальный** | Канал привлечения (6 значений) |
| device_type | **категориальный** | Тип устройства (3 значения) |
| customer_type | **категориальный** | Тип клиента (3 значения) |
| region | **категориальный** | Регион (15 значений) |
| session_duration_sec | **числовой** | Длительность сессии (сек) |
| pages_visited | **числовой** | Количество страниц |
| purchase_amount | **числовой** | Сумма покупки |



##  Развертывание Hadoop (1 DataNode)

### 2.1. Конфигурационные файлы

#### **core-site.xml** (настройка HDFS)
```xml
<configuration>
    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://localhost:9000</value>
    </property>
    <property>
        <name>hadoop.tmp.dir</name>
        <value>C:/hadoop/tmp</value>
    </property>
</configuration>
```
#### **hdfs-site.xml** (настройка репликации и хранения)
```xml
<configuration>
    <property>
        <name>dfs.replication</name>
        <value>1</value>
    </property>
    <property>
        <name>dfs.namenode.name.dir</name>
        <value>C:/hadoop/data/namenode</value>
    </property>
    <property>
        <name>dfs.datanode.data.dir</name>
```
#### **yarn-site.xml** (ограничение памяти)
```xml
<configuration>
    <property>
        <name>yarn.nodemanager.resource.memory-mb</name>
        <value>2048</value>  <!-- Ограничение памяти узла -->
    </property>
    <property>
        <name>yarn.scheduler.maximum-allocation-mb</name>
        <value>2048</value>
    </property>
    <property>
        <name>yarn.nodemanager.vmem-check-enabled</name>
        <value>false</value>
    </property>
</configuration>
        <value>C:/hadoop/data/datanode</value>
    </property>
</configuration>
```
### 2.2. Запуск Hadoop на Windows (ручной режим)
В Windows наблюдались проблемы с автоматическими скриптами, поэтому Hadoop запускался вручную в отдельных окнах командной строки.
####Окно 1: Форматирование и запуск NameNode
<img width="974" height="547" alt="image" src="https://github.com/user-attachments/assets/6259c6c5-d0cc-4927-ba10-14203bb0eedc" />
#### Окно 2: DataNode  
 <img width="974" height="552" alt="image" src="https://github.com/user-attachments/assets/a6058ec3-ffe9-4224-8cda-b387a8000e7e" />
 
#### Окно 3: Запуск ResourceManager
```bash
C:\hadoop>yarn resourcemanager
```
#### Окно 4: Запуск NodeManager
```bash
C:\hadoop>yarn nodemanager
2026-03-21 14:35:02,789 INFO nodemanager.NodeManager: NodeManager started
```
#### Проверка процессов
```bash
C:\hadoop>jps
15420 DataNode
25388 ResourceManager
27788 NodeManager
36860 NameNode
```
#### 2.3. Загрузка данных в HDFS
```bash
 Создание директории
hdfs dfs -mkdir -p /user/data/ecommerce

 Загрузка данных с размером блока 128 MB
hdfs dfs -D dfs.blocksize=134217728 -put "C:\Users\ki200\Downloads\ecommerce_dataset_150k_fixed.csv" /user/data/ecommerce/
```
## Spark Application (1 DataNode)

#### 3.1. Конфигурация Spark

Базовый режим (Эксперимент 1)

<img width="974" height="513" alt="image" src="https://github.com/user-attachments/assets/c42a67c4-7960-4f06-baef-4f9c0b77a0cc" />

<img width="974" height="674" alt="image" src="https://github.com/user-attachments/assets/8e375cc9-5d09-41e0-8acf-cf41b7cde3f4" />

<img width="656" height="720" alt="image" src="https://github.com/user-attachments/assets/7c540f7c-106b-48ff-8add-8c31fa291dd4" />

<img width="974" height="508" alt="image" src="https://github.com/user-attachments/assets/84270639-4b1e-401a-af97-ea603ea81447" />

Оптимизированный режим (Эксперимент 2)

<img width="974" height="493" alt="image" src="https://github.com/user-attachments/assets/74f415f2-123d-43bd-a7c3-ac547a1ce12c" />

<img width="548" height="874" alt="image" src="https://github.com/user-attachments/assets/8341eda9-87d5-4d12-bc59-6f11b3684499" />

<img width="530" height="742" alt="image" src="https://github.com/user-attachments/assets/dd142f56-17d1-48ba-9040-4e9023e10b3d" />

<img width="974" height="508" alt="image" src="https://github.com/user-attachments/assets/cf96041f-454e-4499-8692-fd9a8612d180" />




### 3.2. Анализ экспериментов 1-2
 Сводная таблица результатов 

| Параметр | Exp1 (1 DN, Baseline) | Exp2 (1 DN, Optimized) | Изменение |
|----------|----------------------|------------------------|-----------|
| **Время выполнения** | 9.30 сек | 9.98 сек | **+7.3%**  |
| **Память (пик)** | 47 MB | 47 MB | 0% |
| **Jobs** | 24 | 18 | -25%  |
| **Stages** | 48 | 36 | -25%  |
| **Партиции** | 2 | 8 | +300%  |
| **Кэширование** | Нет | Да | — |
| **Адаптивное выполнение** | Нет | Да | — |

#### Почему оптимизации не дали ускорения?

Несмотря на применение передовых техник оптимизации, время выполнения увеличилось на 7.3%. Основные причины:

| Фактор | Объяснение |
|--------|------------|
| **Малый объем данных** | 150k записей (~14 MB) легко помещаются в память, накладные расходы на оптимизации превышают выгоду |
| **Оверхед репартицирования** | Увеличение числа партиций с 2 до 8 создает дополнительную работу для Spark (сериализация, shuffle) |
| **Накладные расходы кэширования** | `cache()` требует времени на сериализацию и хранение данных, что не окупается при однократном использовании |
| **Отсутствие повторных действий** | В эксперименте данные используются только один раз, кэширование не дает выигрыша |

#### Положительные эффекты оптимизаций

Несмотря на увеличение времени выполнения, оптимизации дали некоторые преимущества:

| Эффект | Значение |
|--------|----------|
| **Уменьшение количества Jobs** | 24 → 18 (-25%) |
| **Уменьшение количества Stages** | 48 → 36 (-25%) |
| **Более равномерное распределение данных** | 8 партиций вместо 2 |

### 3.3. Логирование результатов
Получившиеся логи для эксперимента 1 (базовый режим)
```text
2026-03-21 14:51:39,109 - INFO - Step 1: Loading data from HDFS...
2026-03-21 14:51:39,109 - INFO -    Loaded 150,000 records
2026-03-21 14:51:39,109 - INFO -    Columns: 13
2026-03-21 14:51:39,109 - INFO -    Partitions: 2

2026-03-21 14:51:39,109 - INFO - Step 2: Data analysis...
2026-03-21 14:51:39,109 - INFO -    Numeric columns: ['month', 'hour', 'session_duration_sec', 'pages_visited']
2026-03-21 14:51:39,109 - INFO -    Categorical columns: ['transaction_id', 'customer_id', 'date', 'weekday', 'channel']

2026-03-21 14:51:39,109 - INFO - Step 3: Job information...
2026-03-21 14:51:39,109 - INFO -    Total jobs: 24

2026-03-21 14:51:39,109 - INFO - ======================================================================
2026-03-21 14:51:39,109 - INFO - EXPERIMENT COMPLETED: Experiment1_Baseline
2026-03-21 14:51:39,109 - INFO - Execution time: 9.30 seconds
2026-03-21 14:51:39,109 - INFO - Peak memory: 47 MB
2026-03-21 14:51:39,109 - INFO - Jobs executed: 24
2026-03-21 14:51:39,109 - INFO - ======================================================================
Пример лога эксперимента 2 (оптимизированный режим)
text
2026-03-21 15:13:26,001 - INFO - Step 1: Loading data from HDFS...
2026-03-21 15:13:26,001 - INFO -    Loaded 150,000 records
2026-03-21 15:13:26,001 - INFO -    Columns: 13
2026-03-21 15:13:26,001 - INFO -    Initial partitions: 2

2026-03-21 15:13:26,001 - INFO - Step 2: Applying optimizations...
2026-03-21 15:13:26,001 - INFO -    Repartitioning to 8 partitions...
2026-03-21 15:13:26,001 - INFO -    Caching data...
2026-03-21 15:13:26,001 - INFO -    Cache applied

2026-03-21 15:13:26,001 - INFO - Step 3: Data analysis...
2026-03-21 15:13:26,001 - INFO -    Numeric columns: ['month', 'hour', 'session_duration_sec', 'pages_visited']
2026-03-21 15:13:26,001 - INFO -    Categorical columns: ['transaction_id', 'customer_id', 'date', 'weekday', 'channel']

2026-03-21 15:13:26,001 - INFO - Step 4: Job information...
2026-03-21 15:13:26,001 - INFO -    Total jobs: 18

2026-03-21 15:13:26,001 - INFO - ======================================================================
2026-03-21 15:13:26,001 - INFO - EXPERIMENT COMPLETED: Experiment2_Optimized
2026-03-21 15:13:26,001 - INFO - Execution time: 9.98 seconds
2026-03-21 15:13:26,001 - INFO - Peak memory: 47 MB
2026-03-21 15:13:26,001 - INFO - Jobs executed: 18
2026-03-21 15:13:26,001 - INFO - ======================================================================
```
Логи для эксперимента 2 (оптимизированный режим)
```text
2026-03-21 15:13:26,001 - INFO - Step 1: Loading data from HDFS...
2026-03-21 15:13:26,001 - INFO -    Loaded 150,000 records
2026-03-21 15:13:26,001 - INFO -    Columns: 13
2026-03-21 15:13:26,001 - INFO -    Initial partitions: 2

2026-03-21 15:13:26,001 - INFO - Step 2: Applying optimizations...
2026-03-21 15:13:26,001 - INFO -    Repartitioning to 8 partitions...
2026-03-21 15:13:26,001 - INFO -    Caching data...
2026-03-21 15:13:26,001 - INFO -    Cache applied

2026-03-21 15:13:26,001 - INFO - Step 3: Data analysis...
2026-03-21 15:13:26,001 - INFO -    Numeric columns: ['month', 'hour', 'session_duration_sec', 'pages_visited']
2026-03-21 15:13:26,001 - INFO -    Categorical columns: ['transaction_id', 'customer_id', 'date', 'weekday', 'channel']

2026-03-21 15:13:26,001 - INFO - Step 4: Job information...
2026-03-21 15:13:26,001 - INFO -    Total jobs: 18

2026-03-21 15:13:26,001 - INFO - ======================================================================
2026-03-21 15:13:26,001 - INFO - EXPERIMENT COMPLETED: Experiment2_Optimized
2026-03-21 15:13:26,001 - INFO - Execution time: 9.98 seconds
2026-03-21 15:13:26,001 - INFO - Peak memory: 47 MB
2026-03-21 15:13:26,001 - INFO - Jobs executed: 18
2026-03-21 15:13:26,001 - INFO - ======================================================================
```

#### Вывод по экспериментам 1-2

Для датасета объемом 150k записей (~14 MB) **базовый режим без оптимизаций на 1 DataNode является оптимальным**. Применение оптимизаций Spark (repartition, cache, adaptive execution) на таком малом объеме данных создает больше накладных расходов, чем дает выгоды.


## 4. Развертывание Hadoop (3 DataNode)

### 4.1. Конфигурация для 3 DataNode

#### Файл workers
```txt
localhost
localhost2
localhost3
```

#### hdfs-site.xml (3 DataNode)
```xml
<configuration>
    <property>
        <name>dfs.replication</name>
        <value>3</value>
    </property>
    <property>
        <name>dfs.namenode.name.dir</name>
        <value>file:///C:/hadoop/data/namenode</value>
    </property>
    <property>
        <name>dfs.datanode.data.dir</name>
        <value>file:///C:/hadoop/data/datanode</value>
    </property>
    <property>
        <name>dfs.datanode.data.dir.2</name>
        <value>file:///C:/hadoop/data/datanode2</value>
    </property>
    <property>
        <name>dfs.datanode.data.dir.3</name>
        <value>file:///C:/hadoop/data/datanode3</value>
    </property>
</configuration>
```

### 4.2. Запуск 3 DataNode (ручной режим)

#### Окно 1: NameNode 

<img width="974" height="341" alt="image" src="https://github.com/user-attachments/assets/5af7a7cd-506f-473f-8d67-104473b27342" />


#### Окно 2: DataNode 1 (стандартные порты)

<img width="974" height="268" alt="image" src="https://github.com/user-attachments/assets/1f20be07-f332-469e-bd97-883559ca2b2a" />

#### Окно 3: DataNode 2 (порты +2)

<img width="974" height="205" alt="image" src="https://github.com/user-attachments/assets/59309c69-2409-4c71-9f2d-8aeb27648b47" />

#### Окно 4: DataNode 3 (порты +200)

<img width="974" height="199" alt="image" src="https://github.com/user-attachments/assets/ad4003f8-19c3-4352-a0eb-f80f9bd40109" />

#### Окно 5: ResourceManager

<img width="974" height="269" alt="image" src="https://github.com/user-attachments/assets/03d010ea-92c4-4514-a951-e25a6095419c" />

#### Окно 6: NodeManager
```bash
C:\hadoop>yarn nodemanager
```
#### Проверка 3 DataNode

<img width="974" height="635" alt="image" src="https://github.com/user-attachments/assets/e349c551-4a07-4263-b059-04d2e245f4e9" />

## 5. Spark Application (3 DataNode)

### 5.1. Конфигурация Spark для 3 DataNode

#### Базовый режим (Эксперимент 3)

<img width="974" height="351" alt="image" src="https://github.com/user-attachments/assets/5938338b-a79b-49f6-a090-f78c5ae3d70d" />

<img width="672" height="858" alt="image" src="https://github.com/user-attachments/assets/23c111cd-15aa-437a-8743-b02ad3460e2f" />

<img width="527" height="802" alt="image" src="https://github.com/user-attachments/assets/e02d4238-17b0-4134-a57c-bfdee8e473c3" />

<img width="974" height="516" alt="image" src="https://github.com/user-attachments/assets/b7ddd8a6-67f7-42d4-8a88-54ed4083d035" />

#### Оптимизированный режим (Эксперимент 4)

<img width="974" height="223" alt="image" src="https://github.com/user-attachments/assets/7abc962b-e088-4e6d-a828-d445276dfc76" />

<img width="449" height="496" alt="image" src="https://github.com/user-attachments/assets/0ce0bed8-a73a-41e1-af3a-f6f6ba95acc2" />

<img width="494" height="486" alt="image" src="https://github.com/user-attachments/assets/cad63abc-dc9b-40ee-a98e-0c67f5c7b9de" />

<img width="762" height="400" alt="image" src="https://github.com/user-attachments/assets/6585bc02-1a5b-4e33-b089-92a6797c9355" />

<img width="974" height="301" alt="image" src="https://github.com/user-attachments/assets/312109e8-1ba4-4a41-9ec2-a3f47bd17c01" />


### 5.2. Результаты экспериментов 3-4

| Показатель | Эксперимент 3 (Базовый) | Эксперимент 4 (Оптимизированный) | Изменение |
|------------|-------------------------|----------------------------------|-----------|
| **Время выполнения** | 13.38 сек | 12.63 сек | **-5.6%**  |
| **Память (пик)** | 46 MB | 42 MB | -8.7%  |
| **Jobs** | 22 | 16 | -27%  |
| **Stages** | 44 | 32 | -27%  |
| **Партиции** | 4 | 16 | +300%  |
| **Кэширование** | Нет | Да | — |
| **Адаптивное выполнение** | Нет | Да | — |

### 5.3. Логирование результатов
Пример лога эксперимента 3 (3 DataNode, базовый режим)
```text
2026-03-25 11:27:47,123 - INFO - Step 1: Loading data from HDFS...
2026-03-25 11:27:47,123 - INFO -    Loaded 150,000 records
2026-03-25 11:27:47,123 - INFO -    Columns: 13
2026-03-25 11:27:47,123 - INFO -    Partitions: 4

2026-03-25 11:27:47,123 - INFO - Step 2: Data analysis...
2026-03-25 11:27:47,123 - INFO -    Numeric columns: ['month', 'hour', 'session_duration_sec', 'pages_visited']
2026-03-25 11:27:47,123 - INFO -    Categorical columns: ['transaction_id', 'customer_id', 'date', 'weekday', 'channel']

2026-03-25 11:27:47,123 - INFO - Step 3: Job information...
2026-03-25 11:27:47,123 - INFO -    Total jobs: 22

2026-03-25 11:27:47,123 - INFO - ======================================================================
2026-03-25 11:27:47,123 - INFO - EXPERIMENT COMPLETED: Experiment3_3DN_Baseline
2026-03-25 11:27:47,123 - INFO - Execution time: 13.38 seconds
2026-03-25 11:27:47,123 - INFO - Peak memory: 46 MB
2026-03-25 11:27:47,123 - INFO - Jobs executed: 22
2026-03-25 11:27:47,123 - INFO - ======================================================================
Пример лога эксперимента 4 (3 DataNode, оптимизированный режим)
text
2026-03-25 11:55:56,456 - INFO - Step 1: Loading data from HDFS...
2026-03-25 11:55:56,456 - INFO -    Loaded 150,000 records
2026-03-25 11:55:56,456 - INFO -    Columns: 13
2026-03-25 11:55:56,456 - INFO -    Initial partitions: 4

2026-03-25 11:55:56,456 - INFO - Step 2: Applying optimizations...
2026-03-25 11:55:56,456 - INFO -    Repartitioning to 16 partitions...
2026-03-25 11:55:56,456 - INFO -    Caching data...
2026-03-25 11:55:56,456 - INFO -    Cache applied

2026-03-25 11:55:56,456 - INFO - Step 3: Data analysis...
2026-03-25 11:55:56,456 - INFO -    Numeric columns: ['month', 'hour', 'session_duration_sec', 'pages_visited']
2026-03-25 11:55:56,456 - INFO -    Categorical columns: ['transaction_id', 'customer_id', 'date', 'weekday', 'channel']

2026-03-25 11:55:56,456 - INFO - Step 4: Job information...
2026-03-25 11:55:56,456 - INFO -    Total jobs: 16

2026-03-25 11:55:56,456 - INFO - ======================================================================
2026-03-25 11:55:56,456 - INFO - EXPERIMENT COMPLETED: Experiment4_3DN_Optimized
2026-03-25 11:55:56,456 - INFO - Execution time: 12.63 seconds
2026-03-25 11:55:56,456 - INFO - Peak memory: 42 MB
2026-03-25 11:55:56,456 - INFO - Jobs executed: 16
2026-03-25 11:55:56,456 - INFO - ======================================================================
```
Пример лога эксперимента 4 (3 DataNode, оптимизированный режим)
```text
2026-03-25 11:55:56,456 - INFO - Step 1: Loading data from HDFS...
2026-03-25 11:55:56,456 - INFO -    Loaded 150,000 records
2026-03-25 11:55:56,456 - INFO -    Columns: 13
2026-03-25 11:55:56,456 - INFO -    Initial partitions: 4

2026-03-25 11:55:56,456 - INFO - Step 2: Applying optimizations...
2026-03-25 11:55:56,456 - INFO -    Repartitioning to 16 partitions...
2026-03-25 11:55:56,456 - INFO -    Caching data...
2026-03-25 11:55:56,456 - INFO -    Cache applied

2026-03-25 11:55:56,456 - INFO - Step 3: Data analysis...
2026-03-25 11:55:56,456 - INFO -    Numeric columns: ['month', 'hour', 'session_duration_sec', 'pages_visited']
2026-03-25 11:55:56,456 - INFO -    Categorical columns: ['transaction_id', 'customer_id', 'date', 'weekday', 'channel']

2026-03-25 11:55:56,456 - INFO - Step 4: Job information...
2026-03-25 11:55:56,456 - INFO -    Total jobs: 16

2026-03-25 11:55:56,456 - INFO - ======================================================================
2026-03-25 11:55:56,456 - INFO - EXPERIMENT COMPLETED: Experiment4_3DN_Optimized
2026-03-25 11:55:56,456 - INFO - Execution time: 12.63 seconds
2026-03-25 11:55:56,456 - INFO - Peak memory: 42 MB
2026-03-25 11:55:56,456 - INFO - Jobs executed: 16
2026-03-25 11:55:56,456 - INFO - ======================================================================
```

## 6. Сравнение результатов (Все 4 эксперимента)

### 6.1. Сводная таблица всех экспериментов

| Эксперимент | DataNode | Оптимизация | Время (сек) | Память (MB) | Jobs | Stages | Партиции | Ускорение (vs Exp1) |
|-------------|----------|-------------|-------------|-------------|------|--------|----------|---------------------|
| **Exp1** | 1 | Нет | **9.30** | 47 | 24 | 48 | 2 | 1.00x |
| **Exp2** | 1 | Да | 9.98 | 47 | 18 | 36 | 8 | 0.93x |
| **Exp3** | 3 | Нет | 13.38 | 46 | 22 | 44 | 4 | 0.69x |
| **Exp4** | 3 | Да | 12.63 | 42 | 16 | 32 | 16 | 0.74x |

### 6.3. Динамические графики

<img width="5099" height="4343" alt="ecommerce_full_dashboard" src="https://github.com/user-attachments/assets/ab4512b0-9bd6-425c-b706-a300d79faf19" />

 Полный дашборд с результатами экспериментов и аналитикой датасета

<img width="1394" height="504" alt="image" src="https://github.com/user-attachments/assets/4c4c0237-6377-4d66-9002-0280806eed69" />

Анализ поведения пользователей

## 7. Оптимизация Spark Application

### 7.1. Примененные оптимизации

| Оптимизация | Код | Назначение | Эффект на 1 DN | Эффект на 3 DN |
|-------------|-----|------------|----------------|----------------|
| **Репартицирование** | `df.repartition(8/16)` | Увеличение параллелизма, равномерное распределение данных | +300% партиций | +300% партиций |
| **Кэширование** | `df.cache()` | Хранение данных в памяти для повторного использования | +0% памяти | -8.7% памяти |
| **Адаптивное выполнение** | `spark.sql.adaptive.enabled=true` | Оптимизация запросов во время выполнения | — | — |
| **Объединение партиций** | `spark.sql.adaptive.coalescePartitions.enabled=true` | Уменьшение числа партиций после shuffle | — | — |
| **Управление shuffle** | `spark.sql.shuffle.partitions=16/32` | Контроль числа партиций при shuffle операциях | — | — |

### 7.2. Сравнение эффективности оптимизаций

| Конфигурация | Без оптимизаций | С оптимизациями | Абсолютное изменение | Относительное изменение |
|--------------|-----------------|-----------------|---------------------|------------------------|
| **1 DataNode** | 9.30 сек | 9.98 сек | +0.68 сек | **+7.3%**  |
| **3 DataNode** | 13.38 сек | 12.63 сек | -0.75 сек | **-5.6%**  |

### 7.3. Анализ эффективности оптимизаций

#### На 1 DataNode:
| Показатель | Изменение | Причина |
|------------|-----------|---------|
| **Время** | +7.3%  | Накладные расходы на репартицирование и кэширование превышают выгоду |
| **Jobs** | -25%  | Оптимизации уменьшили количество задач |
| **Память** | 0% | Кэширование не дало выигрыша при однократном использовании |

#### На 3 DataNode:
| Показатель | Изменение | Причина |
|------------|-----------|---------|
| **Время** | -5.6%  | Эффект от оптимизаций на распределенном кластере положительный |
| **Jobs** | -27%  | Значительное уменьшение количества задач |
| **Память** | -8.7%  | Кэширование и репартицирование оптимизировали использование памяти |

## 8. Выводы

### Ключевые выводы

| Аспект | Вывод |
|--------|-------|
| **На малых данных** | Базовый режим на 1 DataNode показывает лучший результат |
| **Оптимизации** | На 1 DN дают отрицательный эффект (+7.3%), на 3 DN — положительный (-5.6%) |
| **Масштабирование** | Без оптимизаций ухудшает время (+43.9%) из-за накладных расходов |
| **Память** | Оптимизации на 3 DN снизили память на 8.7% |
| **Jobs/Stages** | Оптимизации уменьшили количество jobs и stages на 25-27% |

### 8.3. Рекомендации по выбору конфигурации

| Размер данных | Рекомендуемая конфигурация | Обоснование |
|---------------|---------------------------|-------------|
| **< 100 MB** | local[2], без оптимизаций | Минимальные накладные расходы |
| **100 MB - 1 GB** | local[4], выборочные оптимизации | Баланс между производительностью и накладными расходами |
| **1 GB - 10 GB** | 2-3 DataNode, cache() при повторном использовании | Эффект от распределения начинает проявляться |
| **> 10 GB** | Кластер 3+ узлов, полный набор оптимизаций | Максимальная производительность за счет распределения |

### 8.4. Практические рекомендации по оптимизациям

| Сценарий | Рекомендация |
|----------|--------------|
| **Данные используются 1 раз** | Не использовать cache()/persist() |
| **Данные используются многократно** | Использовать cache() после загрузки |
| **Перекос данных (data skew)** | Использовать repartition() с указанием ключа |
| **Много shuffle операций** | Увеличить `spark.sql.shuffle.partitions` |
| **Большой объем данных (> 10 GB)** | Использовать полный набор оптимизаций |

## 9. Заключение

В ходе выполнения лабораторной работы были успешно проведены 4 эксперимента по исследованию производительности Apache Spark в кластере Hadoop. Полученные результаты демонстрируют, что выбор оптимальной конфигурации зависит от объема данных:

- **Для малых данных (< 100 MB)** оптимальной является конфигурация с 1 DataNode без оптимизаций
- **Для средних данных (1-10 GB)** эффективно масштабирование до 3 DataNode с применением оптимизаций
- **Для больших данных (> 10 GB)** рекомендуется полный набор оптимизаций и масштабирование кластера

Практическая значимость работы заключается в получении конкретных численных оценок производительности Spark в зависимости от конфигурации кластера и применяемых оптимизаций.
