# GitLab Runner Autoscale

Автоскейлер GitLab Runner — динамическое масштабирование раннеров на основе очереди задач с поддержкой профилей ресурсов и метрик Prometheus.

## Возможности

- 📈 **Автоматический scale up** — запуск раннеров при росте очереди pending-задач
- 📉 **Автоматический scale down** — остановка лишних раннеров при простое
- 🛡️ **Защита от дребезга** — cooldown-периоды между операциями масштабирования
- 💻 **Контроль ресурсов** — блокировка запуска при высокой нагрузке на хост (CPU/RAM)
- 🕐 **Graceful shutdown** — корректная остановка только раннеров без активных задач
- 🚑 **Самовосстановление** — автоматический перезапуск до `MIN_RUNNERS` при падении
- 🏷️ **Профили раннеров** — small/medium/large с разными лимитами CPU/RAM
- 📊 **Prometheus метрики** — HTTP-эндпоинт `/metrics` для мониторинга

## Быстрый старт

### 1. Настройка конфигурации

Отредактируйте `.env`:

```bash
# GitLab
GITLAB_URL=https://gitlab.example.com
GITLAB_TOKEN=glpat-xxxxxxxxxxxxxxxxxxxx
REG_TOKEN=xxxxxxxxxxxxxxxxxxxx

# Масштабирование
MIN_RUNNERS=2
MAX_RUNNERS=5
CHECK_INTERVAL=30

# Cooldown (защита от дребезга)
SCALE_UP_COOLDOWN=60
SCALE_DOWN_COOLDOWN=120

# Пороги ресурсов хоста
CPU_THRESHOLD=80.0
RAM_THRESHOLD=80.0

# Graceful shutdown
GRACEFUL_PERIOD=300

# Профиль по умолчанию
DEFAULT_RUNNER_PROFILE=medium

# Профили раннеров
PROFILE_SMALL_CPU_LIMIT=0.5
PROFILE_SMALL_MEMORY_LIMIT=512m
PROFILE_SMALL_CONCURRENT=1
PROFILE_SMALL_TAGS=small

PROFILE_MEDIUM_CPU_LIMIT=1.0
PROFILE_MEDIUM_MEMORY_LIMIT=1024m
PROFILE_MEDIUM_CONCURRENT=2
PROFILE_MEDIUM_TAGS=medium

PROFILE_LARGE_CPU_LIMIT=2.0
PROFILE_LARGE_MEMORY_LIMIT=2048m
PROFILE_LARGE_CONCURRENT=4
PROFILE_LARGE_TAGS=large

# Метрики Prometheus
METRICS_PORT=8000
```

### 2. Запуск

```bash
docker-compose up -d
```

### 3. Проверка

```bash
# Логи автоскейлера
docker-compose logs -f autoscaler

# Список раннеров
docker ps --filter "name=autoscale-runner-"

# Метрики Prometheus
curl http://localhost:8000/metrics
```

## Конфигурация

### Обязательные переменные

| Переменная | Описание |
|------------|----------|
| `GITLAB_URL` | URL вашего GitLab-инстанса |
| `GITLAB_TOKEN` | Personal Access Token с доступом к API |
| `REG_TOKEN` | Registration token для новых раннеров |

### Настройки масштабирования

| Переменная | По умолчанию | Описание |
|------------|--------------|----------|
| `MIN_RUNNERS` | `1` | Минимальное количество запущенных раннеров |
| `MAX_RUNNERS` | `5` | Максимальное количество раннеров |
| `CHECK_INTERVAL` | `30` | Интервал проверки очереди (сек) |
| `PROJECTS_LIMIT` | `20` | Лимит проектов для проверки pending-задач |

### Cooldown (защита от дребезга)

| Переменная | По умолчанию | Описание |
|------------|--------------|----------|
| `SCALE_UP_COOLDOWN` | `60` | Задержка между запусками раннеров (сек) |
| `SCALE_DOWN_COOLDOWN` | `120` | Задержка между остановками раннеров (сек) |

### Контроль ресурсов хоста

| Переменная | По умолчанию | Описание |
|------------|--------------|----------|
| `CPU_THRESHOLD` | `80.0` | Порог загрузки CPU для блокировки scale up (%) |
| `RAM_THRESHOLD` | `80.0` | Порог использования RAM для блокировки scale up (%) |

### Graceful shutdown

| Переменная | По умолчанию | Описание |
|------------|--------------|----------|
| `GRACEFUL_PERIOD` | `300` | Время ожидания завершения джоб при остановке (сек) |

### Профили раннеров

| Переменная | По умолчанию | Описание |
|------------|--------------|----------|
| `DEFAULT_RUNNER_PROFILE` | `medium` | Профиль по умолчанию (small/medium/large) |

#### Профиль small

| Переменная | По умолчанию | Описание |
|------------|--------------|----------|
| `PROFILE_SMALL_CPU_LIMIT` | `0.5` | Лимит CPU (в ядрах) |
| `PROFILE_SMALL_MEMORY_LIMIT` | `512m` | Лимит памяти |
| `PROFILE_SMALL_CONCURRENT` | `1` | Параллельных задач |
| `PROFILE_SMALL_TAGS` | `small` | Теги раннера |

#### Профиль medium

| Переменная | По умолчанию | Описание |
|------------|--------------|----------|
| `PROFILE_MEDIUM_CPU_LIMIT` | `1.0` | Лимит CPU (в ядрах) |
| `PROFILE_MEDIUM_MEMORY_LIMIT` | `1024m` | Лимит памяти |
| `PROFILE_MEDIUM_CONCURRENT` | `2` | Параллельных задач |
| `PROFILE_MEDIUM_TAGS` | `medium` | Теги раннера |

#### Профиль large

| Переменная | По умолчанию | Описание |
|------------|--------------|----------|
| `PROFILE_LARGE_CPU_LIMIT` | `2.0` | Лимит CPU (в ядрах) |
| `PROFILE_LARGE_MEMORY_LIMIT` | `2048m` | Лимит памяти |
| `PROFILE_LARGE_CONCURRENT` | `4` | Параллельных задач |
| `PROFILE_LARGE_TAGS` | `large` | Теги раннера |

### Метрики Prometheus

| Переменная | По умолчанию | Описание |
|------------|--------------|----------|
| `METRICS_PORT` | `8000` | Порт для HTTP-эндпоинта метрик |

## Prometheus метрики

Автоскейлер предоставляет следующие метрики:

| Метрика | Тип | Описание |
|---------|-----|----------|
| `gitlab_autoscaler_pending_jobs` | Gauge | Количество pending-задач в GitLab |
| `gitlab_autoscaler_running_runners` | Gauge | Количество запущенных раннеров |
| `gitlab_autoscaler_running_runners_by_profile` | Gauge | Раннеры по профилям (small/medium/large) |
| `gitlab_autoscaler_scale_up_total` | Counter | Всего операций scale up |
| `gitlab_autoscaler_scale_down_total` | Counter | Всего операций scale down |
| `gitlab_autoscaler_scale_up_blocked_cooldown_total` | Counter | Блокировок scale up (cooldown) |
| `gitlab_autoscaler_scale_up_blocked_resources_total` | Counter | Блокировок scale up (ресурсы) |
| `gitlab_autoscaler_scale_down_blocked_cooldown_total` | Counter | Блокировок scale down (cooldown) |
| `gitlab_autoscaler_scale_down_blocked_jobs_total` | Counter | Блокировок scale down (активные джобы) |
| `gitlab_autoscaler_host_cpu_percent` | Gauge | Загрузка CPU хоста |
| `gitlab_autoscaler_host_ram_percent` | Gauge | Использование RAM хоста |
| `gitlab_autoscaler_info` | Info | Информация об автоскейлере |

### Пример scrape-конфигурации для Prometheus

```yaml
scrape_configs:
  - job_name: 'gitlab-autoscaler'
    static_configs:
      - targets: ['localhost:8000']
    metrics_path: /metrics
```

### Пример Grafana dashboard

Импортируйте дашборд с метриками:
- pending jobs vs running runners
- scale up/down операции
- блокировки по причинам
- использование ресурсов хоста

## Логика работы

### Scale Up (расширение)

```
┌─────────────────────────────────────────────────────────┐
│  pending_jobs > running_runners И running < MAX_RUNNERS │
└─────────────────────────────────────────────────────────┘
                          │
                          ▼
              ┌───────────────────────┐
              │  Проверка cooldown    │
              └───────────────────────┘
                          │
                          ▼
              ┌───────────────────────┐
              │  Проверка CPU/RAM     │
              └───────────────────────┘
                          │
              ┌───────────┴───────────┐
              │                       │
           OK (✅)                 FAIL (❌)
              │                       │
              ▼                       ▼
     ┌─────────────────┐     ┌─────────────────┐
     │  Запуск раннера │     │  Отложить запуск│
     │  (с профилем)   │     └─────────────────┘
     └─────────────────┘
```

### Scale Down (сжатие)

```
┌─────────────────────────────────────────────────────┐
│  pending_jobs == 0 И running_runners > MIN_RUNNERS  │
└─────────────────────────────────────────────────────┘
                          │
                          ▼
              ┌───────────────────────┐
              │  Проверка cooldown    │
              └───────────────────────┘
                          │
                          ▼
              ┌───────────────────────┐
              │  Поиск раннера без    │
              │  активных джоб        │
              └───────────────────────┘
                          │
              ┌───────────┴───────────┐
              │                       │
           Найден (✅)            Не найден (❌)
              │                       │
              ▼                       ▼
     ┌─────────────────┐     ┌─────────────────┐
     │  Дерегистрация  │     │  Отложить остановку│
     │  + остановка    │     └─────────────────┘
     └─────────────────┘
```

### Восстановление (Recovery)

```
┌─────────────────────────────────────┐
│  running_runners < MIN_RUNNERS      │
└─────────────────────────────────────┘
                          │
                          ▼
              ┌───────────────────────┐
              │  Немедленный запуск   │
              │  до MIN_RUNNERS       │
              └───────────────────────┘
```

## Требования

- Docker и Docker Compose
- Доступ к Docker socket (`/var/run/docker.sock`)
- Пользователь должен быть в группе docker (GID `986` в данном проекте)

## Структура проекта

```
gitlab-runner-autoscale/
├── .env                      # Конфигурация
├── docker-compose.yml        # Оркестрация
├── README.md                 # Документация
└── autoscaler/
    ├── Dockerfile            # Образ автоскейлера
    ├── requirements.txt      # Python-зависимости
    └── scaler.py             # Основной скрипт
```

## Управление

### Запуск

```bash
docker-compose up -d
```

### Просмотр логов

```bash
docker-compose logs -f autoscaler
```

### Перезапуск

```bash
docker-compose restart autoscaler
```

### Остановка

```bash
docker-compose down
```

### Отладка

```bash
# Запуск в foreground
docker-compose up autoscaler

# Вход в контейнер
docker exec -it gitlab-autoscaler bash

# Проверка метрик
curl http://localhost:8000/metrics
```

## Зависимости

- `docker==7.1.0` — Docker SDK для Python
- `requests==2.32.3` — HTTP-клиент для GitLab API
- `prometheus-client==0.20.0` — Клиент Prometheus

## Примеры сценариев

### Сценарий 1: Пиковая нагрузка

```
1. В очереди 10 pending-задач
2. Запущено 2 раннера (MIN_RUNNERS=2, профиль medium)
3. Автоскейлер запускает раннеры до MAX_RUNNERS=5
4. После обработки очереди — остановка до MIN_RUNNERS
```

### Сценарий 2: Падение раннера

```
1. Запущено 2 раннера (MIN_RUNNERS=2)
2. Один раннер упал (контейнер удалён)
3. На следующей проверке автоскейлер обнаруживает 1 раннер
4. Немедленный запуск нового раннера до MIN_RUNNERS=2
```

### Сценарий 3: Высокая нагрузка на хост

```
1. CPU хоста загружен на 85% (порог 80%)
2. В очереди pending-задачи
3. Scale up блокируется до снижения нагрузки
4. В логах: "⚠️ Недостаточно CPU (85.0% >= 80.0%)"
5. Метрика `gitlab_autoscaler_scale_up_blocked_resources_total` инкрементируется
```

### Сценарий 4: Разные профили для разных задач

```
1. Лёгкие задачи (lint, test) → профиль small (0.5 CPU, 512MB)
2. Стандартные задачи (build) → профиль medium (1 CPU, 1GB)
3. Тяжёлые задачи (docker build, deploy) → профиль large (2 CPU, 2GB)
4. Настройте DEFAULT_RUNNER_PROFILE или модифицируйте логику выбора профиля
```

## Примечания

- Каждый раннер создаётся с собственным volume (`{name}-config`)
- Раннеры помечаются лейблом `autoscale-runner: true` и `runner-profile: <profile>`
- При остановке выбирается самый старый раннер без активных задач
- Cooldown применяется только после успешной операции масштабирования
- Метрики доступны на порту `METRICS_PORT` (по умолчанию 8000)
- **Лимиты ресурсов применяются как к раннеру, так и к создаваемым им задачам** через `config.toml`
