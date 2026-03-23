#!/usr/bin/env python3
import os
import time
import docker
import requests
import logging
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
from datetime import datetime, timedelta
from prometheus_client import Counter, Gauge, Info, generate_latest, CONTENT_TYPE_LATEST

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Конфиг из переменных окружения
GITLAB_URL = os.getenv('GITLAB_URL', 'https://gitlab.com').strip()
GITLAB_TOKEN = os.getenv('GITLAB_TOKEN', '').strip()
REG_TOKEN = os.getenv('REG_TOKEN', '').strip()
MAX_RUNNERS = int(os.getenv('MAX_RUNNERS', '5'))
MIN_RUNNERS = int(os.getenv('MIN_RUNNERS', '1'))
CHECK_INTERVAL = int(os.getenv('CHECK_INTERVAL', '30'))
RUNNER_IMAGE = os.getenv('RUNNER_IMAGE', 'gitlab/gitlab-runner:latest')
RUNNER_CONCURRENT = int(os.getenv('RUNNER_CONCURRENT', '2'))
RUNNER_TAGS = os.getenv('RUNNER_TAGS', 'autoscale')
PROJECTS_LIMIT = int(os.getenv('PROJECTS_LIMIT', '20'))

# Cooldown (защита от дребезга)
SCALE_UP_COOLDOWN = int(os.getenv('SCALE_UP_COOLDOWN', '60'))
SCALE_DOWN_COOLDOWN = int(os.getenv('SCALE_DOWN_COOLDOWN', '120'))

# Пороги ресурсов хоста
CPU_THRESHOLD = float(os.getenv('CPU_THRESHOLD', '80.0'))
RAM_THRESHOLD = float(os.getenv('RAM_THRESHOLD', '80.0'))

# Graceful shutdown
GRACEFUL_PERIOD = int(os.getenv('GRACEFUL_PERIOD', '300'))

# Профили раннеров
DEFAULT_RUNNER_PROFILE = os.getenv('DEFAULT_RUNNER_PROFILE', 'medium').strip()
RUNNER_PROFILES = {
    'small': {
        'cpu_limit': float(os.getenv('PROFILE_SMALL_CPU_LIMIT', '0.5')),
        'memory_limit': os.getenv('PROFILE_SMALL_MEMORY_LIMIT', '512m'),
        'concurrent': int(os.getenv('PROFILE_SMALL_CONCURRENT', '1')),
        'tags': os.getenv('PROFILE_SMALL_TAGS', 'small'),
    },
    'medium': {
        'cpu_limit': float(os.getenv('PROFILE_MEDIUM_CPU_LIMIT', '1.0')),
        'memory_limit': os.getenv('PROFILE_MEDIUM_MEMORY_LIMIT', '1024m'),
        'concurrent': int(os.getenv('PROFILE_MEDIUM_CONCURRENT', '2')),
        'tags': os.getenv('PROFILE_MEDIUM_TAGS', 'medium'),
    },
    'large': {
        'cpu_limit': float(os.getenv('PROFILE_LARGE_CPU_LIMIT', '2.0')),
        'memory_limit': os.getenv('PROFILE_LARGE_MEMORY_LIMIT', '2048m'),
        'concurrent': int(os.getenv('PROFILE_LARGE_CONCURRENT', '4')),
        'tags': os.getenv('PROFILE_LARGE_TAGS', 'large'),
    },
}

# Валидация
if MIN_RUNNERS > MAX_RUNNERS:
    logger.warning(f'⚠️ MIN_RUNNERS ({MIN_RUNNERS}) > MAX_RUNNERS ({MAX_RUNNERS}), устанавливаем MIN=MAX')
    MIN_RUNNERS = MAX_RUNNERS

if DEFAULT_RUNNER_PROFILE not in RUNNER_PROFILES:
    logger.warning(f'⚠️ Неизвестный профиль {DEFAULT_RUNNER_PROFILE}, используем medium')
    DEFAULT_RUNNER_PROFILE = 'medium'

docker_client = docker.from_env()

# Глобальное состояние для cooldown
last_scale_up_time = None
last_scale_down_time = None

# Состояние для метрик
current_pending_jobs = 0
current_running_runners = 0
runners_by_profile = {'small': 0, 'medium': 0, 'large': 0}

# Prometheus метрики
metrics_pending_jobs = Gauge(
    'gitlab_autoscaler_pending_jobs',
    'Количество pending-задач в GitLab'
)
metrics_running_runners = Gauge(
    'gitlab_autoscaler_running_runners',
    'Количество запущенных раннеров'
)
metrics_running_runners_by_profile = Gauge(
    'gitlab_autoscaler_running_runners_by_profile',
    'Количество запущенных раннеров по профилям',
    ['profile']
)
metrics_scale_up_total = Counter(
    'gitlab_autoscaler_scale_up_total',
    'Общее количество операций scale up'
)
metrics_scale_down_total = Counter(
    'gitlab_autoscaler_scale_down_total',
    'Общее количество операций scale down'
)
metrics_scale_up_blocked_cooldown = Counter(
    'gitlab_autoscaler_scale_up_blocked_cooldown_total',
    'Количество блокировок scale up из-за cooldown'
)
metrics_scale_up_blocked_resources = Counter(
    'gitlab_autoscaler_scale_up_blocked_resources_total',
    'Количество блокировок scale up из-за ресурсов'
)
metrics_scale_down_blocked_cooldown = Counter(
    'gitlab_autoscaler_scale_down_blocked_cooldown_total',
    'Количество блокировок scale down из-за cooldown'
)
metrics_scale_down_blocked_jobs = Counter(
    'gitlab_autoscaler_scale_down_blocked_jobs_total',
    'Количество блокировок scale down из-за активных джоб'
)
metrics_host_cpu_percent = Gauge(
    'gitlab_autoscaler_host_cpu_percent',
    'Загрузка CPU хоста'
)
metrics_host_ram_percent = Gauge(
    'gitlab_autoscaler_host_ram_percent',
    'Использование RAM хоста'
)
metrics_autoscaler_info = Info(
    'gitlab_autoscaler',
    'Информация об автоскейлере'
)
metrics_autoscaler_info.info({
    'gitlab_url': GITLAB_URL,
    'min_runners': str(MIN_RUNNERS),
    'max_runners': str(MAX_RUNNERS),
    'check_interval': str(CHECK_INTERVAL),
    'default_profile': DEFAULT_RUNNER_PROFILE,
})


class MetricsHandler(BaseHTTPRequestHandler):
    """HTTP-обработчик для метрик Prometheus"""

    def do_GET(self):
        if self.path == '/metrics':
            self.send_response(200)
            self.send_header('Content-Type', CONTENT_TYPE_LATEST)
            self.end_headers()
            self.wfile.write(generate_latest())
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format, *args):
        logger.debug(f'Metrics: {args}')


def start_metrics_server(port=8000):
    """Запускает HTTP-сервер для метрик Prometheus"""
    server = HTTPServer(('0.0.0.0', port), MetricsHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    logger.info(f'📊 Prometheus metrics доступны на http://0.0.0.0:{port}/metrics')
    return server


def get_host_resources():
    """
    Получаем текущее использование CPU и RAM хоста.
    Возвращает кортеж (cpu_percent, ram_percent).
    """
    try:
        containers = docker_client.containers.list()

        cpu_total = 0.0
        ram_used = 0

        for container in containers:
            try:
                stats = container.stats(stream=False)

                # CPU — нормализуем на количество CPU хоста
                num_cpus = stats['cpu_stats'].get('online_cpus') or \
                           len(stats['cpu_stats']['cpu_usage'].get('percpu_usage', [1]))
                cpu_delta = stats['cpu_stats']['cpu_usage']['total_usage'] - \
                           stats['precpu_stats']['cpu_usage']['total_usage']
                system_delta = stats['cpu_stats']['system_cpu_usage'] - \
                              stats['precpu_stats']['system_cpu_usage']
                cpu_percent = (cpu_delta / system_delta) * num_cpus * 100.0 if system_delta > 0 else 0
                cpu_total += cpu_percent

                # RAM — вычитаем cache (page cache не является "реальным" потреблением)
                mem_stats = stats['memory_stats']
                mem_usage = mem_stats.get('usage', 0)
                mem_cache = mem_stats.get('stats', {}).get('cache', 0)
                ram_used += max(0, mem_usage - mem_cache)
            except Exception:
                continue

        # Получаем общую RAM хоста
        info = docker_client.info()
        host_ram_total = info.get('MemTotal', 1)
        host_ram_percent = (ram_used / host_ram_total) * 100.0 if host_ram_total > 0 else 0

        return cpu_total, host_ram_percent
    except Exception as e:
        logger.error(f'❌ Ошибка получения ресурсов хоста: {e}')
        return 0.0, 0.0


def can_scale_up():
    """
    Проверяет, можно ли масштабироваться вверх:
    1. Прошёл ли cooldown после последнего scale up
    2. Достаточно ли ресурсов на хосте
    """
    global last_scale_up_time

    if last_scale_up_time is not None:
        elapsed = (datetime.now() - last_scale_up_time).total_seconds()
        if elapsed < SCALE_UP_COOLDOWN:
            remaining = int(SCALE_UP_COOLDOWN - elapsed)
            logger.info(f'⏳ Cooldown scale up: осталось {remaining} сек')
            metrics_scale_up_blocked_cooldown.inc()
            return False

    cpu_percent, ram_percent = get_host_resources()
    logger.info(f'📊 Ресурсы хоста: CPU={cpu_percent:.1f}%, RAM={ram_percent:.1f}% '
                f'(пороги: CPU={CPU_THRESHOLD}%, RAM={RAM_THRESHOLD}%)')

    metrics_host_cpu_percent.set(cpu_percent)
    metrics_host_ram_percent.set(ram_percent)

    if cpu_percent >= CPU_THRESHOLD:
        logger.warning(f'⚠️ Недостаточно CPU ({cpu_percent:.1f}% >= {CPU_THRESHOLD}%)')
        metrics_scale_up_blocked_resources.inc()
        return False

    if ram_percent >= RAM_THRESHOLD:
        logger.warning(f'⚠️ Недостаточно RAM ({ram_percent:.1f}% >= {RAM_THRESHOLD}%)')
        metrics_scale_up_blocked_resources.inc()
        return False

    return True


def can_scale_down():
    """
    Проверяет, можно ли масштабироваться вниз:
    1. Прошёл ли cooldown после последнего scale down
    """
    global last_scale_down_time

    if last_scale_down_time is not None:
        elapsed = (datetime.now() - last_scale_down_time).total_seconds()
        if elapsed < SCALE_DOWN_COOLDOWN:
            remaining = int(SCALE_DOWN_COOLDOWN - elapsed)
            logger.info(f'⏳ Cooldown scale down: осталось {remaining} сек')
            metrics_scale_down_blocked_cooldown.inc()
            return False

    return True


def get_runner_active_jobs(container):
    """
    Проверяет, есть ли у раннера активные джобы.
    Возвращает количество активных задач.
    """
    try:
        runners_resp = requests.get(
            f'{GITLAB_URL}/api/v4/runners',
            headers={'PRIVATE-TOKEN': GITLAB_TOKEN},
            timeout=10
        )
        runners_resp.raise_for_status()
        runners = runners_resp.json()

        runner_id = None
        for runner in runners:
            if container.name in runner.get('description', ''):
                runner_id = runner['id']
                break

        if runner_id is None:
            logger.warning(f'⚠️ Раннер {container.name} не найден в GitLab')
            return 0

        jobs_resp = requests.get(
            f'{GITLAB_URL}/api/v4/runners/{runner_id}/jobs',
            params={'scope': ['running', 'pending']},
            headers={'PRIVATE-TOKEN': GITLAB_TOKEN},
            timeout=10
        )

        if jobs_resp.status_code == 200:
            active_jobs = len(jobs_resp.json())
            if active_jobs > 0:
                logger.info(f'📋 У {container.name} активных джоб: {active_jobs}')
            return active_jobs
        return 0
    except Exception as e:
        logger.error(f'❌ Ошибка проверки джоб раннера {container.name}: {e}')
        return 0


def get_runner_profile(container):
    """
    Определяет профиль раннера по его тегам или названию.
    Возвращает 'small', 'medium', 'large' или 'unknown'.
    """
    try:
        tags = container.labels.get('runner-profile', '')
        if tags in RUNNER_PROFILES:
            return tags

        # Проверяем по названию контейнера
        for profile in RUNNER_PROFILES:
            if profile in container.name:
                return profile

        return DEFAULT_RUNNER_PROFILE
    except Exception:
        return DEFAULT_RUNNER_PROFILE


def count_runners_by_profile():
    """Подсчитывает количество раннеров по профилям"""
    global runners_by_profile
    runners_by_profile = {'small': 0, 'medium': 0, 'large': 0}

    try:
        containers = docker_client.containers.list(
            filters={'name': 'autoscale-runner-', 'status': 'running'}
        )
        for container in containers:
            profile = get_runner_profile(container)
            if profile in runners_by_profile:
                runners_by_profile[profile] += 1
    except Exception as e:
        logger.error(f'❌ Ошибка подсчёта раннеров по профилям: {e}')

    return runners_by_profile


def get_queue_stats():
    """Считаем pending и running jobs по всем доступным проектам"""
    try:
        projects_resp = requests.get(
            f'{GITLAB_URL}/api/v4/projects',
            params={'per_page': PROJECTS_LIMIT, 'order_by': 'last_activity_at', 'sort': 'desc'},
            headers={'PRIVATE-TOKEN': GITLAB_TOKEN},
            timeout=15
        )
        projects_resp.raise_for_status()
        projects = projects_resp.json()

        total_pending = 0
        total_running = 0
        for project in projects:
            for scope, counter_ref in [('pending', 'p'), ('running', 'r')]:
                try:
                    jobs_resp = requests.get(
                        f'{GITLAB_URL}/api/v4/projects/{project["id"]}/jobs',
                        params={'scope': scope, 'per_page': 100},
                        headers={'PRIVATE-TOKEN': GITLAB_TOKEN},
                        timeout=5
                    )
                    if jobs_resp.status_code == 200:
                        x_total = jobs_resp.headers.get('X-Total')
                        count = int(x_total) if x_total is not None else len(jobs_resp.json())
                        if scope == 'pending':
                            total_pending += count
                        else:
                            total_running += count
                except:
                    continue
        return total_pending, total_running
    except Exception as e:
        logger.error(f'❌ Ошибка GitLab API: {e}')
        return 0, 0


def get_pending_jobs():
    """Обратная совместимость — возвращает только pending"""
    pending, _ = get_queue_stats()
    return pending


def get_running_runners():
    """Считаем запущенные контейнеры-раннеры"""
    try:
        containers = docker_client.containers.list(
            filters={'name': 'autoscale-runner-', 'status': 'running'}
        )
        return len(containers)
    except Exception as e:
        logger.error(f'❌ Ошибка Docker API: {e}')
        return 0


def write_runner_config(volume_name, config_content):
    """
    Записывает config.toml в Docker volume через временный контейнер.
    Использует base64 для безопасной передачи содержимого (обходит проблемы
    с кавычками и спецсимволами в shell-команде echo).
    """
    import base64
    encoded = base64.b64encode(config_content.encode()).decode()
    docker_client.containers.run(
        image='alpine:latest',
        name=f'{volume_name}-config-writer',
        volumes=[f'{volume_name}:/etc/gitlab-runner'],
        command=['sh', '-c', f'echo {encoded} | base64 -d > /etc/gitlab-runner/config.toml'],
        remove=True
    )


def generate_runner_config(profile, profile_config):
    """
    Генерирует config.toml для GitLab Runner с лимитами ресурсов для задач.
    """
    # Преобразуем memory_limit в байты
    mem_limit = profile_config['memory_limit']
    if mem_limit.endswith('m'):
        mem_bytes = int(mem_limit[:-1]) * 1024 * 1024
    elif mem_limit.endswith('g'):
        mem_bytes = int(mem_limit[:-1]) * 1024 * 1024 * 1024
    elif mem_limit.endswith('k'):
        mem_bytes = int(mem_limit[:-1]) * 1024
    else:
        mem_bytes = int(mem_limit)

    config = f"""concurrent = {profile_config['concurrent']}
check_interval = 0

[session_server]
  session_timeout = 1800

[[runners]]
  name = "Auto {profile} runner"
  url = "{GITLAB_URL}"
  token = "{{TOKEN}}"
  executor = "docker"
  shell = "sh"
  [runners.custom_build_dir]
    enabled = false
  [runners.cache]
    Type = "local"
    Path = ""
  [runners.docker]
    tls_verify = false
    image = "alpine:latest"
    privileged = false
    disable_entrypoint_overwrite = false
    disable_cache = false
    volumes = ["/cache", "/var/run/docker.sock:/var/run/docker.sock"]
    shm_size = 0
    allowed_images = ["*"]
    allowed_services = ["*"]
    memory = {mem_bytes}
    memory_swap = 0
    oom_kill_disable = false
"""
    return config


def start_runner(profile=None):
    """
    Запускаем новый контейнер-раннер с указанным профилем.
    profile: 'small', 'medium', 'large' или None (используется DEFAULT_RUNNER_PROFILE)
    """
    global last_scale_up_time

    if profile is None:
        profile = DEFAULT_RUNNER_PROFILE

    if profile not in RUNNER_PROFILES:
        logger.warning(f'⚠️ Неизвестный профиль {profile}, используем default')
        profile = DEFAULT_RUNNER_PROFILE

    profile_config = RUNNER_PROFILES[profile]
    name = f'autoscale-runner-{profile}-{int(time.time())}'

    try:
        # Шаг 1: Регистрируем раннер (временный контейнер)
        # register создаёт config.toml с реальным токеном — не перезаписываем его!
        docker_client.containers.run(
            image=RUNNER_IMAGE,
            name=f'{name}-register',
            volumes=[
                f'{name}-config:/etc/gitlab-runner'
            ],
            command=[
                'register', '--non-interactive',
                '--url', GITLAB_URL,
                '--registration-token', REG_TOKEN,
                '--executor', 'docker',
                '--docker-image', 'alpine:latest',
                '--tag-list', f'{RUNNER_TAGS},{profile_config["tags"]}',
                '--concurrent', str(profile_config['concurrent']),
                '--description', f'Auto {profile} {name}',
                '--run-untagged', 'false'
            ],
            remove=True,
            detach=False
        )

        # Шаг 2: Патчим memory limit в уже созданном config.toml через sed
        # НЕ перезаписываем весь файл — в нём уже есть реальный токен от register
        mem_limit = profile_config['memory_limit']
        if mem_limit.endswith('m'):
            mem_bytes = int(mem_limit[:-1]) * 1024 * 1024
        elif mem_limit.endswith('g'):
            mem_bytes = int(mem_limit[:-1]) * 1024 * 1024 * 1024
        else:
            mem_bytes = int(mem_limit)

        docker_client.containers.run(
            image='alpine:latest',
            name=f'{name}-config-patcher',
            volumes=[f'{name}-config:/etc/gitlab-runner'],
            command=[
                'sh', '-c',
                f'echo \'    memory = "{mem_bytes}"\' >> /etc/gitlab-runner/config.toml && '
                f'echo \'    memory_swap = "0"\' >> /etc/gitlab-runner/config.toml'
            ],
            remove=True
        )

        # Шаг 3: Запускаем постоянный контейнер с лимитами ресурсов
        docker_client.containers.run(
            image=RUNNER_IMAGE,
            name=name,
            detach=True,
            volumes=[
                '/var/run/docker.sock:/var/run/docker.sock',
                f'{name}-config:/etc/gitlab-runner'
            ],
            command=['run', '--working-directory', '/etc/gitlab-runner'],
            restart_policy={'Name': 'unless-stopped'},
            labels={
                'autoscale-runner': 'true',
                'runner-profile': profile,
            },
            # Лимиты ресурсов для самого раннера
            nano_cpus=int(profile_config['cpu_limit'] * 1e9),
            mem_limit=profile_config['memory_limit'],
        )

        logger.info(f'🚀 Запущен: {name} (профиль: {profile})')
        logger.info(f'📋 Лимиты для задач: CPU={profile_config["cpu_limit"]}, RAM={profile_config["memory_limit"]}')

        # Ждём пока контейнер действительно перейдёт в running
        # иначе следующий цикл не увидит его и снова сделает scale up
        for _ in range(15):
            time.sleep(1)
            try:
                c = docker_client.containers.get(name)
                if c.status == 'running':
                    break
            except Exception:
                break

        last_scale_up_time = datetime.now()
        metrics_scale_up_total.inc()
        return True

    except Exception as e:
        logger.error(f'❌ Ошибка запуска раннера: {e}')
        try:
            docker_client.containers.get(f'{name}-register').remove(force=True)
        except:
            pass
        try:
            docker_client.containers.get(f'{name}-config-patcher').remove(force=True)
        except:
            pass
        try:
            docker_client.volumes.get(f'{name}-config').remove()
        except:
            pass
        return False


def get_runner_id_by_container(container):
    """
    Находит ID раннера в GitLab по имени контейнера.
    Ищет по всем типам раннеров (instance, project, group).
    Возвращает runner_id или None.
    """
    try:
        page = 1
        while True:
            resp = requests.get(
                f'{GITLAB_URL}/api/v4/runners/all',
                params={'per_page': 100, 'page': page},
                headers={'PRIVATE-TOKEN': GITLAB_TOKEN},
                timeout=10
            )
            resp.raise_for_status()
            runners = resp.json()
            if not runners:
                break
            for runner in runners:
                if container.name in runner.get('description', ''):
                    return runner['id']
            # Проверяем есть ли следующая страница
            if int(resp.headers.get('X-Next-Page', 0)) == 0:
                break
            page += 1
    except Exception as e:
        logger.error(f'❌ Ошибка поиска раннера в GitLab: {e}')
    return None


def cleanup_stale_gitlab_runners():
    """
    При старте скейлера удаляет из GitLab раннеры которые зарегистрированы
    как autoscale-раннеры но их контейнеры уже не существуют.
    Защищает от накопления мёртвых раннеров после краша или рестарта.
    """
    logger.info('🧹 Проверка мёртвых раннеров в GitLab...')
    try:
        # Получаем все живые контейнеры-раннеры
        live_containers = set()
        for c in docker_client.containers.list(filters={'name': 'autoscale-runner-'}):
            live_containers.add(c.name)

        # Получаем все раннеры из GitLab
        page = 1
        stale_count = 0
        while True:
            resp = requests.get(
                f'{GITLAB_URL}/api/v4/runners/all',
                params={'per_page': 100, 'page': page},
                headers={'PRIVATE-TOKEN': GITLAB_TOKEN},
                timeout=10
            )
            resp.raise_for_status()
            runners = resp.json()
            if not runners:
                break

            for runner in runners:
                desc = runner.get('description', '')
                # Это наш раннер если описание начинается с "Auto " и содержит "autoscale-runner-"
                if 'autoscale-runner-' in desc:
                    # Извлекаем имя контейнера из описания (формат: "Auto medium autoscale-runner-medium-TIMESTAMP")
                    container_name = None
                    for part in desc.split():
                        if part.startswith('autoscale-runner-'):
                            container_name = part
                            break

                    if container_name and container_name not in live_containers:
                        logger.warning(f'🗑️ Мёртвый раннер в GitLab: {desc} (id={runner["id"]}) — удаляем')
                        deregister_runner_from_gitlab(runner['id'], desc)
                        stale_count += 1

            if int(resp.headers.get('X-Next-Page', 0)) == 0:
                break
            page += 1

        if stale_count == 0:
            logger.info('✅ Мёртвых раннеров не найдено')
        else:
            logger.info(f'✅ Удалено мёртвых раннеров: {stale_count}')

    except Exception as e:
        logger.error(f'❌ Ошибка очистки мёртвых раннеров: {e}')


def deregister_runner_from_gitlab(runner_id, container_name):
    """
    Удаляет раннер из GitLab через API.
    Возвращает True если успешно (или раннер уже не существует).
    """
    try:
        resp = requests.delete(
            f'{GITLAB_URL}/api/v4/runners/{runner_id}',
            headers={'PRIVATE-TOKEN': GITLAB_TOKEN},
            timeout=10
        )
        if resp.status_code == 204:
            logger.info(f'✅ Раннер {container_name} (id={runner_id}) удалён из GitLab')
            return True
        elif resp.status_code == 404:
            logger.info(f'ℹ️ Раннер {runner_id} уже не существует в GitLab')
            return True  # Цель достигнута — раннера нет
        else:
            logger.warning(f'⚠️ GitLab вернул {resp.status_code} при удалении раннера {runner_id}')
            return False
    except Exception as e:
        logger.error(f'❌ Ошибка деrегистрации раннера {runner_id}: {e}')
        return False


def stop_runner():
    """
    Останавливаем и удаляем лишнего раннера с graceful shutdown.
    Не убиваем раннеры с активными джобами.
    """
    global last_scale_down_time

    try:
        containers = docker_client.containers.list(
            filters={'name': 'autoscale-runner-', 'status': 'running'}
        )
        if not containers:
            return False

        # Сортируем по времени создания (самый старый первый)
        sorted_containers = sorted(containers, key=lambda c: c.attrs['Created'])

        # Ищем раннер без активных джоб
        to_remove = None
        runner_id = None
        for container in sorted_containers:
            active_jobs = get_runner_active_jobs(container)
            if active_jobs == 0:
                to_remove = container
                runner_id = get_runner_id_by_container(container)
                break

        if to_remove is None:
            logger.info('⏳ Все раннеры заняты, откладываем остановку')
            metrics_scale_down_blocked_jobs.inc()
            return False

        # Шаг 1: Деrегистрируем раннер в GitLab через API
        if runner_id is not None:
            deregister_runner_from_gitlab(runner_id, to_remove.name)
        else:
            # Фолбэк: пробуем через exec внутри контейнера
            logger.warning(f'⚠️ Раннер {to_remove.name} не найден в GitLab API, пробуем unregister внутри контейнера')
            try:
                exit_code, output = to_remove.exec_run(
                    'gitlab-runner unregister --all-runners',
                    demux=True
                )
                if exit_code == 0:
                    logger.info(f'✅ Unregister через exec успешен для {to_remove.name}')
                else:
                    logger.warning(f'⚠️ Unregister через exec завершился с кодом {exit_code}')
            except Exception as e:
                logger.error(f'❌ Exec unregister упал: {e}')

        # Шаг 2: Останавливаем и удаляем контейнер
        to_remove.stop(timeout=GRACEFUL_PERIOD)
        volume_name = f'{to_remove.name}-config'
        to_remove.remove()

        # Шаг 3: Удаляем volume
        try:
            docker_client.volumes.get(volume_name).remove()
        except Exception:
            pass

        logger.info(f'🛑 Остановлен: {to_remove.name}')
        last_scale_down_time = datetime.now()
        metrics_scale_down_total.inc()
        return True
    except Exception as e:
        logger.error(f'❌ Ошибка остановки раннера: {e}')
        return False




def ensure_min_runners():
    """🔥 Гарантируем запуск минимум MIN_RUNNERS при старте"""
    running = get_running_runners()
    if running < MIN_RUNNERS:
        logger.info(f'📦 Пре-запуск: нужно {MIN_RUNNERS}, есть {running}. Запускаем {MIN_RUNNERS - running}')
        for _ in range(MIN_RUNNERS - running):
            if get_running_runners() >= MIN_RUNNERS:
                break
            start_runner()
            time.sleep(2)



def get_total_runner_capacity():
    """
    Считаем общую мощность раннеров (сколько задач могут выполнять параллельно).
    Учитывает concurrent каждого раннера.
    """
    try:
        containers = docker_client.containers.list(
            filters={'name': 'autoscale-runner-', 'status': 'running'}
        )
        total_capacity = 0
        for container in containers:
            # Получаем concurrent из labels или используем дефолт
            profile = get_runner_profile(container)
            profile_config = RUNNER_PROFILES.get(profile, RUNNER_PROFILES['medium'])
            total_capacity += profile_config['concurrent']
        return total_capacity
    except Exception as e:
        logger.error(f'❌ Ошибка подсчёта мощности раннеров: {e}')
        return 0


def update_metrics():
    """Обновляет Prometheus метрики"""
    global current_pending_jobs, current_running_runners

    metrics_pending_jobs.set(current_pending_jobs)
    metrics_running_runners.set(current_running_runners)

    count_runners_by_profile()
    for profile, count in runners_by_profile.items():
        metrics_running_runners_by_profile.labels(profile=profile).set(count)


def main():
    global last_scale_up_time, last_scale_down_time
    global current_pending_jobs, current_running_runners

    logger.info('🎯 Автоскейлер запущен')
    logger.info(f'📊 GitLab: {GITLAB_URL} | Min: {MIN_RUNNERS} | Max: {MAX_RUNNERS} | Interval: {CHECK_INTERVAL}s')
    logger.info(f'⏱️ Cooldown: up={SCALE_UP_COOLDOWN}s, down={SCALE_DOWN_COOLDOWN}s')
    logger.info(f'📊 Пороги ресурсов: CPU={CPU_THRESHOLD}%, RAM={RAM_THRESHOLD}%')
    logger.info(f'🕐 Graceful shutdown: {GRACEFUL_PERIOD}s')
    logger.info(f'🏷️ Профили: small/medium/large (default: {DEFAULT_RUNNER_PROFILE})')

    # Запуск сервера метрик
    metrics_port = int(os.getenv('METRICS_PORT', '8000'))
    start_metrics_server(metrics_port)

    # 🧹 Чистим мёртвые раннеры в GitLab которые остались от прошлых запусков
    cleanup_stale_gitlab_runners()

    # 🔥 Сразу обеспечиваем минимум раннеров
    ensure_min_runners()

    while True:
        pending, jobs_running = get_queue_stats()
        running = get_running_runners()
        capacity = get_total_runner_capacity()

        # Общий спрос = pending (ждут) + running (уже выполняются)
        # Scale up нужен, когда pending > 0 — т.е. есть джобы, которым не хватило слотов
        total_demand = pending + jobs_running

        current_pending_jobs = pending
        current_running_runners = running

        logger.info(
            f'📈 Pending: {pending} | Running jobs: {jobs_running} | '
            f'Раннеры: {running} | Мощность: {capacity} '
            f'(min: {MIN_RUNNERS}, max: {MAX_RUNNERS})'
        )

        # Обновление метрик
        update_metrics()

        # 🔥 Восстановление до MIN_RUNNERS (если раннер упал)
        if running < MIN_RUNNERS:
            logger.info(f'🚑 Восстановление: нужно {MIN_RUNNERS}, есть {running}')
            start_runner()
            time.sleep(2)
            update_metrics()
            continue

        # Scale UP: есть pending джобы И не достигли лимита раннеров
        # Сравниваем суммарный спрос с суммарной мощностью
        if pending > 0 and total_demand > capacity and running < MAX_RUNNERS:
            if can_scale_up():
                logger.info(f'⬆️ Масштабирование вверх (спрос {total_demand} > мощность {capacity})')
                start_runner()
                update_metrics()
            else:
                logger.info('⏸️ Scale up отложен (cooldown или ресурсы)')

        # Scale DOWN: pending=0 И running jobs < capacity (раннеры простаивают) И выше минимума
        elif pending == 0 and jobs_running < capacity and running > MIN_RUNNERS:
            if can_scale_down():
                logger.info('⬇️ Масштабирование вниз')
                stop_runner()
                update_metrics()
            else:
                logger.info('⏸️ Scale down отложен (cooldown или активные джобы)')

        time.sleep(CHECK_INTERVAL)


if __name__ == '__main__':
    main()