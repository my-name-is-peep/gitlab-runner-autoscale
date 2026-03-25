#!/usr/bin/env python3
import os
import sys
import time
import json
import logging
import docker
import requests
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
from datetime import datetime, timedelta
from prometheus_client import Counter, Gauge, Info, generate_latest, CONTENT_TYPE_LATEST
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from pythonjsonlogger import jsonlogger

# =============================================================================
# JSON Logging
# =============================================================================

class CustomJsonFormatter(jsonlogger.JsonFormatter):
    """Custom JSON formatter with level and timestamp"""

    def add_fields(self, log_record, record, message_dict):
        super().add_fields(log_record, record, message_dict)
        log_record['level'] = record.levelname
        log_record['timestamp'] = datetime.utcnow().isoformat() + 'Z'
        log_record['logger'] = record.name
        if record.funcName:
            log_record['function'] = record.funcName
        if record.lineno:
            log_record['line'] = record.lineno

def setup_json_logging(level='INFO'):
    """Setup JSON logging"""
    logger = logging.getLogger()
    logger.setLevel(getattr(logging, level.upper()))
    
    # Clear existing handlers
    logger.handlers = []
    
    # Create JSON handler for stdout
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(CustomJsonFormatter(
        '%(timestamp)s %(level)s %(name)s %(message)s %(function)s %(line)s'
    ))
    logger.addHandler(handler)
    
    return logger

logging.basicConfig()
logger = setup_json_logging('INFO')

# =============================================================================
# Retry logic для GitLab API
# =============================================================================

class GitLabAPIError(Exception):
    """Ошибка GitLab API"""
    pass

class RateLimitError(Exception):
    """Превышен лимит запросов к GitLab API"""
    pass

def create_retry_decorator():
    """Создаёт декоратор retry с exponential backoff"""
    return retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        retry=retry_if_exception_type((requests.exceptions.RequestException, GitLabAPIError)),
        reraise=True,
        before_sleep=lambda retry_state: logger.warning(
            f'Попытка {retry_state.attempt_number} из 5, ожидание {retry_state.next_action.sleep}с',
            extra={'attempt': retry_state.attempt_number, 'next_wait': retry_state.next_action.sleep}
        )
    )

gitlab_retry = create_retry_decorator()

# =============================================================================
# Конфигурация
# =============================================================================

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

# Retry настройки
MAX_RETRIES = int(os.getenv('MAX_RETRIES', '5'))
RETRY_BASE_DELAY = float(os.getenv('RETRY_BASE_DELAY', '2.0'))
RETRY_MAX_DELAY = float(os.getenv('RETRY_MAX_DELAY', '30.0'))

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

# =============================================================================
# Валидация конфигурации
# =============================================================================

class ConfigValidationError(Exception):
    """Ошибка валидации конфигурации"""
    pass

def validate_config():
    """
    Валидирует конфигурацию при старте.
    Выбрасывает ConfigValidationError при критических ошибках.
    """
    errors = []
    warnings = []

    # Проверка обязательных переменных
    if not GITLAB_URL:
        errors.append('GITLAB_URL не указан')
    elif not GITLAB_URL.startswith(('http://', 'https://')):
        errors.append(f'GITLAB_URL должен начинаться с http:// или https:// (получено: {GITLAB_URL})')

    if not GITLAB_TOKEN:
        errors.append('GITLAB_TOKEN не указан')
    elif len(GITLAB_TOKEN) < 10:
        errors.append(f'GITLAB_TOKEN слишком короткий (длина: {len(GITLAB_TOKEN)})')

    if not REG_TOKEN:
        errors.append('REG_TOKEN не указан')

    # Проверка диапазонов
    if MIN_RUNNERS < 0:
        errors.append(f'MIN_RUNNERS не может быть отрицательным ({MIN_RUNNERS})')
    
    if MAX_RUNNERS < 1:
        errors.append(f'MAX_RUNNERS должен быть >= 1 ({MAX_RUNNERS})')
    
    if MIN_RUNNERS > MAX_RUNNERS:
        warnings.append(f'MIN_RUNNERS ({MIN_RUNNERS}) > MAX_RUNNERS ({MAX_RUNNERS}) — будет установлено MIN=MAX')

    if CHECK_INTERVAL < 5:
        warnings.append(f'CHECK_INTERVAL слишком маленький ({CHECK_INTERVAL}с), рекомендуется >= 30с')

    if SCALE_UP_COOLDOWN < 10:
        warnings.append(f'SCALE_UP_COOLDOWN слишком маленький ({SCALE_UP_COOLDOWN}с), рекомендуется >= 30с')

    if SCALE_DOWN_COOLDOWN < 30:
        warnings.append(f'SCALE_DOWN_COOLDOWN слишком маленький ({SCALE_DOWN_COOLDOWN}с), рекомендуется >= 60с')

    if not (0 < CPU_THRESHOLD <= 100):
        errors.append(f'CPU_THRESHOLD должен быть в диапазоне (0, 100] ({CPU_THRESHOLD})')

    if not (0 < RAM_THRESHOLD <= 100):
        errors.append(f'RAM_THRESHOLD должен быть в диапазоне (0, 100] ({RAM_THRESHOLD})')

    if DEFAULT_RUNNER_PROFILE not in RUNNER_PROFILES:
        errors.append(f'Неизвестный профиль DEFAULT_RUNNER_PROFILE: {DEFAULT_RUNNER_PROFILE} (доступны: {list(RUNNER_PROFILES.keys())})')

    # Проверка профилей
    for profile_name, profile_config in RUNNER_PROFILES.items():
        if profile_config['concurrent'] < 1:
            errors.append(f'PROFILE_{profile_name.upper()}_CONCURRENT должен быть >= 1 ({profile_config["concurrent"]})')
        if profile_config['cpu_limit'] <= 0:
            errors.append(f'PROFILE_{profile_name.upper()}_CPU_LIMIT должен быть > 0 ({profile_config["cpu_limit"]})')

    # Log warnings
    for warning in warnings:
        logger.warning(f'[CONFIG] {warning}', extra={'validation': 'warning'})

    # Raise error if critical issues exist
    if errors:
        for error in errors:
            logger.error(f'[CONFIG] {error}', extra={'validation': 'error'})
        raise ConfigValidationError(f'Critical configuration errors: {"; ".join(errors)}')

    logger.info('[CONFIG] Configuration validated successfully', extra={
        'config': {
            'gitlab_url': GITLAB_URL,
            'min_runners': MIN_RUNNERS,
            'max_runners': MAX_RUNNERS,
            'check_interval': CHECK_INTERVAL,
            'default_profile': DEFAULT_RUNNER_PROFILE,
        }
    })

def validate_gitlab_connection():
    """
    Проверяет подключение к GitLab и валидность токена.
    Выбрасывает exception при ошибке.
    """
    try:
        resp = requests.get(
            f'{GITLAB_URL}/api/v4/user',
            headers={'PRIVATE-TOKEN': GITLAB_TOKEN},
            timeout=10
        )
        
        if resp.status_code == 401:
            raise ConfigValidationError('GITLAB_TOKEN недействителен (401 Unauthorized)')
        elif resp.status_code == 403:
            raise ConfigValidationError('GITLAB_TOKEN недостаточен для доступа к API (403 Forbidden)')
        elif resp.status_code != 200:
            raise ConfigValidationError(f'GitLab вернул статус {resp.status_code}')
        
        user = resp.json()
        logger.info(f'[GITLAB] Connected successfully: {user.get("username")} ({user.get("name")})', extra={
            'gitlab': {
                'url': GITLAB_URL,
                'user': user.get('username', 'unknown'),
                'name': user.get('name', 'unknown'),
            }
        })
        
    except requests.exceptions.ConnectionError as e:
        raise ConfigValidationError(f'Не удалось подключиться к GitLab ({GITLAB_URL}): {e}')
    except requests.exceptions.Timeout:
        raise ConfigValidationError(f'Превышено время ожидания подключения к GitLab ({GITLAB_URL})')
    except ConfigValidationError:
        raise
    except Exception as e:
        raise ConfigValidationError(f'Ошибка проверки подключения к GitLab: {e}')

def validate_docker_connection(docker_client):
    """
    Проверяет подключение к Docker.
    Выбрасывает exception при ошибке.
    """
    try:
        info = docker_client.info()
        logger.info(f'[DOCKER] Connected successfully: Docker {info.get("ServerVersion")} on {info.get("OperatingSystem")}', extra={
            'docker': {
                'version': info.get('ServerVersion', 'unknown'),
                'containers_running': info.get('ContainersRunning', 0),
                'os': info.get('OperatingSystem', 'unknown'),
            }
        })
    except Exception as e:
        raise ConfigValidationError(f'Docker connection error: {e}')

# =============================================================================
# Инициализация Docker клиента (после валидации конфига)
# =============================================================================

try:
    docker_client = docker.from_env()
except Exception as e:
    logger.error(f'Failed to initialize Docker client: {e}')
    docker_client = None

# =============================================================================
# Глобальное состояние
# =============================================================================

last_scale_up_time = None
last_scale_down_time = None

current_pending_jobs = 0
current_running_runners = 0
current_running_jobs = 0
runners_by_profile = {'small': 0, 'medium': 0, 'large': 0}

# Статистика для метрик
api_request_count = 0
api_error_count = 0
last_gitlab_check = None

# =============================================================================
# Prometheus метрики
# =============================================================================

metrics_pending_jobs = Gauge(
    'gitlab_autoscaler_pending_jobs',
    'Количество pending-задач в GitLab'
)
metrics_running_jobs = Gauge(
    'gitlab_autoscaler_running_jobs',
    'Количество running-задач в GitLab'
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
metrics_api_requests_total = Counter(
    'gitlab_autoscaler_api_requests_total',
    'Общее количество запросов к GitLab API',
    ['status']
)
metrics_api_latency_seconds = Gauge(
    'gitlab_autoscaler_api_latency_seconds',
    'Последняя задержка запроса к GitLab API'
)
metrics_uptime_seconds = Gauge(
    'gitlab_autoscaler_uptime_seconds',
    'Время работы автоскейлера в секундах'
)
metrics_autoscaler_info = Info(
    'gitlab_autoscaler',
    'Информация об автоскейлере'
)

startup_time = datetime.now()
metrics_autoscaler_info.info({
    'gitlab_url': GITLAB_URL,
    'min_runners': str(MIN_RUNNERS),
    'max_runners': str(MAX_RUNNERS),
    'check_interval': str(CHECK_INTERVAL),
    'default_profile': DEFAULT_RUNNER_PROFILE,
    'version': '1.0.0',
})

# =============================================================================
# HTTP сервер для метрик и healthcheck
# =============================================================================

class MetricsHandler(BaseHTTPRequestHandler):
    """HTTP-обработчик для метрик Prometheus и healthcheck"""

    def do_GET(self):
        if self.path == '/metrics':
            self.handle_metrics()
        elif self.path == '/health':
            self.handle_health()
        elif self.path == '/ready':
            self.handle_ready()
        else:
            self.send_response(404)
            self.end_headers()

    def handle_metrics(self):
        self.send_response(200)
        self.send_header('Content-Type', CONTENT_TYPE_LATEST)
        self.end_headers()
        self.wfile.write(generate_latest())

    def handle_health(self):
        """
        Проверка здоровья сервиса.
        Возвращает 200 OK если сервис работает.
        """
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.end_headers()
        response = {
            'status': 'healthy',
            'timestamp': datetime.utcnow().isoformat() + 'Z',
            'service': 'gitlab-autoscaler',
            'uptime_seconds': (datetime.now() - startup_time).total_seconds(),
        }
        self.wfile.write(json.dumps(response).encode())

    def handle_ready(self):
        """
        Проверка готовности сервиса.
        Возвращает 200 OK если сервис готов к работе (подключен к GitLab и Docker).
        """
        is_ready = docker_client is not None and last_gitlab_check is not None
        
        self.send_response(200 if is_ready else 503)
        self.send_header('Content-Type', 'application/json')
        self.end_headers()
        response = {
            'ready': is_ready,
            'timestamp': datetime.utcnow().isoformat() + 'Z',
            'checks': {
                'docker': docker_client is not None,
                'gitlab': last_gitlab_check is not None,
            }
        }
        self.wfile.write(json.dumps(response).encode())

    def log_message(self, format, *args):
        logger.debug(f'HTTP request: {args}')


def start_metrics_server(port=8000):
    """Запускает HTTP-сервер для метрик Prometheus"""
    server = HTTPServer(('0.0.0.0', port), MetricsHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    logger.info(f'[METRICS] Prometheus available on port {port}', extra={
        'metrics': {'port': port, 'endpoints': ['/metrics', '/health', '/ready']}
    })
    return server

# =============================================================================
# GitLab API функции с retry
# =============================================================================

@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    retry=retry_if_exception_type((requests.exceptions.RequestException, GitLabAPIError, RateLimitError)),
    reraise=True,
    before_sleep=lambda retry_state: logger.warning(
        f'Попытка {retry_state.attempt_number} из 5, ожидание {retry_state.next_action.sleep}с',
        extra={'attempt': retry_state.attempt_number, 'next_wait': retry_state.next_action.sleep}
    )
)
def gitlab_get(url, params=None, timeout=10):
    """GET запрос к GitLab API с retry"""
    global api_request_count, api_error_count
    
    start_time = time.time()
    api_request_count += 1
    
    try:
        resp = requests.get(
            url,
            params=params,
            headers={'PRIVATE-TOKEN': GITLAB_TOKEN},
            timeout=timeout
        )
        
        latency = time.time() - start_time
        metrics_api_latency_seconds.set(latency)
        
        if resp.status_code == 429:
            retry_after = int(resp.headers.get('Retry-After', 30))
            logger.warning(f'Rate limit exceeded, waiting {retry_after}s', extra={
                'gitlab': {'rate_limit': True, 'retry_after': retry_after}
            })
            metrics_api_requests_total.labels(status='rate_limited').inc()
            time.sleep(retry_after)
            raise RateLimitError(f'Rate limit exceeded, retry after {retry_after}s')
        
        if resp.status_code >= 500:
            metrics_api_requests_total.labels(status='server_error').inc()
            api_error_count += 1
            raise GitLabAPIError(f'GitLab server error: {resp.status_code}')
        
        if resp.status_code >= 400:
            metrics_api_requests_total.labels(status='client_error').inc()
            api_error_count += 1
            logger.error(f'GitLab API error: {resp.status_code}', extra={
                'gitlab': {'url': url, 'status': resp.status_code}
            })
        
        metrics_api_requests_total.labels(status='success').inc()
        resp.raise_for_status()
        return resp
        
    except requests.exceptions.Timeout as e:
        metrics_api_requests_total.labels(status='timeout').inc()
        api_error_count += 1
        logger.error(f'GitLab API timeout: {url}', extra={'gitlab': {'url': url}})
        raise
    except requests.exceptions.ConnectionError as e:
        metrics_api_requests_total.labels(status='connection_error').inc()
        api_error_count += 1
        logger.error(f'GitLab API connection error: {url}', extra={'gitlab': {'url': url}})
        raise

def gitlab_get_runners():
    """Получает список всех раннеров из GitLab"""
    runners = []
    page = 1

    while True:
        resp = gitlab_get(
            f'{GITLAB_URL}/api/v4/runners/all',
            params={'per_page': 100, 'page': page},
            timeout=10
        )
        page_runners = resp.json()

        if not page_runners:
            break

        runners.extend(page_runners)

        # Безопасный парсинг заголовка (может быть пустой строкой)
        next_page = resp.headers.get('X-Next-Page', '')
        if not next_page:
            break
        page += 1

    return runners

def gitlab_get_runner_jobs(runner_id):
    """Получает активные джобы раннера"""
    resp = gitlab_get(
        f'{GITLAB_URL}/api/v4/runners/{runner_id}/jobs',
        params={'scope': ['running', 'pending']},
        timeout=10
    )
    return resp.json()

def gitlab_deregister_runner(runner_id):
    """Remove runner from GitLab"""
    try:
        resp = requests.delete(
            f'{GITLAB_URL}/api/v4/runners/{runner_id}',
            headers={'PRIVATE-TOKEN': GITLAB_TOKEN},
            timeout=10
        )

        if resp.status_code == 204:
            logger.info(f'[GITLAB] Runner removed: id={runner_id}', extra={'gitlab': {'runner_id': runner_id}})
            return True
        elif resp.status_code == 404:
            logger.info(f'[GITLAB] Runner already removed: id={runner_id}', extra={'gitlab': {'runner_id': runner_id}})
            return True
        else:
            logger.warning(f'[GITLAB] Failed to remove runner: status {resp.status_code}, id={runner_id}', extra={
                'gitlab': {'runner_id': runner_id, 'status': resp.status_code}
            })
            return False
    except Exception as e:
        logger.error(f'[GITLAB] Deregister error for runner {runner_id}: {e}', extra={
            'gitlab': {'runner_id': runner_id, 'error': str(e)}
        })
        return False

# =============================================================================
# Функции работы с хостом
# =============================================================================

def get_host_resources():
    """
    Get current host CPU and RAM usage.
    Returns tuple (cpu_percent, ram_percent).
    """
    try:
        containers = docker_client.containers.list()

        cpu_total = 0.0
        ram_used = 0

        for container in containers:
            try:
                stats = container.stats(stream=False)

                # CPU — normalize by host CPU count
                num_cpus = stats['cpu_stats'].get('online_cpus') or \
                           len(stats['cpu_stats']['cpu_usage'].get('percpu_usage', [1]))
                cpu_delta = stats['cpu_stats']['cpu_usage']['total_usage'] - \
                           stats['precpu_stats']['cpu_usage']['total_usage']
                system_delta = stats['cpu_stats']['system_cpu_usage'] - \
                              stats['precpu_stats']['system_cpu_usage']
                cpu_percent = (cpu_delta / system_delta) * num_cpus * 100.0 if system_delta > 0 else 0
                cpu_total += cpu_percent

                # RAM — subtract cache (page cache is not "real" usage)
                mem_stats = stats['memory_stats']
                mem_usage = mem_stats.get('usage', 0)
                mem_cache = mem_stats.get('stats', {}).get('cache', 0)
                ram_used += max(0, mem_usage - mem_cache)
            except Exception:
                continue

        # Get total host RAM
        info = docker_client.info()
        host_ram_total = info.get('MemTotal', 1)
        host_ram_percent = (ram_used / host_ram_total) * 100.0 if host_ram_total > 0 else 0

        return cpu_total, host_ram_percent
    except Exception as e:
        logger.error(f'Failed to get host resources', extra={'error': str(e)})
        return 0.0, 0.0


def can_scale_up():
    """
    Check if we can scale up:
    1. Cooldown period passed since last scale up
    2. Sufficient host resources available
    """
    global last_scale_up_time

    if last_scale_up_time is not None:
        elapsed = (datetime.now() - last_scale_up_time).total_seconds()
        if elapsed < SCALE_UP_COOLDOWN:
            remaining = int(SCALE_UP_COOLDOWN - elapsed)
            logger.info(f'[COOLDOWN] Scale up: waiting {remaining}s', extra={
                'cooldown': {'type': 'scale_up', 'remaining': remaining}
            })
            metrics_scale_up_blocked_cooldown.inc()
            return False

    cpu_percent, ram_percent = get_host_resources()
    logger.info(f'[RESOURCES] Host: CPU={cpu_percent:.1f}%, RAM={ram_percent:.1f}% (threshold: {CPU_THRESHOLD}%, {RAM_THRESHOLD}%)', extra={
        'resources': {'cpu': cpu_percent, 'ram': ram_percent}
    })

    metrics_host_cpu_percent.set(cpu_percent)
    metrics_host_ram_percent.set(ram_percent)

    if cpu_percent >= CPU_THRESHOLD:
        logger.warning(f'[BLOCKED] Insufficient CPU: {cpu_percent:.1f}% >= {CPU_THRESHOLD}%', extra={
            'resources': {'cpu': cpu_percent, 'threshold': CPU_THRESHOLD}
        })
        metrics_scale_up_blocked_resources.inc()
        return False

    if ram_percent >= RAM_THRESHOLD:
        logger.warning(f'[BLOCKED] Insufficient RAM: {ram_percent:.1f}% >= {RAM_THRESHOLD}%', extra={
            'resources': {'ram': ram_percent, 'threshold': RAM_THRESHOLD}
        })
        metrics_scale_up_blocked_resources.inc()
        return False

    return True


def can_scale_down():
    """
    Check if we can scale down:
    1. Cooldown period passed since last scale down
    """
    global last_scale_down_time

    if last_scale_down_time is not None:
        elapsed = (datetime.now() - last_scale_down_time).total_seconds()
        if elapsed < SCALE_DOWN_COOLDOWN:
            remaining = int(SCALE_DOWN_COOLDOWN - elapsed)
            logger.info(f'[COOLDOWN] Scale down: waiting {remaining}s', extra={
                'cooldown': {'type': 'scale_down', 'remaining': remaining}
            })
            metrics_scale_down_blocked_cooldown.inc()
            return False

    return True

# =============================================================================
# Функции работы с раннерами
# =============================================================================

def get_runner_active_jobs(container):
    """
    Check if runner has active jobs.
    Returns number of truly active (running/pending) jobs.
    """
    try:
        runners = gitlab_get_runners()

        runner_id = None
        for runner in runners:
            if container.name in runner.get('description', ''):
                runner_id = runner['id']
                break

        if runner_id is None:
            logger.warning(f'Runner not found in GitLab', extra={'container': container.name})
            return 0

        # Get all jobs and filter only truly active ones
        all_jobs_resp = gitlab_get(
            f'{GITLAB_URL}/api/v4/runners/{runner_id}/jobs',
            params={'per_page': 100},
            timeout=10
        )
        all_jobs = all_jobs_resp.json()

        # Count only running/pending jobs
        active_jobs = sum(1 for job in all_jobs if job.get('status') in ['running', 'pending'])

        if active_jobs > 0:
            logger.info(f'Runner has active jobs', extra={
                'container': container.name,
                'active_jobs': active_jobs
            })
        return active_jobs

    except Exception as e:
        logger.error(f'Failed to check runner jobs', extra={
            'container': container.name,
            'error': str(e)
        })
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
    """Count runners by profile"""
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
        logger.error(f'Failed to count runners by profile', extra={'error': str(e)})

    return runners_by_profile


def get_queue_stats():
    """Get pending and running jobs count from all available projects"""
    try:
        projects_resp = gitlab_get(
            f'{GITLAB_URL}/api/v4/projects',
            params={'per_page': PROJECTS_LIMIT, 'order_by': 'last_activity_at', 'sort': 'desc'},
            timeout=15
        )
        projects = projects_resp.json()

        total_pending = 0
        total_running = 0

        for project in projects:
            for scope in ['pending', 'running']:
                try:
                    jobs_resp = gitlab_get(
                        f'{GITLAB_URL}/api/v4/projects/{project["id"]}/jobs',
                        params={'scope': scope, 'per_page': 100},
                        timeout=5
                    )
                    x_total = jobs_resp.headers.get('X-Total')
                    count = int(x_total) if x_total is not None else len(jobs_resp.json())

                    if scope == 'pending':
                        total_pending += count
                    else:
                        total_running += count
                except Exception:
                    continue

        return total_pending, total_running

    except Exception as e:
        logger.error(f'Failed to get queue stats', extra={'error': str(e)})
        return 0, 0


def get_running_runners():
    """Count running runner containers"""
    try:
        containers = docker_client.containers.list(
            filters={'name': 'autoscale-runner-', 'status': 'running'}
        )
        return len(containers)
    except Exception as e:
        logger.error(f'Failed to get running runners', extra={'error': str(e)})
        return 0


def get_stopped_runners():
    """Get list of stopped runner containers"""
    try:
        containers = docker_client.containers.list(
            filters={'name': 'autoscale-runner-', 'status': 'exited'},
            all=True
        )
        return containers
    except Exception as e:
        logger.error(f'Failed to get stopped runners', extra={'error': str(e)})
        return []


def start_runner(profile=None):
    """
    Start a new runner container with specified profile.
    profile: 'small', 'medium', 'large' or None (uses DEFAULT_RUNNER_PROFILE)
    """
    global last_scale_up_time

    if profile is None:
        profile = DEFAULT_RUNNER_PROFILE

    if profile not in RUNNER_PROFILES:
        logger.warning(f'Unknown profile, using default', extra={
            'requested_profile': profile,
            'using_profile': DEFAULT_RUNNER_PROFILE
        })
        profile = DEFAULT_RUNNER_PROFILE

    profile_config = RUNNER_PROFILES[profile]
    name = f'autoscale-runner-{profile}-{int(time.time())}'

    try:
        # Step 1: Register runner (temporary container)
        docker_client.containers.run(
            image=RUNNER_IMAGE,
            name=f'{name}-register',
            volumes=[f'{name}-config:/etc/gitlab-runner'],
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

        # Step 2: Patch memory limit in config.toml
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

        # Step 3: Start permanent container with resource limits
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
            nano_cpus=int(profile_config['cpu_limit'] * 1e9),
            mem_limit=profile_config['memory_limit'],
        )

        logger.info(f'[SCALE UP] Runner started: {name} (profile: {profile}, CPU: {profile_config["cpu_limit"]}, RAM: {profile_config["memory_limit"]})', extra={
            'action': 'scale_up',
            'container': name,
            'profile': profile,
            'cpu_limit': profile_config['cpu_limit'],
            'memory_limit': profile_config['memory_limit'],
        })

        # Wait for container to be running
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
        logger.error(f'Failed to start runner', extra={
            'error': str(e),
            'container': name,
            'profile': profile
        })
        # Cleanup
        for suffix in ['-register', '-config-patcher', '']:
            try:
                docker_client.containers.get(f'{name}{suffix}').remove(force=True)
            except:
                pass
        try:
            docker_client.volumes.get(f'{name}-config').remove()
        except:
            pass
        return False


def get_runner_id_by_container(container):
    """
    Find runner ID in GitLab by container name.
    """
    try:
        runners = gitlab_get_runners()

        for runner in runners:
            if container.name in runner.get('description', ''):
                return runner['id']

    except Exception as e:
        logger.error(f'Failed to find runner in GitLab', extra={
            'container': container.name,
            'error': str(e)
        })
    return None


def cleanup_stale_gitlab_runners():
    """
    Remove stale GitLab runners that are registered but their containers no longer exist.
    """
    logger.info('[CLEANUP] Searching for stale runners in GitLab...')
    try:
        live_containers = set()
        for c in docker_client.containers.list(filters={'name': 'autoscale-runner-'}):
            live_containers.add(c.name)

        runners = gitlab_get_runners()
        stale_count = 0

        for runner in runners:
            desc = runner.get('description', '')
            if 'autoscale-runner-' in desc:
                container_name = None
                for part in desc.split():
                    if part.startswith('autoscale-runner-'):
                        container_name = part
                        break

                if container_name and container_name not in live_containers:
                    logger.warning(f'[CLEANUP] Stale runner: {container_name} (id={runner["id"]})', extra={
                        'description': desc,
                        'runner_id': runner['id']
                    })
                    gitlab_deregister_runner(runner['id'])
                    stale_count += 1

        logger.info(f'[CLEANUP] Completed: removed {stale_count} stale runners', extra={
            'stale_count': stale_count
        })

    except Exception as e:
        logger.error(f'[CLEANUP] Error: {e}', extra={'error': str(e)})


def cleanup_stopped_runners():
    """
    Remove stopped runner containers that have been stopped for longer than GRACEFUL_PERIOD.
    """
    try:
        stopped = get_stopped_runners()
        now = datetime.now()
        removed_count = 0

        for container in stopped:
            # Get container stop time
            state = container.attrs.get('State', {})
            stopped_at_str = state.get('StoppedAt', '')

            if stopped_at_str:
                try:
                    # Parse ISO format timestamp
                    # Docker возвращает формат: "2024-01-15T10:30:00.123456789Z"
                    # Обрезаем наносекунды до микросекунд и убираем Z
                    stopped_at_clean = stopped_at_str
                    if '.' in stopped_at_clean:
                        date_part, frac = stopped_at_clean.split('.', 1)
                        frac = frac.rstrip('Z')[:6]  # max 6 цифр для микросекунд
                        stopped_at_clean = f'{date_part}.{frac}'
                    else:
                        stopped_at_clean = stopped_at_clean.rstrip('Z')
                    stopped_at = datetime.fromisoformat(stopped_at_clean)
                    elapsed = (now - stopped_at).total_seconds()

                    if elapsed > GRACEFUL_PERIOD:
                        # Remove from GitLab first
                        runner_id = get_runner_id_by_container(container)
                        if runner_id:
                            gitlab_deregister_runner(runner_id)

                        # Remove container
                        container.remove()
                        removed_count += 1

                        logger.info(f'[CLEANUP] Removed stopped runner: {container.name}', extra={
                            'container': container.name,
                            'stopped_duration_seconds': elapsed
                        })
                except Exception as e:
                    logger.warning(f'Failed to parse stop time for {container.name}: {e}')
            else:
                # No stop time, just remove
                container.remove()
                removed_count += 1

        if removed_count > 0:
            logger.info(f'[CLEANUP] Removed {removed_count} stopped runner(s)', extra={
                'removed_count': removed_count
            })

    except Exception as e:
        logger.error(f'[CLEANUP] Error removing stopped runners: {e}', extra={'error': str(e)})


def stop_runner():
    """
    Stop and remove an idle runner with graceful shutdown.
    """
    global last_scale_down_time

    try:
        containers = docker_client.containers.list(
            filters={'name': 'autoscale-runner-', 'status': 'running'}
        )
        if not containers:
            return False

        sorted_containers = sorted(containers, key=lambda c: c.attrs['Created'])

        to_remove = None
        runner_id = None
        for container in sorted_containers:
            active_jobs = get_runner_active_jobs(container)
            if active_jobs == 0:
                to_remove = container
                runner_id = get_runner_id_by_container(container)
                break

        if to_remove is None:
            logger.info('[SCALE DOWN] Skip: all runners are busy', extra={
                'reason': 'all_busy'
            })
            metrics_scale_down_blocked_jobs.inc()
            return False

        # Deregister from GitLab
        if runner_id is not None:
            gitlab_deregister_runner(runner_id)
        else:
            logger.warning(f'[SCALE DOWN] Runner not found in GitLab: {to_remove.name}', extra={
                'container': to_remove.name
            })
            try:
                exit_code, output = to_remove.exec_run(
                    'gitlab-runner unregister --all-runners',
                    demux=True
                )
                if exit_code == 0:
                    logger.info(f'[SCALE DOWN] Unregister via exec successful', extra={'container': to_remove.name})
            except Exception as e:
                logger.error(f'[SCALE DOWN] Exec unregister failed: {e}', extra={'error': str(e)})

        # Stop container
        to_remove.stop(timeout=GRACEFUL_PERIOD)
        volume_name = f'{to_remove.name}-config'
        to_remove.remove()

        # Remove volume
        try:
            docker_client.volumes.get(volume_name).remove()
        except Exception:
            pass

        logger.info(f'[SCALE DOWN] Runner stopped: {to_remove.name}', extra={
            'action': 'scale_down',
            'container': to_remove.name
        })

        last_scale_down_time = datetime.now()
        metrics_scale_down_total.inc()
        return True

    except Exception as e:
        logger.error(f'[SCALE DOWN] Stop error: {e}', extra={'error': str(e)})
        return False


def ensure_min_runners():
    """Ensure minimum runners are running at startup"""
    running = get_running_runners()
    stopped = get_stopped_runners()

    # Сначала чистим stopped контейнеры от предыдущего запуска
    if stopped:
        logger.info(f'[RECOVERY] Cleaning {len(stopped)} stopped runner(s) before recovery', extra={
            'stopped_runners': [c.name for c in stopped],
        })
        for container in stopped:
            try:
                runner_id = get_runner_id_by_container(container)
                if runner_id:
                    gitlab_deregister_runner(runner_id)
                volume_name = f'{container.name}-config'
                container.remove()
                try:
                    docker_client.volumes.get(volume_name).remove()
                except Exception:
                    pass
            except Exception as e:
                logger.warning(f'[RECOVERY] Failed to clean stopped runner {container.name}: {e}')

    if running < MIN_RUNNERS:
        needed = MIN_RUNNERS - running
        logger.info(f'[RECOVERY] Starting runners: need {needed} (current={running}, min={MIN_RUNNERS})', extra={
            'needed': needed,
            'current': running,
            'min': MIN_RUNNERS
        })
        for _ in range(needed):
            if get_running_runners() >= MIN_RUNNERS:
                break
            start_runner()
            time.sleep(2)



def get_total_runner_capacity():
    """
    Calculate total runner capacity (how many jobs can run in parallel).
    """
    try:
        containers = docker_client.containers.list(
            filters={'name': 'autoscale-runner-', 'status': 'running'}
        )
        total_capacity = 0
        for container in containers:
            profile = get_runner_profile(container)
            profile_config = RUNNER_PROFILES.get(profile, RUNNER_PROFILES['medium'])
            total_capacity += profile_config['concurrent']
        return total_capacity
    except Exception as e:
        logger.error(f'Failed to calculate runner capacity', extra={'error': str(e)})
        return 0


def update_metrics():
    """Update Prometheus metrics"""
    global current_pending_jobs, current_running_runners, current_running_jobs

    metrics_pending_jobs.set(current_pending_jobs)
    metrics_running_jobs.set(current_running_jobs)
    metrics_running_runners.set(current_running_runners)

    count_runners_by_profile()
    for profile, count in runners_by_profile.items():
        metrics_running_runners_by_profile.labels(profile=profile).set(count)

    # Uptime
    uptime = (datetime.now() - startup_time).total_seconds()
    metrics_uptime_seconds.set(uptime)


# =============================================================================
# Main loop
# =============================================================================

def main():
    global last_scale_up_time, last_scale_down_time
    global current_pending_jobs, current_running_runners, current_running_jobs
    global last_gitlab_check

    logger.info('[STARTUP] Starting GitLab Runner Autoscaler', extra={
        'gitlab_url': GITLAB_URL,
        'min_runners': MIN_RUNNERS,
        'max_runners': MAX_RUNNERS,
        'check_interval': CHECK_INTERVAL,
    })

    # Validate configuration
    try:
        validate_config()
    except ConfigValidationError as e:
        logger.error(f'[STARTUP] Configuration error: {e}')
        sys.exit(1)

    # Check Docker connection
    if docker_client is None:
        logger.error('[STARTUP] Docker client not initialized')
        sys.exit(1)

    try:
        validate_docker_connection(docker_client)
    except ConfigValidationError as e:
        logger.error(f'[STARTUP] Docker connection error: {e}')
        sys.exit(1)

    # Check GitLab connection
    try:
        validate_gitlab_connection()
        last_gitlab_check = datetime.now()
    except ConfigValidationError as e:
        logger.error(f'[STARTUP] GitLab connection error: {e}')
        sys.exit(1)

    # Start metrics server
    metrics_port = int(os.getenv('METRICS_PORT', '8000'))
    start_metrics_server(metrics_port)

    # Cleanup stale runners
    cleanup_stale_gitlab_runners()

    # Cleanup stopped runners
    cleanup_stopped_runners()

    # Ensure minimum runners
    ensure_min_runners()

    logger.info('[STARTUP] Autoscaler ready')

    # Main loop
    while True:
        try:
            pending, jobs_running = get_queue_stats()
            running = get_running_runners()
            capacity = get_total_runner_capacity()

            current_pending_jobs = pending
            current_running_runners = running
            current_running_jobs = jobs_running
            last_gitlab_check = datetime.now()

            logger.info(f'[QUEUE] Queue: {pending} pending, {jobs_running} running | Runners: {running}/{capacity} (min={MIN_RUNNERS}, max={MAX_RUNNERS})')

            update_metrics()

            # Periodic cleanup of stopped runners
            cleanup_stopped_runners()

            # Recovery to MIN_RUNNERS
            if running < MIN_RUNNERS:
                stopped = get_stopped_runners()
                if stopped:
                    # cleanup_stopped_runners() уже запущен выше, просто пропускаем цикл
                    logger.info(f'[RECOVERY] Skipping recovery: {len(stopped)} stopped runner(s) exist', extra={
                        'stopped_runners': [c.name for c in stopped],
                        'running': running,
                        'min': MIN_RUNNERS
                    })
                else:
                    logger.info(f'[RECOVERY] Recovering runners: current={running}, min={MIN_RUNNERS}')
                    start_runner()
                    time.sleep(2)
                    update_metrics()
                    continue  # пропускаем scale up/down — раннер только что стартовал

            # Scale UP
            total_demand = pending + jobs_running
            if pending > 0 and total_demand > capacity and running < MAX_RUNNERS:
                if can_scale_up():
                    logger.info(f'[SCALE UP] Starting runner: demand={total_demand}, capacity={capacity}')
                    start_runner()
                    update_metrics()
                else:
                    logger.info('[SCALE UP] Deferred (cooldown or resources)')

            # Scale DOWN
            elif pending == 0 and jobs_running < capacity and running > MIN_RUNNERS:
                if can_scale_down():
                    logger.info('[SCALE DOWN] Stopping runner: queue empty')
                    stop_runner()
                    update_metrics()
                else:
                    logger.info('[SCALE DOWN] Deferred (cooldown or active jobs)')

        except Exception as e:
            logger.error(f'[ERROR] Main loop error: {e}', extra={'error': str(e)})

        time.sleep(CHECK_INTERVAL)


if __name__ == '__main__':
    main()