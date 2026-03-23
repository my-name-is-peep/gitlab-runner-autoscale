#!/usr/bin/env python3
import os
import time
import docker
import requests
import logging

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
MIN_RUNNERS = int(os.getenv('MIN_RUNNERS', '1'))  # 👈 Минимум всегда запущенных
CHECK_INTERVAL = int(os.getenv('CHECK_INTERVAL', '30'))
RUNNER_IMAGE = os.getenv('RUNNER_IMAGE', 'gitlab/gitlab-runner:latest')
RUNNER_CONCURRENT = int(os.getenv('RUNNER_CONCURRENT', '2'))
RUNNER_TAGS = os.getenv('RUNNER_TAGS', 'autoscale')
PROJECTS_LIMIT = int(os.getenv('PROJECTS_LIMIT', '20'))

# Валидация
if MIN_RUNNERS > MAX_RUNNERS:
    logger.warning(f'⚠️ MIN_RUNNERS ({MIN_RUNNERS}) > MAX_RUNNERS ({MAX_RUNNERS}), устанавливаем MIN=MAX')
    MIN_RUNNERS = MAX_RUNNERS

docker_client = docker.from_env()

def get_pending_jobs():
    """Считаем pending jobs по всем доступным проектам"""
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
        for project in projects:
            try:
                jobs_resp = requests.get(
                    f'{GITLAB_URL}/api/v4/projects/{project["id"]}/jobs',
                    params={'scope': 'pending', 'per_page': 1},
                    headers={'PRIVATE-TOKEN': GITLAB_TOKEN},
                    timeout=5
                )
                if jobs_resp.status_code == 200:
                    total_pending += len(jobs_resp.json())
            except:
                continue
        return total_pending
    except Exception as e:
        logger.error(f'❌ Ошибка GitLab API: {e}')
        return 0

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

def start_runner():
    """Запускаем новый контейнер-раннер"""
    name = f'autoscale-runner-{int(time.time())}'
    try:
        # Шаг 1: Регистрируем раннер (временный контейнер)
        register_result = docker_client.containers.run(
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
                '--tag-list', RUNNER_TAGS,
                '--concurrent', str(RUNNER_CONCURRENT),
                '--description', f'Auto {name}',
                '--run-untagged', 'false'
            ],
            remove=True,
            detach=False  # Ждём завершения регистрации
        )
        
        # Шаг 2: Запускаем постоянный контейнер с тем же томом конфигурации
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
            labels={'autoscale-runner': 'true'}
        )
        
        logger.info(f'🚀 Запущен: {name}')
        return True
        
    except Exception as e:
        logger.error(f'❌ Ошибка запуска раннера: {e}')
        # Чистим временный контейнер если остался
        try:
            docker_client.containers.get(f'{name}-register').remove(force=True)
        except:
            pass
        return False

def stop_runner():
    """Останавливаем и удаляем лишнего раннера"""
    try:
        containers = docker_client.containers.list(
            filters={'name': 'autoscale-runner-', 'status': 'running'}
        )
        if not containers:
            return False
        
        # Берём самый старый
        to_remove = sorted(containers, key=lambda c: c.created)[0]
        
        # Пытаемся дерегистрировать
        try:
            to_remove.exec_run('gitlab-runner unregister --all-runners')
        except:
            pass
        
        to_remove.stop(timeout=10)
        to_remove.remove()
        logger.info(f'🛑 Остановлен: {to_remove.name}')
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
            time.sleep(2)  # Небольшая пауза между запусками

def main():
    logger.info('🎯 Автоскейлер запущен')
    logger.info(f'📊 GitLab: {GITLAB_URL} | Min: {MIN_RUNNERS} | Max: {MAX_RUNNERS} | Interval: {CHECK_INTERVAL}s')
    
    # 🔥 Сразу обеспечиваем минимум раннеров
    ensure_min_runners()
    
    while True:
        pending = get_pending_jobs()
        running = get_running_runners()
        
        logger.info(f'📈 Очередь: {pending} | Раннеры: {running} (min: {MIN_RUNNERS}, max: {MAX_RUNNERS})')
        
        # Scale UP: если очередь больше раннеров И не достигли максимума
        if pending > running and running < MAX_RUNNERS:
            logger.info('⬆️ Масштабирование вверх')
            start_runner()
        
        # Scale DOWN: если очередь пуста И раннеров больше минимума
        elif pending == 0 and running > MIN_RUNNERS:
            logger.info('⬇️ Масштабирование вниз')
            stop_runner()
        
        time.sleep(CHECK_INTERVAL)

if __name__ == '__main__':
    main()