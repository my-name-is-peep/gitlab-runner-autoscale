#!/usr/bin/env python3
import os
import time
import docker
import requests
import logging
from datetime import datetime

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Конфиг из переменных окружения
GITLAB_URL = os.getenv('GITLAB_URL', 'https://gitlab.com')
GITLAB_TOKEN = os.getenv('GITLAB_TOKEN')
REG_TOKEN = os.getenv('REG_TOKEN')
MAX_RUNNERS = int(os.getenv('MAX_RUNNERS', '5'))
MIN_RUNNERS = int(os.getenv('MIN_RUNNERS', '1'))
CHECK_INTERVAL = int(os.getenv('CHECK_INTERVAL', '30'))
RUNNER_IMAGE = os.getenv('RUNNER_IMAGE', 'gitlab/gitlab-runner:latest')
RUNNER_CONCURRENT = int(os.getenv('RUNNER_CONCURRENT', '2'))
RUNNER_TAGS = os.getenv('RUNNER_TAGS', 'autoscale')

docker_client = docker.from_env()

def get_pending_jobs():
    """Считаем количество задач в очереди"""
    try:
        resp = requests.get(
            f'{GITLAB_URL}/api/v4/jobs',
            params={'scope': 'pending'},
            headers={'PRIVATE-TOKEN': GITLAB_TOKEN},
            timeout=10
        )
        resp.raise_for_status()
        return len(resp.json())
    except Exception as e:
        logger.error(f'Ошибка GitLab API: {e}')
        return 0

def get_running_runners():
    """Считаем запущенные контейнеры-раннеры"""
    try:
        containers = docker_client.containers.list(
            filters={'name': 'autoscale-runner-'}
        )
        return len(containers)
    except Exception as e:
        logger.error(f'Ошибка Docker API: {e}')
        return 0

def start_runner():
    """Запускаем новый контейнер-раннер"""
    name = f'autoscale-runner-{int(time.time())}'
    try:
        container = docker_client.containers.run(
            image=RUNNER_IMAGE,
            name=name,
            detach=True,
            volumes=[
                '/var/run/docker.sock:/var/run/docker.sock',
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
            remove=True,  # Контейнер регистрации удалится после выполнения
            labels={'autoscale-runner': 'true'}
        )
        logger.info(f'🚀 Запущен: {name}')
        return True
    except Exception as e:
        logger.error(f'Ошибка запуска раннера: {e}')
        return False

def stop_runner():
    """Останавливаем и удаляем лишнего раннера"""
    try:
        containers = docker_client.containers.list(
            filters={'name': 'autoscale-runner-'},
            filters={'status': 'running'}
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
        logger.error(f'Ошибка остановки раннера: {e}')
        return False

def main():
    logger.info('🎯 Автоскейлер запущен')
    logger.info(f'📊 Min: {MIN_RUNNERS} | Max: {MAX_RUNNERS} | Interval: {CHECK_INTERVAL}s')
    
    while True:
        pending = get_pending_jobs()
        running = get_running_runners()
        
        logger.info(f'Очередь: {pending} | Раннеры: {running}')
        
        # Scale UP
        if pending > running and running < MAX_RUNNERS:
            logger.info('⬆️ Масштабирование вверх')
            start_runner()
        
        # Scale DOWN
        elif pending == 0 and running > MIN_RUNNERS:
            logger.info('⬇️ Масштабирование вниз')
            stop_runner()
        
        time.sleep(CHECK_INTERVAL)

if __name__ == '__main__':
    main()