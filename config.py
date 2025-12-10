import os
from datetime import datetime

# Пути к файлам
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, 'data')
URLS_FILE = os.path.join(DATA_DIR, 'play_urls.txt')
JSON_FILE = os.path.join(DATA_DIR, 'plays.json')

# Настройки парсера
BASE_URL = "https://msk.kassir.ru"
THEATER_URL = f"{BASE_URL}/bilety-v-teatr"
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
}

# Настройки задержек
REQUEST_DELAY = 3
TIMEOUT = 10

# Настройки MongoDB
MONGO_CONFIG = {
    'host': 'localhost',
    'port': 27017,
    'database': 'theater_db',
    'collection': 'plays'
}

# Настройки Redis для кеширования
REDIS_CONFIG = {
    'host': 'localhost',
    'port': 6379,
    'db': 0,
    'password': None,
    'socket_timeout': 5,
    'socket_connect_timeout': 5,
    'retry_on_timeout': True,
}

# Настройки кеширования
CACHE_CONFIG = {
    'ttl': 3600,  # Время жизни кеша в секундах
    'prefix': 'theater:',
    'enabled': True,
}

# Сложные запросы для кеширования
COMPLEX_QUERIES = [
    'theatre_stats',
    'genre_stats',
    'upcoming_shows',
    'top_actors',
    'popular_directors',
    'date_distribution'
]

PARSING_CONFIG = {
    'min_duration': 30,
    'max_duration': 300,
    'min_actors': 1,
    'expected_fields': ['name', 'theatre', 'dates', 'duration_minutes']
}

# Метаданные
METADATA = {
    'source': 'kassir.ru',
    'collection_date': datetime.now().isoformat(),
    'version': '1.0'
}