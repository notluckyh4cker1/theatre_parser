import json
import time
import hashlib
import pickle
from datetime import datetime, timedelta
import redis
import config
from functools import wraps

class RedisCache:
    def __init__(self):
        self.client = None
        self.prefix = config.CACHE_CONFIG['prefix']
        self.ttl = config.CACHE_CONFIG['ttl']
        self.enabled = config.CACHE_CONFIG['enabled']
        self._connect()

    def _connect(self):
        """Подключается к Redis"""
        try:
            self.client = redis.Redis(
                host=config.REDIS_CONFIG['host'],
                port=config.REDIS_CONFIG['port'],
                db=config.REDIS_CONFIG['db'],
                password=config.REDIS_CONFIG.get('password'),
                socket_timeout=config.REDIS_CONFIG['socket_timeout'],
                socket_connect_timeout=config.REDIS_CONFIG['socket_connect_timeout'],
                retry_on_timeout=config.REDIS_CONFIG['retry_on_timeout'],
                decode_responses=False
            )

            # Проверяем подключение
            self.client.ping()
            print(f"Успешное подключение к Redis")
            return True

        except Exception as e:
            print(f"Ошибка подключения к Redis: {e}")
            print("Продолжаем без кеширования")
            self.enabled = False
            return False

    def _generate_key(self, query_name, params=None):
        """Генерирует ключ для кеша"""
        if params:
            # Создаем строку из параметров
            param_str = json.dumps(params, sort_keys=True)
            # Хешируем чтобы ключ не был слишком длинным
            param_hash = hashlib.md5(param_str.encode()).hexdigest()[:8]
            key = f"{self.prefix}{query_name}:{param_hash}"
        else:
            key = f"{self.prefix}{query_name}"
        return key

    def get(self, query_name, params=None):
        """Получает данные из кеша"""
        if not self.enabled or not self.client:
            return None

        try:
            key = self._generate_key(query_name, params)
            data = self.client.get(key)

            if data:
                # Десериализуем данные
                result = pickle.loads(data)
                print(f"[CACHE HIT] Получены данные из кеша: {query_name}")
                return result
            else:
                print(f"[CACHE MISS] Данных нет в кеше: {query_name}")
                return None

        except Exception as e:
            print(f"Ошибка при чтении из кеша: {e}")
            return None

    def set(self, query_name, data, params=None, ttl=None):
        """Сохраняет данные в кеш"""
        if not self.enabled or not self.client:
            return False

        try:
            key = self._generate_key(query_name, params)

            serialized_data = pickle.dumps(data)

            # Устанавливаем TTL
            expire_time = ttl if ttl is not None else self.ttl

            result = self.client.setex(key, expire_time, serialized_data)

            if result:
                print(f"[CACHE SET] Данные сохранены в кеш: {query_name} (TTL: {expire_time} сек)")
                return True
            else:
                print(f"Ошибка сохранения в кеш: {query_name}")
                return False

        except Exception as e:
            print(f"Ошибка при сохранении в кеш: {e}")
            return False

    def delete(self, query_name, params=None):
        """Удаляет данные из кеша"""
        if not self.enabled or not self.client:
            return False

        try:
            key = self._generate_key(query_name, params)
            result = self.client.delete(key)

            if result > 0:
                print(f"[CACHE DEL] Данные удалены из кеша: {query_name}")
                return True
            else:
                print(f"Данных нет в кеше для удаления: {query_name}")
                return False

        except Exception as e:
            print(f"Ошибка при удалении из кеша: {e}")
            return False

    def clear_all(self):
        """Очищает весь кеш"""
        if not self.enabled or not self.client:
            return False

        try:
            pattern = f"{self.prefix}*"
            keys = self.client.keys(pattern)

            if keys:
                self.client.delete(*keys)
                print(f"[CACHE CLEAR] Очищено {len(keys)} ключей")
                return True
            else:
                print("Кеш уже пуст")
                return True

        except Exception as e:
            print(f"Ошибка при очистке кеша: {e}")
            return False

    def get_stats(self):
        """Получает статистику по кешу"""
        if not self.enabled or not self.client:
            return {}

        try:
            pattern = f"{self.prefix}*"
            keys = self.client.keys(pattern)

            stats = {
                'total_keys': len(keys),
                'memory_usage': 0,
                'keys_by_type': {}
            }

            for key in keys[:100]:
                key_str = key.decode('utf-8')
                parts = key_str.replace(self.prefix, '').split(':')
                if parts:
                    query_type = parts[0]
                    stats['keys_by_type'][query_type] = stats['keys_by_type'].get(query_type, 0) + 1

            return stats

        except Exception as e:
            print(f"Ошибка при получении статистики кеша: {e}")
            return {}

    def close(self):
        """Закрывает соединение с Redis"""
        if self.client:
            self.client.close()
            print("Соединение с Redis закрыто")


def cache_query(query_name, ttl=None):
    """Декоратор для кеширования результатов функций"""

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            cache = RedisCache()

            # Если кеш не доступен, просто выполняем функцию
            if not cache.enabled:
                return func(*args, **kwargs)

            # Формируем параметры для ключа
            params = {
                'args': args,
                'kwargs': kwargs
            }

            cached_result = cache.get(query_name, params)

            if cached_result is not None:
                cache.close()
                return cached_result
            else:
                # Выполняем функцию
                start_time = time.time()
                result = func(*args, **kwargs)
                execution_time = time.time() - start_time

                # Сохраняем результат в кеш
                if result is not None:
                    cache.set(query_name, result, params, ttl)
                    print(f"Время выполнения запроса: {execution_time:.3f} сек")

                cache.close()
                return result

        return wrapper

    return decorator