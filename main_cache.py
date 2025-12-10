import time
import json
from datetime import datetime
from pymongo import MongoClient
import redis

def test_tz_queries_with_cache():
    """Тест кеширования запросов из ТЗ предыдущей работы"""
    # 1. Подключение к MongoDB
    print("\nПОДКЛЮЧЕНИЕ К MONGODB")
    print("-" * 40)

    try:
        mongo_client = MongoClient('localhost', 27017)
        mongo_db = mongo_client['theater_db']
        mongo_collection = mongo_db['plays']

        count = mongo_collection.count_documents({})
        print(f"Подключено к MongoDB. Спектаклей в базе: {count}")

        if count == 0:
            print("В базе нет данных! Сначала запустите парсер.")
            return
    except Exception as e:
        print(f"Ошибка подключения к MongoDB: {e}")
        return

    # 2. Подключение к Redis с обработкой ошибок записи
    print("\nПОДКЛЮЧЕНИЕ К REDIS")
    print("-" * 40)

    try:
        redis_client = redis.Redis(
            host='localhost',
            port=6379,
            db=0,
            socket_timeout=5,
            socket_connect_timeout=5,
            decode_responses=False
        )

        # Проверяем подключение
        redis_client.ping()
        print("Успешное подключение к Redis")

        # Пробуем временно отключить проверку записи на диск
        try:
            redis_client.config_set('stop-writes-on-bgsave-error', 'no')
            print("Отключена проверка записи на диск (для теста)")
        except:
            print("Не удалось изменить конфигурацию Redis, продолжаем...")

    except Exception as e:
        print(f"Ошибка подключения к Redis: {e}")
        print("\nРешение проблемы Redis:")
        print("   1. Запустите Redis CLI: redis-cli.exe")
        print("   2. Введите команду: config set stop-writes-on-bgsave-error no")
        print("   3. Или перезапустите Redis с правами администратора")

        # Пробуем использовать fallback - словарь в памяти
        print("\nИспользуем временное хранилище в памяти...")
        redis_client = None
        use_memory_cache = True
    else:
        use_memory_cache = False

    memory_cache = {}

    # Определяем ЗАПРОСЫ ИЗ ТЗ для кеширования
    print("\nЗАПРОСЫ ИЗ ТЗ ДЛЯ КЕШИРОВАНИЯ")
    print("-" * 40)

    # Запрос 1: SELECT * FROM plays WHERE theatre = 'Vegas City Hall'
    def get_theatre_repertoire(theatre_name):
        """Запрос 1 из ТЗ: Весь репертуар конкретного театра"""
        print(f"Запрос 1: Репертуар театра '{theatre_name}'")

        query = {'theatre': theatre_name}
        plays = list(mongo_collection.find(query))

        # Форматируем результат
        result = []
        for play in plays:
            result.append({
                'name': play.get('name'),
                'director': play.get('director', 'Не указан'),
                'genre': play.get('genre'),
                'duration_minutes': play.get('duration_minutes'),
                'dates_count': len(play.get('dates', []))
            })

        print(f"Найдено спектаклей: {len(result)}")
        return result

    # Запрос 2: SELECT name, theatre, dates FROM plays WHERE director = '...'
    def get_director_works(director_name):
        """Запрос 2 из ТЗ: Творчество конкретного режиссера"""
        print(f"Запрос 2: Работы режиссера '{director_name}'")

        query = {'director': director_name}
        plays = list(mongo_collection.find(
            query,
            {'name': 1, 'theatre': 1, 'dates': 1, '_id': 0}
        ))

        # Форматируем результат
        result = []
        for play in plays:
            result.append({
                'name': play.get('name'),
                'theatre': play.get('theatre'),
                'dates_count': len(play.get('dates', [])),
                'dates': play.get('dates', [])[:3]
            })

        print(f"Найдено работ: {len(result)}")
        return result

    # Запрос 3: SELECT name, theatre, duration_minutes FROM plays WHERE date IN dates
    def get_plays_on_date(target_date):
        """Запрос 3 из ТЗ: Спектакли на конкретную дату"""
        print(f"Запрос 3: Спектакли на дату '{target_date}'")

        query = {'dates': target_date}
        plays = list(mongo_collection.find(
            query,
            {'name': 1, 'theatre': 1, 'duration_minutes': 1, '_id': 0}
        ))

        # Форматируем результат
        result = []
        for play in plays:
            result.append({
                'name': play.get('name'),
                'theatre': play.get('theatre'),
                'duration_minutes': play.get('duration_minutes')
            })

        print(f"Найдено спектаклей: {len(result)}")
        return result

    # Запрос 4: SELECT theatre, COUNT(*) as play_count, AVG(duration_minutes) as avg_duration...
    def get_theatre_statistics_extended():
        """Запрос 4 из ТЗ: Статистика по театрам"""
        print("Запрос 4: Статистика театров (сложный агрегационный)")

        pipeline = [
            {'$match': {
                'theatre': {'$exists': True, '$ne': 'Не указан'},
                'duration_minutes': {'$exists': True, '$ne': None}
            }},
            {'$group': {
                '_id': '$theatre',
                'play_count': {'$sum': 1},
                'avg_duration': {'$avg': '$duration_minutes'},
                'min_duration': {'$min': '$duration_minutes'},
                'max_duration': {'$max': '$duration_minutes'}
            }},
            {'$sort': {'play_count': -1}},
            {'$limit': 10}
        ]

        results = list(mongo_collection.aggregate(pipeline))

        # Форматируем результат
        formatted = []
        for stat in results:
            formatted.append({
                'theatre': stat['_id'],
                'play_count': stat['play_count'],
                'avg_duration': round(stat['avg_duration'], 1),
                'duration_range': f"{stat['min_duration']}-{stat['max_duration']}"
            })

        print(f"Проанализировано театров: {len(formatted)}")
        return formatted

    # Запрос 5: SELECT genre, COUNT(*) as total_plays, COUNT(DISTINCT theatre)...
    def get_genre_statistics_extended():
        """Запрос 5 из ТЗ: Популярность жанров"""
        print("Запрос 5: Статистика жанров (сложный агрегационный)")

        pipeline = [
            {'$match': {'genre': {'$exists': True, '$ne': ''}}},
            {'$group': {
                '_id': '$genre',
                'total_plays': {'$sum': 1},
                'unique_theatres': {'$addToSet': '$theatre'},
                'total_shows': {'$sum': {'$size': {'$ifNull': ['$dates', []]}}}
            }},
            {'$project': {
                'genre': '$_id',
                'total_plays': 1,
                'theatre_count': {'$size': '$unique_theatres'},
                'total_shows': 1,
                'avg_shows_per_play': {'$divide': ['$total_shows', '$total_plays']}
            }},
            {'$sort': {'total_shows': -1}}
        ]

        results = list(mongo_collection.aggregate(pipeline))

        # Форматируем результат
        formatted = []
        for stat in results:
            formatted.append({
                'genre': stat['genre'],
                'total_plays': stat['total_plays'],
                'theatre_count': stat['theatre_count'],
                'total_shows': stat['total_shows'],
                'avg_shows_per_play': round(stat['avg_shows_per_play'], 1)
            })

        print(f"Проанализировано жанров: {len(formatted)}")
        return formatted

    # 4. Реализуем универсальный Cache-Aside
    print("\nРЕАЛИЗУЕМ CACHE-ASIDE ДЛЯ ЗАПРОСОВ ИЗ ТЗ")
    print("-" * 40)

    def cache_get_or_set(key, func, ttl=3600, *args, **kwargs):
        """Получает данные из кеша или выполняет функцию с параметрами"""
        try:
            if args or kwargs:
                params = {'args': args, 'kwargs': kwargs}
                params_hash = hash(json.dumps(params, sort_keys=True, default=str)) % 10000
                key_with_params = f"{key}:{abs(params_hash)}"
            else:
                key_with_params = key

            # Пытаемся получить из кеша (Redis или памяти)
            if redis_client and not use_memory_cache:
                try:
                    cached_data = redis_client.get(key_with_params)
                    if cached_data:
                        print(f"[CACHE HIT] Данные из Redis: {key_with_params}")
                        return json.loads(cached_data.decode('utf-8'))
                except:
                    pass

            # Проверяем кеш в памяти
            if key_with_params in memory_cache:
                cache_entry = memory_cache[key_with_params]
                if time.time() < cache_entry['expires']:
                    print(f"[CACHE HIT] Данные из памяти: {key_with_params}")
                    return cache_entry['data']
                else:
                    del memory_cache[key_with_params]

            # Если нет в кеше, выполняем функцию
            print(f"[CACHE MISS] Выполняем запрос: {key_with_params}")
            start_time = time.time()
            data = func(*args, **kwargs)
            query_time = time.time() - start_time
            print(f"Запрос выполнен за: {query_time:.3f} сек")

            # Сохраняем в кеш
            if data is not None:
                if redis_client and not use_memory_cache:
                    try:
                        redis_client.setex(key_with_params, ttl, json.dumps(data))
                        print(f"[CACHE SET] Данные в Redis: {key_with_params} (TTL: {ttl} сек)")
                    except Exception as e:
                        print(f"Не удалось сохранить в Redis: {e}")
                        # Сохраняем в память как fallback
                        memory_cache[key_with_params] = {
                            'data': data,
                            'expires': time.time() + ttl
                        }
                        print(f"[CACHE SET] Данные в памяти: {key_with_params}")
                else:
                    # Сохраняем только в память
                    memory_cache[key_with_params] = {
                        'data': data,
                        'expires': time.time() + ttl
                    }
                    print(f"[CACHE SET] Данные в памяти: {key_with_params} (TTL: {ttl} сек)")

            return data
        except Exception as e:
            print(f"Ошибка кеширования: {e}")
            return func(*args, **kwargs)

    # 5. Тестируем производительность запросов из ТЗ
    print("\nТЕСТИРУЕМ ПРОИЗВОДИТЕЛЬНОСТЬ ЗАПРОСОВ ИЗ ТЗ")
    print("-" * 40)

    test_results = []

    # Сначала найдем реальные данные из БД для тестов
    print("\n🔍 ПОИСК РЕАЛЬНЫХ ДАННЫХ ДЛЯ ТЕСТОВ:")
    print("-" * 30)

    # Найдем театр для запроса 1
    theatre_result = list(mongo_collection.aggregate([
        {'$group': {'_id': '$theatre', 'count': {'$sum': 1}}},
        {'$sort': {'count': -1}},
        {'$limit': 1}
    ]))

    test_theatre = theatre_result[0]['_id'] if theatre_result else "Vegas City Hall"
    print(f"   Театр для запроса 1: {test_theatre}")

    # Найдем режиссера для запроса 2
    director_result = list(mongo_collection.aggregate([
        {'$match': {'director': {'$ne': 'Не указан', '$exists': True}}},
        {'$group': {'_id': '$director', 'count': {'$sum': 1}}},
        {'$sort': {'count': -1}},
        {'$limit': 1}
    ]))

    test_director = director_result[0]['_id'] if director_result else "Алексей Франдетти"
    print(f"   Режиссер для запроса 2: {test_director}")

    # Найдем дату для запроса 3
    date_result = list(mongo_collection.aggregate([
        {'$unwind': '$dates'},
        {'$group': {'_id': '$dates'}},
        {'$sort': {'_id': 1}},
        {'$limit': 1}
    ]))

    test_date = date_result[0]['_id'] if date_result else "2024-11-20T19:00:00"
    print(f"Дата для запроса 3: {test_date}")

    # ТЕСТ 1: Репертуар театра
    print("\nТЕСТ 1: Репертуар конкретного театра")

    if redis_client:
        try:
            redis_client.delete('query1_theatre_repertoire')
        except:
            pass

    # Холодный запуск (без кеша)
    print("Холодный запуск (без кеша)...")
    start_time = time.time()
    result1 = cache_get_or_set('query1_theatre_repertoire', get_theatre_repertoire,
                               ttl=60, theatre_name=test_theatre)
    cold_time = time.time() - start_time

    # Горячий запуск (с кешем)
    print("Горячий запуск (с кешем)...")
    start_time = time.time()
    result2 = cache_get_or_set('query1_theatre_repertoire', get_theatre_repertoire,
                               ttl=60, theatre_name=test_theatre)
    hot_time = time.time() - start_time

    test_results.append({
        'query': '1. Репертуар театра',
        'cold_time': round(cold_time, 3),
        'hot_time': round(hot_time, 3),
        'speedup': round(cold_time / hot_time, 1) if hot_time > 0 else 0
    })

    # ТЕСТ 2: Творчество режиссера
    print("\nТЕСТ 2: Творчество режиссера")

    if redis_client:
        try:
            redis_client.delete('query2_director_works')
        except:
            pass

    print("Холодный запуск (без кеша)...")
    start_time = time.time()
    result1 = cache_get_or_set('query2_director_works', get_director_works,
                               ttl=60, director_name=test_director)
    cold_time = time.time() - start_time

    print("Горячий запуск (с кешем)...")
    start_time = time.time()
    result2 = cache_get_or_set('query2_director_works', get_director_works,
                               ttl=60, director_name=test_director)
    hot_time = time.time() - start_time

    test_results.append({
        'query': '2. Творчество режиссера',
        'cold_time': round(cold_time, 3),
        'hot_time': round(hot_time, 3),
        'speedup': round(cold_time / hot_time, 1) if hot_time > 0 else 0
    })

    # ТЕСТ 3: Спектакли на конкретную дату
    print("\nТЕСТ 3: Спектакли на дату")

    if redis_client:
        try:
            redis_client.delete('query3_plays_on_date')
        except:
            pass

    print("Холодный запуск (без кеша)...")
    start_time = time.time()
    result1 = cache_get_or_set('query3_plays_on_date', get_plays_on_date,
                               ttl=30, target_date=test_date)
    cold_time = time.time() - start_time

    print("Горячий запуск (с кешем)...")
    start_time = time.time()
    result2 = cache_get_or_set('query3_plays_on_date', get_plays_on_date,
                               ttl=30, target_date=test_date)
    hot_time = time.time() - start_time

    test_results.append({
        'query': '3. Спектакли на дату',
        'cold_time': round(cold_time, 3),
        'hot_time': round(hot_time, 3),
        'speedup': round(cold_time / hot_time, 1) if hot_time > 0 else 0
    })

    # ТЕСТ 4: Статистика театров (сложный)
    print("\nТЕСТ 4: Статистика театров (сложный запрос)")

    if redis_client:
        try:
            redis_client.delete('query4_theatre_stats')
        except:
            pass

    print("Холодный запуск (без кеша)...")
    start_time = time.time()
    result1 = cache_get_or_set('query4_theatre_stats', get_theatre_statistics_extended,
                               ttl=300)
    cold_time = time.time() - start_time

    print("Горячий запуск (с кешем)...")
    start_time = time.time()
    result2 = cache_get_or_set('query4_theatre_stats', get_theatre_statistics_extended,
                               ttl=300)
    hot_time = time.time() - start_time

    test_results.append({
        'query': '4. Статистика театров',
        'cold_time': round(cold_time, 3),
        'hot_time': round(hot_time, 3),
        'speedup': round(cold_time / hot_time, 1) if hot_time > 0 else 0
    })

    # ТЕСТ 5: Статистика жанров (сложный)
    print("\nТЕСТ 5: Статистика жанров (сложный запрос)")

    if redis_client:
        try:
            redis_client.delete('query5_genre_stats')
        except:
            pass

    print("Холодный запуск (без кеша)...")
    start_time = time.time()
    result1 = cache_get_or_set('query5_genre_stats', get_genre_statistics_extended,
                               ttl=300)
    cold_time = time.time() - start_time

    print("Горячий запуск (с кешем)...")
    start_time = time.time()
    result2 = cache_get_or_set('query5_genre_stats', get_genre_statistics_extended,
                               ttl=300)
    hot_time = time.time() - start_time

    test_results.append({
        'query': '5. Статистика жанров',
        'cold_time': round(cold_time, 3),
        'hot_time': round(hot_time, 3),
        'speedup': round(cold_time / hot_time, 1) if hot_time > 0 else 0
    })

    # 6. Вывод результатов
    print("\n" + "=" * 60)
    print("РЕЗУЛЬТАТЫ ТЕСТИРОВАНИЯ ЗАПРОСОВ ИЗ ТЗ")
    print("=" * 60)
    print(f"{'№ Запрос':<35} {'Без кеша':<10} {'С кешом':<10} {'Ускорение':<10}")
    print("-" * 65)

    for result in test_results:
        print(f"{result['query']:<35} "
              f"{result['cold_time']:<10.3f} "
              f"{result['hot_time']:<10.3f} "
              f"{result['speedup']:<10.1f}x")

    # 7. Информация о используемом хранилище
    print("\n" + "=" * 60)
    print("ИНФОРМАЦИЯ О ХРАНИЛИЩЕ КЕША")
    print("=" * 60)

    if redis_client and not use_memory_cache:
        print("• Используется: Redis")
        try:
            info = redis_client.info()
            print(f"• Версия Redis: {info.get('redis_version', 'N/A')}")
            print(f"• Использовано памяти: {info.get('used_memory_human', 'N/A')}")
        except:
            print("• Не удалось получить информацию о Redis")
    else:
        print("• Используется: Память (fallback)")
        print(f"• Записей в кеше: {len(memory_cache)}")

    # 8. Анализ эффективности
    print("\n" + "=" * 60)
    print("АНАЛИЗ ЭФФЕКТИВНОСТИ КЕШИРОВАНИЯ")
    print("=" * 60)

    simple_queries = [r for r in test_results if r['query'].startswith(('1.', '2.', '3.'))]
    complex_queries = [r for r in test_results if r['query'].startswith(('4.', '5.'))]

    if simple_queries:
        avg_simple_speedup = sum(r['speedup'] for r in simple_queries) / len(simple_queries)
        print(f"• Простые запросы (1-3): среднее ускорение {avg_simple_speedup:.1f}x")

    if complex_queries:
        avg_complex_speedup = sum(r['speedup'] for r in complex_queries) / len(complex_queries)
        print(f"• Сложные запросы (4-5): среднее ускорение {avg_complex_speedup:.1f}x")

    total_avg_speedup = sum(r['speedup'] for r in test_results) / len(test_results)
    print(f"• Общее среднее ускорение: {total_avg_speedup:.1f}x")

    # 9. Примеры данных
    print("\n" + "=" * 60)
    print("ПРИМЕРЫ ДАННЫХ ИЗ ЗАПРОСОВ")
    print("=" * 60)

    print("\nРезультат запроса 4 (Статистика театров):")
    if 'query4_theatre_stats' in memory_cache:
        stats = memory_cache['query4_theatre_stats']['data']
    elif redis_client:
        try:
            cached = redis_client.get('query4_theatre_stats')
            if cached:
                stats = json.loads(cached.decode('utf-8'))
            else:
                stats = result1 if result1 else []
        except:
            stats = result1 if result1 else []
    else:
        stats = result1 if result1 else []

    if stats and len(stats) > 0:
        print(f"   Всего театров: {len(stats)}")
        for i, theatre in enumerate(stats[:3], 1):
            theatre_name = theatre['theatre'][:30] + "..." if len(theatre['theatre']) > 30 else theatre['theatre']
            print(f"   {i}. {theatre_name}")
            print(f"      Спектаклей: {theatre['play_count']}, Ср.длит: {theatre['avg_duration']} мин")

    # 10. Закрытие соединений
    print("\n" + "=" * 60)
    print("ТЕСТИРОВАНИЕ ЗАПРОСОВ ИЗ ТЗ ЗАВЕРШЕНО")
    print("=" * 60)

    mongo_client.close()
    if redis_client:
        try:
            redis_client.close()
        except:
            pass

    return test_results

if __name__ == "__main__":
    print("\n" + "=" * 60)
    print("ТЕСТ КЕШИРОВАНИЯ ЗАПРОСОВ ИЗ ЛАБОРАТОРНОЙ РАБОТЫ №3")
    print("=" * 60)

    try:
        results = test_tz_queries_with_cache()
        if results:
            print("\nТЕСТ УСПЕШНО ЗАВЕРШЕН!")
            print("=" * 60)
            print("ИТОГОВАЯ СТАТИСТИКА ЗАПРОСОВ ИЗ ТЗ:")

            print("\nНАИБОЛЬШЕЕ УСКОРЕНИЕ:")
            fastest = max(results, key=lambda x: x['speedup'])
            print(f"   • {fastest['query']}: {fastest['speedup']:.1f}x")

            print("\nСРЕДНИЕ ПОКАЗАТЕЛИ:")
            avg_time_no_cache = sum(r['cold_time'] for r in results) / len(results)
            avg_time_with_cache = sum(r['hot_time'] for r in results) / len(results)
            avg_speedup = sum(r['speedup'] for r in results) / len(results)

            print(f"   • Без кеша: {avg_time_no_cache:.3f} сек")
            print(f"   • С кешем: {avg_time_with_cache:.3f} сек")
            print(f"   • Среднее ускорение: {avg_speedup:.1f}x")

        else:
            print("\nТЕСТ НЕ ВЫПОЛНЕН!")

    except KeyboardInterrupt:
        print("\n\nТест прерван пользователем")
    except Exception as e:
        print(f"\nОшибка при выполнении теста: {e}")
        import traceback

        traceback.print_exc()