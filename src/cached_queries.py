from pymongo import MongoClient
import time
from datetime import datetime, timedelta
from src.redis_cache import RedisCache, cache_query

class CachedQueries:
    def __init__(self, db_name='theater_db', collection_name='plays'):
        self.client = MongoClient('localhost', 27017)
        self.db = self.client[db_name]
        self.collection = self.db[collection_name]
        self.cache = RedisCache()

    @cache_query('theatre_stats', ttl=1800)
    def get_theatre_statistics(self):
        """Статистика по театрам (с кешированием)"""
        print("Выполняем сложный запрос: статистика театров...")

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
                'max_duration': {'$max': '$duration_minutes'},
                'total_shows': {'$sum': {'$size': {'$ifNull': ['$dates', []]}}}
            }},
            {'$sort': {'play_count': -1}},
            {'$limit': 15}
        ]

        results = list(self.collection.aggregate(pipeline))

        # Форматируем результаты
        formatted = []
        for stat in results:
            formatted.append({
                'theatre': stat['_id'],
                'play_count': stat['play_count'],
                'avg_duration': round(stat['avg_duration'], 1),
                'duration_range': f"{stat['min_duration']}-{stat['max_duration']}",
                'total_shows': stat['total_shows']
            })

        return formatted

    @cache_query('genre_stats', ttl=1800)
    def get_genre_statistics(self):
        """Статистика по жанрам (с кешированием)"""
        print("Выполняем сложный запрос: статистика жанров...")

        pipeline = [
            {'$match': {'genre': {'$exists': True, '$ne': ''}}},
            {'$group': {
                '_id': '$genre',
                'total_plays': {'$sum': 1},
                'unique_theatres': {'$addToSet': '$theatre'},
                'total_shows': {'$sum': {'$size': {'$ifNull': ['$dates', []]}}},
                'avg_duration': {'$avg': '$duration_minutes'}
            }},
            {'$project': {
                'genre': '$_id',
                'total_plays': 1,
                'theatre_count': {'$size': '$unique_theatres'},
                'total_shows': 1,
                'avg_duration': 1,
                'avg_shows_per_play': {'$divide': ['$total_shows', '$total_plays']}
            }},
            {'$sort': {'total_shows': -1}}
        ]

        results = list(self.collection.aggregate(pipeline))

        formatted = []
        for stat in results:
            formatted.append({
                'genre': stat['genre'],
                'total_plays': stat['total_plays'],
                'theatre_count': stat['theatre_count'],
                'total_shows': stat['total_shows'],
                'avg_duration': round(stat.get('avg_duration', 0), 1) if stat.get('avg_duration') else None,
                'avg_shows_per_play': round(stat['avg_shows_per_play'], 1)
            })

        return formatted

    @cache_query('upcoming_shows', ttl=300)  # Кешируем на 5 минут (часто меняется)
    def get_upcoming_shows(self, days=7):
        """Предстоящие спектакли на N дней (с кешированием)"""
        print(f"Выполняем сложный запрос: предстоящие спектакли на {days} дней...")

        # Рассчитываем даты
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        future_date = today + timedelta(days=days)

        pipeline = [
            {'$unwind': '$dates'},
            {'$match': {
                'dates': {'$gte': today.isoformat(), '$lt': future_date.isoformat()}
            }},
            {'$group': {
                '_id': {'date': '$dates', 'play_id': '$_id'},
                'name': {'$first': '$name'},
                'theatre': {'$first': '$theatre'},
                'genre': {'$first': '$genre'},
                'duration': {'$first': '$duration_minutes'}
            }},
            {'$group': {
                '_id': '$_id.date',
                'plays': {'$push': {
                    'name': '$name',
                    'theatre': '$theatre',
                    'genre': '$genre',
                    'duration': '$duration'
                }}
            }},
            {'$sort': {'_id': 1}},
            {'$limit': 20}
        ]

        results = list(self.collection.aggregate(pipeline))

        formatted = []
        for day in results:
            formatted.append({
                'date': day['_id'],
                'play_count': len(day['plays']),
                'plays': day['plays'][:5]  # Ограничиваем количество для отображения
            })

        return formatted

    @cache_query('top_actors', ttl=3600)
    def get_top_actors(self, limit=10):
        """Самые популярные актеры (с кешированием)"""
        print("Выполняем сложный запрос: топ актеров...")

        pipeline = [
            {'$unwind': '$actors'},
            {'$match': {'actors': {'$exists': True, '$ne': ''}}},
            {'$group': {
                '_id': '$actors',
                'play_count': {'$sum': 1},
                'total_shows': {'$sum': {'$size': {'$ifNull': ['$dates', []]}}},
                'genres': {'$addToSet': '$genre'},
                'theatres': {'$addToSet': '$theatre'}
            }},
            {'$project': {
                'actor': '$_id',
                'play_count': 1,
                'total_shows': 1,
                'genre_count': {'$size': '$genres'},
                'theatre_count': {'$size': '$theatres'}
            }},
            {'$sort': {'play_count': -1}},
            {'$limit': limit}
        ]

        results = list(self.collection.aggregate(pipeline))

        return results

    @cache_query('date_distribution', ttl=7200)
    def get_date_distribution(self):
        """Распределение спектаклей по месяцам (с кешированием)"""
        print("Выполняем сложный запрос: распределение по датам...")

        pipeline = [
            {'$unwind': '$dates'},
            {'$addFields': {
                'date_obj': {'$dateFromString': {'dateString': '$dates'}}
            }},
            {'$group': {
                '_id': {
                    'year': {'$year': '$date_obj'},
                    'month': {'$month': '$date_obj'}
                },
                'show_count': {'$sum': 1},
                'unique_plays': {'$addToSet': '$_id'}
            }},
            {'$project': {
                'year': '$_id.year',
                'month': '$_id.month',
                'show_count': 1,
                'play_count': {'$size': '$unique_plays'}
            }},
            {'$sort': {'_id.year': 1, '_id.month': 1}}
        ]

        results = list(self.collection.aggregate(pipeline))

        # Форматируем месяцы
        months = {
            1: 'Январь', 2: 'Февраль', 3: 'Март', 4: 'Апрель',
            5: 'Май', 6: 'Июнь', 7: 'Июль', 8: 'Август',
            9: 'Сентябрь', 10: 'Октябрь', 11: 'Ноябрь', 12: 'Декабрь'
        }

        formatted = []
        for stat in results:
            formatted.append({
                'period': f"{months[stat['_id']['month']]} {stat['_id']['year']}",
                'show_count': stat['show_count'],
                'play_count': stat['play_count']
            })

        return formatted

    def run_comparison_test(self):
        """Запускает тест сравнения времени с кешем и без"""
        print("\n" + "=" * 60)
        print("ТЕСТ СРАВНЕНИЯ СКОРОСТИ (С КЕШЕМ И БЕЗ)")
        print("=" * 60)

        test_results = []

        # Тест 1: Статистика театров
        print("\nТЕСТ 1: Статистика театров")

        # Первый запуск (без кеша)
        print("Первый запуск (без кеша)...")
        self.cache.delete('theatre_stats')  # Очищаем кеш
        start_time = time.time()
        result1 = self.get_theatre_statistics()
        time_without_cache = time.time() - start_time

        # Второй запуск (с кешем)
        print("Второй запуск (с кешем)...")
        start_time = time.time()
        result2 = self.get_theatre_statistics()
        time_with_cache = time.time() - start_time

        test_results.append({
            'query': 'Статистика театров',
            'time_without_cache': round(time_without_cache, 3),
            'time_with_cache': round(time_with_cache, 3),
            'speedup': round(time_without_cache / time_with_cache, 1) if time_with_cache > 0 else 0
        })

        # Тест 2: Статистика жанров
        print("\nТЕСТ 2: Статистика жанров")

        self.cache.delete('genre_stats')
        print("Первый запуск (без кеша)...")
        start_time = time.time()
        self.get_genre_statistics()
        time_without_cache = time.time() - start_time

        print("Второй запуск (с кешем)...")
        start_time = time.time()
        self.get_genre_statistics()
        time_with_cache = time.time() - start_time

        test_results.append({
            'query': 'Статистика жанров',
            'time_without_cache': round(time_without_cache, 3),
            'time_with_cache': round(time_with_cache, 3),
            'speedup': round(time_without_cache / time_with_cache, 1) if time_with_cache > 0 else 0
        })

        # Тест 3: Предстоящие спектакли
        print("\nТЕСТ 3: Предстоящие спектакли")

        self.cache.delete('upcoming_shows')
        print("Первый запуск (без кеша)...")
        start_time = time.time()
        self.get_upcoming_shows(7)
        time_without_cache = time.time() - start_time

        print("Второй запуск (с кешем)...")
        start_time = time.time()
        self.get_upcoming_shows(7)
        time_with_cache = time.time() - start_time

        test_results.append({
            'query': 'Предстоящие спектакли',
            'time_without_cache': round(time_without_cache, 3),
            'time_with_cache': round(time_with_cache, 3),
            'speedup': round(time_without_cache / time_with_cache, 1) if time_with_cache > 0 else 0
        })

        # Вывод результатов
        print("\n" + "=" * 60)
        print("РЕЗУЛЬТАТЫ ТЕСТА")
        print("=" * 60)
        print(f"{'Запрос':<30} {'Без кеша':<10} {'С кешом':<10} {'Ускорение':<10}")
        print("-" * 60)

        for result in test_results:
            print(f"{result['query']:<30} "
                  f"{result['time_without_cache']:<10.3f} "
                  f"{result['time_with_cache']:<10.3f} "
                  f"{result['speedup']:<10.1f}x")

        # Статистика кеша
        print("\nСТАТИСТИКА КЕША")
        print("-" * 40)
        cache_stats = self.cache.get_stats()
        print(f"• Всего ключей в кеше: {cache_stats.get('total_keys', 0)}")
        print(f"• Распределение по типам запросов:")
        for query_type, count in cache_stats.get('keys_by_type', {}).items():
            print(f"  - {query_type}: {count} ключей")

        return test_results

    def demo_cached_queries(self):
        """Демонстрация работы кешированных запросов"""
        print("\n" + "=" * 60)
        print("ДЕМОНСТРАЦИЯ КЕШИРОВАННЫХ ЗАПРОСОВ")
        print("=" * 60)

        # 1. Статистика театров
        print("\nСтатистика театров:")
        theatre_stats = self.get_theatre_statistics()
        if theatre_stats:
            print(f"Всего театров: {len(theatre_stats)}")
            for stat in theatre_stats[:3]:
                print(f"   • {stat['theatre'][:30]}...: {stat['play_count']} спектаклей")

        # 2. Статистика жанров
        print("\nСтатистика жанров:")
        genre_stats = self.get_genre_statistics()
        if genre_stats:
            for stat in genre_stats[:5]:
                print(f"   • {stat['genre']}: {stat['total_plays']} спектаклей, {stat['total_shows']} показов")

        # 3. Предстоящие спектакли
        print("\nПредстоящие спектакли (7 дней):")
        upcoming = self.get_upcoming_shows(7)
        if upcoming:
            print(f"Всего дней с показами: {len(upcoming)}")
            for day in upcoming[:2]:
                date_str = day['date'][:10]
                print(f"   • {date_str}: {day['play_count']} спектаклей")

        # 4. Топ актеров
        print("\nТоп актеров:")
        top_actors = self.get_top_actors(5)
        if top_actors:
            for i, actor in enumerate(top_actors[:5], 1):
                print(f"   {i}. {actor['actor']}: {actor['play_count']} спектаклей")

        # 5. Тест производительности
        self.run_comparison_test()

    def close(self):
        """Закрывает соединения"""
        self.client.close()
        self.cache.close()


def main():
    """Основная функция для демонстрации"""
    queries = CachedQueries()

    try:
        # Демонстрация работы
        queries.demo_cached_queries()

        print("\n" + "=" * 60)
        print("ДЕМОНСТРАЦИЯ ЗАВЕРШЕНА")
        print("=" * 60)

    finally:
        queries.close()


if __name__ == "__main__":
    main()