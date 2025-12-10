import os
import json
import time
from datetime import datetime, timedelta
from src.url_collector import URLCollector
from src.page_parser import PageParser
from src.mongo_handler import MongoHandler

def load_existing_data():
    """Загружает существующие данные из JSON"""
    json_files = [
        "data/plays_full.json",
        "data/plays.json",
        "data/plays_final.json"
    ]

    for json_file in json_files:
        if os.path.exists(json_file):
            try:
                with open(json_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)

                if 'plays' in data:
                    plays = data['plays']
                    print(f"Загружено {len(plays)} спектаклей из {json_file}")
                    return plays
            except Exception as e:
                print(f"Ошибка загрузки {json_file}: {e}")

    return []


def main():
    print("\n" + "=" * 60)
    print("ТЕАТРАЛЬНЫЙ ПАРСЕР")
    print("=" * 60)

    # 1. Инициализация MongoDB
    mongo = MongoHandler()

    # 2. Подключаемся к MongoDB
    print("\nПОДКЛЮЧЕНИЕ К MONGODB")
    print("-" * 40)

    if not mongo.connect():
        print("MongoDB недоступна. Завершение.")
        return

    # 3. Загружаем существующие данные
    print("\nЗАГРУЗКА СУЩЕСТВУЮЩИХ ДАННЫХ")
    print("-" * 40)

    plays = load_existing_data()

    if not plays:
        print("Нет данных для обработки")

        print("\nПробуем собрать несколько спектаклей...")

        collector = URLCollector()
        parser = PageParser()

        # Загружаем сохраненные ссылки
        urls_file = "data/play_urls.txt"
        if os.path.exists(urls_file):
            with open(urls_file, 'r', encoding='utf-8') as f:
                urls = [line.strip() for line in f if line.strip()][:10]  # 10 во избежание ошибки 429

            print(f"Загружено {len(urls)} ссылок")

            plays = []
            for i, url in enumerate(urls, 1):
                print(f"   [{i}/{len(urls)}] Парсим {url[:50]}...")

                # Большая задержка для избежания 429
                time.sleep(5)

                play_data = parser.parse_play_page(url)
                if play_data:
                    plays.append(play_data)

        if not plays:
            print("Не удалось собрать данные")
            return

    print(f"Загружено {len(plays)} спектаклей")

    # 4. Сохранение в MongoDB
    print("\nСОХРАНЕНИЕ В MONGODB")
    print("-" * 40)

    mongo.save_all_plays(plays)

    # 5. Выполнение запросов из ТЗ
    print("\nВЫПОЛНЕНИЕ ЗАПРОСОВ ИЗ ТЗ")
    print("-" * 40)

    execute_mongo_queries(mongo)

    # 6. Сохранение в JSON
    print("\nСОХРАНЕНИЕ В JSON")
    print("-" * 40)

    json_file = "data/plays_with_queries.json"
    mongo.save_to_json(plays, json_file)

    # 7. Статистика
    print("\nФИНАЛЬНАЯ СТАТИСТИКА")
    print("-" * 40)

    show_final_stats(mongo, plays)

    # 8. Закрытие
    print("\n" + "=" * 60)
    print("АНАЛИЗ ЗАВЕРШЕН!")
    print("=" * 60)

    mongo.close()

def execute_mongo_queries(mongo):
    """Выполняет запросы из лабораторной работы"""

    print("\nПРОСТЫЕ ЗАПРОСЫ:")
    print("-" * 30)

    # 1. SELECT * FROM plays WHERE theatre = 'Vegas City Hall'
    print("1. Весь репертуар конкретного театра:")

    pipeline = [
        {'$group': {'_id': '$theatre', 'count': {'$sum': 1}}},
        {'$sort': {'count': -1}},
        {'$limit': 3}
    ]

    top_theatres = list(mongo.collection.aggregate(pipeline))

    for theatre_info in top_theatres:
        theatre = theatre_info['_id']
        if theatre and theatre != "Не указан":
            query = {'theatre': theatre}
            plays_in_theatre = list(mongo.collection.find(query, {'name': 1, 'dates': 1}).limit(3))

            print(f"\n   Театр: {theatre}")
            print(f"   Спектаклей в БД: {theatre_info['count']}")
            if plays_in_theatre:
                print("   Примеры спектаклей:")
                for play in plays_in_theatre:
                    print(f"     • {play.get('name')} ({len(play.get('dates', []))} дат)")
            break  # Берем только первый театр

    # 2. SELECT name, theatre, dates FROM plays WHERE director = '...'
    print("\n2. Творчество конкретного режиссера:")

    pipeline = [
        {'$match': {'director': {'$ne': 'Не указан', '$exists': True}}},
        {'$group': {'_id': '$director', 'count': {'$sum': 1}}},
        {'$sort': {'count': -1}},
        {'$limit': 3}
    ]

    top_directors = list(mongo.collection.aggregate(pipeline))

    for director_info in top_directors:
        director = director_info['_id']
        if director:
            query = {'director': director}
            director_plays = list(mongo.collection.find(
                query,
                {'name': 1, 'theatre': 1, 'dates': 1}
            ).limit(3))

            print(f"\n   Режиссер: {director}")
            print(f"   Спектаклей в БД: {director_info['count']}")
            if director_plays:
                print("Примеры работ:")
                for play in director_plays:
                    dates_count = len(play.get('dates', []))
                    print(f"     • {play.get('name')} в {play.get('theatre')} ({dates_count} показов)")
            break

    # 3. SELECT name, theatre, duration_minutes FROM plays WHERE '2024-11-20' IN dates
    print("\n3. Спектакли на конкретную дату:")

    pipeline = [
        {'$unwind': '$dates'},
        {'$group': {'_id': '$dates'}},
        {'$sort': {'_id': 1}},
        {'$limit': 1}
    ]

    nearest_date_result = list(mongo.collection.aggregate(pipeline))

    if nearest_date_result:
        target_date = nearest_date_result[0]['_id']
        print(f"   Ближайшая дата: {target_date}")

        # Ищем спектакли на эту дату
        query = {'dates': target_date}
        plays_on_date = list(mongo.collection.find(
            query,
            {'name': 1, 'theatre': 1, 'duration_minutes': 1}
        ).limit(5))

        if plays_on_date:
            print(f"   Спектакли на {target_date}:")
            for play in plays_on_date:
                duration = play.get('duration_minutes', 'не указана')
                print(f"     • {play.get('name')} в {play.get('theatre')} ({duration} мин)")
        else:
            print(f"   На {target_date} спектаклей не найдено")

    print("\nРАСШИРЕННЫЕ ЗАПРОСЫ:")
    print("-" * 30)

    # 4. SELECT theatre, COUNT(*) as play_count, AVG(duration_minutes) as avg_duration
    print("4. Самые активные театры:")

    pipeline = [
        {'$match': {'duration_minutes': {'$exists': True, '$ne': None}}},
        {'$group': {
            '_id': '$theatre',
            'play_count': {'$sum': 1},
            'avg_duration': {'$avg': '$duration_minutes'},
            'min_duration': {'$min': '$duration_minutes'},
            'max_duration': {'$max': '$duration_minutes'}
        }},
        {'$match': {'_id': {'$ne': 'Не указан'}}},
        {'$sort': {'play_count': -1}},
        {'$limit': 10}
    ]

    theatre_stats = list(mongo.collection.aggregate(pipeline))

    if theatre_stats:
        print(f"   {'Театр':<40} {'Спектаклей':<12} {'Средняя длит.':<12}")
        print("   " + "-" * 70)
        for stat in theatre_stats:
            theatre_name = stat['_id'][:40]
            if len(theatre_name) < 40:
                theatre_name = theatre_name.ljust(40)
            print(f"   {theatre_name} {stat['play_count']:<12} {stat['avg_duration']:<12.0f} мин")
    else:
        print("Недостаточно данных для анализа")

    # 5. SELECT genre, COUNT(*) as total_plays, COUNT(DISTINCT theatre) as theatre_count...
    print("\n5. Популярность жанров:")

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
            'total_shows': 1
        }},
        {'$sort': {'total_shows': -1}}
    ]

    genre_stats = list(mongo.collection.aggregate(pipeline))

    if genre_stats:
        print(f"   {'Жанр':<20} {'Спектаклей':<12} {'Театров':<10} {'Показов':<10}")
        print("   " + "-" * 55)
        for stat in genre_stats:
            genre = stat['_id'][:20]
            if len(genre) < 20:
                genre = genre.ljust(20)
            print(f"   {genre} {stat['total_plays']:<12} {stat['theatre_count']:<10} {stat['total_shows']:<10}")
    else:
        print("Недостаточно данных для анализа")


def show_final_stats(mongo, plays):
    """Показывает финальную статистику"""

    # Базовая статистика
    total_plays = len(plays)
    print(f"ОБЩАЯ СТАТИСТИКА:")
    print(f"Всего спектаклей: {total_plays}")

    if total_plays > 0:
        # Статистика по полям
        fields_stats = {
            'Название': sum(1 for p in plays if p.get('name')),
            'Театр': sum(1 for p in plays if p.get('theatre') and p['theatre'] != 'Не указан'),
            'Режиссер': sum(1 for p in plays if p.get('director') and p['director'] != 'Не указан'),
            'Актеры': sum(1 for p in plays if p.get('actors') and len(p['actors']) > 0),
            'Даты': sum(1 for p in plays if p.get('dates') and len(p['dates']) > 0),
            'Продолжительность': sum(1 for p in plays if p.get('duration_minutes')),
            'Жанр': sum(1 for p in plays if p.get('genre')),
        }

        print(f"   • Заполненность данных:")
        for field, count in fields_stats.items():
            percentage = (count / total_plays) * 100
            icon = "✅" if percentage > 80 else "⚠️" if percentage > 50 else "❌"
            print(f"     {icon} {field}: {count}/{total_plays} ({percentage:.1f}%)")

    # MongoDB статистика
    if mongo.connected:
        db_stats = mongo.get_stats()
        if db_stats:
            print(f"\nMONGODB СТАТИСТИКА:")
            print(f"В базе данных: {db_stats.get('total_plays', 0)} спектаклей")
            print(f"Уникальных жанров: {db_stats.get('genres_count', 0)}")

            if db_stats.get('top_theatres'):
                print(f"Топ-3 театра:")
                for i, theatre in enumerate(db_stats['top_theatres'][:3], 1):
                    print(f"     {i}. {theatre['_id'][:30]}: {theatre['count']} спектаклей")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nПрервано пользователем")
    except Exception as e:
        print(f"\nКритическая ошибка: {e}")
        import traceback

        traceback.print_exc()