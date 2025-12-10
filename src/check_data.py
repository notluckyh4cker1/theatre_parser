import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from pymongo import MongoClient
import json


def check_mongo_data():
    """Проверяет данные в MongoDB"""
    print("ПРОВЕРКА КАЧЕСТВА ДАННЫХ В MONGODB")
    print("=" * 60)

    try:
        client = MongoClient('localhost', 27017)
        db = client.theater_db
        collection = db.plays

        count = collection.count_documents({})
        print(f"Всего спектаклей в БД: {count}")

        if count == 0:
            print("В БД нет данных")
            return

        print("\nСТАТИСТИКА ПО ПОЛЯМ:")

        # Проверяем заполненность полей
        fields_to_check = [
            ('name', 'Название'),
            ('theatre', 'Театр'),
            ('director', 'Режиссер'),
            ('actors', 'Актеры'),
            ('dates', 'Даты'),
            ('duration_minutes', 'Продолжительность'),
            ('genre', 'Жанр')
        ]

        for field, field_name in fields_to_check:
            if field == 'actors':
                # Для актеров проверяем что не пустой массив
                query = {field: {'$exists': True, '$ne': [], '$not': {'$size': 0}}}
            elif field == 'dates':
                # Для дат проверяем что не пустой массив
                query = {field: {'$exists': True, '$ne': [], '$not': {'$size': 0}}}
            else:
                query = {field: {'$exists': True, '$ne': ''}}

            filled_count = collection.count_documents(query)
            percentage = (filled_count / count) * 100
            status = "✅" if percentage > 80 else "⚠️" if percentage > 50 else "❌"

            print(f"{status} {field_name}: {filled_count}/{count} ({percentage:.1f}%)")

        print("\nПРИМЕРЫ СПЕКТАКЛЕЙ С ПРОБЛЕМАМИ:")

        no_actors = list(collection.find(
            {'$or': [
                {'actors': {'$exists': False}},
                {'actors': []},
                {'actors': {'$size': 0}}
            ]}
        ).limit(3))

        if no_actors:
            print("\nСпектакли без актеров:")
            for play in no_actors:
                print(f"  • {play.get('name', 'Без названия')[:50]}...")

        no_director = list(collection.find(
            {'$or': [
                {'director': {'$exists': False}},
                {'director': ''},
                {'director': 'Не указан'}
            ]}
        ).limit(3))

        if no_director:
            print("\nСпектакли без режиссера:")
            for play in no_director:
                print(f"  • {play.get('name', 'Без названия')[:50]}...")

        # Выводим несколько хороших примеров
        print("\n✅ ПРИМЕРЫ КОРРЕКТНЫХ ДАННЫХ:")
        good_examples = list(collection.find({
            'actors': {'$exists': True, '$ne': [], '$not': {'$size': 0}},
            'director': {'$exists': True, '$ne': '', '$ne': 'Не указан'},
            'duration_minutes': {'$exists': True, '$gte': 30}
        }).limit(2))

        for i, play in enumerate(good_examples, 1):
            print(f"\n{i}. {play.get('name', 'Без названия')}")
            print(f"   Театр: {play.get('theatre', 'Не указан')}")
            print(f"   Режиссер: {play.get('director', 'Не указан')}")
            actors = play.get('actors', [])
            if actors:
                print(f"   Актеры ({len(actors)}): {', '.join(actors[:3])}{'...' if len(actors) > 3 else ''}")
            print(f"   Дат: {len(play.get('dates', []))}")
            print(f"   Продолжительность: {play.get('duration_minutes')} мин")

        client.close()

    except Exception as e:
        print(f"Ошибка: {e}")


def check_json_file():
    """Проверяет JSON файл"""
    print("\n" + "=" * 60)
    print("ПРОВЕРКА JSON ФАЙЛА")
    print("=" * 60)

    json_file = os.path.join(os.path.dirname(__file__), 'data', 'plays.json')

    if not os.path.exists(json_file):
        print(f"Файл не найден: {json_file}")
        return

    try:
        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)

        plays = data.get('plays', [])
        print(f"📊 Спектаклей в JSON: {len(plays)}")

        if plays:
            print("\nПЕРВЫЙ СПЕКТАКЛЬ В JSON:")
            first = plays[0]
            print(f"• Название: {first.get('name')}")
            print(f"• Театр: {first.get('theatre')}")
            print(f"• Режиссер: {first.get('director')}")
            actors = first.get('actors', [])
            if actors:
                print(f"• Актеры ({len(actors)}): {', '.join(actors[:3])}{'...' if len(actors) > 3 else ''}")
            print(f"• Дат: {len(first.get('dates', []))}")
            print(f"• Продолжительность: {first.get('duration_minutes')} мин")

    except Exception as e:
        print(f"Ошибка при чтении JSON: {e}")


if __name__ == "__main__":
    check_mongo_data()
    check_json_file()