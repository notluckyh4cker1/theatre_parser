import os
import json
import time
from datetime import datetime
from src.url_collector import URLCollector
from src.page_parser import PageParser
from src.mongo_handler import MongoHandler


def main():
    print("\n" + "=" * 60)
    print("ТЕАТРАЛЬНЫЙ ПАРСЕР")
    print("=" * 60)

    # 1. Инициализация
    collector = URLCollector()
    parser = PageParser()
    mongo = MongoHandler()

    # 2. Подключаемся к MongoDB
    print("\nПОДКЛЮЧЕНИЕ К MONGODB")
    print("-" * 40)

    if not mongo.connect():
        print("Продолжаем без MongoDB")

    # 3. Сбор ссылок
    print("\nСБОР ССЫЛОК")
    print("-" * 40)

    urls = collector.run(force_collect=True)

    if not urls:
        print("Не удалось собрать ссылки")
        return

    print(f"Собрано {len(urls)} ссылок")

    # 4. Парсинг
    print(f"\nПАРСИНГ {len(urls)} СПЕКТАКЛЕЙ")
    print("-" * 40)

    all_plays = []
    successful = 0

    for i, url in enumerate(urls, 1):
        try:
            print(f"\r[{i:3d}/{len(urls)}] Парсим...", end="")

            play_data = parser.parse_play_page(url)

            if play_data and play_data.get('name') and play_data.get('dates'):
                all_plays.append(play_data)
                successful += 1

                # Периодически сохраняем в MongoDB
                if mongo.connected and i % 20 == 0:
                    mongo.save_play(play_data)

        except Exception as e:
            print(f"\nОшибка: {url[:50]}... - {e}")
            continue

    print(f"\nПарсинг завершен: {successful}/{len(urls)} успешно")

    # 5. Сохранение в MongoDB
    print(f"\nСОХРАНЕНИЕ В MONGODB")
    print("-" * 40)

    if mongo.connected:
        mongo.save_all_plays(all_plays)

        stats = mongo.get_stats()
        print(f"В базе: {stats.get('total_plays', 0)} спектаклей")

        if stats.get('top_theatres'):
            print("Топ театров:")
            for theatre in stats['top_theatres'][:3]:
                print(f"   • {theatre['_id']}: {theatre['count']} спектаклей")

    # 6. Сохранение в JSON
    print(f"\nСОХРАНЕНИЕ В JSON")
    print("-" * 40)

    json_file = "data/plays_final.json"
    mongo.save_to_json(all_plays, json_file)

    # 7. Статистика
    print(f"\nСТАТИСТИКА")
    print("-" * 40)

    if all_plays:
        print(f"Обработано спектаклей: {len(all_plays)}")

        # Анализ качества данных
        fields = {
            'name': 'Название',
            'theatre': 'Театр',
            'director': 'Режиссер',
            'actors': 'Актеры',
            'dates': 'Даты',
            'duration_minutes': 'Продолжительность',
            'genre': 'Жанр'
        }

        print("\nЗаполненность полей:")
        for field, name in fields.items():
            if field == 'actors':
                count = sum(1 for p in all_plays if p.get(field) and len(p[field]) > 0)
            elif field == 'dates':
                count = sum(1 for p in all_plays if p.get(field) and len(p[field]) > 0)
            else:
                count = sum(1 for p in all_plays if p.get(field))

            percentage = (count / len(all_plays)) * 100
            icon = "✅" if percentage > 80 else "⚠️" if percentage > 50 else "❌"

            print(f"   {icon} {name}: {count}/{len(all_plays)} ({percentage:.1f}%)")

    # 8. Закрытие
    print(f"\n" + "=" * 60)
    print("ВЫПОЛНЕНО!")
    print(f"   Спектаклей: {len(all_plays)}")
    print(f"   JSON: {json_file}")
    print("=" * 60)

    if mongo.connected:
        mongo.close()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nПрервано пользователем")
    except Exception as e:
        print(f"\nКритическая ошибка: {e}")
        import traceback

        traceback.print_exc()