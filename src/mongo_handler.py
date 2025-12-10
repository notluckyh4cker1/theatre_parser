import json
import os
from datetime import datetime
from pymongo import MongoClient, errors
from typing import List, Dict
import config

class MongoHandler:
    def __init__(self):
        self.client = None
        self.db = None
        self.collection = None
        self.connected = False

    def connect(self):
        """Подключается к MongoDB"""
        try:
            self.client = MongoClient(
                host=config.MONGO_CONFIG['host'],
                port=config.MONGO_CONFIG['port'],
                serverSelectionTimeoutMS=5000
            )

            # Проверяем подключение
            self.client.admin.command('ping')
            print("Успешное подключение к MongoDB")

            self.db = self.client[config.MONGO_CONFIG['database']]
            self.collection = self.db[config.MONGO_CONFIG['collection']]

            self.create_indexes()

            self.connected = True
            return True

        except errors.ConnectionFailure as e:
            print(f"Ошибка подключения к MongoDB: {e}")
            print("Убедитесь, что MongoDB запущена: mongod")
            return False
        except Exception as e:
            print(f"Ошибка: {e}")
            return False

    def create_indexes(self):
        """Создает необходимые индексы"""
        try:
            self.collection.create_index([('url', 1)], unique=True, name='url_index')

            self.collection.create_index([('name', 1)], name='name_index')
            self.collection.create_index([('theatre', 1)], name='theatre_index')
            self.collection.create_index([('genre', 1)], name='genre_index')
            self.collection.create_index([('dates', 1)], name='dates_index')

            print("Созданы индексы для оптимизации запросов")
        except Exception as e:
            print(f"Ошибка при создании индексов: {e}")

    def save_play(self, play_data: Dict) -> bool:
        """Сохраняет один спектакль в MongoDB"""
        if not self.connected:
            print("Нет подключения к MongoDB")
            return False

        try:
            # Удаляем конфликтующие поля, если они уже есть
            play_data_copy = play_data.copy()

            # Убираем временные метки, которые могут конфликтовать
            fields_to_remove = ['_created_at', '_updated_at', '_parsed_date']
            for field in fields_to_remove:
                play_data_copy.pop(field, None)

            # Добавляем свежие метаданные
            now = datetime.now()
            update_data = {
                '$set': play_data_copy,
                '$setOnInsert': {'_created_at': now},
                '$currentDate': {'_updated_at': True}
            }

            # Пробуем вставить или обновить
            result = self.collection.update_one(
                {'url': play_data['url']},
                update_data,
                upsert=True
            )

            if result.upserted_id:
                print(f"Добавлен: {play_data.get('name', 'Без названия')[:40]}...")
                return True
            else:
                print(f"Обновлен: {play_data.get('name', 'Без названия')[:40]}...")
                return True

        except errors.DuplicateKeyError:
            print(f"Дубликат URL: {play_data['url']}")
            return False
        except Exception as e:
            print(f"Ошибка при сохранении: {e}")
            return False

    def save_all_plays(self, plays: List[Dict]) -> bool:
        """Сохраняет все спектакли в MongoDB"""
        if not self.connected:
            if not self.connect():
                return False

        print(f"Сохраняем {len(plays)} спектаклей в MongoDB...")

        successful = 0
        failed = 0

        # Сначала очистим коллекцию, чтобы избежать конфликтов
        print("Очищаем коллекцию...")
        self.collection.delete_many({})

        for i, play in enumerate(plays, 1):
            try:
                # Убедимся, что есть URL
                if 'url' not in play:
                    print(f"Нет URL у спектакля: {play.get('name', 'Без названия')}")
                    failed += 1
                    continue

                play['_id'] = self.generate_id(play)

                if self.save_play(play):
                    successful += 1
                else:
                    failed += 1

                # Прогресс
                if i % 10 == 0:
                    print(f"Обработано: {i}/{len(plays)}")

            except Exception as e:
                failed += 1
                print(f"Ошибка при сохранении {play.get('name', 'Без названия')}: {e}")

        print(f"Завершено: ✅ {successful} успешно, ❌ {failed} ошибок")
        return successful > 0

    def generate_id(self, play_data: Dict) -> str:
        """Генерирует ID для спектакля"""
        import hashlib

        url_hash = hashlib.md5(play_data['url'].encode()).hexdigest()[:8]
        name_slug = play_data.get('name', 'unknown').lower().replace(' ', '_')[:20]

        return f"{name_slug}_{url_hash}"

    def clear_collection(self):
        """Очищает коллекцию"""
        if self.connected:
            result = self.collection.delete_many({})
            print(f"Очищено {result.deleted_count} документов")
            return True
        return False

    def get_stats(self):
        """Возвращает статистику по коллекции"""
        if not self.connected:
            return {}

        stats = {
            'total_plays': self.collection.count_documents({}),
            'by_genre': {},
            'by_theatre': {},
            'date_range': {}
        }

        # Статистика по жанрам
        genres = self.collection.distinct('genre')
        stats['genres_count'] = len(genres)

        # Самый популярный театр
        pipeline = [
            {'$group': {'_id': '$theatre', 'count': {'$sum': 1}}},
            {'$sort': {'count': -1}},
            {'$limit': 5}
        ]
        top_theatres = list(self.collection.aggregate(pipeline))
        stats['top_theatres'] = top_theatres

        # Диапазон дат
        pipeline = [
            {'$unwind': '$dates'},
            {'$group': {
                '_id': None,
                'min_date': {'$min': '$dates'},
                'max_date': {'$max': '$dates'}
            }}
        ]
        date_range = list(self.collection.aggregate(pipeline))
        if date_range:
            stats['date_range'] = date_range[0]

        return stats

    def save_to_json(self, plays: List[Dict], filename: str = None) -> bool:
        """Сохраняет данные в JSON файл"""
        if filename is None:
            filename = config.JSON_FILE

        output = {
            'plays': plays,
            'metadata': {
                'total_plays': len(plays),
                'collection_date': datetime.now().isoformat(),
                'source': 'kassir.ru',
                'database_stats': self.get_stats() if self.connected else {}
            }
        }

        try:
            os.makedirs(os.path.dirname(filename), exist_ok=True)

            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(output, f, ensure_ascii=False, indent=2, default=str)

            print(f"JSON сохранен: {filename} ({len(plays)} спектаклей)")
            return True

        except Exception as e:
            print(f"Ошибка при сохранении JSON: {e}")
            return False

    def close(self):
        """Закрывает соединение с MongoDB"""
        if self.client:
            self.client.close()
            print("🔌 Соединение с MongoDB закрыто")