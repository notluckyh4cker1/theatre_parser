import re
import time
import json
from datetime import datetime
from typing import Dict, List, Optional
from bs4 import BeautifulSoup
import requests
import config
from src.data_cleaner import DataCleaner

class PageParser:
    def __init__(self):
        self.cleaner = DataCleaner()
        self.session = requests.Session()
        self.session.headers.update(config.HEADERS)

    @staticmethod
    def parse_russian_date(date_text):
        """Парсит русскую дату '20 декабря 2024, 19:00' в ISO"""
        months = {
            'января': '01', 'февраля': '02', 'марта': '03',
            'апреля': '04', 'мая': '05', 'июня': '06',
            'июля': '07', 'августа': '08', 'сентября': '09',
            'октября': '10', 'ноября': '11', 'декабря': '12'
        }

        pattern = r'(\d{1,2})\s+([а-яё]+)\s+(\d{4})(?:,\s+(\d{1,2}):(\d{2}))?'
        match = re.search(pattern, date_text.lower())

        if match:
            day, month_ru, year = match.group(1), match.group(2), match.group(3)
            hour = match.group(4) or '19'
            minute = match.group(5) or '00'

            if month_ru in months:
                month = months[month_ru]
                return f"{year}-{month}-{int(day):02d}T{int(hour):02d}:{int(minute):02d}:00"

        return None

    @staticmethod
    def is_valid_actor_name(name):
        """Проверяет, является ли строка именем человека"""
        if not name or len(name) < 5:
            return False

        words = name.split()
        if len(words) < 2:
            return False

        invalid_words = [
            'режиссер', 'продолжительность', 'цена', 'билет',
            'место', 'время', 'дата', 'купить'
        ]

        name_lower = name.lower()
        for word in invalid_words:
            if word in name_lower:
                return False

        if not re.search(r'[А-ЯЁа-яё]', name):
            return False

        return True

    def fetch_page(self, url: str) -> Optional[BeautifulSoup]:
        """Загружает страницу"""
        try:
            print(f"Загружаем: {url}")
            response = self.session.get(url, timeout=config.TIMEOUT)
            response.raise_for_status()
            time.sleep(config.REQUEST_DELAY)
            return BeautifulSoup(response.text, 'html.parser')
        except Exception as e:
            print(f"Ошибка: {e}")
            return None

    def extract_json_ld(self, soup: BeautifulSoup) -> Optional[Dict]:
        """Извлекает JSON-LD данные"""
        scripts = soup.find_all('script', type='application/ld+json')

        for script in scripts:
            try:
                data = json.loads(script.string)
                if isinstance(data, dict) and data.get('@type') == 'Event':
                    return data
                elif isinstance(data, list):
                    for item in data:
                        if isinstance(item, dict) and item.get('@type') == 'Event':
                            return item
            except:
                continue

        return None

    def parse_play_page(self, url: str) -> Optional[Dict]:
        """Парсит страницу спектакля"""
        soup = self.fetch_page(url)
        if not soup:
            return None

        json_data = self.extract_json_ld(soup)

        play_data = {
            'url': url,
            'name': self.extract_name(soup, json_data),
            'theatre': self.extract_theatre(soup, json_data),
            'director': self.extract_director(soup, json_data),
            'actors': self.extract_actors(soup, json_data),
            'dates': self.extract_dates(soup, json_data),
            'genre': self.extract_genre(soup, json_data),
            'duration_minutes': self.extract_duration(soup, json_data),
            'age_rating': self.extract_age_rating(soup, json_data),
            'description': self.extract_description(soup, json_data),
        }

        if not play_data['name'] or not play_data['dates']:
            return None

        print(f"Спарсено: {play_data['name'][:50]}...")
        return play_data

    def extract_name(self, soup: BeautifulSoup, json_data: Optional[Dict]) -> str:
        """Извлекает название"""
        if json_data and json_data.get('name'):
            name = json_data['name']
            return self.cleaner.clean_text(name)

        title_tag = soup.find('title')
        if title_tag:
            title = title_tag.text
            for sep in [' - ', ' – ', ' — ', ' | ']:
                if sep in title:
                    title = title.split(sep)[0]
            return self.cleaner.clean_text(title)

        return ""

    def extract_theatre(self, soup: BeautifulSoup, json_data: Optional[Dict]) -> str:
        """Извлекает театр"""
        # 1. Из JSON-LD
        if json_data and json_data.get('location'):
            location = json_data['location']
            if isinstance(location, dict):
                name = location.get('name')
                if name:
                    theatre = self.cleaner.clean_text(name)
                    theatre = re.sub(r'\s*[—\-\.]\s*расписание.*', '', theatre, flags=re.IGNORECASE)
                    theatre = re.sub(r'\s*\.\s*$', '', theatre)
                    return theatre

        # 2. Из мета-описания
        meta_desc = soup.find('meta', {'name': 'description'})
        if meta_desc and meta_desc.get('content'):
            desc = meta_desc['content']
            if '➤' in desc:
                theatre = desc.split('➤')[1].split(',')[0].strip()
                theatre = self.cleaner.clean_text(theatre)
                theatre = re.sub(r'\s*[—\-\.]\s*расписание.*', '', theatre, flags=re.IGNORECASE)
                return theatre

        return "Не указан"

    def extract_director(self, soup: BeautifulSoup, json_data: Optional[Dict]) -> str:
        """Извлекает режиссера"""

        all_text = soup.get_text()

        lines = all_text.split('\n')

        for line in lines:
            line = line.strip()

            if 'Режиссер' in line and len(line) < 150:
                line = re.sub(r'\s+', ' ', line)

                patterns = [
                    r'Режисс[её]р\s*[—–\-:]\s*([А-ЯЁ][а-яё]+\s+[А-ЯЁ][а-яё]+)',
                    r'Режиссер\s+([А-ЯЁ][а-яё]+\s+[А-ЯЁ][а-яё]+)',
                ]

                for pattern in patterns:
                    match = re.search(pattern, line, re.IGNORECASE)
                    if match:
                        director = match.group(1).strip()

                        bad_words = ['место', 'проведения', 'зал', 'театр', 'сцена']
                        if not any(word in director.lower() for word in bad_words):
                            return director

        # 3. Если не нашли, пробуем через описание
        description = self.extract_description(soup, json_data)
        if description:
            # Прямой поиск в описании
            if 'Режиссер' in description:
                idx = description.find('Режиссер')
                context = description[idx:idx + 100]

                match = re.search(r'Режисс[её]р\s*[—–\-:]\s*([А-ЯЁ][а-яё]+\s+[А-ЯЁ][а-яё]+)', context, re.IGNORECASE)
                if match:
                    director = match.group(1).strip()
                    return director

        return "Не указан"

    def extract_actors(self, soup: BeautifulSoup, json_data: Optional[Dict]) -> List[str]:
        """Извлекает актеров"""
        actors = []

        # 1. Сначала ищем блок "Исполнители" с карточками
        performers_section = None

        # Ищем заголовок "Исполнители"
        for tag in ['h2', 'h3', 'h4']:
            performers_header = soup.find(tag, string=re.compile(r'Исполнители', re.IGNORECASE))
            if performers_header:
                performers_section = performers_header.find_parent('section')
                if not performers_section:
                    performers_section = performers_header.find_parent('div')
                break

        if performers_section:
            # Ищем карточки исполнителей
            cards = performers_section.find_all(['div', 'article'],
                                                class_=lambda x: x and any(word in str(x).lower()
                                                                           for word in ['card', 'slide', 'performer']))

            for card in cards:
                name_elem = card.find(['p', 'span', 'div', 'a'],
                                      class_=lambda x: x and any(cls in str(x).lower()
                                                                 for cls in
                                                                 ['name', 'title', 'semibold', 'font-semibold']))

                if name_elem:
                    name = name_elem.get_text().strip()
                    # Очищаем имя
                    name = re.sub(r'[^\w\s\-\.]', ' ', name)
                    name = re.sub(r'\s+', ' ', name).strip()

                    if self.is_valid_actor_name(name):
                        actors.append(name)

        # 2. Если не нашли в блоке "Исполнители", ищем в описании
        if not actors:
            # Получаем полное описание
            description = ""
            content_block = soup.find('div', class_='content-block')
            if content_block:
                description = content_block.get_text()
            else:
                # Ищем любой текст на странице
                main_content = soup.find('main') or soup.find('article') or soup.find('div', class_=lambda
                    x: x and 'content' in str(x).lower())
                if main_content:
                    description = main_content.get_text()

            # Ищем имена в формате "Имя Фамилия" (минимум 2 слова, начинаются с заглавных)
            name_pattern = r'\b[А-ЯЁ][а-яё]+\s+[А-ЯЁ][а-яё]+\b'
            potential_names = re.findall(name_pattern, description)

            for name in potential_names:
                if self.is_valid_actor_name(name):
                    # Проверяем контекст - исключаем имена в заголовках
                    if not any(word in name.lower() for word in ['город', 'страна', 'улица', 'площадь']):
                        actors.append(name)

        director = self.extract_director(soup, json_data)
        if director and director != "Не указан":
            # Простое сравнение: если актер совпадает с режиссером - пропускаем
            actors = [actor for actor in actors if actor != director]

        # 3. Очищаем и нормализуем
        return self.clean_and_normalize_actors(actors)

    def clean_and_normalize_actors(self, actors):
        """Очищает и нормализует список актеров"""
        cleaned = []

        for actor in actors:
            actor = re.sub(r'[^\w\s\-\.]', ' ', actor)
            actor = re.sub(r'\s+', ' ', actor).strip()

            if ',' in actor:
                parts = [part.strip() for part in actor.split(',')]
                for part in parts:
                    if self.is_valid_actor_name(part):
                        cleaned.append(self.normalize_name(part))
            elif ' и ' in actor:
                parts = [part.strip() for part in actor.split(' и ')]
                for part in parts:
                    if self.is_valid_actor_name(part):
                        cleaned.append(self.normalize_name(part))
            else:
                if self.is_valid_actor_name(actor):
                    cleaned.append(self.normalize_name(actor))

        unique = []
        for actor in cleaned:
            # Нормализуем окончания (Анны -> Анна)
            base_name = re.sub(r'ы$', 'а', actor)
            base_name = re.sub(r'и$', 'я', base_name)

            if actor not in unique and base_name not in unique:
                unique.append(actor)

        return unique

    def normalize_name(self, name):
        """Нормализует имя актера (Имя Фамилия)"""
        words = name.split()
        normalized_words = []

        for word in words:
            if word.isupper():
                normalized_words.append(word.title())
            elif word and word[0].isupper():
                normalized_words.append(word)
            else:
                normalized_words.append(word.capitalize())

        return ' '.join(normalized_words)

    def extract_dates(self, soup: BeautifulSoup, json_data: Optional[Dict]) -> List[str]:
        """Извлекает даты из расписания - ОПТИМИЗИРОВАНО ДЛЯ KASSIR.RU"""
        dates = []

        # 1. Ищем блок "Расписание"
        schedule_header = soup.find(['h2', 'h3'], string=re.compile('Расписание', re.IGNORECASE))

        if schedule_header:
            # Ищем контейнер с датами
            container = schedule_header.find_next(['section', 'div'])

            # Ищем ссылки с датами
            date_links = container.find_all('a', href=re.compile(r'#\d+')) if container else []

            for link in date_links:
                href = link.get('href', '')
                # Извлекаем дату из текста ссылки
                date_text_elem = link.find('span', class_=lambda x: x and 'whitespace-nowrap' in str(x))
                if date_text_elem:
                    day = date_text_elem.get_text().strip()

                    # Ищем месяц (обычно выше в контейнере)
                    month_elem = container.find('span', class_=lambda x: x and 'event-date-selector-month' in str(x))
                    month_map = {
                        'январь': '01', 'февраль': '02', 'март': '03',
                        'апрель': '04', 'май': '05', 'июнь': '06',
                        'июль': '07', 'август': '08', 'сентябрь': '09',
                        'октябрь': '10', 'ноябрь': '11', 'декабрь': '12'
                    }

                    if month_elem:
                        month_ru = month_elem.get_text().strip().lower()
                        month = month_map.get(month_ru, '01')

                        # Год - текущий или следующий
                        current_year = datetime.now().year
                        # Если месяц декабрь и сейчас конец года, возможно следующий год
                        if month_ru == 'декабрь' and datetime.now().month >= 10:
                            year = current_year + 1
                        else:
                            year = current_year

                        # Формируем дату
                        try:
                            date_str = f"{year}-{month}-{int(day):02d}T19:00:00"
                            dates.append(date_str)
                        except:
                            pass

        # 2. Если не нашли через расписание, ищем другими способами
        if not dates:
            if json_data:
                for field in ['startDate', 'endDate']:
                    if field in json_data:
                        date_str = json_data[field]
                        formatted = self.cleaner.format_date(date_str)
                        if formatted and formatted not in dates:
                            dates.append(formatted)

            if not dates:
                scripts = soup.find_all('script')
                for script in scripts:
                    if script.string:
                        iso_dates = re.findall(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}', script.string)
                        for date_str in iso_dates:
                            formatted = self.cleaner.format_date(date_str)
                            if formatted and formatted not in dates:
                                dates.append(formatted)

        return sorted(set(dates))

    def extract_genre(self, soup: BeautifulSoup, json_data: Optional[Dict]) -> str:
        """Извлекает жанр"""
        name = self.extract_name(soup, json_data).lower()
        description = self.extract_description(soup, json_data).lower()

        text = name + " " + description

        genres = {
            'мюзикл': 'Мюзикл',
            'драма': 'Драма',
            'комедия': 'Комедия',
            'трагедия': 'Трагедия',
            'мелодрама': 'Мелодрама',
            'детектив': 'Детектив',
            'шоу': 'Шоу',
            'оперетта': 'Оперетта',
            'опера': 'Опера',
            'балет': 'Балет',
        }

        for keyword, genre in genres.items():
            if keyword in text:
                return genre

        return "Спектакль"

    def extract_duration(self, soup: BeautifulSoup, json_data: Optional[Dict]) -> Optional[int]:
        """Извлекает продолжительность"""
        if json_data and json_data.get('duration'):
            duration_iso = json_data['duration']
            match = re.match(r'PT(?:(\d+)H)?(?:(\d+)M)?', duration_iso)
            if match:
                hours = int(match.group(1) or 0)
                minutes = int(match.group(2) or 0)
                total = hours * 60 + minutes
                if total > 0:
                    return total

        all_text = soup.get_text()
        return self.cleaner.parse_duration(all_text)

    def extract_age_rating(self, soup: BeautifulSoup, json_data: Optional[Dict]) -> str:
        """Извлекает возрастной рейтинг"""
        text = soup.get_text()
        return self.cleaner.parse_age_rating(text)

    def extract_description(self, soup: BeautifulSoup, json_data: Optional[Dict]) -> str:
        """Извлекает полное описание"""

        # Просто ищем content-block и берем весь текст
        content_block = soup.find('div', class_='content-block')
        if content_block:
            full_text = content_block.get_text()
            full_text = re.sub(r'\s+', ' ', full_text).strip()
            return full_text

        return ""