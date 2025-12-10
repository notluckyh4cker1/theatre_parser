import re
from datetime import datetime
from typing import List, Optional

class DataCleaner:
    @staticmethod
    def clean_text(text: str) -> str:
        """Очищает текст"""
        if not text:
            return ""

        text = re.sub(r'<[^>]+>', '', text)
        text = re.sub(r'\s+', ' ', text).strip()
        return text

    @staticmethod
    def extract_actors_from_pattern(text: str) -> List[str]:
        """Извлекает актеров из паттерна 'В ролях: ...'"""
        actors = []

        pattern = r'В\s+ролях[:\s]+([^\.\n]+)'
        match = re.search(pattern, text, re.IGNORECASE)

        if not match:
            return actors

        actors_text = match.group(1)

        actors_text = re.sub(r'\([^)]*\)', '', actors_text)

        split_pattern = r'[,;/]|\s+и\s+'
        parts = re.split(split_pattern, actors_text)

        for part in parts:
            actor = part.strip()

            # Очищаем от лишних символов
            actor = re.sub(r'[^\w\s\-\.]', ' ', actor)
            actor = re.sub(r'\s+', ' ', actor).strip()

            # Проверяем что это имя
            if (actor and len(actor) > 3 and
                    len(actor.split()) >= 2 and
                    re.search(r'[А-ЯЁа-яё]', actor)):

                # Приводим к нормальному виду
                if actor.isupper():
                    actor = actor.title()

                if actor not in actors:
                    actors.append(actor)

        return actors

    @staticmethod
    def extract_actors_with_roles(text: str) -> List[str]:
        """Извлекает актеров из текста с ролями: 'РОЛЬ – Имя Фамилия'"""
        actors = []

        pattern = r'([А-ЯЁ][А-ЯЁа-яё\s]+?)[\–\:\s]\s*([А-ЯЁ][а-яё]+\s+[А-ЯЁ][а-яё]+)'
        matches = re.findall(pattern, text)

        for match in matches:
            if len(match) == 2:
                role, actor = match
                actor = actor.strip()

                # Фильтруем: убираем если роль слишком короткая
                if len(role.strip()) < 3:
                    continue

                # Проверяем что актер - это имя
                if (actor and len(actor.split()) >= 2 and
                        re.search(r'[А-ЯЁа-яё]', actor)):

                    if actor.isupper():
                        actor = actor.title()

                    if actor not in actors:
                        actors.append(actor)

        return actors

    def clean_actors_list(self, actors: List[str]) -> List[str]:
        """Очищает список актеров от мусора - СРОЧНОЕ ИСПРАВЛЕНИЕ"""
        cleaned = []

        invalid_keywords = [
            'афиша', 'расписание', 'сертификат', 'организатор', 'реклама',
            'песни', 'христа', 'спасителя', 'октябрь', 'москвы', 'льва',
            'яшина', 'плющенко', 'евгения', 'контакты', 'залы', 'класс',
            'место', 'дом', 'актера', 'театр', 'имени', 'им', 'сцена',
            'центральный', 'большой', 'зал', 'историческая', 'олега',
            'табакова', 'московский', 'основная', 'исполнители'
        ]

        # Шаблоны для фильтрации
        invalid_patterns = [
            r'^[А-ЯЁ]+\s+[А-ЯЁ][а-яё]+$',
            r'театр.*им',
            r'сртеатр',
        ]

        invalid_phrases = [
            'театр', 'зал', 'сцена', 'дом музыки', 'елисейских',
            'московский', 'международный', 'центральный', 'граф орлов',
            'солнце ландау', 'основная сцена', 'вахтангова'
        ]

        for actor in actors:
            actor = actor.strip()

            # Пропускаем пустые
            if not actor or len(actor) < 3:
                continue

            # Пропускаем если содержит недопустимые слова
            actor_lower = actor.lower()
            if any(keyword in actor_lower for keyword in invalid_keywords):
                continue

            # Пропускаем если соответствует недопустимым паттернам
            if any(re.search(pattern, actor_lower) for pattern in invalid_patterns):
                continue

            # Пропускаем если это не имя
            if (re.search(r'\d', actor) or
                    len(actor.split()) < 2 or
                    len(actor) > 30):
                continue

            # Проверяем что имя содержит русские буквы
            if not re.search(r'[А-ЯЁ][а-яё]+\s+[А-ЯЁ][а-яё]+', actor):
                continue

            # Убираем лишние символы
            actor = re.sub(r'[^\w\s\-\.]', ' ', actor)
            actor = re.sub(r'\s+', ' ', actor).strip()

            # Приводим к нормальному виду
            words = actor.split()
            normalized_words = []
            for word in words:
                if word.isupper():
                    normalized_words.append(word.title())
                elif word and word[0].isupper():
                    normalized_words.append(word)
                else:
                    normalized_words.append(word.capitalize())

            actor = ' '.join(normalized_words)

            # Убираем возможные дубли (Анны -> Анна)
            actor_base = re.sub(r'ы$', 'а', actor)
            actor_base = re.sub(r'и$', 'я', actor_base)

            if actor not in cleaned and actor_base not in cleaned:
                cleaned.append(actor)

        return cleaned

    @staticmethod
    def extract_director_from_text(text: str) -> Optional[str]:
        """Извлекает режиссера"""
        if not text:
            return None

        # Сначала ищем "Режиссер" явно
        if 'Режиссер' in text:
            idx = text.find('Режиссер')
            context = text[idx:idx + 200]

            patterns = [
                r'Режисс[её]р\s*[—–\-:]\s*([А-ЯЁ][а-яё]+\s+[А-ЯЁ][а-яё]+)',
                r'Режиссер\s*[—–\-]\s*([А-ЯЁ][а-яё]+\s+[А-ЯЁ][а-яё]+)',
                r'Режиссер\s*:\s*([А-ЯЁ][а-яё]+\s+[А-ЯЁ][а-яё]+)',
            ]

            for pattern in patterns:
                match = re.search(pattern, context, re.IGNORECASE)
                if match:
                    director = match.group(1).strip()

                    # ФИЛЬТРАЦИЯ: недопустимые слова в имени
                    invalid_words = ['место', 'проведения', 'зал', 'театр', 'сцена']
                    director_lower = director.lower()

                    if not any(word in director_lower for word in invalid_words):
                        return director

        # Также ищем другие варианты
        patterns = [
            r'Режисс[её]р[:\s—\-]+\s*([А-ЯЁ][а-яё]+\s+[А-ЯЁ][а-яё]+)',
            r'Постановка[:\s—\-]+\s*([А-ЯЁ][а-яё]+\s+[А-ЯЁ][а-яё]+)',
        ]

        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                director = match.group(1).strip()

                # Фильтрация
                invalid_words = ['место', 'проведения', 'зал', 'театр']
                director_lower = director.lower()

                if not any(word in director_lower for word in invalid_words):
                    return director

        return None

    @staticmethod
    def parse_duration(text: str) -> Optional[int]:
        """Парсит продолжительность"""
        if not text:
            return None

        # Ищем "Продолжительность:"
        duration_match = re.search(r'Продолжительность[:\s]*([^\n\.]+)', text, re.IGNORECASE)
        if duration_match:
            duration_text = duration_match.group(1)

            hours = minutes = 0

            hour_match = re.search(r'(\d+)\s*час', duration_text)
            if hour_match:
                hours = int(hour_match.group(1))

            minute_match = re.search(r'(\d+)\s*минут', duration_text)
            if minute_match:
                minutes = int(minute_match.group(1))

            total = hours * 60 + minutes
            if total > 0:
                return total

        return None

    @staticmethod
    def parse_age_rating(text: str) -> str:
        """Извлекает возрастной рейтинг"""
        match = re.search(r'(\d{1,2})\+', text)
        if match:
            return match.group(1) + "+"

        return "0+"

    @staticmethod
    def format_date(date_str: str) -> Optional[str]:
        """Форматирует дату в ISO формат"""
        if not date_str:
            return None

        try:
            # Если уже в ISO формате
            if 'T' in date_str:
                parts = date_str.split('T')
                if len(parts) == 2:
                    date_part = parts[0]
                    time_part = parts[1].split('+')[0]

                    # Проверяем что время корректное
                    if ':' in time_part:
                        time_parts = time_part.split(':')
                        if len(time_parts) >= 2:
                            hour = int(time_parts[0])
                            minute = int(time_parts[1])
                            if 0 <= hour <= 23 and 0 <= minute <= 59:
                                return f"{date_part}T{time_part}"


            return None
        except:
            return None

    @staticmethod
    def fix_stuck_names(text: str) -> str:
        """Исправляет слипшиеся имена типа 'Талгат БаталовШанель' -> 'Талгат Баталов'"""
        pattern = r'([а-яё])([А-ЯЁ])'

        def replace_func(match):
            return match.group(1) + ' ' + match.group(2)

        fixed = re.sub(pattern, replace_func, text)

        # Теперь берем только первые два слова (Имя Фамилия)
        words = fixed.split()
        if len(words) >= 2:
            return ' '.join(words[:2])

        return fixed