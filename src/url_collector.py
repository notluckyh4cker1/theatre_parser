import re
from datetime import datetime, timedelta
import requests
import time
import os
import random
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import config

class URLCollector:
    def __init__(self):
        self.base_url = config.BASE_URL
        self.theater_url = config.THEATER_URL
        self.headers = config.HEADERS
        self.session = requests.Session()
        self.session.headers.update(self.headers)
        self.play_urls = set()

        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36 Edg/119.0.0.0',
        ]

    def get_soup(self, url):
        """Загружает страницу с ротацией User-Agent и случайными задержками"""
        try:
            headers = self.headers.copy()
            headers['User-Agent'] = random.choice(self.user_agents)

            time.sleep(random.uniform(2, 5))

            response = self.session.get(url, headers=headers, timeout=config.TIMEOUT)
            response.raise_for_status()

            if 'captcha' in response.text.lower() or 'доступ временно ограничен' in response.text:
                print(f"⚠️ Возможная капча на {url}")
                time.sleep(10)

            return BeautifulSoup(response.text, 'html.parser')
        except requests.RequestException as e:
            print(f"Ошибка при загрузке {url}: {e}")

            if '429' in str(e):
                print(f"⚠️ Слишком много запросов. Ждем 30 секунд...")
                time.sleep(30)

            return None

    def extract_play_urls_from_page(self, soup):
        """Извлекает ссылки на спектакли - УЛУЧШЕННАЯ ВЕРСИЯ"""
        urls = []

        # 1. Ищем ссылки в календаре/расписании
        calendar_links = soup.find_all('a', href=re.compile(r'/teatr/.*/\d{4}-\d{2}-\d{2}'))
        for link in calendar_links:
            href = link['href']
            # Извлекаем базовый URL спектакля (без даты)
            match = re.match(r'(/teatr/[^/]+)/\d{4}-\d{2}-\d{2}', href)
            if match:
                base_url = match.group(1)
                full_url = urljoin(self.base_url, base_url)
                if full_url not in urls:
                    urls.append(full_url)

        # 2. Ищем обычные ссылки на спектакли
        play_patterns = [
            r'/teatr/[^/\s]+',
            r'/event/[^/\s]+',
        ]

        all_links = soup.find_all('a', href=True)
        for link in all_links:
            href = link['href']

            for pattern in play_patterns:
                if re.match(pattern, href):
                    full_url = urljoin(self.base_url, href)

                    # Проверяем, что это действительно спектакль
                    if (full_url.startswith(f"{self.base_url}/teatr/") and
                            len(full_url) > len(self.base_url) + 10 and
                            'category' not in full_url.lower() and
                            'tag' not in full_url.lower() and
                            'author' not in full_url.lower()):

                        # Убираем дату из конца URL если есть
                        full_url = re.sub(r'/\d{4}-\d{2}-\d{2}$', '', full_url)

                        if full_url not in urls:
                            urls.append(full_url)

        return list(set(urls))

    def collect_urls_from_categories(self):
        """Собирает ссылки из разных категорий"""
        categories = [
            '',
            'myuzikl',
            'drama',
            'komediya',
            'balet',
            'opera',
            'detektiv',
            'skazki',
            'melodrama',
            'klassicheskaya-drama',
            'sovremennaya-drama',
            'veselye-komedii',
        ]

        all_urls = set()

        for category in categories:
            try:
                if category:
                    category_url = f"{self.theater_url}/{category}"
                    print(f"📂 Собираем из категории: {category}")
                else:
                    category_url = self.theater_url
                    print(f"📂 Собираем с главной страницы")

                for page in range(1, 6):
                    if page > 1:
                        if category:
                            page_url = f"{category_url}?page={page}"
                        else:
                            page_url = f"{category_url}?page={page}"
                    else:
                        page_url = category_url

                    print(f"   Страница {page}...")
                    soup = self.get_soup(page_url)

                    if not soup:
                        break

                    page_urls = self.extract_play_urls_from_page(soup)
                    all_urls.update(page_urls)

                    print(f"     Найдено {len(page_urls)} ссылок (всего: {len(all_urls)})")

                    if not self.has_next_page(soup):
                        break

                    time.sleep(random.uniform(3, 7))

            except Exception as e:
                print(f"Ошибка: {e}")
                continue

        return list(all_urls)

    def has_next_page(self, soup):
        """Проверяет есть ли следующая страница"""
        next_button = soup.find('a', text=re.compile(r'дальше|следующая|next', re.I))
        return next_button is not None

    def collect_urls_from_calendar(self):
        """Собирает ссылки на ближайшие 30 дней"""
        print("📅 Собираем ссылки по календарю (30 дней)...")

        all_urls = set()
        today = datetime.now()

        for i in range(30):  # Следующие 30 дней
            try:
                date = today + timedelta(days=i)
                date_str = date.strftime('%Y-%m-%d')
                date_url = f"{self.theater_url}/{date_str}"

                print(f"   📆 {date_str}...")

                soup = self.get_soup(date_url)

                if soup:
                    date_urls = self.extract_play_urls_from_page(soup)
                    all_urls.update(date_urls)
                    print(f"     Найдено {len(date_urls)} спектаклей")

                time.sleep(random.uniform(2, 4))

            except Exception as e:
                print(f"     ❌ Ошибка для даты {date_str}: {e}")
                continue

        return list(all_urls)

    def collect_all_urls(self):
        """Основной метод сбора всех ссылок"""
        print("🚀 Начинаем агрессивный сбор ссылок...")

        all_urls = set()

        # Метод 1: Категории
        print("\n📋 Метод 1: Сбор по категориям")
        category_urls = self.collect_urls_from_categories()
        all_urls.update(category_urls)
        print(f"✅ Категории: {len(category_urls)} ссылок")

        # Метод 2: Календарь
        print("\n📅 Метод 2: Сбор по календарю")
        calendar_urls = self.collect_urls_from_calendar()
        all_urls.update(calendar_urls)
        print(f"✅ Календарь: {len(calendar_urls)} ссылок")

        # Метод 3: Популярные страницы
        print("\n🔥 Метод 3: Популярные спектакли")
        popular_urls = self.collect_popular_urls()
        all_urls.update(popular_urls)
        print(f"✅ Популярные: {len(popular_urls)} ссылок")

        urls = list(all_urls)
        print(f"\n🎉 ИТОГО собрано: {len(urls)} уникальных ссылок")

        if urls:
            self.save_urls_to_file(urls)

        return urls

    def collect_popular_urls(self):
        """Собирает ссылки на популярные спектакли"""
        popular_pages = [
            f"{self.theater_url}/popular",
            f"{self.theater_url}/recommendations",
            f"{self.theater_url}/best",
        ]

        urls = set()

        for page_url in popular_pages:
            try:
                soup = self.get_soup(page_url)
                if soup:
                    page_urls = self.extract_play_urls_from_page(soup)
                    urls.update(page_urls)
            except:
                continue

        return list(urls)

    def run(self, force_collect=True):
        """Основной метод сбора ссылок"""
        if not force_collect and os.path.exists(config.URLS_FILE):
            urls = self.load_urls_from_file()
            if urls and len(urls) >= 100:
                print(f"📖 Используем сохраненные ссылки ({len(urls)} шт)")
                return urls

        print("🔄 Начинаем сбор новых ссылок...")
        urls = self.collect_all_urls()

        if len(urls) < 50:
            print(f"⚠️ Собрано мало ссылок ({len(urls)}). Пробуем загрузить старые...")
            old_urls = self.load_urls_from_file()
            urls = list(set(urls + old_urls))

        return urls[:400]

    def save_urls_to_file(self, urls):
        """Сохраняет ссылки в файл"""
        try:
            os.makedirs(os.path.dirname(config.URLS_FILE), exist_ok=True)

            with open(config.URLS_FILE, 'w', encoding='utf-8') as f:
                for url in urls:
                    f.write(url + '\n')
            print(f"💾 Сохранено {len(urls)} ссылок в {config.URLS_FILE}")
        except Exception as e:
            print(f"❌ Ошибка при сохранении ссылок: {e}")

    def load_urls_from_file(self):
        """Загружает ссылки из файла"""
        try:
            if os.path.exists(config.URLS_FILE):
                with open(config.URLS_FILE, 'r', encoding='utf-8') as f:
                    urls = [line.strip() for line in f if line.strip()]
                return urls
        except Exception as e:
            print(f"❌ Ошибка при загрузке ссылок: {e}")
        return []