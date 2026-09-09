# -*- coding: utf-8 -*-
"""
core/freelance_exchange_scraper.py — Парсинг фриланс-бирж (RSS + HTML).

Реализует:
- FreelanceOrder (dataclass) — структурированный заказ с биржи
- ExchangeRSSReceiver — приёмник RSS/Atom лент (stdlib xml.etree)
- ExchangeHtmlReceiver — приёмник SSR-страниц каталогов (aiohttp + BS4)

Оба ресивера реализуют BaseMessageReceiver из core.message_monitor,
поэтому подключаются к существующему FreelanceOrderFilter БЕЗ его изменений:
ресивер генерирует on_message(raw_text, source_id, message_link, chat_title,
sender_username), а фильтр делает MD5-дедуп → ключевые слова → LLM-оценку.

Интервал опроса — по паттерну Watchdog._run_loop (asyncio-цикл с sleep).

Зависимости: aiohttp (есть), beautifulsoup4 (добавлен в requirements.txt).
"""
from __future__ import annotations

import asyncio
import html
import json
import re
from abc import ABC
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional
from urllib.parse import urljoin, urlparse

from loguru import logger

try:
    import aiohttp
    HAS_AIOHTTP = True
except ImportError:  # pragma: no cover
    aiohttp = None
    HAS_AIOHTTP = False

try:
    from bs4 import BeautifulSoup
    HAS_BS4 = True
except ImportError:  # pragma: no cover
    BeautifulSoup = None
    HAS_BS4 = False

from core.message_monitor import BaseMessageReceiver


# ═══════════════════════════════════════════════════════════════
#  Структура заказа
# ═══════════════════════════════════════════════════════════════


@dataclass
class FreelanceOrder:
    """Структурированный заказ с биржи фриланса.

    Парсеры наполняют эти поля; сырой текст для FreelanceOrderFilter
    генерируется через to_raw_text().
    """
    title: str = ""
    description: str = ""
    budget: Optional[str] = None          # "5 000 ₽", "$50", ...
    currency: Optional[str] = None        # RUB / USD / EUR / None
    deadline: Optional[str] = None        # "3 дня", дата, ...
    url: str = ""
    source: str = ""                      # kwork, fl_ru, ...
    tags: List[str] = field(default_factory=list)
    extra: Dict[str, Any] = field(default_factory=dict)
    # ── Расширенные поля (Фаза: полнота данных) ──
    # Автор заказа (username/ник) — для контакта и HITL-диалога.
    author_username: Optional[str] = None
    # Дата публикации (datetime) — из RSS pubDate / HTML published.
    published_at: Optional[datetime] = None
    # Конкуренция: сколько откликов уже есть на заказ.
    responses_count: Optional[int] = None
    # Популярность: сколько просмотров у заказа.
    views_count: Optional[int] = None
    # Выделенный заказ (продвижение на бирже) — может говорить о
    # срочности/бюджете.
    is_featured: bool = False

    def to_raw_text(self) -> str:
        """Сырой текст для FreelanceOrderFilter (Pass 1/2)."""
        parts = [f"📌 {self.title}"]
        if self.description:
            parts.append(self.description)
        if self.budget:
            parts.append(f"💰 Бюджет: {self.budget}")
        if self.deadline:
            parts.append(f"⏱ Дедлайн: {self.deadline}")
        if self.author_username:
            parts.append(f"👤 Автор: {self.author_username}")
        if self.responses_count is not None:
            parts.append(f"📊 Откликов: {self.responses_count}")
        if self.views_count is not None:
            parts.append(f"👁 Просмотров: {self.views_count}")
        if self.tags:
            parts.append(f"🛠 Теги: {', '.join(self.tags)}")
        parts.append(f"📡 Источник: {self.source}")
        if self.url:
            parts.append(f"🔗 {self.url}")
        return "\n".join(parts)

    def to_json(self) -> str:
        """Сериализация (для тестов и логирования)."""
        return json.dumps(
            {
                "title": self.title,
                "description": self.description,
                "budget": self.budget,
                "currency": self.currency,
                "deadline": self.deadline,
                "url": self.url,
                "source": self.source,
                "tags": self.tags,
                "extra": self.extra,
                "author_username": self.author_username,
                "published_at": (
                    self.published_at.isoformat()
                    if self.published_at else None
                ),
                "responses_count": self.responses_count,
                "views_count": self.views_count,
                "is_featured": self.is_featured,
            },
            ensure_ascii=False,
        )


# ═══════════════════════════════════════════════════════════════
#  Базовый класс веб-ресивера
# ═══════════════════════════════════════════════════════════════


class BaseExchangeReceiver(BaseMessageReceiver, ABC):
    """Общая логика для веб-ресиверов: цикл опроса, дедуп, on_message.

    Подклассы реализуют:
        async def _fetch_orders(self) -> List[FreelanceOrder]
    """

    def __init__(
        self,
        sources: List[Dict[str, Any]],
        interval_sec: int = 600,
        max_orders_per_run: int = 60,
        timeout_sec: float = 10.0,
    ):
        """
        Args:
            sources: Список источников [{"url": ..., "name": ..., "tags": [...]}].
            interval_sec: Интервал опроса в секундах.
            max_orders_per_run: Максимум заказов за один цикл (на источник).
            timeout_sec: Таймаут HTTP-запроса.
        """
        self.sources = sources or []
        self.interval_sec = interval_sec
        self.max_orders_per_run = max_orders_per_run
        self.timeout_sec = timeout_sec

        self.on_message = None
        self._is_running = False
        self._loop_task: Optional[asyncio.Task] = None

        # Дедуп по (source, url): URL заказа уникален на бирже
        self._seen_urls: Dict[str, float] = {}  # key -> timestamp (для LRU)

        # Опциональный Redis для персистентного дедупа (переживает рестарты).
        # Устанавливается через set_redis() из main.py; None — только in-memory.
        self._redis = None

    def set_redis(self, redis_client) -> None:
        """Подключить Redis-клиент для персистентного дедупа (необязательно).

        redis_client должен поддерживать корутинный `sadd` (redis.asyncio.Redis).
        При передаче None дедуп работает только в памяти процесса.
        """
        self._redis = redis_client

    # ── BaseMessageReceiver ─────────────────────────────────

    async def start(self) -> None:
        """Запустить цикл опроса источников."""
        if self._is_running:
            logger.warning(f"{type(self).__name__} уже запущен — пропускаем")
            return
        self._is_running = True
        self._loop_task = asyncio.create_task(self._run_loop())
        logger.info(
            f"{type(self).__name__} запущен: {len(self.sources)} источников, "
            f"интервал {self.interval_sec}с"
        )

    async def stop(self) -> None:
        """Остановить цикл опроса."""
        if not self._is_running:
            return
        self._is_running = False
        if self._loop_task is not None:
            self._loop_task.cancel()
            try:
                await asyncio.wait_for(self._loop_task, timeout=5.0)
            except (asyncio.CancelledError, asyncio.TimeoutError):
                pass
            self._loop_task = None
        logger.info(f"{type(self).__name__} остановлен")

    @property
    def is_running(self) -> bool:
        return self._is_running

    async def validate_chats(self) -> Dict[str, bool]:
        """Проверить доступность источников (ping по URL)."""
        status: Dict[str, bool] = {}
        if not HAS_AIOHTTP:
            for src in self.sources:
                status[src.get("name") or src.get("url")] = False
            return status

        async with aiohttp.ClientSession() as session:
            for src in self.sources:
                url = src.get("url")
                name = src.get("name") or url
                status[name] = False
                if not url:
                    continue
                try:
                    async with session.get(
                        url,
                        timeout=aiohttp.ClientTimeout(total=min(self.timeout_sec, 5)),
                    ) as resp:
                        status[name] = resp.status == 200
                except Exception:
                    status[name] = False
        return status

    # ── Цикл опроса ─────────────────────────────────────────

    async def _run_loop(self) -> None:
        """Основной цикл: fetch → emit (по паттерну Watchdog)."""
        while self._is_running:
            try:
                await self._poll_once()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(
                    f"{type(self).__name__}: ошибка цикла опроса: {e}"
                )
            await asyncio.sleep(self.interval_sec)

    async def _poll_once(self) -> None:
        """Один проход: собрать заказы со всех источников и эмитить."""
        for src in self.sources:
            url = src.get("url")
            if not url:
                continue
            try:
                orders = await self._fetch_orders(src)
                for order in orders[: self.max_orders_per_run]:
                    await self._emit_order(order, src)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning(
                    f"{type(self).__name__}: ошибка источника {url}: {e}"
                )

    async def _fetch_orders(self, source: Dict[str, Any]) -> List[FreelanceOrder]:
        """Загрузить и распарсить заказы источника (реализует подкласс)."""
        raise NotImplementedError

    # ── Эмиссия в фильтр ────────────────────────────────────

    async def _emit_order(self, order: FreelanceOrder, source: Dict[str, Any]) -> None:
        """Отправить заказ через on_message (с дедупом по URL).

        Дедуп:
        - in-memory кэш _seen_urls (быстрый, для одного процесса);
        - опциональный Redis-сет (freelance_orders:seen) — переживает рестарты
          и дедуплицирует между ресиверами (RSS/HTML/Playwright).
        """
        if not order.title and not order.description:
            return

        # Ключ дедупа — только URL (без префикса источника): один и тот же заказ
        # может прийти из RSS и HTML/Playwright-источника с разными source-name,
        # и префикс источника ломал бы кросс-источниковый дедуп. URL уникален
        # на бирже; для заказов без URL используем заголовок.
        dedup_key = order.url or order.title
        now = datetime.now(timezone.utc).timestamp()

        # LRU-очистка: держим максимум 5000 записей
        if len(self._seen_urls) > 5000:
            oldest = sorted(self._seen_urls.values())[:1000]
            cutoff = oldest[-1] if oldest else now
            for k, ts in list(self._seen_urls.items()):
                if ts <= cutoff:
                    del self._seen_urls[k]

        if dedup_key in self._seen_urls:
            logger.debug(f"Дубль заказа отсечён: {dedup_key[:80]}")
            return

        # Redis-дедуп (переживает рестарты, работает между ресиверами).
        # Дневной ключ + TTL: старые записи естественно вымываются.
        # При недоступности Redis молча fallback на in-memory кэш.
        redis_seen = False
        if self._redis is not None:
            try:
                day_key = datetime.now(timezone.utc).strftime(
                    "freelance_orders:seen:%Y-%m-%d"
                )
                ok = await self._redis.sadd(day_key, dedup_key)
                if not ok:
                    redis_seen = True
                # TTL на дневной ключ при каждой вставке (3 дня): дёшево (O(1)),
                # и ключ гарантированно вымывается даже при малом числе заказов.
                await self._redis.expire(day_key, 3 * 86400)
            except Exception:
                self._redis = None  # отключаем насовсем при ошибке
        if redis_seen:
            logger.debug(f"Дубль заказа отсечён (Redis): {dedup_key[:80]}")
            return

        self._seen_urls[dedup_key] = now

        if self.on_message is None:
            logger.debug("on_message не установлен — заказ не эмитим")
            return

        raw_text = order.to_raw_text()
        sender = None
        # Автор заказа: приоритет — расширенное поле author_username
        # (RSS/HTML/Playwright-парсеры), fallback — extra["author"].
        if order.author_username:
            sender = order.author_username
        elif order.extra and order.extra.get("author"):
            sender = order.extra["author"]

        try:
            await self.on_message(
                raw_text=raw_text,
                source_id=order.url or order.source,
                message_link=order.url or "",
                chat_title=f"{order.source}: {source.get('name') or order.source}",
                sender_username=sender,
            )
            logger.info(f"Заказ эмитирован: {order.title[:60]}")
        except Exception as e:
            logger.warning(f"Ошибка on_message для заказа: {e}")


# ═══════════════════════════════════════════════════════════════
#  RSS/Atom ресивер
# ═══════════════════════════════════════════════════════════════


class ExchangeRSSReceiver(BaseExchangeReceiver):
    """Приёмник RSS/Atom лент фриланс-бирж.

    Парсит XML через stdlib xml.etree (без внешних зависимостей).
    Формат item:
        <item><title>...</title><link>...</link><description>...</description>
              <category>...</category><pubDate>...</pubDate></item>
    """

    async def _fetch_orders(self, source: Dict[str, Any]) -> List[FreelanceOrder]:
        url = source.get("url")
        if not url:
            return []
        if not HAS_AIOHTTP:
            logger.warning("aiohttp недоступен — RSS не грузится")
            return []

        text = await self._http_get(url)
        if not text or text in ("TIMEOUT_ERROR", "CONNECTION_ERROR"):
            logger.warning(f"RSS: не удалось загрузить {url}: {text}")
            return []

        orders = self.parse_rss(
            text=text,
            source_name=source.get("name") or url,
            base_url=url,
            extra_tags=source.get("tags") or [],
        )
        logger.info(
            f"ExchangeRSSReceiver: {url} → {len(orders)} заказов"
        )
        return orders

    async def _http_get(self, url: str) -> str:
        """GET с таймаутом, возвращает текст или код ошибки."""
        try:
            async with aiohttp.ClientSession(
                headers={"User-Agent": "FreelanceAgent/1.0"}
            ) as session:
                async with session.get(
                    url,
                    timeout=aiohttp.ClientTimeout(total=self.timeout_sec),
                ) as resp:
                    if resp.status == 200:
                        return await resp.text()
                    return "CONNECTION_ERROR"
        except asyncio.TimeoutError:
            return "TIMEOUT_ERROR"
        except Exception:
            return "CONNECTION_ERROR"

    @staticmethod
    def parse_rss(
        text: str,
        source_name: str,
        base_url: str = "",
        extra_tags: Optional[List[str]] = None,
    ) -> List[FreelanceOrder]:
        """Распарсить XML RSS/Atom в список заказов (чистая функция).

        Args:
            text: XML-текст ленты.
            source_name: Имя источника (для поля source заказа).
            base_url: Базовый URL (для резолва относительных ссылок).
            extra_tags: Дополнительные теги из конфига источника.

        Returns:
            Список FreelanceOrder.
        """
        import xml.etree.ElementTree as ET

        orders: List[FreelanceOrder] = []
        try:
            root = ET.fromstring(text)
        except ET.ParseError as e:
            logger.warning(f"RSS: ошибка парсинга XML от {source_name}: {e}")
            return orders

        # Ищем items: RSS (channel/item) и Atom (feed/entry)
        items = root.findall(".//item")
        if not items:
            items = root.findall(".//{*}entry")
        if not items:
            items = root.findall(".//entry")

        for item in items:
            order = ExchangeRSSReceiver._parse_item(
                item=item,
                source_name=source_name,
                base_url=base_url,
                extra_tags=extra_tags or [],
            )
            if order.title or order.description:
                orders.append(order)

        return orders

    @staticmethod
    def _parse_item(
        item: Any,
        source_name: str,
        base_url: str,
        extra_tags: List[str],
    ) -> FreelanceOrder:
        """Распарсить один item RSS/Atom в FreelanceOrder."""
        def _text(tag: str) -> str:
            """Достать текст по тегу (с учётом namespace-префиксов)."""
            # Прямой тег
            el = item.find(tag)
            if el is not None and el.text:
                return el.text.strip()
            # Любой тег с таким локальным именем (namespace-safe)
            for child in item.iter():
                local = child.tag.split("}")[-1] if "}" in child.tag else child.tag
                if local == tag and child.text:
                    return child.text.strip()
            return ""

        title = html.unescape(_text("title"))
        link = html.unescape(_text("link")) or ""
        # Atom: <link href="..."/> — ссылка в атрибуте, а не в тексте.
        # Ищем link с атрибутом href в любом namespace.
        if not link:
            for child in item.iter():
                local = child.tag.split("}")[-1] if "}" in child.tag else child.tag
                if local == "link" and child.get("href"):
                    link = html.unescape(child.get("href"))
                    break
        if link and base_url and link.startswith("/"):
            link = urljoin(base_url, link)

        # Описание: description (RSS) или summary/content (Atom)
        description = html.unescape(_text("description"))
        if not description:
            description = html.unescape(_text("summary"))
        if not description:
            description = html.unescape(_text("content"))
        # Срезаем HTML-разметку
        description = re.sub(r"<[^>]+>", " ", description)
        description = re.sub(r"\s+", " ", description).strip()

        # Теги: category (RSS) / {atom}category.
        # ВНИМАНИЕ: теги источника из конфига (extra_tags) НЕ добавляем в заказ —
        # они попадают в to_raw_text() и "легитимируют" любой заказ для фильтра
        # по ключевым словам (Pass 1) и для LLM-оценки (Pass 2), т.к. всегда
        # содержат keywords (python, bot, backend...). Реальные категории ленты
        # оставляем — они описывают сам заказ.
        tags: List[str] = []
        for cat in item.iter():
            local = cat.tag.split("}")[-1] if "}" in cat.tag else cat.tag
            if local == "category":
                tag_text = None
                if cat.text and cat.text.strip():
                    tag_text = cat.text.strip()
                elif cat.get("term"):  # Atom: <category term="..."/>
                    tag_text = cat.get("term").strip()
                if tag_text:
                    tags.append(tag_text)
        tags = list(dict.fromkeys(tags))[:10]

        pub_date = html.unescape(_text("pubDate")) or html.unescape(_text("updated"))

        # Автор (RSS: dc:creator / author / Atom: <author><name>)
        author = None
        for child in item.iter():
            local = child.tag.split("}")[-1] if "}" in child.tag else child.tag
            if local == "creator" and child.text and child.text.strip():
                author = child.text.strip()
                break
            if local == "author" and child.text and child.text.strip():
                author = child.text.strip()
                break
        if not author:
            name_el = item.find(".//{*}name")
            if name_el is not None and name_el.text and name_el.text.strip():
                author = name_el.text.strip()

        # Извлекаем бюджет из текста (грубая эвристика)
        budget, currency = ExchangeRSSReceiver._extract_budget(
            title + " " + description
        )

        return FreelanceOrder(
            title=title or "Без названия",
            description=description,
            budget=budget,
            currency=currency,
            deadline=pub_date or None,
            url=link,
            source=source_name,
            tags=tags,
            extra={"pub_date": pub_date} if pub_date else {},
            author_username=author,
            published_at=ExchangeRSSReceiver._parse_pub_date(pub_date),
        )

    @staticmethod
    def _parse_pub_date(raw: Optional[str]) -> Optional[datetime]:
        """Распарсить pubDate/updated из RSS/Atom в aware datetime (UTC).

        Поддерживает RFC 822 («Mon, 04 Aug 2025 10:00:00 GMT»), ISO 8601
        («2025-08-04T10:00:00+00:00» / «2025-08-04T10:00:00Z») и naive
        («2025-08-04 10:00:00» — считается UTC).

        Returns:
            aware datetime (UTC) или None.
        """
        if not raw:
            return None
        text = str(raw).strip()
        # RFC 822 (email.utils.parsedate_to_datetime) — aware
        try:
            from email.utils import parsedate_to_datetime

            dt = parsedate_to_datetime(text)
            if dt is not None:
                return dt
        except (TypeError, ValueError, IndexError, OverflowError):
            pass
        # ISO 8601
        try:
            dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        except ValueError:
            return None

    @staticmethod
    def _extract_budget(text: str):
        """Извлечь (бюджет, валюта) из текста грубой эвристикой.

        Ищет паттерны: "5 000 ₽", "5000 руб", "$50", "150 $", "3000-5000 руб" и т.д.
        Поддерживает диапазоны ("3000-5000 руб"), тысячи с пробелами и валюту
        как перед числом ("$50"), так и после ("150 $", "500 €", "5 000 ₽").
        """
        # число (с диапазоном и разделителями тысяч)
        num = r"(\d[\d\s]*\d?(?:\s*[-–]\s*\d[\d\s]*\d?)?)"
        patterns = [
            # 5 000 ₽ / 5000 руб. / 150 р. (валюта после числа)
            (num + r"\s*(₽|руб\.?|rub|р\.)", "RUB"),
            # $50 / $ 50 (валюта перед числом)
            (r"\$\s*" + num, "USD"),
            # 150 $ / 150$ (валюта после числа)
            (num + r"\s*\$", "USD"),
            # 500 € / 500eur (валюта после числа)
            (num + r"\s*(€|eur)", "EUR"),
        ]
        for pat, currency in patterns:
            m = re.search(pat, text, flags=re.IGNORECASE)
            if m:
                amount = m.group(1).strip()
                if amount:
                    # Нормализуем: сжимаем внутренние пробелы, но сохраняем
                    # разделители тысяч ("5 000" → "5 000"), диапазоны не трогаем
                    return f"{amount} {currency}", currency
        return None, None


# ═══════════════════════════════════════════════════════════════
#  HTML-ресивер (SSR-страницы каталогов)
# ═══════════════════════════════════════════════════════════════


class ExchangeHtmlReceiver(BaseExchangeReceiver):
    """Приёмник SSR-страниц каталогов заказов.

    Парсит HTML через BeautifulSoup. Селекторы настраиваются в конфиге
    источника (см. source["selectors"]).

    Дефолтные селекторы (настраиваются через source["selectors"]):
        container: CSS-селектор карточки заказа
        title:     селектор заголовка (CSS или XPath-подобный)
        link:      селектор ссылки
        budget:    селектор бюджета
        description: селектор описания
    """

    DEFAULT_SELECTORS: Dict[str, str] = {
        "container": ".order, .freelance-order, article, .card, .post",
        "title": "h2, h3, .title, .order-title, .post-title",
        "link": "a",
        "budget": ".budget, .price, .order-price, .cost",
        "description": ".description, .order-description, .text, p",
        # Опциональные: извлекаются, если присутствуют на карточке.
        "deadline": ".deadline, .time, .wants-card__informers-row span.mr8, [class*=deadline]",
        "author": ".author, .user-name, .want-payer-statistic [class*=''], .seller",
        "tags": ".tags, .category, .wants-card__tags, [class*=tag]",
        # ── Расширенные поля (полнота данных) ──
        # Ссылка на профиль автора (для контакта).
        "author_link": "a.author, a.user-name, a[href*='/user/'], a[href*='/users/']",
        # Счётчики: количество откликов и просмотров.
        "responses": ".responses, .replies, .offers-count, [class*=response], "
                    "[class*=reply], .wants-card__informers-row span",
        "views": ".views, .view-count, [class*=view]",
        # Дата публикации (текст в относительном формате: «2 ч назад»,
        # «сегодня 14:32» или абсолютная «04.08.2025»).
        "published": ".published, .date, .time-ago, [class*=date], [class*=time]",
    }

    async def _fetch_orders(self, source: Dict[str, Any]) -> List[FreelanceOrder]:
        url = source.get("url")
        if not url:
            return []
        if not HAS_AIOHTTP:
            logger.warning("aiohttp недоступен — HTML не грузится")
            return []
        if not HAS_BS4:
            logger.warning("beautifulsoup4 недоступен — HTML не парсится")
            return []

        text = await self._http_get(url)
        if not text or text in ("TIMEOUT_ERROR", "CONNECTION_ERROR"):
            logger.warning(f"HTML: не удалось загрузить {url}: {text}")
            return []

        orders = self.parse_html(
            text=text,
            source_name=source.get("name") or url,
            base_url=url,
            selectors=source.get("selectors") or {},
        )
        logger.info(
            f"ExchangeHtmlReceiver: {url} → {len(orders)} заказов"
        )
        return orders

    async def _http_get(self, url: str) -> str:
        """GET с таймаутом, возвращает текст или код ошибки."""
        try:
            async with aiohttp.ClientSession(
                headers={"User-Agent": "FreelanceAgent/1.0"}
            ) as session:
                async with session.get(
                    url,
                    timeout=aiohttp.ClientTimeout(total=self.timeout_sec),
                ) as resp:
                    if resp.status == 200:
                        return await resp.text()
                    return "CONNECTION_ERROR"
        except asyncio.TimeoutError:
            return "TIMEOUT_ERROR"
        except Exception:
            return "CONNECTION_ERROR"

    @staticmethod
    def parse_html(
        text: str,
        source_name: str,
        base_url: str = "",
        selectors: Optional[Dict[str, str]] = None,
    ) -> List[FreelanceOrder]:
        """Распарсить HTML-каталог в список заказов (чистая функция).

        Args:
            text: HTML-страница.
            source_name: Имя источника.
            base_url: Базовый URL (для резолва относительных ссылок).
            selectors: Переопределения селекторов.

        Returns:
            Список FreelanceOrder.
        """
        if not HAS_BS4:
            return []

        sel = dict(ExchangeHtmlReceiver.DEFAULT_SELECTORS)
        if selectors:
            sel.update({k: v for k, v in selectors.items() if v})

        soup = BeautifulSoup(text, "html.parser")
        containers = soup.select(sel["container"]) or [soup]

        orders: List[FreelanceOrder] = []
        for container in containers[:50]:
            order = ExchangeHtmlReceiver._parse_container(
                container=container,
                source_name=source_name,
                base_url=base_url,
                selectors=sel,
            )
            if order.title or order.description:
                orders.append(order)

        return orders

    @staticmethod
    def _parse_container(
        container: Any,
        source_name: str,
        base_url: str,
        selectors: Dict[str, str],
    ) -> FreelanceOrder:
        """Распарсить одну карточку заказа.

        Извлекает title, link, budget, description (обязательные) и,
        если селекторы заданы, deadline/author/tags (опциональные).
        """
        title_el = container.select_one(selectors["title"]) if selectors.get("title") else None
        title = title_el.get_text(" ", strip=True) if title_el else ""

        link_el = container.select_one(selectors["link"]) if selectors.get("link") else None
        link = ""
        if link_el and link_el.has_attr("href"):
            link = link_el["href"]
            if link and base_url and link.startswith("/"):
                link = urljoin(base_url, link)

        budget_el = container.select_one(selectors["budget"]) if selectors.get("budget") else None
        budget_text = budget_el.get_text(" ", strip=True) if budget_el else ""
        budget, currency = ExchangeRSSReceiver._extract_budget(budget_text or title)

        desc_el = container.select_one(selectors["description"]) if selectors.get("description") else None
        description = desc_el.get_text(" ", strip=True) if desc_el else ""
        description = re.sub(r"\s+", " ", description).strip()

        # Опциональные поля (deadline/author/tags) — только если селектор задан
        # и элемент найден; иначе остаются пустыми.
        deadline = None
        if selectors.get("deadline"):
            dl_el = container.select_one(selectors["deadline"])
            if dl_el:
                dl_text = dl_el.get_text(" ", strip=True)
                deadline = re.sub(r"\s+", " ", dl_text).strip() or None

        author = None
        if selectors.get("author"):
            auth_el = container.select_one(selectors["author"])
            if auth_el:
                auth_text = auth_el.get_text(" ", strip=True)
                author = re.sub(r"\s+", " ", auth_text).strip() or None

        # ── Расширенные поля (полнота данных) ──
        # Ссылка на профиль автора (для контакта/HITL-диалога).
        author_link = None
        if selectors.get("author_link"):
            al_el = container.select_one(selectors["author_link"])
            if al_el and al_el.has_attr("href"):
                href = al_el["href"]
                if href and base_url and href.startswith("/"):
                    href = urljoin(base_url, href)
                author_link = href or None

        # Количество откликов (конкуренция). Число из текста элемента.
        responses_count = None
        if selectors.get("responses"):
            for r_el in container.select(selectors["responses"]):
                r_text = r_el.get_text(" ", strip=True)
                m = re.search(r"(\d+)", r_text)
                if m:
                    responses_count = int(m.group(1))
                    break

        # Количество просмотров.
        views_count = None
        if selectors.get("views"):
            for v_el in container.select(selectors["views"]):
                v_text = v_el.get_text(" ", strip=True)
                m = re.search(r"(\d+)", v_text)
                if m:
                    views_count = int(m.group(1))
                    break

        # Дата публикации (сырой текст) → published_at.
        published_raw = None
        if selectors.get("published"):
            p_el = container.select_one(selectors["published"])
            if p_el:
                p_text = p_el.get_text(" ", strip=True)
                published_raw = re.sub(r"\s+", " ", p_text).strip() or None

        tags: List[str] = []
        if selectors.get("tags"):
            # Селектор тегов может зацепить и сам заголовок (например, когда
            # title — тоже ссылка на категорию). Отфильтровываем элементы,
            # чей текст совпадает с title или содержит его целиком.
            title_norm = re.sub(r"\s+", " ", title).strip().lower()
            for tag_el in container.select(selectors["tags"]):
                t = tag_el.get_text(" ", strip=True)
                if not t:
                    continue
                t_norm = re.sub(r"\s+", " ", t).strip().lower()
                if title_norm and (t_norm == title_norm or title_norm in t_norm):
                    continue
                tags.append(t)
            tags = list(dict.fromkeys(tags))[:10]

        extra: Dict[str, Any] = {}
        if author:
            extra["author"] = author
        if author_link:
            extra["author_link"] = author_link
        if deadline:
            extra["deadline_raw"] = deadline
        if published_raw:
            extra["published_raw"] = published_raw

        return FreelanceOrder(
            title=title or "Без названия",
            description=description,
            budget=budget,
            currency=currency,
            deadline=deadline,
            url=link,
            source=source_name,
            tags=tags,
            extra=extra,
            author_username=author,
            published_at=ExchangeRSSReceiver._parse_pub_date(published_raw),
            responses_count=responses_count,
            views_count=views_count,
        )


# ═══════════════════════════════════════════════════════════════
#  Playwright-ресивер (SPA/JS-биржи: Kwork, FL.ru)
# ═══════════════════════════════════════════════════════════════


try:
    from playwright.async_api import async_playwright
    HAS_PLAYWRIGHT = True
except ImportError:  # pragma: no cover
    async_playwright = None
    HAS_PLAYWRIGHT = False


class ExchangePlaywrightReceiver(BaseExchangeReceiver):
    """Приёмник заказов с SPA/JS-бирж через Playwright.

    Нужен для Kwork, FL.ru и других бирж, где каталог рендерится
    JavaScript'ом и недоступен через простой HTTP (aiohttp).

    Особенности:
    - Один браузер (Chromium) на весь ресивер, переиспользуется между циклами.
    - Парсинг — через те же чистые функции (parse_cards_html), поэтому
      тестируется без запуска браузера.
    - Если Playwright или браузеры не установлены — ресивер не стартует
      (graceful: лог + return), система продолжает работу с RSS/HTML.
    - Бюджет/дедлайн/теги извлекаются из текста карточки эвристиками.

    Запуск — только для источников с "type": "playwright" в exchange_sources.

    REMOTE-РЕЖИМ (remote_mode=True):
    После переноса Playwright-скрейпера на Windows (2026-09) локальный
    Chromium на VPS не запускается. Windows-скрейпер кладёт заказы в Redis
    (LIST "freelance:remote:orders"), этот ресивер читает их оттуда
    (BLPOP-стиль) и передаёт в FreelanceOrderFilter через on_message.
    """

    def __init__(
        self,
        sources: List[Dict[str, Any]],
        interval_sec: int = 600,
        max_orders_per_run: int = 60,
        timeout_sec: float = 30.0,
        headless: bool = True,
        # ОБЫЧНЫЙ Chrome UA (БЕЗ суффикса FreelanceAgent/1.0 — он палил бота
        # перед антиботом QRATOR/SmartCaptcha на Kwork и других биржах).
        user_agent: str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 "
        "Safari/537.36",
        wait_for_selector: str = "body",
        wait_timeout_ms: int = 15000,
        js_scroll: bool = True,
        max_retries: int = 2,
        rotate_user_agent: bool = True,
        remote_mode: bool = False,
        remote_redis=None,
        remote_list_key: str = "freelance:remote:orders",
        remote_poll_interval: float = 5.0,
    ):
        super().__init__(
            sources=sources,
            interval_sec=interval_sec,
            max_orders_per_run=max_orders_per_run,
            timeout_sec=timeout_sec,
        )
        self.headless = headless
        self.user_agent = user_agent
        self.wait_for_selector = wait_for_selector
        self.wait_timeout_ms = wait_timeout_ms
        self.js_scroll = js_scroll
        self.max_retries = max_retries
        self.rotate_user_agent = rotate_user_agent

        # ── Remote-режим (воркер-скрейпер на Windows) ──
        self.remote_mode = remote_mode
        self.remote_redis = remote_redis
        self.remote_list_key = remote_list_key
        self.remote_poll_interval = remote_poll_interval
        # Счётчик для логгирования: сколько заказов получено из Redis
        self._remote_orders_received = 0

        # Пул реалистичных Chrome/Edge UA для ротации. Антибот (Kwork/QRATOR)
        # банит однообразные запросы — смена UA при каждом заходе снижает
        # вероятность блокировки. Если rotate_user_agent=True — берём
        # случайный из пула на каждую загрузку.
        self._ua_pool = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 "
            "Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/126.0.0.0 Safari/537.36 Edg/126.0.0.0",
        ]
        self._ua_index = 0

        self._playwright = None
        self._browser = None
        self._browser_ready = False

    def _pick_user_agent(self) -> str:
        """Выбрать UA для очередной загрузки.

        rotate_user_agent=True → поочерёдно из пула (стабильная ротация,
        без повторов подряд). False → фиксированный self.user_agent.
        """
        if not self.rotate_user_agent:
            return self.user_agent
        ua = self._ua_pool[self._ua_index % len(self._ua_pool)]
        self._ua_index += 1
        return ua

    async def validate_chats(self) -> Dict[str, bool]:
        """Проверить доступность Playwright-источников.

        В remote-режиме: источник считается доступным, если Redis-лист
        с remote-заказами существует (Windows-скрейпер пишет туда).
        В локальном — через РЕАЛЬНЫЙ браузер.

        Базовый BaseExchangeReceiver.validate_chats делает HTTP-запрос через
        aiohttp, но SPA-биржи (Kwork, FL.ru HTML) блокируют обычные
        запросы (403/redirect) и отдают контент только в реальном браузере.
        Поэтому для playwright-источников проверяем через Chromium:
        открываем страницу и проверяем, что есть хотя бы один контейнер.
        """
        status: Dict[str, bool] = {}

        # Remote-режим: проверяем доступность Redis-листа (без браузера)
        if self.remote_mode:
            try:
                rlen = 0
                if self.remote_redis is not None:
                    try:
                        rlen = await self.remote_redis.llen(self.remote_list_key)
                    except Exception:
                        rlen = 0
                for src in self.sources:
                    name = src.get("name") or src.get("url")
                    # Доступен, если Redis подключён (лист может быть пуст —
                    # скрейпер на Windows заполняет его по мере работы).
                    status[name] = self.remote_redis is not None
                logger.info(
                    f"{type(self).__name__} (remote): источники проверены через "
                    f"Redis-лист {self.remote_list_key} "
                    f"(len={rlen}, подключён={self.remote_redis is not None})"
                )
                return status
            except Exception as e:
                logger.warning(
                    f"{type(self).__name__} (remote): ошибка проверки Redis: {e}"
                )
                for src in self.sources:
                    status[src.get("name") or src.get("url")] = False
                return status

        if not await self._ensure_browser():
            # Браузер недоступен — помечаем все источники недоступными,
            # но не падаем (вызывающий код исключит их из опроса).
            for src in self.sources:
                status[src.get("name") or src.get("url")] = False
            return status

        for src in self.sources:
            url = src.get("url")
            name = src.get("name") or url
            status[name] = False
            if not url:
                continue
            page = None
            try:
                page = await self._browser.new_page(
                    user_agent=self._pick_user_agent()
                )
                await page.goto(
                    url,
                    wait_until="domcontentloaded",
                    timeout=self.wait_timeout_ms,
                )
                # Ждём появления селектора контейнера (или хотя бы body)
                container_sel = (
                    (src.get("selectors") or {}).get("container")
                    or self.wait_for_selector
                )
                try:
                    await page.wait_for_selector(
                        container_sel,
                        timeout=min(self.wait_timeout_ms, 10000),
                    )
                    status[name] = True
                except Exception:
                    # Контейнер не появился — проверяем, не капча/пустая ли это
                    html_len = len(await page.content())
                    status[name] = html_len > 1000
            except Exception as e:
                logger.warning(f"{type(self).__name__}: источник {name} недоступен: {e}")
                status[name] = False
            finally:
                if page is not None:
                    try:
                        await page.close()
                    except Exception:
                        pass
        return status

    # ── Жизненный цикл браузера ────────────────────────────

    async def _ensure_browser(self) -> bool:
        """Поднять Chromium (лениво, один раз). False если недоступен."""
        if self._browser_ready and self._browser is not None:
            return True
        if not HAS_PLAYWRIGHT:
            logger.warning(
                "Playwright не установлен — SPA-скрейпер недоступен "
                "(pip install playwright && playwright install chromium)"
            )
            return False

        try:
            self._playwright = await async_playwright().start()
            self._browser = await self._playwright.chromium.launch(
                headless=self.headless,
                args=["--no-sandbox", "--disable-dev-shm-usage"],
            )
            self._browser_ready = True
            logger.info(
                f"{type(self).__name__}: Chromium запущен "
                f"(headless={self.headless})"
            )
            return True
        except Exception as e:
            self._browser_ready = False
            logger.warning(
                f"{type(self).__name__}: браузер недоступен — "
                f"SPA-скрейпер пропускается: {e}"
            )
            return False

    async def stop(self) -> None:
        """Остановить цикл и закрыть браузер."""
        await super().stop()
        if self._browser is not None:
            try:
                await self._browser.close()
            except Exception:
                pass
            self._browser = None
            self._browser_ready = False
        if self._playwright is not None:
            try:
                await self._playwright.stop()
            except Exception:
                pass
            self._playwright = None

    async def _fetch_orders(self, source: Dict[str, Any]) -> List[FreelanceOrder]:
        url = source.get("url")
        if not url:
            return []

        # ── Remote-режим: читаем заказы из Redis-листа ──
        # Windows-скрейпер (playwright на Windows) кладёт JSON-заказы
        # в LIST "freelance:remote:orders" — здесь только забираем их,
        # без локального Chromium.
        if self.remote_mode:
            return await self._fetch_orders_remote(source)

        if not await self._ensure_browser():
            return []

        # Селектор контейнера из конфига (карточки заказов), иначе body.
        # Для SPA-бирж (Kwork) ждём именно карточки — иначе капча/пустая
        # страница даст пустой результат.
        container_sel = (
            (source.get("selectors") or {}).get("container")
            or self.wait_for_selector
        )

        # Несколько попыток: антибот (QRATOR/SmartCaptcha) блокирует headless
        # браузер недетерминированно — повторная попытка может пройти.
        last_error: Optional[Exception] = None
        for attempt in range(max(1, self.max_retries + 1)):
            page = None
            ctx = None
            try:
                # Stealth: скрываем признаки автоматизации (navigator.webdriver).
                # Создаём контекст с обычным Chrome-UA и stealth-скриптом.
                ctx = await self._browser.new_context(
                    user_agent=self._pick_user_agent(),
                    viewport={"width": 1366, "height": 900},
                    locale="ru-RU",
                    timezone_id="Europe/Moscow",
                )
                await ctx.add_init_script(
                    "Object.defineProperty(navigator, 'webdriver', "
                    "{get: () => undefined});"
                )
                page = await ctx.new_page()
                await page.goto(
                    url,
                    wait_until="domcontentloaded",
                    timeout=self.wait_timeout_ms,
                )
                # Ждём появления карточек (контейнер) — не просто body,
                # чтобы не парсить пустую страницу-заглушку.
                try:
                    await page.wait_for_selector(
                        container_sel,
                        timeout=min(self.wait_timeout_ms, 20000),
                    )
                except Exception:
                    logger.debug(
                        f"{type(self).__name__}: селектор {container_sel} "
                        f"не дождался (попытка {attempt + 1}) — парсим что есть"
                    )

                # Прокрутка вниз для подгрузки контента (lazy-load)
                if self.js_scroll:
                    try:
                        await page.evaluate(
                            "async () => { for (let i = 0; i < 5; i++) { "
                            "window.scrollBy(0, document.body.scrollHeight); "
                            "await new Promise(r => setTimeout(r, 500)); } }"
                        )
                        await page.wait_for_timeout(1000)
                    except Exception:
                        pass

                html_text = await page.content()
                if not html_text:
                    logger.warning(
                        f"{type(self).__name__}: пустая страница {url} "
                        f"(попытка {attempt + 1})"
                    )
                    last_error = RuntimeError("empty page")
                    continue

                orders = self.parse_cards_html(
                    text=html_text,
                    source_name=source.get("name") or url,
                    base_url=url,
                    selectors=source.get("selectors") or {},
                    max_orders=self.max_orders_per_run,
                )
                logger.info(
                    f"{type(self).__name__}: {url} → {len(orders)} заказов"
                )
                return orders
            except Exception as e:
                last_error = e
                logger.warning(
                    f"{type(self).__name__}: ошибка загрузки {url} "
                    f"(попытка {attempt + 1}/{max(1, self.max_retries + 1)}): {e}"
                )
            finally:
                if page is not None:
                    try:
                        await page.close()
                    except Exception:
                        pass
                if ctx is not None:
                    try:
                        await ctx.close()
                    except Exception:
                        pass

        logger.warning(
            f"{type(self).__name__}: {url} не загружен после "
            f"{max(1, self.max_retries + 1)} попыток: {last_error}"
        )
        return []

    # ── Remote-режим: чтение заказов из Redis (Windows-скрейпер) ──

    async def _fetch_orders_remote(self, source: Dict[str, Any]) -> List[FreelanceOrder]:
        """Забрать заказы из Redis-листа, заполняемого Windows-скрейпером.

        Формат элемента листа — JSON:
            {"title": "...", "url": "...", "budget": "...", "description": "...",
             "source": "...", "deadline": "...", "tags": [...]}

        Returns:
            Список FreelanceOrder (пустой при недоступности Redis).
        """
        if self.remote_redis is None:
            logger.warning(
                f"{type(self).__name__} (remote): Redis не подключён — "
                f"заказы не читаются"
            )
            return []

        orders: List[FreelanceOrder] = []
        try:
            # Забираем пачкой (не BLPOP — не блокируемся между источниками)
            items = await self.remote_redis.lrange(
                self.remote_list_key, 0, self.max_orders_per_run - 1
            )
            if not items:
                return []

            # Снимаем только реально обработанные (транзакционно не нужно:
            # дедуп по URL всё равно отсечёт повторы).
            await self.remote_redis.ltrim(
                self.remote_list_key, len(items), -1
            )

            for raw in items:
                if isinstance(raw, bytes):
                    raw = raw.decode("utf-8", errors="replace")
                try:
                    data = json.loads(raw) if isinstance(raw, str) else raw
                except (ValueError, TypeError):
                    logger.warning(
                        f"{type(self).__name__} (remote): не-JSON элемент листа: "
                        f"{str(raw)[:120]}"
                    )
                    continue
                if not isinstance(data, dict):
                    continue

                title = (data.get("title") or "").strip()
                if not title:
                    continue

                order = FreelanceOrder(
                    title=title,
                    url=data.get("url") or "",
                    description=(data.get("description") or "").strip(),
                    budget=(data.get("budget") or "").strip(),
                    currency=data.get("currency") or data.get("budget_currency"),
                    deadline=data.get("deadline") or "",
                    source=data.get("source") or source.get("name") or "remote",
                    tags=data.get("tags") or [],
                    author_username=data.get("author_username"),
                )
                orders.append(order)
                self._remote_orders_received += 1

            if orders:
                logger.info(
                    f"{type(self).__name__} (remote): получено {len(orders)} "
                    f"заказов из Redis-листа (всего за сессию: "
                    f"{self._remote_orders_received})"
                )
        except Exception as e:
            logger.warning(
                f"{type(self).__name__} (remote): ошибка чтения Redis-листа: {e}"
            )
        return orders

    # ── Парсинг (чистые функции, тестируются без браузера) ─

    @staticmethod
    def parse_cards_html(
        text: str,
        source_name: str,
        base_url: str = "",
        selectors: Optional[Dict[str, str]] = None,
        max_orders: int = 60,
    ) -> List[FreelanceOrder]:
        """Распарсить HTML (после JS-рендера) в список заказов.

        Использует те же дефолтные селекторы, что и ExchangeHtmlReceiver,
        но переопределяемые под конкретную биржу (kwork/fl_ru):
            kwork: container=".card__content, .wants-card, article",
                   title=".card__title, .wants-card__title",
                   budget=".card__price, .wants-card__price, .price"
            fl_ru: container=".order-item, .freelance-order, article",
                   title=".order-item__title, a.gazov-link",
                   budget=".order-item__price, .price, .cost"

        Args:
            text: HTML страницы после JS-рендера.
            source_name: Имя источника (поле source заказа).
            base_url: Базовый URL (для резолва относительных ссылок).
            selectors: Переопределения селекторов.
            max_orders: Лимит заказов.

        Returns:
            Список FreelanceOrder.
        """
        if not HAS_BS4:
            logger.warning("beautifulsoup4 недоступен — Playwright-парсинг невозможен")
            return []

        sel = dict(ExchangeHtmlReceiver.DEFAULT_SELECTORS)
        if selectors:
            sel.update({k: v for k, v in selectors.items() if v})

        soup = BeautifulSoup(text, "html.parser")
        containers = soup.select(sel["container"])
        if not containers:
            # SPA-страница без карточек — это заглушка/капча/пустой каталог,
            # а НЕ каталог без контейнеров. Фантомные заказы не создаём.
            return []

        orders: List[FreelanceOrder] = []
        for container in containers[:max_orders]:
            order = ExchangeHtmlReceiver._parse_container(
                container=container,
                source_name=source_name,
                base_url=base_url,
                selectors=sel,
            )
            if order.title or order.description:
                orders.append(order)

        return orders
