# -*- coding: utf-8 -*-
"""
Доменный слой мониторинга фриланс-заказов.

Содержит:
- BaseMessageReceiver (ABC) — интерфейс для транспорта (Telethon, VK, etc.)
- FreelanceOrderFilter — бизнес-логика фильтрации (Pass 0/1/2)
"""
from __future__ import annotations

import asyncio
import hashlib
import html
import json
import re
import time
import copy
from abc import ABC, abstractmethod
from collections import deque
from typing import (
    Awaitable,
    Callable,
    Dict,
    List,
    Optional,
)

from loguru import logger

# Тип коллбэка on_message для BaseMessageReceiver:
# (raw_text, source_id, message_link, chat_title, sender_username) -> None
OnMessageCallback = Callable[
    [str, str, str, str, Optional[str]],
    Awaitable[None],
]


class BaseMessageReceiver(ABC):
    """Абстрактный интерфейс для получения сообщений из внешних источников.

    Реализации: TelegramUserbotMonitor (Telethon), VK-, AmoCRM-адаптеры.
    """

    on_message: Optional[OnMessageCallback] = None
    """Коллбэк, вызываемый при получении нового сообщения.

    Сигнатура: (raw_text, source_id, message_link, chat_title, sender_username)
    Метаданные чата передаются отдельно от LLM-результата — не зависят от провайдера.
    """

    @abstractmethod
    async def start(self) -> None:
        """Запустить прослушивание источников."""
        ...

    @abstractmethod
    async def stop(self) -> None:
        """Остановить прослушивание."""
        ...

    @property
    @abstractmethod
    def is_running(self) -> bool:
        """Флаг: запущен ли приёмник."""
        ...

    @abstractmethod
    async def validate_chats(self) -> Dict[str, bool]:
        """Проверить доступность источников.

        Returns:
            Dict[source_identifier, is_accessible]
        """
        ...


class FreelanceOrderFilter:
    """Бизнес-логика фильтрации фриланс-заказов.

    Трёхпроходная фильтрация:
        Pass 0 — MD5-дедупликация (in-memory deque)
        Pass 1 — Ключевые слова (str.contains, регистронезависимый)
        Pass 2 — LLM-оценка релевантности (JSON-ответ)
    """

    def __init__(
        self,
        config,
        llm_engine,
        notify_callback: Callable[[str], Awaitable[None]],
    ):
        self._config = config
        self._llm_engine = llm_engine
        self._notify_callback = notify_callback

        # In-memory кэш MD5 (дедупликация)
        self._seen_hashes: deque = deque(maxlen=config.dedup_cache_size)

        # Опциональный Redis для персистентного дедупа (переживает рестарты).
        # Устанавливается через set_redis() из main.py; None — только in-memory.
        self._redis = None

        # Семафор для ограничения параллельных LLM-запросов
        self._llm_semaphore = asyncio.Semaphore(config.llm_concurrency)

        # Счётчики статистики
        self._stats: Dict = {
            "total": 0,
            "duplicates": 0,
            "keywords_missed": 0,
            "stopwords_hit": 0,
            "budget_below": 0,
            "llm_calls": 0,
            "llm_rejected": 0,
            "orders_found": 0,
            "by_source": {},
        }

        # Сопоставление причины → ключ в _stats (для сводки в уведомлении)
        self._reject_reason_key = {
            "keywords_missed": "keywords_missed",
            "stopwords_hit": "stopwords_hit",
            "budget_below": "budget_below",
            "llm_rejected": "llm_rejected",
        }

        self._receiver: Optional[BaseMessageReceiver] = None
        self._is_running = False

    def set_redis(self, redis_client) -> None:
        """Подключить Redis-клиент для персистентного дедупа (необязательно).

        redis_client должен поддерживать корутинный `sadd`/`sismember`
        (redis.asyncio.Redis). При None — дедуп только в памяти процесса.
        """
        self._redis = redis_client

    # ── Управление ─────────────────────────────────────────────────

    def set_receiver(self, receiver: BaseMessageReceiver) -> None:
        """Привязать транспортный приёмник."""
        self._receiver = receiver

    async def start(self) -> None:
        """Запустить мониторинг."""
        if self._is_running:
            logger.warning("FreelanceOrderFilter уже запущен — пропускаем")
            return

        if self._receiver is None:
            raise RuntimeError("Message receiver not set")

        # Валидация источников
        chat_status = await self._receiver.validate_chats()
        for source, ok in chat_status.items():
            if not ok:
                logger.warning(f"Источник недоступен: {source}")

        # Подключаем коллбэк
        self._receiver.on_message = self._handle_raw_message

        # Запускаем транспорт
        try:
            await self._receiver.start()
        except Exception as e:
            logger.error(f"Ошибка запуска receiver: {e}")
            return
        self._is_running = True

        logger.info(f"FreelanceOrderFilter запущен ({len(chat_status)} источников)")

    async def stop(self) -> None:
        """Остановить мониторинг."""
        if not self._is_running:
            logger.warning("FreelanceOrderFilter уже остановлен — пропускаем")
            return

        await self._receiver.stop()

        # Сбрасываем семафор (создаём новый)
        self._llm_semaphore = asyncio.Semaphore(self._config.llm_concurrency)

        self._is_running = False
        logger.info("FreelanceOrderFilter остановлен")

    @property
    def is_running(self) -> bool:
        return self._is_running

    def get_stats(self) -> Dict:
        """Вернуть глубокую копию текущей статистики."""
        return copy.deepcopy(self._stats)

    async def update_sources(self, new_sources: List[str]) -> None:
        """Обновить список источников (на лету, с переподпиской)."""
        if self._receiver is None:
            raise RuntimeError("Message receiver not set")
        self._config.target_sources = new_sources
        await self._receiver.update_sources(new_sources)
        logger.info(f"FreelanceMonitor: источники обновлены: {new_sources}")

    async def update_keywords(self, new_keywords: List[str]) -> None:
        """Обновить список ключевых слов (на лету).
        Изменения действуют до перезапуска сервиса."""
        self._config.keywords = new_keywords
        logger.info("FreelanceMonitor: ключевые слова обновлены")

    # ── Обработка сообщений ────────────────────────────────────────

    async def _handle_raw_message(
        self,
        raw_text: str,
        source_id: str,
        message_link: str,
        chat_title: str,
        sender_username: Optional[str],
        extra: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Основной обработчик входящих сообщений (3 прохода).

        Параметр `extra` — необязательные метаданные транспорта
        (client_chat_id, sender_username от TelegramUserbotMonitor).
        Не используется фильтром напрямую, но принимается для
        совместимости с сигнатурой on_message.
        """
        self._stats["total"] += 1
        self._stats.setdefault("by_source", {}).setdefault(chat_title or source_id, 0)
        self._stats["by_source"][chat_title or source_id] += 1

        # Pass 0 — Дедупликация
        text_hash = hashlib.md5(
            raw_text.strip().lower().encode("utf-8")
        ).hexdigest()

        # Redis-дедуп (переживает рестарты). Дневной ключ + TTL: старые записи
        # естественно вымываются, сет не растёт бесконечно. При недоступности
        # Redis — fallback на in-memory дедуп.
        if self._redis is not None:
            try:
                day_key = time.strftime("freelance_orders:hashes:%Y-%m-%d")
                is_new = await self._redis.sadd(day_key, text_hash)
                if not is_new:
                    self._stats["duplicates"] += 1
                    logger.debug(f"Дубликат отсечён (Redis): {text_hash[:8]}...")
                    return
                # TTL на дневной ключ при каждой вставке (3 дня): дёшево (O(1)),
                # и ключ гарантированно вымывается даже при малом числе заказов.
                await self._redis.expire(day_key, 3 * 86400)
            except Exception:
                self._redis = None  # отключаем насовсем при ошибке
                # fallthrough к in-memory дедупу ниже

        if self._redis is None:
            if text_hash in self._seen_hashes:
                self._stats["duplicates"] += 1
                logger.debug(f"Дубликат отсечён: {text_hash[:8]}...")
                return
            self._seen_hashes.append(text_hash)

        # Pass 1 — Жёсткий префильтр: стоп-слова → ключевые слова (regex по
        # границам слов) → минимальный бюджет.
        text_lower = raw_text.lower()

        # 1a. Стоп-слова (явно нерелевантные ниши): отсекаем ДО LLM.
        stopwords = getattr(self._config, "stopwords", None)
        if stopwords and any(sw.lower() in text_lower for sw in stopwords):
            self._stats["stopwords_hit"] += 1
            logger.debug(f"Пропущен (стоп-слово): {raw_text[:60]}...")
            return

        # 1b. Ключевые слова — по границам слов, а не substring.
        #     Слова-маркеры (бот/ai/api/telegram/backend) работают как точные
        #     токены, поэтому «работа», «email», «капитан» не сработают.
        keyword_re = getattr(self._config, "keyword_regex", None)
        if keyword_re:
            if not re.search(keyword_re, text_lower):
                self._stats["keywords_missed"] += 1
                logger.debug(f"Пропущен (нет ключевых слов): {raw_text[:60]}...")
                return
        else:
            # Fallback: прежнее substring-поведение (для обратной совместимости)
            if not any(kw.lower() in text_lower for kw in self._config.keywords):
                self._stats["keywords_missed"] += 1
                logger.debug(f"Пропущен (нет ключевых слов): {raw_text[:60]}...")
                return

        # 1c. Бюджетный фильтр: минимальная сумма в рублях (из текста заказа).
        #     Мелкие заказы (100-1000 ₽) не окупают LLM-оценку.
        min_budget_rub = getattr(self._config, "min_budget_rub", 0)
        if min_budget_rub:
            from core.freelance_exchange_scraper import ExchangeRSSReceiver
            budget_text, _ = ExchangeRSSReceiver._extract_budget(raw_text)
            if budget_text is not None:
                # Нормализуем: неразрывные пробелы (U+00A0, U+202F) → обычные,
                # потом убираем пробелы между цифрами.
                normalized = budget_text.replace("\u00a0", " ").replace("\u202f", " ")
                nums = re.findall(r"\d[\d\s]*\d?", normalized)
                if nums:
                    amount = int(re.sub(r"\s+", "", nums[0]))
                    # Диапазон "3000-5000 руб" → берём нижнюю границу
                    lo = amount
                    if "-" in budget_text or "–" in budget_text:
                        lo = amount
                    if "$" in budget_text and "руб" not in budget_text:
                        # 1$ ≈ 100 ₽ (грубо, как в CURRENCY_TO_CU)
                        lo = lo * 100
                    if lo < min_budget_rub:
                        self._stats["budget_below"] += 1
                        logger.debug(
                            f"Пропущен (бюджет ниже {min_budget_rub}): {raw_text[:60]}..."
                        )
                        return

        # Pass 2 — LLM-оценка (с семафором)
        async with self._llm_semaphore:
            data = await self._analyze_relevance(raw_text)

        if not data.get("relevant", False):
            self._stats["llm_rejected"] += 1
            logger.debug(f"Отклонён LLM: {raw_text[:60]}...")
            return

        await self._notify_admin(
            data=data,
            source_id=source_id,
            message_link=message_link,
            chat_title=chat_title,
            sender_username=sender_username,
        )

    async def _analyze_relevance(self, text: str) -> Dict:
        """LLM-оценка релевантности заказа.

        Запрашивает у LLM JSON с полями:
            relevant (bool), budget (str|None), tech_stack (str|None),
            task_summary (str|None), urgency (str|None)

        Returns:
            Dict с этими полями (по умолчанию relevant=False при ошибке).
        """
        self._stats["llm_calls"] += 1

        # Строгий гейт: relevant=true ТОЛЬКО если задача реально про разработку
        # на Python/backend/AI-автоматизацию (не «упоминание технологии»).
        # Явные запреты — то, что выходит за нашу нишу (из живых выборок).
        system_prompt = (
            "Ты — строгий фильтр заказов для B2B-фриланс разработчика.\n"
            "Ниша: разработка на Python — backend, боты (Telegram/иные), парсеры/скрапинг, "
            "автоматизация, интеграции через API, AI/LLM-агенты (Python-стек).\n\n"
            "relevant=true ставить ТОЛЬКО если заказ действительно про разработку "
            "на этом стеке (в тексте есть реальная Python/backend/бот/парсер/API задача).\n"
            "relevant=false, если заказ про: накрутку/отзывы/SEO/SMM/контекстную рекламу, "
            "дизайн (Figma/баннеры/UI-UX), видео/монтаж, копирайтинг, маркетинг/продвижение, "
            "WordPress/Bitrix/Tilda/Joomla/1С/Lua/Roblox/Excel, администрирование (без разработки), "
            "доставку/фото/мелкие бытовые задачи.\n"
            "Формат ответа — ТОЛЬКО JSON, без markdown, без пояснений:\n"
            '{\n'
            '  "relevant": true/false,\n'
            '  "budget": "сумма или null",\n'
            '  "tech_stack": "какие технологии упомянуты или null",\n'
            '  "task_summary": "краткое описание задачи на русском (1-2 предложения)",\n'
            '  "urgency": "срочность: срочно/не срочно/null"\n'
            '}'
        )

        from core.llm_engine import Message as LLMMessage

        # Текущая дата для анализа (не выводится пользователю в Telegram,
        # нужна LLM для оценки срочности/актуальности заказа)
        from core.freelance_dates import fmt_dt, now_msk

        llm_messages = [
            LLMMessage(role="system", content=system_prompt),
            LLMMessage(
                role="user",
                content=(
                    f"Сегодня: {fmt_dt(now_msk())} (МСК).\n\n"
                    f"Оцени этот заказ:\n\n{text[:3000]}"
                ),
            ),
        ]

        try:
            response = await asyncio.wait_for(
                self._llm_engine.generate_response(
                    messages=llm_messages,
                    model=self._config.llm_model,
                    temperature=self._config.llm_temperature,
                    max_tokens=self._config.llm_max_tokens,
                    # reasoning-модели (deepseek-v4-flash) при finish_reason=length
                    # возвращают пустой ответ; эскалация max_tokens помогает
                    # дождаться JSON (см. пустые ответы в логах).
                    escalate_max_tokens=True,
                ),
                timeout=15.0,
            )

            raw_content = response.content.strip()

            # Очистка JSON от markdown-обёртки
            cleaned = re.sub(
                r'^```(?:json)?\s*', '', raw_content, flags=re.IGNORECASE
            )
            cleaned = re.sub(r'\s*```$', '', cleaned)

            # Вытаскиваем ПЕРВЫЙ JSON-объект из текста (модель могла добавить
            # пролог/эпилог вокруг JSON, или завершить текст после фигурной скобки).
            data = None
            if cleaned.startswith("{"):
                try:
                    data = json.loads(cleaned)
                except json.JSONDecodeError:
                    data = None
            if data is None:
                brace_start = cleaned.find("{")
                if brace_start != -1:
                    depth = 0
                    for i in range(brace_start, len(cleaned)):
                        ch = cleaned[i]
                        if ch == "{":
                            depth += 1
                        elif ch == "}":
                            depth -= 1
                            if depth == 0:
                                candidate = cleaned[brace_start : i + 1]
                                try:
                                    data = json.loads(candidate)
                                except json.JSONDecodeError:
                                    data = None
                                break
            if data is None:
                logger.warning(
                    f"LLM вернул не-JSON ответ: {raw_content[:200]!r}"
                )
                return {"relevant": False}

            # Валидация полей
            if not isinstance(data.get("relevant"), bool):
                logger.warning("LLM вернул relevant не bool")
                return {"relevant": False}
            return {
                "relevant": data["relevant"],
                "budget": data.get("budget"),
                "tech_stack": data.get("tech_stack"),
                "task_summary": data.get("task_summary"),
                "urgency": data.get("urgency"),
            }

        except (json.JSONDecodeError, KeyError, TypeError, AssertionError) as e:
            logger.warning(f"Ошибка парсинга JSON от LLM: {e}")
            return {"relevant": False}
        except asyncio.TimeoutError:
            logger.warning("Таймаут LLM-оценки (>15с)")
            return {"relevant": False}
        except Exception as e:
            logger.warning(f"Неожиданная ошибка LLM-оценки: {e}")
            return {"relevant": False}

    async def _notify_admin(
        self,
        data: Dict,
        source_id: str,
        message_link: str,
        chat_title: str,
        sender_username: Optional[str],
    ) -> None:
        """Отправить HTML-уведомление админу о найденном заказе.

        Включает компактную сводку фильтрации (сколько отсечено по каждой
        причине с запуска сервиса), чтобы был виден «здоровье» фильтра.
        """
        # Экранируем пользовательские поля
        safe_summary = html.escape(data.get("task_summary") or "—")
        safe_budget = html.escape(str(data.get("budget") or "Не указан"))
        safe_tech_stack = html.escape(str(data.get("tech_stack") or "—"))
        safe_urgency = html.escape(str(data.get("urgency") or "—"))
        safe_chat_title = html.escape(chat_title or source_id)
        safe_sender = html.escape(sender_username) if sender_username else None

        # Сводка фильтрации — инкрементируем найденное ДО формирования сводки,
        # чтобы текущий заказ уже входил в счётчик.
        self._stats["orders_found"] += 1
        s = self._stats
        stats_line = (
            "📊 Фильтр: "
            f"всего {s['total']} | "
            f"дублей {s['duplicates']} | "
            f"нет слов {s['keywords_missed']} | "
            f"стоп {s.get('stopwords_hit', 0)} | "
            f"бюджет {s.get('budget_below', 0)} | "
            f"LLM откл {s.get('llm_rejected', 0)} | "
            f"найдено {s['orders_found']}"
        )

        msg = (
            "🎯 Найден заказ!\n\n"
            f"📌 {safe_summary}\n\n"
            f"💰 Бюджет: {safe_budget}\n"
            f"🛠 Стек: {safe_tech_stack}\n"
            f"⏱ Срочность: {safe_urgency}\n\n"
            f"📡 Источник: {safe_chat_title}\n"
            f"👤 Контакт: @{safe_sender or 'скрыт'}\n\n"
            f'🔗 <a href="{html.escape(message_link, quote=True)}">Перейти к сообщению</a>\n'
            f"{stats_line}"
        )

        try:
            await self._notify_callback(msg)
        except Exception:
            logger.exception("Ошибка отправки уведомления о заказе")
            return

        logger.info(
            f"🎯 Заказ найден: {safe_chat_title} — "
            f"{(data.get('task_summary') or '')[:80]}"
        )
