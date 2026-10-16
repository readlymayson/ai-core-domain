# -*- coding: utf-8 -*-
"""Tests for core/message_monitor.py — FreelanceOrderFilter."""
from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, Dict, List, Optional
from unittest.mock import AsyncMock, MagicMock

import pytest

from core.message_monitor import (
    BaseMessageReceiver,
    FreelanceOrderFilter,
    OnMessageCallback,
)


# ═══════════════════════════════════════════════════════════════
#  Mocks
# ═══════════════════════════════════════════════════════════════


class MockConfig:
    """Минимальный конфиг для FreelanceOrderFilter."""
    dedup_cache_size: int = 1000
    llm_concurrency: int = 1
    llm_model: str = "test-model"
    llm_temperature: float = 0.0
    llm_max_tokens: int = 512
    keywords: List[str] = ["python", "bot", "api"]
    keyword_regex = None
    stopwords: Optional[List[str]] = None
    min_budget_rub: int = 0
    target_sources: List[str] = []


class MockLLMResponse:
    """Имитация ответа LLM."""
    def __init__(self, content: str):
        self.content = content


class MockLLMEngine:
    """Мок LLM-движка для тестов.

    Позволяет задать возвращаемый контент, тайм-аут или исключение.
    """
    def __init__(self):
        self.response_content = '{"relevant": true, "budget": "5000", "tech_stack": "Python", "task_summary": "Разработка бота", "urgency": "не срочно"}'
        self.should_timeout = False
        self.should_raise = None
        self.last_messages = None
        self.last_model = None
        self.last_temperature = None
        self.last_max_tokens = None
        self.last_escalate = None

    async def generate_response(
        self,
        messages: List[Any],
        model: str,
        temperature: float = 0.0,
        max_tokens: int = 512,
        escalate_max_tokens: bool = True,
    ) -> MockLLMResponse:
        self.last_messages = messages
        self.last_model = model
        self.last_temperature = temperature
        self.last_max_tokens = max_tokens
        self.last_escalate = escalate_max_tokens

        if self.should_raise:
            raise self.should_raise
        if self.should_timeout:
            raise asyncio.TimeoutError()
        return MockLLMResponse(content=self.response_content)


@pytest.fixture
def mock_config():
    return MockConfig()


@pytest.fixture
def mock_llm():
    return MockLLMEngine()


@pytest.fixture
def notify_callback():
    return AsyncMock()


@pytest.fixture
def filter_instance(mock_config, mock_llm, notify_callback):
    """Создать FreelanceOrderFilter с моками."""
    return FreelanceOrderFilter(
        config=mock_config,
        llm_engine=mock_llm,
        notify_callback=notify_callback,
    )


# ═══════════════════════════════════════════════════════════════
#  FreelanceOrderFilter — конструктор и базовая структура
# ═══════════════════════════════════════════════════════════════


class TestFreelanceOrderFilterInit:
    """FreelanceOrderFilter — конструктор."""

    def test_constructor_stores_deps(self, mock_config, mock_llm, notify_callback):
        f = FreelanceOrderFilter(
            config=mock_config,
            llm_engine=mock_llm,
            notify_callback=notify_callback,
        )
        assert f._config is mock_config
        assert f._llm_engine is mock_llm
        assert f._notify_callback is notify_callback
        assert f._clock is None

    def test_constructor_accepts_clock(self, mock_config, mock_llm, notify_callback):
        clock = MagicMock()
        f = FreelanceOrderFilter(
            config=mock_config,
            llm_engine=mock_llm,
            notify_callback=notify_callback,
            clock=clock,
        )
        assert f._clock is clock

    def test_initial_stats(self, filter_instance):
        stats = filter_instance._stats
        assert stats["total"] == 0
        assert stats["duplicates"] == 0
        assert stats["llm_calls"] == 0
        assert stats["orders_found"] == 0

    def test_initial_state(self, filter_instance):
        assert filter_instance._receiver is None
        assert filter_instance._redis is None
        assert filter_instance._is_running is False


# ═══════════════════════════════════════════════════════════════
#  FreelanceOrderFilter — _analyze_relevance (Pass 2)
# ═══════════════════════════════════════════════════════════════


class TestAnalyzeRelevance:
    """FreelanceOrderFilter._analyze_relevance — LLM-оценка."""

    @pytest.mark.asyncio
    async def test_relevant_true(self, filter_instance, mock_llm):
        mock_llm.response_content = (
            '{"relevant": true, "budget": "5000", "tech_stack": "Python", '
            '"task_summary": "Разработка бота", "urgency": "не срочно"}'
        )
        result = await filter_instance._analyze_relevance("test text")
        assert result["relevant"] is True
        assert result["budget"] == "5000"
        assert result["tech_stack"] == "Python"
        assert result["task_summary"] == "Разработка бота"
        assert result["urgency"] == "не срочно"

    @pytest.mark.asyncio
    async def test_relevant_false(self, filter_instance, mock_llm):
        mock_llm.response_content = '{"relevant": false}'
        result = await filter_instance._analyze_relevance("test text")
        assert result["relevant"] is False

    @pytest.mark.asyncio
    async def test_invalid_json_returns_false(self, filter_instance, mock_llm):
        mock_llm.response_content = "not json"
        result = await filter_instance._analyze_relevance("test text")
        assert result["relevant"] is False
        assert result.get("classification_error") == "invalid_json"

    @pytest.mark.asyncio
    async def test_timeout_returns_false(self, filter_instance, mock_llm):
        mock_llm.should_timeout = True
        result = await filter_instance._analyze_relevance("test text")
        assert result["relevant"] is False
        assert result.get("classification_error") == "timeout"

    @pytest.mark.asyncio
    async def test_exception_returns_false(self, filter_instance, mock_llm):
        mock_llm.should_raise = RuntimeError("LLM crashed")
        result = await filter_instance._analyze_relevance("test text")
        assert result["relevant"] is False
        assert "LLM crashed" in result.get("classification_error", "")

    @pytest.mark.asyncio
    async def test_passes_max_tokens(self, filter_instance, mock_llm):
        await filter_instance._analyze_relevance("test text")
        assert mock_llm.last_max_tokens == 512
        assert mock_llm.last_escalate is True

    @pytest.mark.asyncio
    async def test_lead_type_in_result(self, filter_instance, mock_llm):
        mock_llm.response_content = (
            '{"relevant": true, "lead_type": "заказ", "budget": "5000", '
            '"tech_stack": "Python", "task_summary": "Бот", "urgency": "не срочно"}'
        )
        result = await filter_instance._analyze_relevance("test text")
        assert result["lead_type"] == "заказ"
        assert result["classification_source"] == "llm"

    @pytest.mark.asyncio
    async def test_lead_type_unknown_when_missing(self, filter_instance, mock_llm):
        mock_llm.response_content = '{"relevant": true}'
        result = await filter_instance._analyze_relevance("test text")
        assert result["lead_type"] == "unknown"

    @pytest.mark.asyncio
    async def test_classification_error_on_invalid_json(self, filter_instance, mock_llm):
        mock_llm.response_content = '{"relevant": true, "lead_type": 123}'
        result = await filter_instance._analyze_relevance("test text")
        # lead_type=123 is not a string, so classify_lead_text returns UNKNOWN
        assert result["lead_type"] == "unknown"

    @pytest.mark.asyncio
    async def test_relevant_not_bool_returns_false(self, filter_instance, mock_llm):
        mock_llm.response_content = '{"relevant": "yes"}'
        result = await filter_instance._analyze_relevance("test text")
        assert result["relevant"] is False
        assert result.get("classification_error") == "invalid_relevant"

    @pytest.mark.asyncio
    async def test_markdown_json_parsed(self, filter_instance, mock_llm):
        mock_llm.response_content = '```json\n{"relevant": true}\n```'
        result = await filter_instance._analyze_relevance("test text")
        assert result["relevant"] is True

    @pytest.mark.asyncio
    async def test_llm_calls_incremented(self, filter_instance, mock_llm):
        assert filter_instance._stats["llm_calls"] == 0
        await filter_instance._analyze_relevance("test text")
        assert filter_instance._stats["llm_calls"] == 1

    @pytest.mark.asyncio
    async def test_llm_calls_incremented_on_error(self, filter_instance, mock_llm):
        mock_llm.should_timeout = True
        assert filter_instance._stats["llm_calls"] == 0
        await filter_instance._analyze_relevance("test text")
        assert filter_instance._stats["llm_calls"] == 1


# ═══════════════════════════════════════════════════════════════
#  FreelanceOrderFilter — _handle_raw_message (полный пайплайн)
# ═══════════════════════════════════════════════════════════════


class TestHandleRawMessage:
    """FreelanceOrderFilter._handle_raw_message — полный пайплайн."""

    @pytest.mark.asyncio
    async def test_duplicate_detected(self, filter_instance, mock_llm):
        mock_llm.response_content = '{"relevant": true}'
        text = "python bot for telegram"
        await filter_instance._handle_raw_message(
            raw_text=text,
            source_id="src1",
            message_link="https://example.com/1",
            chat_title="Test Chat",
            sender_username="user1",
        )
        assert filter_instance._stats["total"] == 1
        assert filter_instance._stats["duplicates"] == 0

        # Same text again — should be deduped
        await filter_instance._handle_raw_message(
            raw_text=text,
            source_id="src1",
            message_link="https://example.com/1",
            chat_title="Test Chat",
            sender_username="user1",
        )
        assert filter_instance._stats["total"] == 2
        assert filter_instance._stats["duplicates"] == 1

    @pytest.mark.asyncio
    async def test_stopword_filter(self, filter_instance, mock_config):
        mock_config.stopwords = ["дизайн", "верстка"]
        text = "Нужен дизайн сайта"
        await filter_instance._handle_raw_message(
            raw_text=text,
            source_id="src1",
            message_link="",
            chat_title="Test",
            sender_username=None,
        )
        assert filter_instance._stats["stopwords_hit"] == 1
        assert filter_instance._stats["llm_calls"] == 0

    @pytest.mark.asyncio
    async def test_keyword_miss(self, filter_instance, mock_config):
        mock_config.keywords = ["python", "bot"]
        mock_config.keyword_regex = None
        text = "Нужен дизайн интерьера"
        await filter_instance._handle_raw_message(
            raw_text=text,
            source_id="src1",
            message_link="",
            chat_title="Test",
            sender_username=None,
        )
        assert filter_instance._stats["keywords_missed"] == 1
        assert filter_instance._stats["llm_calls"] == 0

    @pytest.mark.asyncio
    async def test_full_pipeline_relevant(self, filter_instance, mock_llm, notify_callback):
        mock_llm.response_content = (
            '{"relevant": true, "lead_type": "заказ", "budget": "5000", '
            '"tech_stack": "Python", "task_summary": "Разработка бота", '
            '"urgency": "не срочно"}'
        )
        text = "Нужен python бот для telegram"
        await filter_instance._handle_raw_message(
            raw_text=text,
            source_id="src1",
            message_link="https://example.com",
            chat_title="Test Chat",
            sender_username="user1",
        )
        assert filter_instance._stats["total"] == 1
        assert filter_instance._stats["llm_calls"] == 1
        assert filter_instance._stats["orders_found"] == 1
        notify_callback.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_full_pipeline_not_relevant(self, filter_instance, mock_llm, notify_callback):
        mock_llm.response_content = '{"relevant": false}'
        text = "Нужен python бот для telegram"
        await filter_instance._handle_raw_message(
            raw_text=text,
            source_id="src1",
            message_link="https://example.com",
            chat_title="Test Chat",
            sender_username="user1",
        )
        assert filter_instance._stats["llm_calls"] == 1
        assert filter_instance._stats["llm_rejected"] == 1
        assert filter_instance._stats["orders_found"] == 0
        notify_callback.assert_not_awaited()


# ═══════════════════════════════════════════════════════════════
#  BaseMessageReceiver — контракт
# ═══════════════════════════════════════════════════════════════


class TestBaseMessageReceiver:
    """BaseMessageReceiver — абстрактный интерфейс."""

    def test_cannot_instantiate_abc(self):
        with pytest.raises(TypeError):
            BaseMessageReceiver()  # type: ignore

    def test_concrete_subclass(self):
        class ConcreteReceiver(BaseMessageReceiver):
            async def start(self):
                pass
            async def stop(self):
                pass
            @property
            def is_running(self):
                return False
            async def validate_chats(self):
                return {}

        instance = ConcreteReceiver()
        assert instance.is_running is False
        assert instance.on_message is None