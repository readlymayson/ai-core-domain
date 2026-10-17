# -*- coding: utf-8 -*-
"""Tests for core/lead_classification.py — контракт классификации лидов."""
from __future__ import annotations

import pytest
from core.lead_classification import (
    LeadType,
    LeadClassificationResult,
    Message,
    classify_lead_text,
    parse_llm_json_response,
)


class TestMessage:
    """Message dataclass — доменный тип сообщения для LLM."""

    def test_create_message(self):
        msg = Message(role="system", content="Hello")
        assert msg.role == "system"
        assert msg.content == "Hello"

    def test_message_equality(self):
        m1 = Message(role="user", content="Hi")
        m2 = Message(role="user", content="Hi")
        assert m1 == m2


class TestLeadClassificationResult:
    """LeadClassificationResult — результат классификации."""

    def test_default_values(self):
        result = LeadClassificationResult()
        assert result.lead_type == LeadType.UNKNOWN
        assert result.classification_source == "llm"
        assert result.classification_error is None
        assert result.relevant is False

    def test_to_dict_includes_all_fields(self):
        result = LeadClassificationResult(
            lead_type=LeadType.ORDER,
            classification_source="llm",
            classification_error=None,
            relevant=True,
            budget="5000 RUB",
            tech_stack="Python, FastAPI",
            task_summary="Разработка бота",
            urgency="срочно",
        )
        d = result.to_dict()
        assert d["lead_type"] == "заказ"
        assert d["classification_source"] == "llm"
        assert d["classification_error"] is None
        assert d["relevant"] is True
        assert d["budget"] == "5000 RUB"
        assert d["tech_stack"] == "Python, FastAPI"
        assert d["task_summary"] == "Разработка бота"
        assert d["urgency"] == "срочно"

    def test_to_dict_unknown_type(self):
        result = LeadClassificationResult()
        d = result.to_dict()
        assert d["lead_type"] == "unknown"
        assert d["relevant"] is False


class TestParseLlmJsonResponse:
    """parse_llm_json_response — очистка JSON из ответа LLM."""

    def test_plain_json(self):
        raw = '{"relevant": true, "budget": "5000"}'
        result = parse_llm_json_response(raw)
        assert result is not None
        assert result["relevant"] is True
        assert result["budget"] == "5000"

    def test_markdown_code_block(self):
        raw = '```json\n{"relevant": false}\n```'
        result = parse_llm_json_response(raw)
        assert result is not None
        assert result["relevant"] is False

    def test_markdown_without_lang(self):
        raw = '```\n{"relevant": true}\n```'
        result = parse_llm_json_response(raw)
        assert result is not None
        assert result["relevant"] is True

    def test_extra_text_after_json(self):
        raw = '{"relevant": true}\nSome extra text'
        result = parse_llm_json_response(raw)
        assert result is not None
        assert result["relevant"] is True

    def test_invalid_json(self):
        raw = "not json at all"
        result = parse_llm_json_response(raw)
        assert result is None

    def test_empty_string(self):
        result = parse_llm_json_response("")
        assert result is None

    def test_nested_braces(self):
        raw = '{"relevant": true, "nested": {"a": 1}}'
        result = parse_llm_json_response(raw)
        assert result is not None
        assert result["relevant"] is True
        assert result["nested"]["a"] == 1


class TestClassifyLeadText:
    """classify_lead_text — извлечение lead_type из LLM-ответа."""

    def test_order_type(self):
        llm_response = {
            "relevant": True,
            "lead_type": "заказ",
            "budget": "5000 RUB",
            "tech_stack": "Python",
            "task_summary": "Разработка бота",
            "urgency": "не срочно",
        }
        result = classify_lead_text("some text", llm_response)
        assert result.lead_type == LeadType.ORDER
        assert result.relevant is True
        assert result.classification_error is None
        assert result.budget == "5000 RUB"

    def test_vacancy_type(self):
        llm_response = {"relevant": True, "lead_type": "vacancy"}
        result = classify_lead_text("some text", llm_response)
        assert result.lead_type == LeadType.VACANCY

    def test_partnership_type(self):
        llm_response = {"relevant": True, "lead_type": "partnership"}
        result = classify_lead_text("some text", llm_response)
        assert result.lead_type == LeadType.PARTNERSHIP

    def test_unknown_type(self):
        llm_response = {"relevant": False}
        result = classify_lead_text("some text", llm_response)
        assert result.lead_type == LeadType.UNKNOWN

    def test_invalid_type_string(self):
        llm_response = {"relevant": True, "lead_type": "invalid_type"}
        result = classify_lead_text("some text", llm_response)
        assert result.lead_type == LeadType.UNKNOWN
        assert result.classification_error is not None
        assert "invalid_type" in result.classification_error

    def test_classification_source_default(self):
        llm_response = {"relevant": True, "lead_type": "заказ"}
        result = classify_lead_text("some text", llm_response)
        assert result.classification_source == "llm"

    def test_relevant_false_with_type(self):
        """Даже при relevant=False lead_type может быть указан."""
        llm_response = {"relevant": False, "lead_type": "unknown"}
        result = classify_lead_text("some text", llm_response)
        assert result.relevant is False
        assert result.lead_type == LeadType.UNKNOWN
