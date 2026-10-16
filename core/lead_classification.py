# -*- coding: utf-8 -*-
"""
core/lead_classification.py — Контракт классификации лидов (доменный слой).

Определяет типы и протокол для классификации фриланс-заказов по типу лида.
Не содержит импортов primary-репозитория; мок-тестируемый.
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Protocol


class LeadType(str, Enum):
    """Тип лида (заказа/предложения)."""
    ORDER = "заказ"
    VACANCY = "vacancy"
    PARTNERSHIP = "partnership"
    UNKNOWN = "unknown"


@dataclass
class LeadClassificationResult:
    """Результат классификации лида.

    Attributes:
        lead_type: Тип лида (заказ/vacancy/partnership/unknown).
        classification_source: Источник классификации (по умолчанию "llm").
        classification_error: Ошибка парсинга/классификации (None при успехе).
        relevant: Релевантен ли заказ для ниши.
        budget: Бюджет (строка или None).
        tech_stack: Технологический стек (строка или None).
        task_summary: Краткое описание задачи.
        urgency: Срочность (строка или None).
    """
    lead_type: LeadType = LeadType.UNKNOWN
    classification_source: str = "llm"
    classification_error: Optional[str] = None
    relevant: bool = False
    budget: Optional[str] = None
    tech_stack: Optional[str] = None
    task_summary: Optional[str] = None
    urgency: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Сериализация в dict (для обратной совместимости)."""
        return {
            "relevant": self.relevant,
            "budget": self.budget,
            "tech_stack": self.tech_stack,
            "task_summary": self.task_summary,
            "urgency": self.urgency,
            "lead_type": self.lead_type.value if self.lead_type else None,
            "classification_source": self.classification_source,
            "classification_error": self.classification_error,
        }


# ═══════════════════════════════════════════════════════════════
#  Доменный тип сообщения для LLM
# ═══════════════════════════════════════════════════════════════


@dataclass
class Message:
    """Сообщение для LLM (role + content).

    Заменяет импорт core.llm_engine.Message из primary-репозитория.
    """
    role: str
    content: str


# ═══════════════════════════════════════════════════════════════
#  Протокол LLMEngine
# ═══════════════════════════════════════════════════════════════


class LLMEngine(Protocol):
    """Протокол LLM-движка для доменного слоя.

    Потребитель (сервис-обёртка) передаёт объект, совместимый с этим протоколом.
    """

    async def generate_response(
        self,
        messages: List[Message],
        model: str,
        temperature: float = 0.0,
        max_tokens: int = 512,
        escalate_max_tokens: bool = True,
    ) -> Any:
        """Сгенерировать ответ LLM.

        Args:
            messages: Список сообщений (доменный тип Message).
            model: Имя модели.
            temperature: Температура семплирования.
            max_tokens: Максимум токенов в ответе.
            escalate_max_tokens: Увеличивать max_tokens при пустом ответе.

        Returns:
            Объект с атрибутом .content (строка ответа).
        """
        ...


# ═══════════════════════════════════════════════════════════════
#  Провайдер даты/времени (инъекция часов)
# ═══════════════════════════════════════════════════════════════


class Clock(Protocol):
    """Протокол часов для доменного слоя.

    Позволяет инъектировать текущее время без привязки к конкретной реализации.
    """

    def now(self) -> Any:
        """Вернуть текущее время (aware datetime в МСК)."""
        ...

    def fmt(self, dt: Any) -> str:
        """Форматировать datetime в строку для LLM-промпта."""
        ...


# ═══════════════════════════════════════════════════════════════
#  Классификация лидов
# ═══════════════════════════════════════════════════════════════


def classify_lead_text(
    text: str,
    llm_response: Dict[str, Any],
) -> LeadClassificationResult:
    """Классифицировать текст заказа по ответу LLM.

    Извлекает lead_type из LLM-ответа. Если поле отсутствует или
    невалидно — LeadType.UNKNOWN.

    Args:
        text: Исходный текст заказа (для контекста, не используется напрямую).
        llm_response: Распарсенный JSON-ответ LLM (поля relevant, budget и т.д.).

    Returns:
        LeadClassificationResult с заполненными полями.
    """
    result = LeadClassificationResult(
        relevant=llm_response.get("relevant", False),
        budget=llm_response.get("budget"),
        tech_stack=llm_response.get("tech_stack"),
        task_summary=llm_response.get("task_summary"),
        urgency=llm_response.get("urgency"),
    )

    # Извлекаем lead_type из ответа LLM
    raw_type = llm_response.get("lead_type")
    if raw_type and isinstance(raw_type, str):
        for lt in LeadType:
            if lt.value == raw_type or lt.name.lower() == raw_type.lower():
                result.lead_type = lt
                break
        else:
            result.lead_type = LeadType.UNKNOWN
            result.classification_error = f"Unknown lead_type: {raw_type}"
    else:
        result.lead_type = LeadType.UNKNOWN

    return result


def parse_llm_json_response(raw_content: str) -> Optional[Dict[str, Any]]:
    """Распарсить JSON из ответа LLM (с очисткой markdown-обёртки).

    Args:
        raw_content: Сырой текст ответа LLM.

    Returns:
        Dict с полями или None при ошибке парсинга.
    """
    cleaned = re.sub(
        r'^```(?:json)?\s*', '', raw_content, flags=re.IGNORECASE
    )
    cleaned = re.sub(r'\s*```$', '', cleaned)

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
                        candidate = cleaned[brace_start: i + 1]
                        try:
                            data = json.loads(candidate)
                        except json.JSONDecodeError:
                            data = None
                        break

    return data