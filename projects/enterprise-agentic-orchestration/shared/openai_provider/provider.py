"""
shared/openai_provider/provider.py
Single shared OpenAI adapter.

Rules:
- API key loaded from environment only — never hardcoded.
- Captures model, latency, tokens, estimated cost.
- Redacts sensitive values before logging.
- Validates structured output against provided schema.
- One repair attempt on invalid JSON output.
- Fail fast when OPENAI_API_KEY is missing.
"""
from __future__ import annotations

import json
import logging
import os
import time
from typing import Any

import jsonschema
from openai import OpenAI, APIError, RateLimitError, APITimeoutError
from opentelemetry import trace

from shared.contracts.agent_schemas import ModelUsage

logger = logging.getLogger(__name__)

# Pricing approximation (USD per 1K tokens) — update as needed
_COST_PER_1K: dict[str, tuple[float, float]] = {
    "gpt-4o": (0.005, 0.015),
    "gpt-4o-mini": (0.00015, 0.0006),
    "gpt-4-turbo": (0.01, 0.03),
    "gpt-4": (0.03, 0.06),
}


def _estimate_cost(model: str, input_tokens: int, output_tokens: int) -> float:
    base = model.split(":")[0]  # strip fine-tune suffixes
    prices = _COST_PER_1K.get(base, (0.01, 0.03))
    return (input_tokens / 1000) * prices[0] + (output_tokens / 1000) * prices[1]


def _redact(value: Any) -> str:
    """Return a safe placeholder for sensitive values."""
    s = str(value)
    if len(s) <= 8:
        return "***"
    return s[:4] + "***" + s[-4:]


class OpenAIProvider:
    """
    Shared OpenAI provider. Instantiate once per service.
    """

    def __init__(self) -> None:
        api_key = os.environ.get("OPENAI_API_KEY", "")
        if not api_key:
            raise RuntimeError(
                "OPENAI_API_KEY environment variable is required but not set. "
                "Provide it via .env or runtime configuration."
            )

        self._model = os.environ.get("OPENAI_MODEL", "gpt-4o")
        self._timeout = float(os.environ.get("OPENAI_TIMEOUT_SECONDS", "60"))
        self._max_retries = int(os.environ.get("OPENAI_MAX_RETRIES", "2"))
        self._temperature = float(os.environ.get("OPENAI_TEMPERATURE", "0"))
        self._client = OpenAI(
            api_key=api_key,
            timeout=self._timeout,
            max_retries=self._max_retries,
        )
        self._tracer = trace.get_tracer("openai-provider")
        logger.info(
            "openai_provider_initialized",
            extra={"model": self._model, "timeout": self._timeout},
        )

    def chat(
        self,
        messages: list[dict[str, str]],
        *,
        output_schema: dict[str, Any] | None = None,
        tools: list[dict[str, Any]] | None = None,
        max_tokens: int = 1200,
        temperature: float | None = None,
        span_name: str = "openai.chat",
    ) -> tuple[str, ModelUsage]:
        """
        Send a chat completion request.

        Returns:
            (content_string, ModelUsage)

        Raises:
            ValueError on schema validation failure after repair attempt.
            RuntimeError on API errors.
        """
        t_start = time.monotonic()

        with self._tracer.start_as_current_span(span_name) as span:
            span.set_attribute("llm.model", self._model)
            span.set_attribute("llm.max_tokens", max_tokens)

            _messages = list(messages)
            
            kwargs: dict[str, Any] = {
                "model": self._model,
                "messages": _messages,
                "temperature": temperature if temperature is not None else self._temperature,
                "max_tokens": max_tokens,
            }
            if tools:
                kwargs["tools"] = tools
            if output_schema:
                kwargs["response_format"] = {"type": "json_object"}
                _messages.append({
                    "role": "system",
                    "content": f"You MUST return ONLY valid JSON that matches the following JSON Schema:\n{json.dumps(output_schema)}"
                })

            try:
                response = self._client.chat.completions.create(**kwargs)
            except RateLimitError as exc:
                span.record_exception(exc)
                raise RuntimeError(f"OpenAI rate limit: {exc}") from exc
            except APITimeoutError as exc:
                span.record_exception(exc)
                raise RuntimeError(f"OpenAI timeout: {exc}") from exc
            except APIError as exc:
                span.record_exception(exc)
                raise RuntimeError(f"OpenAI API error: {exc}") from exc

            latency_ms = int((time.monotonic() - t_start) * 1000)
            choice = response.choices[0]
            content = choice.message.content or ""

            usage = response.usage
            input_tokens = usage.prompt_tokens if usage else 0
            output_tokens = usage.completion_tokens if usage else 0
            total_tokens = usage.total_tokens if usage else 0
            cost = _estimate_cost(self._model, input_tokens, output_tokens)

            model_usage = ModelUsage(
                model=self._model,
                input_tokens=input_tokens,
                output_tokens=output_tokens,
                total_tokens=total_tokens,
                estimated_cost_usd=round(cost, 6),
                latency_ms=latency_ms,
            )

            span.set_attribute("llm.input_tokens", input_tokens)
            span.set_attribute("llm.output_tokens", output_tokens)
            span.set_attribute("llm.total_tokens", total_tokens)
            span.set_attribute("llm.latency_ms", latency_ms)
            span.set_attribute("llm.estimated_cost_usd", cost)

            logger.info(
                "openai_call_completed",
                extra={
                    "model": self._model,
                    "input_tokens": input_tokens,
                    "output_tokens": output_tokens,
                    "total_tokens": total_tokens,
                    "estimated_cost_usd": round(cost, 6),
                    "latency_ms": latency_ms,
                    "finish_reason": choice.finish_reason,
                },
            )

            if output_schema:
                content = self._validate_with_repair(
                    content, output_schema, messages, max_tokens
                )

            return content, model_usage

    def _validate_with_repair(
        self,
        content: str,
        schema: dict[str, Any],
        original_messages: list[dict[str, str]],
        max_tokens: int,
    ) -> str:
        """Validate JSON output against schema; one repair attempt on failure."""
        try:
            parsed = json.loads(content)
            jsonschema.validate(parsed, schema)
            return content
        except (json.JSONDecodeError, jsonschema.ValidationError) as e:
            first_err_msg = str(e)
            logger.warning(
                "output_schema_validation_failed_attempting_repair",
                extra={"error": first_err_msg},
            )

        # Repair attempt
        repair_messages = original_messages + [
            {"role": "assistant", "content": content},
            {
                "role": "user",
                "content": (
                    f"Your response did not match the required JSON schema. "
                    f"Error: {first_err_msg}. "
                    f"Please respond with valid JSON matching this schema: "
                    f"{json.dumps(schema)}"
                ),
            },
        ]
        try:
            repaired_response = self._client.chat.completions.create(
                model=self._model,
                messages=repair_messages,
                temperature=0,
                max_tokens=max_tokens,
                response_format={"type": "json_object"},
            )
            repaired_content = repaired_response.choices[0].message.content or ""
            parsed = json.loads(repaired_content)
            jsonschema.validate(parsed, schema)
            logger.info("output_schema_repair_succeeded")
            return repaired_content
        except Exception as repair_err:
            logger.error(
                "output_schema_repair_failed",
                extra={"error": str(repair_err)},
            )
            raise ValueError(
                f"LLM output failed schema validation after repair attempt: {repair_err}"
            ) from repair_err


# Singleton accessor
_provider: OpenAIProvider | None = None


def get_provider() -> OpenAIProvider:
    global _provider
    if _provider is None:
        _provider = OpenAIProvider()
    return _provider
