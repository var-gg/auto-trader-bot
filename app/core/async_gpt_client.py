# app/core/async_gpt_client.py
"""
배치 추천 생성을 위한 비동기 GPT 클라이언트.
기존 gpt_client.py와 독립적으로 동작하여 호환성 문제를 방지합니다.
"""

from __future__ import annotations
import json, time, traceback
from typing import Any, Optional
from openai import AsyncOpenAI, BadRequestError
from app.core.config import OPENAI_API_KEY, GPT_LOG_ENABLED, GPT_LOG_MAX_PROMPT_CHARS, GPT_LOG_MAX_RESPONSE_CHARS
from app.core.db import SessionLocal
from app.shared.models.gpt_call_log import GptCallLog

# 배치 처리 전용 비동기 클라이언트
async_client = AsyncOpenAI(api_key=OPENAI_API_KEY)

_JSON_ONLY_SYSTEM = (
    "You are a strict JSON generator. Return ONLY valid JSON that matches the user's schema. "
    "No prose, no code fences, no explanations."
)

def _extract_text_flex(resp) -> str:
    if hasattr(resp, "output_text"):
        return resp.output_text
    try:
        return resp.output[0].content[0].text
    except Exception:
        pass
    try:
        return resp.choices[0].message.content
    except Exception:
        pass
    return str(resp)

def _safe_truncate(s: Optional[str], limit: int) -> Optional[str]:
    if s is None:
        return None
    if len(s) <= limit:
        return s
    return s[:limit] + f"\n... [truncated at {limit} chars]"

def _log_gpt_call(
    *,
    task: str,
    model: str,
    schema_name: Optional[str],
    user_text: str,
    response_text: Optional[str],
    ok: bool,
    err: Optional[str],
    tokens_input: Optional[int],
    tokens_output: Optional[int],
    latency_ms: Optional[float],
    news_id: Optional[int],
    extra: Optional[dict]
) -> None:
    if not GPT_LOG_ENABLED:
        return
    sess = SessionLocal()
    try:
        row = GptCallLog(
            task=task,
            model=model,
            schema_name=schema_name,
            prompt=_safe_truncate(user_text, GPT_LOG_MAX_PROMPT_CHARS),
            response_text=_safe_truncate(response_text, GPT_LOG_MAX_RESPONSE_CHARS),
            ok=1 if ok else 0,
            error=_safe_truncate(err, 4000),
            tokens_input=tokens_input,
            tokens_output=tokens_output,
            latency_ms=latency_ms,
            news_id=news_id,
            extra=extra or {},
        )
        sess.add(row)
        sess.commit()
    except Exception:
        sess.rollback()
    finally:
        sess.close()

def _supports_temperature(model: str) -> bool:
    """
    일부 모델(예: gpt-5-mini)은 temperature를 명시적으로 보내면 400을 냅니다.
    이런 모델에 대해선 temperature 파라미터 자체를 생략해야 합니다.
    """
    m = (model or "").lower()
    # 알려진 비지원(또는 default만 허용) 패밀리
    no_temp_prefixes = ("gpt-5-mini",)
    return not any(m.startswith(p) for p in no_temp_prefixes)

async def responses_json_async(
    *,
    model: str,
    schema_name: str,
    schema: dict,
    user_text: str,
    temperature: float = 0.0,
    task: str = "generic",
    news_id: Optional[int] = None,
    extra: Optional[dict] = None,
) -> Any:
    """
    비동기 버전의 GPT 호출 함수.
    Cloud Run vCPU=1 환경에서 네트워크 I/O 병렬 처리에 최적화.
    기존 gpt_client.py와 독립적으로 동작합니다.
    """
    start = time.time()
    raw_text = None
    tokens_in = None
    tokens_out = None

    # -----------------------------
    # 1) Responses API (json_schema)
    # -----------------------------
    try:
        req_kwargs = dict(
            model=model,
            input=[{"role": "user", "content": user_text}],
            response_format={"type": "json_schema", "json_schema": {"name": schema_name, "strict": True, "schema": schema}},
        )
        if _supports_temperature(model):
            req_kwargs["temperature"] = temperature

        try:
            resp = await async_client.responses.create(**req_kwargs)
        except BadRequestError as e:
            # temperature 문제로 400이면 제거 후 한 번 재시도
            if "temperature" in (e.body.get("error", {}).get("param", "") if hasattr(e, "body") else "") \
               or "temperature" in str(e):
                req_kwargs.pop("temperature", None)
                resp = await async_client.responses.create(**req_kwargs)
            else:
                raise

        raw_text = _extract_text_flex(resp)
        try:
            usage = getattr(resp, "usage", None) or {}
            tokens_in = int(usage.get("input_tokens", None)) if isinstance(usage, dict) else None
            tokens_out = int(usage.get("output_tokens", None)) if isinstance(usage, dict) else None
        except Exception:
            pass
        obj = json.loads(raw_text)
        latency = (time.time() - start) * 1000.0
        _log_gpt_call(
            task=task, model=model, schema_name=schema_name, user_text=user_text, response_text=raw_text,
            ok=True, err=None, tokens_input=tokens_in, tokens_output=tokens_out, latency_ms=latency,
            news_id=news_id, extra=extra
        )
        return obj
    except TypeError:
        # 구형 SDK 경로로 폴백
        pass
    except Exception:
        # 다음 단계로 폴백
        pass

    # --------------------------------------------
    # 2) Chat Completions + response_format=json_object
    # --------------------------------------------
    try:
        req_kwargs = dict(
            model=model,
            messages=[
                {"role": "system", "content": _JSON_ONLY_SYSTEM + f" Schema name: {schema_name}. Schema: {json.dumps(schema)}"},
                {"role": "user", "content": user_text + "\n\nReturn ONLY JSON."},
            ],
            response_format={"type": "json_object"},
        )
        if _supports_temperature(model):
            req_kwargs["temperature"] = temperature

        try:
            resp = await async_client.chat.completions.create(**req_kwargs)
        except BadRequestError as e:
            if "temperature" in (e.body.get("error", {}).get("param", "") if hasattr(e, "body") else "") \
               or "unsupported_value" in str(e) or "temperature" in str(e):
                req_kwargs.pop("temperature", None)
                resp = await async_client.chat.completions.create(**req_kwargs)
            else:
                raise

        raw_text = _extract_text_flex(resp)
        try:
            usage = getattr(resp, "usage", None) or {}
            tokens_in = int(usage.get("prompt_tokens", None)) if isinstance(usage, dict) else None
            tokens_out = int(usage.get("completion_tokens", None)) if isinstance(usage, dict) else None
        except Exception:
            pass
        obj = json.loads(raw_text)
        latency = (time.time() - start) * 1000.0
        _log_gpt_call(
            task=task, model=model, schema_name=schema_name, user_text=user_text, response_text=raw_text,
            ok=True, err=None, tokens_input=tokens_in, tokens_output=tokens_out, latency_ms=latency,
            news_id=news_id, extra=extra
        )
        return obj
    except TypeError:
        pass
    except Exception:
        pass

    # -----------------------------
    # 3) 최후 수단: Chat Completions 일반
    # -----------------------------
    try:
        req_kwargs = dict(
            model=model,
            messages=[
                {"role": "system", "content": _JSON_ONLY_SYSTEM + f" Schema name: {schema_name}. Schema: {json.dumps(schema)}"},
                {"role": "user", "content": user_text + "\n\nReturn ONLY JSON."},
            ],
        )
        if _supports_temperature(model):
            req_kwargs["temperature"] = temperature

        try:
            resp = await async_client.chat.completions.create(**req_kwargs)
        except BadRequestError as e:
            if "temperature" in (e.body.get("error", {}).get("param", "") if hasattr(e, "body") else "") \
               or "unsupported_value" in str(e) or "temperature" in str(e):
                req_kwargs.pop("temperature", None)
                resp = await async_client.chat.completions.create(**req_kwargs)
            else:
                raise

        raw_text = _extract_text_flex(resp)
        txt = raw_text.strip()
        if txt.startswith("```"):
            first = txt.find("{")
            last = txt.rfind("}")
            if first != -1 and last != -1:
                txt = txt[first:last+1]
        obj = json.loads(txt)
        latency = (time.time() - start) * 1000.0
        _log_gpt_call(
            task=task, model=model, schema_name=schema_name, user_text=user_text, response_text=raw_text,
            ok=True, err=None, tokens_input=None, tokens_output=None, latency_ms=latency,
            news_id=news_id, extra=extra
        )
        return obj
    except Exception:
        latency = (time.time() - start) * 1000.0
        _log_gpt_call(
            task=task, model=model, schema_name=schema_name, user_text=user_text, response_text=raw_text,
            ok=False, err=traceback.format_exc(), tokens_input=tokens_in, tokens_output=tokens_out, latency_ms=latency,
            news_id=news_id, extra=extra
        )
        raise
