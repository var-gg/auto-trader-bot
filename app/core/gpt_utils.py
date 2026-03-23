# app/core/gpt_utils.py
import os, json
from typing import Tuple, List, Dict, Any
from openai import OpenAI

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

MODEL_CLASSIFY   = os.getenv("OPENAI_MODEL_CLASSIFY",   "gpt-4o-mini")
MODEL_SUMMARIZE  = os.getenv("OPENAI_MODEL_SUMMARIZE",  "gpt-4o-mini")
MODEL_TICKER_PICK= os.getenv("OPENAI_MODEL_TICKER_PICK","gpt-4o-mini")

# ---- Responses API helper (robust extractor)
def _first_text(resp) -> str:
    # Responses API
    try:
        return resp.output[0].content[0].text  # new-style
    except Exception:
        pass
    try:
        return resp.output_text  # convenience
    except Exception:
        pass
    # Chat Completions fallback
    try:
        return resp.choices[0].message.content
    except Exception:
        raise RuntimeError("No text content in response")

def _responses_json(model: str, user_text: str, schema_name: str, schema: dict, temperature: float = 0) -> Any:
    resp = client.responses.create(
        model=model,
        input=[{"role": "user", "content": user_text}],
        response_format={
            "type": "json_schema",
            "json_schema": {
                "name": schema_name,
                "strict": True,
                "schema": schema
            }
        },
        temperature=temperature,
    )
    txt = _first_text(resp)
    return json.loads(txt)

# ---- 1) 경제/주가 관련성
def classify_finance_relevance(title: str, summary: str | None = None) -> Tuple[bool, float | None, str | None]:
    schema = {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "is_related": {"type": "boolean"},
            "score": {"type": "number"}
        },
        "required": ["is_related", "score"]
    }
    user = (
        "아래 뉴스가 '증시/기업/경제/자금시장/연준/환율/상품/어닝/매크로 지표'와 관련 있으면 "
        "is_related=true, 아니면 false로만 판단하고 score는 0..1로 확신도를 주세요.\n\n"
        f"title: {title}\nsummary: {summary or ''}"
    )
    try:
        obj = _responses_json(MODEL_CLASSIFY, user, "FinanceRelevance", schema, temperature=0)
        return bool(obj["is_related"]), float(obj["score"]), MODEL_CLASSIFY
    except Exception:
        # 백업 키워드 방법 (비용 0)
        text = f"{title} {summary or ''}".lower()
        hit = any(k in text for k in ["stock","market","earnings","nasdaq","s&p","dow","fed","revenue","eps","ipo","guidance"])
        return hit, (0.8 if hit else 0.2), MODEL_CLASSIFY

# ---- 2) 요약(ko) + 테마 매핑
def summarize_and_tag_themes(
    title: str,
    content: str,
    theme_catalog: List[Dict],
    max_themes: int = 5
) -> tuple[str, str, str, List[Dict]]:
    schema = {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "summary_ko": {"type": "string"},
            "themes": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "properties": {
                        "theme_id": {"type": "integer"},
                        "confidence": {"type": "number"}
                    },
                    "required": ["theme_id","confidence"]
                }
            }
        },
        "required": ["summary_ko","themes"]
    }
    catalog_compact = [
        {"id": t["id"], "code": t.get("code"), "ko": t.get("name_ko") or t.get("code"), "en": t.get("name_en") or t.get("code")}
        for t in theme_catalog
    ]
    user = (
        f"다음 영문 기사를 한국어로 3~5문장 요약하고, 제공된 테마 목록에서 관련성이 높은 테마를 최대 {max_themes}개 선택하세요.\n"
        "[규칙]\n- summary_ko는 반드시 한국어\n- themes[].theme_id는 카탈로그의 id 사용\n- confidence는 0..1\n\n"
        f"[제목]\n{title}\n\n[본문]\n{content[:12000]}\n\n[테마 카탈로그]\n{json.dumps(catalog_compact, ensure_ascii=False)}"
    )
    obj = _responses_json(MODEL_SUMMARIZE, user, "NewsSummaryThemes", schema, temperature=0)
    summary_ko = str(obj["summary_ko"]).strip()
    themes = [{"theme_id": int(x["theme_id"]), "confidence": float(x["confidence"])} for x in (obj.get("themes") or [])][:max_themes]
    return summary_ko, "ko", MODEL_SUMMARIZE, themes

# ---- 3) 후보에서 최종 티커 선정
def select_relevant_tickers(title: str, summary_ko: str, candidate_tickers: List[str], max_tickers: int = 8) -> List[Dict]:
    schema = {
        "type": "array",
        "items": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "symbol": {"type": "string"},
                "confidence": {"type": "number"}
            },
            "required": ["symbol","confidence"]
        }
    }
    user = (
        f"다음 한국어 요약과 기사 제목을 근거로, 후보 티커 중 실제로 기사와 가장 연관된 종목을 최대 {max_tickers}개 선정하세요.\n"
        "[규칙]\n- 후보 목록에 없는 심볼은 절대 포함하지 말 것\n- confidence는 0..1\n\n"
        f"[제목]\n{title}\n\n[요약(ko)]\n{summary_ko}\n\n[티커 후보]\n{json.dumps(candidate_tickers, ensure_ascii=False)}"
    )
    obj = _responses_json(MODEL_TICKER_PICK, user, "TickerSelection", schema, temperature=0)
    if not isinstance(obj, list):
        return []
    out = []
    for item in obj[:max_tickers]:
        out.append({"symbol": str(item["symbol"]), "confidence": float(item["confidence"])})
    return out
