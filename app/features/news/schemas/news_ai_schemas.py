# app/features/news/schemas/news_ai_schemas.py

# 뉴스 AI 서비스용 GPT 스키마 정의

# 1. 뉴스 관련성 분류 스키마
FINANCE_RELEVANCE_SCHEMA = {
    "type": "object", 
    "additionalProperties": False,
    "properties": {"score": {"type": "number"}},
    "required": ["score"],
}

# 2. 뉴스 요약 및 테마 태깅 스키마
NEWS_SUMMARY_THEMES_SCHEMA = {
    "type":"object","additionalProperties":False,
    "properties":{
        "title_ko":{"type":"string"},
        "summary_ko":{"type":"string"},
        "themes":{"type":"array","items":{
            "type":"object","additionalProperties":False,
            "properties":{"theme_id":{"type":"integer"},"confidence":{"type":"number"}},
            "required":["theme_id","confidence"]
        }},
        "exchanges":{"type":"array","items":{
            "type":"object","additionalProperties":False,
            "properties":{"exchange_code":{"type":"string"},"confidence":{"type":"number"}},
            "required":["exchange_code","confidence"]
        }},
        "market_wide":{"type":"boolean"}
    },
    "required":["title_ko","summary_ko","themes","exchanges","market_wide"]
}

# 3. 티커/스코프 제안 스키마
TICKER_OR_SCOPE_SCHEMA = {
    "type":"object","additionalProperties":False,
    "properties":{
        "market_scope":{"type":"string","enum":["ALL","SECTOR","TICKERS"]},
        "sector_theme_ids":{"type":"array","items":{"type":"integer"}},
        "tickers":{"type":"array","items":{
            "type":"object","additionalProperties":False,
            "properties":{
                "symbol":{"type":"string"},
                "exchange":{"type":"string"},   # 야후코드(NMS, NYQ, ARCA, …)
                "country":{"type":"string"},    # US/KR/JP 등
                "confidence":{"type":"number"}
            },
            "required":["symbol","confidence"]
        }}
    },
    "required":["market_scope"]
}

# 4. 거래소 매핑 스키마 (추가 함수용)
EXCHANGE_MAPPING_SCHEMA = {
    "type": "object", "additionalProperties": False,
    "properties": {
        "exchanges": {
            "type": "array", "items": {
                "type": "object", "additionalProperties": False,
                "properties": {
                    "exchange_code": {"type": "string"},
                    "confidence": {"type": "number"}
                },
                "required": ["exchange_code", "confidence"]
            }
        }
    },
    "required": ["exchanges"]
}

# 5. 뉴스 V2 요약 스키마 (요약 + 제목)
NEWS_V2_SUMMARY_SCHEMA = {
    "type": "object", "additionalProperties": False,
    "properties": {
        "summary": {"type": "string"},
        "title": {"type": "string"}
    },
    "required": ["summary", "title"]
}

# 6. 뉴스 V2 재평가 스키마 (GPT-5 기반 티커 재평가) - 배열 형태로 비용 절약
NEWS_V2_REEVALUATION_SCHEMA = {
    "type": "object", "additionalProperties": False,
    "properties": {
        "results": {
            "type": "array",
            "items": {
                "type": "object", "additionalProperties": False,
                "properties": {
                    "id": {"type": "integer"},
                    "confi": {"type": "number", "minimum": 0.8, "maximum": 1.0}
                },
                "required": ["id", "confi"]
            }
        }
    },
    "required": ["results"]
}