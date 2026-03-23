# app/features/portfolio/schemas/order_schemas.py

# GPT 응답 스키마 정의 (전역변수)
ORDER_BATCH_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "batch": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "asof_kst": {"type": "string", "format": "date-time"},
                "mode": {"type": "string", "enum": ["BUY", "SELL"]},
                "currency": {"type": "string", "enum": ["USD", "KRW"]},
                "available_cash": {"type": "number", "minimum": 0},
                "notes": {"type": "string"}
            },
            "required": ["asof_kst", "mode", "currency", "available_cash"]
        },
        "plans": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "ticker_id": {"type": "integer"},
                    "action": {"type": "string", "enum": ["BUY", "SELL"]},
                    "reference": {
                        "type": "object",
                        "additionalProperties": False,
                        "properties": {
                            "recommendation_id": {"type": "integer"},
                            "breach": {
                                "type": ["string", "null"],
                                "enum": ["TARGET", "STOP", None]
                            }
                        }
                    },
                    "note": {"type": "string"},
                    "legs": {
                        "type": "array",
                        "minItems": 1,
                        "items": {
                            "type": "object",
                            "additionalProperties": False,
                            "properties": {
                                "type": {"type": "string", "enum": ["LIMIT", "LOC"]},
                                "side": {"type": "string", "enum": ["BUY", "SELL"]},
                                "quantity": {"type": "integer", "minimum": 1},
                                "limit_price": {
                                    "type": "number",
                                    "multipleOf": 0.01,
                                    "minimum": 0
                                }
                            },
                            "required": ["type", "side", "quantity", "limit_price"]
                        }
                    }
                },
                "required": ["ticker_id", "action", "note", "legs"]
            }
        },
        "skipped": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "ticker_id": {"type": "integer"},
                    "code": {
                        "type": "string",
                        "enum": ["HOLD", "CASH", "EXPIRED", "RISK", "SPREAD", "OTHER"]
                    },
                    "note": {"type": "string"}
                },
                "required": ["ticker_id", "code", "note"]
            }
        }
    },
    "required": ["batch", "plans", "skipped"]
}
