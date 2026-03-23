# app/features/ai_test/models/ai_test_models.py

from pydantic import BaseModel
from typing import List, Optional, Dict, Any
from enum import Enum


class AIModel(str, Enum):
    """지원하는 AI 모델 목록"""
    GPT_5_MINI = "gpt-5-mini"
    GPT_5_NANO = "gpt-5-nano"
    GPT_4_MINI = "gpt-4-mini"
    GEMINI_2_FLASH_LITE = "gemini-2.0-flash-lite"
    GEMINI_2_FLASH = "gemini-2.0-flash"
    GEMINI_2_5_FLASH_LITE = "gemini-2.5-flash-lite"


class AIProvider(str, Enum):
    """AI 제공자"""
    OPENAI = "openai"
    GEMINI = "gemini"


class AIRequest(BaseModel):
    """AI 테스트 요청 모델"""
    prompt: str
    model: AIModel


class AIResponse(BaseModel):
    """AI 테스트 응답 모델"""
    response: str
    model: str
    provider: str
    raw_response: Optional[Dict[str, Any]] = None  # 원본 응답 데이터


class ModelInfo(BaseModel):
    """모델 정보"""
    name: str
    display_name: str
    provider: AIProvider
    description: Optional[str] = None


# 하드코딩된 모델 리스트
AVAILABLE_MODELS: List[ModelInfo] = [
    ModelInfo(
        name="gpt-5-mini",
        display_name="GPT-5 Mini",
        provider=AIProvider.OPENAI,
        description="OpenAI의 최신 GPT-5 Mini 모델"
    ),
    ModelInfo(
        name="gpt-5-nano",
        display_name="GPT-5 Nano",
        provider=AIProvider.OPENAI,
        description="OpenAI의 경량화된 GPT-5 Nano 모델"
    ),
    ModelInfo(
        name="gpt-4-mini",
        display_name="GPT-4 Mini",
        provider=AIProvider.OPENAI,
        description="OpenAI의 GPT-4 Mini 모델"
    ),
    ModelInfo(
        name="gemini-2.0-flash-lite",
        display_name="Gemini 2.0 Flash Lite",
        provider=AIProvider.GEMINI,
        description="Google의 Gemini 2.0 Flash Lite 모델"
    ),
    ModelInfo(
        name="gemini-2.0-flash",
        display_name="Gemini 2.0 Flash",
        provider=AIProvider.GEMINI,
        description="Google의 Gemini 2.0 Flash 모델"
    ),
    ModelInfo(
        name="gemini-2.5-flash-lite",
        display_name="Gemini 2.5 Flash Lite",
        provider=AIProvider.GEMINI,
        description="Google의 Gemini 2.5 Flash Lite 모델"
    ),
]
