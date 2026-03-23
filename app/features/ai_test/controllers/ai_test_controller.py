# app/features/ai_test/controllers/ai_test_controller.py

from fastapi import APIRouter, HTTPException, Depends, Request, Form, Query
from typing import Dict, Any
from app.features.ai_test.models.ai_test_models import AIRequest, AIResponse, AIModel, AVAILABLE_MODELS
from app.features.ai_test.services.ai_test_service import AITestService
import logging

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/ai-test", tags=["AI Test"])

# 서비스 인스턴스
ai_test_service = AITestService()


@router.post("/generate", response_model=AIResponse)
async def generate_ai_response(
    model: AIModel = Query(..., description="AI 모델을 선택하세요", enum=["gpt-5-mini", "gpt-5-nano", "gpt-4-mini", "gemini-2.0-flash-lite", "gemini-2.0-flash", "gemini-2.5-flash-lite"]),
    prompt: str = Form(..., description="여기에 프롬프트를 직접 입력하세요 (멀티라인 가능)")
):
    """
    AI 모델로 응답 생성
    
    - **model**: AI 모델 선택 (드롭다운)
    - **prompt**: 프롬프트 입력 (텍스트에어리어, 멀티라인 지원)
    
    응답을 그대로 text로 반환합니다.
    """
    try:
        logger.info(f"AI 테스트 요청 - 모델: {model}, 프롬프트 길이: {len(prompt)}")
        
        ai_request = AIRequest(prompt=prompt, model=model)
        response = await ai_test_service.generate_response(ai_request)
        
        logger.info(f"AI 테스트 응답 완료 - 모델: {response.model}, 응답 길이: {len(response.response)}")
        
        return response
        
    except Exception as e:
        logger.error(f"AI 테스트 API 오류: {str(e)}")
        raise HTTPException(status_code=500, detail=f"AI 응답 생성 중 오류가 발생했습니다: {str(e)}")


@router.get("/models", response_model=Dict[str, Any])
async def get_available_models():
    """
    사용 가능한 AI 모델 목록 조회
    
    현재 지원하는 모든 AI 모델과 그 정보를 반환합니다.
    """
    try:
        return ai_test_service.get_available_models()
        
    except Exception as e:
        logger.error(f"모델 목록 조회 중 오류: {str(e)}")
        raise HTTPException(status_code=500, detail="모델 목록 조회 중 오류가 발생했습니다.")


@router.get("/health")
async def health_check():
    """
    AI 테스트 서비스 상태 확인
    """
    return {
        "status": "healthy",
        "service": "ai-test",
        "available_models": len(AVAILABLE_MODELS)
    }
