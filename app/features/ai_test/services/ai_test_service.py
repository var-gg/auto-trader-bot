# app/features/ai_test/services/ai_test_service.py

import openai
from typing import Dict, Any
from app.core.config import OPENAI_API_KEY, GOOGLE_APPLICATION_CREDENTIALS, GOOGLE_CLOUD_PROJECT, GOOGLE_CLOUD_LOCATION
from app.features.ai_test.models.ai_test_models import AIModel, AIProvider, AIRequest, AIResponse
import logging

# Vertex AI imports
try:
    import vertexai
    from vertexai.preview.generative_models import GenerativeModel
    VERTEX_AI_AVAILABLE = True
except ImportError:
    VERTEX_AI_AVAILABLE = False

logger = logging.getLogger(__name__)


class AITestService:
    """AI 테스트 서비스 - 각종 생성형 AI 모델 테스트"""
    
    def __init__(self):
        self.openai_client = openai.OpenAI(api_key=OPENAI_API_KEY)
        
        # Vertex AI 초기화
        if VERTEX_AI_AVAILABLE:
            try:
                vertexai.init(
                    project=GOOGLE_CLOUD_PROJECT, 
                    location="us-central1"
                )
                self.vertex_ai_initialized = True
            except Exception as e:
                logger.warning(f"Vertex AI 초기화 실패: {e}")
                self.vertex_ai_initialized = False
        else:
            self.vertex_ai_initialized = False
            logger.warning("Vertex AI 라이브러리가 설치되지 않았습니다.")
        
        # 모델별 설정 매핑
        self.model_configs = {
            AIModel.GPT_5_MINI: {
                "provider": AIProvider.OPENAI,
                "model_name": "gpt-5-mini"
            },
            AIModel.GPT_5_NANO: {
                "provider": AIProvider.OPENAI,
                "model_name": "gpt-5-nano"
            },
            AIModel.GPT_4_MINI: {
                "provider": AIProvider.OPENAI,
                "model_name": "gpt-4-mini"
            },
            AIModel.GEMINI_2_FLASH_LITE: {
                "provider": AIProvider.GEMINI,
                "model_name": "gemini-2.0-flash-lite"
            },
            AIModel.GEMINI_2_FLASH: {
                "provider": AIProvider.GEMINI,
                "model_name": "gemini-2.0-flash"
            },
            AIModel.GEMINI_2_5_FLASH_LITE: {
                "provider": AIProvider.GEMINI,
                "model_name": "gemini-2.5-flash-lite"
            }
        }
    
    async def generate_response(self, request: AIRequest) -> AIResponse:
        """AI 모델로 응답 생성"""
        try:
            model_config = self.model_configs.get(request.model)
            if not model_config:
                raise ValueError(f"지원하지 않는 모델입니다: {request.model}")
            
            provider = model_config["provider"]
            model_name = model_config["model_name"]
            
            if provider == AIProvider.OPENAI:
                response_text, raw_response = await self._call_openai(request.prompt, model_name)
            elif provider == AIProvider.GEMINI:
                response_text, raw_response = await self._call_gemini(request.prompt, model_name)
            else:
                raise ValueError(f"지원하지 않는 제공자입니다: {provider}")
            
            return AIResponse(
                response=response_text,
                model=request.model,
                provider=provider,
                raw_response=raw_response
            )
            
        except Exception as e:
            logger.error(f"AI 응답 생성 중 오류 발생: {str(e)}")
            raise
    
    async def _call_openai(self, prompt: str, model_name: str) -> tuple[str, Dict[str, Any]]:
        """OpenAI API 호출 - 원본 응답과 텍스트 모두 반환"""
        try:
            response = self.openai_client.chat.completions.create(
                model=model_name,
                messages=[
                    {"role": "user", "content": prompt}
                ],
                #max_tokens=4000,
                #temperature=1
            )
            
            # 원본 응답을 딕셔너리로 변환
            raw_response = {
                "id": response.id,
                "object": response.object,
                "created": response.created,
                "model": response.model,
                "choices": [
                    {
                        "index": choice.index,
                        "message": {
                            "role": choice.message.role,
                            "content": choice.message.content
                        },
                        "finish_reason": choice.finish_reason
                    }
                    for choice in response.choices
                ],
                "usage": {
                    "prompt_tokens": response.usage.prompt_tokens,
                    "completion_tokens": response.usage.completion_tokens,
                    "total_tokens": response.usage.total_tokens
                }
            }
            
            return response.choices[0].message.content, raw_response
            
        except Exception as e:
            logger.error(f"OpenAI API 호출 중 오류: {str(e)}")
            raise
    
    async def _call_gemini(self, prompt: str, model_name: str) -> tuple[str, Dict[str, Any]]:
        """Vertex AI Gemini API 호출 - 원본 응답과 텍스트 모두 반환"""
        if not VERTEX_AI_AVAILABLE or not self.vertex_ai_initialized:
            raise RuntimeError("Vertex AI가 초기화되지 않았습니다.")
        
        try:
            model = GenerativeModel(model_name)
            response = model.generate_content(prompt, stream=False)
            
            # 원본 응답을 딕셔너리로 변환
            raw_response = {
                "text": response.text,
                "candidates": [
                    {
                        "content": {
                            "parts": [
                                {
                                    "text": part.text
                                }
                                for part in candidate.content.parts
                            ],
                            "role": candidate.content.role
                        },
                        "finish_reason": candidate.finish_reason,
                        "index": candidate.index,
                        "safety_ratings": [
                            {
                                "category": rating.category,
                                "probability": rating.probability
                            }
                            for rating in candidate.safety_ratings
                        ]
                    }
                    for candidate in response.candidates
                ] if response.candidates else [],
                "usage_metadata": {
                    "prompt_token_count": response.usage_metadata.prompt_token_count,
                    "candidates_token_count": response.usage_metadata.candidates_token_count,
                    "total_token_count": response.usage_metadata.total_token_count
                } if response.usage_metadata else None
            }
            
            return response.text, raw_response
            
        except Exception as e:
            logger.error(f"Vertex AI Gemini API 호출 중 오류: {str(e)}")
            raise
    
    def get_available_models(self) -> Dict[str, Any]:
        """사용 가능한 모델 목록 반환"""
        from app.features.ai_test.models.ai_test_models import AVAILABLE_MODELS
        
        return {
            "models": [
                {
                    "name": model.name,
                    "display_name": model.display_name,
                    "provider": model.provider,
                    "description": model.description
                }
                for model in AVAILABLE_MODELS
            ]
        }
