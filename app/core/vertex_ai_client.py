# app/core/vertex_ai_client.py
import os
import time
from typing import List, Optional, Dict, Any
import logging
import tiktoken
from vertexai.preview.language_models import TextEmbeddingModel
from app.core.config import GOOGLE_CLOUD_PROJECT, GOOGLE_CLOUD_LOCATION, VERTEX_AI_EMBEDDING_MODEL

logger = logging.getLogger(__name__)

class VertexAIClient:
    def __init__(self, model_name: str = None):
        """Vertex AI 임베딩 클라이언트 초기화"""
        self.project = GOOGLE_CLOUD_PROJECT
        self.location = GOOGLE_CLOUD_LOCATION
        self.model_name = model_name or VERTEX_AI_EMBEDDING_MODEL or "textembedding-gecko@001"
        
        if not self.project:
            raise ValueError("GOOGLE_CLOUD_PROJECT 환경변수가 설정되지 않았습니다.")
        
        # Vertex AI 임베딩 모델 초기화
        self.model = TextEmbeddingModel.from_pretrained(self.model_name)
        
        logger.info(f"Vertex AI 임베딩 클라이언트 초기화 완료 - 모델: {self.model_name}")
        
        # tiktoken 인코더 초기화 (GPT-4용 인코더 사용)
        self.encoding = tiktoken.get_encoding("cl100k_base")

    def _truncate_text_by_tokens(self, text: str, max_tokens: int = 2048) -> str:
        """
        토큰 수 기준으로 텍스트 자르기
        
        Args:
            text: 원본 텍스트
            max_tokens: 최대 토큰 수 (기본값: 2048)
            
        Returns:
            토큰 수 제한에 맞춰 자른 텍스트
        """
        try:
            # 텍스트를 토큰으로 인코딩
            tokens = self.encoding.encode(text)
            
            # 토큰 수가 제한을 초과하면 자르기
            if len(tokens) > max_tokens:
                tokens = tokens[:max_tokens]
                truncated_text = self.encoding.decode(tokens)
                logger.warning(f"텍스트가 {max_tokens}토큰을 초과하여 잘렸습니다. (원본: {len(self.encoding.encode(text))}토큰)")
                return truncated_text
            
            return text
            
        except Exception as e:
            logger.error(f"토큰 기반 텍스트 자르기 중 오류: {str(e)}")
            # 오류 발생 시 문자 수 기준으로 폴백
            return text[:3072] if len(text) > 3072 else text

    def get_embedding(self, text: str) -> Optional[Dict[str, Any]]:
        """
        텍스트를 임베딩 벡터로 변환
        
        Args:
            text: 임베딩할 텍스트
            
        Returns:
            임베딩 벡터와 메타데이터를 포함한 딕셔너리 또는 None (실패시)
        """
        if not text or not text.strip():
            logger.warning("빈 텍스트는 임베딩할 수 없습니다.")
            return None
        
        try:
            start_time = time.time()
            
            # 원본 텍스트의 토큰 수 계산 (자르기 전)
            original_tokens = self.encoding.encode(text)
            original_token_length = len(original_tokens)
            
            # 토큰 수 기준으로 텍스트 자르기 (2048 토큰 제한)
            processed_text = self._truncate_text_by_tokens(text, max_tokens=2048)
            
            # Vertex API 호출
            embedding = self.model.get_embeddings([processed_text])[0].values
            
            processing_time = int((time.time() - start_time) * 1000)
            logger.info(f"임베딩 생성 완료 - 차원: {len(embedding)}, 원본토큰: {original_token_length}, 처리시간: {processing_time}ms")
            
            return {
                "embedding": embedding,
                "text_length": len(text),
                "token_length": original_token_length,
                "processing_time_ms": processing_time
            }
                
        except Exception as e:
            logger.error(f"임베딩 생성 중 오류 발생: {str(e)}")
            return None

    def get_embeddings_batch(self, texts: List[str]) -> List[Optional[Dict[str, Any]]]:
        """
        여러 텍스트를 배치로 임베딩
        
        Args:
            texts: 임베딩할 텍스트 리스트
            
        Returns:
            각 텍스트에 대한 임베딩 벡터와 메타데이터 리스트
        """
        if not texts:
            return []
        
        try:
            start_time = time.time()
            
            # 토큰 수 기준으로 텍스트 자르기 및 필터링
            valid_texts = []
            text_metadata = []
            
            for text in texts:
                if text and text.strip():
                    # 원본 텍스트의 토큰 수 계산 (자르기 전)
                    original_tokens = self.encoding.encode(text)
                    original_token_length = len(original_tokens)
                    
                    # 토큰 수 기준으로 자르기 (2048 토큰 제한)
                    processed_text = self._truncate_text_by_tokens(text, max_tokens=2048)
                    valid_texts.append(processed_text)
                    
                    # 메타데이터 저장
                    text_metadata.append({
                        "text_length": len(text),
                        "token_length": original_token_length
                    })
                else:
                    valid_texts.append("")  # 빈 텍스트는 빈 문자열로 처리
                    text_metadata.append({
                        "text_length": 0,
                        "token_length": 0
                    })
            
            if not valid_texts:
                logger.warning("유효한 텍스트가 없습니다.")
                return []
            
            # 배치 임베딩 호출
            responses = self.model.get_embeddings(valid_texts)
            
            processing_time = int((time.time() - start_time) * 1000)
            logger.info(f"배치 임베딩 완료 - {len(responses)}개 텍스트, 처리시간: {processing_time}ms")
            
            # 결과 변환
            results = []
            for i, response in enumerate(responses):
                if response and hasattr(response, 'values'):
                    result = {
                        "embedding": response.values,
                        "text_length": text_metadata[i]["text_length"],
                        "token_length": text_metadata[i]["token_length"],
                        "processing_time_ms": processing_time
                    }
                    results.append(result)
                else:
                    results.append(None)
            
            return results
            
        except Exception as e:
            logger.error(f"배치 임베딩 중 오류 발생: {str(e)}")
            return [None] * len(texts)

    def get_model_info(self) -> Dict[str, Any]:
        """
        현재 사용 중인 모델 정보 반환
        
        Returns:
            모델 정보 딕셔너리
        """
        return {
            "model_name": self.model_name,
            "project": self.project,
            "location": self.location,
            "model_type": "textembedding-gecko"
        }
