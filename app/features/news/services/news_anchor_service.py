# app/features/news/services/news_anchor_service.py
import logging
from typing import List, Optional, Dict, Any
from sqlalchemy.orm import Session
from sqlalchemy import and_

from app.core.vertex_ai_client import VertexAIClient
from app.features.news.models.news_anchor_vector import NewsAnchorVector

logger = logging.getLogger(__name__)

class NewsAnchorService:
    def __init__(self, db: Session):
        self.db = db
        self.vertex_ai_client = VertexAIClient()

    def create_anchor(self, code: str, name_ko: str, description: str, anchor_text: str) -> Optional[NewsAnchorVector]:
        """
        새로운 앵커 벡터 생성
        
        Args:
            code: 앵커 코드 (예: MACRO, EARNINGS)
            name_ko: 한글명
            description: 설명
            anchor_text: 앵커 문장 (임베딩 생성용)
            
        Returns:
            생성된 NewsAnchorVector 객체 또는 None
        """
        try:
            # 기존 앵커 확인
            existing_anchor = self.db.query(NewsAnchorVector).filter(
                NewsAnchorVector.code == code
            ).first()
            
            if existing_anchor:
                logger.warning(f"앵커 코드 '{code}'가 이미 존재합니다.")
                return None
            
            # 앵커 문장으로 임베딩 생성
            embedding_result = self.vertex_ai_client.get_embedding(anchor_text)
            
            if not embedding_result:
                logger.error(f"앵커 '{code}'의 임베딩 생성에 실패했습니다.")
                return None
            
            embedding = embedding_result["embedding"]
            
            # NewsAnchorVector 객체 생성
            anchor_vector = NewsAnchorVector(
                code=code,
                name_ko=name_ko,
                description=description,
                anchor_text=anchor_text,
                model_name=self.vertex_ai_client.model_name,
                vector_dimension=len(embedding),
                embedding_vector=embedding
            )
            
            self.db.add(anchor_vector)
            self.db.commit()
            logger.info(f"앵커 '{code}' 생성 완료")
            return anchor_vector
            
        except Exception as e:
            logger.error(f"앵커 '{code}' 생성 중 오류: {str(e)}")
            self.db.rollback()
            return None

    def update_anchor(self, code: str, name_ko: str = None, description: str = None, anchor_text: str = None) -> Optional[NewsAnchorVector]:
        """
        기존 앵커 벡터 업데이트
        
        Args:
            code: 앵커 코드
            name_ko: 한글명 (선택사항, 빈 문자열이면 업데이트하지 않음)
            description: 설명 (선택사항, 빈 문자열이면 업데이트하지 않음)
            anchor_text: 앵커 문장 (선택사항, 빈 문자열이면 업데이트하지 않음)
            
        Returns:
            업데이트된 NewsAnchorVector 객체 또는 None
        """
        try:
            anchor = self.db.query(NewsAnchorVector).filter(
                NewsAnchorVector.code == code
            ).first()
            
            if not anchor:
                logger.error(f"앵커 코드 '{code}'를 찾을 수 없습니다.")
                return None
            
            # 필드 업데이트 (빈 문자열이면 업데이트하지 않음)
            if name_ko is not None and name_ko.strip():
                anchor.name_ko = name_ko
            if description is not None and description.strip():
                anchor.description = description
            if anchor_text is not None and anchor_text.strip():
                anchor.anchor_text = anchor_text
                # 앵커 문장이 변경되면 임베딩도 재생성
                embedding_result = self.vertex_ai_client.get_embedding(anchor_text)
                if embedding_result:
                    anchor.embedding_vector = embedding_result["embedding"]
                    anchor.vector_dimension = len(embedding_result["embedding"])
                    anchor.model_name = self.vertex_ai_client.model_name
            
            self.db.commit()
            logger.info(f"앵커 '{code}' 업데이트 완료")
            return anchor
            
        except Exception as e:
            logger.error(f"앵커 '{code}' 업데이트 중 오류: {str(e)}")
            self.db.rollback()
            return None

    def get_anchor_by_code(self, code: str) -> Optional[NewsAnchorVector]:
        """코드로 앵커 조회"""
        return self.db.query(NewsAnchorVector).filter(
            NewsAnchorVector.code == code
        ).first()

    def get_all_anchors(self) -> List[NewsAnchorVector]:
        """모든 앵커 조회"""
        return self.db.query(NewsAnchorVector).all()

    def delete_anchor(self, code: str) -> bool:
        """
        앵커 삭제
        
        Args:
            code: 앵커 코드
            
        Returns:
            삭제 성공 여부
        """
        try:
            anchor = self.db.query(NewsAnchorVector).filter(
                NewsAnchorVector.code == code
            ).first()
            
            if not anchor:
                logger.error(f"앵커 코드 '{code}'를 찾을 수 없습니다.")
                return False
            
            self.db.delete(anchor)
            self.db.commit()
            logger.info(f"앵커 '{code}' 삭제 완료")
            return True
            
        except Exception as e:
            logger.error(f"앵커 '{code}' 삭제 중 오류: {str(e)}")
            self.db.rollback()
            return False

    def get_model_info(self) -> Dict[str, Any]:
        """현재 사용 중인 모델 정보 반환"""
        return self.vertex_ai_client.get_model_info()

    def get_anchor_stats(self) -> Dict[str, Any]:
        """앵커 통계 조회"""
        total_count = self.db.query(NewsAnchorVector).count()
        
        return {
            "total_anchors": total_count,
            "model_info": self.get_model_info()
        }
