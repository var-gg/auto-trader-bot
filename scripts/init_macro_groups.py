#!/usr/bin/env python3
"""
매크로 그룹 초기화 스크립트
config.py의 MACRO_PROMPT_GROUPS 데이터를 데이터베이스에 저장합니다.
"""

import sys
import os
import logging

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy.orm import Session
from app.core.db import SessionLocal
from app.features.fred.repositories.macro_repository import MacroRepository

logger = logging.getLogger(__name__)

# config.py의 기존 데이터
MACRO_PROMPT_GROUPS = [
    {"code":"INFLATION", "name":"Inflation", "series":["CPIAUCSL","CPILFESL","PCEPI"]},
    {"code":"LABOR",     "name":"Labor",     "series":["UNRATE","PAYEMS","JTSJOL"]},
    {"code":"GROWTH",    "name":"Growth",    "series":["GDPC1","INDPRO","RSXFS"]},
    {"code":"RATES",     "name":"Rates",     "series":["FEDFUNDS","T10Y2Y","M2SL"]},
]

def init_macro_groups():
    """매크로 그룹을 초기화합니다."""
    db: Session = SessionLocal()
    repo = MacroRepository(db)
    
    try:
        for idx, group_data in enumerate(MACRO_PROMPT_GROUPS):
            # 그룹 생성/업데이트
            group = repo.upsert_group({
                "code": group_data["code"],
                "name": group_data["name"],
                "description": f"{group_data['name']} 관련 경제지표",
                "is_active": True,
                "sort_order": idx
            })
            
            # 그룹에 시리즈 연결
            series_count = repo.upsert_group_series(group.id, group_data["series"])
            
            logger.info(f"그룹 '{group_data['name']}' 생성 완료 - {series_count}개 시리즈 연결")
        
        logger.info(f"총 {len(MACRO_PROMPT_GROUPS)}개 그룹 초기화 완료!")
        
    except Exception as e:
        logger.error(f"초기화 실패: {e}")
        db.rollback()
        raise
    finally:
        db.close()

if __name__ == "__main__":
    init_macro_groups()
