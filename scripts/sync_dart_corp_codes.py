#!/usr/bin/env python3
"""
DART 기업코드 정보를 주기적으로 수집하여 데이터베이스에 동기화하는 스크립트
"""

import sys
import os
import logging
from typing import Dict, Any

# 프로젝트 루트를 Python 경로에 추가
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy.orm import Session
from app.core.db import get_db
from app.features.earnings.services.dart_corp_code_service import DartCorpCodeService

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("sync_dart_corp_codes") 


def sync_corp_codes_batch(db: Session, dry_run: bool = False) -> Dict[str, Any]:
    """
    기업코드 동기화 실행
    """
    try:
        logger.info("Starting DART corporation code sync process...")
        
        if dry_run:
            logger.info("DRY RUN MODE - No changes will be made to database")
            return {
                "status": "dry_run",
                "message": "Dry run completed - no changes made"
            }
        
        service = DartCorpCodeService(db)
        result = service.sync_corp_codes()
        
        logger.info(f"Sync result: {result}")
        return result
        
    except Exception as e:
        logger.error(f"Error in sync_corp_codes_batch: {str(e)}")
        return {
            "status": "error",
            "message": str(e)
        }


def get_database_stats(db: Session) -> Dict[str, Any]:
    """
    데이터베이스 현황 조회
    """
    try:
        service = DartCorpCodeService(db)
        stats = service.get_corp_code_stats()
        
        logger.info(f"Database stats: {stats}")
        return stats
        
    except Exception as e:
        logger.error(f"Error in get_database_stats: {str(e)}")
        return {
            "status": "error",
            "message": str(e)
        }


def validate_environment() -> bool:
    """
    환경 변수 및 설정 검증
    """
    try:
        from app.core.config import get_settings
        
        settings = get_settings()
        
        # 필수 설정 확인
        if not settings.DART_API_KEY:
            logger.error("DART_API_KEY is not set in environment")
            return False
        
        if not settings.DB_URL:
            logger.error("DB_URL is not set in environment")
            return False
        
        logger.info("Environment validation passed")
        return True
        
    except Exception as e:
        logger.error(f"Environment validation failed: {str(e)}")
        return False


def main():
    """
    메인 실행 함수
    """
    import argparse
    
    parser = argparse.ArgumentParser(description="Sync DART corporation codes")
    parser.add_argument(
        "--dry-run", 
        action="store_true", 
        help="Run in dry-run mode (no changes to database)"
    )
    parser.add_argument(
        "--stats-only", 
        action="store_true", 
        help="Show database statistics only"
    )
    parser.add_argument(
        "--validate-env", 
        action="store_true", 
        help="Validate environment configuration only"
    )
    
    args = parser.parse_args()
    
    logger.info("=" * 60)
    logger.info("DART CORPORATION CODE SYNC SCRIPT")
    logger.info("=" * 60)
    
    # 환경 검증
    if args.validate_env:
        if validate_environment():
            logger.info("✓ Environment validation passed")
            return 0
        else:
            logger.error("✗ Environment validation failed")
            return 1
    
    # 환경 설정 확인
    if not validate_environment():
        logger.error("Environment validation failed. Exiting.")
        return 1
    
    # 데이터베이스 연결
    db = next(get_db())
    
    try:
        # 통계만 조회하는 경우
        if args.stats_only:
            logger.info("Fetching database statistics...")
            stats_result = get_database_stats(db)
            
            if stats_result["status"] == "success":
                stats = stats_result["stats"]
                logger.info("=" * 50)
                logger.info("DATABASE STATISTICS")
                logger.info("=" * 50)
                logger.info(f"Total corp codes: {stats['total_corp_codes']}")
                logger.info(f"Active corp codes: {stats['active_corp_codes']}")
                logger.info(f"Stock listed: {stats['stock_listed_corp_codes']}")
                logger.info(f"Non-listed: {stats['non_listed_corp_codes']}")
                logger.info(f"Latest collected: {stats['latest_collected']}")
                return 0
            else:
                logger.error(f"Failed to get stats: {stats_result['message']}")
                return 1
        
        # 동기화 실행
        logger.info(f"Sync mode: {'DRY RUN' if args.dry_run else 'FULL SYNC'}")
        
        sync_result = sync_corp_codes_batch(db, args.dry_run)
        
        if sync_result["status"] == "success":
            stats = sync_result["summary"]
            
            logger.info("=" * 50)
            logger.info("SYNC COMPLETED SUCCESSFULLY")
            logger.info("=" * 50)
            logger.info(f"Total corp codes: {stats['total_corp_codes']}")
            logger.info(f"Updated: {stats['successful_updates']}")
            logger.info(f"Created: {stats['successful_creates']}")
            logger.info(f"Errors: {stats['errors']}")
            logger.info(f"success rate: {stats['success_rate']}%")
            
            # 에러가 있는 경우 상세 정보 출력
            if sync_result.get("stats", {}).get("errors", 0) > 0:
                logger.warning("Errors occurred during sync:")
                for error in sync_result["stats"].get("error_details", []):
                    logger.warning(f"  - Corp code {error['corp_code']}: {error['error']}")
            
            return 0
            
        elif sync_result["status"] == "dry_run":
            logger.info("Dry run completed successfully")
            return 0
            
        else:
            logger.error(f"Sync failed: {sync_result['message']}")
            return 1
        
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return 1
        
    finally:
        db.close()
        logger.info("Database connection closed")


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
