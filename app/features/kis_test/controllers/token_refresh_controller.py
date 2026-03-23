# app/features/kis_test/controllers/token_refresh_controller.py

from fastapi import APIRouter, HTTPException, Depends, Body
from sqlalchemy.orm import Session
from app.core.db import get_db
from app.features.kis_test.models.kis_test_models import (
    KISTokenRefreshRequest,
    KISTokenRefreshResponse
)
from app.features.kis_test.services.token_refresh_service import TokenRefreshService
import logging

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/kis-test/token", tags=["KIS Token Management"])


@router.post("/refresh", response_model=KISTokenRefreshResponse, summary="KIS 토큰 일괄 갱신")
async def refresh_expiring_tokens(
    request: KISTokenRefreshRequest = Body(...),
    db: Session = Depends(get_db)
):
    """
    KIS 토큰 일괄 갱신
    
    provider가 KIS인 토큰들 중 만료가 임박한 토큰들을 일괄로 갱신합니다.
    
    **핵심 특징:**
    - KIS API는 `appkey` + `appsecret`으로만 토큰을 발급하므로, 
      같은 appkey_hash + base_url 그룹의 토큰은 모두 동일합니다.
    - 따라서 **그룹별로 한 번만 토큰을 발급**받고, 
      같은 그룹의 모든 tr_id에 동일한 토큰을 적용합니다.
    
    **파라미터:**
    - threshold_hours: 갱신 임계 시간 (시간 단위, 기본값: 24시간)
      - 현재 시간 + threshold_hours 이내에 만료되는 토큰들을 갱신합니다.
      - 예: threshold_hours=24 → 24시간 이내 만료 예정 토큰 갱신
    - provider: 토큰 제공업체 (기본값: KIS)
      - "KIS": 실전투자 토큰
      - "KIS_VIRTUAL": 모의투자 토큰
    
    **응답 데이터:**
    - total_tokens: 갱신 대상 토큰 그룹 개수
    - success_count: 갱신 성공한 그룹 개수
    - failure_count: 갱신 실패한 그룹 개수
    - threshold_datetime: 임계 기준 일시 (UTC)
    - results: 각 그룹별 갱신 결과 상세 정보
    
    **사용 예시:**
    ```json
    {
      "threshold_hours": 48,
      "provider": "KIS"
    }
    ```
    
    **주의사항:**
    - 같은 appkey의 여러 tr_id가 있어도 **그룹별로 1회만 API 호출**합니다.
    - 재시도 로직이 포함되어 있어 일시적 오류에 대응합니다 (최대 3회, 60초 간격).
    - 보수적인 만료 시간을 설정합니다 (KIS 서버 시간 차이 고려, 2시간 skew).
    """
    try:
        logger.info(f"KIS 토큰 일괄 갱신 요청 - provider: {request.provider}, 임계: {request.threshold_hours}시간")
        
        service = TokenRefreshService(db)
        response = await service.refresh_expiring_tokens(request)
        
        logger.info(f"KIS 토큰 일괄 갱신 완료 - 성공: {response.success_count}, 실패: {response.failure_count}")
        
        return response
        
    except Exception as e:
        logger.error(f"KIS 토큰 일괄 갱신 중 오류: {str(e)}")
        raise HTTPException(status_code=500, detail=f"토큰 갱신 중 오류가 발생했습니다: {str(e)}")


@router.get("/status", summary="KIS 토큰 상태 조회")
async def get_token_status(
    provider: str = "KIS",
    db: Session = Depends(get_db)
):
    """
    KIS 토큰 상태 조회
    
    현재 저장된 KIS 토큰들의 상태를 조회합니다.
    
    **파라미터:**
    - provider: 토큰 제공업체 (기본값: KIS)
    
    **응답 데이터:**
    - tokens: 토큰 목록 (tr_id, base_url, 만료일시, 남은 시간 등)
    - summary: 토큰 상태 요약 (총 개수, 만료 임박 개수 등)
    """
    try:
        from datetime import datetime, timezone, timedelta
        from app.core.models.kis_token import KISToken
        
        logger.info(f"KIS 토큰 상태 조회 요청 - provider: {provider}")
        
        # 모든 토큰 조회
        tokens = (
            db.query(KISToken)
            .filter(KISToken.provider == provider)
            .order_by(KISToken.expires_at.asc())
            .all()
        )
        
        now_utc = datetime.now(timezone.utc)
        kst = timezone(timedelta(hours=9))
        
        # 토큰 정보 구성
        token_list = []
        expired_count = 0
        expiring_24h_count = 0
        expiring_7d_count = 0
        
        for token in tokens:
            expires_at_kst = token.expires_at.astimezone(kst)
            time_remaining = token.expires_at - now_utc
            
            # 상태 판정
            if time_remaining.total_seconds() <= 0:
                status = "만료됨"
                expired_count += 1
            elif time_remaining.total_seconds() <= 86400:  # 24시간
                status = "만료 임박 (24시간 이내)"
                expiring_24h_count += 1
            elif time_remaining.total_seconds() <= 604800:  # 7일
                status = "만료 임박 (7일 이내)"
                expiring_7d_count += 1
            else:
                status = "정상"
            
            token_list.append({
                "tr_id": token.tr_id,
                "base_url": token.base_url,
                "appkey_hash": token.appkey_hash[:8] + "...",
                "expires_at_utc": token.expires_at.isoformat(),
                "expires_at_kst": expires_at_kst.isoformat(),
                "time_remaining_hours": round(time_remaining.total_seconds() / 3600, 2),
                "status": status
            })
        
        summary = {
            "total_tokens": len(tokens),
            "expired_count": expired_count,
            "expiring_24h_count": expiring_24h_count,
            "expiring_7d_count": expiring_7d_count,
            "healthy_count": len(tokens) - expired_count - expiring_24h_count - expiring_7d_count
        }
        
        logger.info(f"KIS 토큰 상태 조회 완료 - 총 {len(tokens)}개")
        
        return {
            "provider": provider,
            "current_time_utc": now_utc.isoformat(),
            "current_time_kst": now_utc.astimezone(kst).isoformat(),
            "tokens": token_list,
            "summary": summary
        }
        
    except Exception as e:
        logger.error(f"KIS 토큰 상태 조회 중 오류: {str(e)}")
        raise HTTPException(status_code=500, detail=f"토큰 상태 조회 중 오류가 발생했습니다: {str(e)}")

