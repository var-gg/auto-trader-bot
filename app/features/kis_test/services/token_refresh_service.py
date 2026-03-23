# app/features/kis_test/services/token_refresh_service.py

from typing import List
from sqlalchemy.orm import Session
from datetime import datetime, timedelta, timezone
import time
import hashlib
import httpx
import logging

from app.core import config as settings
from app.core.models.kis_token import KISToken
from app.core.repositories.kis_token_repository import KISTokenRepository
from app.features.kis_test.models.kis_test_models import (
    KISTokenRefreshRequest,
    KISTokenRefreshResponse,
    KISTokenRefreshResult
)

logger = logging.getLogger(__name__)


def _appkey_hash(appkey: str, appsecret: str) -> str:
    """appkey + secret 해시 생성 (kis_client.py와 동일한 로직)"""
    src = f"{appkey}::{appsecret}".encode("utf-8")
    return hashlib.sha256(src).hexdigest()


class TokenRefreshService:
    """KIS 토큰 일괄 갱신 서비스
    
    provider가 KIS인 토큰들 중 만료가 임박한 토큰들을 일괄로 갱신합니다.
    """
    
    def __init__(self, db: Session):
        self.db = db
        self.repo = KISTokenRepository(db)
    
    async def refresh_expiring_tokens(self, request: KISTokenRefreshRequest) -> KISTokenRefreshResponse:
        """만료 임박 토큰 일괄 갱신
        
        Args:
            request: 토큰 갱신 요청 (임계 시간, provider)
            
        Returns:
            KISTokenRefreshResponse: 토큰 갱신 결과
            
        Raises:
            Exception: DB 조회 실패 시
        """
        logger.info(f"🔄 토큰 일괄 갱신 시작 - provider: {request.provider}, 임계: {request.threshold_hours}시간")
        
        # 임계 시간 계산 (현재 시간 + N시간)
        now_utc = datetime.now(timezone.utc)
        threshold_datetime = now_utc + timedelta(hours=request.threshold_hours)
        
        logger.info(f"⏰ 현재 시간(UTC): {now_utc}")
        logger.info(f"⏰ 임계 기준(UTC): {threshold_datetime} ({request.threshold_hours}시간 후)")
        
        # 만료 임박 토큰 조회 (provider 필터링)
        expiring_tokens = self._get_expiring_tokens(request.provider, threshold_datetime)
        
        total_tokens = len(expiring_tokens)
        logger.info(f"📋 갱신 대상 토큰: {total_tokens}개")
        
        if total_tokens == 0:
            logger.info("✅ 갱신 대상 토큰이 없습니다.")
            return KISTokenRefreshResponse(
                total_tokens=0,
                success_count=0,
                failure_count=0,
                threshold_hours=request.threshold_hours,
                threshold_datetime=threshold_datetime.isoformat(),
                results=[]
            )
        
        # 토큰 정보 로깅
        for token in expiring_tokens:
            kst = timezone(timedelta(hours=9))
            expires_at_kst = token.expires_at.astimezone(kst)
            logger.info(f"  - tr_id: {token.tr_id}, base_url: {token.base_url}, "
                       f"만료일시(UTC): {token.expires_at}, 만료일시(KST): {expires_at_kst}")
        
        # 각 토큰 갱신
        results: List[KISTokenRefreshResult] = []
        success_count = 0
        failure_count = 0
        
        for token in expiring_tokens:
            result = await self._refresh_single_token(token)
            results.append(result)
            
            if result.success:
                success_count += 1
            else:
                failure_count += 1
        
        logger.info(f"✅ 토큰 일괄 갱신 완료 - 성공: {success_count}, 실패: {failure_count}")
        
        return KISTokenRefreshResponse(
            total_tokens=total_tokens,
            success_count=success_count,
            failure_count=failure_count,
            threshold_hours=request.threshold_hours,
            threshold_datetime=threshold_datetime.isoformat(),
            results=results
        )
    
    def _get_expiring_tokens(self, provider: str, threshold_datetime: datetime) -> List[KISToken]:
        """만료 임박 토큰 그룹 조회 (appkey_hash + base_url 기준)
        
        KIS 토큰은 tr_id와 무관하게 appkey+appsecret으로 발급되므로,
        같은 appkey_hash + base_url + provider 조합의 토큰들은 동일합니다.
        따라서 각 그룹별로 대표 토큰 1개만 반환합니다.
        
        Args:
            provider: 토큰 제공업체
            threshold_datetime: 임계 기준 일시 (UTC)
            
        Returns:
            List[KISToken]: 만료 임박 토큰 목록 (그룹별 대표 1개씩)
        """
        try:
            # 모든 만료 임박 토큰 조회
            all_tokens = (
                self.db.query(KISToken)
                .filter(
                    KISToken.provider == provider,
                    KISToken.expires_at <= threshold_datetime
                )
                .order_by(KISToken.expires_at.asc())
                .all()
            )
            
            # appkey_hash + base_url 기준으로 그룹핑 (각 그룹당 대표 1개만)
            token_groups = {}
            for token in all_tokens:
                key = f"{token.appkey_hash}::{token.base_url}"
                if key not in token_groups:
                    token_groups[key] = token
            
            representative_tokens = list(token_groups.values())
            
            logger.debug(f"🔍 만료 임박 토큰 {len(all_tokens)}개 발견 → 그룹핑 결과 {len(representative_tokens)}개 대표 토큰")
            
            return representative_tokens
            
        except Exception as e:
            logger.error(f"❌ 만료 임박 토큰 조회 실패: {str(e)}")
            raise
    
    async def _refresh_single_token(self, token: KISToken) -> KISTokenRefreshResult:
        """토큰 그룹 갱신 (같은 appkey_hash + base_url의 모든 tr_id 토큰 업데이트)
        
        KIS 토큰은 tr_id와 무관하게 동일하므로, 한 번 발급받은 토큰을
        같은 appkey_hash + base_url을 가진 모든 tr_id 레코드에 적용합니다.
        
        Args:
            token: 갱신할 토큰 그룹의 대표 토큰
            
        Returns:
            KISTokenRefreshResult: 갱신 결과
        """
        logger.info(f"🔄 토큰 그룹 갱신 시작 - appkey_hash: {token.appkey_hash[:8]}..., base_url: {token.base_url}")
        
        old_expires_at = token.expires_at.isoformat()
        
        try:
            # provider에 따른 APPKEY/APPSECRET 선택
            if token.provider == "KIS_VIRTUAL":
                appkey = settings.KIS_VIRTUAL_APPKEY
                appsecret = settings.KIS_VIRTUAL_APPSECRET
                base_url = settings.KIS_VIRTUAL_BASE_URL
            else:  # KIS
                appkey = settings.KIS_APPKEY
                appsecret = settings.KIS_APPSECRET
                base_url = settings.KIS_BASE_URL
            
            # 같은 그룹의 모든 토큰 조회
            app_hash = _appkey_hash(appkey, appsecret)
            group_tokens = (
                self.db.query(KISToken)
                .filter(
                    KISToken.provider == token.provider,
                    KISToken.appkey_hash == app_hash,
                    KISToken.base_url == base_url
                )
                .all()
            )
            
            tr_ids = [t.tr_id for t in group_tokens]
            logger.info(f"📋 갱신 대상 tr_id 목록: {tr_ids} (총 {len(tr_ids)}개)")
            
            # 토큰 발급 (kis_client.py의 로직 참고)
            url = f"{base_url}/oauth2/tokenP"
            payload = {
                "grant_type": "client_credentials",
                "appkey": appkey,
                "appsecret": appsecret,
            }
            
            # 재시도 로직 (최대 3회, 60초 간격)
            max_retries = 3
            retry_delay = 60
            access_token = None
            data = {}
            
            for attempt in range(max_retries):
                try:
                    with httpx.Client(timeout=15) as client:
                        logger.debug(f"🌐 토큰 발급 API 호출 (시도 {attempt + 1}/{max_retries})")
                        r = client.post(url, json=payload)
                        r.raise_for_status()
                        data = r.json()
                        
                        access_token = data.get("access_token")
                        if not access_token:
                            raise ValueError("토큰 발급 응답에 access_token이 없습니다")
                        
                        # 성공하면 루프 탈출
                        break
                        
                except Exception as e:
                    logger.error(f"❌ 토큰 발급 실패 (시도 {attempt + 1}/{max_retries}): {str(e)}")
                    
                    # 마지막 시도가 아니면 대기 후 재시도
                    if attempt < max_retries - 1:
                        logger.info(f"⏰ {retry_delay}초 후 재시도합니다...")
                        time.sleep(retry_delay)
                    else:
                        # 모든 재시도 실패
                        raise
            
            # access_token이 설정되지 않았다면 에러
            if not access_token:
                raise ValueError("토큰 발급 응답에 access_token이 없습니다")
            
            # 만료 시간 계산 (kis_client.py와 동일한 로직)
            expires_in = int(data.get("expires_in", settings.KIS_TOKEN_TTL))
            
            # 보수적인 만료 시간 설정 (2시간 skew)
            conservative_skew = 7200  # 2시간
            expires_in_adj = max(300, expires_in - conservative_skew)  # 최소 5분은 보장
            
            expires_at_utc = datetime.now(timezone.utc) + timedelta(seconds=expires_in_adj)
            
            # KST 시간으로도 로깅
            kst = timezone(timedelta(hours=9))
            expires_at_kst = expires_at_utc.astimezone(kst)
            
            logger.info(f"⏰ 새 토큰 만료 정보 - 원본TTL: {expires_in}초, 조정TTL: {expires_in_adj}초")
            logger.info(f"⏰ 새 만료일시(UTC): {expires_at_utc}, 새 만료일시(KST): {expires_at_kst}")
            
            # 같은 그룹의 모든 tr_id 토큰 업데이트
            updated_count = 0
            for group_token in group_tokens:
                try:
                    self.repo.upsert_token(
                        appkey_hash=app_hash,
                        tr_id=group_token.tr_id,
                        base_url=base_url,
                        access_token=access_token,
                        expires_at_utc=expires_at_utc,
                        provider=token.provider,
                    )
                    updated_count += 1
                    logger.debug(f"  ✅ tr_id '{group_token.tr_id}' 토큰 업데이트 완료")
                except Exception as e:
                    logger.error(f"  ❌ tr_id '{group_token.tr_id}' 토큰 업데이트 실패: {str(e)}")
            
            logger.info(f"✅ 토큰 그룹 갱신 완료 - 업데이트: {updated_count}/{len(group_tokens)}개")
            
            return KISTokenRefreshResult(
                tr_id=f"그룹({', '.join(tr_ids)})",
                base_url=token.base_url,
                success=True,
                old_expires_at=old_expires_at,
                new_expires_at=expires_at_utc.isoformat(),
                error_message=None
            )
            
        except Exception as e:
            error_msg = str(e)
            logger.error(f"❌ 토큰 그룹 갱신 실패 - appkey_hash: {token.appkey_hash[:8]}..., 오류: {error_msg}")
            
            return KISTokenRefreshResult(
                tr_id=f"그룹(appkey_hash: {token.appkey_hash[:8]}...)",
                base_url=token.base_url,
                success=False,
                old_expires_at=old_expires_at,
                new_expires_at=None,
                error_message=error_msg
            )

