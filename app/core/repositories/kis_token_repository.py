from __future__ import annotations
from datetime import datetime, timezone, timedelta
from typing import Optional
from sqlalchemy.orm import Session
from sqlalchemy import and_
import logging
from app.core.models.kis_token import KISToken

class KISTokenRepository:
    def __init__(self, db: Session):
        self.db = db

    def get_valid_token(self, appkey_hash: str, tr_id: str, base_url: str, provider: str = "KIS") -> Optional[KISToken]:
        logger = logging.getLogger(__name__)
        # KST 기준으로 현재 시간 계산 (UTC+9)
        kst = timezone(timedelta(hours=9))
        now_kst = datetime.now(kst)
        now_utc = datetime.now(timezone.utc)
        
        logger.debug(f"🔍 토큰 만료 체크 - 현재시간(KST): {now_kst}, 현재시간(UTC): {now_utc}, tr_id: {tr_id}")
        
        # 먼저 조건 없이 토큰 조회해서 만료 시간 확인
        all_tokens = (
            self.db.query(KISToken)
            .filter(
                KISToken.provider == provider,
                KISToken.appkey_hash == appkey_hash,
                KISToken.tr_id == tr_id,
                KISToken.base_url == base_url,
            )
            .order_by(KISToken.expires_at.desc())
            .all()
        )
        
        if all_tokens:
            latest_token = all_tokens[0]
            # DB에 저장된 UTC 시간을 KST로 변환해서 비교
            expires_at_kst = latest_token.expires_at.astimezone(kst)
            logger.debug(f"📅 최신 토큰 만료시간(UTC): {latest_token.expires_at}")
            logger.debug(f"📅 최신 토큰 만료시간(KST): {expires_at_kst}")
            logger.debug(f"⏰ 만료 여부(KST 기준): {expires_at_kst} > {now_kst} = {expires_at_kst > now_kst}")
        
        # 유효한 토큰만 반환 (UTC 기준으로 비교하되, KST 기준으로 로깅)
        valid_token = (
            self.db.query(KISToken)
            .filter(
                KISToken.provider == provider,
                KISToken.appkey_hash == appkey_hash,
                KISToken.tr_id == tr_id,
                KISToken.base_url == base_url,
                KISToken.expires_at > now_utc,  # UTC 기준으로 DB 비교
            )
            .order_by(KISToken.expires_at.desc())
            .first()
        )
        
        if valid_token:
            expires_at_kst = valid_token.expires_at.astimezone(kst)
            logger.debug(f"✅ 유효한 토큰 발견 - 만료시간(KST): {expires_at_kst}")
        else:
            logger.debug(f"❌ 유효한 토큰 없음 - 모든 토큰이 만료됨")
            
        return valid_token

    def upsert_token(
        self,
        appkey_hash: str,
        tr_id: str,
        base_url: str,
        access_token: str,
        expires_at_utc: datetime,
        provider: str = "KIS",
    ) -> KISToken:
        token = (
            self.db.query(KISToken)
            .filter(
                KISToken.provider == provider,
                KISToken.appkey_hash == appkey_hash,
                KISToken.tr_id == tr_id,
                KISToken.base_url == base_url,
            )
            .first()
        )
        if token:
            token.access_token = access_token
            token.expires_at = expires_at_utc
        else:
            token = KISToken(
                provider=provider,
                base_url=base_url,
                appkey_hash=appkey_hash,
                tr_id=tr_id,
                access_token=access_token,
                expires_at=expires_at_utc,
            )
            self.db.add(token)
        self.db.commit()
        self.db.refresh(token)
        return token
