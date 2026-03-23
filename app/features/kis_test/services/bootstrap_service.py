# app/features/kis_test/services/bootstrap_service.py

import time
from datetime import datetime, timezone, date, timedelta
from typing import List
from sqlalchemy.orm import Session
import logging

from app.features.kis_test.models.kis_test_models import (
    BootstrapRequest,
    BootstrapResponse,
    BootstrapStepResult,
    KISTokenRefreshRequest
)
from app.features.kis_test.services.token_refresh_service import TokenRefreshService
from app.features.fred.services.fred_sync_service import FredSyncService
from app.features.fred.repositories.macro_repository import MacroRepository
from app.features.yahoo_finance.services.yahoo_index_service import YahooIndexService
from app.features.yahoo_finance.models.yahoo_finance_models import YahooIndexIngestRequest
# v2 사용으로 변경 (import는 함수 내부에서 처리)

logger = logging.getLogger(__name__)


class BootstrapService:
    """장전 기초데이터 일괄 갱신 서비스
    
    여러 데이터 수집 서비스를 순차적으로 호출하여 장전에 필요한 기초 데이터를 갱신합니다.
    내부 서비스를 직접 호출하므로 네트워크 오버헤드 없이 효율적으로 동작합니다.
    """
    
    def __init__(self, db: Session):
        self.db = db
    
    async def run_bootstrap(self, request: BootstrapRequest) -> BootstrapResponse:
        """장전 기초데이터 일괄 갱신 실행
        
        Args:
            request: Bootstrap 요청 설정
            
        Returns:
            BootstrapResponse: 전체 실행 결과
        """
        logger.info("🚀 장전 기초데이터 일괄 갱신 시작")
        
        started_at = datetime.now(timezone.utc)
        steps: List[BootstrapStepResult] = []
        
        # 1. 토큰 갱신
        if not request.skip_token_refresh:
            step_result = await self._refresh_tokens(request.token_threshold_hours)
            steps.append(step_result)
        else:
            steps.append(BootstrapStepResult(
                step_name="token_refresh",
                step_description="KIS 토큰 갱신",
                success=True,
                duration_seconds=0,
                result_summary={"status": "skipped"},
                error_message=None
            ))
        
        # 2. FRED 데이터 수집
        if not request.skip_fred_ingest:
            step_result = await self._ingest_fred_data(request.fred_lookback_days)
            steps.append(step_result)
        else:
            steps.append(BootstrapStepResult(
                step_name="fred_ingest",
                step_description="FRED 매크로 데이터 수집",
                success=True,
                duration_seconds=0,
                result_summary={"status": "skipped"},
                error_message=None
            ))
        
        # 3. Yahoo Finance 데이터 수집
        if not request.skip_yahoo_ingest:
            step_result = await self._ingest_yahoo_data(request.yahoo_period)
            steps.append(step_result)
        else:
            steps.append(BootstrapStepResult(
                step_name="yahoo_ingest",
                step_description="Yahoo Finance 데이터 수집",
                success=True,
                duration_seconds=0,
                result_summary={"status": "skipped"},
                error_message=None
            ))
        
        # 4. 프리마켓 리스크 스냅샷 갱신
        if not request.skip_risk_refresh:
            step_result = await self._refresh_premarket_risk()
            steps.append(step_result)
        else:
            steps.append(BootstrapStepResult(
                step_name="risk_refresh",
                step_description="프리마켓 리스크 스냅샷 갱신",
                success=True,
                duration_seconds=0,
                result_summary={"status": "skipped"},
                error_message=None
            ))

        # 5. 시그널 갱신
        if not request.skip_signal_update:
            step_result = await self._update_signals()
            steps.append(step_result)
        else:
            steps.append(BootstrapStepResult(
                step_name="signal_update",
                step_description="프리마켓 시그널 갱신",
                success=True,
                duration_seconds=0,
                result_summary={"status": "skipped"},
                error_message=None
            ))
        
        # 전체 결과 집계
        completed_at = datetime.now(timezone.utc)
        total_duration = (completed_at - started_at).total_seconds()
        
        successful_steps = sum(1 for s in steps if s.success and s.result_summary.get("status") != "skipped")
        failed_steps = sum(1 for s in steps if not s.success)
        skipped_steps = sum(1 for s in steps if s.result_summary and s.result_summary.get("status") == "skipped")
        
        overall_success = failed_steps == 0
        
        logger.info(f"✅ 장전 기초데이터 일괄 갱신 완료 - 성공: {successful_steps}, 실패: {failed_steps}, 스킵: {skipped_steps}")
        
        return BootstrapResponse(
            overall_success=overall_success,
            total_steps=len(steps),
            successful_steps=successful_steps,
            failed_steps=failed_steps,
            skipped_steps=skipped_steps,
            total_duration_seconds=total_duration,
            started_at=started_at.isoformat(),
            completed_at=completed_at.isoformat(),
            steps=steps
        )
    
    async def _refresh_tokens(self, threshold_hours: int) -> BootstrapStepResult:
        """토큰 갱신 단계 (내부 서비스 직접 호출)
        
        Args:
            threshold_hours: 토큰 갱신 임계 시간
            
        Returns:
            BootstrapStepResult: 단계 실행 결과
        """
        logger.info("🔑 1단계: KIS 토큰 갱신 시작")
        start_time = time.time()
        
        try:
            # TokenRefreshService 직접 호출
            token_service = TokenRefreshService(self.db)
            token_request = KISTokenRefreshRequest(
                threshold_hours=threshold_hours,
                provider="KIS"
            )
            
            response = await token_service.refresh_expiring_tokens(token_request)
            
            duration = time.time() - start_time
            
            result_summary = {
                "total_tokens": response.total_tokens,
                "success_count": response.success_count,
                "failure_count": response.failure_count
            }
            
            success = response.failure_count == 0
            
            logger.info(f"✅ 1단계 완료 - 토큰 갱신: {result_summary['success_count']}/{result_summary['total_tokens']}개")
            
            return BootstrapStepResult(
                step_name="token_refresh",
                step_description="KIS 토큰 갱신",
                success=success,
                duration_seconds=duration,
                result_summary=result_summary,
                error_message=None if success else f"일부 토큰 갱신 실패: {result_summary['failure_count']}개"
            )
            
        except Exception as e:
            duration = time.time() - start_time
            error_msg = str(e)
            logger.error(f"❌ 1단계 실패 - 토큰 갱신 오류: {error_msg}")
            
            return BootstrapStepResult(
                step_name="token_refresh",
                step_description="KIS 토큰 갱신",
                success=False,
                duration_seconds=duration,
                result_summary=None,
                error_message=error_msg
            )
    
    async def _ingest_fred_data(self, lookback_days: int) -> BootstrapStepResult:
        """FRED 데이터 수집 단계 (내부 서비스 직접 호출)
        
        Args:
            lookback_days: FRED 데이터 수집 기간 (일)
            
        Returns:
            BootstrapStepResult: 단계 실행 결과
        """
        logger.info(f"📊 2단계: FRED 매크로 데이터 수집 시작 (최근 {lookback_days}일)")
        start_time = time.time()
        
        try:
            # lookback_days를 since 날짜로 변환
            since_date = date.today() - timedelta(days=lookback_days)
            
            # FredSyncService 직접 호출
            fred_service = FredSyncService(self.db)
            macro_repo = MacroRepository(self.db)
            
            # 모든 활성 시리즈 ID 가져오기
            series_ids = [s.fred_series_id for s in macro_repo.get_active_series()]
            
            # bulk_sync_since 메서드로 데이터 수집
            total_series, total_rows = fred_service.bulk_sync_since(series_ids, since_date)
            
            duration = time.time() - start_time
            
            result_summary = {
                "status": "completed",
                "since_date": since_date.isoformat(),
                "series_count": total_series,
                "rows_upserted": total_rows
            }
            
            logger.info(f"✅ 2단계 완료 - FRED 데이터 수집: {total_series}개 시리즈, {total_rows}개 행")
            
            return BootstrapStepResult(
                step_name="fred_ingest",
                step_description="FRED 매크로 데이터 수집",
                success=True,
                duration_seconds=duration,
                result_summary=result_summary,
                error_message=None
            )
            
        except Exception as e:
            duration = time.time() - start_time
            error_msg = str(e)
            logger.error(f"❌ 2단계 실패 - FRED 데이터 수집 오류: {error_msg}")
            
            return BootstrapStepResult(
                step_name="fred_ingest",
                step_description="FRED 매크로 데이터 수집",
                success=False,
                duration_seconds=duration,
                result_summary=None,
                error_message=error_msg
            )
    
    async def _ingest_yahoo_data(self, period: str) -> BootstrapStepResult:
        """Yahoo Finance 데이터 수집 단계 (내부 서비스 직접 호출)
        
        Args:
            period: Yahoo Finance 데이터 수집 기간
            
        Returns:
            BootstrapStepResult: 단계 실행 결과
        """
        logger.info(f"📈 3단계: Yahoo Finance 데이터 수집 시작 (기간: {period})")
        start_time = time.time()
        
        try:
            # YahooIndexService 직접 호출
            yahoo_service = YahooIndexService(self.db)
            yahoo_request = YahooIndexIngestRequest(period=period)
            
            response = await yahoo_service.ingest_data(yahoo_request)
            
            duration = time.time() - start_time
            
            result_summary = {
                "status": "completed",
                "period": period,
                "total_symbols": response.total_symbols,
                "successful_symbols": response.successful_symbols,
                "failed_symbols": response.failed_symbols
            }
            
            logger.info(f"✅ 3단계 완료 - Yahoo Finance 데이터 수집: {response.successful_symbols}/{response.total_symbols}개 심볼")
            
            return BootstrapStepResult(
                step_name="yahoo_ingest",
                step_description="Yahoo Finance 데이터 수집",
                success=response.success,
                duration_seconds=duration,
                result_summary=result_summary,
                error_message=None if response.success else f"일부 심볼 수집 실패: {response.failed_symbols}개"
            )
            
        except Exception as e:
            duration = time.time() - start_time
            error_msg = str(e)
            logger.error(f"❌ 3단계 실패 - Yahoo Finance 데이터 수집 오류: {error_msg}")
            
            return BootstrapStepResult(
                step_name="yahoo_ingest",
                step_description="Yahoo Finance 데이터 수집",
                success=False,
                duration_seconds=duration,
                result_summary=None,
                error_message=error_msg
            )
    
    async def _refresh_premarket_risk(self) -> BootstrapStepResult:
        """프리마켓 리스크 스냅샷 갱신 단계 (내부 서비스 직접 호출)

        KR/US 스냅샷만 기본 갱신한다. GLOBAL은 표본 부족 시 HeadlineRiskService 내부 블렌딩으로 보완한다.
        이렇게 하면 bootstrap 1회당 LLM 호출 수를 최소 2회로 제한할 수 있다.
        """
        logger.info("🛡️ 4단계: 프리마켓 리스크 스냅샷 갱신 시작 (KR/US)")
        start_time = time.time()

        try:
            from app.features.premarket.services.headline_risk_service import HeadlineRiskService

            risk_service = HeadlineRiskService(self.db)
            scopes = ["KR", "US"]
            refreshed = []
            failures = []

            for scope in scopes:
                try:
                    out = risk_service.refresh_snapshot(scope=scope, window_minutes=720)
                    refreshed.append({
                        "scope": scope,
                        "snapshot_id": out.get("snapshot_id"),
                        "headline_count_primary": out.get("headline_count_primary"),
                        "headline_count_used": out.get("headline_count_used"),
                        "blend_applied": out.get("blend_applied"),
                        "discount_multiplier": out.get("discount_multiplier"),
                        "sell_markup_multiplier": out.get("sell_markup_multiplier"),
                        "regime_score": out.get("regime_score"),
                    })
                except Exception as scope_error:
                    logger.warning("[BOOTSTRAP] risk refresh failed for %s: %s", scope, scope_error)
                    failures.append({"scope": scope, "error": str(scope_error)})

            duration = time.time() - start_time
            success = len(failures) == 0
            result_summary = {
                "status": "completed" if success else "partial_failure",
                "scopes_requested": scopes,
                "refreshed_count": len(refreshed),
                "failed_count": len(failures),
                "refreshed": refreshed,
                "failures": failures,
            }

            logger.info("✅ 4단계 완료 - 프리마켓 리스크 스냅샷 갱신: 성공=%s 실패=%s", len(refreshed), len(failures))
            return BootstrapStepResult(
                step_name="risk_refresh",
                step_description="프리마켓 리스크 스냅샷 갱신",
                success=success,
                duration_seconds=duration,
                result_summary=result_summary,
                error_message=None if success else f"risk refresh partial failure: {len(failures)} scope(s) failed"
            )
        except Exception as e:
            duration = time.time() - start_time
            error_msg = str(e)
            logger.error(f"❌ 4단계 실패 - 프리마켓 리스크 스냅샷 갱신 오류: {error_msg}")

            return BootstrapStepResult(
                step_name="risk_refresh",
                step_description="프리마켓 리스크 스냅샷 갱신",
                success=False,
                duration_seconds=duration,
                result_summary=None,
                error_message=error_msg
            )

    async def _update_signals(self) -> BootstrapStepResult:
        """시그널 갱신 단계 (v2 사용, 내부 서비스 직접 호출)
        
        Returns:
            BootstrapStepResult: 단계 실행 결과
        """
        logger.info("🎯 5단계: 프리마켓 시그널 갱신 시작 (V2)")
        logger.info("📌 V2 특징: breadth 축소(0.25) + β gating + TopK(20)")
        start_time = time.time()
        
        try:
            # PMSignalServiceV2 직접 호출 (v2 개선 버전)
            from app.features.premarket.services.pm_signal_service_v2 import PMSignalServiceV2
            signal_service = PMSignalServiceV2(self.db)
            
            logger.info("[BOOTSTRAP] Calling PMSignalServiceV2.update_signals_v2()")
            response = signal_service.update_signals_v2(
                tickers=None,      # None이면 전체 티커 처리
                country=None,      # None이면 전체 (US + KR)
                dry_run=False,     # 실제 저장
                anchor_date=None   # None이면 오늘
            )
            
            duration = time.time() - start_time
            logger.info(f"[BOOTSTRAP] V2 response type: {type(response)}")
            
            results = response.get('results') if isinstance(response, dict) else None
            if not results:
                # UpdatePMSignalsResponse 객체인 경우
                results = response.results if hasattr(response, 'results') else response.get('results')
            result_summary = {
                "status": "completed",
                "total": results.get("total", 0) if isinstance(results, dict) else 0,
                "success": results.get("success", 0) if isinstance(results, dict) else 0,
                "failed": results.get("failed", 0) if isinstance(results, dict) else 0,
                "elapsed_seconds": response.elapsed_seconds if hasattr(response, 'elapsed_seconds') else duration
            }
            
            logger.info(f"✅ 4단계 완료 - 프리마켓 시그널 갱신 (V2): {result_summary['success']}/{result_summary['total']}개 성공")
            
            return BootstrapStepResult(
                step_name="signal_update",
                step_description="프리마켓 시그널 갱신",
                success=response.success,
                duration_seconds=duration,
                result_summary=result_summary,
                error_message=None
            )
            
        except Exception as e:
            duration = time.time() - start_time
            error_msg = str(e)
            logger.error(f"❌ 4단계 실패 - 프리마켓 시그널 갱신 오류: {error_msg}")
            
            return BootstrapStepResult(
                step_name="signal_update",
                step_description="프리마켓 시그널 갱신",
                success=False,
                duration_seconds=duration,
                result_summary=None,
                error_message=error_msg
            )

