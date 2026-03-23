# app/features/earnings/controllers/earnings_controller.py

from fastapi import APIRouter, Depends, Query, HTTPException
from typing import List, Optional
from sqlalchemy.orm import Session
from app.core.db import get_db
from app.features.earnings.services.earnings_service import EarningsService
from app.features.earnings.services.naver_earnings_service import NaverEarningsService

router = APIRouter(prefix="/earnings", tags=["earnings"])

@router.post(
    "/sync",
    summary="어닝 데이터 동기화 [미국주식] ★★★",
    description="NMS, NYQ 거래소 티커들의 어닝 데이터를 수집합니다. [미국주식] ticker_ids를 지정하면 해당 티커들만, 지정하지 않으면 전체 티커를 대상으로 합니다. 현재일 기준 1년전부터 3개월 후까지의 데이터를 수집하며, 캘린더와 스톡 API를 통해 예상치와 실제치를 모두 수집합니다.",
    response_description="동기화된 어닝 데이터 범위와 상태 정보를 반환합니다."
)
def sync_earnings(
    ticker_ids: Optional[List[int]] = Query(None, description="동기화할 티커 ID 배열 (선택사항, 미지정시 전체 티커 대상)"),
    db: Session = Depends(get_db)
):
    """
    Earnings sync endpoint
    - ticker_ids가 있으면: 지정된 티커들만 대상
    - ticker_ids가 없으면: NMS, NYQ 거래소 전체 티커 대상
    - 각 티커별로 calendar API와 stock API 호출
    - 현재일 기준 1년전 ~ 3개월 후 범위
    """
    service = EarningsService(db)
    result = service.sync(ticker_ids)
    return result

@router.get(
    "/analyst/{ticker_id}",
    summary="애널리스트 AI용 어닝 정보 조회 [미국주식]",
    description="특정 티커의 최신 발표된 분기와 다음 예정된 분기의 어닝 정보를 애널리스트 AI 프롬프트 형식으로 반환합니다. [미국주식]",
    response_description="티커의 최신 및 예정 어닝 정보를 구조화된 형태로 반환합니다."
)
def get_earnings_for_analyst(
    ticker_id: int,
    db: Session = Depends(get_db)
):
    """
    애널리스트 AI용 어닝 정보 조회 엔드포인트
    - 티커 ID로 최신 발표된 분기 정보 조회
    - 다음 예정된 분기 정보 조회
    - AI 프롬프트에 적합한 구조로 포맷팅
    """
    service = EarningsService(db)
    earnings_data = service.get_earnings_for_analyst(ticker_id)
    
    if not earnings_data:
        raise HTTPException(status_code=404, detail=f"티커 ID {ticker_id}에 대한 어닝 정보를 찾을 수 없습니다.")
    
    return {"earnings": earnings_data}

@router.post(
    "/naver/sync",
    summary="국내주식 전체 실적 데이터 동기화 [국내주식]",
    description="KOE 거래소의 모든 국내주식에 대해 네이버 증권에서 실적/예상 데이터를 수집하여 동기화합니다. [국내주식] 각 티커별로 개별 처리하며, 오류가 발생해도 다음 티커로 계속 진행합니다.",
    response_description="동기화된 국내주식 실적 데이터 통계 정보를 반환합니다."
)
def sync_all_korean_earnings(
    db: Session = Depends(get_db)
):
    """
    국내주식 전체 실적 데이터 동기화 엔드포인트
    - KOE 거래소의 모든 국내주식 대상
    - 네이버 증권 API를 통한 실적/예상 데이터 수집
    - 개별 티커별 처리로 오류 격리
    - 상세한 처리 결과 로그 제공
    """
    try:
        service = NaverEarningsService(db)
        result = service.sync_all_korean_earnings()
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"국내주식 실적 데이터 동기화 중 오류가 발생했습니다: {str(e)}")

@router.post(
    "/naver/schedule",
    summary="실적발표일자 크롤링 및 통합 [국내주식]",
    description="KIND 증권거래소에서 실적발표 예정일을 크롤링하여 earnings_event 테이블에 통합합니다. [국내주식] '실적발표'와 '실적 발표' 키워드로 검색하여 현재부터 3개월 후까지의 발표일정을 수집합니다.",
    response_description="크롤링된 실적발표일자 통계 정보를 반환합니다."
)
async def sync_earnings_schedule(
    db: Session = Depends(get_db)
):
    """
    실적발표일자 크롤링 및 통합 엔드포인트
    - KIND 증권거래소 IR 게시판 크롤링
    - '실적발표', '실적 발표' 키워드로 이중 검색
    - 오늘부터 3개월 후까지의 발표일정 수집
    - ticker 테이블에 존재하는 종목만 처리
    - 가장 가까운 scheduled 항목에 발표일자 업데이트
    """
    try:
        service = NaverEarningsService(db)
        result = await service.sync_earnings_schedule()
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"실적발표일자 크롤링 중 오류가 발생했습니다: {str(e)}")

@router.post(
    "/naver/sync-all",
    summary="국내주식 실적 데이터 통합 동기화 [국내주식] ★★★",
    description="국내주식의 실적 데이터 동기화와 실적발표일자 크롤링을 한번에 실행합니다. [국내주식] 1단계: KOE 거래소 전체 네이버 실적 데이터 동기화, 2단계: KIND 실적발표일자 크롤링 및 통합을 순차적으로 진행합니다.",
    response_description="통합 동기화 결과 요약 정보를 반환합니다."
)
async def sync_all_korean_earnings_with_schedule(
    db: Session = Depends(get_db)
):
    """
    국내주식 실적 데이터 통합 동기화 엔드포인트
    - Step 1: KOE 거래소 전체 네이버 실적 데이터 동기화
    - Step 2: KIND 실적발표일자 크롤링 및 통합 (가장 가까운 scheduled 항목 업데이트)
    - 순차 실행으로 데이터 일관성 보장
    - 통합 결과 요약 제공
    """
    try:
        service = NaverEarningsService(db)
        result = await service.sync_all_korean_earnings_with_schedule()
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"국내주식 실적 데이터 통합 동기화 중 오류가 발생했습니다: {str(e)}")

@router.post(
    "/naver/{ticker_id}",
    summary="네이버 실적 데이터 동기화 [국내주식]",
    description="네이버 증권에서 특정 티커의 실적/예상 데이터를 수집하여 동기화합니다. [국내주식] YYMM 형식의 분기별 데이터를 파싱하여 실적(A)과 예상(E) 데이터를 구분하여 저장합니다.",
    response_description="동기화된 실적 데이터 정보를 반환합니다."
)
def sync_naver_earnings(
    ticker_id: int,
    db: Session = Depends(get_db)
):
    """
    네이버 실적 데이터 동기화 엔드포인트
    - 티커 ID로 해당 종목의 네이버 실적/예상 데이터 수집
    - YYMM 형식 데이터를 fiscal_year/fiscal_quarter로 변환
    - (A) 실적 데이터: actual_eps, actual_revenue, period_end_date 설정
    - (E) 예상 데이터: estimate_eps, estimate_revenue 설정
    - 서프라이즈 EPS 자동 계산
    """
    try:
        service = NaverEarningsService(db)
        result = service.sync_earnings_by_ticker_id(ticker_id)
        return result
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"네이버 실적 데이터 동기화 중 오류가 발생했습니다: {str(e)}")
