# app/features/trading_hybrid/repositories/recommendation_repository.py
from typing import List, Dict, Any
from sqlalchemy.orm import Session
from sqlalchemy import text
from datetime import datetime, timedelta
from app.features.recommendation.models.analyst_recommendation import AnalystRecommendation, PositionType
from app.shared.models.ticker import Ticker
from app.features.signals.services.signal_detection_service import SignalDetectionService
from app.features.signals.models.similarity_models import SimilaritySearchRequest
from app.features.signals.models.signal_models import AlgorithmVersion
from app.features.marketdata.services.technical_indicators import calculate_technical_indicators
from app.features.marketdata.models.ohlcv_daily import OhlcvDaily
import pandas as pd
import logging

logger = logging.getLogger(__name__)


def load_swing_recommendations(db: Session, market: str) -> List[Dict[str, Any]]:
    """
    스윙 추천 종목 로드 (패턴 유사도 기반 필터링)
    
    로직:
    1. AnalystRecommendation + SimilarityAnalysis JOIN 쿼리
    2. 필터 조건:
       - 추천서: 7일 이내, is_latest=true, position_type=LONG, valid_until > now
       - 패턴: top_similarity > 0.7, p_up > 0.8
    3. 정렬: p_up DESC, top_similarity DESC
    4. ATR%, 유동성, 뉴스 히트, 현재가 계산
    
    Args:
        db: DB 세션
        market: "KR" 또는 "US"
    
    Returns:
        패턴 유사도 필터 통과한 추천 종목 리스트 (패턴 정보 포함)
    """
    from app.features.signals.models.similarity_analysis import SimilarityAnalysis
    
    country = "KR" if market == "KR" else "US"
    cutoff = datetime.now() - timedelta(days=3)
    
    # 시장별 거래소 필터
    if market == "KR":
        exchange_filter = ["KOE"]
    else:
        exchange_filter = ["NYQ", "NMS"]

    try:
        # similarity_analysis + analyst_recommendation JOIN 쿼리
        recs = (
            db.query(AnalystRecommendation, Ticker, SimilarityAnalysis)
              .join(Ticker, AnalystRecommendation.ticker_id == Ticker.id)
              .join(SimilarityAnalysis, SimilarityAnalysis.ticker_id == AnalystRecommendation.ticker_id)
              .filter(
                  Ticker.country == country,
                  SimilarityAnalysis.exchange.in_(exchange_filter),
                  AnalystRecommendation.recommended_at >= cutoff,
                  AnalystRecommendation.valid_until > datetime.now(),
                  AnalystRecommendation.position_type == PositionType.LONG,
                  AnalystRecommendation.is_latest == True,
                  SimilarityAnalysis.top_similarity > 0.7,
                  SimilarityAnalysis.p_up > 0.8
              )
              .order_by(
                  SimilarityAnalysis.p_up.desc(),
                  SimilarityAnalysis.top_similarity.desc()
              )
              .all()
        )
        
        logger.info(f"🔍 패턴+추천 JOIN 쿼리: {len(recs)}개 조회 (top_sim>0.7, p_up>0.8)")
        
    except Exception as e:
        logger.error(f"❌ 추천+패턴 JOIN 쿼리 실패: {e}", exc_info=True)
        return []

    out: List[Dict[str, Any]] = []

    for rec, ticker, sim in recs:
        symbol = ticker.symbol
        ticker_id = ticker.id
        
        try:
            logger.debug(f"🔍 [{symbol}] (ticker_id={ticker_id}) 처리 시작")
            
            # ATR 계산
            logger.debug(f"  📊 [{symbol}] ATR 계산 중...")
            atr_pct = _calculate_atr_pct(db, ticker_id)
            logger.debug(f"    ATR={atr_pct:.2%}")
            
            # 실시간 현재가 조회
            logger.debug(f"  💵 [{symbol}] 현재가 조회 중...")
            current_price = _get_current_price(db, ticker, market, float(rec.entry_price or 0))
            logger.debug(f"    current_price={current_price:.2f}")
            
            # 유동성 백분위수
            logger.debug(f"  💧 [{symbol}] 유동성 계산 중...")
            liquidity_rank = _calculate_liquidity_percentile(db, ticker_id)
            logger.debug(f"    liquidity={liquidity_rank:.1f}%")
            
            # 뉴스 히트
            logger.debug(f"  📰 [{symbol}] 뉴스 히트 계산 중...")
            news_heat = _calculate_news_heat(db, ticker_id)
            logger.debug(f"    news_heat={news_heat:.2f}")
            
            out.append({
                "recommendation_id": rec.id,
                "ticker_id": rec.ticker_id,
                "symbol": ticker.symbol,
                "exchange": ticker.exchange,
                "action": (rec.position_type.value if getattr(rec, "position_type", None) else "LONG"),
                "entry_price": float(rec.entry_price or 0),
                "target_price": float(rec.target_price or 0),
                "stop_loss_price": float(rec.stop_price or 0),
                "confidence_score": float(rec.confidence_score or 0.6),
                "recommended_at": rec.recommended_at,
                "valid_until": rec.valid_until,
                "current_price": current_price,
                "atr_pct": float(atr_pct),
                "liquidity_rank_pct": liquidity_rank,
                "news_heat": news_heat,
                # 패턴 정보 추가
                "pattern_p_up": float(sim.p_up),
                "pattern_top_sim": float(sim.top_similarity),
                "pattern_exp_up": float(sim.exp_up),
                "pattern_exp_down": float(sim.exp_down),
            })
            
            logger.info(f"  ✅ [{symbol}] 추가 (p_up={sim.p_up:.2%}, sim={sim.top_similarity:.2f})")
            
        except Exception as e:
            logger.error(f"❌ [{symbol}] (ticker_id={ticker_id}) 처리 중 예외 발생: {str(e)}", exc_info=True)
            continue
    
    logger.info(f"✅ 패턴+추천 통합 로드 완료: {len(out)}개 (p_up↓, sim↓ 정렬)")
    return out


def _get_current_price(db: Session, ticker: Ticker, market: str, fallback_price: float) -> float:
    """
    실시간 현재가 조회 (KIS API)
    
    Args:
        db: DB 세션
        ticker: 티커 객체
        market: "KR" 또는 "US"
        fallback_price: API 실패 시 대체 가격
    
    Returns:
        현재가 (float)
    """
    try:
        from app.core.kis_client import KISClient
        kis = KISClient(db)
        
        if market == "KR":
            price_data = kis.kr_current_price(ticker.symbol)
            output = price_data.get("output", {})
            current_price = float(output.get("stck_prpr", fallback_price))
        else:
            price_data = kis.price_detail(ticker.symbol, ticker.exchange)
            output = price_data.get("output", {})
            current_price = float(output.get("last", fallback_price))
        
        logger.debug(f"현재가 조회 성공: {ticker.symbol} = {current_price}")
        return current_price
        
    except Exception as e:
        logger.warning(f"현재가 조회 실패 (fallback 사용): {ticker.symbol} - {str(e)}")
        return fallback_price


def _calculate_liquidity_percentile(db: Session, ticker_id: int) -> float:
    """
    유동성 백분위수 계산 (최근 30일 평균 거래대금 기준)
    
    Args:
        db: DB 세션
        ticker_id: 티커 ID
    
    Returns:
        백분위수 (0~100, 기본값: 50.0)
    """
    try:
        result = db.execute(text("""
            WITH vol_rank AS (
                SELECT ticker_id,
                       AVG(volume * close) as avg_turnover_30d,
                       PERCENT_RANK() OVER (ORDER BY AVG(volume * close)) * 100 as rank_pct
                FROM trading.ohlcv_daily
                WHERE is_final = true
                  AND trade_date >= CURRENT_DATE - INTERVAL '30 days'
                GROUP BY ticker_id
            )
            SELECT rank_pct FROM vol_rank WHERE ticker_id = :ticker_id
        """), {"ticker_id": ticker_id}).fetchone()
        
        if result and result[0] is not None:
            return float(result[0])
        return 50.0
        
    except Exception as e:
        logger.debug(f"유동성 백분위수 계산 실패 (기본값 사용): ticker_id={ticker_id} - {str(e)}")
        return 50.0


def _calculate_news_heat(db: Session, ticker_id: int) -> float:
    """
    뉴스 히트 점수 계산 (최근 7일 일평균 뉴스 언급 빈도)
    
    Args:
        db: DB 세션
        ticker_id: 티커 ID
    
    Returns:
        일평균 뉴스 언급 횟수 (0~N, 기본값: 0.0)
    """
    try:
        result = db.execute(text("""
            SELECT COUNT(DISTINCT n.id)::float / 7.0 as daily_avg
            FROM trading.news n
            JOIN trading.news_ticker nt ON n.id = nt.news_id
            WHERE nt.ticker_id = :ticker_id
              AND n.published_date_kst >= CURRENT_DATE - INTERVAL '7 days'
              AND nt.confidence >= 0.8
        """), {"ticker_id": ticker_id}).fetchone()
        
        if result and result[0] is not None:
            return float(result[0])
        return 0.0
        
    except Exception as e:
        logger.debug(f"뉴스 히트 계산 실패 (기본값 사용): ticker_id={ticker_id} - {str(e)}")
        return 0.0


def _calculate_atr_pct(db: Session, ticker_id: int, period: int = 14) -> float:
    """
    ATR% 계산 (ohlcv_daily.is_final=true 사용)
    
    로직:
    1. OhlcvDaily에서 is_final=true, 최근 15개 조회
    2. pandas DataFrame 변환
    3. calculate_technical_indicators() 호출 → ATR(14) 계산
    4. atr_percentage를 소수점으로 변환 (5.0% → 0.05)
    
    Args:
        db: DB 세션
        ticker_id: 티커 ID
        period: ATR 계산 기간 (기본값: 14)
    
    Returns:
        ATR% (소수점, 예: 0.05)
    """
    rows = (db.query(OhlcvDaily)
              .filter(OhlcvDaily.ticker_id == ticker_id, OhlcvDaily.is_final == True)
              .order_by(OhlcvDaily.trade_date.desc())
              .limit(period + 1).all())
    if len(rows) < period:
        return 0.05
    df = pd.DataFrame([{
        "trade_date": r.trade_date, "open": float(r.open or 0),
        "high": float(r.high or 0), "low": float(r.low or 0),
        "close": float(r.close or 0), "volume": float(r.volume or 0)
    } for r in reversed(rows)])
    res = calculate_technical_indicators(df, min_periods=period)
    try:
        return float(res["indicators"]["atr"]["atr_percentage"]) / 100.0
    except Exception:
        return 0.05
