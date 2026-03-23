from typing import Dict, Any, List
from sqlalchemy.orm import Session
from datetime import datetime, timezone, timedelta
import json
import logging

from app.core.gpt_client import responses_json
from app.features.portfolio.services.kr_sell_order_prompt_service import KrSellOrderPromptService
from app.features.portfolio.services.us_portfolio_service import PortfolioService
from app.features.portfolio.models.trading_models import OrderBatch, OrderPlan, OrderLeg
from app.features.marketdata.services.kr_market_holiday_service import KRMarketHolidayService
from app.core import config as settings
from app.features.portfolio.schemas.order_schemas import ORDER_BATCH_SCHEMA
from app.shared.models.ticker import Ticker

class KrSellOrderAIService:
    """국내주식 매도주문 AI 생성 서비스"""
    
    def __init__(self, db: Session):
        self.db = db
        self.prompt_service = KrSellOrderPromptService(db)
        self.portfolio_service = PortfolioService(db)
        self.market_holiday_service = KRMarketHolidayService(db)
    
    def _get_symbol_by_ticker_id(self, ticker_id: int) -> str:
        """티커ID로 symbol을 조회합니다."""
        ticker = self.db.query(Ticker).filter(Ticker.id == ticker_id).first()
        if not ticker:
            raise ValueError(f"티커를 찾을 수 없습니다: ticker_id={ticker_id}")
        return ticker.symbol
    
    def generate_sell_order_batch(self) -> Dict[str, Any]:
        """
        국내주식 매도주문 배치를 AI로 생성하고 즉시 실행합니다.
        
        Returns:
            Dict[str, Any]: 생성된 주문 배치 정보 및 실행 결과
        """
        logger = logging.getLogger(__name__)
        logger.info("=== 국내주식 매도주문 AI 배치 생성 시작 ===")
        
        try:
            # 0. 휴장 여부 확인
            logger.info("0️⃣ 휴장 여부 확인 중...")
            is_market_closed = self.market_holiday_service.is_market_closed_now()
            
            if is_market_closed:
                logger.warning("🚫 현재 휴장 중입니다. 매도주문 배치 생성을 중단합니다.")
                return {
                    "batch_id": None,
                    "asof_kst": datetime.now(timezone(timedelta(hours=9))).isoformat(),
                    "mode": "SELL",
                    "currency": "KRW",
                    "available_cash": 0.0,
                    "notes": "휴장으로 인한 주문 중단",
                    "plans_count": 0,
                    "skipped_count": 0,
                    "executed_orders": [],
                    "is_market_closed": True,
                    "generated_at": datetime.now(timezone.utc).isoformat(),
                    "message": "현재 휴장 중이므로 매도주문 배치가 생성되지 않았습니다."
                }
            
            logger.info("✅ 시장 개장 확인 - 매도주문 배치 생성을 진행합니다.")
            
            # 1. 자산 스냅샷 강제 갱신 (KR)
            logger.info("1️⃣ 자산 스냅샷 강제 갱신 중 (KR)...")
            try:
                # AssetSnapshotService를 통해 국내 자산 스냅샷 수집
                from app.features.portfolio.services.asset_snapshot_service import AssetSnapshotService
                asset_snapshot_service = AssetSnapshotService(self.db)
                
                # 국내 계좌 스냅샷 수집
                kr_result = asset_snapshot_service.collect_kr_account_snapshot()
                
                if "snapshot_id" in kr_result:
                    logger.info(f"✅ 국내 자산 스냅샷 갱신 성공 (스냅샷 ID: {kr_result['snapshot_id']})")
                else:
                    logger.warning(f"⚠️ 국내 자산 스냅샷 갱신 실패: {kr_result}")
                    
            except Exception as e:
                logger.error(f"❌ 국내 자산 스냅샷 갱신 중 오류: {str(e)}")
                # 갱신 실패해도 기존 데이터로 진행
            
            # 2. 프롬프트 생성
            logger.info("2️⃣ 프롬프트 생성 중...")
            prompt_text = self.prompt_service.generate_sell_order_prompt()
            logger.info(f"✅ 프롬프트 생성 완료 (길이: {len(prompt_text)} 문자)")
            
            # 3. GPT 호출
            logger.info("3️⃣ GPT AI 호출 중...")
            gpt_response = self._call_sell_order_ai(prompt_text)
            logger.info(f"✅ GPT 응답 수신 완료")
            
            # 4. 데이터베이스 저장
            logger.info("4️⃣ 데이터베이스 저장 중...")
            batch_id = self._save_order_batch(gpt_response)
            logger.info(f"✅ 주문 배치 저장 완료 (배치 ID: {batch_id})")
            
            # 5. 주문 즉시 실행
            logger.info("5️⃣ 실제 주문 실행 중...")
            executed_orders = self._execute_order_batch(batch_id)
            
            # 성공/실패 통계 계산
            success_count = sum(1 for order in executed_orders if order.get("status") == "SUCCESS")
            failed_count = sum(1 for order in executed_orders if order.get("status") in ["FAILED", "ERROR"])
            
            logger.info(f"🎯 주문 실행 완료: 성공 {success_count}개, 실패 {failed_count}개")
            
            # 6. 결과 반환
            result = {
                "batch_id": batch_id,
                "asof_kst": datetime.now(timezone(timedelta(hours=9))).isoformat(),
                "mode": "SELL",
                "currency": "KRW",
                "available_cash": gpt_response.get("batch", {}).get("available_cash", 0.0),
                "notes": gpt_response.get("batch", {}).get("notes", ""),
                "plans_count": len(gpt_response.get("plans", [])),
                "skipped_count": len(gpt_response.get("skipped", [])),
                "executed_orders": executed_orders,
                "is_market_closed": False,
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "message": f"국내주식 매도주문 배치 생성 및 실행 완료: 성공 {success_count}개, 실패 {failed_count}개"
            }
            
            logger.info("🎉 국내주식 매도주문 AI 배치 생성 및 실행 완료")
            return result
            
        except Exception as e:
            logger.error(f"❌ 국내주식 매도주문 배치 생성 중 오류: {str(e)}")
            raise
    
    def _call_sell_order_ai(self, prompt_text: str) -> Dict[str, Any]:
        """GPT AI를 호출하여 국내주식 매도주문 배치를 생성합니다."""
        logger = logging.getLogger(__name__)
        
        try:
            logger.info("🤖 GPT AI 호출 시작")
            response = responses_json(
                model=settings.MODEL_BUY_ORDER_AI,  # 매도주문도 동일한 모델 사용
                schema_name="kr_sell_order_batch",
                schema=ORDER_BATCH_SCHEMA,
                user_text=prompt_text,
                temperature=0.0,
                task="kr_sell_order_batch"
            )
            
            logger.info("✅ GPT AI 응답 수신 성공")
            return response
            
        except Exception as e:
            logger.error(f"❌ GPT AI 호출 실패: {str(e)}")
            raise
    
    def _save_order_batch(self, gpt_response: Dict[str, Any]) -> int:
        """GPT 응답을 데이터베이스에 저장합니다."""
        logger = logging.getLogger(__name__)
        
        try:
            # 1. 배치 헤더 저장
            logger.info("📦 배치 헤더 저장 중...")
            batch_data = gpt_response["batch"]
            
            # KST 시간 파싱
            asof_kst = datetime.fromisoformat(batch_data["asof_kst"].replace('Z', '+00:00'))
            
            batch = OrderBatch(
                asof_kst=asof_kst,
                mode=batch_data["mode"],
                currency=batch_data["currency"],
                available_cash=batch_data["available_cash"],
                notes=batch_data.get("notes", "")
            )
            self.db.add(batch)
            self.db.flush()  # ID 획득을 위해 flush
            batch_id = batch.id
            logger.info(f"✅ 배치 헤더 저장 완료 (ID: {batch_id})")
            
            # 2. 실행 계획 저장
            plans_data = gpt_response["plans"]
            logger.info(f"📋 실행 계획 저장 중... ({len(plans_data)}개)")
            
            for i, plan_data in enumerate(plans_data):
                # 티커ID로 symbol 조회
                symbol = self._get_symbol_by_ticker_id(plan_data["ticker_id"])
                logger.info(f"  계획 {i+1}/{len(plans_data)}: {symbol} {plan_data['action']}")
                
                # 계획 저장
                plan = OrderPlan(
                    batch_id=batch_id,
                    ticker_id=plan_data["ticker_id"],
                    symbol=symbol,
                    action=plan_data["action"],
                    recommendation_id=plan_data.get("reference", {}).get("recommendation_id"),
                    note=plan_data["note"],
                    decision="EXECUTE"
                )
                self.db.add(plan)
                self.db.flush()  # ID 획득을 위해 flush
                plan_id = plan.id
                
                # 레그 저장
                legs_data = plan_data["legs"]
                logger.info(f"    레그 저장 중... ({len(legs_data)}개)")
                
                for j, leg_data in enumerate(legs_data):
                    logger.info(f"      레그 {j+1}: {leg_data['type']} {leg_data['side']} {leg_data['quantity']}주")
                    
                    leg = OrderLeg(
                        plan_id=plan_id,
                        type=leg_data["type"],
                        side=leg_data["side"],
                        quantity=leg_data["quantity"],
                        limit_price=leg_data.get("limit_price")
                    )
                    self.db.add(leg)
            
            # 3. 제외된 계획 저장
            skipped_data = gpt_response["skipped"]
            logger.info(f"⏭️ 제외된 계획 저장 중... ({len(skipped_data)}개)")
            
            for i, skip_data in enumerate(skipped_data):
                # 티커ID로 symbol 조회
                symbol = self._get_symbol_by_ticker_id(skip_data["ticker_id"])
                logger.info(f"  제외 {i+1}/{len(skipped_data)}: {symbol} ({skip_data['code']})")
                
                skip_plan = OrderPlan(
                    batch_id=batch_id,
                    ticker_id=skip_data["ticker_id"],
                    symbol=symbol,
                    action="SELL",  # 매도 배치이므로 SELL로 고정
                    note=skip_data["note"],
                    decision="SKIP",
                    skip_code=skip_data["code"],
                    skip_note=skip_data["note"]
                )
                self.db.add(skip_plan)
            
            # 4. 최종 커밋
            logger.info("💾 최종 커밋 중...")
            self.db.commit()
            logger.info(f"🎉 주문 배치 저장 완료 (배치 ID: {batch_id})")
            
            return batch_id
            
        except Exception as e:
            logger.error(f"❌ 주문 배치 저장 실패: {str(e)}")
            self.db.rollback()
            raise
    
    def _execute_order_batch(self, batch_id: int) -> List[Dict[str, Any]]:
        """
        저장된 주문 배치를 실제로 실행합니다.
        
        Args:
            batch_id: 실행할 배치 ID
            
        Returns:
            List[Dict[str, Any]]: 실행된 주문들의 결과 목록
        """
        from app.features.portfolio.services.kr_order_service import KrOrderService
        from app.shared.models.ticker import Ticker
        
        logger = logging.getLogger(__name__)
        kr_order_service = KrOrderService(self.db)
        executed_orders = []
        
        # 배치에서 EXECUTE 결정인 계획들만 조회
        execute_plans = (
            self.db.query(OrderPlan)
            .filter(
                OrderPlan.batch_id == batch_id,
                OrderPlan.decision == "EXECUTE"
            )
            .all()
        )
        
        logger.info(f"📋 실행할 계획 수: {len(execute_plans)}")
        
        for plan in execute_plans:
            try:
                logger.info(f"🚀 계획 실행 중: {plan.symbol} (Plan ID: {plan.id})")
                
                # 계획의 모든 레그 실행
                for leg in plan.legs:
                    try:
                        # OrderLeg → 국내주식 주문 파라미터 변환
                        order_params = self._convert_leg_to_order_params(leg, plan)
                        
                        # 실제 주문 실행
                        logger.info(f"📤 주문 실행: {leg.side} {leg.quantity} {plan.symbol} @ {leg.limit_price or 'MARKET'} ({leg.type})")
                        logger.debug(f"🔍 주문 파라미터: {order_params}")
                        
                        order_result = kr_order_service.execute_order(
                            symbol=order_params["symbol"],
                            side=order_params["side"],
                            quantity=order_params["quantity"],
                            price=order_params["price"],
                            order_type=order_params["order_type"],
                            leg_id=leg.id
                        )
                        
                        # 실행 결과 기록
                        execution_result = {
                            "plan_id": plan.id,
                            "leg_id": leg.id,
                            "symbol": plan.symbol,
                            "side": leg.side,
                            "quantity": leg.quantity,
                            "order_method": leg.type,
                            "limit_price": float(leg.limit_price) if leg.limit_price else None,
                            "kr_result": order_result,
                            "status": "SUCCESS" if order_result.get("success") else "FAILED",
                            "order_number": order_result.get("order_id", ""),
                            "message": order_result.get("message", "")
                        }
                        
                        executed_orders.append(execution_result)
                        
                        if execution_result["status"] == "SUCCESS":
                            logger.info(f"✅ 주문 성공: {plan.symbol} 레그 {leg.id} (주문번호: {execution_result['order_number']})")
                        else:
                            logger.error(f"❌ 주문 실패: {plan.symbol} 레그 {leg.id}")
                            logger.error(f"   📋 파라미터: {order_params}")
                            logger.error(f"   🚫 오류: {execution_result['message']}")
                            logger.error(f"   📄 전체 응답: {order_result}")
                            
                    except Exception as leg_error:
                        logger.error(f"❌ 레그 실행 중 오류: 레그 ID {leg.id} - {str(leg_error)}")
                        executed_orders.append({
                            "plan_id": plan.id,
                            "leg_id": leg.id,
                            "symbol": plan.symbol,
                            "status": "ERROR",
                            "error": str(leg_error)
                        })
                        
            except Exception as plan_error:
                logger.error(f"❌ 계획 실행 중 오류: 계획 ID {plan.id} - {str(plan_error)}")
                executed_orders.append({
                    "plan_id": plan.id,
                    "symbol": plan.symbol,
                    "status": "ERROR", 
                    "error": str(plan_error)
                })
        
        # 성공/실패 통계 계산
        success_count = sum(1 for order in executed_orders if order.get("status") == "SUCCESS")
        failed_count = sum(1 for order in executed_orders if order.get("status") in ["FAILED", "ERROR"])
        
        logger.info(f"🎯 주문 실행 완료: 총 {len(executed_orders)}개 (성공: {success_count}, 실패: {failed_count})")
        
        if failed_count > 0:
            logger.warning(f"⚠️ {failed_count}개 주문이 실패했습니다. 상세 내용을 확인하세요.")
        
        return executed_orders
    
    def _convert_leg_to_order_params(self, leg: "OrderLeg", plan: "OrderPlan") -> Dict[str, Any]:
        """
        OrderLeg를 국내주식 주문 파라미터로 변환합니다.
        
        Args:
            leg: 주문 레그
            plan: 주문 계획
            
        Returns:
            Dict[str, Any]: 국내주식 주문 파라미터
        """
        logger = logging.getLogger(__name__)
        
        # 안전한 side 변환
        side_mapping = {
            "BUY": "BUY",
            "SELL": "SELL"
        }
        side = side_mapping.get(leg.side, leg.side)
        
        # 안전한 type 변환 (LIMIT/LOC만 지원)
        valid_types = ["LIMIT", "LOC"]
        order_type = leg.type if leg.type in valid_types else "LIMIT"
        
        # 파라미터 검증 로깅
        logger.debug(f"🔄 변환: {leg.side} → {side}, {leg.type} → {order_type}")
        
        # 가격 설정 (LOC 주문은 시장가이므로 0)
        if order_type == "LOC":
            price = 0.0  # LOC는 시장가
        else:
            price = float(leg.limit_price) if leg.limit_price else 0.0
        
        return {
            "symbol": plan.symbol,
            "side": side,
            "quantity": int(leg.quantity),
            "price": price,
            "order_type": order_type
        }
