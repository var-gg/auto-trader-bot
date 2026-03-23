# app/features/earnings/services/dart_corp_code_service.py
import logging
import requests
import zipfile
import xml.etree.ElementTree as ET
from io import BytesIO
from typing import List, Dict, Optional, Any, Tuple
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError
from datetime import datetime

from app.core.config import get_settings
from app.shared.models.dart_corp_code import DartCorpCode

logger = logging.getLogger(__name__)
settings = get_settings()


class DartCorpCodeService:
    """DART 기업코드 수집 및 관리 서비스"""
    
    def __init__(self, db: Session):
        self.db = db
        self.base_url = "https://opendart.fss.or.kr/api/corpCode"
    
    def fetch_corp_code_data(self) -> Optional[bytes]:
        """
        DART API에서 기업코드 ZIP 파일 다운로드
        """
        try:
            params = {
                "crtfc_key": settings.DART_API_KEY
            }
            
            logger.info("Fetching corp code data from DART API...")
            
            response = requests.get(
                f"{self.base_url}.xml",
                params=params,
                timeout=60  # ZIP 파일 다운로드이므로 시간 여유
            )
            
            if response.status_code != 200:
                logger.error(f"DART API HTTP error: {response.status_code}")
                return None
            
            # ZIP 파일인지 확인
            content_type = response.headers.get('content-type', '')
            if 'zip' not in content_type.lower() and 'octet-stream' not in content_type.lower():
                logger.warning(f"Unexpected content type: {content_type}")
            
            logger.info(f"Successfully downloaded corp code data, size: {len(response.content)} bytes")
            return response.content
            
        except requests.exceptions.Timeout:
            logger.error("DART API request timeout")
            return None
        except requests.exceptions.RequestException as e:
            logger.error(f"DART API request error: {str(e)}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error in fetch_corp_code_data: {str(e)}")
            return None
    
    def parse_corp_code_zip(self, zip_content: bytes) -> List[Dict[str, str]]:
        """
        ZIP 파일에서 기업코드 정보 파싱
        """
        try:
            logger.info("Parsing corp code ZIP file...")
            
            corp_codes = []
            
            with zipfile.ZipFile(BytesIO(zip_content), 'r') as zip_file:
                # ZIP 파일 안의 파일 목록 확인
                file_list = zip_file.namelist()
                logger.info(f"ZIP file contains {len(file_list)} files: {file_list}")
                
                # XML 파일 찾기 (일반적으로 CORPCODE.xml)
                xml_file = None
                for file_name in file_list:
                    if file_name.endswith('.xml'):
                        xml_file = file_name
                        break
                
                if not xml_file:
                    logger.error("No XML file found in ZIP")
                    return []
                
                # XML 파일 읽기 및 파싱
                with zip_file.open(xml_file) as xml_stream:
                    tree = ET.parse(xml_stream)
                    root = tree.getroot()
                    
                    logger.info(f"XML root tag: {root.tag}")
                    
                    # 기업 정보 요소들 찾기
                    for corp in root.findall('.//list'):
                        corp_data = {}
                        
                        # 각 필드 추출
                        corp_data['corp_code'] = corp.find('corp_code').text if corp.find('corp_code') is not None else None
                        corp_data['corp_name'] = corp.find('corp_name').text if corp.find('corp_name') is not None else None
                        corp_data['corp_eng_name'] = corp.find('corp_eng_name').text if corp.find('corp_eng_name') is not None else None
                        corp_data['stock_code'] = corp.find('stock_code').text if corp.find('stock_code') is not None else None
                        corp_data['modify_date'] = corp.find('modify_date').text if corp.find('modify_date') is not None else None
                        
                        # 빈 값이나 빈 문자열 처리
                        corp_data = {k: v.strip() if v and v.strip() else None for k, v in corp_data.items()}
                        
                        # 필수 필드 검증 및 상장 기업만 필터링
                        if corp_data['corp_code'] and corp_data['corp_name'] and corp_data['modify_date'] and corp_data['stock_code']:
                            corp_data['is_stock_listed'] = True
                            corp_codes.append(corp_data)
                        
                logger.info(f"Successfully parsed {len(corp_codes)} corp codes from ZIP file")
                return corp_codes
                
        except zipfile.BadZipFile:
            logger.error("Invalid ZIP file format")
            return []
        except ET.ParseError as e:
            logger.error(f"XML parsing error: {str(e)}")
            return []
        except Exception as e:
            logger.error(f"Unexpected error in parse_corp_code_zip: {str(e)}")
            return []
    
    def upsert_corp_code(self, corp_data: Dict[str, Any]) -> Tuple[DartCorpCode, bool]:
        """
        기업코드 정보 업서트 (있으면 업데이트, 없으면 생성)
        is_new: 새로 생성된 엔티티인지 여부
        """
        corp_code = corp_data['corp_code']
        
        try:
            # 기존 데이터 조회
            existing = self.db.query(DartCorpCode).filter(
                DartCorpCode.corp_code == corp_code
            ).first()
            
            if existing:
                # 기존 데이터 업데이트
                existing.corp_name = corp_data['corp_name']
                existing.corp_eng_name = corp_data['corp_eng_name']
                existing.stock_code = corp_data['stock_code']
                existing.modify_date = corp_data['modify_date']
                existing.is_stock_listed = corp_data['is_stock_listed']
                existing.is_active = True
                existing.collected_at = datetime.now()
                
                self.db.commit()
                logger.debug(f"Updated corp code: {corp_code}")
                return existing, False
                
            else:
                # 새 데이터 생성
                new_corp = DartCorpCode(
                    corp_code=corp_code,
                    corp_name=corp_data['corp_name'],
                    corp_eng_name=corp_data['corp_eng_name'],
                    stock_code=corp_data['stock_code'],
                    modify_date=corp_data['modify_date'],
                    is_stock_listed=corp_data['is_stock_listed'],
                    is_active=True,
                    collected_at=datetime.now()
                )
                
                self.db.add(new_corp)
                self.db.commit()
                logger.debug(f"Created corp code: {corp_code}")
                return new_corp, True
                
        except IntegrityError:
            self.db.rollback()
            logger.warning(f"Integrity error for corp_code: {corp_code}")
            # 재시도 (다른 프로세스에서 동시에 생성했을 수 있음)
            existing = self.db.query(DartCorpCode).filter(
                DartCorpCode.corp_code == corp_code
            ).first()
            if existing:
                return self.upsert_corp_code(corp_data)
            else:
                raise
        except Exception as e:
            self.db.rollback()
            logger.error(f"Error upserting corp_code {corp_code}: {str(e)}")
            raise
    
    def update_active_status(self, collected_corp_codes: List[str]) -> None:
        """
        최신 수집에서 누락된 기업코드는 비활성으로 표시
        """
        try:
            # 최신 수집에 포함되지 않은 기업코드들을 비활성화
            inactive_count = self.db.query(DartCorpCode).filter(
                DartCorpCode.corp_code.notin_(collected_corp_codes),
                DartCorpCode.is_active == True
            ).update({'is_active': False})
            
            if inactive_count > 0:
                logger.info(f"Deactivated {inactive_count} corp codes not in latest collection")
            
            self.db.commit()
            
        except Exception as e:
            logger.error(f"Error updating active status: {str(e)}")
            self.db.rollback()
            raise
    
    def sync_corp_codes(self) -> Dict[str, Any]:
        """
        DART에서 최신 기업코드 데이터 동기화
        """
        try:
            logger.info("Starting corp code sync process...")
            
            # 1. DART API에서 데이터 다운로드
            zip_content = self.fetch_corp_code_data()
            if zip_content is None:
                return {
                    "status": "error",
                    "message": "Failed to fetch data from DART API"
                }
            
            # 2. ZIP 파일 파싱
            corp_codes_data = self.parse_corp_code_zip(zip_content)
            if not corp_codes_data:
                return {
                    "status": "error", 
                    "message": "Failed to parse ZIP file"
                }
            
            # 3. 데이터베이스 업데이트 통계
            stats = {
                "total_from_dart": len(corp_codes_data),
                "updated": 0,
                "created": 0,
                "errors": 0,
                "error_details": []
            }
            
            processed_corp_codes = []
            
            # 4. 각 기업코드 업서트
            for corp_data in corp_codes_data:
                try:
                    dart_corp, is_new = self.upsert_corp_code(corp_data)
                    processed_corp_codes.append(corp_data['corp_code'])
                    
                    if is_new:
                        stats["created"] += 1
                    else:
                        stats["updated"] += 1
                        
                except Exception as e:
                    stats["errors"] += 1
                    stats["error_details"].append({
                        "corp_code": corp_data.get('corp_code', 'unknown'),
                        "error": str(e)
                    })
                    logger.error(f"Error processing corp_code {corp_data.get('corpor_code')}: {str(e)}")
            
            # 5. 비활성 기업코드 업데이트
            self.update_active_status(processed_corp_codes)
            
            # 6. 최종 통계
            result = {
                "status": "success",
                "message": "Corp code sync completed successfully",
                "stats": stats,
                "summary": {
                    "total_corp_codes": stats["total_from_dart"],
                    "successful_updates": stats["updated"],
                    "successful_creates": stats["created"],
                    "errors": stats["errors"],
                    "success_rate": round(
                        (stats["updated"] + stats["created"]) / stats["total_from_dart"] * 100, 2
                    ) if stats["total_from_dart"] > 0 else 0
                }
            }
            
            logger.info(f"Corp code sync completed: {result['summary']}")
            return result
            
        except Exception as e:
            logger.error(f"Fatal error in sync_corp_codes: {str(e)}")
            return {
                "status": "error",
                "message": f"Sync failed: {str(e)}"
            }
    
    def get_corp_code_lookup(self, corp_code: str = None, stock_code: str = None) -> Optional[DartCorpCode]:
        """
        기업코드 또는 종목코드로 조회
        """
        try:
            query = self.db.query(DartCorpCode).filter(DartCorpCode.is_active == True)
            
            if corp_code:
                query = query.filter(DartCorpCode.corp_code == corp_code)
            elif stock_code:
                query = query.filter(DartCorpCode.stock_code == stock_code)
            else:
                return None
                
            return query.first()
            
        except Exception as e:
            logger.error(f"Error in get_corp_code_lookup: {str(e)}")
            return None
    
    def get_corp_by_code(self, corp_code: str) -> Optional[DartCorpCode]:
        """
        기업코드로 기업 정보 조회
        """
        try:
            return self.db.query(DartCorpCode).filter(
                DartCorpCode.corp_code == corp_code,
                DartCorpCode.is_active == True
            ).first()
        except Exception as e:
            logger.error(f"Error getting corp by code {corp_code}: {str(e)}")
            return None
    
    def get_corp_code_stats(self) -> Dict[str, Any]:
        """
        기업코드 데이터베이스 현황 조회
        """
        try:
            total_count = self.db.query(DartCorpCode).count()
            active_count = self.db.query(DartCorpCode).filter(DartCorpCode.is_active == True).count()
            stock_listed_count = self.db.query(DartCorpCode).filter(
                DartCorpCode.is_stock_listed == True,
                DartCorpCode.is_active == True
            ).count()
            
            latest_collected = self.db.query(DartCorpCode.collected_at).filter(
                DartCorpCode.is_active == True
            ).order_by(DartCorpCode.collected_at.desc()).first()
            
            return {
                "status": "success",
                "stats": {
                    "total_corp_codes": total_count,
                    "active_corp_codes": active_count,
                    "stock_listed_corp_codes": stock_listed_count,
                    "non_listed_corp_codes": active_count - stock_listed_count,
                    "latest_collected": latest_collected[0].isoformat() if latest_collected else None
                }
            }
            
        except Exception as e:
            logger.error(f"Error in get_corp_code_stats: {str(e)}")
            return {
                "status": "error",
                "message": str(e)
            }
