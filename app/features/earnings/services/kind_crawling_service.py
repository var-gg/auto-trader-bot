# app/features/earnings/services/kind_crawling_service.py

import httpx
from typing import Dict, Any, Optional, List
import logging
import re
from bs4 import BeautifulSoup
from datetime import datetime

logger = logging.getLogger(__name__)


class KindCrawlingService:
    def __init__(self):
        self.base_url = "https://kind.krx.co.kr"
        self.session_url = f"{self.base_url}/corpgeneral/irschedule.do"
        
    async def crawl_ir_schedule(self, 
                               search_code: str = "034220",
                               from_date: str = "2025-10-04",
                               to_date: str = "2026-01-04",
                               page_size: int = 15,
                               page_index: int = 1) -> str:
        """
        KIND IR 게시판 크롤링
        """
        headers = {
            'Accept': 'text/html, */*; q=0.01',
            'Accept-Encoding': 'gzip, deflate, br, zstd',
            'Accept-Language': 'ko-KR,ko;q=0.9,ja-JP;q=0.8,ja;q=0.7,en-US;q=0.6,en;q=0.5,vi;q=0.4',
            'Cache-Control': 'no-cache',
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'Origin': 'https://kind.krx.co.kr',
            'Pragma': 'no-cache',
            'Referer': 'https://kind.krx.co.kr/corpgeneral/irschedule.do?method=searchIRScheduleMain&gubun=iRSchedule',
            'Sec-Ch-Ua': '"Google Chrome";v="123", "Not:A-Brand";v="8", "Chromium";v="123"',
            'Sec-Ch-Ua-Mobile': '?0',
            'Sec-Ch-Ua-Platform': '"Windows"',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
            'X-Requested-With': 'XMLHttpRequest'
        }
        
        payload = {
            'method': 'searchIRScheduleSub',
            'paxreq': '',
            'outsvcno': '',
            'currentPageSize': str(page_size),
            'pageIndex': str(page_index),
            'orderMode': '4',
            'orderStat': 'D',
            'searchCodeType': 'char',
            'repIsuSrtCd': f'A{search_code}',
            'irSeq': '',
            'forward': 'searchirschedule_sub',
            'searchCorpName': search_code,
            'resoroomType': '',
            'searchFromDate': from_date,
            'searchToDate': to_date,
            'marketType': '1',
            'searchName': search_code,
            'title': '',
            'fromDate': from_date,
            'toDate': to_date,
        }
        
        try:
            async with httpx.AsyncClient() as client:
                logger.info(f"KIND IR 게시판 크롤링 시작 - 검색코드: {search_code}, 기간: {from_date} ~ {to_date}")
                
                response = await client.post(
                    self.session_url,
                    headers=headers,
                    data=payload,
                    timeout=30.0
                )
                
                response.raise_for_status()
                
                logger.info(f"KIND IR 게시판 응답 수신 완료 - 상태코드: {response.status_code}, 길이: {len(response.text)}")
                
                return response.text
                
        except httpx.RequestError as e:
            logger.error(f"KIND IR 게시판 크롤링 요청 오류: {str(e)}")
            raise Exception(f"KIND 웹사이트 요청 실패: {str(e)}")
            
        except httpx.HTTPStatusError as e:
            logger.error(f"KIND IR 게시판 HTTP 오류: {e.response.status_code}")
            raise Exception(f"KIND 웹사이트 응답 오류: {e.response.status_code}")
            
        except Exception as e:
            logger.error(f"KIND IR 게시판 크롤링 예상치 못한 오류: {str(e)}")
            raise Exception(f"크롤링 오류: {str(e)}")
    
    def _extract_total_count(self, html_content: str) -> int:
        """HTML에서 전체 건수 추출"""
        try:
            # "전체 <em>215</em>건" 패턴 찾기
            pattern = r'전체\s*<em>(\d+)</em>건'
            match = re.search(pattern, html_content)
            if match:
                return int(match.group(1))
            return 0
        except Exception as e:
            logger.error(f"전체 건수 추출 실패: {str(e)}")
            return 0
    
    def _extract_company_data(self, html_content: str) -> List[Dict[str, str]]:
        """HTML에서 회사 데이터 추출 (종목코드, 날짜)"""
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            results = []
            
            # tbody 내의 모든 tr 태그 찾기
            tbody = soup.find('tbody')
            if not tbody:
                logger.warning("tbody를 찾을 수 없습니다")
                return results
            
            rows = tbody.find_all('tr')
            logger.info(f"추출된 행 수: {len(rows)}")
            
            for row in rows:
                try:
                    # onclick="companysummary_open('03422')" 에서 종목코드 추출
                    company_link = row.find('a', onclick=re.compile(r"companysummary_open\('(\d+)'\)"))
                    if not company_link:
                        continue
                    
                    # 종목코드 추출 및 6자리로 패딩
                    company_code_raw = re.search(r"companysummary_open\('(\d+)'\)", company_link.get('onclick', ''))
                    if not company_code_raw:
                        continue
                    
                    company_code = company_code_raw.group(1).ljust(6, '0')  # 6자리 우측 패딩
                    
                    # 모든 td 태그 찾기
                    tds = row.find_all('td')
                    if len(tds) < 5:  # 최소 5개 컬럼 필요
                        continue
                    
                    # 뒤에서 두번째 td에서 날짜 추출
                    date_td = tds[-2]  # 뒤에서 두번째
                    date_text = date_td.get_text(strip=True)
                    
                    # 날짜 형식 검증 (YYYY-MM-DD)
                    if re.match(r'\d{4}-\d{2}-\d{2}', date_text):
                        results.append({
                            'company_code': company_code,
                            'date': date_text,
                            'company_name': company_link.get_text(strip=True)
                        })
                        logger.debug(f"추출된 데이터: {company_code} - {date_text}")
                    
                except Exception as e:
                    logger.warning(f"행 데이터 추출 실패: {str(e)}")
                    continue
            
            logger.info(f"성공적으로 추출된 데이터 수: {len(results)}")
            return results
            
        except Exception as e:
            logger.error(f"회사 데이터 추출 실패: {str(e)}")
            return []
    
    async def crawl_ir_schedule_advanced(self, 
                                       title: str,
                                       from_date: str,
                                       to_date: str,
                                       current_page_size: int = 15) -> Dict[str, Any]:
        """
        고급 KIND IR 게시판 크롤링 - 모든 페이지 탐색 및 데이터 추출
        """
        all_results = []
        page_index = 1
        total_count = 0
        
        try:
            logger.info(f"고급 KIND IR 크롤링 시작 - 제목: {title}, 기간: {from_date} ~ {to_date}")
            
            while True:
                # 페이로드 구성
                payload = {
                    'method': 'searchIRScheduleSub',
                    'paxreq': '',
                    'outsvcno': '',
                    'currentPageSize': str(current_page_size),
                    'pageIndex': str(page_index),
                    'orderMode': '4',
                    'orderStat': 'D',
                    'searchCodeType': 'char',
                    'repIsuSrtCd': '',
                    'irSeq': '',
                    'forward': 'searchirschedule_sub',
                    'searchCorpName': '',
                    'resoroomType': '',
                    'searchFromDate': from_date,
                    'searchToDate': to_date,
                    'marketType': '1',
                    'searchName': '',
                    'title': title,
                    'fromDate': from_date,
                    'toDate': to_date,
                }
                
                # 헤더 구성
                headers = {
                    'Accept': 'text/html, */*; q=0.01',
                    'Accept-Encoding': 'gzip, deflate, br, zstd',
                    'Accept-Language': 'ko-KR,ko;q=0.9,ja-JP;q=0.8,ja;q=0.7,en-US;q=0.6,en;q=0.5,vi;q=0.4',
                    'Cache-Control': 'no-cache',
                    'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
                    'Origin': 'https://kind.krx.co.kr',
                    'Pragma': 'no-cache',
                    'Referer': 'https://kind.krx.co.kr/corpgeneral/irschedule.do?method=searchIRScheduleMain&gubun=iRSchedule',
                    'Sec-Ch-Ua': '"Google Chrome";v="123", "Not:A-Brand";v="8", "Chromium";v="123"',
                    'Sec-Ch-Ua-Mobile': '?0',
                    'Sec-Ch-Ua-Platform': '"Windows"',
                    'Sec-Fetch-Dest': 'empty',
                    'Sec-Fetch-Mode': 'cors',
                    'Sec-Fetch-Site': 'same-origin',
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
                    'X-Requested-With': 'XMLHttpRequest'
                }
                
                # 요청 실행
                async with httpx.AsyncClient() as client:
                    response = await client.post(
                        self.session_url,
                        headers=headers,
                        data=payload,
                        timeout=30.0
                    )
                    response.raise_for_status()
                    html_content = response.text
                
                # 첫 페이지에서 전체 건수 확인
                if page_index == 1:
                    total_count = self._extract_total_count(html_content)
                    logger.info(f"전체 건수: {total_count}")
                    
                    if total_count == 0:
                        logger.info("검색 결과가 없습니다.")
                        break
                
                # 현재 페이지 데이터 추출
                page_results = self._extract_company_data(html_content)
                all_results.extend(page_results)
                
                logger.info(f"페이지 {page_index} 처리 완료 - 추출된 데이터: {len(page_results)}건")
                
                # 다음 페이지가 있는지 확인
                if len(page_results) < current_page_size or len(all_results) >= total_count:
                    logger.info("모든 페이지 처리 완료")
                    break
                
                page_index += 1
                
                # 안전장치: 최대 100페이지까지만 처리
                if page_index > 100:
                    logger.warning("최대 페이지 수(100)에 도달했습니다.")
                    break
            
            return {
                'success': True,
                'total_count': total_count,
                'extracted_count': len(all_results),
                'page_count': page_index,
                'results': all_results,
                'search_params': {
                    'title': title,
                    'from_date': from_date,
                    'to_date': to_date,
                    'current_page_size': current_page_size
                }
            }
            
        except Exception as e:
            logger.error(f"고급 KIND IR 크롤링 실패: {str(e)}")
            return {
                'success': False,
                'error': str(e),
                'total_count': 0,
                'extracted_count': 0,
                'page_count': 0,
                'results': [],
                'search_params': {
                    'title': title,
                    'from_date': from_date,
                    'to_date': to_date,
                    'current_page_size': current_page_size
                }
            }
    
    async def test_connection(self) -> Dict[str, Any]:
        """
        KIND 웹사이트 연결 테스트
        """
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(
                    f"{self.base_url}/corpgeneral/irschedule.do",
                    timeout=10.0
                )
                
                return {
                    "status": "success",
                    "status_code": response.status_code,
                    "message": "KIND 웹사이트 연결 성공",
                    "accessible": response.status_code == 200
                }
                
        except Exception as e:
            return {
                "status": "error",
                "status_code": None,
                "message": f"KIND 웹사이트 연결 실패: {str(e)}",
                "accessible": False
            }
