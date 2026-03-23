# app/features/marketdata/services/kr_kospi_parser_service.py
from __future__ import annotations
import os
import logging
import pandas as pd
from typing import List, Dict, Any
from datetime import datetime

logger = logging.getLogger(__name__)


class KrKospiParserService:
    """
    📊 KOSPI 마스터 파일 파싱 서비스
    
    주요 기능:
    - 📁 KOSPI 마스터 파일(.mst) 파싱
    - 🔍 파생상품/ETF 제외하고 순수 주식만 필터링
    - 📋 JSON 형태로 데이터 반환
    """
    
    def __init__(self, mst_file_path: str = None):
        """
        KOSPI 파서 서비스 초기화
        
        Args:
            mst_file_path: KOSPI 마스터 파일 경로 (기본값: kospi_code.mst)
        """
        self.mst_file_path = mst_file_path or "kospi_code.mst"
        logger.debug(f"KrKospiParserService 초기화 - 파일 경로: {self.mst_file_path}")
    
    def parse_mst_file(self) -> pd.DataFrame:
        """
        📁 KOSPI 마스터 파일(.mst) 파싱
        
        Returns:
            pd.DataFrame: 파싱된 KOSPI 데이터프레임
        """
        logger.info(f"KOSPI 마스터 파일 파싱 시작: {self.mst_file_path}")
        
        if not os.path.exists(self.mst_file_path):
            raise FileNotFoundError(f"KOSPI 마스터 파일을 찾을 수 없습니다: {self.mst_file_path}")
        
        # 임시 파일 경로 설정
        base_dir = os.path.dirname(self.mst_file_path) or "."
        tmp_fil1 = os.path.join(base_dir, "kospi_code_part1.tmp")
        tmp_fil2 = os.path.join(base_dir, "kospi_code_part2.tmp")
        
        try:
            logger.debug("파일 분할 처리 중...")
            wf1 = open(tmp_fil1, mode="w", encoding="utf-8")
            wf2 = open(tmp_fil2, mode="w", encoding="utf-8")

            with open(self.mst_file_path, mode="r", encoding="cp949") as f:
                for row_num, row in enumerate(f, 1):
                    if row_num % 1000 == 0:
                        logger.debug(f"{row_num}행 처리 중...")
                    
                    # 앞부분 파싱 (단축코드, 표준코드, 한글명)
                    rf1 = row[0:len(row) - 228]
                    rf1_1 = rf1[0:9].rstrip()
                    rf1_2 = rf1[9:21].rstrip()
                    rf1_3 = rf1[21:].strip()
                    wf1.write(rf1_1 + ',' + rf1_2 + ',' + rf1_3 + '\n')
                    
                    # 뒷부분 파싱 (상세 정보)
                    rf2 = row[-228:]
                    wf2.write(rf2)

            wf1.close()
            wf2.close()
            logger.debug("파일 분할 완료")

            # Part 1: 기본 정보
            logger.debug("Part 1 데이터프레임 생성 중...")
            part1_columns = ['단축코드', '표준코드', '한글명']
            df1 = pd.read_csv(tmp_fil1, header=None, names=part1_columns, encoding='utf-8')

            # Part 2: 상세 정보 (고정폭 필드)
            logger.debug("Part 2 데이터프레임 생성 중...")
            field_specs = [2, 1, 4, 4, 4,
                          1, 1, 1, 1, 1,
                          1, 1, 1, 1, 1,
                          1, 1, 1, 1, 1,
                          1, 1, 1, 1, 1,
                          1, 1, 1, 1, 1,
                          1, 9, 5, 5, 1,
                          1, 1, 2, 1, 1,
                          1, 2, 2, 2, 3,
                          1, 3, 12, 12, 8,
                          15, 21, 2, 7, 1,
                          1, 1, 1, 1, 9,
                          9, 9, 5, 9, 8,
                          9, 3, 1, 1, 1
                          ]

            part2_columns = ['그룹코드', '시가총액규모', '지수업종대분류', '지수업종중분류', '지수업종소분류',
                           '제조업', '저유동성', '지배구조지수종목', 'KOSPI200섹터업종', 'KOSPI100',
                           'KOSPI50', 'KRX', 'ETP', 'ELW발행', 'KRX100',
                           'KRX자동차', 'KRX반도체', 'KRX바이오', 'KRX은행', 'SPAC',
                           'KRX에너지화학', 'KRX철강', '단기과열', 'KRX미디어통신', 'KRX건설',
                           'Non1', 'KRX증권', 'KRX선박', 'KRX섹터_보험', 'KRX섹터_운송',
                           'SRI', '기준가', '매매수량단위', '시간외수량단위', '거래정지',
                           '정리매매', '관리종목', '시장경고', '경고예고', '불성실공시',
                           '우회상장', '락구분', '액면변경', '증자구분', '증거금비율',
                           '신용가능', '신용기간', '전일거래량', '액면가', '상장일자',
                           '상장주수', '자본금', '결산월', '공모가', '우선주',
                           '공매도과열', '이상급등', 'KRX300', 'KOSPI', '매출액',
                           '영업이익', '경상이익', '당기순이익', 'ROE', '기준년월',
                           '시가총액', '그룹사코드', '회사신용한도초과', '담보대출가능', '대주가능'
                           ]

            df2 = pd.read_fwf(tmp_fil2, widths=field_specs, names=part2_columns)

            # 데이터 병합
            logger.debug("데이터 병합 중...")
            df = pd.merge(df1, df2, how='outer', left_index=True, right_index=True)

            logger.info(f"파싱 완료 - 총 {len(df)}개 종목")
            return df

        finally:
            # 임시 파일 정리
            if os.path.exists(tmp_fil1):
                os.remove(tmp_fil1)
            if os.path.exists(tmp_fil2):
                os.remove(tmp_fil2)
            logger.debug("임시 파일 정리 완료")

    def filter_stocks_only(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        🔍 순수 주식만 필터링 (파생상품, ETF, ELW 등 제외)
        
        Args:
            df: 원본 KOSPI 데이터프레임
            
        Returns:
            pd.DataFrame: 순수 주식만 필터링된 데이터프레임
        """
        logger.debug("순수 주식 필터링 시작...")
        original_count = len(df)
        
        # 📊 KRX 값 분포 확인
        if 'KRX' in df.columns:
            krx_counts = df['KRX'].value_counts()
            logger.debug("KRX 값 분포:")
            for value, count in krx_counts.items():
                logger.debug(f"  KRX='{value}': {count}개")
        
        # 🎯 주요 필터링: KRX = 'Y' (실제 주식)
        filtered_df = df.copy()
        if 'KRX' in filtered_df.columns:
            before_count = len(filtered_df)
            filtered_df = filtered_df[filtered_df['KRX'] == 'Y']
            after_count = len(filtered_df)
            logger.debug(f"KRX='Y' 필터링: {before_count} → {after_count}개")
        
        # 🔍 KOSPI200 섹터업종 필터링 (0이 아닌 값들만)
        if 'KOSPI200섹터업종' in filtered_df.columns:
            before_count = len(filtered_df)
            # KOSPI200섹터업종이 0이 아닌 값들만 필터링 (숫자, 문자 모두 포함)
            filtered_df = filtered_df[
                (filtered_df['KOSPI200섹터업종'] != 0) & 
                (filtered_df['KOSPI200섹터업종'] != '0') &
                (filtered_df['KOSPI200섹터업종'].notna())
            ]
            after_count = len(filtered_df)
            logger.debug(f"KOSPI200섹터업종 필터링 (0 제외): {before_count} → {after_count}개")
        
        logger.debug("KRX='Y' + KOSPI200 조건으로 실제 주식만 선별 완료")
        
        # 🔍 선택적 키워드 필터링 (명백한 파생상품만 제외)
        exclude_keywords = ['ETN', 'ETF', 'ELW', 'SPAC', '리츠', 'REITs']
        for keyword in exclude_keywords:
            before_count = len(filtered_df)
            # 정규표현식 특수문자 이스케이프 처리
            escaped_keyword = keyword.replace('(', r'\(').replace(')', r'\)')
            filtered_df = filtered_df[~filtered_df['한글명'].str.contains(escaped_keyword, na=False, regex=True)]
            after_count = len(filtered_df)
            if before_count != after_count:
                logger.debug(f"'{keyword}' 키워드 제외: {before_count} → {after_count}개")
        
        final_count = len(filtered_df)
        logger.info(f"필터링 완료: {original_count} → {final_count}개 (순수 주식)")
        
        # 📋 필터링된 결과 샘플 출력
        if final_count > 0:
            logger.debug("필터링된 결과 샘플 (첫 10개):")
            sample_columns = ['단축코드', '한글명', 'KRX', 'KOSPI200섹터업종']
            available_sample_columns = [col for col in sample_columns if col in filtered_df.columns]
            logger.debug(f"\n{filtered_df[available_sample_columns].head(10).to_string(index=False)}")
        
        return filtered_df

    def to_json_format(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """
        📋 데이터프레임을 JSON 형태로 변환
        
        Args:
            df: KOSPI 데이터프레임
            
        Returns:
            List[Dict]: JSON 형태의 종목 리스트
        """
        logger.debug("JSON 형태로 변환 중...")
        
        # 주요 컬럼만 선택하여 정리
        main_columns = [
            '단축코드', '표준코드', '한글명', '그룹코드', '지수업종대분류', 
            '지수업종중분류', '지수업종소분류', 'KOSPI', 'KOSPI100', 'KOSPI200섹터업종',
            'KRX300', 'KRX', '시가총액', '상장일자', '매매수량단위', '기준가'
        ]
        
        # 존재하는 컬럼만 선택
        available_columns = [col for col in main_columns if col in df.columns]
        df_selected = df[available_columns].copy()
        
        # NaN 값 정리
        df_selected = df_selected.fillna('')
        
        # JSON 형태로 변환
        result = []
        for _, row in df_selected.iterrows():
            def safe_str(value):
                """안전하게 문자열로 변환하는 헬퍼 함수"""
                if pd.isna(value) or value is None:
                    return ''
                return str(value).strip()
            
            item = {
                'symbol': safe_str(row.get('단축코드', '')),
                'standard_code': safe_str(row.get('표준코드', '')),
                'name_kr': safe_str(row.get('한글명', '')),
                'group_code': safe_str(row.get('그룹코드', '')),
                'industry_large': safe_str(row.get('지수업종대분류', '')),
                'industry_medium': safe_str(row.get('지수업종중분류', '')),
                'industry_small': safe_str(row.get('지수업종소분류', '')),
                'is_kospi': safe_str(row.get('KOSPI', '')),
                'is_kospi100': safe_str(row.get('KOSPI100', '')),
                'is_kospi200': safe_str(row.get('KOSPI200섹터업종', '')),
                'is_krx300': safe_str(row.get('KRX300', '')),
                'is_krx': safe_str(row.get('KRX', '')),
                'market_cap': safe_str(row.get('시가총액', '')),
                'listing_date': safe_str(row.get('상장일자', '')),
                'trading_unit': safe_str(row.get('매매수량단위', '')),
                'base_price': safe_str(row.get('기준가', ''))
            }
            result.append(item)
        
        logger.info(f"JSON 변환 완료 - {len(result)}개 종목")
        return result

    def get_stocks_only(self) -> List[Dict[str, Any]]:
        """
        🎯 순수 주식만 JSON 형태로 반환하는 메인 메서드
        
        Returns:
            List[Dict]: 순수 주식 종목 리스트 (JSON 형태)
        """
        logger.info("KOSPI 순수 주식 데이터 추출 시작")
        
        try:
            # 1. 마스터 파일 파싱
            df = self.parse_mst_file()
            
            # 2. 순수 주식만 필터링
            filtered_df = self.filter_stocks_only(df)
            
            # 3. JSON 형태로 변환
            result = self.to_json_format(filtered_df)
            
            logger.info(f"완료! 총 {len(result)}개 순수 주식 추출")
            return result
            
        except Exception as e:
            logger.error(f"오류 발생: {e}")
            raise


# 테스트용 함수
def test_kr_kospi_parser():
    """KOSPI 파서 테스트 함수"""
    try:
        parser = KrKospiParserService()
        stocks = parser.get_stocks_only()
        
        logger.info(f"테스트 결과:")
        logger.info(f"총 종목 수: {len(stocks)}개")
        
        if stocks:
            logger.info("첫 5개 종목 예시:")
            for i, stock in enumerate(stocks[:5], 1):
                logger.info(f"{i}. {stock['name_kr']} ({stock['symbol']}) - {stock['industry_large']}")
        
        return stocks
        
    except Exception as e:
        logger.error(f"테스트 실패: {e}")
        return []


if __name__ == "__main__":
    test_kr_kospi_parser()
