from __future__ import annotations

from app.core.kis_client import KISClient
from app.features.portfolio.services.domestic_fill_collection_service import DomesticFillCollectionService
from app.features.portfolio.services.overseas_fill_collection_service import OverseasFillCollectionService


class _CaptureRequestClient(KISClient):
    def __init__(self):
        self.calls = []
        self.base_url = 'https://example.test'
        self.exchange_map = {"NASDAQ": "NAS", "NYSE": "NYS"}
        self.db = object()

    def _make_request(self, url, tr_id, params, retry_count=0, extra_headers=None):
        self.calls.append({
            "url": url,
            "tr_id": tr_id,
            "params": params,
            "retry_count": retry_count,
            "extra_headers": extra_headers,
        })
        return {"rt_cd": "0", "output": [], "output1": [], "_headers": {}}


class _NoopDB:
    def query(self, *args, **kwargs):
        raise AssertionError("query should not be reached in request-shape test")


def test_kis_client_builds_us_order_request_shape_without_sending():
    client = _CaptureRequestClient()
    client.order_stock(order_type='buy', symbol='BF-B', quantity='7', price='321.55', order_method='LIMIT', exchange='NASDAQ')

    call = client.calls[0]
    assert '/uapi/overseas-stock/v1/trading/order' in call['url']
    assert call['params']['PDNO'] == 'BF/B'
    assert call['params']['ORD_QTY'] == '7'
    assert call['params']['OVRS_ORD_UNPR'] == '321.55'
    assert call['params']['ORD_SVR_DVSN_CD'] == '0'
    assert call['params']['OVRS_EXCG_CD'] == 'NASD'


def test_kis_client_builds_kr_buy_request_shape_without_sending():
    client = _CaptureRequestClient()
    client.order_cash_buy(CANO='12345678', ACNT_PRDT_CD='01', PDNO='005930', ORD_DVSN='00', ORD_QTY='3', ORD_UNPR='71200', EXCG_ID_DVSN_CD='KRX')

    call = client.calls[0]
    assert '/uapi/domestic-stock/v1/trading/order-cash' in call['url']
    assert call['params']['CANO'] == '12345678'
    assert call['params']['ACNT_PRDT_CD'] == '01'
    assert call['params']['PDNO'] == '005930'
    assert call['params']['ORD_QTY'] == '3'
    assert call['params']['ORD_UNPR'] == '71200'
    assert call['params']['ORD_DVSN'] == '00'
    assert call['params']['EXCG_ID_DVSN_CD'] == 'KRX'


def test_fill_collection_requests_match_expected_query_contracts(monkeypatch):
    domestic = DomesticFillCollectionService(_NoopDB())
    overseas = OverseasFillCollectionService(_NoopDB())
    capture = _CaptureRequestClient()
    domestic.kis_client = capture
    overseas.kis_client = capture

    import asyncio
    asyncio.run(domestic._collect_domestic_fills_single_day('20260324', ccld_dvsn='01'))
    asyncio.run(overseas._collect_overseas_fills_single_day('20260324', ccld_nccs_dvsn='01'))

    d_call = capture.calls[0]
    o_call = capture.calls[1]

    assert d_call['params']['INQR_STRT_DT'] == '20260324'
    assert d_call['params']['INQR_END_DT'] == '20260324'
    assert d_call['params']['CCLD_DVSN'] == '01'
    assert d_call['params']['EXCG_ID_DVSN_CD'] == 'KRX'

    assert o_call['params']['ORD_STRT_DT'] == '20260324'
    assert o_call['params']['ORD_END_DT'] == '20260324'
    assert o_call['params']['CCLD_NCCS_DVSN'] == '01'
    assert o_call['params']['OVRS_EXCG_CD'] == '%'


def test_kis_client_holiday_and_news_requests_preserve_contract_without_sending():
    client = _CaptureRequestClient()
    client.domestic_holiday_check('20260325')
    client.overseas_news_test(SYMB='AAPL', EXCD='NAS', DATA_DT='20260325')
    client.domestic_news_test(FID_INPUT_ISCD='005930', FID_INPUT_DATE_1='20260325')

    holiday, overseas_news, domestic_news = client.calls
    assert holiday['params']['BASS_DT'] == '20260325'
    assert overseas_news['params']['SYMB'] == 'AAPL'
    assert overseas_news['params']['EXCD'] == 'NAS'
    assert domestic_news['params']['FID_INPUT_ISCD'] == '005930'
    assert domestic_news['params']['FID_INPUT_DATE_1'] == '20260325'
