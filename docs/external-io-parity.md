# External IO Parity

Created: 2026-03-25
Branch: `public-release-20260323`
Scope: outbound broker requests and external read-adapter request contracts without sending live orders.

## Goal
Verify that the TO-BE code builds the same effective external request meaning as AS-IS for:
- broker outbound (KIS order requests)
- market data / holiday / news read adapters
- fill collection adapter requests
- common failure handling at the adapter boundary

No live broker orders are sent in this validation.
All checks are stub / no-op / request-shape tests.

## Added tests
- `tests/integration/test_broker_adapter_parity_requests.py`
- `tests/integration/test_marketdata_adapter_contracts.py`

## Broker outbound parity

### Active live path under test
`app/features/trading_hybrid/repositories/order_repository._submit_leg_to_broker`

This is the active live money-path adapter seam for current trading-hybrid execution.

### Verified outbound meaning

#### US order path
`ctx(country=US, side=BUY, leg_type=LIMIT)` maps to:
- `KISClient.order_stock(...)`
- `order_type='buy'`
- `symbol=<plan symbol>`
- `quantity=str(quantity)`
- `price=str(limit_price)` for LIMIT
- `order_method='LIMIT'`
- exchange mapped from internal exchange via `KIS_OVERSEAS_EXCHANGE_MAP`

#### KR order path
`ctx(country=KR, side=SELL, leg_type=AFTER_HOURS_06)` maps to:
- `KISClient.order_cash_sell(...)`
- `PDNO=<symbol>`
- `ORD_DVSN='00'` (limit)
- `ORD_QTY=str(quantity)`
- `ORD_UNPR=<integer price string>`
- `EXCG_ID_DVSN_CD='NXT'` for after-hours legs

Regular KR LIMIT and MARKET path remain encoded in the same mapper:
- LIMIT -> `ORD_DVSN='00'`, price passed
- MARKET -> `ORD_DVSN='01'`, price forced to `0`

### Stored broker correlation behavior
On submit result, repository still writes:
- `trading.broker_order.payload`
- `status` (`SUBMITTED` / `REJECTED`)
- `order_number`
- `reject_code`
- `reject_message`

This is the current common local model for outbound result persistence.

## Failure handling parity

### Verified now
Tests confirm that broker failures normalize into the same local reject model:
- KIS reject response -> `status='REJECTED'`, `reject_code=msg_cd`, `reject_message=msg1`
- thrown exception -> `status='REJECTED'`, `reject_code='EXCEPTION'`, `reject_message=str(error)`

This uses `extract_reject_reason(...)` as the current common mapping point.

### Important limitation
A full provider-agnostic failure taxonomy for:
- rate limit
- auth failure
- transient network error
- permanent data error

is **not fully centralized yet**.
Current code proves:
- token-expiry retry exists in `KISClient._make_request`
- rejects/exceptions are normalized for order persistence

But there is not yet one explicit shared enum/model covering all adapter failures.

## KIS auth / token handling parity

### Observed behavior
- `KISAuth.token(...)` caches tokens in DB by app-hash / tr_id / base_url / provider
- token issuance retries up to 3 times with delay
- `_make_request(...)` detects token-expiry responses and forces one refresh/retry

### Meaning for parity
TO-BE still preserves essential auth behavior:
- bearer token header added automatically
- token refresh is automatic on expiry-specific error
- no silent success masking on token failure

## Market-data / read-adapter parity

### Verified request-shape tests
The integration tests capture request shape without sending traffic for:
- `KISClient.order_stock(...)`
- `KISClient.order_cash_buy(...)`
- `KISClient.domestic_holiday_check(...)`
- `KISClient.overseas_news_test(...)`
- `KISClient.domestic_news_test(...)`

### Contract details confirmed
- US symbols are normalized to KIS style (`BF-B` -> `BF/B`) before request
- US order request carries `PDNO`, `ORD_QTY`, `OVRS_ORD_UNPR`, `OVRS_EXCG_CD`
- KR cash order request carries `CANO`, `ACNT_PRDT_CD`, `PDNO`, `ORD_QTY`, `ORD_UNPR`, `ORD_DVSN`, `EXCG_ID_DVSN_CD`
- holiday/news adapters preserve their expected request parameter names

## Fill collection adapter parity

### Verified request-shape tests
The integration tests capture that:
- `DomesticFillCollectionService._collect_domestic_fills_single_day(...)` calls `domestic_order_test(...)` with same-day query window and expected KRX query contract
- `OverseasFillCollectionService._collect_overseas_fills_single_day(...)` calls `overseas_order_history_test(...)` with same-day query window and expected overseas query contract

### Query meaning preserved
Domestic:
- `INQR_STRT_DT = date`
- `INQR_END_DT = date`
- `CCLD_DVSN = requested fill-state`
- `EXCG_ID_DVSN_CD = 'KRX'`

Overseas:
- `ORD_STRT_DT = date`
- `ORD_END_DT = date`
- `CCLD_NCCS_DVSN = requested fill-state`
- `OVRS_EXCG_CD = '%'`

## What is parity-preserved now
- active broker outbound parameter meaning for KR/US submit path
- request-shape normalization for KIS US symbol format
- broker reject/exception persistence model
- fill collection request contracts
- holiday/news request parameter contracts
- token-expiry retry behavior at KIS client layer

## Gaps still open

### 1) Idempotency key parity is weak
Current KIS integration path does not expose a dedicated broker idempotency key field.
Current local correlation uses:
- `broker_order.id` (local fallback)
- `order_number` when returned

This is traceable but not equivalent to a first-class idempotency contract.

### 2) Retry taxonomy is incomplete
Token-expiry retry exists, but there is no unified explicit policy for:
- rate-limit retries
- transient 5xx retries beyond token refresh
- provider/data-validation hard-fail classification

### 3) Silent defaulting risk remains in exchange mapping
`KIS_OVERSEAS_EXCHANGE_MAP.get(..., 'NAS')` still defaults unknown exchanges to NAS.
That is operationally risky and should eventually move from silent default to explicit validation.

### 4) Market-data/headline-risk parity is only request-shape level here
This pass checks request parameter shape, not full semantic response normalization for every external reader.
Headline risk currently depends on DB-backed news fetch plus LLM scoring; that needs a separate response-contract parity pass if promoted to blocking risk control.

## Bottom line
At the outbound adapter layer, TO-BE currently preserves the essential live request meaning for the active KIS submit path and the main external read adapters we inspected.

The biggest remaining risks are:
- lack of first-class idempotency keys,
- incomplete shared failure taxonomy,
- and silent exchange defaulting for overseas routes.
