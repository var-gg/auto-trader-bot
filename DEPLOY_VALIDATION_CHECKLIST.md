# Deploy Validation Checklist

## 목적
Secret Manager + Cloud Run 주입 구조로 전환한 뒤,
실제 runtime이 안전하게 올라오고 주문/체결 파이프라인까지 이어지는지 검증한다.

## 0. 배포 전 확인
- public repo에 실키/실계좌 하드코딩이 없는지 재스캔
- Secret Manager에 최신 값이 등록되어 있는지 확인
- Cloud Run service account가 Secret Accessor 권한을 갖는지 확인
- `KIS_VIRTUAL` 값이 의도와 일치하는지 확인

## 1. 배포
- 새 revision 배포
- `--set-secrets` 로 키/계좌/DB 관련 secret 주입
- `--set-env-vars` 로 일반 설정 주입
- startup 로그에서 fail-fast 예외가 없는지 확인

## 2. 부팅 검증
- Cloud Run revision 상태 Ready 확인
- `/health` 200 확인
- startup 로그에서 다음 확인
  - `Missing required runtime environment variables` 없음
  - `Refusing to start with placeholder runtime values` 없음

## 3. 설정 검증
- token refresh 호출 성공 여부
- KIS balance / snapshot / order preview 계열 정상 여부
- account_uid가 의도한 계좌 기준으로 형성되는지 확인
- Cloud SQL 연결 및 schema 접근 정상 여부

## 4. 사전 주문 검증
### 권장 순서
1. 모의투자 (`KIS_VIRTUAL=true`) 검증
2. 실전이 필요하면 장외/영향 낮은 구간 검증
3. 소량/최소 리스크 주문으로 검증

### 확인 포인트
- 주문 요청이 Cloud Run 로그에 찍히는지
- `rt_cd`, `msg_cd`, `msg1` 값이 기대 범위인지
- broker_order 생성 여부
- order_plan / order_leg 생성 여부
- fill collection 후속 반영 여부

## 5. 주문 발생 이후 모니터링
### 앱 레벨
- `app.features.portfolio.services.kr_order_service`
- `app.features.trading_hybrid.repositories.order_repository`
- `app.features.trading_hybrid.services.executor_service`
- fill collection 서비스 로그

### 데이터 레벨
- `broker_order` 생성
- `order_fill` 생성 또는 미체결 유지
- `asset_snapshot` / 포트폴리오 반영
- runtime state / backlog / change log 반영

### 인프라 레벨
- Cloud Scheduler -> Cloud Run 호출 성공 여부
- Cloud Run request latency / error rate
- restart / crash loop 여부

## 6. 실패 시 분기
### startup 실패
- Secret Manager 매핑 확인
- Cloud Run service account 권한 확인
- placeholder 값 주입 여부 확인

### API는 뜨지만 주문 실패
- KIS key/secret/계좌 env 실제 값 확인
- `KIS_VIRTUAL` / TR ID / 계좌상품코드 확인
- KIS 응답 `msg_cd`, `msg1` 기록 확인

### 주문은 됐는데 후속 반영 실패
- broker_order 저장 확인
- fill collection 스케줄/수동 호출 확인
- order_fill / snapshot 저장 경로 확인

## 7. 최종 승인 기준
아래가 모두 만족되면 전환 성공으로 본다.
- startup fail-fast 통과
- `/health` 정상
- token refresh 정상
- 잔고/스냅샷 정상
- 최소 1건 주문 경로 검증
- broker_order / fill 후속 반영 확인
- Cloud Scheduler 연동 이상 없음
