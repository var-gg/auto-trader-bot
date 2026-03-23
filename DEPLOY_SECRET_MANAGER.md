# Cloud Run + Secret Manager 배포 가이드

## 목표
- 로컬은 `.env` 기반으로 개발
- Cloud Run은 Secret Manager 기반으로 주입
- 필수 계정/키 누락 시 앱이 startup 단계에서 fail-fast

## 필수 secret/env
### Secret Manager 권장
- `KIS_APPKEY`
- `KIS_APPSECRET`
- `KIS_VIRTUAL_APPKEY`
- `KIS_VIRTUAL_APPSECRET`
- `OPENAI_API_KEY`
- `FRED_API_KEY`
- `DART_API_KEY`
- `KIS_CANO`
- `KIS_VIRTUAL_CANO`
- `KIS_ACNT_PRDT_CD`
- `DB_USER`
- `DB_PASS`
- `DB_NAME`

### 일반 env
- `CLOUD_CODE_ENV=true`
- `INSTANCE_CONNECTION_NAME=<project:region:instance>`
- `DB_SCHEMA=trading`
- `KIS_VIRTUAL=false` 또는 `true`

## Secret 생성 예시
```bash
echo -n 'your-kis-appkey' | gcloud secrets create KIS_APPKEY --data-file=-
echo -n 'your-kis-appsecret' | gcloud secrets create KIS_APPSECRET --data-file=-
echo -n 'your-account-8digits' | gcloud secrets create KIS_CANO --data-file=-
echo -n '01' | gcloud secrets create KIS_ACNT_PRDT_CD --data-file=-
```

이미 secret이 있으면:
```bash
echo -n 'new-value' | gcloud secrets versions add KIS_APPKEY --data-file=-
```

## Cloud Run 배포 예시
```bash
gcloud run deploy auto-trader-bot \
  --source . \
  --platform managed \
  --region asia-northeast3 \
  --allow-unauthenticated \
  --memory=1Gi \
  --cpu=1 \
  --timeout=300 \
  --max-instances=10 \
  --set-env-vars="CLOUD_CODE_ENV=true,DB_SCHEMA=trading,INSTANCE_CONNECTION_NAME=your-project:asia-northeast3:your-instance,KIS_VIRTUAL=false" \
  --set-secrets="DB_USER=DB_USER:latest,DB_PASS=DB_PASS:latest,DB_NAME=DB_NAME:latest,KIS_APPKEY=KIS_APPKEY:latest,KIS_APPSECRET=KIS_APPSECRET:latest,KIS_CANO=KIS_CANO:latest,KIS_ACNT_PRDT_CD=KIS_ACNT_PRDT_CD:latest,OPENAI_API_KEY=OPENAI_API_KEY:latest,FRED_API_KEY=FRED_API_KEY:latest,DART_API_KEY=DART_API_KEY:latest" \
  --add-cloudsql-instances=your-project:asia-northeast3:your-instance
```

### 모의투자 사용 시 추가
```bash
--set-env-vars="CLOUD_CODE_ENV=true,DB_SCHEMA=trading,INSTANCE_CONNECTION_NAME=your-project:asia-northeast3:your-instance,KIS_VIRTUAL=true" \
--set-secrets="KIS_VIRTUAL_APPKEY=KIS_VIRTUAL_APPKEY:latest,KIS_VIRTUAL_APPSECRET=KIS_VIRTUAL_APPSECRET:latest,KIS_VIRTUAL_CANO=KIS_VIRTUAL_CANO:latest,..."
```

## 운영 안전장치
앱은 Cloud Run / `CLOUD_CODE_ENV=true` 환경에서 아래 값이 없으면 startup 실패한다.
- `KIS_APPKEY`
- `KIS_APPSECRET`
- `KIS_CANO`
- `KIS_ACNT_PRDT_CD`
- `KIS_VIRTUAL=true` 일 때 `KIS_VIRTUAL_CANO`

또한 placeholder 값(`00000000` 등)으로도 startup 실패한다.

## 배포 후 확인
```bash
gcloud run services describe auto-trader-bot \
  --region asia-northeast3 \
  --format export
```

확인 포인트:
- env에 plain-text 비밀값이 직접 들어가 있지 않은지
- secretKeyRef/secret 주입이 기대대로 연결됐는지
- `KIS_VIRTUAL` 값이 의도한 런타임과 일치하는지

## 실배포 테스트 체크리스트
1. 새 revision 배포
2. Cloud Run startup 성공 여부 확인
3. `/health` 응답 확인
4. token refresh / balance / order preview 계열 호출로 기본 설정 확인
5. 실제 주문 경로는 장 외/모의투자/소량 검증 순으로 확인
6. 주문 발생 시:
   - Cloud Run request log
   - broker order 생성 여부
   - fill collection 후속 반영 여부
   - order note / runtime state 변화
   를 함께 확인

## 모니터링 포인트
- startup 실패 로그: secret/env 누락 여부
- KIS token refresh 성공/실패
- order endpoint 호출 시 `rt_cd`, `msg_cd`, `msg1`
- broker_order / order_fill 생성 여부
- Cloud Scheduler -> Cloud Run 호출 성공 여부

## 로컬 개발
로컬은 기존처럼 `.env` 사용 가능.
다만 `.env`는 git에 올리지 않는다.
