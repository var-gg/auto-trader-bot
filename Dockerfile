FROM python:3.11-slim

# 시스템 패키지 설치 (로그 회전, Cloud SQL 커넥터, psycopg2를 위해)
RUN apt-get update && apt-get install -y \
    logrotate \
    wget \
    libpq-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Cloud SQL 커넥터 설치
RUN wget https://dl.google.com/cloudsql/cloud_sql_proxy.linux.amd64 -O cloud_sql_proxy && \
    chmod +x cloud_sql_proxy && \
    mv cloud_sql_proxy /usr/local/bin/

WORKDIR /app

# 로그 디렉토리 생성
RUN mkdir -p /app/logs

# Python 의존성 설치
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# 애플리케이션 코드 복사
COPY . .

# 시작 스크립트 줄바꿈 정규화 + 실행 권한
RUN sed -i 's/\r$//' start.sh && chmod +x start.sh

# 로그 회전 설정
RUN echo "/app/logs/*.log {\n  daily\n  missingok\n  rotate 5\n  compress\n  notifempty\n  create 644 root root\n}" > /etc/logrotate.d/auto-trader-bot

# 포트 노출
EXPOSE 8080

# 헬스체크 추가
HEALTHCHECK --interval=30s --timeout=30s --start-period=5s --retries=3 \
  CMD curl -f http://localhost:8080/health || exit 1

# 시작 스크립트 실행
CMD ["./start.sh"]
