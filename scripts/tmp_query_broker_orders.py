import json
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus

user='curioustore'
password='sysop123!@#'
db='curioustore'
url=f"postgresql+psycopg2://{user}:{quote_plus(password)}@127.0.0.1:5432/{db}"
engine=create_engine(url)
q=text('''
SELECT bo.id, bo.leg_id, bo.order_number, bo.status, bo.reject_code, bo.reject_message,
       bo.payload::text as payload
FROM trading.broker_order bo
WHERE bo.id BETWEEN 69904 AND 69921
ORDER BY bo.id
''')
with engine.connect() as conn:
    rows=[]
    for r in conn.execute(q):
        m=dict(r._mapping)
        payload=m.get('payload')
        if payload and len(payload) > 500:
            m['payload']=payload[:500]+'...'
        rows.append(m)
print(json.dumps(rows, ensure_ascii=False, default=str, indent=2))
