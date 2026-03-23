# python -m scripts.init_themes
from app.core.db import SessionLocal
from app.shared.models.theme import Theme
from app.shared.models.theme_i18n import ThemeI18n

themes = [
    {"code": "AI", "en": "Artificial Intelligence", "ko": "인공지능"},
    {"code": "SEMICON", "en": "Semiconductors", "ko": "반도체"},
    {"code": "CLOUD", "en": "Cloud Computing", "ko": "클라우드"},
    {"code": "EV", "en": "Electric Vehicles", "ko": "전기차"},
    {"code": "BATTERY", "en": "Batteries & Storage", "ko": "배터리"},
    {"code": "GREEN", "en": "Renewable Energy", "ko": "친환경 에너지"},
    {"code": "HEALTHCARE", "en": "Healthcare & Biotech", "ko": "헬스케어·바이오"},
    {"code": "PHARMA", "en": "Pharmaceuticals", "ko": "제약"},
    {"code": "CYBERSEC", "en": "Cybersecurity", "ko": "사이버보안"},
    {"code": "FINTECH", "en": "Fintech", "ko": "핀테크"},
    {"code": "ESG", "en": "ESG & Sustainability", "ko": "ESG 지속가능성"},
    {"code": "DEFENSE", "en": "Defense & Aerospace", "ko": "방산·항공우주"},
    {"code": "5G", "en": "5G & Next-Gen Connectivity", "ko": "5G 차세대 통신"},
    {"code": "REITS", "en": "REITs", "ko": "리츠"},
    {"code": "E-COMMERCE", "en": "E-commerce", "ko": "전자상거래"},
    {"code": "FINANCIALS", "en": "Banking & Financials", "ko": "은행·금융"},
    {"code": "CONSUMER", "en": "Consumer & Retail", "ko": "소비재·리테일"},
    {"code": "ENERGY", "en": "Energy & Materials", "ko": "에너지·원자재"},
]

db = SessionLocal()

for t in themes:
    theme = Theme(code=t["code"])
    db.add(theme)
    db.flush()
    db.add(ThemeI18n(theme_id=theme.id, lang_code="en", name=t["en"]))
    db.add(ThemeI18n(theme_id=theme.id, lang_code="ko", name=t["ko"]))

db.commit()
db.close()
