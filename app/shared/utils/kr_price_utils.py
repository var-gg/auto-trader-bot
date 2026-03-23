"""
국내주식 가격 관련 유틸리티 함수들
"""

def get_kr_tick_size(price: float) -> float:
    """
    국내주식 호가단위를 반환합니다.
    
    Args:
        price: 주가
        
    Returns:
        float: 호가단위
    """
    if price < 1000:
        return 1
    elif price < 5000:
        return 5
    elif price < 10000:
        return 10
    elif price < 50000:
        return 50
    elif price < 100000:
        return 100
    elif price < 500000:
        return 500
    else:
        return 1000


def round_down_to_tick_size(price: float) -> float:
    """
    가격을 호가단위에 맞춰 내림합니다.
    
    Args:
        price: 원본 가격
        
    Returns:
        float: 호가단위에 맞춰 내림된 가격
    """
    if price <= 0:
        return price
        
    tick_size = get_kr_tick_size(price)
    return int(price // tick_size) * tick_size


def round_up_to_tick_size(price: float) -> float:
    """
    가격을 호가단위에 맞춰 올림합니다.
    
    Args:
        price: 원본 가격
        
    Returns:
        float: 호가단위에 맞춰 올림된 가격
    """
    if price <= 0:
        return price
        
    tick_size = get_kr_tick_size(price)
    return int((price + tick_size - 1) // tick_size) * tick_size


def validate_kr_price(price: float) -> bool:
    """
    국내주식 가격이 호가단위에 맞는지 검증합니다.
    
    Args:
        price: 검증할 가격
        
    Returns:
        bool: 호가단위에 맞으면 True, 아니면 False
    """
    if price <= 0:
        return False
        
    tick_size = get_kr_tick_size(price)
    return price % tick_size == 0


def adjust_kr_price(price: float, mode: str = "down") -> float:
    """
    국내주식 가격을 호가단위에 맞춰 조정합니다.
    
    Args:
        price: 조정할 가격
        mode: 조정 모드 ("down", "up", "nearest")
        
    Returns:
        float: 조정된 가격
    """
    if price <= 0:
        return price
        
    if mode == "down":
        return round_down_to_tick_size(price)
    elif mode == "up":
        return round_up_to_tick_size(price)
    elif mode == "nearest":
        tick_size = get_kr_tick_size(price)
        remainder = price % tick_size
        if remainder >= tick_size / 2:
            return round_up_to_tick_size(price)
        else:
            return round_down_to_tick_size(price)
    else:
        raise ValueError(f"지원하지 않는 모드: {mode}. 지원 모드: down, up, nearest")


def format_kr_price(price: float) -> str:
    """
    국내주식 가격을 포맷팅합니다.
    
    Args:
        price: 포맷팅할 가격
        
    Returns:
        str: 포맷팅된 가격 문자열
    """
    return f"{price:,.0f}원"


def get_kr_tick_size_info(price: float) -> dict:
    """
    국내주식 가격의 호가단위 정보를 반환합니다.
    
    Args:
        price: 기준 가격
        
    Returns:
        dict: 호가단위 정보
    """
    tick_size = get_kr_tick_size(price)
    
    return {
        "price": price,
        "tick_size": tick_size,
        "is_valid": validate_kr_price(price),
        "rounded_down": round_down_to_tick_size(price),
        "rounded_up": round_up_to_tick_size(price),
        "formatted": format_kr_price(price)
    }
