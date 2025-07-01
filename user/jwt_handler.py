import jwt
import datetime
from datetime import timedelta

# JWT配置
JWT_SECRET = "datang_solar_power_secret_key_2025_falldeep"  # 自定义的密钥
JWT_ALGORITHM = "HS256"  # 加密算法
JWT_EXPIRATION_DELTA = datetime.timedelta(days=1)  # Token有效期为1天

def generate_token(username: str, user_type: str) -> str:
    """
    生成JWT token
    :param username: 用户名
    :param user_type: 用户类型
    :return: JWT token
    """
    payload = {
        'username': username,
        'user_type': user_type,
        'exp': datetime.datetime.utcnow() + JWT_EXPIRATION_DELTA,
        'iat': datetime.datetime.utcnow()
    }
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)

def verify_token(token: str) -> dict:
    """
    验证JWT token
    :param token: JWT token
    :return: 解码后的payload
    """
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
        return {'valid': True, 'data': payload}
    except jwt.ExpiredSignatureError:
        return {'valid': False, 'message': 'Token已过期'}
    except jwt.InvalidTokenError:
        return {'valid': False, 'message': 'Token无效'}