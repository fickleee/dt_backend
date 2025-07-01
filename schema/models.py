# File: backend/database/models.py
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Float, Boolean, BigInteger, Index

Base = declarative_base()

# 动态模型生成工厂

def create_station_models(station_prefix):
    # 动态生成唯一类名
    StationInfo = type(
        f"{station_prefix.capitalize()}StationInfo",
        (Base,),
        {
            "__tablename__": f"{station_prefix}StationInfo",
            "timestamp": Column(BigInteger, primary_key=True),
            "irradiance": Column(Float),
            "temperature": Column(Float),
            "power": Column(Float),
            "is_valid": Column(Boolean, default=True),
            "__table_args__": (
                Index('idx_station_ts', 'timestamp'),
                {'mysql_engine': 'InnoDB'}
            ),
        }
    )

    InverterInfo = type(
        f"{station_prefix.capitalize()}InverterInfo",
        (Base,),
        {
            "__tablename__": f"{station_prefix}InverterInfo",
            "timestamp": Column(BigInteger, primary_key=True),
            "device_id": Column(String(50), primary_key=True),
            "inverter_id": Column(String(50)),
            "box_id": Column(String(50)),
            "intensity": Column(Float),
            "voltage": Column(Float),
            "power": Column(Float),
            "generated_energy": Column(Float),
            "sum_energy": Column(Float),
            "month_energy": Column(Float),
            "temperature": Column(Float),
            "sig_overvoltage": Column(Integer),
            "sig_undervoltage": Column(Integer),
            "sig_overfrequency": Column(Integer),
            "sig_underfrequency": Column(Integer),
            "sig_gridless": Column(Integer),
            "sig_imbalance": Column(Integer),
            "sig_overcurrent": Column(Integer),
            "sig_midpoint_grounding": Column(Integer),
            "sig_insulation_failure": Column(Integer),
            "sig_excessive_DC": Column(Integer),
            "sig_arc_self_protection": Column(Integer),
            "sig_arc_failure": Column(Integer),
            "is_valid": Column(Boolean, default=True),
            "__table_args__": (
                Index(f'idx_ivt_td', 'timestamp', 'device_id'),
                {'mysql_engine': 'InnoDB'}
            ),
        }
    )

    StringInfo = type(
        f"{station_prefix.capitalize()}StringInfo",
        (Base,),
        {
            "__tablename__": f"{station_prefix}StringInfo",
            "timestamp": Column(BigInteger, primary_key=True),
            "device_id": Column(String(50), primary_key=True),
            "string_id": Column(String(50)),
            "inverter_id": Column(String(50)),
            "box_id": Column(String(50)),
            "intensity": Column(Float),
            "voltage": Column(Float),
            "fixed_intensity": Column(Float),
            "fixed_voltage": Column(Float),
            "is_valid": Column(Boolean, default=True),
            "__table_args__": (
                Index(f'idx_str_td', 'timestamp', 'device_id'),
                {'mysql_engine': 'InnoDB'}
            ),
        }
    )

    return StationInfo, InverterInfo, StringInfo

# 填补统计模型
def create_impute_model(station_prefix):
    StringOverview = type(
        f"{station_prefix.capitalize()}StringOverview",
        (Base,),
        {
            "__tablename__": f"{station_prefix}StringOverview",
            "timestamp": Column(BigInteger, primary_key=True),
            "device_id": Column(String(50), primary_key=True),
            "error_count_intensity": Column(Integer),
            "missing_count_intensity": Column(Integer),
            "error_count_voltage": Column(Integer),
            "missing_count_voltage": Column(Integer),
            "__table_args__": (
                Index(f'idx_impute_td', 'timestamp', 'device_id'),
                {'mysql_engine': 'InnoDB'}
            ),
        }
    )
    return StringOverview

def create_user_model():
    """
    创建用户模型
    """
    class UserInfo(Base):
        __tablename__ = 'UserInfo'
        user_name = Column(String(50), primary_key=True)
        user_type = Column(String(50))
        user_password = Column(String(128))  # 建议存储哈希值
        user_email = Column(String(100))
        user_phone = Column(String(20))
        user_validated = Column(Boolean, default=False)
        __table_args__ = (
            Index('idx_user_email', 'user_email'),
            {'mysql_engine': 'InnoDB'}
        )
    return UserInfo

# 功率损失和预测模型
def create_power_models(station_prefix):
    class PowerLoss(Base):
        __tablename__ = f"{station_prefix}PowerLoss"
        date = Column(String(10), primary_key=True)
        box_id = Column(String(50), primary_key=True)
        inverter_id = Column(String(50), primary_key=True)
        string_id = Column(String(50), primary_key=True)
        power_loss = Column(Float)
        __table_args__ = (
            Index(f'idx_powerloss_date', 'date'),
            {'mysql_engine': 'InnoDB'}
        )

    class PowerPrediction(Base):
        __tablename__ = f"{station_prefix}PowerPrediction"
        date = Column(String(10), primary_key=True)
        box_id = Column(String(50), primary_key=True)
        inverter_id = Column(String(50), primary_key=True)
        string_id = Column(String(50), primary_key=True)
        predicted_loss = Column(Float)
        __table_args__ = (
            Index(f'idx_powerpred_date', 'date'),
            {'mysql_engine': 'InnoDB'}
        )

    return PowerLoss, PowerPrediction