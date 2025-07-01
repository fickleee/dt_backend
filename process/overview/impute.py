import os
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import func
import logging

# 日志配置（只需在模块顶部配置一次即可）
logger = logging.getLogger(__name__)

def get_impute_info(start_timestamp, database_manager, station_name, impute_model):
    try:
        with database_manager.get_session(station_name) as session:
            # 查询指定时间戳下所有 device_id 的 error_count_intensity 和 error_count_voltage
            rows = session.query(
                impute_model.device_id,
                impute_model.error_count_intensity,
                impute_model.error_count_voltage
            ).filter(impute_model.timestamp == start_timestamp).all()

            # 对每个 device_id，取 intensity 和 voltage 的较大者
            max_list = [max(row.error_count_intensity or 0, row.error_count_voltage or 0, 0) for row in rows]

            # 求和，计算错误率
            error_rate = sum(max_list) / (24 * len(max_list)) if max_list else 0
            return error_rate

    except SQLAlchemyError as e:
        logger.error(f"Error occurred while getting impute info: {e}")
        return 0