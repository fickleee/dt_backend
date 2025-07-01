from process.merge.index import create_log, merge_log
from process.preprocess.index import get_basis_info, preprocess_log, get_basis_info_manual,check_timestamp_is_exist, check_history_timestamp_is_exist
from process.impute.index import impute_schedule_bulk, load_impute_models
from process.detect.index import detect_schedule, detect_schedule_orm
from process.detect.utils import get_time_range
from process.predict.index import predict_schedule
from process.diagnose.index import diagnosis_schedule, diagnosis_schedule_orm
from process.overview.index import post_schedule, overview_process
from process.overview.utils import get_token
import logging
import os
from concurrent.futures import ThreadPoolExecutor
import time

# 日志配置（只需在模块顶部配置一次即可）
logger = logging.getLogger(__name__)

def process_create(process_date, station_name, repo_abs_path,start_timestamp,end_timestamp):
    database_path = os.path.join(repo_abs_path, 'database', '{}.db'.format(station_name))
    data_dir_path = os.path.join(repo_abs_path, 'data')
    create_log(process_date, station_name, database_path, data_dir_path, start_timestamp,end_timestamp)

def process_merge(process_date, station_name, repo_abs_path):
    data_dir_path = os.path.join(repo_abs_path, 'data')
    merge_dir_path = os.path.join(repo_abs_path, 'merge')
    merge_log(process_date, station_name, data_dir_path, merge_dir_path)

def process_impute_global(process_date, station_name, repo_abs_path, database_manager, station_model, model_dict, impute_model, position=0):
    # impute_schedule(process_date, station_name, repo_abs_path, database_manager, station_model)
    impute_schedule_bulk(process_date, station_name, repo_abs_path, database_manager, station_model, model_dict, impute_model, position)

def process_detect(process_date, station_name, repo_abs_path, time_window, database_manager, station_model):
    # detect_schedule(station_name, process_date, repo_abs_path)
    detect_schedule_orm(station_name, process_date, repo_abs_path, time_window, database_manager, station_model)

def process_predict(process_date, station_name, repo_abs_path, database_manager, station_models):
    predict_schedule(process_date=process_date, repo_abs_path=repo_abs_path, database_manager=database_manager, station_models=station_models, station_name=station_name)

def process_diagnose(process_date, station_name, repo_abs_path, database_manager, station_model):
    # diagnosis_schedule(process_date, station_name, repo_abs_path)
    diagnosis_schedule_orm(process_date, station_name, repo_abs_path, database_manager, station_model)

def process_postprocess(start_timestamp, end_timestamp, repo_abs_path, database_manager, station_model, station_name, process_date, kairosdb_url, impute_model=None, token=None):
    post_schedule(start_timestamp, end_timestamp, repo_abs_path, database_manager, station_model, station_name, process_date, kairosdb_url, impute_model, token)

def run_process_schedule(kairosdb_url, repo_abs_path,time_window, database_manager, station_models, impute_models, station_list=None, api_user=None, api_password=None):
    yesterday_date, yesterday_start_timestamp, yesterday_end_timestamp, _ = get_basis_info(repo_abs_path=repo_abs_path)
    model_dict = load_impute_models(repo_abs_path)

    token = get_token(api_user, api_password) 

    with ThreadPoolExecutor() as executor:
        # 1. 并行执行 preprocess_log，并计算执行时间
        logger.info("start preprocess")
        start_time = time.time()
        preprocess_futures = [
            executor.submit(preprocess_log, yesterday_start_timestamp, yesterday_end_timestamp, station_name,
                            kairosdb_url, repo_abs_path, database_manager, station_models[station_name]) for station_name in station_list]
        
        for future in preprocess_futures:
            future.result()
        end_time = time.time()
        logger.info(f"preprocess completed in {end_time - start_time:.2f} seconds")

        # 2. 并行执行 process_impute_global，并计算执行时间
        logger.info("start impute")
        start_time = time.time()

        impute_futures = [executor.submit(process_impute_global, yesterday_date, station_name, repo_abs_path, database_manager, station_models[station_name], 
                            model_dict, impute_models[station_name], position=idx) for idx, station_name in enumerate(station_list)]

        for future in impute_futures:
            future.result()

        end_time = time.time()
        logger.info(f"impute completed in {(end_time - start_time)/len(station_list):.2f} seconds")

        # 3. 并行执行 process_predict，并计算执行时间（该步骤会创建json或更新json）
        logger.info("start predict")
        start_time = time.time()

        predict_futures = [executor.submit(process_predict, yesterday_date, station_name, repo_abs_path, database_manager, station_models) for
                          station_name in station_list]
        
        for future in predict_futures:
            future.result()

        end_time = time.time()
        logger.info(f"predict completed in {(end_time - start_time)/len(station_list):.2f} seconds")

        # 4. 并行执行 process_merge （该步骤不计算时间）
        logger.info("start merge")
        merge_futures = [executor.submit(process_merge, yesterday_date, station_name, repo_abs_path) for
                          station_name in station_list]

        # 等待所有 process_merge 任务完成
        for future in merge_futures:
            future.result()
        logger.info("merge completed")

        # 5. 并行执行 process_diagnose，并计算执行时间
        logger.info("start diagnose")
        start_time = time.time()

        diagnose_futures = [executor.submit(process_diagnose, yesterday_date, station_name, repo_abs_path, database_manager, station_models[station_name]) for
                         station_name in station_list]
        
        for future in diagnose_futures:
            future.result()
        end_time = time.time()
        logger.info(f"diagnose completed in {(end_time - start_time)/len(station_list):.2f} seconds")

        # 6. 并行执行 process_detect，并计算执行时间
        # 劣化率计算方式修改，暂时取消detect定时器
        # logger.info("start detect")
        # start_time = time.time()
        # detect_futures = [executor.submit(process_detect, yesterday_date, station_name, repo_abs_path, time_window, database_manager, station_models[station_name]) for
        #             station_name in station_list]
        
        # for future in detect_futures:
        #     future.result()
        # end_time = time.time()
        # logger.info(f"detect completed in {(end_time - start_time)/len(station_list):.2f} seconds")

        # 7. 并行执行 process_postprocess，并计算执行时间
        logger.info("start postprocess")
        start_time = time.time()
        postprocess_futures = [executor.submit(process_postprocess, yesterday_start_timestamp, yesterday_end_timestamp, repo_abs_path, database_manager, station_models[station_name],
                                  station_name, yesterday_date, kairosdb_url, impute_model=impute_models[station_name], token=token) for
                               station_name in station_list]
        for future in postprocess_futures:
            future.result()
        end_time = time.time()
        logger.info(f"postprocess completed in {(end_time - start_time)/len(station_list):.2f} seconds")

    # 8. 执行 overview_process，进行数据汇总
    overview_process(repo_abs_path, yesterday_date, station_list)
        

if __name__ == '__main__':
    station_name = "datu"
    process_date = "2025-03-31"
    repo_abs_path = r"d:\DaTang\datangSystemBackend\datangbackend"  # 当前项目路径
    
    print(f"开始测试 {station_name} 电站 {process_date} 的故障诊断功能")
    
    try:
        process_diagnose(process_date, station_name, repo_abs_path)
        print("故障诊断测试完成")
    except Exception as e:
        print(f"故障诊断测试失败: {str(e)}")
    
    # run_process_manual("datu")