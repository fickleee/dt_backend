import os
import torch
import platform
from tsai.all import *
from tsai.all import load_learner
import pathlib
import logging

# 日志配置（只需在模块顶部配置一次即可）
logger = logging.getLogger(__name__)

plt = platform.system()
if plt == 'Windows':
    pathlib.PosixPath = pathlib.WindowsPath
else:
    pathlib.WindowsPath = pathlib.PosixPath

def model_byStation(trans_data, repo_abs_path):
    # 更新类别定义，对应新的模型架构
    fault_classes = ['正常', '异常']  # fault_detection 模型的输出类别
    anomaly_classes = {
        1: '表面污迹',
        2: '二极管故障', 
        3: '组串开路或短路'  # 包含"组串开路或低效"和"组串短路"
    }
    
    model_folder = os.path.join(repo_abs_path, 'process','diagnose', 'model')
    
    # 加载两个模型
    fault_detection_path = os.path.join(model_folder, "fault_detection.pt")
    anomaly_classifier_path = os.path.join(model_folder, "anomaly_classifier.pt")
    
    # 初始化模型
    fault_detection_model = None
    anomaly_classifier_model = None
    
    try:
        # 获取数据维度（从第一个样本）
        sample_key = list(trans_data.keys())[0]
        sample_data = trans_data[sample_key]
        
        # 加载故障检测模型（二分类）
        fault_detection_model = MiniRocketFeatures(sample_data.shape[1], sample_data.shape[2]).to(default_device())
        fault_detection_model.load_state_dict(torch.load(fault_detection_path, map_location=torch.device('cpu')), strict=True)
        
        # 加载异常分类模型（多分类）
        anomaly_classifier_model = MiniRocketFeatures(sample_data.shape[1], sample_data.shape[2]).to(default_device())
        anomaly_classifier_model.load_state_dict(torch.load(anomaly_classifier_path, map_location=torch.device('cpu')), strict=True)

        logger.info("models loaded successfully: fault_detection and anomaly_classifier")
    except Exception as e:
        logger.error(f"models loading failed: {e}")
        return {}
    
    station_model_result = {}
    
    # 统计变量
    total_strings = len(trans_data)
    normal_count = 0
    anomaly_count = 0
    error_count = 0
    
    # 指定要检查的组串
    # target_strings = ['003-013-006', '002-004-001', '005-009-018', '004-008-002']
    target_strings = []
    
    for key, per_trans_data in trans_data.items():
        try:
            # if key in target_strings:
            #     print(f"\n=== 检查组串 {key} 的输入数据 ===")
            #     print(f"数据形状: {per_trans_data.shape}")
            #     print(f"数据类型: {per_trans_data.dtype}")
            #     print(f"数据范围: [{per_trans_data.min():.4f}, {per_trans_data.max():.4f}]")
            #     print(f"数据均值: {per_trans_data.mean():.4f}")
            #     print(f"数据标准差: {per_trans_data.std():.4f}")
            #     print(f"前10个数据点: {per_trans_data.flatten()[:10]}")
            #     print(f"后10个数据点: {per_trans_data.flatten()[-10:]}")
                
            # 第一步：使用 fault_detection 模型进行二分类
            fault_X_feat = get_minirocket_features(per_trans_data, fault_detection_model, chunksize=64, to_np=True)
            
            # 加载故障检测分类器
            fault_learner_path = os.path.join(model_folder, "fault_detection.pkl")
            fault_learner = load_learner(fault_learner_path)
            
            fault_probas, _, fault_preds = fault_learner.get_X_preds(fault_X_feat)
            normal_prob = fault_probas[0, 0].item()  # 正常概率
            anomaly_prob = fault_probas[0, 1].item()  # 异常概率
            
            # 判定逻辑：正常概率必须大于0.5才判定为正常
            is_normal = normal_prob >= 0.5
            
            preds_result = []
            
            if not is_normal:  # 如果不是正常（即异常）
                anomaly_count += 1
                
                # 第二步：如果检测到异常，使用 anomaly_classifier 进行多分类
                anomaly_X_feat = get_minirocket_features(per_trans_data, anomaly_classifier_model, chunksize=64, to_np=True)
                
                anomaly_learner_path = os.path.join(model_folder, "anomaly_classifier.pkl")
                anomaly_learner = load_learner(anomaly_learner_path)
                
                anomaly_probas, _, anomaly_preds = anomaly_learner.get_X_preds(anomaly_X_feat)
                
                # 获取三种异常类型的概率（对应类别1,2,3）
                anomaly_probs = {
                    1: anomaly_probas[0, 0].item(),  # 表面污迹
                    2: anomaly_probas[0, 1].item(),  # 二极管故障
                    3: anomaly_probas[0, 2].item()   # 组串开路或低效
                }
                
                # 按概率排序异常类型
                sorted_anomalies = sorted(anomaly_probs.items(), key=lambda x: x[1], reverse=True)
                
                # 直接保存各异常类型的概率，不包含正常概率
                for class_id, prob in sorted_anomalies:
                    preds_result.append({
                        "result": anomaly_classes[class_id],
                        "rate": round(prob, 2)
                    })
            else:
                normal_count += 1
                # 如果检测为正常，设置diagnosis_results为空数组
                preds_result = []
            
            # 解析设备信息
            parts = key.split('-')
            station_model_result[key] = {
                "box_id": parts[0],
                "inverter_id": parts[1],
                "string_id": parts[2],
                "diagnosis_results": preds_result
            }
            
        except Exception as e:
            error_count += 1
            logger.error(f"process string_id {key} failed: {e}")
            # 添加默认结果 - 正常组串设置为空数组
            parts = key.split('-')
            station_model_result[key] = {
                "box_id": parts[0],
                "inverter_id": parts[1],
                "string_id": parts[2],
                "diagnosis_results": []
            }
    
    # 打印统计信息
    logger.info(f"\n=== Diagnosis Statistics ===")
    logger.info(f"Total strings: {total_strings}")
    logger.info(f"Normal strings: {normal_count}")
    logger.info(f"Abnormal strings: {anomaly_count}")

    return station_model_result