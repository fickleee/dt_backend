import collections

import pandas as pd
import numpy as np
from sklearn.svm import OneClassSVM
from sklearn.neighbors import LocalOutlierFactor
from sklearn.covariance import EllipticEnvelope
from sklearn.mixture import GaussianMixture
import os
from .save_to_result import update_anomaly_scores, update_degradation_rates, update_anomaly_dates

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
SAVE_DIR = "./data"
# os.path.join(PROJECT_ROOT, "data")

def calc_anomaly_score(dr_inv_list: list[pd.DataFrame], station_name, end_date, repo_abs_path) ->pd.DataFrame:
    all_ano_df = []
    all_ano_ids = []
    all_ano_model = []
    daily_anomaly_status = {}
    all_string_ids = set()  # 用于收集所有组串ID

    for inv_df in dr_inv_list:
        ano_df, ano_model = pd.DataFrame(), {}
        
        string_ids = inv_df['id'].apply(lambda x: x.split('@')[0]).unique()
        all_string_ids.update(string_ids)  # 收集所有组串ID
        dates = inv_df.index.unique()
        date_to_idx = {date: idx for idx, date in enumerate(dates)}
        
        for string_id in string_ids:
            if string_id not in daily_anomaly_status:
                daily_anomaly_status[string_id] = [0] * len(dates)
        
        for label, group in inv_df.groupby('label'):
            X = group.drop(['id'], axis=1)
            group_ano_model = {'ocsvm': None, 'ee': None, 'lof': None}

            try:
                # Elliptic Envelope
                ee_model = EllipticEnvelope(contamination=0.05).fit(X)  # 降低 contamination 参数
                pred_ee = ee_model.predict(X)
                group_ano_model['ee'] = ee_model
                ad_df_ee = group[pred_ee == -1]
            except ValueError as e:
                print(f"Error fitting EllipticEnvelope for group {label}: {e}")
                continue

            # One-Class SVM
            ocsvm_model = OneClassSVM(kernel='rbf', gamma='auto', nu=0.1, max_iter=1000).fit(X)
            pred_ocsvm = ocsvm_model.predict(X)
            group_ano_model['ocsvm'] = ocsvm_model
            ad_df_ocsvm = group[pred_ocsvm == -1]

            # # Elliptic Envelope
            # ee_model = EllipticEnvelope(contamination=0.1).fit(X)
            # pred_ee = ee_model.predict(X)
            # group_ano_model['ee'] = ee_model
            # ad_df_ee = group[pred_ee == -1]

            # Local Outlier Factor
            lof_model = LocalOutlierFactor(n_neighbors=20, algorithm='ball_tree', 
                                         leaf_size=40, n_jobs=-1, contamination=0.1)
            pred_lof = lof_model.fit_predict(X)
            group_ano_model['lof'] = lof_model
            ad_df_lof = group[pred_lof == -1]

            # 修改后的逻辑：只要有任何一个模型检测到异常就记录
            anomaly_dfs = []
            if not ad_df_ocsvm.empty:
                anomaly_dfs.append(ad_df_ocsvm)
            if not ad_df_ee.empty:
                anomaly_dfs.append(ad_df_ee)
            if not ad_df_lof.empty:
                anomaly_dfs.append(ad_df_lof)
            
            if anomaly_dfs:  # 只要有任何异常就合并
                ano_df = pd.concat([ano_df] + anomaly_dfs, axis=0)
            
            ano_model[label] = group_ano_model

            # 更新每日异常状态
            for _, row in group.iterrows():
                date = row.name 
                day_idx = date_to_idx[date] 
                string_id = row['id'].split('@')[0] 
                
                is_anomaly = (row['id'] in ad_df_ocsvm['id'].values or 
                            row['id'] in ad_df_ee['id'].values or 
                            row['id'] in ad_df_lof['id'].values)
                
                if is_anomaly:
                    daily_anomaly_status[string_id][day_idx] = 1

        if not ano_df.empty:
            all_ano_df.append(ano_df)
            all_ano_ids.extend(ano_df['id'].tolist())
            all_ano_model.append(ano_model)

    ano_static = collections.Counter([k.split('@')[0] for k in all_ano_ids])
    
    # 确保所有组串都有记录，没有异常的赋值为0
    for string_id in all_string_ids:
        if string_id not in ano_static:
            ano_static[string_id] = 0
    
    # 转换为DataFrame并排序
    ano_st_df = pd.DataFrame(list(ano_static.items()), columns=['pid', 'count'])
    ano_st_df = ano_st_df.sort_values(by='count', ascending=False)

    # 更新异常分数
    save_dir = os.path.join(repo_abs_path, 'data', station_name, "results")
    file_path = os.path.join(save_dir, f"{end_date}.json")
    update_anomaly_scores(file_path, ano_st_df.to_dict('records'))
    update_anomaly_dates(file_path, daily_anomaly_status)

    return ano_st_df
    

def calc_deg_score(dr_inv_list, ano_st_df, station_name, end_date, repo_abs_path):
    inv_deg_dict = dict()
    threshold = ano_st_df["count"].mean() + 3*ano_st_df["count"].std()
    ano_threshold_id_list = list(ano_st_df[ano_st_df["count"]>=threshold]["pid"].values)

    for inv_df in dr_inv_list:
        inv_df["pid"] = inv_df['id'].apply(lambda x: x.split('@')[0])
        normal_inv_df = inv_df[~inv_df['pid'].isin(ano_threshold_id_list)].drop(columns=['pid'])
        gmm_dict = dict()   
        inv_bd_df = pd.DataFrame()
        inv_deg_df = pd.DataFrame()

        for label, group in normal_inv_df.groupby('label'):
            normal_ids = set(group["id"].apply(lambda x: x.split('@')[0]).values)
            X = group.drop(['label', 'id'], axis=1).values

            best_bic = np.inf 
            best_gmm = None
            for nc in range(len(normal_ids), 0, -1):
                gmm = GaussianMixture(n_components=nc, random_state=42).fit(X)
                bic = gmm.bic(X)
                if bic < best_bic:
                    best_bic = bic 
                    best_gmm = gmm
            gmm_dict[label] = best_gmm

            group["result"] = gmm.score_samples(X)
            bd_df = group.groupby(group.index)["result"].min().reset_index(name='boundary')
            inv_bd_df = pd.concat([inv_bd_df, bd_df], ignore_index=True, axis=0)
        
        inv_bd_df = inv_bd_df.set_index("index").sort_index()

        for label, group in inv_df.groupby('label'):
            X = group.drop(['label', 'id', 'pid'], axis=1).values
            gmm = gmm_dict[label]
            group["bd_val"] = gmm.score_samples(X)
            group['boundary'] = group.index.map(lambda idx: inv_bd_df.loc[idx, 'boundary'] if idx in inv_bd_df.index else None)
            deg_df = group[["id", "x", "y", "label", "bd_val", "boundary"]]
            inv_deg_df = pd.concat([inv_deg_df, deg_df], ignore_index=True, axis=0)
            
        
        scale_range = pd.concat([inv_deg_df["bd_val"], inv_deg_df["boundary"]])
        d_min, d_max = scale_range.min(), scale_range.max()
        inv_deg_df["bd_val_scaled"] = (inv_deg_df['bd_val'] - d_min) / (d_max - d_min)
        inv_deg_df["boundary_scaled"] = (inv_deg_df['boundary'] - d_min) / (d_max - d_min)
        inv_deg_df["deg"] = inv_deg_df.apply(lambda row: (row["boundary_scaled"] - row["bd_val_scaled"]) / row["boundary_scaled"]
                                            if row["bd_val_scaled"] < row["boundary_scaled"] else 0, axis=1)
        inv_deg_df['pid'] = inv_deg_df['id'].apply(lambda x: x.split('@')[0])
        inv_deg_dict = inv_deg_dict | inv_deg_df.groupby('pid')['deg'].mean().to_dict()

    deg_st_df = pd.DataFrame(list(inv_deg_dict.items()), columns=["pid", "deg"])
    # 更新劣化率
    save_dir = os.path.join(repo_abs_path, 'data', station_name, "results")
    file_path = os.path.join(save_dir, f"{end_date}.json")
    update_degradation_rates(file_path, inv_deg_dict)

    return deg_st_df 