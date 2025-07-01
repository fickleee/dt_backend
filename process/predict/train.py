import sqlite3 as sql
import pandas as pd
import os
import numpy as np
import torch
import torch.nn as nn
import torch.optim as optim
from sklearn.model_selection import train_test_split
from torch.utils.data import TensorDataset, DataLoader
import json

# 读取数据库
def read_database(database_path, station_name):
    conn = sql.connect(database_path)
    cursor = conn.cursor()
    # 使用新的功率计算语句执行查询
    cursor.execute(f"""
        SELECT 
            datetime(timestamp, 'unixepoch', '+8 hours') AS date, 
            device_id, 
            intensity * voltage / 6 AS power 
        FROM {station_name}StringInfo;
    """)
    data = cursor.fetchall()
    columns = [column[0] for column in cursor.description]
    df = pd.DataFrame(data, columns=columns)
    conn.close()

    # 将 'date' 列转换为 datetime 类型
    df['date'] = pd.to_datetime(df['date'])
    # 将日期设为索引
    df.set_index('date', inplace=True)
    # 将 DataFrame 转换为 pivot 表格格式，并填充缺失值
    pivot_df = df.pivot(columns='device_id', values='power')
    # 将所有 device_id 列转为 float 类型
    pivot_df = pivot_df.astype(float)

    return pivot_df

# 读取电站信息
def read_station_info_database(database_path, station_name):
    conn = sql.connect(database_path)
    cursor = conn.cursor()
    # 使用新的功率计算语句执行查询
    cursor.execute(f"""
        SELECT
            datetime(timestamp, 'unixepoch', '+8 hours') AS date,
            irradiance
        FROM {station_name}StationInfo;
    """)
    data = cursor.fetchall()
    columns = [column[0] for column in cursor.description]
    df = pd.DataFrame(data, columns=columns)
    conn.close()

    # 将 'date' 列转换为 datetime 类型
    df['date'] = pd.to_datetime(df['date'])

    return df

# 定义损失函数
class AverageDifferenceLoss(nn.Module):
    def __init__(self):
        super(AverageDifferenceLoss, self).__init__()

    def forward(self, tensor1, tensor2):
        # 确保两个tensor的形状相同
        if tensor1.shape != tensor2.shape:
            raise ValueError("The shapes of the two tensors must be the same.")

        # 计算差的平方
        difference_square = (tensor1 - tensor2) ** 2

        # 计算平方差的平均值
        average_difference = torch.mean(difference_square)

        # 返回平均差值
        return average_difference

# 定义 MLP 模型
class MLP(nn.Module):
    def __init__(self, input_size, hidden_size1, hidden_size2, hidden_size3, output_size):
        super(MLP, self).__init__()
        # 定义第一层全连接层
        self.fc1 = nn.Linear(input_size, hidden_size2)
        # 定义第三层全连接层
        self.fc3 = nn.Linear(hidden_size2, hidden_size3)
        # 定义第四层全连接层
        self.fc4 = nn.Linear(hidden_size3, output_size)
        # 定义ReLU激活函数
        self.relu = nn.ReLU()

    def forward(self, x):
        # 前向传播
        x = self.relu(self.fc1(x))
        x = self.relu(self.fc3(x))
        x = self.fc4(x)  # 输出层不需要激活函数
        return x

def calculate_ratio(group_mean, group_diff, group_data):
    if group_diff <= 0.005:
        # 如果diff小于或等于0.01，ratio为mean除以0.088425
        return group_mean / 0.088425
    else:
        # 如果diff大于0.01，找出value大于mean的点求均值
        over_mean_data = group_data[group_data['value'] > group_mean]
        if not over_mean_data.empty:
            return over_mean_data['value'].mean() / 0.088425
        else:
            # 如果没有value大于mean的点，可以选择返回NaN或者其他适当的值
            return 0.0

def check_condition(row, group_mean_diff):
    group = row['Group']
    group_mean = group_mean_diff[group]['mean']
    group_diff = group_mean_diff[group]['diff']

    # 检查条件：group_diff <= 0.01 或 value >= group_mean
    # 条件要求组内的四分位距小于或等于0.01，或者当前值大于等于组内的均值
    condition = (group_diff <= 0.005) or (row['value'] >= group_mean)
    return condition

def main():
    # 设置路径和参数
    database_path = os.path.join(database_dir, f'{station_name}.db')
    device = 'cuda' if torch.cuda.is_available() else 'cpu'
    
    # 全局参数字典
    global_params = dict()
    
    # Step1: 计算先天因素（ratio）
    print("Step1: 计算先天因素（ratio）")
    
    # 读取数据库
    df = read_database(database_path, station_name)
    data = df.fillna(0)
    
    # 将所有小于0的值设置为0
    data = data.clip(lower=0)
    
    # 计算全局最小值和最大值
    global_min = data.min().min()  # 取所有数值列的最小值中的最小值
    global_max = data.max().max()  # 取所有数值列的最大值中的最大值
    
    global_params['power'] = dict()
    global_params['power']['min'] = global_min
    global_params['power']['max'] = global_max
    
    # 对所有数值列进行归一化
    for column in data.columns:
        if data[column].dtype.kind in 'biufc':  # 检查是否为数值类型
            data[column] = (data[column] - global_min) / (global_max - global_min)
    
    print(f"全局最小值: {global_min}")
    print(f"全局最大值: {global_max}")
    
    # 计算每个ID的平均值
    df = pd.DataFrame(data.mean()).reset_index()
    df.columns=['ID','value']
    # 将ID列分割成两部分
    df['Group'] = df['ID'].str.extract(r'^(\d{3}-\d{3})-\d{3}')
    df['BT'] = df['ID'].str.extract(r'^(\d{3})-\d{3}-\d{3}')
    df['IV'] = df['ID'].str.extract(r'^\d{3}-(\d{3})-\d{3}')
    
    # 计算每个组的统计量
    grouped_stats = df.groupby('Group')['value'].agg(['mean', 'count'])
    q1 = df.groupby('Group')['value'].quantile(q=0.25)  # 计算四分位数
    q3 = df.groupby('Group')['value'].quantile(q=0.75)
    
    grouped_stats['25']=q1
    grouped_stats['75']=q3
    
    grouped_stats['diff'] = grouped_stats['75'] - grouped_stats['25']  # 计算四分位距
    
    # 按mean降序和diff升序排序
    grouped_stats_sorted = grouped_stats.sort_values(by=['mean', 'diff'], ascending=[False, True]).reset_index()
    
    # 应用函数计算每个Group的ratio
    grouped_stats_sorted['ratio'] = grouped_stats_sorted.apply(
        lambda row: calculate_ratio(row['mean'], row['diff'], df[df['Group'] == row['Group']]), 
        axis=1
    )
    
    # 转换为字典便于查找
    group_mean_diff = grouped_stats_sorted.set_index('Group').to_dict(orient='index')
    
    # 应用条件函数
    df['condition'] = df.apply(lambda row: check_condition(row, group_mean_diff), axis=1)
    
    # 筛选出condition为True的ID
    ids_with_true_condition = df[df['condition']]['ID']
    
    # 使用这些ID筛选数据
    df_power_filtered = data[ids_with_true_condition.tolist()]
    
    # 读取电站信息
    df_irradiation = read_station_info_database(database_path, station_name)
    
    # 筛选9点到18点之间的数据
    df_irradiation_filtered = df_irradiation[
        (df_irradiation['date'].dt.hour >= 9) &
        (df_irradiation['date'].dt.hour < 18)
    ]
    
    # 转换为字典
    grouped_stats_dict = grouped_stats_sorted.set_index('Group').to_dict()
    global_params['ratio'] = grouped_stats_dict['ratio']
    
    # 转换为长格式
    df_power_filtered_long = pd.melt(df_power_filtered.reset_index(), id_vars='date', var_name='ID', value_name='Power_Value')
    df_power_filtered_long['date'] = pd.to_datetime(df_power_filtered_long['date'])
    
    # 合并数据
    df_final = df_irradiation_filtered.iloc[:,:].merge(
        df_power_filtered_long,
        on='date',
        how='left'
    ).merge(
        df[['ID', 'Group']],
        on='ID',
        how='left'
    ).merge(
        grouped_stats_sorted[['Group','ratio']],
        left_on='Group',
        right_on='Group',
        how='left'
    )
    
    # 提取小时特征并归一化
    df_final['hour'] = pd.to_datetime(df_final['date']).dt.hour / 23
    
    irradiance_min = df_final['irradiance'].min()
    irradiance_max = df_final['irradiance'].max()
    global_params['irradiance'] = dict()
    global_params['irradiance']['min'] = irradiance_min
    global_params['irradiance']['max'] = irradiance_max
    
    df_final['irradiance'] = (df_final['irradiance'] - irradiance_min) / (irradiance_max - irradiance_min)
    
    # 找到每个 'date' 和 'Group' 组合中 'Power_Value' 最大的行
    idx_max_power = df_final.groupby(['date', 'Group'])['Power_Value'].idxmax()
    data_filtered = df_final.loc[idx_max_power].reset_index(drop=True)
    df_final = data_filtered
    
    # Step2: 构建网络，训练
    print("Step2: 构建网络，训练")
    
    # 准备训练数据
    X = df_final[['hour', 'ratio', 'irradiance']].values
    y = df_final['Power_Value'].values
    
    # 划分训练集和测试集
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=test_size, random_state=42)
    
    # 转换为 PyTorch 张量
    X_train_tensor = torch.tensor(X_train, dtype=torch.float32).to(device)
    y_train_tensor = torch.tensor(y_train, dtype=torch.float32).to(device)
    X_test_tensor = torch.tensor(X_test, dtype=torch.float32).to(device)
    y_test_tensor = torch.tensor(y_test, dtype=torch.float32).to(device)
    
    # 创建 DataLoader
    train_dataset = TensorDataset(X_train_tensor, y_train_tensor)
    train_loader = DataLoader(dataset=train_dataset, batch_size=batch_size, shuffle=True)
    
    test_dataset = TensorDataset(X_test_tensor, y_test_tensor)
    test_loader = DataLoader(dataset=test_dataset, batch_size=batch_size, shuffle=False)
    
    # 设置模型参数
    input_size = 3
    hidden_size1 = 32
    hidden_size2 = 64
    hidden_size3 = 32
    output_size = 1
    
    # 初始化模型
    model = MLP(input_size, hidden_size1, hidden_size2, hidden_size3, output_size).to(device)
    criterion = AverageDifferenceLoss()
    optimizer = optim.Adam(model.parameters(), lr=learning_rate)
    
    # 训练模型
    for epoch in range(num_epochs):
        model.train()
        running_loss = 0.0
        
        for batch_idx, (data, target) in enumerate(train_loader):
            optimizer.zero_grad()
            
            outputs = model(data)
            outputs = outputs.view(-1)
            loss = criterion(outputs, target)
            
            loss.backward()
            optimizer.step()
            
            running_loss += loss.item() * data.size(0)
        
        epoch_loss = running_loss / len(train_loader.dataset)
        
        # 评估模型
        model.eval()
        wmape_total_absolute_error = 0.0
        wmape_total_actual_sum = 0.0
        
        with torch.no_grad():
            running_loss_eval = 0.0
            
            for data_batch, target_batch in test_loader:
                data_batch = data_batch.to(device)
                target_batch = target_batch.to(device)
                
                outputs = model(data_batch)
                outputs = outputs.view(-1)
                target_clean = target_batch.view(-1)
                
                loss = criterion(outputs, target_clean)
                
                wmape_total_absolute_error += torch.abs(target_clean - outputs).sum().item()
                wmape_total_actual_sum += target_clean.sum().item()
                
                shape = outputs.shape
                zero_tensor = torch.zeros(shape, device=device)
                one_tensor = torch.ones(shape, device=device)
                
                zero_l = criterion(zero_tensor, target_clean)
                one_l = criterion(one_tensor, target_clean)
                result = torch.max(zero_l, one_l)
                
                running_loss_eval += loss.item() * data_batch.size(0)
            
        epoch_loss_eval = running_loss_eval / len(test_loader.dataset)
        epoch_acc_eval = 1.0 - epoch_loss_eval
        
        if wmape_total_actual_sum == 0:
            wmape_epoch_value = float('inf')
        else:
            wmape_epoch_value = (wmape_total_absolute_error / wmape_total_actual_sum) * 100
        
        if (epoch + 1) % 10 == 0:
            print(f'Epoch [{epoch+1}/{num_epochs}]')
            print(f'Training Loss: {epoch_loss:.4f}')
            print(f'Evaluation Loss: {epoch_loss_eval:.4f}')
            print(f'Evaluation Acc: {epoch_acc_eval:.4f}')
            print(f'Evaluation WMAPE: {wmape_epoch_value:.2f}%')
    
    # 保存模型
    save_dir = os.path.join(models_dir, station_name)
    os.makedirs(save_dir, exist_ok=True)
    model_save_path = os.path.join(save_dir, 'mlp_model.pth')
    params_save_path = os.path.join(save_dir, 'mlp_params.json')
    
    torch.save(model.state_dict(), model_save_path)
    with open(params_save_path, 'w') as f:
        json.dump(global_params, f, indent=4)
    
    print(f"模型已保存至 {model_save_path}")
    print(f"参数已保存至 {params_save_path}")

if __name__ == "__main__":
    # 直接在代码中设置参数
    # 修改这些参数以适应不同的训练需求
    station_name = 'wushashan'  # 电站名称
    database_dir = 'database'   # 数据库目录
    models_dir = 'models'       # 模型保存目录
    num_epochs = 500           # 训练轮数
    batch_size = 1024          # 批次大小
    learning_rate = 1e-5       # 学习率
    test_size = 0.2            # 测试集比例

    main()