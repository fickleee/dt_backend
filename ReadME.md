## 后端启动说明
### 步骤
1. 安装依赖库 `pip install -r requirements.txt`
2. 运行
	1. 本地开发模式：`python app.py`
	2. 本地部署模式（暂定）：`export APP_ENV=local && python app.py`
	3. 大唐部署模式（暂定）：`export APP_ENV=production && python app.py`
> 备注：export指令仅在 linux 下有效；非显式设置APP_ENV时，默认为development
### 环境变量说明
#### Part1 命令行参数
1. `APP_ENV`：环境变量，可选值有`development`、`local`、`production`，分别表示本地开发、本地部署（docker）和大唐部署模式。
#### Part2 配置文件
> 备注：在 `./setting/`下有三个模式对应的配置文件
1. `DB_TYPE`：数据库类型，可选值有`sqlite`、`mariadb`，分别表示使用sqlite、mariadb数据库。其中，仅在本地开发模式中可选 `sqlite`（前期测试使用），其它模式仅可选 `mariadb`（正式使用）
	1. `SQLITE_DIR`：数据库文件存放路径（for sqlite）
2. `MARIADB_HOST`：mariadb 数据库地址
3. `MARIADB_POST`：mariadb 数据库端口号
4. `MARIADB_USER`：mariadb 数据库用户名
5. `MARIADB_PASSWORD`：mariadb 数据库密码
6. `MARIADB_SCHEMA`：mariadb 数据库名
7. `KAIROSDB_URL`：kairosdb 地址
8. `TIMEWINDOW`：时间窗口尺寸
9. `STATION_LIST`: 场站列表
#### Part3 项目中的全局变量
> 备注：在 `app.py`中定义
1. `global_repo_abs_path`: 项目根目录的绝对路径
2. `global_kairosdb_url`：kairosdb 地址，读取自配置文件中的 KAIROSDB_URL
3. `global_time_window`：时间窗口尺寸，读取自配置文件中的 TIMEWINDOW
4. `global_db_manager`：数据库管理器，数据库的类型由配置文件决定
5. `global_station_list`：场站列表，读取自配置文件中的 STATION_LIST
6. `global_station_models`: 代表各场站的数据表模型，以字典的形式存放，键为场站名，值为各场站对应的 `{}StationInfo`、`{}InverterInfo`、`{}StationInfo`表
7. `global_impute_models`: 代表各场站的impute表模型，以字典的形式存放，键为场站名，值为各场站对应的 impute表
8. `global_user_model`: 代表用户表模型
## 数据库使用指南
本文档为模块开发人员提供了与项目中的数据库交互的指导。它包括如何使用 `DatabaseManager` 和在 `schema` 目录中定义的 ORM 模型的说明。提供的示例将帮助开发人员在模块中集成数据库操作。
### 背景知识
#### sqlite数据库
1. 所有的db文件位于 `./database`下
2. 有哪些数据库？
	1. 每个场站对应两个数据库（以 `datu`为例）
		1. `datu.db`：包含三张表，`datuStationInfo`、`datuInverterInfo`和`datuStringInfo`
		2. `datu_impute.db`：包含一张表，`datuStringOverview`
	2. `user.db`：包含一张表，`UserInfo`
3. 若有7个场站，则应有 `7*2+1=15`个 `.db`文件，有 `7*(3+1)+1=29`张数据表
#### mariadb数据库
1. 连接配置在 `./setting`下的配置文件中
2. 所有场站的表和用户表都在同一个数据库中，该数据库下有 29 张数据表（假设有 7 个场站）
3. ⚠️⚠️⚠️mariadb数据库的值**不允许出现nan值**（当使用numpy或者dataframe进行数据处理时），即使该字段允许为空。在插入数据时，请务必将nan值替换为None。
### 初始化数据库
在 `app.py` 中，已经初始化了一个全局的 `DatabaseManager` 并加载了所有的数据表模型：
```py app.py
global_database_manager = DatabaseManager(global_repo_abs_path)

global_station_models = {station_name: create_station_models(station_name) for station_name in global_station_list}
global_impute_models = {station_name: create_impute_model(station_name) for station_name in global_station_list}
global_user_model = create_user_model()
```
请注意：
1. `global_station_models`中，以键值对的形式存储了所有场站对应的`{}StationInfo`、`{}InverterInfo`和 `{}StringInfo`表。现假设，你想获取`datu`的三张表，代码如下：
```python
station_name = 'datu'
datu_station_info, datu_inverter_info, datu_string_info = global_station_models.get(station_name)
```
2. `global_impute_models`中，以键值对的形式存储了所有场站对应的 `{}StringOverview`表。现假设，你想获取 `datu`的对应表，代码如下：
```python
station_name = 'datu'
datu_string_overview = global_impute_models.get(station_name)
```
3. 用户表 `global_user_model` 直接使用即可
### 在模块中使用数据库

在模块开发过程中，你应该将 `DatabaseManager` 、**相关的数据库名称(详见备注)** 以及相关的 ORM 模型传递给你的函数。以下是具体步骤：
1. **传递 `DatabaseManager` 、数据库名称和模型**：在调用执行数据库操作的函数时，传递 `global_database_manager` 、数据库名称以及所需的具体模型。
2. **使用会话进行数据库事务**：利用 `DatabaseManager` 提供的会话来执行 CRUD（创建、读取、更新、删除）操作。
> 备注：数据库名称是相对于sqlite数据库而言，比如`datu`、`datu_impute`和`user`等，因为mariadb中仅一个数据库。

> 例一：你打算对`datu`电站的 `datuStationInfo`表进行处理，那么你需要 `DatabaseManager`、数据库名称(`datu`)和具体模型表；

> 例二：你打算对`datu`电站的 `datuStringOverview`表（位于`datu_impute.db`）进行处理，那么你需要 `DatabaseManager`、数据库名称(`datu_impute`)和具体模型表
### 示例函数

以下是一些示例函数，展示了如何使用 `DatabaseManager` 和 ORM 模型进行数据库操作。在以下示例中，由于是对用户表处理，其数据库名称固定（user.db -> user），则直接写在函数中。然而，这种做法并不推荐，还是建议数据库名称作为函数的输入参数

```py
# 查询
def validate_username_exists_orm(username, database_manager, user_model):
    db_name = 'user' # 数据库名称
    try:
        with database_manager.get_session(db_name) as session:
            
            query = session.query(user_model).filter(user_model.user_name == username).first()
            return query is not None
    except Exception as e:
        print(f"Error validating username '{username}' in database '{db_name}': {e}")
        return False

# 修改
def change_user_status_orm(username, status, database_manager, user_model):
    db_name = 'user'
    try:
        with database_manager.get_session(db_name) as session:
            # Update user status
            user = session.query(user_model).filter(user_model.user_name == username).first()
            if user:
                user.user_validated = status
                session.commit()
                return 1
            else:
                return 0
    except Exception as e:
        print(f"Error changing status for user '{username}' in database '{db_name}': {e}")
        session.rollback()
        return 0

# 删除
def delete_user_orm(username, database_manager, user_model):
    db_name = 'user'
    try:
        with database_manager.get_session(db_name) as session:
            # Delete user
            user = session.query(user_model).filter(user_model.user_name == username).first()
            if user:
                session.delete(user)
                session.commit()
                return 1
            else:
                return 0
    except Exception as e:
        print(f"Error deleting user '{username}' from database '{db_name}': {e}")
        session.rollback()
        return 0

# 增加
def user_register_orm(username, password, email, phone, database_manager, user_model):
    db_name = 'user'
    try:
        with database_manager.get_session(db_name) as session:
            # Create a new UserInfo object
            new_user = user_model(
                user_name=username,
                user_type='user',
                user_password=password,
                user_email=email,
                user_phone=phone,
                user_validated=0
            )
            
            # Add the new user to the session
            session.add(new_user)
            
            # Commit the transaction
            session.commit()
            return 1
    except Exception as e:
        print(f"Error registering user '{username}' in database '{db_name}': {e}")
        session.rollback()
        return 0
```

### 独立模块测试

当开发人员想对模块进行单独测试，而不是启动 `app.py` 时，需要先初始化数据库管理器，并按需创建表模型，找到对应的数据库名称，然后进行数据库的操作。具体步骤如下：

1. **初始化 `DatabaseManager`**：在测试脚本或模块中初始化 `DatabaseManager`。
 ```py
 database_manager = DatabaseManager(repo_path="你的仓库路径")
 ```

2. **创建表模型**：根据需要创建相关的 ORM 模型。
	1. 方式一：创建所有的表模型，按需使用
	2. 方式二：根据场站创建表
	3. 对于用户表，直接创建即可：`user_model = create_user_model()`
```py
# 方式一
station_models = {station_name: create_station_models(station_name) for station_name in station_list}
impute_models = {station_name: create_impute_model(station_name) for station_name in station_list}
station_name = 'datu'
datu_model = station_models.get(station_name)
datu_station_info, datu_inverter_info, datu_string_info = datu_model
datu_string_overview = impute_models.get(station_name)
```
```py
# 方式二
station_name = 'datu'
datu_station_info, datu_inverter_info, datu_string_info = create_station_models(station_name)
datu_string_overview = create_impute_model(station_name)
```

3. 设置数据库名称：一般情况下和 `station_name`参数相同，或者 `f{station_name}_impute`，或者 `user`
4. **执行数据库操作**：使用上述函数或其他自定义函数执行数据库操作。
通过遵循以上指南，开发人员可以在模块中方便地进行数据库操作。如有任何疑问或需要进一步的帮助，请随时联系 Zelin 或参考相关文档。

#### 测试示例（from `process/preprocess/index.py`）
```python
# 已有的库......
# 测试用
from schema.session import DatabaseManager
from schema.models import create_station_models
from dotenv import load_dotenv

# 相关函数
def preprocess_log(start_timestamp, end_timestamp, station_name,kairosdb_url, repo_abs_path, database_manager, station_model):
    # 省略其他函数，重点关注 df2orm 函数
    df2orm(dataframe_dict, station_name, processing_stamps, database_manager, station_model)

def df2orm(dataframe_dict, station_name, processing_stamps, database_manager, station_model):
    """
    使用 ORM 方式批量写入数据，支持事务回滚

    参数：
    - dataframe_dict: 包含 DataFrame 的字典 {表名: DataFrame}
    - station_name: 场站名称，用于确定数据库连接和表名
    - processing_stamps: 需要处理的 timestamp 列表（毫秒级）
    """

    # 获取对应表模型
    station_info, inverter_info, string_info = station_model

    session = None
    try:
        # 获取数据库会话
        session = database_manager.get_session(station_name) 

        for table_name, df in dataframe_dict.items():
            if df.empty:
                print(f"警告: {table_name} 无数据。")
                continue

            df['timestamp'] = (df['timestamp'] / 1000).astype(int)

            # 获取当前表模型
            Model = station_info
            if not Model:
                raise ValueError(f"未定义的表名: {table_name}")

            # 处理 device_id 字段（保持原逻辑）
            if table_name in ['InverterInfo', 'StringInfo']:
                device_info = df['device_id'].str.split('-', expand=True)
                if table_name == 'InverterInfo':
                    Model = inverter_info
                    df['box_id'] = device_info[0]
                    df['inverter_id'] = device_info[1]
                else:
                    Model = string_info
                    df['box_id'] = device_info[0]
                    df['inverter_id'] = device_info[1]
                    df['string_id'] = device_info[2]

            # 删除已存在数据（事务1）
            if processing_stamps:
                delete_stmt = (
                    session.query(Model)
                    .filter(Model.timestamp.in_(processing_stamps))
                    .delete(synchronize_session=False)
                )
                print(f"已删除 {delete_stmt} 条记录")

            # 批量插入（事务2）
            df = df.replace({np.nan: None}) # 新增：清洗所有NaN值（兼容所有数据库）
            records = df.to_dict('records')
            session.bulk_insert_mappings(Model, records)

        session.commit()

    except SQLAlchemyError as e:
        print(f"数据库操作失败: {str(e)}")
        if session:
            session.rollback()
        raise
    finally:
        if session:
            session.close()

    print(f"成功插入 {sum(len(df) for df in dataframe_dict.values())} 条记录")



if __name__ == '__main__':
    # 获取当前环境变量，默认为development
    env_name = os.getenv("APP_ENV", "development").strip()
    global_repo_abs_path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))) 
    # 加载环境变量配置文件
    load_dotenv(os.path.join(global_repo_abs_path, 'setting', f".env.{env_name}")) 
    global_database_manager = DatabaseManager(global_repo_abs_path)
    global_kairosdb_url = os.getenv('KAIROSDB_URL', 'http://localhost:8080/api/v1/datapoints/query').strip() # 获取KairosDB的URL

    station_name = 'datu'
    test_date_str = '2025-05-20'
    station_model = create_station_models(station_name)

    start_timestamp, end_timestamp, station_list = get_basis_info_anyday(repo_abs_path=global_repo_abs_path,process_date=test_date_str)
    preprocess_log(start_timestamp, end_timestamp, station_name, global_kairosdb_url, global_repo_abs_path, global_database_manager, station_model)
```


## 后端接口说明
本后端分为两大模块：process和connect。
1. process模块读取数据库跑模型或算法得到结果，并将结果存储入文件夹data/{场站名}/results/ 
2. connect模块读取results中的文件或数据库数据，返回给前端。
与之前的不同在于：前后端通信只读取已处理好的最近一次结果，即该文件夹下最新的结果文件。把算法部分挪到process模块，晚上12点获取前一天数据定时完成。

### 数据文件说明
#### 1 results
1. 每一个results文件的文件名都为当天的日期，即"2024-12-18.json"记录了2024年12月18日的所有结果。
2. results文件是一个json文件，例子格式如下所示：
```
{
    date: "2024-12-18", //这是2024年12月18日的结果
    results: { //结果字典
        "011-009-018": { // key为device_id即“箱变器号-逆变器号-组串号”每一个都变成规律的三位数字字符串，不足长度的用0补齐
            string_id: "018", //组串号
            inverter_id: "009", //逆变器号
            box_id: "011", //箱变器号， 上述三个描述的设备为ZZ011NB009-18
            location_id: "12,09,07", //无人机坐标号
            anomaly_identifier: "normal", //当前劣化组串的状态，normal表示没做特殊处理，zero表示为过滤的零电流，double表示该组串为过滤的单口接两串
            degredation_rate: 0.5, //一个数值，表示劣化率
            anomaly_score: 5, //一个数值，表示异常值
            rdc_position: [0,1], //降维坐标
            anomaly_dates: [0,1,0,1,1,0,...,..], //30天
            diagnosis_results: [ //异常诊断的结果数组
                {
                    result: "遮挡", //诊断的成因
                    rate: 0.9,//概率
                },
                {
                    result: "热斑", //诊断的成因
                    rate: 0.08,//概率
                },
                {
                    result: "正常", //诊断的成因
                    rate: 0.02,//概率
                },
            ],
            loss_volume: [ // 损失量数组，过去三十天+未来七天小时级的损失量估计
                {
                    timestamp: 123245651, //时间戳，精确到小时
                    loss: 56 //损失的发电量
                }
            ]
        }
    }
}
```
#### 2 reports
这是无人机诊断报告对应的数据


#### 3 config
这是系统配置文件部分，存储了每个场站的组串号、无人机位置对应情况

#### 4 database
这是系统的数据库部分，所有数据库相关全部采用SQlite。
对每个场站的每个功能建表，如电站唐景共计4张表。

组串电气量表StringInfo
|timestamp|string_id|inverter_id|box_id|intensity|voltage|fixed_intensity|fixed_voltage|
|-|-|-|-|-|-|-|-|
|精确到小时的时间戳|组串号|逆变器号|箱变号|电流|电压|修复+填补后的电流|修复+填补后的电压|
||05-07-01|0.5|0.7|0.6|0.5|

逆变器电气量表InverterInfo
|timestamp|inverter_id|box_id|intensity|voltage|power|sig1|...|sign|
|-|-|-|-|-|-|-|-|-|
|精确到小时的时间戳|逆变器号|箱变号|总电流|总电压|功率|异常信号1|...|异常信号n|
||05-07|0.5|0.7|0.35|1|...|0|

场站环境表StationInfo
|timestamp|irradiance|temperature|power|
|-|-|-|-|
|精确到小时的时间戳|辐照强度|温度|总发电量|

此外还有系统对应的表格一张，存储用户。

用户表UserInfo
|user_id|user_name|user_type|user_password|user_email|user_phone|
|-|-|-|-|-|-|
|用户id|用户名|用户权限（admin，user)|用户密码|用户邮箱|用户手机号|


### 数据库说明
1. 第一步安装sqlite数据库 pip install sqlite3
2. 第二步运行python create_database.py，完成这一步之后database文件夹下面会有一个datang.db文件出现。
3. 第三步运行python write_data.py，完成这一步之后datang.db中写入了rawdata\2024-11-05\文件夹下所有xlsx的数据（电流电压等），辐照数据还没有写入。
4. 第四步运行python read_data.py，查看数据库是否正常获取到了数据。
5. 参考read_data.py里的SELECT相关代码（目前是所有列都获取了，可以只获取device_id和intensity列），实现各模块connect时需要读取的电气量数据。


### 各部分接口说明
#### 1 用户管理
输入部分


#### 2 数据清洗


#### 3 数据融合

#### 4 劣化识别