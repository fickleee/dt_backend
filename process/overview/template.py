OVERVIEW_TEMPLATE = {
    "memo": "模板数据,仅当overview源文件损坏时使用",
    "dailyGeneration": 0, # 总日发电量
    "monthlyGeneration": 0, # 总月发电量
    "cumulativeGeneration": 0, # 总累计发电量
    "cumulativeLossGeneration": 0, # 总累计损失量
    "cumulativeFaultInverterDetection": 0, # 总累计逆变器故障检测量
    "cumulativeFaultDetection": 0, # 总累计组串故障检测量
    "toOMInverterFault": 0, # 总当日逆变器故障数量
    "toOMFault": 0, # 总当日组串故障数量
    "estimatedLoss": 0, # 总当日损失量
    "level1OMQuantity": 0, 
    "level2OMQuantity": 0,
    "level3OMQuantity": 0,
    "stringAnomalyData": { # 总组串异常类型分布
        "surfaceStain": 0, # 表面污迹
        "diodeFault": 0, # 二极管故障
        "circuitFault": 0 # 组串开路或短路
    },
    "stationData": [
        {
            "name": "大峃光伏电站",
            "shortName": "大峃",
            "label": "daxue",
            "planPowerGeneration": 0, # 月计划发电量
            "powerGeneration": 0, # 月发电量
            "auxiliaryPowerRate": 0, # 日发电量
            "dataAnomalyRate": 0.0, # 数据异常率
            "lowEfficiencyAnomalyRate": 0.0, # 组串异常率
            "volume": 25, # 装机容量
            "geo": [ # 经纬度
                120.0892,
                27.8387
            ],
            "location": "浙江省文成县大峃镇", # 场站位置
            "level1OMQuantity": 0, # 一级运维数量
            "inefficientStringNumber": 0 # 故障组串数
        },
        {
            "name": "大唐长大涂光伏电站",
            "shortName": "长大涂",
            "label": "datu",
            "planPowerGeneration": 0,
            "powerGeneration": 0,
            "auxiliaryPowerRate": 0,
            "dataAnomalyRate": 0.0,
            "lowEfficiencyAnomalyRate": 0.0,
            "volume": 300,
            "geo": [
                121.77,
                29.13
            ],
            "location": "浙江省宁波市象山县",
            "level1OMQuantity": 0,
            "inefficientStringNumber": 0
        },
        {
            "name": "唐景光伏电站",
            "shortName": "唐景",
            "label": "tangjing",
            "planPowerGeneration": 0,
            "powerGeneration": 0,
            "auxiliaryPowerRate": 0,
            "dataAnomalyRate": 0.0,
            "lowEfficiencyAnomalyRate": 0.0,
            "volume": 52,
            "geo": [
                119.745923,
                28.004601
            ],
            "location": "浙江省丽水市景宁畲族自治县半垟村",
            "level1OMQuantity": 0,
            "inefficientStringNumber": 0
        },
        {
            "name": "浙江大唐唐云光伏电站",
            "shortName": "唐云",
            "label": "tangyun",
            "planPowerGeneration": 0,
            "powerGeneration": 0,
            "auxiliaryPowerRate": 0,
            "dataAnomalyRate": 0.0,
            "lowEfficiencyAnomalyRate": 0.0,
            "volume": 30,
            "geo": [
                120.216667,
                28.766667
            ],
            "location": "浙江省丽水市缙云县东方镇",
            "level1OMQuantity": 0,
            "inefficientStringNumber": 0
        },
        {
            "name": "大唐乌沙山厂区光伏电站",
            "shortName": "乌沙山",
            "label": "wushashan",
            "planPowerGeneration": 0,
            "powerGeneration": 0,
            "auxiliaryPowerRate": 0,
            "dataAnomalyRate": 0.0,
            "lowEfficiencyAnomalyRate": 0.0,
            "volume": 43.2,
            "geo": [
                121.483333,
                29.5
            ],
            "location": "浙江省宁波市象山县西周镇",
            "level1OMQuantity": 0,
            "inefficientStringNumber": 0
        },
        {
            "name": "大唐文成县二源光伏电站",
            "shortName": "二源",
            "label": "eryuan",
            "planPowerGeneration": 0,
            "powerGeneration": 0,
            "auxiliaryPowerRate": 0,
            "dataAnomalyRate": 0.0,
            "lowEfficiencyAnomalyRate": 0.0,
            "volume": 23,
            "geo": [
                120.0252,
                27.5305
            ],
            "location": "浙江省文成县二源镇",
            "level1OMQuantity": 0,
            "inefficientStringNumber": 0
        },
        {
            "name": "浙江万市光伏电站",
            "shortName": "富阳",
            "label": "fuyang",
            "planPowerGeneration": 0,
            "powerGeneration": 0,
            "auxiliaryPowerRate": 0,
            "dataAnomalyRate": 0.0,
            "lowEfficiencyAnomalyRate": 0.0,
            "volume": 31,
            "geo": [
                119.5443,
                30.117
            ],
            "location": "浙江省杭州市富阳区万市镇三九山",
            "level1OMQuantity": 0,
            "inefficientStringNumber": 0
        },
        {
            "name": "唐屿光伏电站",
            "shortName": "马屿",
            "label": "mayu",
            "planPowerGeneration": 0,
            "powerGeneration": 0,
            "auxiliaryPowerRate": 0,
            "dataAnomalyRate": 0.0,
            "lowEfficiencyAnomalyRate": 0.0,
            "volume": 40,
            "geo": [
                120.7083,
                27.8333
            ],
            "location": "浙江省温州市瑞安市马屿镇",
            "level1OMQuantity": 0,
            "inefficientStringNumber": 0
        }
    ]
}