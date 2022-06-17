'''
Trade Front:180.168.146.187:10201, Market Front:180.168.146.187:10211, 电信,看穿式前置,第一组;
Trade Front:180.168.146.187:10202, Market Front:180.168.146.187:10212, 电信,看穿式前置,第二组;
Trade Front:218.202.237.33:10203,  Market Front:218.202.237.33:10213,  移动,看穿式前置,第三组;
Trade Front:180.168.146.187:10130, Market Front:180.168.146.187:10131, 电信,7x24环境;
config_ctp, 第一组账密
    broker_id:9999, investor_id:183302, password:myctp@2021
config_ctp, 第二组账密
    broker_id:9999, investor_id:197778, password:myctp@2022
'''
config_ctp = {
    "broker_id":    "9999",
    "investor_id":  "183302",
    "password":     "myctp@2021",
    "front_address":{"trade_front":"180.168.146.187:10130", "market_front":"180.168.146.187:10131", "remark":"电信,7x24环境"}
}

config_worker = [
    {'name':'ctp_0'},
    {'name':'ctp_1'},
    {'name':'ctp_2'},
    {'name':'ctp_3'},
    {'name':'ctp_4'},
    {'name':'ctp_5'},
    {'name':'ctp_6'},
    {'name':'ctp_7'},
    {'name':'ctp_8'},
    {'name':'ctp_9'},
]

config = {
    "ctp":config_ctp,
    "worker":config_worker
}

