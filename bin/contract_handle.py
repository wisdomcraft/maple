import os, sys, time, numpy, pandas


'''
contract是编译之后的可执行程序, 用于访问CTP的交易接口, 获取当前CTP可调用的所有合约, 获取合约代码、交易所等信息
contract.cpp是源程序
contract_handle.py是处理程序, 由contract获取的合约信息存储于contract/contract.csv文件中
    contract_handle.py读取csv文件, 然后生成若干*.log文件
'''


class ContractHandle:


    __contractclass = 'exchange'        #默认按交易所进行分划
    __average_count = 10                #如果等分, 划分的份数
    __root          = ''                #根路径
    __logfile       = ''                #日志文件
    __contract      = None              #合约, 经构造函数后将成为pandas的DataFrame


    def __init__(self, contractclass):
        self.__contractclass = contractclass
        
        file        = __file__
        if file.find('/./'):
            file    = file.replace('/./', '/')
        root        = os.path.dirname(os.path.dirname(file))
        self.__root = root
        
        self.__logfile = root + '/log/contract/contract_handle.log'
        
        file        = root + '/contract/contract.csv'
        if os.path.exists(file) == False:
            print('error, csv file not exist, ' + file)
            os._exit(0)
        
        contract    = pandas.read_csv(file, header=0, sep=',', encoding='gbk')
        contract    = contract.loc[:, ['number', 'ExchangeID', 'InstrumentID']]
        if len(contract) == 0:
            print('error, empty contract in csv file, ' + file)
            os._exit(0)
        contract['month'] = contract['InstrumentID'].str[-2:].astype('int')
        contract['month'] = numpy.where( contract['month']>12, contract['month']%10, contract['month'] )
        self.__contract = contract
    
        path = root + '/contract'
        for filename in os.listdir(path):
            if filename.find('ctp_')==0 and filename.find('.log')>1:
                os.unlink(path + '/' + filename)
            del filename
    
    
    #日志的记录
    def __log(self, message):
        print("%s\n" % (message))
        nowtime     = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        message     = nowtime + "\n" + message + "\n\n"
        fileopen    = open(self.__logfile, "a")
        fileopen.write(message)
        fileopen.close()
        
    
    def __average(self):
        contract        = self.__contract
        contract        = contract.sample(frac=1)
        average_count   = self.__average_count
        step            = int( len(contract) / average_count )
        root            = self.__root
        for i in range(10):
            start   = i*step
            end     = (i+1) * step
            if i < 9:
                contract_list   = contract.iloc[start:end]['InstrumentID']
            else:
                contract_list   = contract.iloc[start:]['InstrumentID']
            contract_content    = '\n'.join(contract_list)
            file    = root + '/contract/ctp_' + str(i) + '.log'
            fileopen= open(file, 'w', encoding='utf-8')
            fileopen.write(contract_content)
            fileopen.close()
            del start, end, contract_list, contract_content, file, fileopen
        
        self.__log("success, average, contract_handle.py #82")


    def main(self):
        contract        = self.__contract
        contractclass   = self.__contractclass
        key     = ''
        if contractclass == 'exchange':
            key = 'ExchangeID'
        elif contractclass == 'month':
            key = 'month'
        else:
            self.__average()
            return None
        classlist   = contract[key].unique().tolist()
        root        = self.__root
        for _classlist in classlist:
            contract_list   = contract[ contract[key] == _classlist]['InstrumentID'].tolist()
            contract_content= '\n'.join(contract_list)
            file    = root + '/contract/ctp_' + str(_classlist).lower() + '.log'
            fileopen= open(file, 'w', encoding='utf-8')
            fileopen.write(contract_content)
            fileopen.close()
            del contract_list, contract_content, file, fileopen
        
        self.__log("success, {}, contract_handle.py #107" . format(contractclass))


if __name__ == "__main__":
    argv = sys.argv
    if len(argv) < 2:
        print('error, argv count is incorrect')
        os._exit(0)
    contractclass = argv[1]
    if contractclass not in ['exchange', 'month', 'average']:
        print('error, argv value is incorrect, [exchange, month, average]')
        os._exit(0)
    
    handle = ContractHandle(contractclass)
    handle.main()
