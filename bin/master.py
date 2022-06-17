import sys, os, socket, time, pandas, psutil, pytz, datetime, subprocess
from master_config import config


class Master:


    __root              = ''
    __logfile           = ''
    __logfile_error     = ''
    __logfile_repair    = ''
    __logfile_address   = ''
    __worker            = None


    def __init__(self):
        #获取软件的根目录
        file        = __file__
        if file.find('/./'):
            file    = file.replace('/./', '/')
        root        = os.path.dirname(os.path.dirname(file))
        self.__root = root
        
        #日志文件路径
        if not os.path.exists(root + '/log/master'):
            os.makedirs(root + '/log/master')
        if not os.path.exists(root + '/log/worker'):
            os.makedirs(root + '/log/worker')
        if not os.path.exists(root + '/log/contract'):
            os.makedirs(root + '/log/contract')
        self.__logfile          = root + '/log/master/log.log'
        self.__logfile_error    = root + '/log/master/log_error.log'
        self.__logfile_repair   = root + '/log/master/log_repair.log'
        self.__logfile_address  = root + '/log/master/log_address.log'
        
        #检查动态链接库文件是否已创建软连接
        if os.path.exists('/lib64/thosttraderapi_se.so') == False:
            os.system('/usr/bin/ln -sf {}/sdk/20210406_v6.6.1_api_linux64/thosttraderapi_se.so /lib64/thosttraderapi_se.so' . format(root))
        if os.path.exists('/lib64/thostmduserapi_se.so') == False:
            os.system('/usr/bin/ln -sf {}/sdk/20210406_v6.6.1_api_linux64/thostmduserapi_se.so /lib64/thostmduserapi_se.so' . format(root))
        
        #初始化worker
        worker  = pandas.DataFrame(config['worker'])
        if len(worker) == 0:
            self.__log_error('worker empty from master_config.py, master.py #43')
            os._exit(0)
        worker['running']   = 0;
        worker['pid']       = 0;
        worker['pidfile']   = worker['name'].apply( lambda x: "{}/log/worker/pid_{}.log" . format(root, x) )
        worker['command']   = worker['name'].apply( lambda x: "{}/bin/worker {} {} {} tcp://{} {} 0 2>&1 &" \
                . format(root, config["ctp"]["broker_id"], config["ctp"]['investor_id'], config["ctp"]['password'], config["ctp"]['front_address']['market_front'], x) )
        self.__worker = worker
    
    
    #日志的记录
    def __log(self, message):
        print("%s\n" % (message))
        nowtime     = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        message     = nowtime + "\n" + message + "\n\n"
        fileopen    = open(self.__logfile, "a")
        fileopen.write(message)
        fileopen.close()
    
    
    #错误日志的记录
    def __log_error(self, message):
        print("%s\n" % (message))
        nowtime     = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        message     = nowtime + "\n" + message + "\n\n"
        fileopen    = open(self.__logfile_error, "a")
        fileopen.write(message)
        fileopen.close()


    #CTP的IP和端口地址是否连通的记录
    def __log_address(self, message):
        timezone    = pytz.timezone('Asia/Shanghai')
        today       = datetime.datetime.fromtimestamp(int(time.time()), timezone).strftime("%Y-%m-%d")
        logfile     = self.__logfile_address.replace('log_address.log', 'log_address_{}.log'.format(today))
            
        print("%s\n" % (message))
        nowtime     = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        message     = nowtime + "\n" + message + "\n\n"
        fileopen    = open(logfile, "a")
        fileopen.write(message)
        fileopen.close()


    #每日维护, 关闭所有worker, 退出master进程再由supervisord重新拉起
    def __start_dailyrepair(self):
        #判断维护日志文件是否存在, 如果存在, 读取上一次维护的unix timestamp
        #然后, 通过比对unix时间戳, 来决定是否进行本次维护
        file        = self.__logfile_repair
        if os.path.exists(file) == True:
            fileopen    = open(file, "r")
            lasttime    = fileopen.read()
            fileopen.close()
            lasttime    = lasttime.strip()
            if len(lasttime) < 5:
                self.__log("start model, dailyrepair, lasttime: [{}] , master.py #126". format(lasttime))
                self.__start_dailyrepair_handle()
            else:
                lasttime= int(lasttime)
                unixtime= int(time.time())
                if (unixtime-lasttime) > 3600:
                    self.__log("start model, dailyrepair, lasttime: [{}] , master.py #132". format(lasttime))
                    self.__start_dailyrepair_handle()
        else:
            self.__start_dailyrepair_handle()
        


    #每日维护的具体执行
    def __start_dailyrepair_handle(self):
        self.__log("start model, dailyrepair handle, master.py #141")
        #结束所有worker
        worker  = self.__worker
        root    = self.__root
        exefile = root + '/bin/worker'
        for i in range(len(worker)):
            pidfile = worker.iloc[i].at['pidfile']
            if not os.path.exists(pidfile):
                del pidfile
                continue
            
            fileopen= open(pidfile, "r")
            pid     = fileopen.read()
            fileopen.close()
            pid     = int(pid)
            if psutil.pid_exists(pid) == False:
                os.unlink(pidfile)
                del pidfile, fileopen, pid
                continue
            
            process = psutil.Process(pid)
            if process.exe() != exefile:
                os.unlink(pidfile)
                del pidfile, fileopen, pid, process
                continue
            
            name    = worker.iloc[i].at['name']
            if name not in process.cmdline():
                os.unlink(pidfile)
                del pidfile, fileopen, pid, process, name
                continue
            
            os.system("/usr/bin/kill {}" . format(str(pid)))
            self.__log("start model, stopped worker progress for daily repair, worker: {}, pid: {}, master.py #174" . format(name, str(pid)))
            del pidfile, fileopen, pid, process, name
        
        worker['running']   = 0;
        worker['pid']       = 0;
        
        #将此次维护的unix时间戳, 写入维护日志
        file        = self.__logfile_repair
        unixtime    = int(time.time())
        fileopen    = open(file, "w")
        fileopen.write(str(unixtime))
        fileopen.close()
        
        #优化内存
        os.system("/usr/bin/echo 3 > /proc/sys/vm/drop_caches")
        
        #退出master进程
        self.__log("start model, exit master process for daily repair, master.py #188")
        os._exit(0)


    #将之前日期的目录, 打包成zip, 并删除这些目录
    def __start_zip(self):
        timezone    = pytz.timezone('Asia/Shanghai')
        nowday      = datetime.datetime.fromtimestamp(int(time.time()), timezone).strftime("%Y%m%d")
        nowday      = int(nowday)
        
        root    = self.__root
        path    = root + '/data'
        for subpath in os.listdir(path):
            if subpath.isdigit() == True:
                if int(subpath) < nowday:
                    timestamp   = int(time.time())
                    command     = "cd {} && /usr/bin/zip -r {}/{}_{}.zip {} && /usr/bin/rm -rf {}/{} && cd -" . format(path, path, subpath, timestamp, subpath, path, subpath)
                    #command    = "cd {} && /usr/bin/zip -r {}/{}_{}.zip {} && cd -" . format(path, path, subpath, timestamp, subpath)
                    os.system(command)
                    self.__log("start model, zip compress folder and then delete it, command: {}, master.py #223" . format(command))
                    #os.rename('{}/{}'.format(path, subpath), '{}/{}_compressed_{}'.format(path, subpath, timestamp))
                    del timestamp, command


    #停止所有worker, 用于闲时停止数据采集
    def __start_stopworker(self):
        worker  = self.__worker
        root    = self.__root
        exefile = root + '/bin/worker'
        for i in range(len(worker)):
            pidfile = worker.iloc[i].at['pidfile']
            if not os.path.exists(pidfile):
                del pidfile
                continue
            
            fileopen= open(pidfile, "r")
            pid     = fileopen.read()
            fileopen.close()
            pid     = int(pid)
            if psutil.pid_exists(pid) == False:
                os.unlink(pidfile)
                del pidfile, fileopen, pid
                continue
            
            process = psutil.Process(pid)
            if process.exe() != exefile:
                os.unlink(pidfile)
                del pidfile, fileopen, pid, process
                continue
            
            name    = worker.iloc[i].at['name']
            if name not in process.cmdline():
                os.unlink(pidfile)
                del pidfile, fileopen, pid, process, name
                continue
            
            os.system("/usr/bin/kill {}" . format(str(pid)))
            self.__log("stopped worker progress, it will not work in daily spare time, worker: {}, pid: {} , master.py #222". format(name, str(pid)))
            del pidfile, fileopen, pid, process, name


    #获取所有worker及其状态
    def __start_workerstate(self):
        worker  = self.__worker
        worker['running']   = 0;
        worker['pid']       = 0;
        root    = self.__root
        exefile = root + '/bin/worker'
        for i in range(len(worker)):
            pidfile = worker.iloc[i].at['pidfile']
            if not os.path.exists(pidfile):
                del pidfile
                continue
            
            fileopen= open(pidfile, "r")
            pid     = fileopen.read()
            fileopen.close()
            pid     = pid.strip()
            pid     = int(pid)
            if psutil.pid_exists(pid) == False:
                os.unlink(pidfile)
                del pidfile, fileopen, pid
                continue
            
            process = psutil.Process(pid)
            if process.exe() != exefile:
                os.unlink(pidfile)
                del pidfile, fileopen, pid, process
                continue
            
            name    = worker.iloc[i].at['name']
            if name not in process.cmdline():
                os.unlink(pidfile)
                del pidfile, fileopen, pid, process, name
                continue
            
            worker.loc[i,'running'] = 1
            worker.loc[i,'pid']     = pid
            del pidfile, fileopen, pid, process, name
        
        return worker


    #检查front_address的TCP地址是否可用, 包括交易地址和行情地址
    def __start_tcpcheck(self):
        trade_front_list    = config["ctp"]['front_address']['trade_front'].split(":")
        trade_front_ip      = trade_front_list[0]
        trade_front_port    = int(trade_front_list[1])
        trade_front_tcp     = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        trade_front_result  = trade_front_tcp.connect_ex((trade_front_ip, trade_front_port))
        if trade_front_result != 0:
            del trade_front_list, trade_front_ip, trade_front_port, trade_front_tcp, trade_front_result
            return False
        
        market_front_list   = config["ctp"]['front_address']['market_front'].split(":")
        market_front_ip     = market_front_list[0]
        market_front_port   = int(market_front_list[1])
        market_front_tcp    = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        market_front_result = market_front_tcp.connect_ex((market_front_ip, market_front_port))
        if market_front_result != 0:
            del market_front_list, market_front_ip, market_front_port, market_front_tcp, market_front_result
            return False
        
        del trade_front_list,  trade_front_ip,  trade_front_port,  trade_front_tcp,  trade_front_result
        del market_front_list, market_front_ip, market_front_port, market_front_tcp, market_front_result
        return True


    def start(self):
        self.__log("start, master.py #239")
        while(True):
            #时间检查, 如果在08:00至08:30、20:00至20:30这两个时间段, 则进行维护
            #结束所有worker、退出master进程
            timezone    = pytz.timezone('Asia/Shanghai')
            nowtime     = datetime.datetime.fromtimestamp(int(time.time()), timezone).strftime("%H%M")
            nowtime     = int(nowtime)
            if nowtime>835 and nowtime<855:
                self.__start_dailyrepair()
            elif nowtime>2035 and nowtime<2055:
                self.__start_dailyrepair()
            
            #时间检查, 如果在03:00至05:00这个时间段, 对于data路径下的行情数据
            #将之前日期的目录, 打包成zip, 并删除这些目录
            if nowtime>300 and nowtime<500:
                self.__start_zip()
            
            #每日03:00至08:35、16:00至20:35为闲时, 不再运行worker进行数据采集
            if (nowtime>300 and nowtime<=835) or (nowtime>1600 and nowtime<=2035):
                self.__start_stopworker()
                time.sleep(10)
                continue
            
            #获取所有worker及其状态
            worker  = self.__start_workerstate()
            worker.to_csv(self.__root + '/log/master/worker.csv', encoding='utf_8_sig')
            #如果有未启用的woker, 检查front_address的TCP地址是否可用, 然后启用worker
            #此外, 如果全部worker都没有运行, 则运行一次contract, 来更新所有合约代码
            stop    = len( worker[worker['pid']==0] )
            if stop > 0:
                #检查TCP地址是否连通, 并记录日志
                tcpcheck = self.__start_tcpcheck()
                if tcpcheck == False:
                    self.__log_address('CTP address network blocked, trade_front:{}, market_front:{}, master.py #282' . format(config["ctp"]['front_address']['trade_front'], config["ctp"]['front_address']['market_front']))
                    del worker, stop, tcpcheck
                    time.sleep(1)
                    continue
                else:
                    self.__log_address('CTP address network connectivity, trade_front:{}, market_front:{}, master.py #287' . format(config["ctp"]['front_address']['trade_front'], config["ctp"]['front_address']['market_front']))
                
                #全部worker都没有运行, 执行contract; 此外, 处理contract超时卡死的现象;
                if stop == len(worker):
                    command = "{}/bin/contract {} {} {} tcp://{} 1" . format(self.__root, config["ctp"]["broker_id"], config["ctp"]['investor_id'], config["ctp"]['password'], config["ctp"]['front_address']['trade_front'])
                    self.__log("will run contract to fetch lastest contracts, master.py #273, command: {}" . format(command))
                    process = subprocess.Popen(command.split(' '))
                    try:
                        self.__log("wait for command by subprocess.Popen(), run contract to fetch contracts, master.py #275")
                        process.wait(60)
                    except subprocess.TimeoutExpired:
                        self.__log("timeout expired to fetch lastest contracts by contract , master.py #278")
                        process.kill()
                        del command, process
                        time.sleep(1)
                        continue

                    command2 = "/usr/local/python3.10/bin/python3 {}/bin/contract_handle.py average" . format(self.__root)
                    self.__log("contract_handle.py average to handle contracts, master.py #285, command: {}" . format(command2))
                    os.system(command2)
                    
                    #结束可能存在的、历史遗留的、处理卡死状态的contract进程
                    command3 = "ps aux | grep '{}/bin/contract' | grep -v grep | awk '{{print $2}}' | xargs kill -9" . format(self.__root)
                    self.__log("close possible left behind contract process, ignore 'kill: not enough arguments', master.py #289, command: {}" . format(command3))
                    os.system(command3)
                    
                    del command, process, command2, command3
                
                #循环方式启动未运行的worker
                worker_stop = worker[worker['pid']==0]
                for i in range(len(worker_stop)):
                    self.__log("start worker by os.system(), master.py #277, command: {}" . format(worker_stop.iloc[i].at['command']))
                    os.system(worker_stop.iloc[i].at['command'])
                
                del worker, stop, tcpcheck, worker_stop
                time.sleep(1)
            else:
                del worker, stop
                time.sleep(1)


    #停止所有worker进程
    def stop(self):
        worker  = self.__worker
        root    = self.__root
        exefile = root + '/bin/worker'
        for i in range(len(worker)):
            pidfile = worker.iloc[i].at['pidfile']
            if not os.path.exists(pidfile):
                del pidfile
                continue
            
            fileopen= open(pidfile, "r")
            pid     = fileopen.read()
            fileopen.close()
            pid     = int(pid)
            if psutil.pid_exists(pid) == False:
                os.unlink(pidfile)
                del pidfile, fileopen, pid
                continue
            
            process = psutil.Process(pid)
            if process.exe() != exefile:
                os.unlink(pidfile)
                del pidfile, fileopen, pid, process
                continue
            
            name    = worker.iloc[i].at['name']
            if name not in process.cmdline():
                os.unlink(pidfile)
                del pidfile, fileopen, pid, process, name
                continue
            
            os.system("/usr/bin/kill {}" . format(str(pid)))
            print("stopped worker progress, worker: {}, pid: {}" . format(name, str(pid)))
            del pidfile, fileopen, pid, process, name
    
    
    #查看worker的运行状态
    def status(self):
        worker  = self.__worker
        root    = self.__root
        exefile = root + '/bin/worker'
        for i in range(len(worker)):
            pidfile = worker.iloc[i].at['pidfile']
            if not os.path.exists(pidfile):
                del pidfile
                continue
            
            fileopen= open(pidfile, "r")
            pid     = fileopen.read()
            fileopen.close()
            pid     = int(pid)
            if psutil.pid_exists(pid) == False:
                os.unlink(pidfile)
                del pidfile, fileopen, pid
                continue
            
            process = psutil.Process(pid)
            if process.exe() != exefile:
                os.unlink(pidfile)
                del pidfile, fileopen, pid, process
                continue
            
            name    = worker.iloc[i].at['name']
            if name not in process.cmdline():
                os.unlink(pidfile)
                del pidfile, fileopen, pid, process, name
                continue
            
            worker.loc[i,'running'] = 1
            worker.loc[i,'pid']     = pid
            del pidfile, fileopen, pid, process, name
        
        for i in range(len(worker)):
            row = worker.iloc[i]
            print("worker:{}, running:{}, pid:{}" . format(row.at['name'], row.at['running'], row.at['pid']))


    #查看worker的启动命令, 方便测试用的
    def command(self):
        cmd     = "contract: {}/bin/contract {} {} {} tcp://{} 1" . format(self.__root, config["ctp"]["broker_id"], config["ctp"]['investor_id'], config["ctp"]['password'], config["ctp"]['front_address']['trade_front'])
        cmd2    = "contract: /usr/local/python3.10/bin/python3 {}/bin/contract_handle.py average" . format(self.__root)
        print(cmd)
        print(cmd2)
        worker  = self.__worker
        for i in range(len(worker)):
            print( "worker: {}, command: {}" . format(worker.iloc[i].at['name'], worker.iloc[i].at['command']) )


if __name__ == "__main__":
    if len(sys.argv) == 1:
        print("error, argv empty, start, stop, status, command\n")
        os._exit(0)
    cmd = sys.argv[1]
    if cmd not in ["start", "stop", "status", "command"]:
        print("error, argv is incorrect, start, stop, status, command\n")
        os._exit(0)
    
    master = Master()
    if cmd == "start":
        master.start()
    elif cmd == "stop":
        master.stop()
    elif cmd == "status":
        master.status()
    else:
        master.command()


