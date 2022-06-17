#include <iostream>
#include <cstring>
#include <fstream>
#include <ctime>
#include <unistd.h>
#include <sys/stat.h>
#include <errno.h>
#include <libgen.h>
#include "../sdk/20210406_v6.6.1_api_linux64/ThostFtdcUserApiDataType.h"
#include "../sdk/20210406_v6.6.1_api_linux64/ThostFtdcUserApiStruct.h"
#include "../sdk/20210406_v6.6.1_api_linux64/ThostFtdcMdApi.h"


using namespace std;


string ROOT             = "";                       //根路径，被MarketDataSpi::OnRtnDepthMarketData()和init()使用
string LOGFILE          = "";                       //日志文件, 被MarketDataSpi::Log()和log()使用
string LOGFILE_ERROR    = "";                       //错误日志文件, 被MarketDataSpi::LogError()和log_error()使用
string LOGFILE_MARKET   = "";                       //深度行情通知日志文件, 被MarketDataSpi::LogMarket()使用
string CATEGORY;                                    //类别, 为了worker多进程, 被main()、init()和contract()使用
int PRINT               = 1;                        //是否打印输出到窗口, 0表示不打印, 1表示打印


CThostFtdcMdApi *pointerUserApi;
TThostFtdcBrokerIDType      BROKER_ID;
TThostFtdcInvestorIDType    INVESTOR_ID;
TThostFtdcPasswordType      PASSWORD;
int intRequestID = 0;
string *CONTRACT_ARRAY;                             //合约, 字符串数组, 被MarketDataSpi::SubscribeMarketData()和contract()使用
int CONTRACT_COUNT;                                 //合约, 数量, 被MarketDataSpi::SubscribeMarketData()和contract()使用


/*
* ------------------------------------------------------------------
* 对API中的类CThostFtdcMdSpi, 做了继承
* ------------------------------------------------------------------
*/
class MarketDataSpi : public CThostFtdcMdSpi{
    
public:
    //错误应答
    virtual void OnRspError(CThostFtdcRspInfoField *pRspInfo, int nRequestID, bool bIsLast);
    
    //当客户端与交易后台建立通信连接时，还未登录前，该方法被调用
    virtual void OnFrontConnected();
    
    //当客户端与交易后台通信连接断开时，该方法被调用
	virtual void OnFrontDisconnected(int nReason);
    
    //登录请求响应，回调
    virtual void OnRspUserLogin(CThostFtdcRspUserLoginField *pRspUserLogin, CThostFtdcRspInfoField *pRspInfo, int nRequestID, bool bIsLast);
    
    //访问行情应答
    virtual void OnRspSubMarketData(CThostFtdcSpecificInstrumentField *pSpecificInstrument, CThostFtdcRspInfoField *pRspInfo, int nRequestID, bool bIsLast);
    
    //深度行情通知
    virtual void OnRtnDepthMarketData(CThostFtdcDepthMarketDataField *pDepthMarketData);

private:
    //用户登录
    void UserLogin();
    
    //订单行情数据
    void SubscribeMarketData();
    
    //错误应答的判断
    bool IsErrorResponseInformation(CThostFtdcRspInfoField *pRspInfo);
    
    //日志
    void Log(string message);
    
    //错误日志
    void LogError(string message);
    
    //日志, 深度行情通知
    void LogMarket(string message);
    
    //生成目录
    void createDirectory(string path);
};


/*
* ------------------------------------------------------------------
* 对API中的类CThostFtdcMdSpi, 继承了以后, 进行了方法的实现
* ------------------------------------------------------------------
*/


//response回复出错
void MarketDataSpi::OnRspError(CThostFtdcRspInfoField *pRspInfo, int nRequestID, bool bIsLast){
    LogError("error, OnRspError, worker.cpp #88, ErrorID: " + to_string(pRspInfo->ErrorID));
}


//判断是否已连接
void MarketDataSpi::OnFrontConnected(){
    UserLogin();
}


//连接断开
void MarketDataSpi::OnFrontDisconnected(int nReason){
    LogError("OnFrontDisconnected, nReason:" + to_string(nReason));
}


//用户登录
void MarketDataSpi::UserLogin(){
    CThostFtdcReqUserLoginField user;
    memset(&user, 0, sizeof(user));
    strcpy(user.BrokerID,   BROKER_ID);
    strcpy(user.UserID,     INVESTOR_ID);
    strcpy(user.Password,   PASSWORD);
    int result = pointerUserApi->ReqUserLogin(&user, ++intRequestID);
    
    if(result == 0){
        Log("login success, worker.cpp #109");
    }else{
        LogError("error, login failed, worker.cpp #111");
    }
}


//用户登录，回调
void MarketDataSpi::OnRspUserLogin(CThostFtdcRspUserLoginField *pRspUserLogin, CThostFtdcRspInfoField *pRspInfo, int nRequestID, bool bIsLast){
    Log("OnRspUserLogin, callback after login success, worker.cpp #118");
    if (bIsLast && !IsErrorResponseInformation(pRspInfo)){
        string tradingDay = pointerUserApi->GetTradingDay();
        Log("OnRspUserLogin, 获取当前交易日, " + tradingDay + ", worker.cpp #121");
        SubscribeMarketData();
    }
    
}


//订阅行情数据
void MarketDataSpi::SubscribeMarketData(){
    int iInstrumentID = CONTRACT_COUNT;
    
    char *ppInstrumentID[iInstrumentID];
    for(int i=0;i<iInstrumentID;i++){
        ppInstrumentID[i] = const_cast<char *>(CONTRACT_ARRAY[i].c_str());
    }

    int intResult = pointerUserApi->SubscribeMarketData(ppInstrumentID, iInstrumentID);
    string result;
    if(intResult == 0){
        result = "成功";
    }else{
        result = "失败";
    }
    Log("发送行情订阅请求, 结果: " + result + ", worker.cpp #131");
}


//订阅行情应答
void MarketDataSpi::OnRspSubMarketData(CThostFtdcSpecificInstrumentField *pSpecificInstrument, CThostFtdcRspInfoField *pRspInfo, int nRequestID, bool bIsLast){
    string instrumentID = pSpecificInstrument->InstrumentID;
    Log("OnRspSubMarketData, 合约代码: " + instrumentID + ", worker.cpp #158");
}


//深度行情通知
void MarketDataSpi::OnRtnDepthMarketData(CThostFtdcDepthMarketDataField *pDepthMarketData){
    string tradingDay   = pDepthMarketData->TradingDay;                 //交易日
    string contract     = pDepthMarketData->InstrumentID;               //合约代码
    
    LogMarket("交易日:" + tradingDay \
        + ", 合约代码: " + contract \
        + ", 最新价: " + to_string(pDepthMarketData->LastPrice) \
        + ", 最后修改时间: " + pDepthMarketData->UpdateTime \
        + ", 最后修改毫秒: " + to_string(pDepthMarketData->UpdateMillisec));
    
    struct tm *nowtime;
    time_t lt1;
    time( &lt1 );
    nowtime = localtime(&lt1);
    char tmpbuffer[128];
    strftime(tmpbuffer, 128, "%Y%m%d", nowtime);
    string today= tmpbuffer;
    
    string path = ROOT + "/data/" + today;
    string file = path + "/" + contract + "_" + tradingDay + ".csv";
    fstream outfile;
    outfile.open(file, ios::in);
    if(!outfile.is_open()){
        Log("open file failed, file or directory not exist, file: " + file + ", worker.cpp #194");
        
        fstream directory;
        directory.open(path, ios::in);
        if(!directory.is_open()){
            Log("directory not exist, go to create directory, path:" + path + ", worker.cpp #199");
            createDirectory(path);
        }else{
            directory.close();
        }

        outfile.open(file, ios::app);
        if(!outfile.is_open()){
            LogError("create file or directory failed, file: " + file + ", path: " + path + ", worker.cpp #207");
        }
        outfile << "TradingDay,"    << "reserve1,"          << "ExchangeID,"        << "reserve2,"      << "LastPrice,"
            << "PreSettlementPrice,"<< "PreClosePrice,"     << "PreOpenInterest,"   << "OpenPrice,"     << "HighestPrice,"
            << "LowestPrice,"       << "Volume,"            << "Turnover,"          << "OpenInterest,"  << "ClosePrice,"
            << "SettlementPrice,"   << "UpperLimitPrice,"   << "LowerLimitPrice,"   << "PreDelta,"      << "CurrDelta,"
            << "UpdateTime,"        << "UpdateMillisec,"    << "BidPrice1,"         << "BidVolume1,"    << "AskPrice1,"
            << "AskVolume1,"        << "BidPrice2,"         << "BidVolume2,"        << "AskPrice2,"     << "AskVolume2,"
            << "BidPrice3,"         << "BidVolume3,"        << "AskPrice3,"         << "AskVolume3,"    << "BidPrice4,"
            << "BidVolume4,"        << "AskPrice4,"         << "AskVolume4,"        << "BidPrice5,"     << "BidVolume5,"
            << "AskPrice5,"         << "AskVolume5,"        << "AveragePrice,"      << "ActionDay,"     << "InstrumentID,"
            << "ExchangeInstID,"    << "BandingUpperPrice," << "BandingLowerPrice" << endl;
        outfile.close();
    }else{
        outfile.close();
    }
    
    outfile.open(file, ios::app);
    
    outfile << tradingDay                       << "," << pDepthMarketData->reserve1            << ","
        << pDepthMarketData->ExchangeID         << "," << pDepthMarketData->reserve2            << ","
        << pDepthMarketData->LastPrice          << "," << pDepthMarketData->PreSettlementPrice  << ","
        << pDepthMarketData->PreClosePrice      << "," << pDepthMarketData->PreOpenInterest     << ","
        << pDepthMarketData->OpenPrice          << "," << pDepthMarketData->HighestPrice        << ","
        << pDepthMarketData->LowestPrice        << "," << pDepthMarketData->Volume              << ","
        << pDepthMarketData->Turnover           << "," << pDepthMarketData->OpenInterest        << ","
        << pDepthMarketData->ClosePrice         << "," << pDepthMarketData->SettlementPrice     << ","
        << pDepthMarketData->UpperLimitPrice    << "," << pDepthMarketData->LowerLimitPrice     << ","
        << pDepthMarketData->PreDelta           << "," << pDepthMarketData->CurrDelta           << ","
        << pDepthMarketData->UpdateTime         << "," << pDepthMarketData->UpdateMillisec      << ","
        << pDepthMarketData->BidPrice1          << "," << pDepthMarketData->BidVolume1          << ","
        << pDepthMarketData->AskPrice1          << "," << pDepthMarketData->AskVolume1          << ","
        << pDepthMarketData->BidPrice2          << "," << pDepthMarketData->BidVolume2          << ","
        << pDepthMarketData->AskPrice2          << "," << pDepthMarketData->AskVolume2          << ","
        << pDepthMarketData->BidPrice3          << "," << pDepthMarketData->BidVolume3          << ","
        << pDepthMarketData->AskPrice3          << "," << pDepthMarketData->AskVolume3          << ","
        << pDepthMarketData->BidPrice4          << "," << pDepthMarketData->BidVolume4          << ","
        << pDepthMarketData->AskPrice4          << "," << pDepthMarketData->AskVolume4          << ","
        << pDepthMarketData->BidPrice5          << "," << pDepthMarketData->BidVolume5          << ","
        << pDepthMarketData->AskPrice5          << "," << pDepthMarketData->AskVolume5          << ","
        << pDepthMarketData->AveragePrice       << "," << pDepthMarketData->ActionDay           << ","
        << pDepthMarketData->InstrumentID       << "," << pDepthMarketData->ExchangeInstID      << ","
        << pDepthMarketData->BandingUpperPrice  << "," << pDepthMarketData->BandingLowerPrice   << endl;
         
    outfile.close();
}


//回调的错误信息
bool MarketDataSpi::IsErrorResponseInformation(CThostFtdcRspInfoField *pRspInfo){
    bool bResult = ((pRspInfo) && (pRspInfo->ErrorID != 0));
    if (bResult){
        string errorId      = to_string(pRspInfo->ErrorID);
        string errorMessage = pRspInfo->ErrorMsg;
        LogError("ErrorID: " + errorId + ", ErrorMsg: " + errorMessage + ", worker.cpp #261");
    }
    return bResult;
}


//日志
void MarketDataSpi::Log(string message){
    struct tm *nowtime;
    time_t lt1;
    time( &lt1 );
    nowtime = localtime(&lt1);
    char now[128];
    strftime(now, 128, "%Y-%m-%d %H:%M:%S", nowtime);
    
    if(PRINT == 1)
        cout << now << "\t" << message << endl;
    
    ofstream outfile;
    outfile.open(LOGFILE, ios::app);
    outfile << now << "\t" << message << "\n" << endl;
    outfile.close();
}


//错误日志
void MarketDataSpi::LogError(string message){
    struct tm *nowtime;
    time_t lt1;
    time( &lt1 );
    nowtime = localtime(&lt1);
    char now[128];
    strftime(now, 128, "%Y-%m-%d %H:%M:%S", nowtime);
    
    if(PRINT == 1)
        cout << now << "\t" << message << endl;
    
    ofstream outfile;
    outfile.open(LOGFILE_ERROR, ios::app);
    outfile << now << "\t" << message << "\n" << endl;
    outfile.close();
}


//日志, 深度行情通知
void MarketDataSpi::LogMarket(string message){
    struct tm *nowtime;
    time_t lt1;
    time( &lt1 );
    nowtime = localtime(&lt1);
    char now[128];
    strftime(now, 128, "%Y-%m-%d %H:%M:%S", nowtime);
    
    if(PRINT == 1)
        cout << now << "\t" << message << endl;
    
    ofstream outfile;
    outfile.open(LOGFILE_MARKET, ios::out);
    outfile << now << "\t" << message << endl;
    outfile.close();
}


//生成目录
void MarketDataSpi::createDirectory(string path){
    int start   = 0;
    int position= 0;
    while(1){
        if((position=path.find("/", start)) != string::npos){
            string pathTemp = path.substr(0, position);
            mkdir(pathTemp.c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
            start = position+1;
        }else{
            mkdir(path.c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
            break;
        }
    }
}


/*
* ------------------------------------------------------------------
* 围绕着main()方法, 展开的相关操作
* ------------------------------------------------------------------
*/


/*
* 输出日志至窗口
* 记录日志至文本
*/
void log(string message){
    struct tm *nowtime;
    time_t lt1;
    time( &lt1 );
    nowtime = localtime(&lt1);
    char now[128];
    strftime(now, 128, "%Y-%m-%d %H:%M:%S", nowtime);
    
    if(PRINT == 1)
        cout << now << "\t" << message << endl;
    
    ofstream infile;
    infile.open(LOGFILE, ios::app);
    infile << now << "\t" << message << "\n" << endl;
    infile.close();
}


/*
* 记录错误日志
*/
void log_error(string message){
    struct tm *nowtime;
    time_t lt1;
    time( &lt1 );
    nowtime = localtime(&lt1);
    char now[128];
    strftime(now, 128, "%Y-%m-%d %H:%M:%S", nowtime);
    
    if(PRINT == 1)
        cout << now << "\t" << message << endl;
    
    ofstream logerrorfile;
    logerrorfile.open(LOGFILE_ERROR, ios::app);
    logerrorfile << now << "\t" << message << "\n" << endl;
    logerrorfile.close();
}


/*
* 初始化
* 获取根目录
* 判断目录log/worker是否存在
* 设定日志文件的路径名称
* 记录进程的pid
*/
void init(char *argv[]){
    string pwd          = getcwd(NULL, 0);
    string file         = argv[0];
    string file_start   = file.substr(0, 1);
    string path = "";
    if(file_start == "."){
        path    = pwd + file.substr(1);
    }else{
        path    = file;
    }
    ROOT = dirname( dirname( const_cast<char *>(path.c_str()) ) );
    
    fstream _file;
    _file.open(ROOT + "/log/worker", ios::in);
    if(!_file){
        string cmd = "mkdir -p " + ROOT + "/log/worker";
        system(cmd.c_str());
    }
    
    struct tm *nowtime;
    time_t lt1;
    time( &lt1 );
    nowtime = localtime(&lt1);

    char tmpbuffer[128];
    strftime(tmpbuffer, 128, "_%Y-%m-%d_%H-%M-%S", nowtime);
    LOGFILE         += ROOT + "/log/worker/log_" + CATEGORY + tmpbuffer + ".log";
    LOGFILE_ERROR   += ROOT + "/log/worker/error_" + CATEGORY + ".log";
    LOGFILE_MARKET  += ROOT + "/log/worker/log_" + CATEGORY + tmpbuffer + "_market.log";
    
    int pid = getpid();
    ofstream infile;
    infile.open(ROOT + "/log/worker/pid_" + CATEGORY + ".log", ios::out);
    infile << pid;
    infile.close();
}


/*
* 期货合约
* 对期货合约中的空格进行替换
* 被函数contact()调用
*/
string& contact_replace(string& str, const string& old_value, const string& new_value){
    for(string::size_type pos(0); pos!=string::npos; pos+=new_value.length()){
        if( (pos=str.find(old_value,pos)) != string::npos ){
            str.replace(pos,old_value.length(),new_value);
        }else{
            break;
        }
    }
    return str;
}


/*
* 期货合约
* 从对应的文本文件中, 获取期货合约的合约代码
*/
void contact(){
    string file = ROOT + "/contract/" + CATEGORY + ".log";
    
    fstream infile;
    infile.open(file, ios::in);
    if(!infile.is_open()){
        log_error("error, open contract file failed, worker.cpp #365, file: " + file);
        exit(0);
    }
    
    //计算文件的行数
    string stringTemp;
    int line = 0;
    while(getline(infile, stringTemp)){
        line++;
    }
    
    //移动至文件开头
    infile.clear();
    infile.seekg(0, ios::beg);
    string *stringArrayResult = new string[line];
    int line2 = 0;
    while(getline(infile, stringTemp)){
        stringTemp = contact_replace(stringTemp, "\n", "");
        stringTemp = contact_replace(stringTemp, "\r", "");
        if(stringTemp.find(" ") != string::npos)
            stringTemp = contact_replace(stringTemp, " ", "");
        if(stringTemp.length() == 0)
            continue;
        stringArrayResult[line2] = stringTemp;
        line2++;
    }
    
    if(line2 == 0){
        log_error("error, contract code empty in contact file, worker.cpp #410, file: " + file);
        exit(0);
    }
    
    string message  = "[cu] contract count: " + to_string(line2) + ", contract below: ";
    log(message);
    CONTRACT_ARRAY  = new string[line2];
    for(int i=0;i<line2;i++){
        CONTRACT_ARRAY[i] = stringArrayResult[i];
        log("[" + to_string(i) + "]" + stringArrayResult[i]);
    }

    CONTRACT_COUNT  = line2;

    delete []stringArrayResult;
    infile.close();
}


/*
* main()方法, 程序的入口
* argv[1], BROKER_ID
* argv[2], INVESTOR_ID
* argv[3], PASSWORD
* argv[4], FRONT_ADDR
* argv[5], CATEGORY
* argv[6], PRINT
*/
int main(int argc, char *argv[]){
    if(argc < 7){
        cout << "error, agrc count is incorrect, less than 7, worker.cpp #415" << endl;
        return 0;
    }

    strcpy(BROKER_ID,   argv[1]);
    strcpy(INVESTOR_ID, argv[2]);
    strcpy(PASSWORD,    argv[3]);

    int length              = strlen(argv[4]);
    char FRONT_ADDR[length] = {0};
    strcpy(FRONT_ADDR,  argv[4]);

    CATEGORY                = argv[5];
    
    init(argv);

    PRINT                   = std::atoi( argv[6] );
    if(PRINT !=0 && PRINT !=1){
        PRINT = 1;
        log_error("error, the sixth agrc is incorrect about PRINT, it must 0 or 1, worker.cpp #434\n");
        return 0;
    }
    
    contact();
    
    string BROKER_ID_string     = BROKER_ID;
    string INVESTOR_ID_string   = INVESTOR_ID;
    string PASSWORD_string      = PASSWORD;
    string FRONT_ADDR_string    = FRONT_ADDR;
    log("BROKER_ID: " + BROKER_ID_string + ", INVESTOR_ID: " + INVESTOR_ID_string + ", PASSWORD: " + PASSWORD_string + ", FRONT_ADDR: " + FRONT_ADDR_string + ", worker.cpp #537");
    
    pointerUserApi = CThostFtdcMdApi::CreateFtdcMdApi();
    CThostFtdcMdSpi *pointerUserSpi = new MarketDataSpi();
    pointerUserApi->RegisterSpi(pointerUserSpi);
    pointerUserApi->RegisterFront(FRONT_ADDR);
    pointerUserApi->Init();
    pointerUserApi->Join();
    pointerUserApi->Release();
    
    return 0;
}

