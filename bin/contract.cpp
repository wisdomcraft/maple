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
#include "../sdk/20210406_v6.6.1_api_linux64/ThostFtdcTraderApi.h"


using namespace std;


string ROOT             = "";                       //根路径，被TradeSpi::OnRspQryInstrument和init()使用
string LOGFILE          = "";                       //日志文件, 被TradeSpi::Log和log()使用
string LOGFILE_ERROR    = "";                       //错误日志文件, 被TradeSpi::LogError和log_error()使用
int PRINT               = 1;                        //是否打印输出到窗口, 0表示不打印, 1表示打印
int UNLINKED            = 0;                        //被TradeSpi::OnRspQryInstrument使用, 判断之前存在的contract.csc是否被删除


CThostFtdcTraderApi *pointerTradeApi;
TThostFtdcBrokerIDType      BROKER_ID;
TThostFtdcInvestorIDType    INVESTOR_ID;
TThostFtdcPasswordType      PASSWORD;
int intRequestID        = 0;
int intInstrumentCount  = 0;

//--------------------------------------------


class TradeSpi : public CThostFtdcTraderSpi{
    
public:
    //错误应答
    virtual void OnRspError(CThostFtdcRspInfoField *pRspInfo, int nRequestID, bool bIsLast);
    //当客户端与交易后台连接时, 还未登录前, 该方法被调用
    virtual void OnFrontConnected();
    //当客户端与交易后台通信连接断开时，该方法被调用
    virtual void OnFrontDisconnected(int nReason);
    //客户端认证响应
    virtual void OnRspAuthenticate(CThostFtdcRspAuthenticateField *pRspAuthenticateField, CThostFtdcRspInfoField *pRspInfo, int nRequestID, bool bIsLast);
    //登录请求响应
    virtual void OnRspUserLogin(CThostFtdcRspUserLoginField *pRspUserLogin, CThostFtdcRspInfoField *pRspInfo, int nRequestID, bool bIsLast);
    //请求查询合约响应
    virtual void OnRspQryInstrument(CThostFtdcInstrumentField *pInstrument, CThostFtdcRspInfoField *pRspInfo, int nRequestID, bool bIsLast);
    
private:
    //客户端认证
    void RequestAuthenticate();
    //用户登录
    void UserTradeLogin();
    //错误应答的判断
    bool IsErrorResponseInformation(CThostFtdcRspInfoField *pRspInfo);
    //日志
    void Log(string message);
    //错误日志
    void LogError(string message);
};


//--------------------------------------------


void TradeSpi::OnRspError(CThostFtdcRspInfoField *pRspInfo, int nRequestID, bool bIsLast){
    string errorId      = to_string(pRspInfo->ErrorID);
    string errorMessage = pRspInfo->ErrorMsg;
    LogError("ErrorID: " + errorId + ", ErrorMsg: " + errorMessage);
}


//当客户端与交易后台连接时
void TradeSpi::OnFrontConnected(){
    Log("OnFrontConnected");
    RequestAuthenticate();
}


//当客户端与交易后台通信连接断开时
void TradeSpi::OnFrontDisconnected(int nReason){
    Log("OnFrontDisconnected, nReason:" + to_string(nReason));
}


//客户端认证
void TradeSpi::RequestAuthenticate(){
    CThostFtdcReqAuthenticateField auth;
    memset(&auth, 0, sizeof(auth));
    strcpy(auth.AppID,      "simnow_client_test");
    strcpy(auth.AuthCode,   "0000000000000000");
    strcpy(auth.BrokerID,   "9999");
    strcpy(auth.UserID,     INVESTOR_ID);
    int result = pointerTradeApi->ReqAuthenticate(&auth, ++intRequestID);
    if(result == 0){
        Log("authenticate success");
    }else{
        Log("error, authenticate failed");
    }
}


//客户端认证响应
void TradeSpi::OnRspAuthenticate(CThostFtdcRspAuthenticateField *pRspAuthenticateField, CThostFtdcRspInfoField *pRspInfo, int nRequestID, bool bIsLast){
    if (!IsErrorResponseInformation(pRspInfo)){
        UserTradeLogin();
    }
}


//用户登录
void TradeSpi::UserTradeLogin(){
    CThostFtdcReqUserLoginField user;
    memset(&user, 0, sizeof(user));
    strcpy(user.BrokerID,   BROKER_ID);
    strcpy(user.UserID,     INVESTOR_ID);
    strcpy(user.Password,   PASSWORD);
    int result = pointerTradeApi->ReqUserLogin(&user, ++intRequestID);
    if(result == 0){
        Log("login success");
    }else{
        Log("error, login failed");
    }
}


//登录请求响应
void TradeSpi::OnRspUserLogin(CThostFtdcRspUserLoginField *pRspUserLogin, CThostFtdcRspInfoField *pRspInfo, int nRequestID, bool bIsLast){
    Log("OnRspUserLogin, callback after login success");
    if (bIsLast && !IsErrorResponseInformation(pRspInfo)){
        string tradingDay = pointerTradeApi->GetTradingDay();
        Log("OnRspUserLogin, 获取当前交易日, " + tradingDay);
        
        Log("接下来, 请求查询合约, 请稍等");
        
        CThostFtdcQryInstrumentField request;
        memset(&request, 0, sizeof(request));  
        int result = pointerTradeApi->ReqQryInstrument(&request, ++intRequestID);
        if(result != 0){
            LogError("请求查询合约, ReqQryInstrument() failed");
        }
    }
    
}


//CThostFtdcTraderApi的ReqQryInstrument()为查询合约, 这里为查询合约的回调
//该回调会被多次触发, 频繁为查询得到的合约数量
void TradeSpi::OnRspQryInstrument(CThostFtdcInstrumentField *pInstrument, CThostFtdcRspInfoField *pRspInfo, int nRequestID, bool bIsLast){
    string path = ROOT + "/contract";
    string file = path + "/contract.csv";
    fstream outfile;
    outfile.open(file, ios::in);
    if(!outfile.is_open()){
        UNLINKED= 1;
        Log("open file failed, file or directory not exist, file: " + file + ", program will go to handle this, contract.cpp #126");
        
        fstream directory;
        directory.open(path, ios::in);
        if(!directory.is_open()){
            LogError("directory not exist, go to create directory, path:" + path + ", contract.cpp #131");
            string cmd = "mkdir -p " + path;
            system(cmd.c_str());
        }else{
            directory.close();
        }

        outfile.open(file, ios::app);
        if(!outfile.is_open()){
            LogError("create file or directory failed, file: " + file + ", path: " + path + ", contract.cpp #140");
        }
        outfile << "number,"    << "ExchangeID,"        << "InstrumentID,"      << "InstrumentName,"    << "ProductID,"
            << "ProductClass,"  << "CreateDate,"        << "OpenDate,"          << "DeliveryYear,"      << "DeliveryMonth,"
            << "ExpireDate,"    << "StartDelivDate,"    << "EndDelivDate,"      << "InstLifePhase,"     << "IsTrading,"
            << "OptionsType,"   << "ExchangeInstID,"    << "UnderlyingInstrID," << "PriceTick"          <<endl;
        outfile.close();
    }else{
        outfile.close();
        if(UNLINKED == 0){
            unlink(file.c_str());
            UNLINKED = 1;
            
            outfile.open(file, ios::app);
            if(!outfile.is_open()){
                LogError("create file or directory failed, file: " + file + ", path: " + path + ", contract.cpp #152");
            }
            outfile << "number,"    << "ExchangeID,"        << "InstrumentID,"      << "InstrumentName,"    << "ProductID,"
                << "ProductClass,"  << "CreateDate,"        << "OpenDate,"          << "DeliveryYear,"      << "DeliveryMonth,"
                << "ExpireDate,"    << "StartDelivDate,"    << "EndDelivDate,"      << "InstLifePhase,"     << "IsTrading,"
                << "OptionsType,"   << "ExchangeInstID,"    << "UnderlyingInstrID," << "PriceTick"          <<endl;
            outfile.close();
        }
    }
    
    outfile.open(file, ios::app);
    
    outfile << intInstrumentCount       << "," << pInstrument->ExchangeID       << ","
        << pInstrument->InstrumentID    << "," << pInstrument->InstrumentName   << ","
        << pInstrument->ProductID       << "," << pInstrument->ProductClass     << ","
        << pInstrument->CreateDate      << "," << pInstrument->OpenDate         << ","
        << pInstrument->DeliveryYear    << "," << pInstrument->DeliveryMonth    << ","
        << pInstrument->ExpireDate      << "," << pInstrument->StartDelivDate   << ","
        << pInstrument->EndDelivDate    << "," << pInstrument->InstLifePhase    << ","
        << pInstrument->IsTrading       << "," << pInstrument->OptionsType      << ","
        << pInstrument->ExchangeInstID  << "," << pInstrument->UnderlyingInstrID<< ","
        << pInstrument->PriceTick       << endl;
         
    outfile.close();
    
    intInstrumentCount++;

    if(bIsLast){
        Log("fetch contract, number: " + to_string(intInstrumentCount));
        
        struct tm *nowtime;
        time_t lt1;
        time( &lt1 );
        nowtime     = localtime(&lt1);
        char tmpbuffer[128];
        strftime(tmpbuffer, 128, "contract_%Y-%m-%d_%H-%M-%S.csv", nowtime);
        string file2= path + "/" + tmpbuffer;
        string cmd2 = "cp " + file + " " + file2;
        system(cmd2.c_str());
        
        exit(0);
    }
}


//回调的错误信息
bool TradeSpi::IsErrorResponseInformation(CThostFtdcRspInfoField *pRspInfo){
    bool bResult = ((pRspInfo) && (pRspInfo->ErrorID != 0));
    if (bResult){
        string errorId      = to_string(pRspInfo->ErrorID);
        string errorMessage = pRspInfo->ErrorMsg;
        LogError("ErrorID: " + errorId + ", ErrorMsg: " + errorMessage);
    }
    return bResult;
}


//日志
void TradeSpi::Log(string message){
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
void TradeSpi::LogError(string message){
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


//--------------------------------------------


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
    
    ofstream outfile;
    outfile.open(LOGFILE, ios::app);
    outfile << now << "\t" << message << "\n" << endl;
    outfile.close();
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
* 判断目录log/contract是否存在
* 设定日志文件的路径名称
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
    _file.open(ROOT + "/log/contract", ios::in);
    if(!_file){
        string cmd = "mkdir -p " + ROOT + "/log/contract";
        system(cmd.c_str());
    }else{
        _file.close();
    }
    
    struct tm *nowtime;
    time_t lt1;
    time( &lt1 );
    nowtime = localtime(&lt1);

    char tmpbuffer[128];
    strftime(tmpbuffer, 128, "./log/contract/log_%Y-%m-%d_%H-%M-%S.log", nowtime);
    LOGFILE         += ROOT + "/" + tmpbuffer;
    LOGFILE_ERROR   += ROOT + "/log/contract/error.log";
    
    int pid = getpid();
    ofstream outfile;
    outfile.open(ROOT + "/log/contract/pid.log", ios::out);
    outfile << pid;
    outfile.close();
}


/*
* main()方法, 程序的入口
* argv[1], BROKER_ID
* argv[2], INVESTOR_ID
* argv[3], PASSWORD
* argv[4], FRONT_ADDRESS
* argv[5], PRINT
*/
int main(int argc, char *argv[]){
    init(argv);
    
    if(argc < 6){
        log_error("error, agrc count is incorrect, less than 7, contract.cpp #458");
        return 0;
    }
    
    strcpy(BROKER_ID,   argv[1]);
    strcpy(INVESTOR_ID, argv[2]);
    strcpy(PASSWORD,    argv[3]);
    
    int length = strlen(argv[4]);
    char FRONT_ADDRESS[length]  = {0};
    strcpy(FRONT_ADDRESS,  argv[4]);
    
    PRINT = std::atoi( argv[5] );
    if(PRINT !=0 && PRINT !=1){
        log_error("error, the fifth agrc is incorrect about PRINT, it must 0 or 1, contract.cpp #475");
        return 0;
    }
    
    pointerTradeApi = CThostFtdcTraderApi::CreateFtdcTraderApi();
    
    CThostFtdcTraderSpi *pointerTradeSpi = new TradeSpi();
    pointerTradeApi->RegisterSpi(pointerTradeSpi);
    pointerTradeApi->RegisterFront(FRONT_ADDRESS);
    pointerTradeApi->Init();
    pointerTradeApi->Join();

    return 0;
}

