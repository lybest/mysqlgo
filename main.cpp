#include "coroutine.h"
#include "sql.h"
#include <boost/date_time/gregorian/gregorian.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/thread.hpp>
#include <memory>
#include <stdio.h>
#include "WaitGroup.h"

using namespace std;
using namespace boost::gregorian;
// 数据处理函数
// @sess session标识
// @data 收到的数据起始指针
// @bytes 收到的数据长度
// @returns: 返回一个size_t, 表示已处理的数据长度. 当返回-1时表示数据错误, 链接即会被关闭.

int main()
{
    // Step1: 创建一个Server对象

    sql::ConnectOptionsMap connection_properties;

    connection_properties["hostName"] = "10.3.247.59";
    connection_properties["userName"] = "web";
    connection_properties["password"] = "web123!@#";
    connection_properties["schema"] = "db15";
    connection_properties["port"] = 3306;
    connection_properties["OPT_RECONNECT"] = true;
    connection_properties["maxOpen"] = 10;
    connection_properties["maxIdle"] = 3;

    std::shared_ptr<DB> db(new DB());
    db->Open(connection_properties);

    boost::thread_group tg;

    WaitGroup wg;
    go[=]
    {
        auto pre = boost::posix_time::microsec_clock::local_time();

        wg.Wait();
        auto now = boost::posix_time::microsec_clock::local_time();
        cout << now << endl;
        cout << now - pre << endl;
        // db->Close();
    };
    for (int i = 0; i < 20; ++i) {
        wg.Add();
        go[&]
        {
            cout << "run here" << endl;
            for (int i = 0; i < 1000; i++) {
                if (i == 500) {
                    //db->Close();
                }
                auto dcResult = db->GetConn();
                auto stmtResult = get<0>(dcResult)->prepareStatement("select * from user where uuid = ?");
                get<0>(stmtResult)->setInt(1, 1);
                auto tup = get<0>(stmtResult)->executeQuery();
                auto result = get<0>(tup);
                if (result == nullptr) {
                    // cout << "null " << endl;
                    continue;
                }
                while (result->next()) {
                    cout << result->getString("uuid") << endl;
                }
                //cout << "complelte" <<endl;
            }
            wg.Done();
        };
    }
    //boost::thread_group tg;
    for (int i = 0; i < 3; ++i)
        tg.create_thread([] { co_sched.RunUntilNoTask(); });
    co_sched.RunUntilNoTask();

    /*
    for (int i = 0; i < 100; ++i)
        tg.create_thread([=] {
            while (1) {
              //sleep(10);
              try{
                   auto result = db->executeQuery("select * from activity");
                   while(result->next()){
                       cout << result->getString("id") << endl;
                   }
              }catch(...){
                  
              }
               
            } });
            */

    //co_sched.RunUntilNoTask();

    return 0;
}
