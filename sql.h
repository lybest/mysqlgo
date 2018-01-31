#include "boost/date_time/posix_time/posix_time.hpp"
#include "coroutine.h"
#include "mysql_driver.h"
#include <atomic>
#include <cppconn/driver.h>
#include <cppconn/exception.h>
#include <cppconn/resultset.h>
#include <cppconn/statement.h>
#include <cppconn/prepared_statement.h>

#include <iostream>
#include <memory>
#include <mutex>
#include <queue>

using namespace std;
using namespace boost::gregorian;
using namespace boost::posix_time;

enum errCode {
    nil = 0,
    NoFreeConn = 1,
    ConnClosed = 2,
};

class DB;
class Statement;
class PreparedStatement;
class driverConn : public std::enable_shared_from_this<driverConn> {

public:
    driverConn(std::shared_ptr<sql::Connection>, std::shared_ptr<DB>);
    ~driverConn();
    int Close();
    std::shared_ptr<Statement> createStatement();
    std::shared_ptr<PreparedStatement> prepareStatement(const sql::SQLString& sql);
    void setAutoCommit(bool autoCommit);
    void rollback();

private:
    std::shared_ptr<sql::Connection> ci;
    std::shared_ptr<DB> db;
    bool closed;
    int err; //记录最后一次执行结果

public:
    friend class DB;
    friend class Statement;
    friend class PreparedStatement;
};

class Statement {
public:
    Statement(sql::Statement* st, std::shared_ptr<driverConn> c);
    ~Statement();

    std::shared_ptr<sql::ResultSet> executeQuery(const sql::SQLString& sql);
    int executeUpdate(const sql::SQLString& sql);

private:
    sql::Statement* stmt;
    std::shared_ptr<driverConn> dc;
};

class PreparedStatement {
public:
    PreparedStatement(sql::PreparedStatement* st, std::shared_ptr<driverConn> c);
    ~PreparedStatement();

    std::shared_ptr<sql::ResultSet> executeQuery(const sql::SQLString& sql);
    int executeUpdate(const sql::SQLString& sql);

private:
    sql::PreparedStatement* stmt;
    std::shared_ptr<driverConn> dc;
};

struct Requst {
    Requst()
        : addtime(0)
        , ch(2)
    {
        assert(0);
    }
    Requst(uint64_t t)
        : ch(2)
        , addtime(t)
    {
    }
    co_chan<std::shared_ptr<sql::Connection>> ch;
    uint64_t addtime;
};

class DB : public std::enable_shared_from_this<DB> {

public:
    DB();
    ~DB();
    void Open(const sql::ConnectOptionsMap& properties);

    std::shared_ptr<driverConn> GetConn();
    void ReturnConn(std::shared_ptr<sql::Connection> ci, int err);
    void Close();

    std::shared_ptr<sql::ResultSet> executeQuery(const sql::SQLString& sql);
    int executeUpdate(const sql::SQLString& sql);

    friend class driverConn;

private:
    void maybeOpenNewConnLocked();
    void connectionOpener();
    void requestTimer();
    void updateTime();
    std::shared_ptr<sql::Connection> openNewConn();

private:
    sql::mysql::MySQL_Driver* driver;
    sql::ConnectOptionsMap properties;
    bool closed;
    std::atomic<int> numOpen;
    co_mutex mu; // protects following fields
    int maxIdle; // <= 0 means unlimited
    int maxOpen; // <= 0 means unlimited
    uint64_t reqKey;

    map<uint64_t, Requst> connRequests;
    std::queue<std::shared_ptr<sql::Connection>> freeConn;
    co_chan<bool> openerCh;

    uint64_t millSecond;
    uint64_t second;
    ptime cur;
};
