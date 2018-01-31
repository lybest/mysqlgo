#include "sql.h"

driverConn::driverConn(std::shared_ptr<sql::Connection> c, std::shared_ptr<DB> d)
    : ci(c)
    , db(d)
    , closed(false)
    , err(nil)
{
}

driverConn::~driverConn()
{
    db->ReturnConn(ci, err);
}

int driverConn::Close()
{
    if (closed) {
        return ConnClosed; //sql: duplicate driverConn close"
    }
    closed = true;
    err = ConnClosed;
    ci->close();
    return nil;
}

std::shared_ptr<Statement> driverConn::createStatement()
{
    std::shared_ptr<Statement> stmt(new Statement(ci->createStatement(), shared_from_this()));
    return stmt;
}

std::shared_ptr<PreparedStatement> driverConn::prepareStatement(const sql::SQLString& sql)
{
    std::shared_ptr<PreparedStatement> stmt(new PreparedStatement(ci->prepareStatement(sql), shared_from_this()));
    return stmt;
}

void driverConn::setAutoCommit(bool autoCommit)
{
    ci->setAutoCommit(autoCommit);
}

void driverConn::rollback()
{
    ci->rollback();
}

Statement::Statement(sql::Statement* st, std::shared_ptr<driverConn> c)
    : stmt(st)
    , dc(c)
{
}

Statement::~Statement()
{
    if (stmt != NULL) {
        delete stmt;
    }
}

std::shared_ptr<sql::ResultSet> Statement::executeQuery(const sql::SQLString& sql)
{
    try {
        std::shared_ptr<sql::ResultSet> rs(stmt->executeQuery(sql));
        return rs;
    } catch (sql::SQLException& e) {
        dc->err = e.getErrorCode();
        cout << "# ERR: " << e.what();
        //cout << " (MySQL error code: " << e.getErrorCode();
        //cout << ", SQLState: " << e.getSQLState() << " )" << endl;
    }
    return nullptr;
}

int Statement::executeUpdate(const sql::SQLString& sql)
{
    try {
        return stmt->executeUpdate(sql);
    } catch (sql::SQLException& e) {
        dc->err = e.getErrorCode();
        cout << "# ERR: " << e.what();
        //cout << " (MySQL error code: " << e.getErrorCode();
        //cout << ", SQLState: " << e.getSQLState() << " )" << endl;
    }
    return -1;
}

PreparedStatement::PreparedStatement(sql::PreparedStatement* st, std::shared_ptr<driverConn> c)
    : stmt(st)
    , dc(c)
{
}

PreparedStatement::~PreparedStatement()
{
    if (stmt != NULL) {
        delete stmt;
    }
}

std::shared_ptr<sql::ResultSet> PreparedStatement::executeQuery(const sql::SQLString& sql)
{
    try {
        std::shared_ptr<sql::ResultSet> rs(stmt->executeQuery(sql));
        return rs;
    } catch (sql::SQLException& e) {
        dc->err = e.getErrorCode();
        cout << "# ERR: " << e.what();
        //cout << " (MySQL error code: " << e.getErrorCode();
        //cout << ", SQLState: " << e.getSQLState() << " )" << endl;
    }
    return nullptr;
}

int PreparedStatement::executeUpdate(const sql::SQLString& sql)
{
    try {
        return stmt->executeUpdate(sql);
    } catch (sql::SQLException& e) {
        dc->err = e.getErrorCode();
        cout << "# ERR: " << e.what();
        //cout << " (MySQL error code: " << e.getErrorCode();
        //cout << ", SQLState: " << e.getSQLState() << " )" << endl;
    }
    return -1;
}

DB::DB()
    : closed(false)
    , numOpen(0)
    , maxIdle(0)
    , maxOpen(0)
    , reqKey(0)
    , openerCh(1000)
{
}

DB::~DB()
{
    Close();
}

void DB::Open(const sql::ConnectOptionsMap& properties)
{
    driver = sql::mysql::get_mysql_driver_instance();
    this->properties = properties;
    auto it = properties.find("maxOpen");
    if (it != properties.end()) {
        maxOpen = *(it->second).get<int>();
    }
    it = properties.find("maxIdle");
    if (it != properties.end()) {
        maxIdle = *(it->second).get<int>();
    }
    updateTime();
    go[=] { connectionOpener(); };
    go[=] { requestTimer(); };
}

std::shared_ptr<driverConn> DB::GetConn()
{
    bool open = false;
    {
        std::lock_guard<co_mutex> guard(mu);
        if (closed) {
            return nullptr; //errDBClosed
        }
        if (freeConn.size() > 0) {
            auto conn = freeConn.front();
            freeConn.pop();
            std::shared_ptr<driverConn> dc(new driverConn(conn, shared_from_this()));
            return dc;
        }
        //* 可以打开新连接
        if (maxOpen <= 0 || numOpen < maxOpen) {
            ++numOpen;
            //cout << "numopen" << numOpen;
            open = true;
        }
    }
    if (open) {
        auto conn = openNewConn();
        if (conn) {
            std::shared_ptr<driverConn> dc(new driverConn(conn, shared_from_this()));
            return dc;
        } else {
            //* 通知打开连接
            --numOpen;
            maybeOpenNewConnLocked();
        }
        return nullptr;
    }
    //* 等待空闲连接
    uint64_t reqId;
    Requst req(millSecond);
    {
        std::lock_guard<co_mutex> guard(mu);
        reqId = reqKey++;
        auto res = connRequests.insert(make_pair(reqId, req));
        if (res.second == false) {
            assert(0);
        }
    }

    //cout << "ci yield to get: " << reqId << endl;
    std::shared_ptr<sql::Connection> conn;
    req.ch >> conn;
    if (!conn) {
        //cout << "ci timeout : " << reqId << endl;
        return nullptr;
    }
    //cout << "get ci with: " << reqId << endl;
    std::shared_ptr<driverConn> dc(new driverConn(conn, shared_from_this()));
    return dc;
}

void DB::ReturnConn(std::shared_ptr<sql::Connection> ci, int err)
{
    if (err != nil) {
        ci->close();
        --numOpen;
        maybeOpenNewConnLocked();
        cout << "ReturnConn close  err " << numOpen << endl;
        return;
    }
    ////cout << numOpen << endl;

    std::lock_guard<co_mutex> guard(mu);
    if (closed) {
        cout << "ReturnConn dbclose  err " << numOpen << endl;
        return;
    }
    if (maxOpen > 0 && numOpen > maxOpen) {
        cout << "ReturnConn close  maxOpen " << numOpen << endl;
        --numOpen;
        ci->close();
        return;
    }
    if (connRequests.size() > 0) {
        auto it = connRequests.begin();
        auto req = it->second;
        connRequests.erase(it);
        //cout << "begin send ci to: " << it->first << endl;
        req.ch << ci;
        //cout << "end send ci to: " << it->first << endl;
        return;
    }
    if (maxIdle > 0 && freeConn.size() > maxIdle) {
        //cout << "ReturnConn close  maxIdle " << numOpen << endl;
        --numOpen;
        ci->close();
        return;
    }

    freeConn.push(ci);
    return;
}

std::shared_ptr<sql::ResultSet> DB::executeQuery(const sql::SQLString& sql)
{
    if (closed) {
        return nullptr;
    }
    auto dc = GetConn();
    if (dc == nullptr) {
        return nullptr;
    }
    auto stmt = dc->createStatement();
    return stmt->executeQuery(sql);
}

int DB::executeUpdate(const sql::SQLString& sql)
{
    if (closed) {
        return -1;
    }
    auto dc = GetConn();
    if (dc) {
        auto stmt = dc->createStatement();
        return stmt->executeUpdate(sql);
    }
    return -1;
}

void DB::Close()
{
    std::lock_guard<co_mutex> guard(mu);
    if (closed) { // Make DB.Close idempotent
        return;
    }
    closed = true;
    //* 关闭连接
    while (freeConn.size() != 0) {
        auto conn = freeConn.front();
        conn->close();
        cout << "conn closed" << endl;
        freeConn.pop();
    }
    openerCh << false;

    return;
}

void DB::maybeOpenNewConnLocked()
{

    std::lock_guard<co_mutex> guard(mu);
    if (closed) {
        return;
    }
    auto numRequests = connRequests.size();
    cout << "numRequests" << numRequests <<endl;
    if (maxOpen > 0) {
        auto numCanOpen = maxOpen - numOpen;
        if (numRequests > numCanOpen) {
            numRequests = numCanOpen;
        }
    }
    numOpen += numRequests; //* 直接增加numOpen
    cout << "numRequests" << numRequests <<endl;
    while (numRequests > 0) {
        numRequests--;
        openerCh << true;
    }
}

void DB::connectionOpener()
{
    while (1) {
        bool open;
        openerCh >> open;
        if (closed) {
            return;
        }
        cout << "connectionOpener "<<endl;
        auto conn = openNewConn();
        if (conn) {
            ReturnConn(conn, nil);
        } else {
            --numOpen;
            maybeOpenNewConnLocked();
        }
    }
}

std::shared_ptr<sql::Connection> DB::openNewConn()
{
    if (closed) {
        return nullptr;
    }
    try {
        std::shared_ptr<sql::Connection> conn(driver->connect(properties));
        return conn;
    } catch (sql::SQLException& e) {
        //cout << "# ERR: " << e.what();
        //cout << " (MySQL error code: " << e.getErrorCode();
        //cout << ", SQLState: " << e.getSQLState() << " )" << endl;
    }
    return nullptr;
}
void DB::requestTimer()
{
    while (1) {
        if (closed) {
            return;
        }
        //* sleep 200ms
        co_sleep(200);
        updateTime();
        std::lock_guard<co_mutex> guard(mu);
        while (1) {
            auto it = connRequests.begin();
            if (it == connRequests.end()) {
                break;
            }
            if (it->second.addtime + 2000 < millSecond) {
                cout << "timeout now" << millSecond << " pre " << it->second.addtime << "  reqid " << it->first << endl;
                it->second.ch << nullptr;
                connRequests.erase(it);
            } else {
                break;
            }
        }
    }
}

void DB::updateTime()
{
    cur = microsec_clock::local_time();
    const static ptime begin(date(1970, 1, 1));
    auto duration = cur - begin;
    millSecond = duration.total_milliseconds();
    second = duration.total_seconds();
    //cout << "cur: " << millSecond << endl;
}
