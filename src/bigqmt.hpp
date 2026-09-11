// MiniQMT-style Big QMT Redis-RPC client: a C++ port of the parts of
// bigqmt_signal_trader.xtquant_compat that test_callback.py exercises.
//
// Talks the same wire protocol as the Python client, so both can run side by
// side against one QMT bridge:
//   - RPC request  : RPUSH bigqmt:rpc:queue:{account_id}  with payload
//                    "b64s:" + base64(JSON) using the bridge's digit
//                    substitution (see rpc_payload());
//   - RPC response : GET bigqmt:rpc:resp:{account_id}:{request_id}, or
//                    BLPOP bigqmt:rpc:respq:{account_id}:{request_id} with a
//                    1s timeout, envelope {"ok","data","error","server_error"};
//   - exec events  : pub/sub on
//                    bigqmt:{order,trade,order_error,cancel_error}_events:
//                    {account_id}, JSON payloads shaped into MiniQMT-style
//                    callback objects.
//
// No third-party dependencies: POSIX sockets + std threads only.
#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "json.hpp"
#include "redis_resp.hpp"

namespace bigqmt {

// ---------------------------------------------------------------------------
// Human-readable labels for console output.
//
// On the wire (and in XtOrder/XtTrade...) these fields stay plain numbers --
// the bridge sends exactly the MiniQMT xtconstant codes. The enum classes
// below give the codes compile-time names, and the label() helpers map a
// number (or enum) to its Chinese text for printing:
//
//     printf("... %s ...", order_status_label(order.order_status));
//     if (order.order_status == OrderStatus::Reported) { ... }
//
// Value/name tables copied from the installed xtquant/xtconstant.py
// (the copy that sits next to this bridge), so a label can only drift if the
// Python package does.
// ---------------------------------------------------------------------------

// 委托方向 order_type (23 买入 / 24 卖出; 融资融券同段代码区)
enum class OrderAction : long long {
    Buy = 23,                  // 买入 / 担保品买入
    Sell = 24,                 // 卖出 / 担保品卖出
    CreditFinBuy = 27,         // 融资买入
    CreditSloSell = 28,        // 融券卖出
    CreditBuySecuRepay = 29,   // 买券还券
    CreditDirectSecuRepay = 30,// 直接还券
    CreditSellSecuRepay = 31,  // 卖券还款
    CreditDirectCashRepay = 32,// 直接还款
    CreditFinBuySpecial = 40,  // 专项融资买入
    CreditSloSellSpecial = 41, // 专项融券卖出
    CreditBuySecuRepaySpecial = 42,  // 专项买券还券
    CreditDirectSecuRepaySpecial = 43,  // 专项直接还券
    CreditSellSecuRepaySpecial = 44,  // 专项卖券还款
    CreditDirectCashRepaySpecial = 45,  // 专项直接还款
};

// 报价方式 price_type (股票常用段)
enum class StockPriceType : long long {
    Latest = 5,                 // 最新价
    Fix = 11,                   // 指定价/限价
    ShConvert5Cancel = 42,      // 最优五档即时成交剩余撤销 [沪][北]
    ShConvert5Limit = 43,       // 最优五档即时成交剩余转限价 [沪][北]
    PeerPriceFirst = 44,        // 对手方最优价格 [沪][深][北]
    MinePriceFirst = 45,        // 本方最优价格 [沪][深][北]
    SzInstBusiRestCancel = 46,  // 即时成交剩余撤销委托 [深]
    SzConvert5Cancel = 47,      // 最优五档即时成交剩余撤销 [深]
    SzFullOrCancel = 48,        // 全额成交或撤销委托 [深]
};

// 委托状态 order_status
enum class OrderStatus : long long {
    Unreported = 48,      // 未报
    WaitReporting = 49,   // 待报
    Reported = 50,        // 已报
    ReportedCancel = 51,  // 已报待撤
    PartSuccCancel = 52,  // 部成待撤
    PartCancel = 53,      // 部撤
    Canceled = 54,        // 已撤
    PartSucc = 55,        // 部成
    Succeeded = 56,       // 已成
    Junk = 57,            // 废单
    Unknown = 255,        // 未知
};

// 账户类型 account_type (xtconstant: STOCK=2, CREDIT=3)。客户端声明只当
// 本地标签 —— RPC 请求只带 account_id, 类型不发给服务器; 服务器 ping 会
// 回报真实类型 (见 note_server_account_type), 显示时以服务器为准。
enum class AccountType : long long { Security = 2, Credit = 3 };

inline const char* account_type_name(AccountType t) {
    switch (t) {
        case AccountType::Security: return "STOCK";
        case AccountType::Credit: return "CREDIT";
    }
    return "STOCK";
}

// 配置字符串 -> 枚举: 只认 STOCK/CREDIT (大小写不敏感), 其余回落 STOCK。
// 配置加载端已对未知值告警 (apply_env_overrides), 这里保持安静。
inline AccountType account_type_from_name(const std::string& name) {
    std::string up;
    up.reserve(name.size());
    for (char c : name) {
        if (c >= 'a' && c <= 'z') c = static_cast<char>(c - 'a' + 'A');
        up += c;
    }
    return up == "CREDIT" ? AccountType::Credit : AccountType::Security;
}

// 账户连接状态 (on_account_status 的 status)
enum class AccountStatus : long long { Online = 1 };

inline const char* order_type_label(long long v) {
    switch (v) {
        case static_cast<long long>(OrderAction::Buy): return "买入";
        case static_cast<long long>(OrderAction::Sell): return "卖出";
        case static_cast<long long>(OrderAction::CreditFinBuy): return "融资买入";
        case static_cast<long long>(OrderAction::CreditSloSell): return "融券卖出";
        case static_cast<long long>(OrderAction::CreditBuySecuRepay): return "买券还券";
        case static_cast<long long>(OrderAction::CreditDirectSecuRepay): return "直接还券";
        case static_cast<long long>(OrderAction::CreditSellSecuRepay): return "卖券还款";
        case static_cast<long long>(OrderAction::CreditDirectCashRepay): return "直接还款";
        default: return "未知";
    }
}

inline const char* price_type_label(long long v) {
    switch (v) {
        case static_cast<long long>(StockPriceType::Latest): return "最新价";
        case static_cast<long long>(StockPriceType::Fix): return "限价";
        case static_cast<long long>(StockPriceType::ShConvert5Cancel): return "最优五档即成剩撤";
        case static_cast<long long>(StockPriceType::ShConvert5Limit): return "最优五档即成剩转限价";
        case static_cast<long long>(StockPriceType::PeerPriceFirst): return "对手方最优价";
        case static_cast<long long>(StockPriceType::MinePriceFirst): return "本方最优价";
        case static_cast<long long>(StockPriceType::SzInstBusiRestCancel): return "即时成交剩余撤销";
        case static_cast<long long>(StockPriceType::SzConvert5Cancel): return "最优五档即成剩撤(深)";
        case static_cast<long long>(StockPriceType::SzFullOrCancel): return "全额成交或撤销";
        default: return "未知";
    }
}

inline const char* order_status_label(long long v) {
    switch (v) {
        case static_cast<long long>(OrderStatus::Unreported): return "未报";
        case static_cast<long long>(OrderStatus::WaitReporting): return "待报";
        case static_cast<long long>(OrderStatus::Reported): return "已报";
        case static_cast<long long>(OrderStatus::ReportedCancel): return "已报待撤";
        case static_cast<long long>(OrderStatus::PartSuccCancel): return "部成待撤";
        case static_cast<long long>(OrderStatus::PartCancel): return "部撤";
        case static_cast<long long>(OrderStatus::Canceled): return "已撤";
        case static_cast<long long>(OrderStatus::PartSucc): return "部成";
        case static_cast<long long>(OrderStatus::Succeeded): return "已成";
        case static_cast<long long>(OrderStatus::Junk): return "废单";
        case static_cast<long long>(OrderStatus::Unknown): return "未知";
        default: return "未知";
    }
}

// Enum overloads so both forms print nicely.
inline const char* order_type_label(OrderAction v) {
    return order_type_label(static_cast<long long>(v));
}
inline const char* price_type_label(StockPriceType v) {
    return price_type_label(static_cast<long long>(v));
}
inline const char* order_status_label(OrderStatus v) {
    return order_status_label(static_cast<long long>(v));
}

// ---------------------------------------------------------------------------
// OrderId: "an int for MiniQMT, a string for the broker". Holds the exact
// 合同编号 string and the int MiniQMT-style code compares against.
// ---------------------------------------------------------------------------

class OrderId {
public:
    OrderId() = default;
    explicit OrderId(std::string sys_id);

    const std::string& sys_id() const { return sys_id_; }
    long long value() const { return int_value_; }

    // What python's print(order_id) shows: the broker string, or the number
    // when there is no string (OrderId("") prints "0").
    std::string str() const {
        if (!sys_id_.empty()) return sys_id_;
        return std::to_string(int_value_);
    }

private:
    std::string sys_id_;
    long long int_value_ = 0;
};

// ---------------------------------------------------------------------------
// Callback data objects (CompatObject shapes, MiniQMT field names)
// ---------------------------------------------------------------------------

struct XtOrder {
    std::string account_id;
    std::string stock_code;
    OrderAction order_type = static_cast<OrderAction>(0);
    OrderStatus order_status = OrderStatus::Unknown;
    long long order_volume = 0;
    long long traded_volume = 0;
    double price = 0.0;
    double traded_price = 0.0;
    double trade_amount = 0.0;
    std::string order_sysid;
    OrderId order_id;
    std::string strategy_name;
    std::string order_remark;
    long long order_time = 0;  // unix seconds
    std::string status_msg;
    StockPriceType price_type = static_cast<StockPriceType>(0);
    AccountType account_type = AccountType::Security;
    std::string instrument_name;
    std::string secu_account;
    long long offset_flag = 0;
    long long direction = 0;
};

struct XtTrade {
    std::string account_id;
    std::string stock_code;
    OrderAction order_type = static_cast<OrderAction>(0);
    std::string order_sysid;
    OrderId order_id;
    std::string trade_id;
    std::string traded_id;
    long long traded_volume = 0;
    double traded_price = 0.0;
    long long traded_time = 0;  // unix seconds
    double traded_amount = 0.0;
    std::string traded_at;
    std::string strategy_name;
    std::string order_remark;
    AccountType account_type = AccountType::Security;
    std::string instrument_name;
    std::string secu_account;
    double commission = 0.0;
    long long offset_flag = 0;
    long long direction = 0;
};

struct XtOrderError {
    long long error_id = 0;
    std::string error_msg;
    std::string order_sysid;
    std::string order_sys_id;
    OrderId order_id;
    std::string stock_code;
    long long seq = 0;
    std::string order_remark;
    std::string strategy_name;
    long long status = 0;
};

struct XtCancelError {
    long long error_id = 0;
    std::string error_msg;
    std::string order_sysid;
    std::string order_sys_id;
    OrderId order_id;
    std::string stock_code;
    long long seq = 0;
    std::string order_remark;
};

struct XtOrderStockResponse {
    std::string account_id;
    long long seq = 0;
    OrderId order_id;
    std::string order_sysid;
    std::string stock_code;
    std::string strategy_name;
    std::string order_remark;
    std::string error_msg;
};

// 撤单异步回报 (cancel_order_stock_async 的响应对象, 字段镜像 Python compat
// 层 CompatObject)。注意: 回报的是"撤单请求是否被桥受理", 不是最终撤成 ——
// MiniQMT XtCancelOrderResponse 契约: cancel_result=0 受理成功, 非零给出
// 错误码和可读 error_msg。实际撤没撤成仍看委托状态事件 (51 已报待撤 ->
// 53/54 已撤), RPC 异常则走 on_cancel_error。
struct XtCancelOrderStockResponse {
    std::string account_id;
    long long seq = 0;
    bool success = false;  // cancel_order_stock 返回 0 即为 true
    long long cancel_result = -1;  // 0 = 已受理, -1 = 被拒绝
    std::string error_msg;
    std::string order_sysid;   // == order_id 参数 (合同编号原样)
    std::string order_sys_id;  // Python compat 层的冗余别名, 一并镜像
    OrderId order_id;          // 合同编号的 OrderId 形态 (全数字 -> int)
};

struct XtAccountStatus {
    std::string account_id;
    std::string account_type;
    AccountStatus status = static_cast<AccountStatus>(0);
};

// Query results, mirroring the bridge server's snapshots / MiniQMT XtAsset.
// The server reports None for fields a terminal did not provide; that
// "unknown" state is distinct from 0.0, so each XtAsset field carries a
// has_* flag (false = the terminal did not report it).
struct XtAsset {
    std::string account_id;
    double cash = 0.0;            // 可用资金 (不是资金余额)
    double total_asset = 0.0;     // 总资产 == cash + frozen_cash + market_value
    double frozen_cash = 0.0;     // 冻结资金
    double market_value = 0.0;    // 持仓市值
    bool has_cash = false;
    bool has_total_asset = false;
    bool has_frozen_cash = false;
    bool has_market_value = false;
};

// One 持仓 row (server PositionSnapshot fields, MiniQMT XtPosition naming).
struct XtPosition {
    std::string account_id;
    std::string stock_code;     // 完整代码 (服务器已规范化, 带交易所后缀)
    std::string stock_name;
    long long volume = 0;           // 总持仓量
    long long can_use_volume = 0;   // 可用持仓
    long long frozen_volume = 0;    // 冻结持仓
    long long on_road_volume = 0;   // 在途持仓
    long long yesterday_volume = 0; // 昨仓
    long long direction = 48;       // 48 = 多头持仓 (QMT XtDirection)
    double avg_price = 0.0;         // 成本均价 (服务器字段 cost)
    double open_price = 0.0;        // 开仓价 (未上报时服务器给成本价)
    double price = 0.0;             // 最新价
    double market_value = 0.0;      // 市值 (未上报时按 最新价x持仓量 估算)
};

// Base class mirrors xtquant_compat.XtQuantTraderCallback: override the
// virtuals you care about and register an instance with the trader.
//
// NOTE: like the Python client, callbacks may fire from any of three threads
// (the pub/sub event thread, the async-outcome dispatcher thread, and the
// thread that called connect()/subscribe()). Keep handlers short and guard
// shared state.
class XtQuantTraderCallback {
public:
    virtual ~XtQuantTraderCallback() = default;

    virtual void on_disconnected() {}
    virtual void on_stock_order(const XtOrder& order) { (void)order; }
    virtual void on_stock_trade(const XtTrade& trade) { (void)trade; }
    virtual void on_order_error(const XtOrderError& error) { (void)error; }
    virtual void on_cancel_error(const XtCancelError& error) { (void)error; }
    virtual void on_order_stock_async_response(const XtOrderStockResponse& response) {
        (void)response;
    }
    virtual void on_cancel_order_stock_async_response(
        const XtCancelOrderStockResponse& response) {
        (void)response;
    }
    virtual void on_account_status(const XtAccountStatus& status) { (void)status; }
};

class StockAccount {
public:
    StockAccount() = default;
    StockAccount(std::string account_id, AccountType account_type_code = AccountType::Security)
        : account_id(std::move(account_id)), account_type_code(account_type_code) {}

    std::string account_id;
    AccountType account_type_code = AccountType::Security;
};

// ---------------------------------------------------------------------------
// Client configuration.
//
// Load order (later wins):
//   1. built-in defaults below -- deliberately credential-free, a committed
//      binary must never carry real account/password material;
//   2. from_yaml() reads the flat YAML file  (./bigqmt_client_config.yaml by
//      default; override the path with BIGQMT_CONFIG_FILE);
//   3. BIGQMT_* environment variables then override the file:
//      BIGQMT_ACCOUNT_ID BIGQMT_ACCOUNT_TYPE BIGQMT_REDIS_HOST
//      BIGQMT_REDIS_PORT BIGQMT_REDIS_DB BIGQMT_REDIS_USERNAME
//      BIGQMT_REDIS_PASSWORD BIGQMT_RPC_TIMEOUT_SECONDS
// (env wins over the file, the file wins over the built-ins: a committed
// binary can still be repointed without recompilation).
//
// The YAML reader supports a deliberately flat subset: "key: value" lines,
// '#' comments (full-line or after the value), bare / "double-quoted" /
// 'single-quoted' scalars. No nested blocks, lists or flow syntax; the keys
// are exactly the flat field names below. Unknown keys warn on stderr so a
// typo does not silently fall back to defaults.
//
// Do not commit real credentials; prefer the BIGQMT_* env vars in production.
// ---------------------------------------------------------------------------

struct ClientConfig {
    std::string account_id;             // 留空则 call() 报错 (见 call_impl)
    std::string account_type = "STOCK"; // STOCK(普通) / CREDIT(信用); 本地标签
    std::string redis_host = "127.0.0.1";
    int redis_port = 6379;
    int redis_db = 0;
    std::string redis_username;
    std::string redis_password;         // 留空则不 AUTH
    double rpc_timeout_seconds = 30.0;

    // Defaults overridden by BIGQMT_* environment variables only.
    static ClientConfig from_env();

    // YAML file overridden by BIGQMT_* environment variables. path == ""
    // searches BIGQMT_CONFIG_FILE, then ./bigqmt_client_config.yaml.
    // Throws std::runtime_error when the file is unreadable or a value does
    // not parse.
    static ClientConfig from_yaml(const std::string& path = "");
};

// RPC failure shapes, mirroring the Python client's exceptions.
class RpcError : public std::runtime_error {
public:
    explicit RpcError(const std::string& msg) : std::runtime_error(msg) {}
};
class RpcServerRepliedError : public RpcError {
public:
    explicit RpcServerRepliedError(const std::string& msg) : RpcError(msg) {}
};
class RpcTimeoutError : public RpcError {
public:
    explicit RpcTimeoutError(const std::string& msg) : RpcError(msg) {}
};

// ---------------------------------------------------------------------------
// The trader: XtQuantTrader / xt_trader equivalent.
//
// Threading model (mirrors the Python client):
//   - one pub/sub listener thread delivers order/trade/error pushes;
//   - one async-order worker submits queued orders over RPC;
//   - one outcome thread fires on_order_stock_async_response / on_order_error;
//   - the #51 ordering barrier holds an async order's push events until its
//     response has been delivered, so response-before-events holds.
// ---------------------------------------------------------------------------

class BigQmtXtTrader {
public:
    explicit BigQmtXtTrader(ClientConfig config = ClientConfig::from_env());
    ~BigQmtXtTrader();
    BigQmtXtTrader(const BigQmtXtTrader&) = delete;
    BigQmtXtTrader& operator=(const BigQmtXtTrader&) = delete;

    // register_callback(cb) -> 0  (MiniQMT return contract)
    int register_callback(std::shared_ptr<XtQuantTraderCallback> callback);

    // connect(): RPC ping when an account id is configured, then fire a
    // synthesized on_account_status (MiniQMT parity). Returns 0.
    int connect();

    // subscribe(account): (re)start the exec-event listener for the account's
    // channels, then fire on_account_status again. Returns 0.
    int subscribe(const StockAccount& account);

    // Drains queued async orders (bounded), then winds the threads down.
    int stop();

    // MiniQMT semantics: returns a seq immediately; the outcome arrives via
    // on_order_stock_async_response / on_order_error (both carry the seq).
    long long order_stock_async(const StockAccount& account, const std::string& stock_code,
                                OrderAction order_type, long long order_volume,
                                StockPriceType price_type, double price,
                                const std::string& strategy_name,
                                const std::string& order_remark);

    // Block until every queued async order has been submitted and its
    // callback fired. False on timeout.
    bool wait_async_orders(double timeout_seconds = 10.0);

    // 撤单 (MiniQMT cancel_order_stock 契约, 镜像 Python compat 层):
    // order_id 传委托的 合同编号 (XtOrder.order_sysid 原样即可)。同步 RPC,
    // 返回 0 = 桥已受理, -1 = 桥拒绝; 最终成败不看返回值, 而是经委托状态
    // 事件 (51 已报待撤 -> 53/54 已撤) 或 on_cancel_error 回报。被拒原因
    // (桥 CancelResult.message, 如 "cancel returned false") 打到 stderr。
    long long cancel_order_stock(const StockAccount& account,
                                 const std::string& order_id);

    // MiniQMT cancel_order_stock_async 契约, 镜像 Python compat 层: 分配
    // 并返回 seq, 内部同步执行撤单 RPC, 结果在函数返回前经
    // on_cancel_order_stock_async_response 回报 (受理与否, success/
    // cancel_result); RPC/协议异常不抛给调用方, 改走 on_cancel_error。
    // 回报在调用方线程内触发 (python compat 的同步模拟即是如此, 无独立
    // outcome 线程)。
    long long cancel_order_stock_async(const StockAccount& account,
                                       const std::string& order_id);

    // Generic synchronous RPC (ping, query_stock_positions, ...). Returns the
    // response "data" value; throws RpcServerRepliedError / RpcTimeoutError /
    // RpcError on failure.
    Json call(const std::string& method, Json params = Json::make_object());

    const std::string& account_id() const { return config_.account_id; }

    // ---- account queries (synchronous RPC; method names match the bridge) --
    //
    // 账户参数传空 StockAccount() 表示用配置里的 account_id。全部为同步
    // 调用: 内部走 call(), 失败抛 RpcServerRepliedError / RpcTimeoutError /
    // RpcError。

    // 资金概况 (服务器 READ_METHODS "get_asset", 别名 query_stock_asset)。
    XtAsset get_asset(const StockAccount& account = StockAccount());

    // 全部持仓列表 ("get_positions", 别名 query_stock_positions)。服务器
    // 返回按股票代码索引的映射, 这里统一展开成列表。
    std::vector<XtPosition> get_positions(const StockAccount& account = StockAccount());

    // 单股持仓 ("query_stock_position")。返回 false = 无该股持仓 (服务器
    // data 为 null); 找到时 *out 被填充并返回 true。RPC 失败仍抛异常。
    bool query_stock_position(const StockAccount& account,
                              const std::string& stock_code, XtPosition* out);

    // 当日委托列表 ("query_orders")。strategy_name 为空 = 查该账户全部委托
    // (服务器语义: 空串不过滤); 非空则只返回该策略名下的。cancelable_only
    // = true 时只返回仍可撤单的委托。
    std::vector<XtOrder> query_orders(const StockAccount& account = StockAccount(),
                                      const std::string& strategy_name = "",
                                      bool cancelable_only = false);

    // 当日成交列表 ("query_trades")。strategy_name 语义同 query_orders。
    std::vector<XtTrade> query_trades(const StockAccount& account = StockAccount(),
                                      const std::string& strategy_name = "");

    // 官方 get_history_trade_detail_data(accountID, accountType, datatype,
    // startDate, endDate) 的透传: detail_type 为 "ORDER" / "DEAL"(默认),
    // 日期格式 "YYYYMMDD"。返回原始 JSON (字段是 QMT 官方 m_* 命名, 不在此
    // 强行映射, 需要时按官方属性名取)。
    Json get_history_trade_detail_data(const StockAccount& account,
                                       const std::string& detail_type,
                                       const std::string& start_date,
                                       const std::string& end_date);

    // 官方 get_value_by_order_id(orderId, ...) 的透传: 按委托号查单个官方
    // 委托/成交对象。detail_type 默认 "ORDER", "DEAL" 查成交。返回原始
    // JSON; 服务器查不到时 data 为 null。等价地也可传 order_sysid 格式。
    Json get_value_by_order_id(const StockAccount& account,
                               const std::string& order_id,
                               const std::string& detail_type = "ORDER");

    // 官方 get_last_order_id(...) 的透传: 返回最近一次委托号字符串; 查不到
    // 时服务器返回 "-1"。detail_type 默认 "ORDER"。
    std::string get_last_order_id(const StockAccount& account,
                                  const std::string& detail_type = "ORDER");

private:
    // ---- pipeline plumbing --------------------------------------------------

    // One blocking order job. seq 0 is the shutdown sentinel.
    struct OrderJob {
        long long seq = 0;
        StockAccount account;
        std::string stock_code;
        long long order_type = 0;
        long long order_volume = 0;
        long long price_type = 0;
        double price = 0.0;
        std::string strategy_name;
        std::string order_remark;
    };

    // One async outcome (response or submit error). seq 0 is the sentinel.
    struct OutcomeUnit {
        bool is_error = false;
        long long seq = 0;
        std::string remark;
        std::string stock_code;
        std::string strategy_name;
        std::string order_remark;
        std::string order_sys_id;   // response path
        std::string user_order_id;  // response path
        bool wait_for_sysid = false;
        long long error_id = 0;     // error path
        std::string error_msg;      // error path
    };

    // issue #51 barrier entry: events of an in-flight async order are held
    // here (keyed by remark) until its response fires.
    struct BarrierEntry {
        long long seq = 0;
        double deadline = 0.0;  // monotonic seconds
        std::vector<std::string> sys_ids;
        struct HeldEvent {
            enum class Type : uint8_t { Order, Trade, OrderError, CancelError };
            Type type = Type::Order;
            XtOrder order;
            XtTrade trade;
            XtOrderError order_error;
            XtCancelError cancel_error;
        };
        std::vector<HeldEvent> events;
    };

    // One guarded queue + shutdown state per worker pipeline. A pipeline only
    // ever uses one of the two deques (order worker -> order_jobs, outcome
    // worker -> outcomes); the type is shared so stop() can wait on one shape.
    struct Pipeline {
        std::mutex m;
        std::condition_variable cv;
        std::deque<OrderJob> order_jobs;
        std::deque<OutcomeUnit> outcomes;
        bool stopping = false;
        bool exited = false;
    };

    ClientConfig config_;
    std::shared_ptr<XtQuantTraderCallback> callback_;
    std::mutex callback_mutex_;

    std::string server_account_type_;
    std::string declared_account_type_;

    RedisConnection cmd_conn_;  // guarded by cmd_mutex_
    std::mutex cmd_mutex_;

    std::atomic<long long> async_seq_{0};
    std::atomic<long long> async_pending_{0};  // queued jobs not yet fired
    Pipeline order_pipe_;    // carries OrderJobs  (guarded by order_pipe_.m)
    Pipeline outcome_pipe_;  // carries OutcomeUnits (guarded by outcome_pipe_.m)
    std::thread order_thread_;
    std::thread outcome_thread_;

    std::thread event_thread_;
    std::atomic<bool> event_running_{false};
    std::atomic<bool> event_thread_exited_{false};

    std::mutex barrier_mutex_;
    std::deque<std::pair<std::string, BarrierEntry>> barriers_;  // keyed by remark

    // ---- helpers ------------------------------------------------------------
    static std::string rpc_payload(const Json& request);  // "b64s:..." wire form
    Json call_impl(const std::string& method, const Json& params,
                   double timeout_seconds);  // caller holds cmd_mutex_
    // cancel RPC with the bridge's own rejection reason extracted from the
    // reply data (CancelResult.message and friends). Returns 0 = 受理 /
    // -1 = 被拒, exactly like cancel_order_stock; *reason is filled only on
    // rejection (empty when the bridge gave no message).
    long long cancel_rpc(const StockAccount& account, const std::string& order_id,
                         std::string* reason);
    void send_cmd(const std::vector<std::string>& argv);  // caller holds cmd_mutex_
    void expect_ok(const std::string& what);              // caller holds cmd_mutex_

    void event_loop();
    void event_loop_redis(RedisConnection& conn);
    void on_event_json(const Json& event);
    void deliver_event(const Json& event);
    void fire_account_status();
    void note_server_account_type(const Json& ping_data);

    bool arm_barrier(const std::string& remark, long long seq);
    void release_barrier(const std::string& remark, long long seq);
    void sweep_barriers();
    void deliver_barrier_events(const BarrierEntry& entry);
    bool hold_if_pending(const Json& event);

    void order_worker_loop();
    void outcome_worker_loop();
    void submit_order(const OrderJob& job);
    // Full RPC round trip for one order submit (the "order_stock" method).
    // Returns the server-side 合同编号 text, or "-1" when order_stock returned
    // -1 (submit failed).
    std::string order_stock_result(const StockAccount& account,
                                   const std::string& stock_code,
                                   long long order_type, long long order_volume,
                                   long long price_type, double price,
                                   const std::string& strategy_name,
                                   const std::string& order_remark,
                                   bool wait_settlement);
    void fire_outcome(const OutcomeUnit& unit);
    std::string learn_sysid(const std::string& remark, double max_wait_seconds);

    void start_pipelines();
    // Waits up to budget_seconds for the pipeline to exit, then joins its
    // thread (detaching only if it is still stuck, so stop() is bounded).
    void join_pipeline(Pipeline& pipe, std::thread& t, double budget_seconds,
                       const char* what);

    static double monotonic_now() {
        return std::chrono::duration<double>(
                   std::chrono::steady_clock::now().time_since_epoch())
            .count();
    }
};

}  // namespace bigqmt
