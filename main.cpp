// C++ 移植版 test_callback.py:
//
//     xt_trader = XtQuantTrader(run_path, session_id)
//     xt_trader.register_callback(MyCallback())
//     ...
//     acc = StockAccount(config["account_id"], "STOCK")
//     xt_trader.subscribe(acc)
//
// 行为与 Python 版一致:
//   - configure() -> ClientConfig::from_env()(默认值即 bigqmt_signal_trader_client_config.py)
//   - 注册回调, connect() 后先主动推送一次账户状态,
//   - 在 exec_events 事件线程里持续订阅
//     bigqmt:{order,trade,order_error,cancel_error}_events:{account_id}
//     并触发委托回报 / 成交回报 / 委托失败 / 撤单失败回调,
//   - order_stock_async 经独立 worker 线程走 Redis RPC 提交, 由 outcome
//     线程触发 on_order_stock_async_response / on_order_error (带 seq),
//   - 演示撤单: 连下两笔限价单, 等回执拿到合同编号后按编号撤单, 受理与否
//     经 on_cancel_order_stock_async_response 回报 (撤单回报), 实际撤成看
//     委托状态事件 (51 已报待撤 -> 53/54 已撤), 失败看 on_cancel_error,
//   - Ctrl-C 后调 trader.stop() 收尾 (Python 版按 KeyboardInterrupt 退出,
//     daemon 线程随之消亡; 这里显式 stop 保证排队委托有界排空).

#include <chrono>
#include <csignal>
#include <cstdio>
#include <map>
#include <memory>
#include <string>
#include <thread>

#include "src/bigqmt.hpp"

using namespace bigqmt;

// 与 MyCallback 一一对应: 委托回报 / 成交回报 / 委托失败 / 撤单失败 /
// 异步下单回报 / 撤单回报 / 账户状态。
class MyCallback : public XtQuantTraderCallback {
public:
    void on_disconnected() override {
        printf("[callback] on_disconnected\n");
    }

    void on_stock_order(const XtOrder& order) override {
        printf("[callback] 委托回报   stock=%s 方向=%s(%lld) 状态=%s(%lld) "
               "委托量=%lld 已成量=%lld 委托价=%.3f 成交价=%.3f "
               "order_sysid=%s order_id=%s order_remark=%s status_msg=%s\n",
               order.stock_code.c_str(),
               order_type_label(order.order_type),
               static_cast<long long>(order.order_type),
               order_status_label(order.order_status),
               static_cast<long long>(order.order_status),
               static_cast<long long>(order.order_volume),
               static_cast<long long>(order.traded_volume), order.price,
               order.traded_price, order.order_sysid.c_str(),
               order.order_id.str().c_str(), order.order_remark.c_str(),
               order.status_msg.c_str());
    }

    void on_stock_trade(const XtTrade& trade) override {
        printf("[callback] 成交回报   stock=%s 方向=%s(%lld) 成交量=%lld "
               "成交价=%.3f 成交额=%.3f order_sysid=%s trade_id=%s order_remark=%s\n",
               trade.stock_code.c_str(),
               order_type_label(trade.order_type),
               static_cast<long long>(trade.order_type),
               static_cast<long long>(trade.traded_volume), trade.traded_price,
               trade.traded_amount, trade.order_sysid.c_str(), trade.trade_id.c_str(),
               trade.order_remark.c_str());
    }

    void on_order_error(const XtOrderError& error) override {
        printf("[callback] 委托失败   error_id=%lld error_msg=%s order_sysid=%s "
               "stock=%s order_remark=%s\n",
               static_cast<long long>(error.error_id), error.error_msg.c_str(),
               error.order_sysid.c_str(), error.stock_code.c_str(),
               error.order_remark.c_str());
    }

    void on_cancel_error(const XtCancelError& error) override {
        printf("[callback] 撤单失败   error_id=%lld error_msg=%s order_sysid=%s "
               "stock=%s order_remark=%s\n",
               static_cast<long long>(error.error_id), error.error_msg.c_str(),
               error.order_sysid.c_str(), error.stock_code.c_str(),
               error.order_remark.c_str());
    }

    void on_order_stock_async_response(const XtOrderStockResponse& response) override {
        printf("[callback] 异步下单回报 seq=%lld stock=%s order_id=%s order_sysid=%s "
               "order_remark=%s\n",
               static_cast<long long>(response.seq), response.stock_code.c_str(),
               response.order_id.str().c_str(), response.order_sysid.c_str(),
               response.order_remark.c_str());
        note_sysid(response.order_remark, response.order_sysid);
    }

    void on_cancel_order_stock_async_response(
        const XtCancelOrderStockResponse& response) override {
        printf("[callback] 撤单回报   seq=%lld success=%d cancel_result=%lld "
               "order_sysid=%s order_id=%s error_msg=%s\n",
               static_cast<long long>(response.seq), static_cast<int>(response.success),
               static_cast<long long>(response.cancel_result),
               response.order_sysid.c_str(), response.order_id.str().c_str(),
               response.error_msg.c_str());
    }

    void on_account_status(const XtAccountStatus& status) override {
        printf("[callback] 账户状态   account_id=%s account_type=%s status=%lld\n",
               status.account_id.c_str(), status.account_type.c_str(),
               static_cast<long long>(status.status));
    }

    // ---- 撤单测试辅助: 收集异步下单回执里的 remark -> 合同编号 ------------
    // 回执经内部 outcome 线程触发, 主线程 wait_async_orders() 返回后调用
    // sysid_for() 取数, 因此这里需要互斥保护。
    std::string sysid_for(const std::string& remark) {
        std::lock_guard<std::mutex> lock(sysid_mutex_);
        auto it = sysid_by_remark_.find(remark);
        return it == sysid_by_remark_.end() ? std::string() : it->second;
    }

private:
    void note_sysid(const std::string& remark, const std::string& sysid) {
        if (remark.empty() || sysid.empty()) return;
        std::lock_guard<std::mutex> lock(sysid_mutex_);
        sysid_by_remark_[remark] = sysid;
    }

    std::mutex sysid_mutex_;
    std::map<std::string, std::string> sysid_by_remark_;
};

namespace {

volatile std::sig_atomic_t g_running = 1;

extern "C" void on_signal(int) { g_running = 0; }

}  // namespace

int main() {
    // configure() 等价物: 读取根目录 bigqmt_client_config.yaml (路径可用
    // BIGQMT_CONFIG_FILE 覆盖), 之后 BIGQMT_* 环境变量可再覆盖文件里的值。
    ClientConfig config;
    try {
        config = ClientConfig::from_yaml();
    } catch (const std::exception& e) {
        std::fprintf(stderr,
                     "配置加载失败: %s\n"
                     "请确认项目根目录存在 bigqmt_client_config.yaml,\n"
                     "或用 BIGQMT_CONFIG_FILE 指定配置文件路径,\n"
                     "或直接用 BIGQMT_* 环境变量提供各配置项。\n",
                     e.what());
        return 1;
    }

    // Python 版在这里 session_id=int(time.time()); 本实现不需要 session id,
    // request_id 由客户端每次 RPC 自行生成 (uuid4 hex)。
    BigQmtXtTrader trader(config);

    auto callback = std::make_shared<MyCallback>();
    trader.register_callback(callback);

    // 账户类型来自配置 (STOCK/CREDIT, yaml 的 account_type 或环境变量
    // BIGQMT_ACCOUNT_TYPE), 只作本地标签 —— RPC 请求只带 account_id, 服务器
    // ping 回报的真实类型会覆盖显示 (见 on_account_status)。
    StockAccount acc(config.account_id,
                     account_type_from_name(config.account_type));
    printf("connecting with account_id=%s account_type=%s redis=%s:%d db=%d\n",
           config.account_id.c_str(), account_type_name(acc.account_type_code),
           config.redis_host.c_str(), config.redis_port, config.redis_db);

    trader.connect();
    trader.subscribe(acc);

    // 查询失败不应直接崩掉整个 demo (如断线/桥忙时), 打印原因后继续。
    try {
        auto asset = trader.get_asset();                // 资金
        printf("总资产=%.2f 可用=%.2f 市值=%.2f\n",
            asset.total_asset, asset.cash, asset.market_value);

        for (auto& pos : trader.get_positions())        // 持仓
            printf("%s 持仓=%lld 可用=%lld 成本=%.3f\n", pos.stock_code.c_str(),
                pos.volume, pos.can_use_volume, pos.avg_price);
    } catch (const std::exception& e) {
        std::fprintf(stderr, "账户查询失败: %s\n", e.what());
    }

    // ---- 下单 + 撤单演示 ---------------------------------------------------
    // 连下两笔同价限价单(价格低于现价通常不成交, 撤单才有意义; 若高于现价
    // 会秒成, 撤单自然无效 —— 看事件输出即可判断)。每笔用 remark 区分,
    // 回执里带合同编号, 之后按编号逐笔撤单。
    // 撤单: cancel_order_stock_async 的回报回调里 cancel_result=0 只代表
    // "桥已受理", 实际撤成看委托状态事件 (51 已报待撤 -> 53/54 已撤),
    // 桥拒绝/RPC 异常看 [callback] 撤单失败。
    // 第一笔普通买入作对照; 第二笔为融资买入。注意: 桥对 27 等两融类型
    // 原样转发 passorder (不会悄悄降级成普通买入, 那是已知 bug 行为),
    // 是否受理取决于账户的两融权限; 而事件/查询里的方向恒为 买入(23)/
    // 卖出(24) —— 事件只带买卖侧, 不带两融细分 (python compat 同样),
    // 确认是否按融资记账需查 QMT 客户端委托明细。
    struct OrderSpec {
        const char* remark;
        OrderAction action;
        long long volume;
        double price;
    };
    static const OrderSpec kOrderSpecs[] = {
        {"cancel_demo_1", OrderAction::Buy, 100, 3.01},
        {"cancel_demo_2", OrderAction::CreditFinBuy, 200, 3.02},
    };

    for (const auto& s : kOrderSpecs) {
        long long seq = trader.order_stock_async(acc, "159845.SZ", s.action, s.volume,
                                                 StockPriceType::Fix, s.price,
                                                 "rpc_test", s.remark);
        printf("已提交下单 seq=%lld remark=%s 方向=%s(%lld)\n", seq, s.remark,
               order_type_label(s.action), static_cast<long long>(s.action));
    }
    if (!trader.wait_async_orders(15.0)) {
        std::fprintf(stderr, "下单回执超时, 跳过撤单\n");
    } else {
        for (const auto& s : kOrderSpecs) {
            std::string sysid = callback->sysid_for(s.remark);
            if (sysid.empty()) {
                printf("跳过撤单 remark=%s (回执未带合同编号)\n", s.remark);
                continue;
            }
            printf("发起撤单 remark=%s sysid=%s ...\n", s.remark, sysid.c_str());
            // 撤单回报在函数返回前即经回调触发 (python compat 同步模拟,
            // 期间阻塞在撤单 RPC 上)。
            long long seq = trader.cancel_order_stock_async(acc, sysid);
            printf("撤单调用已返回 remark=%s seq=%lld\n", s.remark, seq);
        }
    }

    printf("running... press Ctrl-C to exit\n");
    std::signal(SIGINT, on_signal);
    std::signal(SIGTERM, on_signal);
    while (g_running) {
        // 主线程仅负责等待退出信号; 委托/成交事件由内部事件线程投递。
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }
    printf("\nstopping...\n");
    trader.stop();
    printf("bye\n");
    return 0;
}
