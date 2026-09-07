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
//   - Ctrl-C 后调 trader.stop() 收尾 (Python 版按 KeyboardInterrupt 退出,
//     daemon 线程随之消亡; 这里显式 stop 保证排队委托有界排空).

#include <chrono>
#include <csignal>
#include <cstdio>
#include <memory>
#include <string>
#include <thread>

#include "src/bigqmt.hpp"

using namespace bigqmt;

// 与 MyCallback 一一对应: 委托回报 / 成交回报 / 委托失败 / 撤单失败 /
// 异步下单回报 / 账户状态。
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
    }

    void on_account_status(const XtAccountStatus& status) override {
        printf("[callback] 账户状态   account_id=%s account_type=%s status=%lld\n",
               status.account_id.c_str(), status.account_type.c_str(),
               static_cast<long long>(status.status));
    }
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

    StockAccount acc(config.account_id, SECURITY_ACCOUNT);
    printf("connecting with account_id=%s redis=%s:%d db=%d\n",
           config.account_id.c_str(), config.redis_host.c_str(), config.redis_port,
           config.redis_db);

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

    // 真实下单调用保持注释, 需要时取消注释即可:
    auto seq = trader.order_stock_async(acc, "600654.SH", STOCK_BUY, 100,
                                   FIX_PRICE, 2.95, "rpc_test", "备注");
    printf("order_stock_async seq=%lld\n", seq);

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
