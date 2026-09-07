# /dev/shm/miniconda3/envs/qmt/bin/python3.8
#  pip install xtquant-big-convert
#  pip install xtquant-big-convert[redis]
from bigqmt_signal_trader.xtquant_compat import (
    StockAccount, XtQuantTraderCallback, configure, xt_trader,
)

class MyCallback(XtQuantTraderCallback):
    def on_stock_order(self, order):
        print("委托回报:", order.stock_code, order.order_status, order.order_sysid)

    def on_stock_trade(self, trade):
        print("成交回报:", trade.stock_code, trade.order_id, trade.traded_volume, trade.traded_price)

    def on_order_error(self, order_error):
        print("委托失败:", order_error.order_id, order_error.error_id, order_error.error_msg)

    def on_cancel_error(self, cancel_error):
        print("撤单失败:", cancel_error.order_id, cancel_error.error_id, cancel_error.error_msg)

    def on_order_stock_async_response(self, response):
        print("异步下单回报:", response.account_id, response.order_id, response.seq)

    def on_account_status(self, status):
        print("账户状态:", status.account_id, status.account_type, status.status)

configure()
xt_trader.register_callback(MyCallback())
acc = StockAccount(xt_trader.client.account_id, "STOCK")
xt_trader.connect()
xt_trader.subscribe(acc)

# # 异步下单（返回 seq，回报走回调）
seq = xt_trader.order_stock_async(acc, "600654.SH", 23, 100, 11, 2.95, "rpc_test", "备注")