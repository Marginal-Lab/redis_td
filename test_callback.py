# /dev/shm/miniconda3/envs/qmt/bin/python3.8
# pip install xtquant-big-convert
# pip install xtquant-big-convert[redis]
#
# 撤单测试（委托状态事件驱动版）:
#   下单 -> on_stock_order 收到委托状态更新为 已报(50) 时置可撤标记 ->
#   主流程发起撤单 -> 撤单回报(on_cancel_order_stock_async_response) +
#   委托状态事件 51 已报待撤 -> 54 已撤 确认 -> 打印总览退出。
#
# 数值口径以本机 xtconstant 和实测事件为准:
#   48 未报 / 49 待报 / 50 已报 / 51 已报待撤 / 52 部成待撤 /
#   53 部撤 / 54 已撤 / 55 部成 / 56 已成 / 57 废单
# 注意: 事件流里一笔已受理限价单的静止状态是 50 "已报" (实测输出
# "状态=已报(50)"、撤单 settle 消息 "still status 50"), 不是 48 ——
# 48 只是柜台未回报的瞬态。所以可撤触发只看 50/55; 对未报(48/49)的单
# 撤单, 服务端可能根本不受理, 桥会一直轮询到超时 ("67 lookup(s)" 那类)。
# 另: 撤单 RPC 在桥 settle 期间阻塞 (顺利 ~1-2s, 部署端空操作时 ~60s),
# 因此不在事件线程里直接发撤单, 只置标记, 由主流程发起 —— 否则 51/54
# 事件会一直卡在事件线程里。

import os
import time

os.environ["BIGQMT_AUTO_SYNC"] = "1"

from bigqmt_signal_trader.xtquant_compat import (
    StockAccount, XtQuantTraderCallback, configure, xt_trader,
)

# ---- 全局状态机 ---------------------------------------------------------------
# 状态号 -> 中文名
ORDER_STATUS_NAMES = {
    48: "未报", 49: "待报", 50: "已报", 51: "已报待撤", 52: "部成待撤",
    53: "部撤", 54: "已撤", 55: "部成", 56: "已成", 57: "废单",
}

def status_name(code):
    return "%s(%s)" % (ORDER_STATUS_NAMES.get(code, "?"), code)

CANCELABLE_STATUSES = (50, 55)   # 已报 / 部成(撤剩余)
FINAL_STATUSES = (53, 54, 56, 57)  # 部撤 / 已撤 / 已成 / 废单

order_to_cancel_id = None   # 可撤时记录的订单号 (OrderId 对象, str()=合同编号原串)
order_ready_to_cancel = False
cancel_ack = None           # None / "ok"(桥受理) / "fail"(桥拒绝)
cancel_ack_error = ""
cancel_seq = None           # 撤单请求 seq
order_terminal_status = None  # 见到终态(53/54/56/57)时记录, 主流程据此退出
status_seen = set()         # 全部见过的状态, 总览打状态链
last_event_at = 0.0         # 最后一次委托事件时间, watchdog 用


class MyCallback(XtQuantTraderCallback):
    def on_stock_order(self, order):
        global order_to_cancel_id, order_ready_to_cancel
        global order_terminal_status, last_event_at

        status = order.order_status
        status_seen.add(status)
        last_event_at = time.time()

        print("=" * 60)
        print(f"【委托回报】remark={order.order_remark} stock={order.stock_code}")
        print(f"  状态: {status_name(status)}")
        print(f"  order_sysid (合同编号): {order.order_sysid or '(未分配)'}")
        if hasattr(order, "order_id"):
            print(f"  order_id (OrderId): {order.order_id!r} | str() = {str(order.order_id)!r}")

        # ---- 撤单触发点: 柜台已受理(已报 50)或已部分成交(部成 55) ----------
        if status in CANCELABLE_STATUSES and not order_ready_to_cancel:
            if not order.order_sysid:
                # 事件还没带合同编号 (极少数): 此时 order_id 只携带 remark,
                # 拿去撤单会撤错; 等下一次带号的回报/事件。
                print("⏳ 事件未带合同编号, 暂不置可撤标记, 等下次回报")
            else:
                order_to_cancel_id = order.order_id  # round-trip: 原串随对象送回
                order_ready_to_cancel = True
                print(f"✅ 订单已 {status_name(status)}, 可发起撤单: "
                      f"order_id={order_to_cancel_id!r} str()={str(order_to_cancel_id)!r}")

        # 终态判定 (含撤单在途直接到 54 的情况)
        if status in FINAL_STATUSES:
            if order_terminal_status is None:
                order_terminal_status = status
                print(f"🏁 委托进入终态 {status_name(status)}"
                      + (" —— 撤单已由服务端确认" if status in (53, 54)
                         and order_ready_to_cancel else ""))

    def on_stock_trade(self, trade):
        print("【成交回报】")
        print(f"  股票: {trade.stock_code}, 订单号: {trade.order_id!r}, "
              f"成交量: {trade.traded_volume}, 成交价: {trade.traded_price}")

    def on_order_error(self, order_error):
        print(f"【委托失败】订单号: {order_error.order_id!r}, "
              f"错误码: {order_error.error_id}, 错误信息: {order_error.error_msg}")

    def on_cancel_error(self, cancel_error):
        # 撤单 RPC 异常 (非业务拒绝) 走这里, 与回报回调互斥。
        print(f"【撤单失败/异常】订单号: {cancel_error.order_id!r}, "
              f"错误码: {cancel_error.error_id}, 错误信息: {cancel_error.error_msg}")

    def on_order_stock_async_response(self, response):
        print("【异步下单响应】")
        print(f"  seq: {response.seq}")
        print(f"  order_id: {response.order_id!r} | str() = {str(response.order_id)!r}")
        print(f"  order_sysid: {response.order_sysid!r}")
        print(f"  error_msg: {response.error_msg!r}")
        if response.error_msg:
            print("  ❌ 下单 RPC 失败 (废单拒绝会另走 on_order_error/状态 57)")

    def on_cancel_order_stock_async_response(self, response):
        # 撤单回报: 在 cancel_order_stock_async 返回前、同线程同步触发。
        global cancel_ack, cancel_ack_error, cancel_seq
        cancel_ack = "ok" if response.success and response.cancel_result == 0 else "fail"
        cancel_ack_error = response.error_msg or ""
        print("【异步撤单响应】")
        print(f"  seq: {response.seq}  cancel_result: {response.cancel_result}  "
              f"success: {response.success}")
        print(f"  error_msg: {cancel_ack_error!r}")
        print(f"  order_sysid: {response.order_sysid!r}")
        if cancel_ack == "ok":
            print("  ✅ 桥已受理, 实际撤成看委托状态事件 (应到 51 已报待撤 -> 54 已撤)")
        else:
            # 本版 compat 失败时只给泛化文案, 桥的真实拒绝原因
            # (CancelResult.message, 如 "cancel was not confirmed after
            # N lookup(s): ... still status 50") 不透传; 真实原因见
            # 服务端日志或 C++ 版 main.cpp (撤单回报 error_msg 带全因)。
            print("  ❌ 桥拒绝了撤单 (error_msg 为泛化文案, 真实原因看服务端日志)")

    def on_account_status(self, status):
        print(f"【账户状态】account_id={status.account_id} "
              f"account_type={status.account_type} status={status.status}")


# ===== 主流程 =====

configure()
xt_trader.register_callback(MyCallback())

# 部署方是信用账户 (CREDIT); 普通账户请改 "STOCK"。
acc = StockAccount(xt_trader.client.account_id, "STOCK")
xt_trader.connect()
xt_trader.subscribe(acc)

print("=" * 60)
print("📤 提交一笔限价买单 (600654.SH @2.95, 远低于现价 -> 停在 已报(50) 等撤)...")
seq = xt_trader.order_stock_async(acc, "600654.SH", 23, 100, 11, 2.95, "rpc_test", "cancel_test")
print(f"下单请求序号 (seq): {seq}")
print("=" * 60)

# ---- 阶段 1: 等委托到 已报(50)/部成(55) -------------------------------------
print("⏳ 等待订单进入可撤状态 (最多 30s)...")
deadline = time.time() + 30.0
while not order_ready_to_cancel and time.time() < deadline:
    if order_terminal_status is not None:
        break
    time.sleep(0.3)
    if int(time.time()) % 2 == 0:
        pass  # 回报都在回调里打, 这里安静等即可
if order_terminal_status is not None and not order_ready_to_cancel:
    print(f"⚠️ 委托已先进入终态 {status_name(order_terminal_status)}, 无需撤单")
elif not order_ready_to_cancel:
    print("⚠️ 30s 内未进入可撤状态。看上面的【委托回报】行确认状态号:")
    print("   预期已报=50 (48=未报瞬态, 别按 48 触发); 若看到 56/57 则委托")
    print("   已成/废单; 若完全无回报则查事件通道或账户类型。")
    print("测试中止。")
else:
    # ---- 阶段 2: 发起撤单 -------------------------------------------------
    print("=" * 60)
    print(f"📤 发起撤单请求, 目标: {order_to_cancel_id!r}")
    print(f"   order_id 类型: {type(order_to_cancel_id).__name__}")
    print(f"   str(order_id): {str(order_to_cancel_id)!r}  <- 送回的是券商原串")
    print(f"   (撤单 RPC 阻塞到桥 settle 结束: 顺利 ~1-2s, 部署端空操作 ~60s)")
    cancel_seq = xt_trader.cancel_order_stock_async(acc, order_to_cancel_id)
    print(f"撤单请求序号 (seq): {cancel_seq}")
    print("=" * 60)

    # ---- 阶段 3: 等撤单结果 (54) 或桥拒绝, 最多 90s -----------------------
    print("⏳ 等待撤单确认 (委托状态 51 -> 54)...")
    deadline = time.time() + 90.0
    warned_noop = False
    while time.time() < deadline:
        if order_terminal_status is not None:
            break
        if cancel_ack == "fail":
            break
        time.sleep(0.3)
        # watchdog: 桥说受理了但服务端迟迟不动 -> 与实测 "API 撤单被
        # 服务端静默无视 (67 lookups still 50)" 同一症状: 查部署端
        # qmt_api["cancel"] (与下单同通道 / 信用上下文不降级 / 真实返回值)。
        if (cancel_ack == "ok" and not warned_noop
                and time.time() - last_event_at > 20.0):
            print("⚠️ 撤单已被桥受理, 但 20s 无后续委托状态事件 (应到 51/54)。")
            print("   若状态停在 50, 很可能又是部署端 API 撤单空操作 —— ")
            print("   对照: 客户端手动撤单是立刻出 54 的。查 qmt_api['cancel']。")
            warned_noop = True

    print("\n===== 总览 =====")
    chain = " -> ".join(status_name(s) for s in sorted(status_seen)) or "(无事件)"
    print(f"状态链: {chain}")
    if order_terminal_status in (53, 54):
        print(f"🎉 撤单成功: 服务端确认 {status_name(order_terminal_status)}")
    elif cancel_ack == "fail":
        print(f"❌ 撤单被桥拒绝: cancel_result=-1, error_msg={cancel_ack_error!r}")
        print("   (compat 失败时丢弃桥回包细节, 真实原因下面直接看原始回包)")

        # ---- 诊断: 同一参数重发一次撤单 RPC, 直接看桥回包里的真实原因 ----
        # 走 compat 内部客户端 (client.call 是公开 API), 不换语言不换库;
        # 回包是取消结算的 CancelResult -> {"success":..., "message":...},
        # compat 只取 success 折成 0/-1, message 在这里能看全。
        print("---- 诊断: 原样重发撤单 RPC, 看桥真实回包 ----")
        try:
            raw = xt_trader.client.call(
                "cancel_order_stock_sysid",
                {"account_id": xt_trader.client.account_id, "market": "",
                 "order_sysid": str(order_to_cancel_id)},
                account_id=xt_trader.client.account_id,
            )
            data = raw.get("data") if isinstance(raw, dict) else None
            if isinstance(data, dict):
                raw = data
            print("桥原始回包:", raw)
            if isinstance(raw, dict):
                reason = (raw.get("message") or raw.get("error_msg")
                          or raw.get("error"))
                if raw.get("success"):
                    print("🎉 这次桥受理了! 等 3s 看 51/54 委托回报...")
                    time.sleep(3)
                elif reason:
                    print(f"真实拒绝原因: {reason}")
        except Exception as exc:
            print("诊断重发异常:", exc)
            print("  (若卡到超时 = '受理但服务端不动'的 67-lookup 模式 ——")
            print("   查部署端 qmt_api['cancel']: 同通道/信用上下文不降级/真实返回值)")
    elif order_terminal_status == 56:
        print("ℹ️ 委托已成, 撤单自然无效")
    elif order_terminal_status == 57:
        print("ℹ️ 委托已废单")
    else:
        print("⏰ 90s 未确认, 上面 watchdog 提示即为排查方向")

print("测试结束。")
