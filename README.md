# qmt-cpp —— Python 版 MiniQMT 桥接客户端的 C++ 移植

把 Python 侧 `test_callback.py`(以及它用到的 `bigqmt_signal_trader` 客户端)
的行为移植成 **纯标准库 C++17**(零第三方依赖, POSIX socket + std::thread),
通过 Redis RPC 与同一套 MiniQMT 桥(bridge, "Big QMT signal trader")通信,
因此可以和 Python 版**并排运行在同一套桥后面**。

> 给后续开发者: 本项目最常遇到的需求是"桥上新方法, 客户端适配一下"。
> 直接跳到 [§7 新接口适配指南](#7-新接口适配指南), 那里是给这一类需求的配方。

> **让 AI agent 协助做转换/适配前, 先准备参照环境**(agent 需要对照 Python
> 实现才能准确转换):
>
> - Python **3.8**
> - `pip install xtquant-big-convert`
> - `pip install xtquant-big-convert[redis]`(含 Redis 通信部分)
>
> 装好后把 Python 侧安装路径(site-packages 里的 `bigqmt_signal_trader` 与
> `xtquant`)指给 agent —— 那就是 §2 说的一手权威参照物; 没有它, agent 只能
> 靠猜。

## 目录

1. [一句话原理](#1-一句话原理)
2. [权威参照物(移植对照时必须看的 Python 代码)](#2-权威参照物)
3. [文件布局](#3-文件布局)
4. [整体数据流与线程模型](#4-整体数据流与线程模型)
5. [线上协议(与桥之间的契约)](#5-线上协议)
6. [移植时的关键坑(务必读)](#6-移植时的关键坑)
7. [新接口适配指南](#7-新接口适配指南)
8. [构建 / 运行 / 测试](#8-构建--运行--测试)
9. [安全与凭据](#9-安全与凭据)

## 1. 一句话原理

Python 版客户端只做两件事, C++ 版原样复刻这两件事:

```
MiniQMT 桥(bridge 进程)  ←── Redis ──→  本库(BigQmtXtTrader)  ←── C++ 用户代码
      ↑                                      ↑                     (main.cpp)
  交易柜台/行情            ①RPC(请求-响应)       ②pub/sub(事件推送)
```

- **① 同步/异步调用**: 客户端把方法调用打包成 JSON 信封, `RPUSH` 进 Redis
  队列, 桥的 worker 取走执行后把结果写回 Redis, 客户端轮询取回 —— 相当于
  Python 的 `call_redis_rpc(transport="queue")`。C++ 侧 `BigQmtXtTrader::call()`。
- **② 事件**: 委托/成交/失败回报由桥主动 `PUBLISH` 到 Redis 频道, 客户端
  订阅后还原成 MiniQMT 风格的回调对象 —— 相当于 Python 的 `subscribe()`
  + `XtQuantTraderCallback`。

协议本身(Redis key 命名、JSON 字段、频道名)就是唯一的契约, 两端代码可以
完全独立, 只要协议一致。移植的本质就是 **照着 Python 客户端把协议在 C++
里重写一遍, 再把桥返回的 JSON 映回 MiniQMT 形态的对象**。

## 2. 权威参照物

适配新接口时, 先在这三处确认"桥到底怎么定义的", 再动 C++ 代码:

| 参照物 | 内容 | 在移植中对应 |
|---|---|---|
| `test_callback.py`(本仓库根目录) | 行为目标: 怎么连、怎么回调 | [main.cpp](main.cpp) |
| `.../site-packages/bigqmt_signal_trader/redis_rpc.py` | **协议 + 桥全部 method** 的定义(READ_METHODS / 各 handler), 参数名、别名、返回形状 | [src/bigqmt.cpp](src/bigqmt.cpp) 的 `call_impl` 与各查询方法 |
| `.../bigqmt_signal_trader/models.py` | 快照模型(Asset/Position/Order 的 JSON key 集), 与事件模型同源 | 结构体 `XtAsset/XtPosition/...` 与各 `*_from_*` 转换器 |
| `.../bigqmt_signal_trader/xtquant_compat.py` | Python 客户端的包装层: 默认值/兜底逻辑怎么写的 | 各转换器的默认值与兜底 |
| `.../site-packages/xtquant/xtconstant.py` | MiniQMT 状态码/类型码的权威数字 | [src/bigqmt.hpp](src/bigqmt.hpp) 的枚举与 `*_label()` |

Python 路径是本机调试环境里的
`/dev/shm/miniconda3/envs/qmt/lib/python3.8/site-packages/...`; 新装的环境
用 `pip install xtquant-big-convert` / `xtquant-big-convert[redis]`(见文首
的 agent 环境提示), 参照代码与上面的表一一对应。
**当 C++ 行为与 Python 不一致时, 以 Python 侧为唯一真相** —— C++ 只是复刻。

## 3. 文件布局

```
qmt-cpp/
├── main.cpp                  # 行为对齐 test_callback.py 的演示程序(含真实下单调用, 运行=真下单!)
├── xmake.lua                 # 构建脚本(target: qmt_callback_test, c++17, 仅链接 pthread)
├── bigqmt_client_config.yaml # 运行配置(真实凭据, 已 gitignore)
├── bigqmt_client_config.yaml.example  # 无凭据模板, 可提交
├── src/
│   ├── bigqmt.hpp            # 公共头: 常量/枚举/中文label、XtOrder 等回调对象、
│   │                         #   XtAsset/XtPosition 查询结果、ClientConfig、
│   │                         #   BigQmtXtTrader 全部公开接口
│   ├── bigqmt.cpp            # 核心实现(约 1700 行), 见下
│   ├── redis_resp.hpp/.cpp   # 最小 RESP2 Redis 客户端(非阻塞 connect、读超时、
│   │                         #   整帧提交式解析、断线重连), 无第三方库
│   └── json.hpp              # 手写 JSON(value 语义, python-repr 风格 double,
│                             #   \uXXXX、原始 UTF-8), 无第三方库
```

[src/bigqmt.cpp](src/bigqmt.cpp) 内部段落(行号会随改动漂移, 按函数名找):

| 函数/区域 | 职责 |
|---|---|
| `rpc_payload()` L536 | 请求体编码: `"b64s:" + base64(JSON)`, 再做 0-9 → `!#$%&()*~?` 逐位替换(**与 Python 一字不差**) |
| `call_impl()` L540 | 一次完整 RPC: RPUSH 队列 + EXPIRE → 轮询 `GET resp_key` → `BLPOP resp_list`(1s 切片) → 解析信封 |
| `call()` L697 | 公开同步 RPC: 加 `cmd_mutex_` 后走 call_impl, 信封 `ok/data/error/server_error` → 抛 `RpcServerRepliedError/RpcTimeoutError` |
| `event_loop()` L772 / `event_loop_redis()` L804 | pub/sub 监听线程: SUBSCRIBE 四个事件频道, 每帧 JSON 进 `on_event_json` |
| `order_from_event/trade_from_event/...` L856-960 | 事件 JSON → XtOrder/XtTrade/XtOrderError/XtCancelError(与查询行同源, 可复用) |
| `arm_barrier/release_barrier/hold_if_pending` L1010-1136 | issue #51 乱序屏障: 以 remark 为键暂存 in-flight 委托的事件, 直到其 async 响应到达 |
| `order_stock_async` L1169 / `order_worker_loop` L1216 / `submit_order` L1267 | 异步下单流水线: seq 递增 → 队列 → worker 串行 RPC |
| `outcome_worker_loop` L1301 / `fire_outcome` L1366 | 把 async 响应(带 seq)分发成 `on_order_stock_async_response`/`on_order_error` |
| `learn_sysid()` L1342 | issue #72: 下单后 ≤2s 内从事件流学出 合同编号(纯数字→int 的 key) |
| `asset_from_data/position_from_item` L1543-1579 | 查询快照 JSON → 结构体(**None 与 0 的区分、成本/市值兜底照抄 Python**) |
| `get_asset/get_positions/query_orders/...` L1599-1717 | 8 个账户查询方法的统一模板(见 §7) |

## 4. 整体数据流与线程模型

```
BigQmtXtTrader 内部:
                                  ┌─────────────────────────────┐
   用户线程(main)                 │                             │
   order_stock_async ──► OrderJob ┤  order_thread_ (下单 worker) │
   (返回 seq, 立即)     (队列)     │   逐条串行 RPC: RPUSH 队列    │
                                  │   + 屏障(barrier, 按 remark) │
                                  └─────────────────────────────┘
   Redis pub/sub 事件(4 频道)
        │  event_thread_  ◄─────── Redis
        │  on_event_json
        ├─► 屏障命中? ──是──► 暂存(BarrierEntry, 10s 过期)
        └─► 否 ──► deliver_event → 用户回调(委托/成交/失败)

   async 响应/下单错误(桥发回)
        │  outcome_thread_  ◄─── RPC 结果
        │  带 seq ──► fire_outcome ──► on_order_stock_async_response
        │                        └──► 释放同名 remark 屏障 → 补发暂存事件
```

- 回调可能从 **三个线程** 之一触发: pub/sub 事件线程、outcome 分发线程、
  调 `connect()/subscribe()` 的那个线程 —— 回调里别碰共享状态, 保持简短。
- 两条 worker 流水线(order/outcome)以 **seq=0 哨兵** 收尾, 事件线程以
  `event_running_` 标志退出; `stop()` 有界排空(不会无限等)。
- `cmd_conn_`(共享 RPC 连接)由 `cmd_mutex_` 串行化; 每次 RPC 轮询的
  `BLPOP` 用**本次调用专属的短连接**, 避免半截回复污染共享连接。

## 5. 线上协议

### 5.1 RPC(请求-响应)

1. 生成 `request_id`(32 位 hex, 等价 uuid4)。
2. `RPUSH bigqmt:rpc:queue:{account_id}` 请求体, 随后 `EXPIRE` 60s。
   请求体 = `"b64s:"` + base64(JSON) + 0-9 → `!#$%&()*~?` 替换(见
   [src/bigqmt.cpp](src/bigqmt.cpp) `rpc_payload` 与 Python `redis_rpc.py` 同款函数)。
3. 信封字段(与 Python 完全一致):
   `schema_version:1, request_id, account_id, method, params,
    reply_channel/reply_list/reply_key(三种写法都是
    bigqmt:rpc:resp|respq:{account_id}:{request_id}), ttl_seconds:60`。
4. 取回结果(与 Python `call_redis_rpc` 同款双通道):
   轮询 `GET bigqmt:rpc:resp:{acc}:{reqid}`; 未命中则
   `BLPOP bigqmt:rpc:respq:{acc}:{reqid} 1`(1 秒切片, 可打断);
   直到 `rpc_timeout_seconds`(默认 30s)deadline。
5. 响应信封: `{"ok": bool, "data": ..., "error"?, "server_error"?}`。
   `ok=false` → `RpcServerRepliedError(error)`;
   `server_error` 非空 → 同上; 超时 → `RpcTimeoutError`。

### 5.2 事件(pub/sub)

订阅四个频道(模板常量在 [src/bigqmt.cpp](src/bigqmt.cpp) L23-26):

```
bigqmt:order_events:{account_id}           → on_stock_order
bigqmt:trade_events:{account_id}           → on_stock_trade
bigqmt:order_error_events:{account_id}     → on_order_error
bigqmt:cancel_error_events:{account_id}    → on_cancel_error
```

payload 是 JSON 事件 dict, 字段形状**与查询返回的行同源**(都来自桥内部的
mini-QMT 行), 所以事件转换器 `order_from_event` 等能直接吃查询行 —— 新查询
方法看到委托/成交列表时, 复用事件转换器即可(见 [src/bigqmt.cpp](src/bigqmt.cpp) L1581-1590)。

### 5.3 连接细节

- RESP2 协议; `AUTH [username] password`、`SELECT db` 在懒连接建立时自动完成。
- 读超时 1s(可配); 轮询 deadline 由外层 RPC 控制。
- 共享连接 `cmd_conn_` 上**只跑短命令**(RPUSH/EXPIRE/GET/DEL), 长等待的
  BLPOP 一律走每调用独立的连接 —— 这是协议级纪律, 见 §6。

## 6. 移植时的关键坑

这几条都是踩过真实崩溃的教训, 改 RESP 或 RPC 代码前必读:

1. **RESP 分帧必须"整帧提交"**。桥的大回复(>64KB, 如多股持仓)要分多次
   `recv` 才收齐。解析器若先消费 `$<len>\r\n` 帧头再等负载, 下一轮会把负载
   首字节(`{`)当 RESP 类型字节 → `unknown reply type byte '{'`。因此
   [src/redis_resp.cpp](src/redis_resp.cpp) 的 `parse_value()` 用**局部光标**:
   帧不完整就回滚到帧首, 整帧到齐才提交。**任何新解析逻辑都要保持这个性质**。
2. **读超时后不许在同一连接上继续发命令**。超时会留下"半截帧"或"迟到回复",
   再发命令会导致帧错位、把别人的回复当成自己的。机制: `unsafe_` 标记 →
   `command()` 先断线重连再发; 共享连接 `cmd_conn_` 的轮询/清理读超时一律
   `close()`。别图省事绕开。
3. **忽略 SIGPIPE**。对端(桥/Redis)中途断开时 `send()` 会触发 SIGPIPE,
   默认动作是杀死整个进程。库在加载时 `SIG_IGN` 了它, 自己写网络代码也别依赖
   默认动作。
4. **服务器 `None` ≠ 数字 0**。桥对终端未上报的字段给 `null`; Python 里是
   None, C++ 里数字字段没法表达"未知", 所以 `XtAsset` 带 `has_*` 标志;
   `XtPosition` 的兜底(成本价当开仓价、市值按 最新价×量 估算等)**逐条镜像
   Python**([src/bigqmt.cpp](src/bigqmt.cpp) L1553-1579), 别自己发明默认。
5. **OrderId 的换算规则**(与 Python 完全一致):
   纯数字串 → 存成 int; 空串 → 0; 其它(含字母的系统单号)→
   `crc32(sys_id) & 0x3FFFFFFF`(结果 0 则置 1)。已在 [src/bigqmt.hpp](src/bigqmt.hpp)
   `OrderId` 里, 新代码直接用它, 不要另写。
6. **状态码是数字, 别当魔法数**: 23/24 买卖、5/11/42-48 报价、48-57/255 委托
   状态, 枚举与中文 `*_label()` 在 [src/bigqmt.hpp](src/bigqmt.hpp), 值表抄自
   桥旁边的 xtconstant.py。加新码先查 Python。
7. **异步下单的时序契约**: MiniQMT 语义要求"先 async 响应、后委托/成交事件";
   桥自己做不到时, 由 `order_stock_async` + 屏障(release-barrier, 见 §3 表)
   在客户端保证; 屏障键是 **remark**, 所以 remark 要能唯一定位一次委托。

## 7. 新接口适配指南

桥的方法分四类, 适配工作量递增:

| 类型 | 例子 | 适配难度 |
|---|---|---|
| A. 只读查询(同步) | get_asset / query_orders / get_full_tick | 低 —— 套模板 |
| B. 官方函数透传 | get_history_trade_detail_data 等 | 极低 —— call() 原样透传 |
| C. 写操作/下单 | order_stock_async 模式 | 中 —— 有现成流水线 |
| D. 新事件类型 | 订阅新频道 | 中 —— 动事件分发 |

### A. 只读查询 —— 标准模板(最常见的需求)

以仓库里 8 个查询方法([src/bigqmt.cpp](src/bigqmt.cpp) L1599-1717)为样板, 配方:

```cpp
// 头文件 src/bigqmt.hpp: 声明
XtSomething get_something(const StockAccount& account = StockAccount());

// 实现 src/bigqmt.cpp: 三步 —— 解析账户 -> 组 params -> call + 转换
XtSomething BigQmtXtTrader::get_something(const StockAccount& account) {
    // 1) 空 StockAccount() 表示用配置里的 account_id (统一约定)
    const std::string account_id = resolved_account_id(config_, account);
    // 2) params 的 key 必须与桥 handler 的参数名一致 (查 redis_rpc.py)
    Json params = Json::make_object();
    params.set("account_id", Json::make_string(account_id));
    params.set("xxx", Json::make_string(xxx));
    // 3) call() 只返回信封里的 data; ok=false/server_error/超时都会抛异常
    Json data = call("get_something", std::move(params));
    //    然后按桥的 data 形状转换; null 语义、缺省值照抄 Python 包装层
    return something_from_data(account_id, data);
}
```

具体步骤:

1. **找桥的定义**: 在 Python `redis_rpc.py` 里搜 method 名(只读方法在
   `READ_METHODS` 集合或对应 handler), 记下: 准确 method 名、参数 key 名、
   是否带别名(如 `query_stock_asset`→`get_asset`)、handler 返回什么形状。
2. **确认 data 形状**: 返回对象在 `models.py` 里有快照类(字段即 JSON key);
   没有模型的行情类(如 `get_full_tick`)就直接把 data 当 JSON 用
   (`Json::make_object/member_*/as_*` API 见 [src/json.hpp](src/json.hpp))。
3. **写转换器**: 委托/成交行 → **直接复用** `order_from_event`/`trade_from_event`
   (字段同源, 已有别名 `order_from_snapshot`/`trade_from_snapshot`);
   新形状(资产/持仓/其它)自己写 `*_from_data` 转换器, 放在 L1543 附近。
4. **空值语义**: 桥对"无数据"有两种表达 —— `data: null`(如单股持仓查无)
   与"对象但字段 null"。前者用 `is_null()` 区分(返回 false / 空列表 / "-1",
   见 `query_stock_position` L1637 与 `get_last_order_id` L1713); 后者用
   `json_number_or_null` 式辅助把 null 当"未知"。
5. **编译零警告 + 不真跑**: 见 §8; 只读查询可对着真实桥在你自己终端跑,
   别在自动化环境跑 demo(见 §9)。

### B. 官方函数透传

服务器把 QMT 官方接口原样暴露时(m_* 字段、ORDER/DEAL 语义), **不要强行
建模**: 声明返回 `Json`, 参数透传, 让调用方按官方属性名取(样板:
`get_history_trade_detail_data` / `get_value_by_order_id` / `get_last_order_id`
L1682-1716)。注意服务器对"查不到"的约定各异(get_last_order_id 返回字符串
`"-1"`、get_value_by_order_id 返回 null), 处理时照抄实现。

### C. 写操作 / 下单类

桥的写操作多数是"提交后异步回报", Python 侧把它们做成**事件驱动的双通道**:
调用立即返回 seq, 结果走事件/outcome。C++ 里最省事的接法是:

1. 若桥对该操作有同步语义(立即返回结果), 直接 `call()` 就行。
2. 需要"先回执后事件"顺序保证的, 复刻 `order_stock_async` 的三件套:
   `arm_barrier(remark, seq)`(发请求前挂屏障)→ 事件侧 `hold_if_pending`
   自动暂存同 remark 的事件 → 回执到达时 `release_barrier` 补发。
   关键约束: **屏障以 remark 为键**, 每个进行中的操作 remark 必须唯一。
3. 走独立 worker 还是直接同步调, 看 Python 包装层怎么做的 —— 别发明新模型。

### D. 新事件类型

桥在 `redis_rpc.py` 里 `PUBLISH` 了什么新频道, 就三步:

1. [src/bigqmt.cpp](src/bigqmt.cpp) L23-26 加频道模板常量 + L804 的 SUBSCRIBE 列表加上。
2. `on_event_json`(L962)按事件 dict 的判别字段分发到新转换器。
3. [src/bigqmt.hpp](src/bigqmt.hpp) `XtQuantTraderCallback` 加 virtual +
   对应结构体 —— 默认空实现, 老代码不受影响。

> **改协议层前先读 §6 的坑** —— RESP 分帧、超时纪律、SIGPIPE 三条不能破。

## 8. 构建 / 运行 / 测试

```bash
# 方式一: xmake(仓库正式构建)
xmake                    # 产物: build/linux/x86_64/release/qmt_callback_test
xmake f -m debug && xmake && xmake run   # 调试模式

# 方式二: 直接 g++(快速迭代, 必须保持零警告)
g++ -std=c++17 -Wall -Wextra \
    main.cpp src/bigqmt.cpp src/redis_resp.cpp \
    -o /tmp/qmt_test -lpthread
```

- 项目无第三方测试框架; 对桥的**离线验证**手段是本地假 RESP 服务器
  (思路: 用 `RedisConnection` 连接一个回放脚本化回复的本地 socket, 验证
  大回复分帧解析、读超时后的断线重连 —— 复现过真实的
  `unknown reply type byte '{'` 崩溃并确认修复)。改 RESP/RPC 代码后建议照此
  思路补一个离线用例再上真桥。
- 与 Python 版的**行为对照**方法: 同一桥后面分别跑 Python 和 C++, 对打印
  输出; 字段缺省/兜底逻辑不一致时, Python 侧为准(§2)。

## 9. 安全与凭据

- `ClientConfig` 的**内建默认值刻意不含任何凭据**(空账号、localhost、无
  密码), 源码可以放心提交。真实凭据只允许出现在本机未提交的
  `bigqmt_client_config.yaml`(已 gitignore)或 `BIGQMT_*` 环境变量里。
- 加载优先级: 内建默认 < YAML 文件 < 环境变量(见 [src/bigqmt.hpp](src/bigqmt.hpp)
  `ClientConfig` 注释); 环境变量名: `BIGQMT_ACCOUNT_ID`、`BIGQMT_REDIS_HOST/
  PORT/DB/USERNAME/PASSWORD`、`BIGQMT_RPC_TIMEOUT_SECONDS`; 配置文件路径可用
  `BIGQMT_CONFIG_FILE` 覆盖。只靠内建默认跑起来会连 localhost:6379 失败并
  报错, 这是故意的。
- **`main.cpp` 的下单/撤单演示(600654.SH 限价单 + 按合同编号撤单)全是
  真实委托操作**: 任何自动化/沙箱环境不要运行该 demo; 确认"只读查询"是否
  安全后也只在本人终端跑。日常验证优先用 §8 的离线假服务器或 `get_*`
  只读方法; 只读 API 之外的新方法适配请对照 Python 侧再上线。
