-- qmt-cpp: C++17 移植版 MiniQMT Big-QMT Redis-RPC 测试程序
-- 零第三方库依赖; 用法: xmake && xmake run
set_project("qmt-cpp")
set_version("0.1.0")
set_xmakever("2.5.0")

add_rules("mode.debug", "mode.release")
set_languages("c++17")
set_warnings("all", "extra")

target("qmt_callback_test")
    set_kind("binary")
    add_files("main.cpp", "src/bigqmt.cpp", "src/redis_resp.cpp")
    -- json.hpp / bigqmt.hpp / redis_resp.hpp 均为纯头文件, 无需第三方库
    add_syslinks("pthread")
target_end()
