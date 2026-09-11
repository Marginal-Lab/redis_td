#include "bigqmt.hpp"

#include <unistd.h>  // readlink (/proc/self/exe)

#include <cctype>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <fstream>
#include <iterator>
#include <map>
#include <random>

namespace bigqmt {

// ---------------------------------------------------------------------------
// constants / channel names (match exec_events.py and redis_rpc.py)
// ---------------------------------------------------------------------------

namespace {

const char* kOrderChannelTemplate = "bigqmt:order_events:%s";
const char* kTradeChannelTemplate = "bigqmt:trade_events:%s";
const char* kOrderErrorChannelTemplate = "bigqmt:order_error_events:%s";
const char* kCancelErrorChannelTemplate = "bigqmt:cancel_error_events:%s";

// async-order barrier timeouts (issue #51/#72 in the Python package)
const double kBarrierTimeoutSeconds = 10.0;
const double kSysidLearnWaitSeconds = 2.0;
const double kShutdownDrainSeconds = 5.0;

double env_double(const char* name, double def) {
    const char* v = std::getenv(name);
    if (!v || !*v) return def;
    return std::strtod(v, nullptr);
}
long long env_int(const char* name, long long def) {
    const char* v = std::getenv(name);
    if (!v || !*v) return def;
    return std::strtoll(v, nullptr, 10);
}
std::string env_str(const char* name, const std::string& def) {
    const char* v = std::getenv(name);
    if (!v || !*v) return def;
    return v;
}

}  // namespace

namespace {

// BIGQMT_* environment variables override whatever the caller loaded.
void apply_env_overrides(ClientConfig& cfg) {
    cfg.account_id = env_str("BIGQMT_ACCOUNT_ID", cfg.account_id);
    cfg.account_type = env_str("BIGQMT_ACCOUNT_TYPE", cfg.account_type);
    cfg.redis_host = env_str("BIGQMT_REDIS_HOST", cfg.redis_host);
    cfg.redis_port = static_cast<int>(env_int("BIGQMT_REDIS_PORT", cfg.redis_port));
    cfg.redis_db = static_cast<int>(env_int("BIGQMT_REDIS_DB", cfg.redis_db));
    cfg.redis_username = env_str("BIGQMT_REDIS_USERNAME", cfg.redis_username);
    cfg.redis_password = env_str("BIGQMT_REDIS_PASSWORD", cfg.redis_password);
    cfg.rpc_timeout_seconds = env_double("BIGQMT_RPC_TIMEOUT_SECONDS",
                                         cfg.rpc_timeout_seconds);
    // account_type: 规范化成大写, 只认 STOCK/CREDIT, 其余回落 STOCK 并告警
    // (配置文件里拼错、大小写混写都不至于静默跑成别的账户类型标签)。
    std::string up;
    up.reserve(cfg.account_type.size());
    for (char c : cfg.account_type) {
        if (c >= 'a' && c <= 'z') c = static_cast<char>(c - 'a' + 'A');
        up += c;
    }
    if (up.empty()) {
        cfg.account_type = "STOCK";
    } else if (up != "STOCK" && up != "CREDIT") {
        std::fprintf(stderr, "[bigqmt] config: unknown account_type '%s', "
                             "falling back to STOCK (expect \"STOCK\" or \"CREDIT\")\n",
                     cfg.account_type.c_str());
        cfg.account_type = "STOCK";
    } else {
        cfg.account_type = up;
    }
}

std::string trim_ascii(const std::string& s) {
    size_t b = 0, e = s.size();
    while (b < e && (s[b] == ' ' || s[b] == '\t')) ++b;
    while (e > b && (s[e - 1] == ' ' || s[e - 1] == '\t')) --e;
    return s.substr(b, e - b);
}

// "..." with backslash escapes, '...' with YAML '' escape. Input is the raw
// value text (already trimmed, first char is the quote).
std::string unquote_yaml_scalar(const std::string& raw) {
    if (raw.size() < 2) {
        throw std::runtime_error("bigqmt config: unterminated quoted value: " + raw);
    }
    if (raw[0] == '"') {
        std::string out;
        for (size_t i = 1; i < raw.size(); ++i) {
            char c = raw[i];
            if (c == '"') return out;
            if (c == '\\' && i + 1 < raw.size()) {
                char n = raw[++i];
                switch (n) {
                    case 'n': out += '\n'; break;
                    case 't': out += '\t'; break;
                    case 'r': out += '\r'; break;
                    default: out += n;  // \\ \" \/ etc. pass the char through
                }
                continue;
            }
            out += c;
        }
    } else if (raw[0] == '\'') {
        std::string out;
        for (size_t i = 1; i < raw.size(); ++i) {
            if (raw[i] == '\'') {
                if (i + 1 < raw.size() && raw[i + 1] == '\'') {  // '' -> '
                    out += '\'';
                    ++i;
                    continue;
                }
                return out;
            }
            out += raw[i];
        }
    }
    throw std::runtime_error("bigqmt config: unterminated quoted value: " + raw);
}

// Flat YAML subset: "key: value" per line, '#' comments, quoted or bare
// scalars. Returns a key -> raw scalar text map; unknown keys are rejected
// later so their names can appear in the message.
std::map<std::string, std::string> parse_yaml_flat(const std::string& text) {
    std::map<std::string, std::string> out;
    size_t pos = 0;
    while (pos < text.size()) {
        size_t eol = text.find('\n', pos);
        if (eol == std::string::npos) eol = text.size();
        std::string line = text.substr(pos, eol - pos);
        pos = eol + 1;
        if (!line.empty() && line.back() == '\r') line.pop_back();

        line = trim_ascii(line);
        if (line.empty() || line[0] == '#') continue;  // blank / comment

        size_t colon = line.find(':');
        if (colon == std::string::npos || trim_ascii(line.substr(colon + 1)).empty()) {
            // Nested blocks ("redis:" with indented children) are outside the
            // supported subset; skip the line quietly.
            continue;
        }
        std::string key = trim_ascii(line.substr(0, colon));
        std::string value = line.substr(colon + 1);
        // Keys must look like field names; anything else is a typo to report.
        bool key_ok = !key.empty();
        for (char c : key) {
            if (!(std::isalnum(static_cast<unsigned char>(c)) || c == '_')) key_ok = false;
        }
        if (!key_ok) continue;

        value = trim_ascii(value);
        if (!value.empty() && (value[0] == '"' || value[0] == '\'')) {
            value = unquote_yaml_scalar(value);
        } else {
            // Trailing comment: '#' on its own or after whitespace.
            size_t hash = value.find('#');
            while (hash != std::string::npos && hash > 0 &&
                   value[hash - 1] != ' ' && value[hash - 1] != '\t') {
                hash = value.find('#', hash + 1);
            }
            if (hash != std::string::npos) value = value.substr(0, hash);
            value = trim_ascii(value);
        }
        out[key] = value;
    }
    return out;
}

long long yaml_strict_int(const std::string& value, const std::string& key) {
    errno = 0;
    char* end = nullptr;
    long long v = std::strtoll(value.c_str(), &end, 10);
    if (errno == ERANGE || end == value.c_str() || *end != '\0') {
        throw std::runtime_error("bigqmt config: key '" + key +
                                 "' expects an integer, got '" + value + "'");
    }
    return v;
}

double yaml_strict_double(const std::string& value, const std::string& key) {
    errno = 0;
    char* end = nullptr;
    double v = std::strtod(value.c_str(), &end);
    if (errno == ERANGE || end == value.c_str() || *end != '\0') {
        throw std::runtime_error("bigqmt config: key '" + key +
                                 "' expects a number, got '" + value + "'");
    }
    return v;
}

// Where to look for the config file, in order:
//   1. explicit path argument / BIGQMT_CONFIG_FILE;
//   2. <cwd>/bigqmt_client_config.yaml;
//   3. the executable's directory, then its ancestors -- xmake run launches
//      the binary from a build/ subdirectory several levels under the project
//      root, so the file usually lives up the tree, not in the cwd.
std::vector<std::string> config_candidates(const std::string& path) {
    std::vector<std::string> out;
    if (!path.empty()) {
        out.push_back(path);
        return out;
    }
    if (const char* p = std::getenv("BIGQMT_CONFIG_FILE"); p && *p) {
        out.push_back(p);
    }
    out.push_back("bigqmt_client_config.yaml");  // cwd
    char exe[4096];
    ssize_t n = ::readlink("/proc/self/exe", exe, sizeof(exe) - 1);
    if (n > 0) {
        exe[n] = '\0';
        std::string dir(exe);
        size_t slash = dir.find_last_of('/');
        if (slash != std::string::npos) {
            dir = dir.substr(0, slash);
            for (int level = 0; level < 8 && !dir.empty(); ++level) {
                out.push_back(dir + "/bigqmt_client_config.yaml");
                size_t up = dir.find_last_of('/');
                if (up == std::string::npos) break;
                dir = dir.substr(0, up);
            }
        }
    }
    return out;
}

}  // namespace

ClientConfig ClientConfig::from_env() {
    ClientConfig cfg;
    apply_env_overrides(cfg);
    return cfg;
}

ClientConfig ClientConfig::from_yaml(const std::string& path) {
    std::string chosen;
    std::string text;
    for (const std::string& cand : config_candidates(path)) {
        std::ifstream file(cand);
        if (!file) continue;
        chosen = cand;
        text.assign((std::istreambuf_iterator<char>(file)),
                    std::istreambuf_iterator<char>());
        break;
    }
    if (chosen.empty()) {
        std::string searched;
        std::vector<std::string> cands = config_candidates(path);
        for (size_t i = 0; i < cands.size() && i < 6; ++i) {
            if (i) searched += ", ";
            searched += "'" + cands[i] + "'";
        }
        throw std::runtime_error(
            "bigqmt config: cannot open any of " + searched +
            " -- create bigqmt_client_config.yaml at the project root "
            "(next to main.cpp), set BIGQMT_CONFIG_FILE to its path, or set "
            "the BIGQMT_* environment variables");
    }
    std::map<std::string, std::string> kv = parse_yaml_flat(text);

    ClientConfig cfg;  // built-in defaults fill anything the file omits
    for (const auto& entry : kv) {
        const std::string& k = entry.first;
        const std::string& v = entry.second;
        if (k == "account_id") {
            cfg.account_id = v;
        } else if (k == "account_type") {
            cfg.account_type = v;
        } else if (k == "redis_host") {
            cfg.redis_host = v;
        } else if (k == "redis_port") {
            cfg.redis_port = static_cast<int>(yaml_strict_int(v, k));
        } else if (k == "redis_db") {
            cfg.redis_db = static_cast<int>(yaml_strict_int(v, k));
        } else if (k == "redis_username") {
            cfg.redis_username = v;
        } else if (k == "redis_password") {
            cfg.redis_password = v;
        } else if (k == "rpc_timeout_seconds") {
            cfg.rpc_timeout_seconds = yaml_strict_double(v, k);
        } else {
            std::fprintf(stderr, "[bigqmt] config: ignoring unknown key '%s'\n",
                         k.c_str());
        }
    }
    apply_env_overrides(cfg);
    return cfg;
}

// ---------------------------------------------------------------------------
// small utils
// ---------------------------------------------------------------------------

namespace {

// CRC-32 (IEEE, same polynomial as python zlib.crc32), used to derive a
// stable positive int surrogate for non-numeric 合同编号 strings.
class Crc32 {
public:
    Crc32() {
        for (uint32_t i = 0; i < 256; ++i) {
            uint32_t c = i;
            for (int k = 0; k < 8; ++k) c = (c & 1) ? 0xEDB88320u ^ (c >> 1) : c >> 1;
            table_[i] = c;
        }
    }
    uint32_t update(uint32_t crc, const char* data, size_t n) const {
        crc = ~crc;
        for (size_t i = 0; i < n; ++i) {
            crc = table_[(crc ^ static_cast<unsigned char>(data[i])) & 0xFF] ^ (crc >> 8);
        }
        return ~crc;
    }

private:
    uint32_t table_[256];
};
const Crc32& crc32_table() {
    static Crc32 table;
    return table;
}

bool all_ascii_digits(const std::string& s) {
    if (s.empty()) return false;
    for (char c : s) {
        if (c < '0' || c > '9') return false;
    }
    return true;
}

std::string random_hex_id() {
    // uuid4-style: 16 random bytes as hex. /dev/urandom for uniqueness.
    std::string out(32, '0');
    static const char* HEX = "0123456789abcdef";
    unsigned char bytes[16];
    FILE* f = std::fopen("/dev/urandom", "rb");
    if (f) {
        size_t got = std::fread(bytes, 1, sizeof(bytes), f);
        std::fclose(f);
        if (got == sizeof(bytes)) {
            for (size_t i = 0; i < sizeof(bytes); ++i) {
                out[2 * i] = HEX[bytes[i] >> 4];
                out[2 * i + 1] = HEX[bytes[i] & 0xF];
            }
            return out;
        }
    }
    // Fallback: PRNG seeded from the clock.
    static std::mt19937_64 rng(static_cast<uint64_t>(std::time(nullptr)) ^
                               static_cast<uint64_t>(
                                   std::chrono::steady_clock::now()
                                       .time_since_epoch()
                                       .count()));
    for (int i = 0; i < 16; ++i) {
        unsigned int b = static_cast<unsigned int>(rng() % 256);
        out[2 * i] = HEX[b >> 4];
        out[2 * i + 1] = HEX[b & 0xF];
    }
    return out;
}

// Standard base64 (no padding-free variants needed; python encodes with
// padding and the bridge decodes it the same way).
std::string base64_encode(const std::string& in) {
    static const char* T = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    std::string out;
    out.reserve(((in.size() + 2) / 3) * 4);
    size_t i = 0;
    for (; i + 3 <= in.size(); i += 3) {
        uint32_t v = (static_cast<unsigned char>(in[i]) << 16) |
                     (static_cast<unsigned char>(in[i + 1]) << 8) |
                     static_cast<unsigned char>(in[i + 2]);
        out += T[(v >> 18) & 63];
        out += T[(v >> 12) & 63];
        out += T[(v >> 6) & 63];
        out += T[v & 63];
    }
    size_t rest = in.size() - i;
    if (rest == 1) {
        uint32_t v = static_cast<unsigned char>(in[i]) << 16;
        out += T[(v >> 18) & 63];
        out += T[(v >> 12) & 63];
        out += "==";
    } else if (rest == 2) {
        uint32_t v = (static_cast<unsigned char>(in[i]) << 16) |
                     (static_cast<unsigned char>(in[i + 1]) << 8);
        out += T[(v >> 18) & 63];
        out += T[(v >> 12) & 63];
        out += T[(v >> 6) & 63];
        out += '=';
    }
    return out;
}

// The bridge keeps stock-code text out of plain Redis traffic by rotating the
// base64 digit alphabet before the "b64s:" prefix (redis_rpc.py). Digits '0'
//..'9' map onto punctuation; the server reverses it before decoding.
const char* kB64DigitSubst = "!#$%&()*~?";
std::string substitute_b64_digits(const std::string& encoded) {
    std::string out = encoded;
    for (char& c : out) {
        if (c >= '0' && c <= '9') c = kB64DigitSubst[c - '0'];
    }
    return out;
}

// local-time parse of QMT-style date/time text ("%Y-%m-%d %H:%M:%S[.%f]" or
// "%Y%m%d%H%M%S" digits), like python time.mktime(time.strptime(...)).
long long parse_unix_seconds_text(const std::string& raw) {
    std::string text;
    for (char c : raw) {
        if (c >= '0' && c <= '9') text += c;
    }
    if (text.size() < 8) return 0;
    if (text.size() >= 14) text = text.substr(0, 14);
    text = text.substr(0, 8) +
           (text.size() > 8 ? text.substr(8, 6) : std::string(6, '0'));
    struct tm tmv;
    std::memset(&tmv, 0, sizeof(tmv));
    char buf[32];
    std::snprintf(buf, sizeof(buf), "%s", text.c_str());
    if (std::sscanf(buf, "%4d%2d%2d%2d%2d%2d", &tmv.tm_year, &tmv.tm_mon,
                    &tmv.tm_mday, &tmv.tm_hour, &tmv.tm_min, &tmv.tm_sec) != 6) {
        return 0;
    }
    tmv.tm_year -= 1900;
    tmv.tm_mon -= 1;
    tmv.tm_isdst = -1;
    time_t t = std::mktime(&tmv);
    return t == static_cast<time_t>(-1) ? 0 : static_cast<long long>(t);
}

// _full_a_share_code: bare 6-digit codes gain their exchange suffix so
// callback consumers can key on the full form.
std::string full_a_share_code(const std::string& code) {
    std::string text;
    for (char c : code) text += static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
    if (text.find('.') != std::string::npos || text.size() != 6) return text;
    bool digits = true;
    for (char c : text) {
        if (!std::isdigit(static_cast<unsigned char>(c))) {
            digits = false;
            break;
        }
    }
    if (!digits) return text;
    static const char* kSh = "600601603605688689";
    static const char* kSz = "000001002003300301";
    for (int i = 0; kSh[i * 3]; ++i) {
        if (text.compare(0, 3, kSh + i * 3, 3) == 0) return text + ".SH";
    }
    for (int i = 0; kSz[i * 3]; ++i) {
        if (text.compare(0, 3, kSz + i * 3, 3) == 0) return text + ".SZ";
    }
    return text;
}

// action ("BUY"/"SELL" from the server) -> MiniQMT order_type 23/24.
// Returns 0 (as OrderAction) for anything unrecognized.
OrderAction action_to_order_type(const std::string& action) {
    std::string text;
    for (char c : action) text += static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
    if (text == "BUY") return OrderAction::Buy;
    if (text == "SELL") return OrderAction::Sell;
    if (text == "23") return OrderAction::Buy;   // server may echo the numeric form
    if (text == "24") return OrderAction::Sell;
    return static_cast<OrderAction>(0);
}

}  // namespace

// ---------------------------------------------------------------------------
// OrderId
// ---------------------------------------------------------------------------

OrderId::OrderId(std::string sys_id) : sys_id_(std::move(sys_id)) {
    if (sys_id_.empty()) {
        int_value_ = 0;  // python OrderId("") == 0
        return;
    }
    if (all_ascii_digits(sys_id_)) {
        errno = 0;
        char* end = nullptr;
        long long v = std::strtoll(sys_id_.c_str(), &end, 10);
        if (errno == 0 && end == sys_id_.c_str() + sys_id_.size() && v > 0) {
            int_value_ = v;
            return;
        }
        // Overflow or a leading-zero id whose int would not round trip:
        // fall through to the surrogate.
    }
    // Stable surrogate for non-numeric 合同编号: same string gives the same
    // value in any process (zlib.crc32 & 0x3FFFFFFF, or 1 to stay positive).
    uint32_t crc = crc32_table().update(0, sys_id_.data(), sys_id_.size());
    int_value_ = static_cast<long long>(crc & 0x3FFFFFFF);
    if (int_value_ == 0) int_value_ = 1;
}

// ---------------------------------------------------------------------------
// trader
// ---------------------------------------------------------------------------

BigQmtXtTrader::BigQmtXtTrader(ClientConfig config) : config_(std::move(config)) {
    // The shared command connection is used lazily (first command triggers a
    // connect, failures trigger a reconnect), so it never sees connect().
    // Hand it the endpoint/auth options up front; without this it would fall
    // back to the RedisConnection defaults (127.0.0.1:6379, no auth).
    RedisConnection::Options opts;
    opts.host = config_.redis_host;
    opts.port = config_.redis_port;
    opts.db = config_.redis_db;
    opts.username = config_.redis_username;
    opts.password = config_.redis_password;
    cmd_conn_.configure(opts);
}

BigQmtXtTrader::~BigQmtXtTrader() {
    if (event_running_ || !order_pipe_.exited || !outcome_pipe_.exited) {
        stop();
    }
}

int BigQmtXtTrader::register_callback(std::shared_ptr<XtQuantTraderCallback> callback) {
    std::lock_guard<std::mutex> lock(callback_mutex_);
    callback_ = std::move(callback);
    return 0;
}

void BigQmtXtTrader::send_cmd(const std::vector<std::string>& argv) {
    cmd_conn_.command(argv);
}

void BigQmtXtTrader::expect_ok(const std::string& what) {
    Resp r;
    if (!cmd_conn_.read(r)) {
        // Timed out. The reply may still arrive after we give up on it; drop
        // the socket so a stale frame is never consumed as the reply to the
        // next command on this shared connection.
        cmd_conn_.close();
        throw RpcError("redis rpc " + what + ": no reply (timeout)");
    }
    if (r.is_err()) {
        cmd_conn_.close();
        throw RpcError("redis rpc " + what + " failed: " + r.error_text());
    }
}

std::string BigQmtXtTrader::rpc_payload(const Json& request) {
    return std::string("b64s:") + substitute_b64_digits(base64_encode(request.dump()));
}

Json BigQmtXtTrader::call_impl(const std::string& method, const Json& params,
                               double timeout_seconds) {
    const std::string& account = config_.account_id;
    if (account.empty()) {
        throw RpcError(
            "Big QMT account_id is required -- set BIGQMT_ACCOUNT_ID or fix the "
            "ClientConfig defaults. The account id is what keys every Redis "
            "request queue and event channel.");
    }
    std::string request_id = random_hex_id();
    std::string resp_key = "bigqmt:rpc:resp:" + account + ":" + request_id;
    std::string resp_list = "bigqmt:rpc:respq:" + account + ":" + request_id;

    Json request = Json::make_object();
    request.set("schema_version", Json::make_int(1));
    request.set("request_id", Json::make_string(request_id));
    request.set("account_id", Json::make_string(account));
    request.set("method", Json::make_string(method));
    request.set("params", params);
    request.set("reply_channel", Json::make_string(resp_key));
    request.set("reply_list", Json::make_string(resp_list));
    request.set("reply_key", Json::make_string(resp_key));
    request.set("ttl_seconds", Json::make_int(60));

    std::string queue = "bigqmt:rpc:queue:" + account;
    std::string payload = rpc_payload(request);

    send_cmd({"RPUSH", queue, payload});
    expect_ok("submit");
    send_cmd({"EXPIRE", queue, "60"});
    expect_ok("expire");

    // Poll the reply key first; fall back to BLPOP on the per-request list,
    // exactly like call_redis_rpc(transport="queue").
    RedisConnection blpop_conn;
    RedisConnection::Options blopts;
    blopts.host = config_.redis_host;
    blopts.port = config_.redis_port;
    blopts.db = config_.redis_db;
    blopts.username = config_.redis_username;
    blopts.password = config_.redis_password;
    blopts.read_timeout_ms = 1000;

    double deadline = monotonic_now() + timeout_seconds;
    auto poll_key = [&]() -> Json {
        send_cmd({"GET", resp_key});
        Resp r;
        if (!cmd_conn_.read(r)) {
            // Read timeout on the shared connection. The GET reply may arrive
            // late; if it does while we already sent another command, its '{'
            // payload byte would be parsed as the next RESP frame header.
            // Drop the socket (next send_cmd reconnects) and keep polling.
            cmd_conn_.close();
            return Json();
        }
        if (r.kind != Resp::Kind::Str || r.text.empty()) return Json();
        try {
            return Json::parse(r.text);
        } catch (const JsonError&) {
            // Unparseable body (e.g. a stale frame such as "+OK" that slipped
            // through): treat as a miss and drop the polluted connection.
            cmd_conn_.close();
            return Json();
        }
    };

    Json response;
    bool have_response = false;
    while (monotonic_now() < deadline) {
        Json direct = poll_key();
        if (!direct.is_null()) {
            response = std::move(direct);
            have_response = true;
            break;
        }
        if (!blpop_conn.connected()) {
            // (Re)connect lazily: the command path above reconnects itself on
            // failure, but a fresh per-call connection avoids a partially
            // consumed reply on the shared socket.
            try {
                blpop_conn.connect(blopts);
            } catch (const RedisError&) {
                std::this_thread::sleep_for(std::chrono::milliseconds(200));
                continue;
            }
        }
        try {
            // BLPOP only takes whole seconds; 1s slices keep the deadline
            // honest (a nil at the 1s mark just loops back to poll_key).
            blpop_conn.command({"BLPOP", resp_list, "1"});
        } catch (const RedisError& e) {
            throw RpcError(std::string("redis rpc blpop failed: ") + e.what());
        }
        Resp r;
        if (!blpop_conn.read(r)) continue;  // blocking timeout: loop again
        if (r.is_err()) {
            if (r.text.find("NOAUTH") != std::string::npos ||
                r.text.find("WRONGPASS") != std::string::npos ||
                r.text.find("READONLY") != std::string::npos ||
                r.text.find("OOM") != std::string::npos) {
                throw RpcError("redis rpc blpop error: " + r.error_text());
            }
            continue;  // transient (e.g. busy key): keep waiting
        }
        if (r.kind == Resp::Kind::Arr && !r.items.empty()) {
            // BLPOP reply: [list_name, value]
            const Resp& value = r.items.back();
            if (value.kind == Resp::Kind::Str && !value.text.empty()) {
                send_cmd({"DEL", resp_list});  // best-effort cleanup
                try {
                    Resp del;
                    if (!cmd_conn_.read(del)) {
                        // The DEL reply is still owed; never leave it pending on
                        // the shared connection where the next RPC would consume
                        // it as its own reply.
                        cmd_conn_.close();
                    }
                } catch (...) {
                    cmd_conn_.close();
                }
                response = Json::parse(value.text);
                have_response = true;
                break;
            }
        }
    }
    if (!have_response) {
        // Last look at the reply key before giving up (the server may have
        // answered between our final poll and the deadline).
        Json direct = poll_key();
        if (!direct.is_null()) {
            response = std::move(direct);
            have_response = true;
        }
    }
    if (!have_response) {
        throw RpcTimeoutError("redis rpc timeout: " + method + " account_id=" + account +
                              " request_queue=" + queue);
    }

    if (!response.is_object()) {
        throw RpcError("redis rpc " + method + ": malformed response envelope");
    }
    // RPC envelope: {"ok": bool, "data": ..., "error"?, "server_error"?}
    if (!response.member_bool("ok", false)) {
        throw RpcServerRepliedError(response.member_string("error") != ""
                                        ? response.member_string("error")
                                        : "Big QMT RPC failed: " + method);
    }
    std::string server_error = response.member_string("server_error");
    if (!server_error.empty()) {
        throw RpcServerRepliedError("Big QMT " + method + " server_error: " + server_error);
    }
    const Json* data = response.get("data");
    return data ? *data : Json();
}

Json BigQmtXtTrader::call(const std::string& method, Json params) {
    std::lock_guard<std::mutex> lock(cmd_mutex_);
    // A stale shared connection transparently reconnects inside command();
    // transient errors surface as RpcError for the caller to decide.
    return call_impl(method, params, config_.rpc_timeout_seconds);
}

void BigQmtXtTrader::note_server_account_type(const Json& ping_data) {
    if (!ping_data.is_object()) return;
    std::string reported = ping_data.member_string("account_type");
    if (!reported.empty()) {
        server_account_type_ = reported;
    }
}

void BigQmtXtTrader::fire_account_status() {
    std::shared_ptr<XtQuantTraderCallback> cb;
    {
        std::lock_guard<std::mutex> lock(callback_mutex_);
        cb = callback_;
    }
    if (!cb) return;
    // status=1 == AccountStatus::Online (MiniQMT XtAccountStatus). The server
    // is authoritative about the account type; fall back to what the caller
    // declared, then "STOCK" (issue #103 parity).
    XtAccountStatus status;
    status.account_id = config_.account_id;
    status.account_type = server_account_type_.empty() ? declared_account_type_
                                                       : server_account_type_;
    if (status.account_type.empty()) status.account_type = "STOCK";
    status.status = AccountStatus::Online;
    try {
        cb->on_account_status(status);
    } catch (const std::exception& e) {
        std::fprintf(stderr, "[bigqmt] user callback failed: on_account_status: %s\n", e.what());
    } catch (...) {
        std::fprintf(stderr, "[bigqmt] user callback failed: on_account_status (unknown)\n");
    }
}

int BigQmtXtTrader::connect() {
    if (!config_.account_id.empty()) {
        Json pong;
        bool ping_ok = false;
        try {
            pong = call("ping");
            ping_ok = true;
        } catch (const std::exception& e) {
            std::fprintf(stderr, "[bigqmt] connect: ping failed: %s\n", e.what());
        }
        if (ping_ok) note_server_account_type(pong);
    }
    fire_account_status();
    return 0;
}

int BigQmtXtTrader::subscribe(const StockAccount& account) {
    // The declared account type never travels to the server; it only labels
    // the synthesized account status when the server did not answer ping.
    declared_account_type_ = account_type_name(account.account_type_code);
    if (config_.account_id.empty() && !account.account_id.empty()) {
        config_.account_id = account.account_id;
    }

    if (!event_running_) {
        event_running_ = true;
        event_thread_exited_ = false;
        event_thread_ = std::thread([this] { event_loop(); });
    }
    fire_account_status();
    return 0;
}

void BigQmtXtTrader::event_loop() {
    // Receive exec events from Redis pub/sub. The server publishes to Redis
    // whenever it can (even under a zmq transport), so a Redis listener is the
    // primary channel; on failure, sleep ~1s and re-select (mirrors the
    // Python client's per-round reconnect).
    while (event_running_) {
        RedisConnection conn;
        RedisConnection::Options opts;
        opts.host = config_.redis_host;
        opts.port = config_.redis_port;
        opts.db = config_.redis_db;
        opts.username = config_.redis_username;
        opts.password = config_.redis_password;
        opts.read_timeout_ms = 1000;
        try {
            conn.connect(opts);
            event_loop_redis(conn);
        } catch (const std::exception& e) {
            if (event_running_) {
                std::fprintf(stderr, "[bigqmt] exec-event listener: %s (retrying)\n", e.what());
            }
        } catch (...) {
        }
        conn.close();
        // Sleep in small slices so stop() is honoured promptly.
        for (int i = 0; i < 10 && event_running_; ++i) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
    }
    event_thread_exited_ = true;
}

void BigQmtXtTrader::event_loop_redis(RedisConnection& conn) {
    std::string acc = config_.account_id;
    char order_ch[256], trade_ch[256], order_err_ch[256], cancel_err_ch[256];
    std::snprintf(order_ch, sizeof(order_ch), kOrderChannelTemplate, acc.c_str());
    std::snprintf(trade_ch, sizeof(trade_ch), kTradeChannelTemplate, acc.c_str());
    std::snprintf(order_err_ch, sizeof(order_err_ch), kOrderErrorChannelTemplate, acc.c_str());
    std::snprintf(cancel_err_ch, sizeof(cancel_err_ch), kCancelErrorChannelTemplate,
                  acc.c_str());

    conn.command({"SUBSCRIBE", order_ch, trade_ch, order_err_ch, cancel_err_ch});

    long long last_heartbeat = 0;
    while (event_running_) {
        // Pub/sub connections carry no idle timeout at the protocol level,
        // but a dead TCP peer is only noticed when traffic flows; a periodic
        // PING keeps the round trip honest.
        long long now = static_cast<long long>(monotonic_now());
        if (now - last_heartbeat >= 15 && !conn.mid_frame()) {
            // Skip the PING while a push frame is half-delivered: sending then
            // would pair the pong with the wrong frame (and this reconnect would
            // lose the subscription). The pending frame completes on its own.
            conn.command({"PING"});
            last_heartbeat = now;
        }
        Resp msg;
        if (!conn.read(msg)) continue;  // read timeout: loop, honour stop()
        if (msg.kind != Resp::Kind::Arr || msg.items.size() < 3) continue;
        const Resp& kind = msg.items[0];
        if (kind.kind != Resp::Kind::Str) continue;
        const std::string& type = kind.text;
        if (type == "message") {
            const Resp& payload = msg.items[2];
            if (payload.kind != Resp::Kind::Str || payload.text.empty()) continue;
            try {
                Json event = Json::parse(payload.text);
                on_event_json(event);
            } catch (const JsonError&) {
                // not JSON (should not happen on these channels): drop
            }
        }
        // "subscribe"/"unsubscribe"/"pong" acks are ignored.
    }
}

// ---- event shaping ---------------------------------------------------------

namespace {

// Build the MiniQMT-shaped objects exactly as the Python compat layer does
// (_order_from_dict / _trade_from_dict / the order_error/cancel_error
// CompatObjects in _deliver_event). Safe defaults so a missing key never
// throws; the field spelling matches what the server publishes.
XtOrder order_from_event(const std::string& account_id, const Json& e) {
    XtOrder o;
    o.account_id = account_id;
    o.stock_code = full_a_share_code(e.member_string("stock_code"));
    o.order_type = action_to_order_type(e.member_string("action"));
    o.order_status =
        static_cast<OrderStatus>(e.member_int("status",
                                              e.member_int("order_status",
                                                           static_cast<long long>(
                                                               OrderStatus::Unknown))));
    o.order_volume = e.member_int("volume", e.member_int("order_volume", 0));
    o.traded_volume = e.member_int("traded_volume", 0);
    o.price = e.member_double("price", 0.0);
    o.traded_price = e.member_double("traded_price",
                                     e.member_double("avg_traded_price", 0.0));
    o.trade_amount = e.member_double("trade_amount", 0.0);
    o.order_sysid = e.member_string("order_sys_id",
                                    e.member_string("order_sysid",
                                                    e.member_string("order_id", "")));
    o.order_id = OrderId(o.order_sysid.empty()
                             ? e.member_string("user_order_id")
                             : o.order_sysid);
    o.strategy_name = e.member_string("strategy_name");
    o.order_remark = e.member_string("remark",
                                    e.member_string("user_order_id", ""));
    o.order_time = e.member_int("order_time", 0);
    if (o.order_time == 0) {
        o.order_time = static_cast<long long>(e.member_double("created_at_ts", 0.0));
    }
    o.status_msg = e.member_string("status_msg");
    o.price_type = static_cast<StockPriceType>(e.member_int("price_type", 0));
    o.account_type =
        static_cast<AccountType>(e.member_int("account_type",
                                              static_cast<long long>(AccountType::Security)));
    o.instrument_name = e.member_string("instrument_name");
    o.secu_account = e.member_string("secu_account");
    o.offset_flag = e.member_int("offset_flag", 0);
    o.direction = e.member_int("direction", 0);
    return o;
}

XtTrade trade_from_event(const std::string& account_id, const Json& e) {
    XtTrade t;
    t.account_id = account_id;
    t.stock_code = full_a_share_code(e.member_string("stock_code"));
    t.order_type = action_to_order_type(e.member_string("action"));
    t.order_sysid = e.member_string("order_sys_id",
                                    e.member_string("order_sysid", ""));
    t.order_id = OrderId(t.order_sysid);
    t.trade_id = e.member_string("trade_id");
    t.traded_id = t.trade_id;
    t.traded_volume = e.member_int("volume", e.member_int("traded_volume", 0));
    t.traded_price = e.member_double("price", e.member_double("traded_price", 0.0));
    double amount = e.member_double("amount", 0.0);
    if (amount == 0.0) amount = t.traded_price * static_cast<double>(t.traded_volume);
    t.traded_amount = amount;
    // traded_time priority: server real time -> created_at_ts -> traded_at text.
    long long ts = e.member_int("traded_time", 0);
    if (ts == 0) ts = static_cast<long long>(e.member_double("created_at_ts", 0.0));
    if (ts == 0) ts = parse_unix_seconds_text(e.member_string("traded_at"));
    t.traded_time = ts;
    t.traded_at = e.member_string("traded_at");
    t.strategy_name = e.member_string("strategy_name");
    t.order_remark = e.member_string("user_order_id",
                                     e.member_string("remark", ""));
    t.account_type =
        static_cast<AccountType>(e.member_int("account_type",
                                              static_cast<long long>(AccountType::Security)));
    t.instrument_name = e.member_string("instrument_name");
    t.secu_account = e.member_string("secu_account");
    t.commission = e.member_double("commission", 0.0);
    t.offset_flag = e.member_int("offset_flag", 0);
    t.direction = e.member_int("direction", 0);
    return t;
}

XtOrderError order_error_from_event(const Json& e) {
    XtOrderError oe;
    std::string sysid = e.member_string("order_sys_id",
                                        e.member_string("order_sysid",
                                                        e.member_string("order_id", "")));
    oe.error_id = e.member_int("error_id", 0);
    oe.error_msg = e.member_string("error_msg");
    oe.order_sysid = sysid;
    oe.order_sys_id = sysid;
    oe.order_id = OrderId(sysid);
    oe.stock_code = e.member_string("stock_code");
    oe.order_remark = e.member_string("order_remark",
                                      e.member_string("remark",
                                                      e.member_string("user_order_id", "")));
    oe.strategy_name = e.member_string("strategy_name");
    oe.status = e.member_int("status", e.member_int("order_status", 0));
    return oe;
}

XtCancelError cancel_error_from_event(const Json& e) {
    XtCancelError ce;
    std::string sysid = e.member_string("order_sys_id",
                                        e.member_string("order_sysid",
                                                        e.member_string("order_id", "")));
    ce.error_id = e.member_int("error_id", 0);
    ce.error_msg = e.member_string("error_msg");
    ce.order_sysid = sysid;
    ce.order_sys_id = sysid;
    ce.order_id = OrderId(sysid);
    ce.stock_code = e.member_string("stock_code");
    ce.order_remark = e.member_string("order_remark",
                                      e.member_string("remark",
                                                      e.member_string("user_order_id", "")));
    return ce;
}

}  // namespace

void BigQmtXtTrader::on_event_json(const Json& event) {
    if (!event.is_object()) return;
    std::string event_type = event.member_string("event_type");
    try {
        sweep_barriers();
        if (event_type == "order" || event_type == "trade" ||
            event_type == "order_error" || event_type == "cancel_error") {
            if (hold_if_pending(event)) return;  // waits for its async response
        }
    } catch (const std::exception& e) {
        // A barrier fault must never swallow an event (Python parity).
        std::fprintf(stderr, "[bigqmt] barrier handling failed: %s\n", e.what());
    }
    deliver_event(event);
}

void BigQmtXtTrader::deliver_event(const Json& event) {
    std::shared_ptr<XtQuantTraderCallback> cb;
    {
        std::lock_guard<std::mutex> lock(callback_mutex_);
        cb = callback_;
    }
    if (!cb) return;
    std::string account_id = event.member_string("account_id");
    if (account_id.empty()) account_id = config_.account_id;
    std::string event_type = event.member_string("event_type");

    try {
        if (event_type == "trade") {
            cb->on_stock_trade(trade_from_event(account_id, event));
        } else if (event_type == "order") {
            cb->on_stock_order(order_from_event(account_id, event));
        } else if (event_type == "order_error") {
            cb->on_order_error(order_error_from_event(event));
        } else if (event_type == "cancel_error") {
            cb->on_cancel_error(cancel_error_from_event(event));
        }
    } catch (const std::exception& e) {
        std::fprintf(stderr, "[bigqmt] user callback failed: event_type=%s: %s\n",
                     event_type.c_str(), e.what());
    } catch (...) {
        std::fprintf(stderr, "[bigqmt] user callback failed: event_type=%s (unknown)\n",
                     event_type.c_str());
    }
}

// ---- issue #51 ordering barrier -------------------------------------------

bool BigQmtXtTrader::arm_barrier(const std::string& remark, long long seq) {
    if (remark.empty()) return false;  // nothing to correlate on
    std::lock_guard<std::mutex> lock(barrier_mutex_);
    // A reused remark supersedes an older pending entry; its held events must
    // not be lost, so hand them back to the caller for immediate delivery.
    for (auto it = barriers_.begin(); it != barriers_.end(); ++it) {
        if (it->first == remark) {
            BarrierEntry old = std::move(it->second);
            barriers_.erase(it);
            BarrierEntry entry;
            entry.seq = seq;
            entry.deadline = monotonic_now() + kBarrierTimeoutSeconds;
            barriers_.emplace_front(remark, std::move(entry));
            deliver_barrier_events(old);  // may invoke callbacks: keep it outside
            return true;
        }
    }
    BarrierEntry entry;
    entry.seq = seq;
    entry.deadline = monotonic_now() + kBarrierTimeoutSeconds;
    barriers_.emplace_front(remark, std::move(entry));
    return true;
}

void BigQmtXtTrader::release_barrier(const std::string& remark, long long seq) {
    if (remark.empty()) return;
    BarrierEntry entry;
    bool found = false;
    {
        std::lock_guard<std::mutex> lock(barrier_mutex_);
        for (auto it = barriers_.begin(); it != barriers_.end(); ++it) {
            if (it->first == remark) {
                if (seq != 0 && it->second.seq != seq) return;  // newer order owns it
                entry = std::move(it->second);
                barriers_.erase(it);
                found = true;
                break;
            }
        }
    }
    if (found) deliver_barrier_events(entry);
}

void BigQmtXtTrader::sweep_barriers() {
    // Release barriers whose response never arrived (failed submits, lost
    // messages): holding events forever is worse than ordering them wrongly.
    std::vector<BarrierEntry> expired;
    {
        std::lock_guard<std::mutex> lock(barrier_mutex_);
        double now = monotonic_now();
        for (auto it = barriers_.begin(); it != barriers_.end();) {
            if (now >= it->second.deadline) {
                expired.push_back(std::move(it->second));
                it = barriers_.erase(it);
            } else {
                ++it;
            }
        }
    }
    for (auto& entry : expired) deliver_barrier_events(entry);
}

bool BigQmtXtTrader::hold_if_pending(const Json& event) {
    std::lock_guard<std::mutex> lock(barrier_mutex_);
    if (barriers_.empty()) return false;
    std::string remark = event.member_string("remark");
    if (remark.empty()) remark = event.member_string("user_order_id");
    std::string sys_id = event.member_string("order_sys_id");
    if (sys_id.empty()) sys_id = event.member_string("order_sysid");

    BarrierEntry* entry = nullptr;
    if (!remark.empty()) {
        for (auto& kv : barriers_) {
            if (kv.first == remark) {
                entry = &kv.second;
                break;
            }
        }
    }
    if (!entry && !sys_id.empty()) {
        // A trade event may arrive without the remark; correlate via the
        // order_sys_id learned from the held order events.
        for (auto& kv : barriers_) {
            for (const auto& known : kv.second.sys_ids) {
                if (known == sys_id) {
                    entry = &kv.second;
                    break;
                }
            }
            if (entry) break;
        }
    }
    if (!entry) return false;
    if (!sys_id.empty()) {
        bool known = false;
        for (const auto& s : entry->sys_ids) {
            if (s == sys_id) {
                known = true;
                break;
            }
        }
        if (!known) entry->sys_ids.push_back(sys_id);
    }

    BarrierEntry::HeldEvent held;
    std::string event_type = event.member_string("event_type");
    std::string account_id = event.member_string("account_id");
    if (account_id.empty()) account_id = config_.account_id;
    if (event_type == "trade") {
        held.type = BarrierEntry::HeldEvent::Type::Trade;
        held.trade = trade_from_event(account_id, event);
    } else if (event_type == "order") {
        held.type = BarrierEntry::HeldEvent::Type::Order;
        held.order = order_from_event(account_id, event);
    } else if (event_type == "order_error") {
        held.type = BarrierEntry::HeldEvent::Type::OrderError;
        held.order_error = order_error_from_event(event);
    } else if (event_type == "cancel_error") {
        held.type = BarrierEntry::HeldEvent::Type::CancelError;
        held.cancel_error = cancel_error_from_event(event);
    } else {
        return false;
    }
    entry->events.push_back(std::move(held));
    return true;
}

void BigQmtXtTrader::deliver_barrier_events(const BarrierEntry& entry) {
    std::shared_ptr<XtQuantTraderCallback> cb;
    {
        std::lock_guard<std::mutex> lock(callback_mutex_);
        cb = callback_;
    }
    if (!cb) return;
    for (const auto& held : entry.events) {
        try {
            switch (held.type) {
                case BarrierEntry::HeldEvent::Type::Order:
                    cb->on_stock_order(held.order);
                    break;
                case BarrierEntry::HeldEvent::Type::Trade:
                    cb->on_stock_trade(held.trade);
                    break;
                case BarrierEntry::HeldEvent::Type::OrderError:
                    cb->on_order_error(held.order_error);
                    break;
                case BarrierEntry::HeldEvent::Type::CancelError:
                    cb->on_cancel_error(held.cancel_error);
                    break;
            }
        } catch (const std::exception& e) {
            std::fprintf(stderr, "[bigqmt] user callback failed (barrier release): %s\n",
                         e.what());
        }
    }
}

// ---- async order pipeline --------------------------------------------------

long long BigQmtXtTrader::order_stock_async(const StockAccount& account,
                                            const std::string& stock_code,
                                            OrderAction order_type, long long order_volume,
                                            StockPriceType price_type, double price,
                                            const std::string& strategy_name,
                                            const std::string& order_remark) {
    long long seq = ++async_seq_;
    // Arm the barrier BEFORE queueing: the push may beat this function's
    // return (Python parity, issue #51).
    OrderJob job;
    job.seq = seq;
    job.account = account;
    job.stock_code = stock_code;
    job.order_type = static_cast<long long>(order_type);  // wire side: plain code
    job.order_volume = order_volume;
    job.price_type = static_cast<long long>(price_type);
    job.price = price;
    job.strategy_name = strategy_name;
    job.order_remark = order_remark;

    // Order of the first two statements matters: barrier first, then the
    // pending counter, so a response that races the enqueue still sees the
    // barrier armed.
    arm_barrier(order_remark, seq);

    {
        std::lock_guard<std::mutex> lock(order_pipe_.m);
        order_pipe_.order_jobs.push_back(std::move(job));
        ++async_pending_;
    }
    order_pipe_.cv.notify_all();
    start_pipelines();
    return seq;
}

void BigQmtXtTrader::start_pipelines() {
    // Lazily started on first use (Python parity: a client that never calls
    // order_stock_async pays nothing).
    std::lock_guard<std::mutex> lock(order_pipe_.m);
    if (!order_thread_.joinable()) {
        order_thread_ = std::thread([this] { order_worker_loop(); });
    }
    if (!outcome_thread_.joinable()) {
        outcome_thread_ = std::thread([this] { outcome_worker_loop(); });
    }
}

void BigQmtXtTrader::order_worker_loop() {
    for (;;) {
        OrderJob job;
        {
            std::unique_lock<std::mutex> lock(order_pipe_.m);
            order_pipe_.cv.wait(lock, [this] {
                return !order_pipe_.order_jobs.empty() || order_pipe_.stopping;
            });
            if (order_pipe_.order_jobs.empty()) {
                if (order_pipe_.stopping) {
                    order_pipe_.exited = true;
                    order_pipe_.cv.notify_all();
                    return;
                }
                continue;
            }
            job = std::move(order_pipe_.order_jobs.front());
            order_pipe_.order_jobs.pop_front();
            if (job.seq == 0) {  // shutdown sentinel
                if (order_pipe_.stopping) {
                    order_pipe_.exited = true;
                    order_pipe_.cv.notify_all();
                    return;
                }
                continue;
            }
        }
        try {
            submit_order(job);
        } catch (const std::exception& e) {
            // A worker that dies takes every later async order with it; a
            // failure must surface as the order's error outcome instead.
            std::fprintf(stderr, "[bigqmt] async order submit failed seq=%lld: %s\n",
                         static_cast<long long>(job.seq), e.what());
            OutcomeUnit unit;
            unit.is_error = true;
            unit.seq = job.seq;
            unit.remark = job.order_remark;
            unit.stock_code = job.stock_code;
            unit.order_remark = job.order_remark;
            unit.error_id = 0;
            unit.error_msg = e.what();
            {
                std::lock_guard<std::mutex> lock(outcome_pipe_.m);
                outcome_pipe_.outcomes.push_back(std::move(unit));
            }
            outcome_pipe_.cv.notify_all();
        }
    }
}

void BigQmtXtTrader::submit_order(const OrderJob& job) {
    // wait_settlement=false: the server answers as soon as passorder returns,
    // without waiting for QMT to assign the 合同编号. The id then arrives
    // through the order push, which the barrier learns from.
    std::string result_id = order_stock_result(job.account, job.stock_code,
                                               job.order_type, job.order_volume,
                                               job.price_type, job.price,
                                               job.strategy_name, job.order_remark,
                                               /*wait_settlement=*/false);
    OutcomeUnit unit;
    unit.seq = job.seq;
    unit.remark = job.order_remark;
    unit.stock_code = job.stock_code;
    unit.strategy_name = job.strategy_name;
    unit.order_remark = job.order_remark;
    if (result_id == "-1") {
        // order_stock returned -1: the submit itself failed (the server also
        // pushes an order_error for a 废单 -- the two carry different info).
        unit.is_error = true;
        unit.error_id = -1;
        unit.error_msg = "order submit failed (order_stock returned -1)";
    } else {
        unit.is_error = false;
        unit.order_sys_id = result_id;
        unit.user_order_id = job.order_remark;
        unit.wait_for_sysid = true;
    }
    {
        std::lock_guard<std::mutex> lock(outcome_pipe_.m);
        outcome_pipe_.outcomes.push_back(std::move(unit));
    }
    outcome_pipe_.cv.notify_all();
}

void BigQmtXtTrader::outcome_worker_loop() {
    for (;;) {
        OutcomeUnit unit;
        {
            std::unique_lock<std::mutex> lock(outcome_pipe_.m);
            outcome_pipe_.cv.wait(lock, [this] {
                return !outcome_pipe_.outcomes.empty() || outcome_pipe_.stopping;
            });
            if (outcome_pipe_.outcomes.empty()) {
                if (outcome_pipe_.stopping) {
                    outcome_pipe_.exited = true;
                    outcome_pipe_.cv.notify_all();
                    return;
                }
                continue;
            }
            unit = std::move(outcome_pipe_.outcomes.front());
            outcome_pipe_.outcomes.pop_front();
            if (unit.seq == 0) {  // shutdown sentinel
                if (outcome_pipe_.stopping) {
                    outcome_pipe_.exited = true;
                    outcome_pipe_.cv.notify_all();
                    return;
                }
                continue;
            }
        }
        // Fire the outcome, then release the barrier so this order's held
        // push events are delivered after its response (issue #51).
        try {
            fire_outcome(unit);
        } catch (const std::exception& e) {
            std::fprintf(stderr, "[bigqmt] async outcome dispatch failed seq=%lld: %s\n",
                         static_cast<long long>(unit.seq), e.what());
        }
        release_barrier(unit.remark, unit.seq);
        --async_pending_;
        outcome_pipe_.cv.notify_all();  // wake wait_async_orders
    }
}

std::string BigQmtXtTrader::learn_sysid(const std::string& remark, double max_wait_seconds) {
    // The push usually beats the RPC reply, so the barrier may already hold an
    // order event carrying the real 合同编号. Wait briefly, then fall back to
    // the remark (Python's ASYNC_SYSID_WAIT_SECONDS = 2.0 behaviour).
    double deadline = monotonic_now() + max_wait_seconds;
    for (;;) {
        {
            std::lock_guard<std::mutex> lock(barrier_mutex_);
            for (const auto& kv : barriers_) {
                if (kv.first != remark) continue;
                if (!kv.second.sys_ids.empty()) {
                    std::string best = kv.second.sys_ids.front();
                    for (const auto& s : kv.second.sys_ids) {
                        if (s < best) best = s;  // python picks sorted()[0]
                    }
                    return best;
                }
            }
        }
        if (monotonic_now() >= deadline) return "";
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
}

void BigQmtXtTrader::fire_outcome(const OutcomeUnit& unit) {
    std::shared_ptr<XtQuantTraderCallback> cb;
    {
        std::lock_guard<std::mutex> lock(callback_mutex_);
        cb = callback_;
    }
    if (unit.is_error) {
        if (!cb) return;
        XtOrderError error;
        error.error_id = unit.error_id;
        error.error_msg = unit.error_msg;
        error.order_sysid = "";
        error.order_sys_id = "";
        error.order_id = OrderId("");
        error.stock_code = unit.stock_code;
        error.seq = unit.seq;
        error.order_remark = unit.order_remark;
        try {
            cb->on_order_error(error);
        } catch (const std::exception& e) {
            std::fprintf(stderr, "[bigqmt] user callback failed: on_order_error: %s\n",
                         e.what());
        }
        return;
    }
    if (!cb) return;
    // The server usually has no 合同编号 yet when it answers; learn it from
    // the held order event before firing, so order_id is real (issue #72).
    std::string order_sys_id = unit.order_sys_id;
    if (unit.wait_for_sysid && order_sys_id.empty() && !unit.remark.empty()) {
        order_sys_id = learn_sysid(unit.remark, kSysidLearnWaitSeconds);
    }
    XtOrderStockResponse response;
    response.account_id = config_.account_id;
    response.seq = unit.seq;
    response.order_id = OrderId(order_sys_id.empty() ? unit.user_order_id : order_sys_id);
    response.order_sysid = order_sys_id;
    response.stock_code = unit.stock_code;
    response.strategy_name = unit.strategy_name;
    response.order_remark = unit.order_remark;
    response.error_msg = "";
    try {
        cb->on_order_stock_async_response(response);
    } catch (const std::exception& e) {
        std::fprintf(stderr,
                     "[bigqmt] user callback failed: on_order_stock_async_response: %s\n",
                     e.what());
    }
}

std::string BigQmtXtTrader::order_stock_result(const StockAccount& account,
                                               const std::string& stock_code,
                                               long long order_type, long long order_volume,
                                               long long price_type, double price,
                                               const std::string& strategy_name,
                                               const std::string& order_remark,
                                               bool wait_settlement) {
    std::string account_id = account.account_id.empty() ? config_.account_id
                                                        : account.account_id;
    // An empty user remark gets a unique tag so the bridge can correlate and
    // dedup; an explicit remark travels verbatim (Python parity).
    std::string user_order_id = order_remark;

    Json payload = Json::make_object();
    payload.set("account_id", Json::make_string(account_id));
    payload.set("stock_code", Json::make_string(stock_code));
    payload.set("order_type", Json::make_int(order_type));
    payload.set("order_volume", Json::make_int(order_volume));
    payload.set("price_type", Json::make_int(price_type));
    payload.set("price", Json::make_double(price));
    payload.set("strategy_name", Json::make_string(strategy_name));
    payload.set("order_remark", Json::make_string(user_order_id));
    if (!wait_settlement) {
        payload.set("wait_settlement", Json::make_bool(false));
    }
    try {
        Json data = call("order_stock", payload);
        if (data.is_number()) {
            // data == -1: submit failed (the string "-1" is handled below).
            return data.as_int64(0) == -1 ? "-1" : "";
        }
        std::string sys_id = data.member_string("order_sys_id");
        if (sys_id.empty()) sys_id = data.member_string("order_sysid");
        return sys_id;
    } catch (const RpcTimeoutError& e) {
        throw RpcTimeoutError(
            std::string("order_stock rpc timeout; user_order_id=") + user_order_id +
            ". Query orders/trades before retrying to avoid duplicate orders. " + e.what());
    }
}

bool BigQmtXtTrader::wait_async_orders(double timeout_seconds) {
    double deadline = monotonic_now() + timeout_seconds;
    std::unique_lock<std::mutex> lock(outcome_pipe_.m);
    while (async_pending_.load() > 0) {
        double remain = deadline - monotonic_now();
        if (remain <= 0) return false;
        outcome_pipe_.cv.wait_for(lock, std::chrono::duration<double>(remain));
    }
    return true;
}

int BigQmtXtTrader::stop() {
    // 1. stop accepting new events
    event_running_ = false;

    // 2. drain queued orders (bounded), then wind the workers down
    bool drained = wait_async_orders(kShutdownDrainSeconds);
    {
        std::lock_guard<std::mutex> lock(order_pipe_.m);
        order_pipe_.stopping = true;
        OrderJob sentinel;  // seq == 0
        order_pipe_.order_jobs.push_back(sentinel);
    }
    order_pipe_.cv.notify_all();
    {
        std::lock_guard<std::mutex> lock(outcome_pipe_.m);
        outcome_pipe_.stopping = true;
        OutcomeUnit sentinel;  // seq == 0
        outcome_pipe_.outcomes.push_back(sentinel);
    }
    outcome_pipe_.cv.notify_all();
    (void)drained;

    // The outcome thread releases barriers as it drains; sweep below runs
    // after the threads are joined to flush anything left behind.
    join_pipeline(order_pipe_, order_thread_, config_.rpc_timeout_seconds + 4.0,
                  "async-order worker");
    join_pipeline(outcome_pipe_, outcome_thread_, 6.0, "async-outcome dispatcher");
    sweep_barriers();

    // 3. stop the event listener (read timeout wakes it within ~1s)
    double deadline = monotonic_now() + 3.0;
    while (!event_thread_exited_ && monotonic_now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    if (event_thread_.joinable()) {
        if (event_thread_exited_) {
            event_thread_.join();
        } else {
            std::fprintf(stderr, "[bigqmt] event listener did not exit in time\n");
            event_thread_.detach();
        }
    }
    return 0;
}

void BigQmtXtTrader::join_pipeline(Pipeline& pipe, std::thread& t, double budget_seconds,
                                   const char* what) {
    if (!t.joinable()) return;
    std::unique_lock<std::mutex> lock(pipe.m);
    if (!pipe.cv.wait_for(lock, std::chrono::duration<double>(budget_seconds),
                          [&] { return pipe.exited; })) {
        std::fprintf(stderr, "[bigqmt] %s did not exit within %.0fs; detaching\n",
                     what, budget_seconds);
        lock.unlock();
        t.detach();
        return;
    }
    lock.unlock();
    t.join();
}

// ---------------------------------------------------------------------------
// account queries (RPC methods mirror the bridge READ_METHODS)
// ---------------------------------------------------------------------------

namespace {

// true when the key exists and is not JSON null; writes the value into out.
bool json_number_or_null(const Json& obj, const char* key, double& out) {
    const Json* v = obj.get(key);
    if (!v || v->is_null()) return false;
    out = v->as_double(0.0);
    return true;
}

XtAsset asset_from_data(const Json& data) {
    XtAsset asset;
    asset.account_id = data.member_string("account_id");
    asset.has_cash = json_number_or_null(data, "cash", asset.cash);
    asset.has_total_asset = json_number_or_null(data, "total_asset", asset.total_asset);
    asset.has_frozen_cash = json_number_or_null(data, "frozen_cash", asset.frozen_cash);
    asset.has_market_value = json_number_or_null(data, "market_value", asset.market_value);
    return asset;
}

// PositionSnapshot -> XtPosition; defaults mirror xtquant_compat.py's
// _position_object (cost 未上报时补 0 / 最新价缺失时市值按 价x量 估算等).
XtPosition position_from_item(const std::string& account_id, const Json& item) {
    XtPosition p;
    p.account_id = account_id;
    p.stock_code = full_a_share_code(item.member_string("stock_code"));
    p.stock_name = item.member_string("stock_name");
    p.volume = item.member_int("volume", 0);
    p.can_use_volume = item.member_int("available", p.volume);
    p.frozen_volume = item.member_int("frozen_volume", 0);
    p.on_road_volume = item.member_int("on_road_volume", 0);
    p.direction = item.member_int("direction", 48);
    double cost = item.member_double("cost", 0.0);
    p.avg_price = cost;
    p.price = item.member_double("price", 0.0);
    double market_value = 0.0;
    if (!json_number_or_null(item, "market_value", market_value)) {
        market_value = p.price * static_cast<double>(p.volume);  // 未上报时估算
    }
    p.market_value = market_value;
    double open_price = cost;
    if (!json_number_or_null(item, "open_price", open_price)) open_price = cost;
    p.open_price = open_price;
    const Json* yv = item.get("yesterday_volume");
    p.yesterday_volume = (yv && !yv->is_null()) ? yv->as_int64(p.volume) : p.volume;
    return p;
}

// The bridge shapes both its exec-event pushes AND its ORDER/DEAL query rows
// from the same mini-QMT row (action/volume/status/remark/order_sys_id...),
// so the event converters already accept query rows verbatim. The aliases
// below exist only to say so at the call site.
inline XtOrder order_from_snapshot(const std::string& account_id, const Json& row) {
    return order_from_event(account_id, row);
}
inline XtTrade trade_from_snapshot(const std::string& account_id, const Json& row) {
    return trade_from_event(account_id, row);
}

// Resolves a per-call account override: empty StockAccount -> config account.
std::string resolved_account_id(const ClientConfig& config, const StockAccount& account) {
    return account.account_id.empty() ? config.account_id : account.account_id;
}

}  // namespace

XtAsset BigQmtXtTrader::get_asset(const StockAccount& account) {
    const std::string account_id = resolved_account_id(config_, account);
    Json params = Json::make_object();
    params.set("account_id", Json::make_string(account_id));
    Json data = call("get_asset", std::move(params));
    XtAsset asset;
    asset.account_id = account_id;
    if (data.is_object()) asset = asset_from_data(data);
    return asset;
}

std::vector<XtPosition> BigQmtXtTrader::get_positions(const StockAccount& account) {
    const std::string account_id = resolved_account_id(config_, account);
    Json params = Json::make_object();
    params.set("account_id", Json::make_string(account_id));
    Json data = call("get_positions", std::move(params));
    std::vector<XtPosition> out;
    if (data.is_array()) {
        for (const auto& item : data.array_items()) {
            out.push_back(position_from_item(account_id, item));
        }
    } else if (data.is_object()) {
        // The server answers with a code -> snapshot map; expand to a list.
        for (const auto& kv : data.object_items()) {
            out.push_back(position_from_item(account_id, kv.second));
        }
    }
    return out;
}

bool BigQmtXtTrader::query_stock_position(const StockAccount& account,
                                          const std::string& stock_code,
                                          XtPosition* out) {
    const std::string account_id = resolved_account_id(config_, account);
    Json params = Json::make_object();
    params.set("account_id", Json::make_string(account_id));
    params.set("stock_code", Json::make_string(stock_code));
    Json data = call("query_stock_position", std::move(params));
    if (data.is_null()) return false;  // 无该股持仓 (server data == null)
    if (out) *out = position_from_item(account_id, data);
    return true;
}

std::vector<XtOrder> BigQmtXtTrader::query_orders(const StockAccount& account,
                                                  const std::string& strategy_name,
                                                  bool cancelable_only) {
    const std::string account_id = resolved_account_id(config_, account);
    Json params = Json::make_object();
    params.set("account_id", Json::make_string(account_id));
    if (!strategy_name.empty()) {
        params.set("strategy_name", Json::make_string(strategy_name));
    }
    if (cancelable_only) {
        params.set("cancelable_only", Json::make_bool(true));
    }
    Json data = call("query_orders", std::move(params));
    std::vector<XtOrder> out;
    if (data.is_array()) {
        for (const auto& row : data.array_items()) {
            out.push_back(order_from_snapshot(account_id, row));
        }
    }
    return out;
}

std::vector<XtTrade> BigQmtXtTrader::query_trades(const StockAccount& account,
                                                  const std::string& strategy_name) {
    const std::string account_id = resolved_account_id(config_, account);
    Json params = Json::make_object();
    params.set("account_id", Json::make_string(account_id));
    if (!strategy_name.empty()) {
        params.set("strategy_name", Json::make_string(strategy_name));
    }
    Json data = call("query_trades", std::move(params));
    std::vector<XtTrade> out;
    if (data.is_array()) {
        for (const auto& row : data.array_items()) {
            out.push_back(trade_from_snapshot(account_id, row));
        }
    }
    return out;
}

Json BigQmtXtTrader::get_history_trade_detail_data(const StockAccount& account,
                                                   const std::string& detail_type,
                                                   const std::string& start_date,
                                                   const std::string& end_date) {
    const std::string account_id = resolved_account_id(config_, account);
    Json params = Json::make_object();
    params.set("account_id", Json::make_string(account_id));
    params.set("detail_type", Json::make_string(detail_type));
    params.set("start_date", Json::make_string(start_date));
    params.set("end_date", Json::make_string(end_date));
    return call("get_history_trade_detail_data", std::move(params));
}

Json BigQmtXtTrader::get_value_by_order_id(const StockAccount& account,
                                           const std::string& order_id,
                                           const std::string& detail_type) {
    const std::string account_id = resolved_account_id(config_, account);
    Json params = Json::make_object();
    params.set("account_id", Json::make_string(account_id));
    params.set("order_id", Json::make_string(order_id));
    params.set("detail_type", Json::make_string(detail_type));
    return call("get_value_by_order_id", std::move(params));
}

std::string BigQmtXtTrader::get_last_order_id(const StockAccount& account,
                                              const std::string& detail_type) {
    const std::string account_id = resolved_account_id(config_, account);
    Json params = Json::make_object();
    params.set("account_id", Json::make_string(account_id));
    params.set("detail_type", Json::make_string(detail_type));
    Json data = call("get_last_order_id", std::move(params));
    if (data.is_string()) return data.as_string();
    if (data.is_number()) return std::to_string(data.as_int64(0));
    return data.is_null() ? "-1" : "";
}

long long BigQmtXtTrader::cancel_rpc(const StockAccount& account,
                                     const std::string& order_id,
                                     std::string* reason) {
    // 镜像 xtquant_compat.cancel_order_stock_sysid: 桥端别名到 cancel_order,
    // 按 合同编号 (order_sysid) 撤; Python 契约 0 成功 / -1 失败 (issue #113:
    // 早期把 bool 当返回值的坑是 False == 0, 失败会被误判为成功)。
    const std::string account_id = resolved_account_id(config_, account);
    Json params = Json::make_object();
    params.set("account_id", Json::make_string(account_id));
    params.set("market", Json::make_string(""));
    params.set("order_sysid", Json::make_string(order_id));
    Json data = call("cancel_order_stock_sysid", std::move(params));
    if (!data.is_object()) return -1;  // 意外形状按失败处理
    // python: bool(data.get("success", data)) -- 有 success 字段取其值
    // (null == False, 与 python bool(None) 一致), 没有则按整个 dict 的真值
    // (非空 == 成功)。
    const Json* s = data.get("success");
    bool ok;
    if (s) {
        ok = !s->is_null() && s->as_bool(false);
    } else {
        ok = !data.object_items().empty();
    }
    if (!ok && reason) {
        // 桥的取消结果对象 (CancelResult) 序列化后带 message 等键: 被拒
        // 时把真实原因带回去 (例如 "cancel returned false", 或 settle 给出
        // "cancel was not confirmed: order ... still status ...")。
        static const char* const kReasonKeys[] = {"message", "error_msg", "error"};
        for (const char* key : kReasonKeys) {
            std::string why = data.member_string(key);
            if (!why.empty()) {
                *reason = std::move(why);
                break;
            }
        }
    }
    return ok ? 0 : -1;
}

long long BigQmtXtTrader::cancel_order_stock(const StockAccount& account,
                                             const std::string& order_id) {
    std::string reason;
    long long rc = cancel_rpc(account, order_id, &reason);
    if (rc != 0 && !reason.empty()) {
        // 契约返回值只有 0/-1 (与 python 一致), 被拒原因打到 stderr 便于
        // 定位 (常见: 非交易时段 / 委托已不可撤 / 权限等)。
        std::fprintf(stderr, "[bigqmt] cancel_order_stock 被拒 order_sysid=%s: %s\n",
                     order_id.c_str(), reason.c_str());
    }
    return rc;
}

long long BigQmtXtTrader::cancel_order_stock_async(const StockAccount& account,
                                                   const std::string& order_id) {
    // 镜像 xtquant_compat.cancel_order_stock_async: seq 分配后同步执行撤单
    // RPC (python compat 的"异步"就是同步撤单 + 返回前回调), 结果经
    // on_cancel_order_stock_async_response 回报 (受理与否, 非最终撤成)。
    long long seq = ++async_seq_;
    long long rc = -1;
    std::string reason;
    try {
        rc = cancel_rpc(account, order_id, &reason);
    } catch (const std::exception& e) {
        // RPC/协议异常不抛给调用方: 同 python 一样改走 on_cancel_error
        // (error_id = getattr(exc, "errno", 0) == 0, 本实现异常无 errno)。
        std::shared_ptr<XtQuantTraderCallback> cb;
        {
            std::lock_guard<std::mutex> lock(callback_mutex_);
            cb = callback_;
        }
        if (cb) {
            XtCancelError error;
            error.error_id = 0;
            error.error_msg = e.what();
            error.order_sysid = order_id;
            error.order_sys_id = order_id;
            error.order_id = OrderId(order_id);
            error.stock_code = "";
            try {
                cb->on_cancel_error(error);
            } catch (const std::exception& ue) {
                std::fprintf(stderr, "[bigqmt] user callback failed: on_cancel_error: %s\n",
                             ue.what());
            }
        }
        return seq;
    }
    const bool ok = rc == 0;
    // 桥带了拒绝原因就用真实原因 (python compat 只有固定文案, 这里增强:
    // cancel_result/-1 + 真实原因能直接定位问题)。
    const std::string reject_msg =
        reason.empty() ? "cancel_order_stock rejected by server" : reason;
    std::shared_ptr<XtQuantTraderCallback> cb;
    {
        std::lock_guard<std::mutex> lock(callback_mutex_);
        cb = callback_;
    }
    if (cb) {
        XtCancelOrderStockResponse response;
        response.account_id = config_.account_id;
        response.seq = seq;
        response.success = ok;
        response.cancel_result = ok ? 0 : -1;
        response.error_msg = ok ? "" : reject_msg;
        response.order_sysid = order_id;
        response.order_sys_id = order_id;
        response.order_id = OrderId(order_id);
        try {
            cb->on_cancel_order_stock_async_response(response);
        } catch (const std::exception& e) {
            std::fprintf(stderr,
                         "[bigqmt] user callback failed: "
                         "on_cancel_order_stock_async_response: %s\n",
                         e.what());
        }
    }
    return seq;
}

}  // namespace bigqmt
