// Minimal JSON value type: parse + dump. No third-party dependencies.
//
// Wire-compatible with what the Big QMT bridge exchanges (Python json module):
//  - parse accepts NaN / Infinity / -Infinity (Python json.dumps emits those
//    by default when a float is not a number);
//  - dump is compact and keeps non-ASCII text as raw UTF-8 bytes
//    (the Python side uses json.dumps(..., ensure_ascii=False)).
#pragma once

#include <cstdint>
#include <cstdio>
#include <cmath>
#include <cerrno>
#include <cstring>
#include <string>
#include <utility>
#include <vector>
#include <stdexcept>

namespace bigqmt {

class JsonError : public std::runtime_error {
public:
    explicit JsonError(const std::string& msg) : std::runtime_error(msg) {}
};

class Json {
public:
    enum class Type : uint8_t { Null, Bool, Int, Double, Str, Arr, Obj };

    Json() : type_(Type::Null) {}
    static Json null() { return Json(); }

    Type type() const { return type_; }
    bool is_null() const { return type_ == Type::Null; }
    bool is_object() const { return type_ == Type::Obj; }
    bool is_array() const { return type_ == Type::Arr; }
    bool is_string() const { return type_ == Type::Str; }
    bool is_number() const { return type_ == Type::Int || type_ == Type::Double; }
    bool is_bool() const { return type_ == Type::Bool; }

    // ---- accessors (python-ish coercions, mirror the compat layer helpers) --

    bool as_bool(bool def = false) const {
        switch (type_) {
            case Type::Bool: return b_;
            case Type::Int: return i_ != 0;
            case Type::Double: return d_ != 0.0;
            case Type::Str: return !s_.empty();
            default: return def;
        }
    }

    // Mirrors python int(value): int stays, float truncates toward zero,
    // numeric strings parse, anything else -> default.
    int64_t as_int64(int64_t def = 0) const {
        switch (type_) {
            case Type::Int: return i_;
            case Type::Bool: return b_ ? 1 : 0;
            case Type::Double: return static_cast<int64_t>(d_);
            case Type::Str: {
                char* end = nullptr;
                errno = 0;
                long long v = std::strtoll(s_.c_str(), &end, 10);
                if (end == s_.c_str() || *end != '\0' || errno == ERANGE) {
                    // python int("1.5") raises -> default
                    return def;
                }
                return static_cast<int64_t>(v);
            }
            default: return def;
        }
    }

    // Mirrors python float(value): int -> double, numeric string parsed,
    // anything else -> default.
    double as_double(double def = 0.0) const {
        switch (type_) {
            case Type::Int: return static_cast<double>(i_);
            case Type::Double: return d_;
            case Type::Bool: return b_ ? 1.0 : 0.0;
            case Type::Str: {
                char* end = nullptr;
                double v = std::strtod(s_.c_str(), &end);
                if (end == s_.c_str() || *end != '\0') {
                    return def;
                }
                return v;
            }
            default: return def;
        }
    }

    const std::string& as_string(const std::string& def = "") const {
        return type_ == Type::Str ? s_ : def;
    }

    // Object member lookup; nullptr when absent or not an object.
    const Json* get(const std::string& key) const {
        if (type_ != Type::Obj) return nullptr;
        for (const auto& kv : obj_) {
            if (kv.first == key) return &kv.second;
        }
        return nullptr;
    }

    const std::vector<std::pair<std::string, Json>>& object_items() const {
        return obj_;
    }

    // Convenience: value of member or a default when missing / null.
    const Json* member(const char* key) const { return get(key); }

    int64_t member_int(const char* key, int64_t def = 0) const {
        const Json* v = get(key);
        return v ? v->as_int64(def) : def;
    }

    double member_double(const char* key, double def = 0.0) const {
        const Json* v = get(key);
        return v ? v->as_double(def) : def;
    }

    std::string member_string(const char* key, const std::string& def = "") const {
        const Json* v = get(key);
        return v ? v->as_string(def) : def;
    }

    bool member_bool(const char* key, bool def = false) const {
        const Json* v = get(key);
        return v ? v->as_bool(def) : def;
    }

    const std::vector<Json>& array_items() const { return arr_; }
    size_t size() const {
        if (type_ == Type::Arr) return arr_.size();
        if (type_ == Type::Obj) return obj_.size();
        return 0;
    }
    bool empty() const { return size() == 0; }

    // ---- builders -----------------------------------------------------------

    static Json make_bool(bool b) { Json j; j.type_ = Type::Bool; j.b_ = b; return j; }
    static Json make_int(int64_t v) { Json j; j.type_ = Type::Int; j.i_ = v; return j; }
    static Json make_double(double v) { Json j; j.type_ = Type::Double; j.d_ = v; return j; }
    static Json make_string(std::string s) { Json j; j.type_ = Type::Str; j.s_ = std::move(s); return j; }
    static Json make_object() { Json j; j.type_ = Type::Obj; return j; }
    static Json make_array() { Json j; j.type_ = Type::Arr; return j; }

    // Insert-or-replace a member, keeping first-insert position (dict order).
    void set(const std::string& key, Json value) {
        if (type_ != Type::Obj) {
            *this = make_object();
        }
        for (auto& kv : obj_) {
            if (kv.first == key) {
                kv.second = std::move(value);
                return;
            }
        }
        obj_.emplace_back(key, std::move(value));
    }

    void push(Json value) {
        if (type_ != Type::Arr) {
            *this = make_array();
        }
        arr_.push_back(std::move(value));
    }

    // ---- parse / dump -------------------------------------------------------

    static Json parse(const std::string& text);
    std::string dump() const;

private:
    Type type_ = Type::Null;
    bool b_ = false;
    int64_t i_ = 0;
    double d_ = 0.0;
    std::string s_;
    std::vector<std::pair<std::string, Json>> obj_;
    std::vector<Json> arr_;
};

// Shortest-roundtrip double text, python-repr style ("2.95", "100.0", "1e+20",
// "nan", "inf", "-inf"). Used for both dumping and console printing.
std::string py_double_text(double v);

// Parses whole text; throws JsonError on malformed input.
inline Json Json::parse(const std::string& text) {
    class P {
    public:
        explicit P(const std::string& t) : t_(t) {}
        Json value() {
            skip_space();
            Json v = parse_value();
            skip_space();
            if (pos_ != t_.size()) fail("trailing characters");
            return v;
        }

    private:
        const std::string& t_;
        size_t pos_ = 0;

        [[noreturn]] void fail(const std::string& why) {
            throw JsonError("json parse error at byte " + std::to_string(pos_) + ": " + why);
        }

        void skip_space() {
            while (pos_ < t_.size()) {
                char c = t_[pos_];
                if (c == ' ' || c == '\t' || c == '\n' || c == '\r') {
                    ++pos_;
                } else {
                    break;
                }
            }
        }

        Json parse_value() {
            if (pos_ >= t_.size()) fail("unexpected end of input");
            char c = t_[pos_];
            switch (c) {
                case '{': return parse_object();
                case '[': return parse_array();
                case '"': return Json::make_string(parse_string());
                case 't': expect_literal("true"); return Json::make_bool(true);
                case 'f': expect_literal("false"); return Json::make_bool(false);
                case 'n': expect_literal("null"); return Json();
                case 'N': expect_literal("NaN"); return Json::make_double(NAN);
                case 'I': expect_literal("Infinity"); return Json::make_double(INFINITY);
                default:
                    if (c == '-' || c == '+' || (c >= '0' && c <= '9') || c == '.') {
                        return parse_number();
                    }
                    fail(std::string("unexpected character '") + c + "'");
            }
        }

        void expect_literal(const char* lit) {
            size_t n = std::strlen(lit);
            if (t_.compare(pos_, n, lit) != 0) fail(std::string("expected ") + lit);
            pos_ += n;
        }

        // Accepts '-Infinity' too (a leading '-' with a following 'I').
        Json parse_number() {
            size_t start = pos_;
            bool neg = false;
            if (pos_ < t_.size() && t_[pos_] == '-') {
                neg = true;
                ++pos_;
            }
            if (t_.compare(pos_, 8, "Infinity") == 0) {
                pos_ += 8;
                return Json::make_double(neg ? -INFINITY : INFINITY);
            }
            bool is_int = true;
            while (pos_ < t_.size()) {
                char c = t_[pos_];
                if (c >= '0' && c <= '9') {
                    ++pos_;
                } else if (c == '.' || c == 'e' || c == 'E' || c == '+' || c == '-') {
                    is_int = false;
                    ++pos_;
                } else {
                    break;
                }
            }
            std::string tok = t_.substr(start, pos_ - start);
            if (is_int && tok.find_first_not_of("-0123456789") == std::string::npos) {
                char* end = nullptr;
                errno = 0;
                long long v = std::strtoll(tok.c_str(), &end, 10);
                if (errno != ERANGE && end == tok.c_str() + tok.size()) {
                    return Json::make_int(v);
                }
                // Overflow: fall through to double (python big-int round trip is
                // out of scope for this client; the bridge never sends them).
            }
            char* end = nullptr;
            double v = std::strtod(tok.c_str(), &end);
            if (end != tok.c_str() + tok.size()) fail("bad number");
            return Json::make_double(v);
        }

        Json parse_object() {
            Json obj = Json::make_object();
            ++pos_;  // '{'
            skip_space();
            if (pos_ < t_.size() && t_[pos_] == '}') {
                ++pos_;
                return obj;
            }
            for (;;) {
                skip_space();
                if (pos_ >= t_.size() || t_[pos_] != '"') fail("expected string key");
                std::string key = parse_string();
                skip_space();
                if (pos_ >= t_.size() || t_[pos_] != ':') fail("expected ':'");
                ++pos_;
                skip_space();
                obj.set(key, parse_value());
                skip_space();
                if (pos_ >= t_.size()) fail("unterminated object");
                char c = t_[pos_++];
                if (c == ',') continue;
                if (c == '}') return obj;
                fail("expected ',' or '}'");
            }
        }

        Json parse_array() {
            Json arr = Json::make_array();
            ++pos_;  // '['
            skip_space();
            if (pos_ < t_.size() && t_[pos_] == ']') {
                ++pos_;
                return arr;
            }
            for (;;) {
                skip_space();
                arr.push(parse_value());
                skip_space();
                if (pos_ >= t_.size()) fail("unterminated array");
                char c = t_[pos_++];
                if (c == ',') continue;
                if (c == ']') return arr;
                fail("expected ',' or ']'");
            }
        }

        void parse_hex4(uint32_t& out) {
            if (pos_ + 4 > t_.size()) fail("short \\u escape");
            uint32_t v = 0;
            for (int k = 0; k < 4; ++k) {
                char c = t_[pos_++];
                v <<= 4;
                if (c >= '0' && c <= '9') v |= static_cast<uint32_t>(c - '0');
                else if (c >= 'a' && c <= 'f') v |= static_cast<uint32_t>(c - 'a' + 10);
                else if (c >= 'A' && c <= 'F') v |= static_cast<uint32_t>(c - 'A' + 10);
                else fail("bad \\u escape");
            }
            out = v;
        }

        void append_utf8(std::string& out, uint32_t cp) {
            if (cp < 0x80) {
                out += static_cast<char>(cp);
            } else if (cp < 0x800) {
                out += static_cast<char>(0xC0 | (cp >> 6));
                out += static_cast<char>(0x80 | (cp & 0x3F));
            } else if (cp < 0x10000) {
                out += static_cast<char>(0xE0 | (cp >> 12));
                out += static_cast<char>(0x80 | ((cp >> 6) & 0x3F));
                out += static_cast<char>(0x80 | (cp & 0x3F));
            } else {
                out += static_cast<char>(0xF0 | (cp >> 18));
                out += static_cast<char>(0x80 | ((cp >> 12) & 0x3F));
                out += static_cast<char>(0x80 | ((cp >> 6) & 0x3F));
                out += static_cast<char>(0x80 | (cp & 0x3F));
            }
        }

        std::string parse_string() {
            ++pos_;  // '"'
            std::string out;
            for (;;) {
                if (pos_ >= t_.size()) fail("unterminated string");
                unsigned char c = static_cast<unsigned char>(t_[pos_++]);
                if (c == '"') return out;
                if (c == '\\') {
                    if (pos_ >= t_.size()) fail("unterminated escape");
                    char e = t_[pos_++];
                    switch (e) {
                        case '"': out += '"'; break;
                        case '\\': out += '\\'; break;
                        case '/': out += '/'; break;
                        case 'b': out += '\b'; break;
                        case 'f': out += '\f'; break;
                        case 'n': out += '\n'; break;
                        case 'r': out += '\r'; break;
                        case 't': out += '\t'; break;
                        case 'u': {
                            uint32_t cp = 0;
                            parse_hex4(cp);
                            if (cp >= 0xD800 && cp <= 0xDBFF && pos_ + 1 < t_.size() &&
                                t_[pos_] == '\\' && t_[pos_ + 1] == 'u') {
                                pos_ += 2;
                                uint32_t lo = 0;
                                parse_hex4(lo);
                                if (lo >= 0xDC00 && lo <= 0xDFFF) {
                                    cp = 0x10000 + ((cp - 0xD800) << 10) + (lo - 0xDC00);
                                } else {
                                    append_utf8(out, cp);  // lone surrogate: keep as-is
                                    cp = lo;
                                }
                            }
                            append_utf8(out, cp);
                            break;
                        }
                        default: fail("bad escape");
                    }
                    continue;
                }
                if (c < 0x20) fail("raw control character in string");
                out += static_cast<char>(c);
            }
        }
    };
    P parser(text);
    return parser.value();
}

inline void dump_escaped_string(std::string& out, const std::string& s) {
    out += '"';
    static const char* HEX = "0123456789abcdef";
    for (unsigned char c : s) {
        switch (c) {
            case '"': out += "\\\""; break;
            case '\\': out += "\\\\"; break;
            case '\b': out += "\\b"; break;
            case '\f': out += "\\f"; break;
            case '\n': out += "\\n"; break;
            case '\r': out += "\\r"; break;
            case '\t': out += "\\t"; break;
            default:
                if (c < 0x20) {
                    // python json.dumps(..., ensure_ascii=False) escapes control
                    // characters as \u00XX.
                    out += "\\u00";
                    out += HEX[(c >> 4) & 0xF];
                    out += HEX[c & 0xF];
                } else {
                    out += static_cast<char>(c);  // UTF-8 bytes pass through
                }
        }
    }
    out += '"';
}

inline std::string Json::dump() const {
    std::string out;
    switch (type_) {
        case Type::Null: out += "null"; break;
        case Type::Bool: out += b_ ? "true" : "false"; break;
        case Type::Int: out += std::to_string(i_); break;
        case Type::Double: out += py_double_text(d_); break;
        case Type::Str: dump_escaped_string(out, s_); break;
        case Type::Arr: {
            out += '[';
            bool first = true;
            for (const auto& v : arr_) {
                if (!first) out += ',';
                first = false;
                out += v.dump();
            }
            out += ']';
            break;
        }
        case Type::Obj: {
            out += '{';
            bool first = true;
            for (const auto& kv : obj_) {
                if (!first) out += ',';
                first = false;
                dump_escaped_string(out, kv.first);
                out += ':';
                out += kv.second.dump();
            }
            out += '}';
            break;
        }
    }
    return out;
}

inline std::string py_double_text(double v) {
    if (std::isnan(v)) return "nan";
    if (std::isinf(v)) return v > 0 ? "inf" : "-inf";
    // Integral doubles print with a trailing .0, like python repr(100.0).
    if (v == std::floor(v) && std::fabs(v) < 1e15) {
        char buf[40];
        std::snprintf(buf, sizeof(buf), "%.0f", v);
        return std::string(buf) + ".0";
    }
    // Shortest representation that round-trips (python uses repr internally,
    // json.dumps emits exactly that).
    char buf[64];
    for (int prec = 1; prec <= 17; ++prec) {
        std::snprintf(buf, sizeof(buf), "%.*g", prec, v);
        if (std::strtod(buf, nullptr) == v) break;
    }
    return std::string(buf);
}

}  // namespace bigqmt
