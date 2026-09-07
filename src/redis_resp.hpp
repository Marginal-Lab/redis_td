// Minimal RESP2 Redis client over POSIX sockets. No third-party libraries.
//
// Supports exactly what the Big QMT bridge needs:
//   command()  - send one command (array of bulk strings)
//   read()     - read one reply, with a timeout (returns false on timeout)
//   subscribe mode pushes ("message") parse as ordinary arrays, so the same
//   reader is used by both the pub/sub listener and blocking calls.
#pragma once

#include <stdexcept>
#include <string>
#include <vector>

namespace bigqmt {

class RedisError : public std::runtime_error {
public:
    explicit RedisError(const std::string& msg) : std::runtime_error(msg) {}
};

// One decoded RESP value.
struct Resp {
    enum class Kind : uint8_t { Nil, Str, Err, Int, Arr };
    Kind kind = Kind::Nil;
    std::string text;      // Str / Err payload (binary safe)
    long long num = 0;     // Int payload
    std::vector<Resp> items;  // Arr payload

    bool is_err() const { return kind == Kind::Err; }
    const std::string& error_text() const { return text; }
};

class RedisConnection {
public:
    struct Options {
        std::string host = "127.0.0.1";
        int port = 6379;
        std::string username;   // empty -> plain AUTH password
        std::string password;
        int db = 0;
        // Read-side timeout in milliseconds. read() reports a timeout by
        // returning false instead of throwing.
        int read_timeout_ms = 1000;
        double connect_timeout_seconds = 5.0;
    };

    RedisConnection() = default;
    ~RedisConnection();
    RedisConnection(const RedisConnection&) = delete;
    RedisConnection& operator=(const RedisConnection&) = delete;

    // Connects, AUTHs (when a password is set) and SELECTs the database.
    // Throws RedisError on failure.
    void connect(const Options& opts);

    // Stores the endpoint/auth options without connecting. A later
    // command()/read() (or a reconnect after a failure) uses these options.
    // This is how the shared RPC connection learns its real endpoint: it is
    // used lazily, so connect(opts) would be redundant work on every call.
    void configure(const Options& opts) { opts_ = opts; }

    bool connected() const { return fd_ >= 0; }

    // True while a reply frame is half-delivered (header or payload bytes
    // buffered but not yet a full frame). Sending a command then would pair it
    // with the wrong reply; readers use this to skip keepalive pings instead
    // of poisoning the stream.
    bool mid_frame() const { return inpos_ > 0 || inpos_ < inbuf_.size(); }

    void close();

    // Sends one RESP command; caller must then call read().
    void command(const std::vector<std::string>& argv);

    // Reads exactly one reply. Returns false on read timeout (nothing was
    // delivered in read_timeout_ms); throws RedisError on a broken socket.
    bool read(Resp& out);

private:
    int fd_ = -1;
    std::string inbuf_;   // bytes read from the socket but not yet parsed
    size_t inpos_ = 0;    // parse cursor inside inbuf_
    Options opts_;
    // True when read() timed out with a frame half-delivered (or otherwise
    // left a reply orphaned in the buffer). The byte stream can no longer be
    // paired with commands, so command() drops the socket and reconnects
    // before sending anything.
    bool unsafe_ = false;

    void ensure_socket();
    bool fill_buffer(int timeout_ms);  // false = timeout
    bool parse_one(Resp& out);         // false = need more bytes
    // Parses one RESP value from inbuf_ starting at *pos. On success *pos
    // advances past the value. Returns false when more bytes are needed, with
    // *pos rolled back to the value's start so a later retry re-parses the
    // frame from its type byte (never resumes inside the payload).
    bool parse_value(size_t& pos, Resp& out);
    // Closes the socket, then throws RedisError. Never returns.
    [[noreturn]] void parse_error(const std::string& why);
};

}  // namespace bigqmt
