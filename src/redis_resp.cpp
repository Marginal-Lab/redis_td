#include "redis_resp.hpp"

#include <arpa/inet.h>
#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include <chrono>
#include <csignal>
#include <cstdio>
#include <string>

namespace bigqmt {

namespace {

constexpr size_t kMaxReplyBytes = 256u * 1024 * 1024;  // sanity ceiling

// Sending to a peer that already closed its end raises SIGPIPE, which by
// default kills the whole process. Network code handles EPIPE itself (see
// command()), so the signal must never fire.
struct SigpipeIgnorer {
    SigpipeIgnorer() { std::signal(SIGPIPE, SIG_IGN); }
};
SigpipeIgnorer kIgnoreSigpipe;

double now_seconds() {
    using namespace std::chrono;
    return duration_cast<duration<double>>(steady_clock::now().time_since_epoch()).count();
}

}  // namespace

RedisConnection::~RedisConnection() { close(); }

void RedisConnection::connect(const Options& opts) {
    // Re-entrant: drops any existing socket, then establishes a fresh one
    // (AUTH + SELECT happen inside ensure_socket once the TCP link is up).
    close();
    configure(opts);
    ensure_socket();
}

void RedisConnection::close() {
    if (fd_ >= 0) {
        ::close(fd_);
        fd_ = -1;
    }
    inbuf_.clear();
    inpos_ = 0;
    unsafe_ = false;  // orphaned bytes are gone; a new socket starts clean
}

void RedisConnection::parse_error(const std::string& why) {
    // A protocol violation means the reply stream can no longer be trusted.
    // Drop the socket: the next command()/read() transparently reconnects.
    close();
    throw RedisError("redis protocol error: " + why);
}

void RedisConnection::ensure_socket() {
    if (fd_ >= 0) return;

    struct addrinfo hints;
    std::memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;

    struct addrinfo* list = nullptr;
    std::string port = std::to_string(opts_.port);
    int rc = ::getaddrinfo(opts_.host.c_str(), port.c_str(), &hints, &list);
    if (rc != 0) {
        throw RedisError("redis resolve failed: " + std::string(::gai_strerror(rc)));
    }

    double deadline = now_seconds() + opts_.connect_timeout_seconds;
    int last_errno = ECONNREFUSED;
    for (struct addrinfo* ai = list; ai; ai = ai->ai_next) {
        int s = ::socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
        if (s < 0) continue;
        // Non-blocking connect with a deadline, so an unreachable host fails
        // fast instead of hanging on the kernel default.
        int flags = ::fcntl(s, F_GETFL, 0);
        ::fcntl(s, F_SETFL, flags | O_NONBLOCK);
        int ret = ::connect(s, ai->ai_addr, ai->ai_addrlen);
        if (ret != 0 && errno == EINPROGRESS) {
            double remain = deadline - now_seconds();
            if (remain <= 0) {
                ::close(s);
                last_errno = ETIMEDOUT;
                break;
            }
            struct pollfd pfd;
            pfd.fd = s;
            pfd.events = POLLOUT;
            int pr = ::poll(&pfd, 1, static_cast<int>(remain * 1000.0));
            if (pr <= 0) {
                ::close(s);
                last_errno = pr == 0 ? ETIMEDOUT : errno;
                continue;
            }
            int soerr = 0;
            socklen_t len = sizeof(soerr);
            ::getsockopt(s, SOL_SOCKET, SO_ERROR, &soerr, &len);
            if (soerr != 0) {
                ::close(s);
                last_errno = soerr;
                continue;
            }
            ret = 0;
        }
        if (ret != 0) {
            last_errno = errno;
            ::close(s);
            continue;
        }
        ::fcntl(s, F_SETFL, flags);  // back to blocking; read() drives timeouts
        fd_ = s;
        break;
    }
    ::freeaddrinfo(list);
    if (fd_ < 0) {
        throw RedisError("redis connect " + opts_.host + ":" + port +
                         " failed: " + std::string(::strerror(last_errno)));
    }

    // AUTH: "AUTH <password>", or "AUTH <username> <password>" when set.
    if (!opts_.password.empty()) {
        std::vector<std::string> argv = {"AUTH"};
        if (!opts_.username.empty()) argv.push_back(opts_.username);
        argv.push_back(opts_.password);
        command(argv);
        Resp r;
        if (!read(r)) parse_error("no reply to AUTH");
        if (r.is_err()) {
            close();
            throw RedisError("redis auth failed: " + r.error_text());
        }
    }
    if (opts_.db != 0) {
        command({"SELECT", std::to_string(opts_.db)});
        Resp r;
        if (!read(r)) parse_error("no reply to SELECT");
        if (r.is_err()) {
            close();
            throw RedisError("redis select db failed: " + r.error_text());
        }
    }
}

bool RedisConnection::fill_buffer(int timeout_ms) {
    struct pollfd pfd;
    pfd.fd = fd_;
    pfd.events = POLLIN;
    int rc = ::poll(&pfd, 1, timeout_ms);
    if (rc == 0) return false;  // timeout, nothing to read
    if (rc < 0) {
        throw RedisError(std::string("redis poll failed: ") + ::strerror(errno));
    }
    if (pfd.revents & (POLLERR | POLLHUP | POLLNVAL)) {
        throw RedisError("redis connection closed by peer");
    }
    if (!(pfd.revents & POLLIN)) return false;

    char buf[65536];
    ssize_t n = ::recv(fd_, buf, sizeof(buf), 0);
    if (n <= 0) {
        throw RedisError(n == 0 ? "redis connection closed by peer" : "redis recv failed");
    }
    inbuf_.append(buf, static_cast<size_t>(n));
    return true;
}

// One full CRLF line from the buffer; false when incomplete.
static bool take_line(const std::string& buf, size_t& pos, std::string& line) {
    size_t eol = buf.find("\r\n", pos);
    if (eol == std::string::npos) return false;
    line.assign(buf, pos, eol - pos);
    pos = eol + 2;
    return true;
}

bool RedisConnection::parse_value(size_t& pos, Resp& out) {
    // Parse starting at *pos without committing: on "need more bytes" the
    // cursor is rolled back to the value's start. A partial frame therefore
    // never leaves the cursor inside its payload, where a later parse attempt
    // would read a payload byte (e.g. '{') as a reply type byte -- the
    // "unknown reply type byte '{'" corruption seen on replies >64KB that
    // arrive across several recv chunks.
    if (pos >= inbuf_.size()) return false;
    size_t start = pos;

    auto want = [&](size_t n) -> bool { return inbuf_.size() - pos >= n; };
    auto readline = [&](std::string& line) -> bool { return take_line(inbuf_, pos, line); };
    auto need_more = [&]() -> bool {
        pos = start;
        return false;
    };

    char type = inbuf_[pos];
    ++pos;
    switch (type) {
        case '+':
        case '-': {
            std::string line;
            if (!readline(line)) return need_more();
            out.kind = type == '+' ? Resp::Kind::Str : Resp::Kind::Err;
            out.text = std::move(line);
            return true;
        }
        case ':': {
            std::string line;
            if (!readline(line)) return need_more();
            out.kind = Resp::Kind::Int;
            out.num = std::strtoll(line.c_str(), nullptr, 10);
            return true;
        }
        case '$': {
            std::string line;
            if (!readline(line)) return need_more();
            long long len = std::strtoll(line.c_str(), nullptr, 10);
            if (len < 0) {
                out.kind = Resp::Kind::Nil;
                return true;
            }
            if (static_cast<unsigned long long>(len) > kMaxReplyBytes) {
                parse_error("bulk reply too large");
            }
            size_t n = static_cast<size_t>(len);
            if (!want(n + 2)) return need_more();
            out.kind = Resp::Kind::Str;
            out.text.assign(inbuf_, pos, n);
            pos += n + 2;  // payload + CRLF
            return true;
        }
        case '*': {
            std::string line;
            if (!readline(line)) return need_more();
            long long count = std::strtoll(line.c_str(), nullptr, 10);
            if (count < 0) {
                out.kind = Resp::Kind::Nil;
                return true;
            }
            if (count > 1 << 20) parse_error("array reply too large");
            out.kind = Resp::Kind::Arr;
            out.items.resize(static_cast<size_t>(count));
            for (size_t k = 0; k < static_cast<size_t>(count); ++k) {
                if (!parse_value(pos, out.items[k])) return need_more();
            }
            return true;
        }
        default:
            parse_error(std::string("unknown reply type byte '") + type + "'");
    }
}

bool RedisConnection::parse_one(Resp& out) {
    // Parse into a local cursor and commit (advance inpos_) only when the
    // whole frame is present; a partial frame never advances inpos_.
    size_t pos = inpos_;
    if (!parse_value(pos, out)) return false;
    inpos_ = pos;
    return true;
}

void RedisConnection::command(const std::vector<std::string>& argv) {
    if (unsafe_) {
        // A previous read() timed out with a frame half-delivered (or left a
        // reply orphaned in the buffer). The remaining bytes belong to a reply
        // we abandoned; sending another command would pair it with the wrong
        // frame. Drop the socket so every command still gets exactly one reply.
        close();
        unsafe_ = false;
    }
    ensure_socket();
    std::string out;
    out.reserve(64);
    out += '*';
    out += std::to_string(argv.size());
    out += "\r\n";
    for (const auto& arg : argv) {
        out += '$';
        out += std::to_string(arg.size());
        out += "\r\n";
        out += arg;
        out += "\r\n";
    }
    size_t sent = 0;
    while (sent < out.size()) {
        ssize_t n = ::send(fd_, out.data() + sent, out.size() - sent, 0);
        if (n <= 0) {
            close();
            throw RedisError(n == 0 ? "redis connection closed while sending"
                                    : "redis send failed");
        }
        sent += static_cast<size_t>(n);
    }
}

bool RedisConnection::read(Resp& out) {
    ensure_socket();
    int timeout_ms = opts_.read_timeout_ms;
    int elapsed = 0;
    for (;;) {
        if (parse_one(out)) {
            unsafe_ = false;  // one clean reply consumed -> stream is paired again
            return true;
        }
        // Bytes already buffered (type byte / header / partial payload consumed)
        // mean a reply is half-delivered. Timing out now would orphan it, and
        // the next command on this socket would resume parsing inside that
        // payload — exactly the "unknown reply type byte '{'" corruption. Flag
        // the socket so command() reconnects before sending anything.
        bool partial = inpos_ > 0 || inpos_ < inbuf_.size();
        int slice = timeout_ms > elapsed ? timeout_ms - elapsed : 1;
        bool got = fill_buffer(slice);
        elapsed += slice;
        if (!got) {
            if (elapsed >= timeout_ms) {
                if (partial) unsafe_ = true;
                return false;
            }
            continue;
        }
    }
}

}  // namespace bigqmt
