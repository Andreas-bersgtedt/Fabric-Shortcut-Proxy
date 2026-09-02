// -----------------------------------------------------------------------------
// C++ serving Agent - Phase 6
//
// A stateless Agent that serves the S3 data plane from a shared artifact store.
// It performs no SQL, Parquet generation, or Iceberg/Delta materialization.
// -----------------------------------------------------------------------------

#ifdef _WIN32
#define _CRT_SECURE_NO_WARNINGS
#define _WINSOCK_DEPRECATED_NO_WARNINGS
#endif

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cctype>
#include <csignal>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <ctime>
#include <climits>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <mutex>
#include <queue>
#include <sstream>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "tier1/sha256.hpp"

#ifdef _WIN32
#define WIN32_LEAN_AND_MEAN
#define NOMINMAX
#include <winsock2.h>
#include <ws2tcpip.h>
#include <windows.h>
#pragma comment(lib, "ws2_32.lib")
using SocketHandle = SOCKET;
static const SocketHandle kInvalidSocket = INVALID_SOCKET;
#else
#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
using SocketHandle = int;
static const SocketHandle kInvalidSocket = -1;
#endif

namespace fs = std::filesystem;

static const char* APP_VERSION = "cpp-0.3.0";

// ---------------------------------------------------------------------------
// platform shim
// ---------------------------------------------------------------------------

static bool net_init() {
#ifdef _WIN32
    WSADATA wsa;
    return WSAStartup(MAKEWORD(2, 2), &wsa) == 0;
#else
    return true;
#endif
}

static void net_cleanup() {
#ifdef _WIN32
    WSACleanup();
#endif
}

static void close_socket(SocketHandle s) {
#ifdef _WIN32
    closesocket(s);
#else
    close(s);
#endif
}

static bool set_reuseaddr(SocketHandle s) {
    int yes = 1;
    return setsockopt(s, SOL_SOCKET, SO_REUSEADDR, (const char*)&yes, sizeof(yes)) == 0;
}

static void set_socket_timeouts(SocketHandle s, int ms) {
#ifdef _WIN32
    DWORD t = (DWORD)ms;
    setsockopt(s, SOL_SOCKET, SO_RCVTIMEO, (const char*)&t, sizeof(t));
    setsockopt(s, SOL_SOCKET, SO_SNDTIMEO, (const char*)&t, sizeof(t));
#else
    struct timeval tv;
    tv.tv_sec = ms / 1000;
    tv.tv_usec = (ms % 1000) * 1000;
    setsockopt(s, SOL_SOCKET, SO_RCVTIMEO, (const char*)&tv, sizeof(tv));
    setsockopt(s, SOL_SOCKET, SO_SNDTIMEO, (const char*)&tv, sizeof(tv));
#endif
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

static std::string getenv_str(const char* name, const std::string& def) {
    const char* v = std::getenv(name);
    return (v && *v) ? std::string(v) : def;
}

static bool getenv_bool(const char* name, bool def) {
    std::string value = getenv_str(name, def ? "1" : "0");
    std::transform(value.begin(), value.end(), value.begin(),
                   [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    return value == "1" || value == "true" || value == "yes" || value == "on";
}

static bool parse_i64(const std::string& s, long long& out) {
    if (s.empty()) return false;

    size_t i = 0;
    bool neg = false;
    if (s[0] == '+' || s[0] == '-') {
        neg = (s[0] == '-');
        i = 1;
        if (i == s.size()) return false;
    }

    unsigned long long acc = 0ULL;
    unsigned long long limit = neg ? (unsigned long long)LLONG_MAX + 1ULL : (unsigned long long)LLONG_MAX;
    for (; i < s.size(); ++i) {
        char c = s[i];
        if (c < '0' || c > '9') return false;
        int d = c - '0';
        if (acc > limit / 10ULL || (acc == limit / 10ULL && (unsigned long long)d > limit % 10ULL)) {
            return false;
        }
        acc = acc * 10ULL + (unsigned long long)d;
    }

    if (neg) {
        if (acc == (unsigned long long)LLONG_MAX + 1ULL) {
            out = LLONG_MIN;
            return true;
        }
        out = -static_cast<long long>(acc);
        return true;
    }

    if (acc > (unsigned long long)LLONG_MAX) return false;
    out = static_cast<long long>(acc);
    return true;
}

static int parse_int_or(const std::string& s, int def, int min_v, int max_v) {
    long long v = 0;
    if (!parse_i64(s, v)) return def;
    if (v < min_v || v > max_v) return def;
    return (int)v;
}

static void log_line(const std::string& msg) {
    std::time_t t = std::time(nullptr);
    char buf[32];
    std::strftime(buf, sizeof(buf), "%H:%M:%S", std::gmtime(&t));
    std::fprintf(stderr, "%s [cpp-agent] %s\n", buf, msg.c_str());
    std::fflush(stderr);
}

static std::string to_lower_ascii(const std::string& s) {
    std::string out = s;
    std::transform(out.begin(), out.end(), out.begin(), [](unsigned char c) {
        return (char)std::tolower(c);
    });
    return out;
}

static std::string url_decode(const std::string& s) {
    std::string out;
    out.reserve(s.size());
    for (size_t i = 0; i < s.size(); ++i) {
        if (s[i] == '%' && i + 2 < s.size()) {
            auto hex = [](char c) -> int {
                if (c >= '0' && c <= '9') return c - '0';
                if (c >= 'a' && c <= 'f') return c - 'a' + 10;
                if (c >= 'A' && c <= 'F') return c - 'A' + 10;
                return -1;
            };
            int hi = hex(s[i + 1]), lo = hex(s[i + 2]);
            if (hi >= 0 && lo >= 0) {
                out.push_back((char)((hi << 4) | lo));
                i += 2;
                continue;
            }
        }
        if (s[i] == '+') out.push_back(' ');
        else out.push_back(s[i]);
    }
    return out;
}

static std::string iso_now() {
    std::time_t t = std::time(nullptr);
    char buf[32];
    std::strftime(buf, sizeof(buf), "%Y-%m-%dT%H:%M:%S.000Z", std::gmtime(&t));
    return std::string(buf);
}

static std::string etag_for(const std::string& key) {
    uint64_t h = 1469598103934665603ULL;
    for (unsigned char c : key) {
        h ^= c;
        h *= 1099511628211ULL;
    }
    char buf[33];
    std::snprintf(buf, sizeof(buf), "%016llx%016llx",
                  (unsigned long long)h,
                  (unsigned long long)(h * 2654435761ULL));
    return std::string(buf);
}

static std::string xml_escape(const std::string& s) {
    std::string o;
    for (char c : s) {
        switch (c) {
            case '&': o += "&amp;"; break;
            case '<': o += "&lt;"; break;
            case '>': o += "&gt;"; break;
            default: o.push_back(c);
        }
    }
    return o;
}

// ---------------------------------------------------------------------------
// config
// ---------------------------------------------------------------------------

struct Config {
    std::string host = getenv_str("HOST", "0.0.0.0");
    int port = parse_int_or(getenv_str("PORT", "9400"), 9400, 1, 65535);
    std::string store_dir = getenv_str("STORE_DIR", getenv_str("ARTIFACT_STORE_DIR", "./.artifacts"));
    std::string index_file = getenv_str("INDEX_FILE", "");
    bool require_generation = getenv_bool("REQUIRE_GENERATION", false);
    std::string bucket = getenv_str("S3_BUCKET", "fabric-iceberg-poc");
    std::string agent_id = getenv_str("AGENT_ID", "cpp-agent-1");
    std::string advertise_host = getenv_str("AGENT_ADVERTISE_HOST", "");
    std::string manager_url = getenv_str("MANAGER_URL", "");
    std::string manager_auth_username = getenv_str("MANAGER_AUTH_USERNAME", "");
    std::string manager_auth_password = getenv_str("MANAGER_AUTH_PASSWORD", "");
    int heartbeat_ms = parse_int_or(getenv_str("HEARTBEAT_MS", "2000"), 2000, 200, 600000);
    int socket_timeout_ms = parse_int_or(getenv_str("SOCKET_TIMEOUT_MS", "10000"), 10000, 1000, 60000);
    int max_inflight = parse_int_or(getenv_str("MAX_INFLIGHT", "256"), 256, 1, 100000);
    // Lazy materialization: on a store miss, ask the Manager to materialize the
    // object's table into the shared store, then serve it. Only when MATERIALIZE_MODE
    // is "lazy" and a MANAGER_URL is set. The request blocks up to this timeout while
    // the Manager generates the splits.
    std::string materialize_mode = getenv_str("MATERIALIZE_MODE", "eager");
    int materialize_timeout_ms = parse_int_or(getenv_str("MATERIALIZE_TIMEOUT_MS", "120000"), 120000, 1000, 600000);
    int index_refresh_seconds = parse_int_or(getenv_str("INDEX_REFRESH_SECONDS", "300"), 300, 0, 86400);
    // On drain: serve /readyz 503, then exit after this window so the LB can
    // deregister and in-flight requests finish.
    int drain_grace_ms = parse_int_or(getenv_str("AGENT_DRAIN_GRACE_SECONDS", "15"), 15, 0, 3600) * 1000;
};

static Config CFG;

// Set true when the Manager asks us to drain; /readyz then reports 503.
static std::atomic<bool> g_draining{false};
static std::atomic<bool> g_generation_ready{false};
static volatile std::sig_atomic_t g_termination_requested = 0;

static void request_termination(int) {
    g_termination_requested = 1;
}

// Forward decl: lazy on-miss materialization request to the Manager. Defined with
// the HTTP client further below (register/heartbeat share the same transport).
static bool try_manager_materialize(const std::string& key);

// ---------------------------------------------------------------------------
// store/path safety
// ---------------------------------------------------------------------------

static bool path_has_prefix(const fs::path& base, const fs::path& p) {
    auto bit = base.begin();
    auto pit = p.begin();
    for (; bit != base.end(); ++bit, ++pit) {
        if (pit == p.end() || *bit != *pit) return false;
    }
    return true;
}

static bool key_is_basic_safe(const std::string& key) {
    if (key.empty()) return false;
    if (key.find('\0') != std::string::npos) return false;
    if (key.size() >= 2 && std::isalpha((unsigned char)key[0]) && key[1] == ':') return false;
    if (key.rfind("//", 0) == 0 || key.rfind("\\\\", 0) == 0) return false;
    return true;
}

static fs::path canonical_store_root() {
    std::error_code ec;
    fs::path root = fs::weakly_canonical(fs::path(CFG.store_dir), ec);
    if (ec) return fs::path(CFG.store_dir).lexically_normal();
    return root;
}

static fs::path g_active_root;
static std::string g_active_generation;
static std::mutex g_object_index_mu;

static fs::path active_store_root() {
    std::lock_guard<std::mutex> lock(g_object_index_mu);
    return g_active_root.empty() ? canonical_store_root() : g_active_root;
}

static bool resolve_key_path(const std::string& raw_key, fs::path& out_path) {
    if (!key_is_basic_safe(raw_key)) return false;
    if (raw_key.front() == '/' || raw_key.front() == '\\') return false;

    std::string key = raw_key;
    std::replace(key.begin(), key.end(), '\\', '/');
    while (!key.empty() && key.front() == '/') key.erase(key.begin());
    while (!key.empty() && key.back() == '/') key.pop_back();
    if (key.empty()) return false;

    size_t pos = 0;
    while (pos <= key.size()) {
        size_t next = key.find('/', pos);
        std::string seg = (next == std::string::npos) ? key.substr(pos) : key.substr(pos, next - pos);
        if (seg.empty() || seg == "." || seg == "..") return false;
        if (next == std::string::npos) break;
        pos = next + 1;
    }

    fs::path rel(key);
    if (rel.is_absolute()) return false;

    fs::path root = active_store_root();
    std::error_code ec;
    fs::path cand = fs::weakly_canonical(root / rel, ec);
    if (ec) return false;
    if (!path_has_prefix(root, cand)) return false;
    out_path = cand;
    return true;
}

struct IndexedObject {
    std::string key;
    uintmax_t size = 0;
};

static std::vector<IndexedObject> g_object_index;
static const char* kObjectIndexFile = ".cpp-agent-index";

static fs::path object_index_path() {
    if (!CFG.index_file.empty()) return fs::path(CFG.index_file);
    return canonical_store_root() / kObjectIndexFile;
}

static bool write_object_index(const std::vector<IndexedObject>& entries) {
    fs::path path = object_index_path();
    fs::path tmp = path;
    tmp += ".tmp";
    std::ofstream out(tmp, std::ios::trunc);
    if (!out) return false;
    out << "cpp-agent-index-v1\n";
    for (const auto& entry : entries) {
        out << std::quoted(entry.key) << " " << entry.size << "\n";
    }
    out.close();
    if (!out) return false;
    std::error_code ec;
    fs::rename(tmp, path, ec);
    if (ec) {
        fs::remove(path, ec);
        ec.clear();
        fs::rename(tmp, path, ec);
    }
    return !ec;
}

static bool load_object_index(std::vector<IndexedObject>& entries) {
    std::ifstream in(object_index_path());
    std::string header;
    if (!in || !std::getline(in, header) || header != "cpp-agent-index-v1") return false;

    IndexedObject entry;
    while (in >> std::quoted(entry.key) >> entry.size) {
        if (entry.key.empty() || entry.key == kObjectIndexFile) return false;
        entries.push_back(entry);
    }
    if (!in.eof()) return false;
    return std::is_sorted(entries.begin(), entries.end(),
                          [](const auto& left, const auto& right) { return left.key < right.key; });
}

static void rebuild_object_index() {
    std::vector<IndexedObject> entries;
    std::error_code ec;
    fs::path root = canonical_store_root();
    if (fs::exists(root, ec)) {
        for (auto it = fs::recursive_directory_iterator(root, ec);
             it != fs::recursive_directory_iterator(); it.increment(ec)) {
            if (ec) break;
            if (!it->is_regular_file(ec)) continue;
            fs::path file = fs::weakly_canonical(it->path(), ec);
            if (ec || !path_has_prefix(root, file)) continue;
            std::string rel = fs::relative(file, root, ec).generic_string();
            if (ec || rel.empty() || rel == kObjectIndexFile) continue;
            entries.push_back({rel, it->file_size(ec)});
        }
    }
    std::sort(entries.begin(), entries.end(),
              [](const auto& left, const auto& right) { return left.key < right.key; });
    write_object_index(entries);
    std::lock_guard<std::mutex> lock(g_object_index_mu);
    g_object_index = std::move(entries);
}

static bool read_file_text(const fs::path& path, std::string& value) {
    std::ifstream in(path, std::ios::binary);
    if (!in) return false;
    value.assign(std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>());
    return in.good() || in.eof();
}

static bool file_sha256(const fs::path& path, std::string& digest) {
    std::ifstream in(path, std::ios::binary);
    if (!in) return false;
    fsp::Sha256 sha;
    char buffer[1024 * 1024];
    while (in) {
        in.read(buffer, sizeof(buffer));
        std::streamsize count = in.gcount();
        if (count > 0) {
            sha.update(reinterpret_cast<const uint8_t*>(buffer), static_cast<size_t>(count));
        }
    }
    if (!in.eof()) return false;
    digest = sha.hex();
    return true;
}

static bool json_string_value(const std::string& text, const std::string& name, std::string& value) {
    std::string marker = "\"" + name + "\"";
    size_t pos = text.find(marker);
    if (pos == std::string::npos) return false;
    pos = text.find(':', pos + marker.size());
    if (pos == std::string::npos) return false;
    pos = text.find('"', pos + 1);
    if (pos == std::string::npos) return false;
    size_t end = text.find('"', pos + 1);
    if (end == std::string::npos) return false;
    value = text.substr(pos + 1, end - pos - 1);
    return !value.empty();
}

static bool json_i64_value(const std::string& text, const std::string& name, long long& value) {
    std::string marker = "\"" + name + "\"";
    size_t pos = text.find(marker);
    if (pos == std::string::npos) return false;
    pos = text.find(':', pos + marker.size());
    if (pos == std::string::npos) return false;
    size_t start = text.find_first_of("-0123456789", pos + 1);
    if (start == std::string::npos) return false;
    size_t end = text.find_first_not_of("0123456789", start + (text[start] == '-' ? 1 : 0));
    return parse_i64(text.substr(start, end - start), value);
}

static bool activate_current_generation() {
    fs::path store_root = canonical_store_root();
    std::string current;
    if (!read_file_text(store_root / "CURRENT", current)) {
        if (CFG.require_generation) return false;
        rebuild_object_index();
        std::lock_guard<std::mutex> lock(g_object_index_mu);
        g_active_root = store_root;
        g_active_generation.clear();
        g_generation_ready = true;
        return true;
    }

    std::string generation_id, expected_ready_sha;
    long long current_fence = -1;
    if (!json_string_value(current, "generation_id", generation_id)
        || !json_i64_value(current, "fence", current_fence)
        || !json_string_value(current, "ready_sha256", expected_ready_sha)
        || !key_is_basic_safe(generation_id)
        || generation_id.find('/') != std::string::npos
        || generation_id.find('\\') != std::string::npos) {
        log_line("CURRENT validation failed");
        return false;
    }

    fs::path root = store_root / "generations" / generation_id;
    std::string ready;
    if (!read_file_text(root / "READY.json", ready)) return false;
    if (fsp::sha256_hex(ready) != expected_ready_sha) return false;
    std::string ready_generation, state, expected_index_sha;
    long long ready_fence = -1, object_count = -1;
    if (!json_string_value(ready, "generation_id", ready_generation)
        || !json_string_value(ready, "state", state)
        || !json_i64_value(ready, "fence", ready_fence)
        || !json_i64_value(ready, "object_count", object_count)
        || !json_string_value(ready, "index_sha256", expected_index_sha)
        || ready_generation != generation_id || state != "READY"
        || ready_fence != current_fence || object_count < 0) return false;

    std::string index_digest;
    if (!file_sha256(root / "OBJECTS.index", index_digest) || index_digest != expected_index_sha) return false;
    std::ifstream index(root / "OBJECTS.index", std::ios::binary);
    std::string header;
    if (!index || !std::getline(index, header) || header != "fsp-generation-index-v1") return false;
    std::vector<IndexedObject> entries;
    std::string line;
    while (std::getline(index, line)) {
        size_t first = line.find('\t');
        size_t second = first == std::string::npos ? std::string::npos : line.find('\t', first + 1);
        if (first == std::string::npos || second == std::string::npos) return false;
        std::string key = line.substr(0, first);
        std::string expected_object_sha = line.substr(second + 1);
        long long declared_size = -1;
        if (!key_is_basic_safe(key) || key.front() == '/' || key.find("..") != std::string::npos
            || !parse_i64(line.substr(first + 1, second - first - 1), declared_size)
            || declared_size < 0) return false;
        fs::path file = root / fs::path(key);
        std::error_code ec;
        fs::path canonical = fs::weakly_canonical(file, ec);
        if (ec || !path_has_prefix(root, canonical) || !fs::is_regular_file(canonical, ec)
            || static_cast<long long>(fs::file_size(canonical, ec)) != declared_size) return false;
        std::string object_digest;
        if (!file_sha256(canonical, object_digest) || object_digest != expected_object_sha) return false;
        entries.push_back({key, static_cast<uintmax_t>(declared_size)});
    }
    if (!index.eof() || static_cast<long long>(entries.size()) != object_count
        || !std::is_sorted(entries.begin(), entries.end(),
                           [](const auto& left, const auto& right) { return left.key < right.key; })) return false;

    {
        std::lock_guard<std::mutex> lock(g_object_index_mu);
        g_active_root = fs::weakly_canonical(root);
        g_active_generation = generation_id;
        g_object_index = std::move(entries);
    }
    g_generation_ready = true;
    log_line("activated generation=" + generation_id + " objects=" + std::to_string(object_count));
    return true;
}

static void initialize_object_index() {
    if (fs::exists(canonical_store_root() / "CURRENT")) {
        if (!activate_current_generation()) {
            g_generation_ready = false;
            log_line("no valid generation is ready");
        }
        return;
    }
    if (CFG.require_generation) {
        g_generation_ready = false;
        log_line("CURRENT is required but no generation is active");
        return;
    }
    std::vector<IndexedObject> entries;
    if (load_object_index(entries)) {
        std::lock_guard<std::mutex> lock(g_object_index_mu);
        g_object_index = std::move(entries);
        g_active_root = canonical_store_root();
        g_generation_ready = true;
        log_line("loaded object index entries=" + std::to_string(g_object_index.size()));
        return;
    }
    log_line("building object index");
    rebuild_object_index();
    g_generation_ready = true;
    std::lock_guard<std::mutex> lock(g_object_index_mu);
    log_line("built object index entries=" + std::to_string(g_object_index.size()));
}

// ---------------------------------------------------------------------------
// socket send helpers
// ---------------------------------------------------------------------------

static bool send_all(SocketHandle s, const char* data, size_t len) {
    size_t sent = 0;
    while (sent < len) {
        size_t rem = len - sent;
        int chunk = (int)std::min(rem, (size_t)1 << 20);
#ifdef _WIN32
        int n = ::send(s, data + sent, chunk, 0);
#else
        int n = (int)::send(s, data + sent, chunk, 0);
#endif
        if (n <= 0) return false;
        sent += (size_t)n;
    }
    return true;
}

static bool send_headers(SocketHandle s,
                         int status,
                         const std::string& reason,
                         const std::string& content_type,
                         long long content_length,
                         const std::string& extra_headers = "") {
    std::ostringstream h;
    h << "HTTP/1.1 " << status << " " << reason << "\r\n"
      << "Content-Type: " << content_type << "\r\n"
      << "Content-Length: " << content_length << "\r\n"
      << "Accept-Ranges: bytes\r\n"
      << "Server: s3emu-cpp-agent/" << APP_VERSION << "\r\n"
      << extra_headers
      << "Connection: close\r\n\r\n";
    std::string head = h.str();
    return send_all(s, head.data(), head.size());
}

static bool send_body_from_file(SocketHandle s, const fs::path& p, long long start, long long count) {
    std::ifstream f(p, std::ios::binary);
    if (!f) return false;
    f.seekg(start, std::ios::beg);
    if (!f) return false;

    static const size_t kBuf = 64 * 1024;
    std::vector<char> buf(kBuf);
    long long left = count;
    while (left > 0) {
        size_t want = (size_t)std::min<long long>((long long)kBuf, left);
        f.read(buf.data(), (std::streamsize)want);
        std::streamsize got = f.gcount();
        if (got <= 0) return false;
        if (!send_all(s, buf.data(), (size_t)got)) return false;
        left -= (long long)got;
    }
    return true;
}

static std::string s3_error_xml(const std::string& code, const std::string& msg, const std::string& res) {
    std::ostringstream x;
    x << "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Error><Code>" << code
      << "</Code><Message>" << xml_escape(msg) << "</Message><Resource>" << xml_escape(res)
      << "</Resource><RequestId>cpp</RequestId></Error>";
    return x.str();
}

static void send_fixed_response(SocketHandle s,
                                int status,
                                const std::string& reason,
                                const std::string& content_type,
                                const std::string& body,
                                bool head_only,
                                const std::string& extra_headers = "") {
    if (!send_headers(s, status, reason, content_type, (long long)body.size(), extra_headers)) return;
    if (!head_only && !body.empty()) send_all(s, body.data(), body.size());
}

// ---------------------------------------------------------------------------
// request handling
// ---------------------------------------------------------------------------

struct Request {
    std::string method;
    std::string path;
    std::string query;
    std::string range;
};

static std::string content_type_for(const std::string& key) {
    if (key.size() >= 5 && key.compare(key.size() - 5, 5, ".json") == 0) return "application/json";
    if (key.size() >= 5 && key.compare(key.size() - 5, 5, ".text") == 0) return "text/plain";
    if (key.size() >= 4 && key.compare(key.size() - 4, 4, ".avro") == 0) return "application/avro";
    if (key.size() >= 8 && key.compare(key.size() - 8, 8, ".parquet") == 0) return "application/octet-stream";
    return "application/octet-stream";
}

static std::string query_param(const std::string& q, const std::string& name) {
    std::string pfx = name + "=";
    size_t pos = 0;
    while (pos < q.size()) {
        size_t amp = q.find('&', pos);
        std::string part = q.substr(pos, amp == std::string::npos ? std::string::npos : amp - pos);
        if (part.compare(0, pfx.size(), pfx) == 0) return url_decode(part.substr(pfx.size()));
        if (amp == std::string::npos) break;
        pos = amp + 1;
    }
    return "";
}

static bool continuation_token_valid(const std::string& prefix,
                                     const std::string& token,
                                     const std::string& delimiter) {
    if (token.empty() || token.rfind(prefix, 0) != 0) return false;

    std::lock_guard<std::mutex> lock(g_object_index_mu);
    auto exact = std::lower_bound(
        g_object_index.begin(), g_object_index.end(), token,
        [](const auto& entry, const std::string& key) { return entry.key < key; });
    if (exact != g_object_index.end() && exact->key == token) return true;

    if (!delimiter.empty() && token.size() >= delimiter.size()
        && token.compare(token.size() - delimiter.size(), delimiter.size(), delimiter) == 0) {
        for (const auto& entry : g_object_index) {
            if (entry.key.rfind(token, 0) == 0) return true;
        }
    }
    return false;
}

static bool query_has_param(const std::string& q, const std::string& name) {
    size_t pos = 0;
    while (pos <= q.size()) {
        size_t amp = q.find('&', pos);
        std::string part = q.substr(pos, amp == std::string::npos ? std::string::npos : amp - pos);
        size_t equal = part.find('=');
        if (part.substr(0, equal) == name) return true;
        if (amp == std::string::npos) break;
        pos = amp + 1;
    }
    return false;
}

struct ListItem {
    std::string value;
    uintmax_t size = 0;
    bool common_prefix = false;
};

static void handle_list(SocketHandle s,
                        const std::string& prefix,
                        int max_keys,
                        const std::string& continuation_token,
                        const std::string& delimiter,
                        bool head_only) {
    std::lock_guard<std::mutex> lock(g_object_index_mu);
    auto first = std::lower_bound(
        g_object_index.begin(), g_object_index.end(), prefix,
        [](const auto& entry, const std::string& key) { return entry.key < key; });
    if (!continuation_token.empty() && first != g_object_index.end()) {
        auto token_pos = std::upper_bound(
            g_object_index.begin(), g_object_index.end(), continuation_token,
            [](const std::string& key, const auto& entry) { return key < entry.key; });
        if (!delimiter.empty() && continuation_token.size() >= delimiter.size()
            && continuation_token.compare(continuation_token.size() - delimiter.size(),
                                          delimiter.size(), delimiter) == 0) {
            token_pos = std::lower_bound(
                g_object_index.begin(), g_object_index.end(), continuation_token,
                [](const auto& entry, const std::string& key) { return entry.key < key; });
            while (token_pos != g_object_index.end()
                   && token_pos->key.rfind(continuation_token, 0) == 0) {
                ++token_pos;
            }
        }
        if (token_pos > first) first = token_pos;
    }

    size_t page_size = static_cast<size_t>(max_keys);
    std::vector<ListItem> page;
    std::set<std::string> emitted_prefixes;
    auto scan = first;
    while (scan != g_object_index.end() && page.size() < page_size) {
        if (scan->key.rfind(prefix, 0) != 0) break;
        if (delimiter.empty()) {
            page.push_back({scan->key, scan->size, false});
        } else {
            std::string remainder = scan->key.substr(prefix.size());
            size_t delimiter_pos = remainder.find(delimiter);
            if (delimiter_pos == std::string::npos) {
                page.push_back({scan->key, scan->size, false});
            } else {
                std::string common = prefix + remainder.substr(0, delimiter_pos + delimiter.size());
                if (emitted_prefixes.insert(common).second) {
                    page.push_back({common, 0, true});
                }
            }
        }
        ++scan;
    }
    bool truncated = scan != g_object_index.end() && scan->key.rfind(prefix, 0) == 0;

    std::ostringstream x;
    x << "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
      << "<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">"
      << "<Name>" << xml_escape(CFG.bucket) << "</Name>"
      << "<Prefix>" << xml_escape(prefix) << "</Prefix>"
      << "<KeyCount>" << page.size() << "</KeyCount>"
            << "<MaxKeys>" << max_keys << "</MaxKeys><IsTruncated>" << (truncated ? "true" : "false") << "</IsTruncated>";
        if (truncated && !page.empty()) {
                x << "<NextContinuationToken>" << xml_escape(page.back().value)
                    << "</NextContinuationToken>";
        }
    if (!delimiter.empty()) x << "<Delimiter>" << xml_escape(delimiter) << "</Delimiter>";
    std::string now = iso_now();
        for (const auto& item : page) {
            if (item.common_prefix) {
                x << "<CommonPrefixes><Prefix>" << xml_escape(item.value) << "</Prefix></CommonPrefixes>";
                continue;
            }
                x << "<Contents><Key>" << xml_escape(item.value) << "</Key>"
          << "<LastModified>" << now << "</LastModified>"
                    << "<ETag>\"" << etag_for(item.value) << "\"</ETag>"
                    << "<Size>" << item.size << "</Size>"
          << "<StorageClass>STANDARD</StorageClass></Contents>";
    }
    x << "</ListBucketResult>";
    send_fixed_response(s, 200, "OK", "application/xml", x.str(), head_only);
}

struct RangeResult {
    bool is_partial = false;
    bool unsat = false;
    bool invalid = false;
    long long start = 0;
    long long end = -1;
};

static RangeResult parse_range_header(const std::string& range, long long total) {
    RangeResult rr;
    rr.start = 0;
    rr.end = total - 1;
    if (range.empty()) return rr;
    if (range.rfind("bytes=", 0) != 0 || total < 0) {
        rr.invalid = true;
        return rr;
    }

    std::string spec = range.substr(6);
    size_t comma = spec.find(',');
    if (comma != std::string::npos) spec = spec.substr(0, comma);
    size_t dash = spec.find('-');
    if (dash == std::string::npos) {
        rr.invalid = true;
        return rr;
    }

    std::string a = spec.substr(0, dash);
    std::string b = spec.substr(dash + 1);

    if (a.empty()) {
        long long n = 0;
        if (!parse_i64(b, n) || n <= 0) {
            rr.invalid = true;
            return rr;
        }
        if (n > total) n = total;
        rr.start = total - n;
        rr.end = total - 1;
        rr.is_partial = true;
        return rr;
    }

    long long start = 0;
    if (!parse_i64(a, start) || start < 0) {
        rr.invalid = true;
        return rr;
    }
    long long end = total - 1;
    if (!b.empty()) {
        if (!parse_i64(b, end) || end < 0) {
            rr.invalid = true;
            return rr;
        }
    }
    if (start >= total) {
        rr.unsat = true;
        return rr;
    }
    if (end > total - 1) end = total - 1;
    if (end < start) {
        rr.unsat = true;
        return rr;
    }
    rr.start = start;
    rr.end = end;
    rr.is_partial = true;
    return rr;
}

static void handle_get(SocketHandle s, const std::string& key, const std::string& range, bool head_only) {
    fs::path p;
    if (!resolve_key_path(key, p)) {
        std::string body = s3_error_xml("InvalidArgument", "bad key", "/" + key);
        send_fixed_response(s, 400, "Bad Request", "application/xml", body, head_only);
        return;
    }

    std::error_code ec;
    if (!fs::exists(p, ec) || !fs::is_regular_file(p, ec)) {
        // Lazy: ask the Manager to materialize this object's table into the shared
        // store, then retry once. Mirrors the Python Agent's on-demand gate.
        bool served = false;
        if (CFG.materialize_mode == "lazy" && !CFG.manager_url.empty()
            && try_manager_materialize(key)) {
            served = fs::exists(p, ec) && fs::is_regular_file(p, ec);
        }
        if (!served) {
            std::string body = s3_error_xml("NoSuchKey", "The specified key does not exist.", "/" + key);
            send_fixed_response(s, 404, "Not Found", "application/xml", body, head_only);
            return;
        }
    }

    long long total = (long long)fs::file_size(p, ec);
    if (ec || total < 0) {
        std::string body = s3_error_xml("InternalError", "stat failed", "/" + key);
        send_fixed_response(s, 500, "Internal Server Error", "application/xml", body, head_only);
        return;
    }

    std::string ct = content_type_for(key);
    RangeResult rr = parse_range_header(range, total);
    if (rr.invalid) {
        std::string body = s3_error_xml("InvalidRange", "malformed range", "/" + key);
        send_fixed_response(s, 416, "Range Not Satisfiable", "application/xml", body, head_only);
        return;
    }
    if (rr.unsat) {
        std::string body = s3_error_xml("InvalidRange", "range not satisfiable", "/" + key);
        send_fixed_response(s, 416, "Range Not Satisfiable", "application/xml", body, head_only);
        return;
    }

    long long send_start = rr.is_partial ? rr.start : 0;
    long long send_end = rr.is_partial ? rr.end : (total - 1);
    long long send_len = (total == 0) ? 0 : (send_end - send_start + 1);

    std::string extra;
    int status = 200;
    std::string reason = "OK";
    if (rr.is_partial) {
        status = 206;
        reason = "Partial Content";
        std::ostringstream e;
        e << "Content-Range: bytes " << send_start << "-" << send_end << "/" << total << "\r\n";
        extra = e.str();
    }

    if (!send_headers(s, status, reason, ct, send_len, extra)) return;
    if (!head_only && send_len > 0) {
        if (!send_body_from_file(s, p, send_start, send_len)) {
            log_line("send_body_from_file failed for key=" + key);
        }
    }
}

static bool parse_request_line(const std::string& line, std::string& method, std::string& target) {
    std::istringstream ls(line);
    std::string version;
    ls >> method >> target >> version;
    if (method.empty() || target.empty() || version.empty()) return false;
    return true;
}

static void handle_connection(SocketHandle client) {
    static const size_t kMaxRequestHeaders = 64 * 1024;
    std::string buf;
    char tmp[8192];
    for (int i = 0; i < 64; ++i) {
#ifdef _WIN32
        int n = ::recv(client, tmp, (int)sizeof(tmp), 0);
#else
        int n = (int)::recv(client, tmp, sizeof(tmp), 0);
#endif
        if (n <= 0) break;
        buf.append(tmp, (size_t)n);
        if (buf.size() > kMaxRequestHeaders) {
            send_fixed_response(client, 413, "Payload Too Large", "text/plain", "", true);
            close_socket(client);
            return;
        }
        if (buf.find("\r\n\r\n") != std::string::npos) break;
    }
    if (buf.empty()) {
        close_socket(client);
        return;
    }

    Request req;
    size_t line_end = buf.find("\r\n");
    std::string line = buf.substr(0, line_end == std::string::npos ? buf.size() : line_end);
    std::string target;
    if (!parse_request_line(line, req.method, target)) {
        send_fixed_response(client, 400, "Bad Request", "application/xml",
                            s3_error_xml("InvalidRequest", "malformed request line", "/"),
                            true);
        close_socket(client);
        return;
    }

    {
        std::string lower = to_lower_ascii(buf);
        size_t rp = lower.find("\r\nrange:");
        if (rp != std::string::npos) {
            size_t vs = rp + 8;
            size_t ve = buf.find("\r\n", vs);
            if (ve != std::string::npos && ve > vs) {
                std::string v = buf.substr(vs, ve - vs);
                size_t a = v.find_first_not_of(" ");
                req.range = a == std::string::npos ? "" : v.substr(a);
            }
        }
    }

    size_t qpos = target.find('?');
    std::string raw_path = qpos == std::string::npos ? target : target.substr(0, qpos);
    req.query = qpos == std::string::npos ? "" : target.substr(qpos + 1);
    req.path = url_decode(raw_path);

    bool head_only = (req.method == "HEAD");

    if (req.path == "/healthz") {
        send_fixed_response(client, 200, "OK", "application/json",
                            std::string("{\"status\":\"ok\",\"role\":\"agent\",\"impl\":\"cpp\",\"version\":\"") + APP_VERSION + "\"}",
                            head_only);
        close_socket(client);
        return;
    }

    if (req.path == "/readyz") {
        const bool draining = g_draining.load();
        const bool ready = !draining && g_generation_ready.load();
        send_fixed_response(client, ready ? 200 : 503, ready ? "OK" : "Service Unavailable",
                            "application/json",
                            std::string("{\"status\":\"") + (draining ? "draining" : (ready ? "ready" : "not_ready")) +
                                "\",\"role\":\"agent\",\"impl\":\"cpp\",\"version\":\"" + APP_VERSION + "\"}",
                            head_only);
        close_socket(client);
        return;
    }

    if (req.path == "/favicon.ico") {
        send_fixed_response(client, 204, "No Content", "text/plain", "", head_only);
        close_socket(client);
        return;
    }

    if (req.method != "GET" && req.method != "HEAD") {
        send_fixed_response(client, 405, "Method Not Allowed", "application/xml",
                            s3_error_xml("MethodNotAllowed", "only GET/HEAD", req.path),
                            true);
        close_socket(client);
        return;
    }

    std::string p = req.path;
    if (!p.empty() && p[0] == '/') p = p.substr(1);
    size_t slash = p.find('/');
    std::string bucket = slash == std::string::npos ? p : p.substr(0, slash);
    std::string key = slash == std::string::npos ? "" : p.substr(slash + 1);
    if (bucket != CFG.bucket) {
        send_fixed_response(client, 404, "Not Found", "application/xml",
                            s3_error_xml("NoSuchBucket", "The specified bucket does not exist.", "/" + bucket),
                            head_only);
        close_socket(client);
        return;
    }

    if (key.empty()) {
        std::string max_keys_value = query_param(req.query, "max-keys");
        int max_keys = 1000;
        if (query_has_param(req.query, "max-keys")) {
            long long parsed_max_keys = 0;
            if (!parse_i64(max_keys_value, parsed_max_keys) || parsed_max_keys < 0 || parsed_max_keys > 1000) {
                send_fixed_response(client, 400, "Bad Request", "application/xml",
                                    s3_error_xml("InvalidArgument", "invalid max-keys", req.path),
                                    head_only);
                close_socket(client);
                return;
            }
            max_keys = static_cast<int>(parsed_max_keys);
        }
        std::string prefix = query_param(req.query, "prefix");
        std::string continuation_token = query_param(req.query, "continuation-token");
        std::string delimiter = query_param(req.query, "delimiter");
        if (!continuation_token.empty()
            && !continuation_token_valid(prefix, continuation_token, delimiter)) {
            send_fixed_response(client, 400, "Bad Request", "application/xml",
                                s3_error_xml("InvalidArgument", "invalid continuation-token", req.path),
                                head_only);
            close_socket(client);
            return;
        }
        handle_list(client, prefix, max_keys, continuation_token, delimiter, head_only);
    } else {
        handle_get(client, key, req.range, head_only);
    }

    close_socket(client);
}

// ---------------------------------------------------------------------------
// minimal HTTP client (register/heartbeat)
// ---------------------------------------------------------------------------

static bool parse_url(const std::string& url, std::string& host, int& port, std::string& base) {
    std::string u = url;
    if (u.rfind("http://", 0) == 0) u = u.substr(7);
    size_t slash = u.find('/');
    std::string hostport = slash == std::string::npos ? u : u.substr(0, slash);
    base = slash == std::string::npos ? "" : u.substr(slash);
    size_t colon = hostport.find(':');
    host = colon == std::string::npos ? hostport : hostport.substr(0, colon);
    std::string p = colon == std::string::npos ? "80" : hostport.substr(colon + 1);
    port = parse_int_or(p, 80, 1, 65535);
    if (host == "0.0.0.0" || host == "::") host = "127.0.0.1";
    return !host.empty();
}

static std::string base64_encode(const std::string& value) {
    static const char alphabet[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    std::string encoded;
    encoded.reserve(((value.size() + 2) / 3) * 4);
    for (size_t index = 0; index < value.size(); index += 3) {
        const unsigned int first = static_cast<unsigned char>(value[index]);
        const bool has_second = index + 1 < value.size();
        const bool has_third = index + 2 < value.size();
        const unsigned int second = has_second ? static_cast<unsigned char>(value[index + 1]) : 0;
        const unsigned int third = has_third ? static_cast<unsigned char>(value[index + 2]) : 0;
        const unsigned int block = (first << 16) | (second << 8) | third;
        encoded.push_back(alphabet[(block >> 18) & 0x3f]);
        encoded.push_back(alphabet[(block >> 12) & 0x3f]);
        encoded.push_back(has_second ? alphabet[(block >> 6) & 0x3f] : '=');
        encoded.push_back(has_third ? alphabet[block & 0x3f] : '=');
    }
    return encoded;
}

static std::string manager_auth_header() {
    if (CFG.manager_auth_username.empty() || CFG.manager_auth_password.empty()) return "";
    return "Authorization: Basic "
        + base64_encode(CFG.manager_auth_username + ":" + CFG.manager_auth_password) + "\r\n";
}

static int http_post(const std::string& host, int port, const std::string& path,
                     const std::string& body, std::string& resp_body, int timeout_ms = 0) {
    struct addrinfo hints;
    std::memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;

    struct addrinfo* res = nullptr;
    std::string ports = std::to_string(port);
    if (getaddrinfo(host.c_str(), ports.c_str(), &hints, &res) != 0) return -1;

    SocketHandle s = socket(res->ai_family, res->ai_socktype, res->ai_protocol);
    if (s == kInvalidSocket) {
        freeaddrinfo(res);
        return -1;
    }
    set_socket_timeouts(s, timeout_ms > 0 ? timeout_ms : CFG.socket_timeout_ms);

    if (connect(s, res->ai_addr, (int)res->ai_addrlen) != 0) {
        close_socket(s);
        freeaddrinfo(res);
        return -1;
    }
    freeaddrinfo(res);

    std::ostringstream r;
    r << "POST " << path << " HTTP/1.1\r\n"
      << "Host: " << host << ":" << port << "\r\n"
      << "Content-Type: application/json\r\n"
            << manager_auth_header()
      << "Content-Length: " << body.size() << "\r\n"
      << "Connection: close\r\n\r\n" << body;
    std::string reqs = r.str();
    if (!send_all(s, reqs.data(), reqs.size())) {
        close_socket(s);
        return -1;
    }

    std::string full;
    char tmp[8192];
    for (;;) {
#ifdef _WIN32
        int n = ::recv(s, tmp, (int)sizeof(tmp), 0);
#else
        int n = (int)::recv(s, tmp, sizeof(tmp), 0);
#endif
        if (n <= 0) break;
        full.append(tmp, (size_t)n);
    }
    close_socket(s);

    int status = -1;
    if (full.rfind("HTTP/1.1 ", 0) == 0 || full.rfind("HTTP/1.0 ", 0) == 0) {
        long long st = 0;
        if (full.size() >= 12 && parse_i64(full.substr(9, 3), st)) status = (int)st;
    }
    size_t hb = full.find("\r\n\r\n");
    resp_body = hb == std::string::npos ? "" : full.substr(hb + 4);
    return status;
}

static std::string json_str(const std::string& body, const std::string& name) {
    std::string pat = "\"" + name + "\"";
    size_t k = body.find(pat);
    if (k == std::string::npos) return "";
    size_t colon = body.find(':', k + pat.size());
    if (colon == std::string::npos) return "";
    size_t q1 = body.find('"', colon + 1);
    if (q1 == std::string::npos) return "";
    size_t q2 = body.find('"', q1 + 1);
    if (q2 == std::string::npos) return "";
    return body.substr(q1 + 1, q2 - q1 - 1);
}

// Minimal JSON string escape (object keys only contain / - . _ alnum, but be safe).
static std::string json_escape(const std::string& s) {
    std::string o;
    o.reserve(s.size() + 8);
    for (char c : s) {
        if (c == '"' || c == '\\') { o.push_back('\\'); o.push_back(c); }
        else if (c == '\n') { o += "\\n"; }
        else if (c == '\r') { o += "\\r"; }
        else { o.push_back(c); }
    }
    return o;
}

// Ask the Manager to materialize the table owning ``key`` into the shared store.
// Blocks up to CFG.materialize_timeout_ms while the Manager generates the splits.
// Returns true only on a 200 with ok:true.
static bool try_manager_materialize(const std::string& key) {
    std::string host, base;
    int port = 80;
    if (!parse_url(CFG.manager_url, host, port, base)) return false;
    std::string body = "{\"key\":\"" + json_escape(key) + "\"}";
    std::string rb;
    int st = http_post(host, port, "/control/materialize", body, rb, CFG.materialize_timeout_ms);
    bool ok = (st == 200) && (rb.find("\"ok\":true") != std::string::npos
                              || rb.find("\"ok\": true") != std::string::npos);
    if (!ok) {
        log_line("materialize request key=" + key + " status=" + std::to_string(st));
    } else {
        activate_current_generation();
    }
    return ok;
}

static std::atomic<bool> g_running{true};
static std::atomic<int> g_inflight{0};
static std::mutex g_conn_mu;
static std::condition_variable g_conn_cv;
static std::queue<SocketHandle> g_conn_queue;

static void index_refresh_loop() {
    if (CFG.index_refresh_seconds <= 0) return;
    while (g_running) {
        for (int elapsed = 0; elapsed < CFG.index_refresh_seconds && g_running; ++elapsed) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }
        if (g_running && !activate_current_generation() && g_active_generation.empty()) {
            g_generation_ready = false;
        }
    }
}

static void worker_loop() {
    while (g_running) {
        SocketHandle client = kInvalidSocket;
        {
            std::unique_lock<std::mutex> lock(g_conn_mu);
            g_conn_cv.wait(lock, [] { return !g_conn_queue.empty() || !g_running.load(); });
            if (!g_running.load() && g_conn_queue.empty()) break;
            if (g_conn_queue.empty()) continue;
            client = g_conn_queue.front();
            g_conn_queue.pop();
        }

        if (client == kInvalidSocket) continue;
        handle_connection(client);
        g_inflight.fetch_sub(1);
    }
}

static void control_loop() {
    std::string host, base;
    int port = 80;
    if (!parse_url(CFG.manager_url, host, port, base)) {
        log_line("manager_url parse failed");
        return;
    }

    std::string lease;

    auto do_register = [&]() -> bool {
        std::ostringstream j;
        j << "{\"agent_id\":\"" << CFG.agent_id << "\",\"host\":\"0.0.0.0\",\"port\":" << CFG.port
          << ",\"os\":\""
#ifdef _WIN32
          << "windows"
#else
          << "linux"
#endif
          << "\",\"version\":\"" << APP_VERSION
          << "\",\"capacity_hint\":0,\"advertise_host\":\"" << CFG.advertise_host
          << "\",\"contract_version\":\"1.0\"}";

        std::string rb;
        int st = http_post(host, port, "/control/register", j.str(), rb);
        if (st == 200) {
            lease = json_str(rb, "lease_id");
            if (!lease.empty()) {
                log_line("registered lease=" + lease.substr(0, std::min<size_t>(8, lease.size())));
                return true;
            }
        }
        log_line("register failed status=" + std::to_string(st));
        return false;
    };

    while (g_running && !do_register()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    }

    while (g_running) {
        std::this_thread::sleep_for(std::chrono::milliseconds(CFG.heartbeat_ms));
        if (!g_running) break;

        std::ostringstream j;
        j << "{\"agent_id\":\"" << CFG.agent_id << "\",\"lease_id\":\"" << lease
          << "\",\"health\":{\"cpu_pct\":0.0,\"mem_bytes\":0,\"cache_bytes\":0,\"inflight\":" << g_inflight.load() << "},"
          << "\"serving_tables\":[],\"epochs\":{}}";

        std::string rb;
        int st = http_post(host, port, "/control/heartbeat", j.str(), rb);
        if (st != 200) {
            log_line("heartbeat status=" + std::to_string(st) + " -> re-register");
            while (g_running && !do_register()) {
                std::this_thread::sleep_for(std::chrono::milliseconds(1000));
            }
        } else if (rb.find("\"drain\"") != std::string::npos && !g_draining.load()) {
            // Flip readiness so the LB deregisters us, then exit after the grace
            // window so in-flight requests finish.
            g_draining = true;
            log_line("drain requested -> readyz 503, exiting after grace");
            std::thread([]() {
                std::this_thread::sleep_for(std::chrono::milliseconds(CFG.drain_grace_ms));
                g_running = false;
            }).detach();
        } else if (rb.find("\"reload\"") != std::string::npos) {
            // Stateless serving agent reads the store live per request, so a reload
            // has no cached config/state to refresh. Acknowledge for observability;
            // confirms Manager->Agent push-down control is flowing.
            log_line("reload requested (no-op: serving agent reads the store live)");
        }
    }
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

static void print_usage(const char* prog) {
    std::printf(
        "Fabric Shortcut Proxy - C++ serving Agent (%s)\n\n"
        "A stateless S3 data-plane Agent that serves objects from a shared artifact\n"
        "store. It performs no SQL/Parquet/Iceberg work of its own. Under lazy mode it\n"
        "asks the Manager to materialize an object's table on a store miss, then serves it.\n\n"
        "Usage:\n"
        "  %s [--help|-h] [--version|-V]\n\n"
        "Configuration is read from environment variables (defaults in parentheses):\n\n"
        "  HOST (0.0.0.0)                 Bind address for the S3 port.\n"
        "  PORT (9400)                    S3 data-plane listen port.\n"
        "  STORE_DIR / ARTIFACT_STORE_DIR (./.artifacts)\n"
        "                                 Artifact store directory to serve from (shared with\n"
        "                                 the Manager under lazy mode).\n"
        "  INDEX_FILE ()                  Per-Pod legacy object index path.\n"
        "  REQUIRE_GENERATION (0)         Stay unready until CURRENT is valid.\n"
        "  S3_BUCKET (fabric-iceberg-poc) Advertised bucket name.\n"
        "  AGENT_ID (cpp-agent-1)         Identity used for Manager registration.\n"
        "  AGENT_ADVERTISE_HOST ()        Routable host advertised to the Manager/gateway.\n"
        "  MANAGER_URL ()                 Manager control plane URL; empty = standalone (no\n"
        "                                 register/heartbeat, no lazy materialize requests).\n"
        "  HEARTBEAT_MS (2000)            Heartbeat cadence to the Manager (ms).\n"
        "  SOCKET_TIMEOUT_MS (10000)      Socket send/recv timeout (ms).\n"
        "  MAX_INFLIGHT (256)             Max concurrent connections.\n"
        "  MATERIALIZE_MODE (eager)       eager | lazy. lazy: on a store miss, ask the Manager\n"
        "                                 (POST /control/materialize) to materialize the object's\n"
        "                                 table into the shared store, then serve it.\n"
        "  MATERIALIZE_TIMEOUT_MS (120000) Max wait for a lazy materialize request (ms).\n"
        "  INDEX_REFRESH_SECONDS (300)    Refresh persisted object index; 0 disables periodic refresh.\n"
        "  AGENT_DRAIN_GRACE_SECONDS (15) Drain grace window before exit (s).\n\n"
        "Endpoints: GET/HEAD /{bucket}/{key} (range-aware), GET /{bucket}?list-type=2,\n"
        "           GET /healthz, GET /readyz (503 while draining).\n\n"
        "Effective configuration (from the current environment):\n"
        "  host=%s port=%d store_dir=%s bucket=%s require_generation=%s\n"
        "  agent_id=%s manager_url=%s\n"
        "  materialize_mode=%s materialize_timeout_ms=%d\n"
        "  heartbeat_ms=%d socket_timeout_ms=%d max_inflight=%d index_refresh_seconds=%d drain_grace_ms=%d\n",
        APP_VERSION, prog,
        CFG.host.c_str(), CFG.port, CFG.store_dir.c_str(), CFG.bucket.c_str(),
        CFG.require_generation ? "true" : "false",
        CFG.agent_id.c_str(), CFG.manager_url.empty() ? "(standalone)" : CFG.manager_url.c_str(),
        CFG.materialize_mode.c_str(), CFG.materialize_timeout_ms,
        CFG.heartbeat_ms, CFG.socket_timeout_ms, CFG.max_inflight, CFG.index_refresh_seconds, CFG.drain_grace_ms);
}

int main(int argc, char** argv) {
    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        if (a == "--help" || a == "-h") { print_usage(argv[0]); return 0; }
        if (a == "--version" || a == "-V") { std::printf("%s\n", APP_VERSION); return 0; }
        std::fprintf(stderr, "unknown argument: %s (try --help)\n", a.c_str());
        return 2;
    }

    if (!net_init()) {
        log_line("network init failed");
        return 1;
    }

    std::signal(SIGTERM, request_termination);
    std::signal(SIGINT, request_termination);

    initialize_object_index();

    SocketHandle srv = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (srv == kInvalidSocket) {
        log_line("socket() failed");
        net_cleanup();
        return 1;
    }
    set_reuseaddr(srv);
    set_socket_timeouts(srv, CFG.socket_timeout_ms);

    sockaddr_in addr;
    std::memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons((unsigned short)CFG.port);
    if (CFG.host == "0.0.0.0") addr.sin_addr.s_addr = INADDR_ANY;
    else addr.sin_addr.s_addr = inet_addr(CFG.host.c_str());

    if (bind(srv, (sockaddr*)&addr, sizeof(addr)) != 0) {
        log_line("bind failed on port " + std::to_string(CFG.port));
        close_socket(srv);
        net_cleanup();
        return 1;
    }
    if (listen(srv, 128) != 0) {
        log_line("listen failed");
        close_socket(srv);
        net_cleanup();
        return 1;
    }

    log_line("serving S3 from '" + CFG.store_dir + "' on " + CFG.host + ":" + std::to_string(CFG.port) +
             " (bucket=" + CFG.bucket + ")");

    std::thread ctl;
    if (!CFG.manager_url.empty()) {
        log_line("control link -> " + CFG.manager_url + " as " + CFG.agent_id);
        ctl = std::thread(control_loop);
    }

    std::thread index_refresher(index_refresh_loop);

    const int worker_count = std::max(1, std::min(CFG.max_inflight, 32));
    std::vector<std::thread> workers;
    workers.reserve((size_t)worker_count);
    for (int i = 0; i < worker_count; ++i) {
        workers.emplace_back(worker_loop);
    }

    while (g_running) {
        if (g_termination_requested) {
            g_draining = true;
            log_line("termination signal received -> readyz 503, draining requests");
            break;
        }

        fd_set readable;
        FD_ZERO(&readable);
        FD_SET(srv, &readable);
        timeval timeout{};
        timeout.tv_sec = 0;
        timeout.tv_usec = 200000;
        int ready = select(static_cast<int>(srv) + 1, &readable, nullptr, nullptr, &timeout);
        if (ready <= 0) continue;

        sockaddr_in caddr;
#ifdef _WIN32
        int clen = sizeof(caddr);
#else
        socklen_t clen = sizeof(caddr);
#endif
        SocketHandle client = accept(srv, (sockaddr*)&caddr, &clen);
        if (client == kInvalidSocket) continue;

        set_socket_timeouts(client, CFG.socket_timeout_ms);

        int prev = g_inflight.fetch_add(1);
        if (prev >= CFG.max_inflight) {
            g_inflight.fetch_sub(1);
            send_fixed_response(client, 503, "Service Unavailable", "text/plain", "", true);
            close_socket(client);
            continue;
        }

        {
            std::lock_guard<std::mutex> lock(g_conn_mu);
            g_conn_queue.push(client);
        }
        g_conn_cv.notify_one();
    }

    close_socket(srv);
    srv = kInvalidSocket;
    const auto drain_deadline = std::chrono::steady_clock::now()
        + std::chrono::milliseconds(CFG.drain_grace_ms);
    while (g_inflight.load() > 0 && std::chrono::steady_clock::now() < drain_deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    g_running = false;
    g_conn_cv.notify_all();
    for (auto& worker : workers) {
        if (worker.joinable()) worker.join();
    }
    if (ctl.joinable()) ctl.join();
    if (index_refresher.joinable()) index_refresher.join();
    net_cleanup();
    return 0;
}
