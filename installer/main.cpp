#include <cerrno>
#include <cctype>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fcntl.h>
#include <fstream>
#include <iostream>
#include <set>
#include <string>
#include <stdexcept>
#include <termios.h>
#include <unistd.h>
#include <vector>
#include <sys/stat.h>
#include <sys/wait.h>

namespace {

class TerminalMode {
public:
    TerminalMode() : active_(false) {}

    bool enable() {
        if (!isatty(STDIN_FILENO) || tcgetattr(STDIN_FILENO, &original_) != 0) {
            return false;
        }
        termios raw = original_;
        raw.c_lflag &= static_cast<tcflag_t>(~(ICANON | ECHO | ISIG));
        raw.c_cc[VMIN] = 1;
        raw.c_cc[VTIME] = 0;
        if (tcsetattr(STDIN_FILENO, TCSAFLUSH, &raw) != 0) {
            return false;
        }
        active_ = true;
        return true;
    }

    void restore() {
        if (active_) {
            tcsetattr(STDIN_FILENO, TCSAFLUSH, &original_);
            std::cout << "\033[0m\033[?25h" << std::flush;
            active_ = false;
        }
    }

    ~TerminalMode() {
        restore();
    }

private:
    termios original_{};
    bool active_;
};

std::filesystem::path script_path(const char* argv0) {
    std::error_code error;
    const auto executable = std::filesystem::canonical(argv0, error);
    if (!error) {
        return executable.parent_path() / "install.sh";
    }
    return std::filesystem::path(argv0).parent_path() / "install.sh";
}

bool valid_answer_key(const std::string& key) {
    static const std::set<std::string> keys = {
        "APPLY", "install_dir", "service_user", "service_group", "unit_name",
        "identity_mode", "tenant_id", "client_id", "client_secret_reference",
        "keyvault_mode", "keyvault_uri", "secret_backend", "manager_auth_username",
        "generate_admin_credentials", "generate_s3_credentials", "generate_agent_token",
        "tls_mode", "tls_hostname", "tls_cert_file", "tls_key_file", "start_service",
    };
    return keys.find(key) != keys.end();
}

bool valid_key_name(const std::string& key) {
    if (key.empty() || !(std::isalpha(static_cast<unsigned char>(key.front())) ||
                         key.front() == '_')) {
        return false;
    }
    for (const char character : key) {
        if (!(std::isalnum(static_cast<unsigned char>(character)) || character == '_')) {
            return false;
        }
    }
    return true;
}

bool validate_answers_file(const std::filesystem::path& path) {
    std::ifstream answers(path);
    if (!answers) {
        std::cerr << "Error: answers file could not be opened: " << path << '\n';
        return false;
    }
    std::set<std::string> seen;
    std::string line;
    std::size_t line_number = 0;
    while (std::getline(answers, line)) {
        ++line_number;
        const auto first = line.find_first_not_of(" \t\r");
        if (first == std::string::npos || line[first] == '#') {
            continue;
        }
        const auto equals = line.find('=', first);
        if (equals == std::string::npos) {
            std::cerr << "Error: answers line " << line_number << " has no '='\n";
            return false;
        }
        auto key = line.substr(first, equals - first);
        while (!key.empty() && (key.back() == ' ' || key.back() == '\t')) {
            key.pop_back();
        }
        if (!valid_key_name(key) || !valid_answer_key(key)) {
            std::cerr << "Error: unknown or invalid answers key on line "
                      << line_number << ": " << key << '\n';
            return false;
        }
        if (!seen.insert(key).second) {
            std::cerr << "Error: duplicate answers key on line "
                      << line_number << ": " << key << '\n';
            return false;
        }
    }
    return true;
}

bool validate_forwarded_arguments(const std::vector<std::string>& arguments) {
    for (std::size_t index = 0; index < arguments.size(); ++index) {
        const auto& argument = arguments[index];
        if (argument == "--answers") {
            if (index + 1 >= arguments.size() ||
                !validate_answers_file(arguments[index + 1])) {
                return false;
            }
            ++index;
        } else if (argument.rfind("--answers=", 0) == 0) {
            if (!validate_answers_file(argument.substr(10))) {
                return false;
            }
        }
    }
    return true;
}

int run_shell_installer(
    const std::filesystem::path& script,
    const std::vector<std::string>& extra_arguments) {
    std::vector<std::string> arguments;
    arguments.emplace_back("/bin/sh");
    arguments.emplace_back(script.string());
    for (const auto& argument : extra_arguments) {
        arguments.emplace_back(argument);
    }
    std::vector<char*> command;
    command.reserve(arguments.size() + 1);
    for (auto& argument : arguments) {
        command.push_back(argument.data());
    }
    command.push_back(nullptr);
    if (isatty(STDIN_FILENO)) {
        const int terminal = open("/dev/tty", O_RDWR);
        if (terminal < 0 || dup2(terminal, STDIN_FILENO) < 0 ||
            dup2(terminal, STDOUT_FILENO) < 0 || dup2(terminal, STDERR_FILENO) < 0) {
            if (terminal >= 0) {
                close(terminal);
            }
            std::perror("unable to attach installer to /dev/tty");
            return 127;
        }
        if (terminal > STDERR_FILENO) {
            close(terminal);
        }
    }
    execv(command[0], command.data());
    std::perror("unable to start installer/install.sh");
    return 127;
}

std::string prompt(const std::string& label, const std::string& default_value = {}) {
    std::cout << label;
    if (!default_value.empty()) {
        std::cout << " [" << default_value << "]";
    }
    std::cout << ": " << std::flush;
    std::string value;
    if (!std::getline(std::cin, value)) {
        throw std::runtime_error("input closed");
    }
    return value.empty() ? default_value : value;
}

std::string choice(
    const std::string& label,
    const std::string& default_value,
    const std::set<std::string>& allowed) {
    while (true) {
        const auto value = prompt(label, default_value);
        if (allowed.find(value) != allowed.end()) {
            return value;
        }
        std::cerr << "Invalid value. Choose one of:";
        for (const auto& item : allowed) {
            std::cerr << ' ' << item;
        }
        std::cerr << '\n';
    }
}

std::filesystem::path write_cpp_answers() {
    std::string install_dir = prompt("Installation directory", "/opt/fabric-shortcut-proxy");
    std::string service_user = prompt("Service user", "fsp");
    std::string service_group = prompt("Service group", "fsp");
    std::string unit_name = prompt("systemd unit name", "fabric-shortcut-proxy.service");
    std::string identity_mode = choice(
        "Identity (managed_identity, service_principal, default)",
        "managed_identity", {"default", "managed_identity", "service_principal"});
    std::string tenant_id;
    std::string client_id;
    std::string client_secret_reference;
    if (identity_mode == "service_principal") {
        tenant_id = prompt("Tenant ID");
        client_id = prompt("Client/application ID");
        client_secret_reference = prompt("Client secret reference (env:NAME or file:/path)");
    }
    std::string keyvault_mode = choice(
        "Key Vault mode (disabled, read-through, write-back, required)",
        "disabled", {"disabled", "read-through", "required", "write-back"});
    std::string keyvault_uri;
    if (keyvault_mode != "disabled") {
        keyvault_uri = prompt("Key Vault URI");
    }
    std::string secret_backend = choice(
        "Secret backend (keyvault, env-file)",
        keyvault_mode == "disabled" ? "env-file" : "keyvault",
        {"env-file", "keyvault"});
    std::string manager_auth_username = prompt("Manager auth username", "operator");
    std::string generate_admin = choice(
        "Generate admin token and password (yes/no)", "yes", {"no", "yes"});
    std::string generate_s3 = choice(
        "Generate S3 access credentials (yes/no)", "yes", {"no", "yes"});
    std::string generate_agent = choice(
        "Generate unused AGENT_TOKEN placeholder (yes/no)", "no", {"no", "yes"});
    std::string tls_mode = choice(
        "TLS mode (disabled, nginx, direct)", "disabled", {"disabled", "direct", "nginx"});
    std::string tls_hostname;
    std::string tls_cert_file;
    std::string tls_key_file;
    if (tls_mode != "disabled") {
        tls_hostname = prompt("DNS hostname");
        tls_cert_file = prompt("Certificate/full-chain path");
        tls_key_file = prompt("Private-key path");
    }
    std::string start_service = choice(
        "Start and enable the systemd service (yes/no)", "no", {"no", "yes"});

    char template_path[] = "/tmp/fsp-installer-answers-XXXXXX";
    const int descriptor = mkstemp(template_path);
    if (descriptor < 0) {
        throw std::runtime_error("could not create protected answers file");
    }
    if (fchmod(descriptor, S_IRUSR | S_IWUSR) != 0) {
        close(descriptor);
        unlink(template_path);
        throw std::runtime_error("could not protect answers file");
    }
    std::ofstream answers(template_path);
    if (!answers) {
        close(descriptor);
        unlink(template_path);
        throw std::runtime_error("could not open protected answers file");
    }
    answers << "APPLY=APPLY\n"
            << "install_dir=" << install_dir << '\n'
            << "service_user=" << service_user << '\n'
            << "service_group=" << service_group << '\n'
            << "unit_name=" << unit_name << '\n'
            << "identity_mode=" << identity_mode << '\n';
    if (!tenant_id.empty()) {
        answers << "tenant_id=" << tenant_id << '\n'
                << "client_id=" << client_id << '\n'
                << "client_secret_reference=" << client_secret_reference << '\n';
    }
    answers << "keyvault_mode=" << keyvault_mode << '\n';
    if (!keyvault_uri.empty()) {
        answers << "keyvault_uri=" << keyvault_uri << '\n';
    }
    answers << "secret_backend=" << secret_backend << '\n'
            << "manager_auth_username=" << manager_auth_username << '\n'
            << "generate_admin_credentials=" << generate_admin << '\n'
            << "generate_s3_credentials=" << generate_s3 << '\n'
            << "generate_agent_token=" << generate_agent << '\n'
            << "tls_mode=" << tls_mode << '\n';
    if (!tls_hostname.empty()) {
        answers << "tls_hostname=" << tls_hostname << '\n'
                << "tls_cert_file=" << tls_cert_file << '\n'
                << "tls_key_file=" << tls_key_file << '\n';
    }
    answers << "start_service=" << start_service << '\n';
    answers.close();
    close(descriptor);
    return template_path;
}

int run_cpp_wizard(const std::filesystem::path& script) {
    try {
        std::cout << "\nC++ setup wizard\n"
                  << "The shell backend will apply these reviewed answers without prompting.\n\n";
        const auto answers = write_cpp_answers();
        const std::vector<std::string> arguments = {
            "--no-color", "--answers", answers.string()};
        const pid_t child = fork();
        if (child < 0) {
            unlink(answers.c_str());
            throw std::runtime_error("could not start provisioning backend");
        }
        if (child == 0) {
            run_shell_installer(script, arguments);
            _exit(127);
        }
        int status = 0;
        waitpid(child, &status, 0);
        unlink(answers.c_str());
        return WIFEXITED(status) ? WEXITSTATUS(status) : 1;
    } catch (const std::exception& error) {
        std::cerr << "C++ setup cancelled: " << error.what() << '\n';
        return 1;
    }
}

bool ansi_enabled() {
    const char* term = std::getenv("TERM");
    return isatty(STDOUT_FILENO) && term != nullptr && std::strcmp(term, "dumb") != 0;
}

void clear_screen() {
    if (ansi_enabled()) {
        std::cout << "\033[2J\033[H";
    }
}

void draw_menu(std::size_t selected) {
    static const char* const entries[] = {
        "Start setup wizard (C++)",
        "Resume setup wizard",
        "Preview setup (dry-run)",
        "Run read-only checks",
        "Use line-based installer fallback",
        "Quit",
    };
    clear_screen();
    if (ansi_enabled()) {
        std::cout << "\033[3m  FSP\033[0m\n";
    } else {
        std::cout << "  FSP\n";
    }
    std::cout << "  FABRIC SHORTCUT PROXY\n"
              << "========================================\n\n"
              << "  SSH-safe installer\n\n";
    for (std::size_t index = 0; index < 6; ++index) {
        std::cout << (index == selected ? "  > " : "    ") << (index + 1) << ") "
                  << entries[index] << '\n';
    }
    std::cout << "\n  Use Up/Down, 1-6, Enter, or Q to quit.\n" << std::flush;
}

int interactive(const std::filesystem::path& script) {
    TerminalMode terminal;
    if (!terminal.enable()) {
        std::cerr << "Interactive terminal input is unavailable; using the shell installer.\n";
        return run_shell_installer(script, {});
    }

    std::size_t selected = 0;
    while (true) {
        draw_menu(selected);
        char key = '\0';
        if (read(STDIN_FILENO, &key, 1) != 1) {
            return 1;
        }
        if (key == 'q' || key == 'Q' || key == 3) {
            return 0;
        }
        if (key >= '1' && key <= '6') {
            selected = static_cast<std::size_t>(key - '1');
            key = '\r';
        }
        if (key == '\r' || key == '\n') {
            if (selected == 5) {
                return 0;
            }
            if (selected == 4) {
                terminal.restore();
                return run_shell_installer(script, {});
            }
            if (selected == 3) {
                terminal.restore();
                return run_shell_installer(script, {"--check"});
            }
            if (selected == 2) {
                terminal.restore();
                return run_shell_installer(script, {"--dry-run"});
            }
            if (selected == 1) {
                terminal.restore();
                return run_shell_installer(script, {"--resume"});
            }
            terminal.restore();
            return run_cpp_wizard(script);
        }
        if (key == '\033') {
            char sequence[2];
            if (read(STDIN_FILENO, sequence, sizeof(sequence)) == 2 && sequence[0] == '[') {
                if (sequence[1] == 'A' && selected > 0) {
                    --selected;
                } else if (sequence[1] == 'B' && selected < 5) {
                    ++selected;
                }
            }
        }
    }
}

} // namespace

int main(int argc, char** argv) {
    const auto script = script_path(argv[0]);
    std::vector<std::string> arguments;
    if (argc > 1) {
        arguments.reserve(static_cast<std::size_t>(argc - 1));
        for (int index = 1; index < argc; ++index) {
            arguments.emplace_back(argv[index]);
        }
        if (!validate_forwarded_arguments(arguments)) {
            return 2;
        }
        return run_shell_installer(script, arguments);
    }
    return interactive(script);
}
