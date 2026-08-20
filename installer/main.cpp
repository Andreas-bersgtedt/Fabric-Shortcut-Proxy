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
#include <termios.h>
#include <unistd.h>
#include <vector>

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
        "Start setup wizard",
        "Resume setup wizard",
        "Preview setup (dry-run)",
        "Run read-only checks",
        "Use line-based installer",
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
            return run_shell_installer(script, {});
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
