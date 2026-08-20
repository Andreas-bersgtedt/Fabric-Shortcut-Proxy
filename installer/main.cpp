#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <iostream>
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
        raw.c_lflag &= static_cast<tcflag_t>(~(ICANON | ECHO));
        raw.c_cc[VMIN] = 1;
        raw.c_cc[VTIME] = 0;
        if (tcsetattr(STDIN_FILENO, TCSAFLUSH, &raw) != 0) {
            return false;
        }
        active_ = true;
        return true;
    }

    ~TerminalMode() {
        if (active_) {
            tcsetattr(STDIN_FILENO, TCSAFLUSH, &original_);
            std::cout << "\033[0m\033[?25h" << std::flush;
        }
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

int run_shell_installer(const std::filesystem::path& script, int argc, char** argv) {
    std::vector<std::string> arguments;
    arguments.emplace_back(script.string());
    for (int index = 1; index < argc; ++index) {
        arguments.emplace_back(argv[index]);
    }
    std::vector<char*> command;
    command.reserve(arguments.size() + 1);
    for (auto& argument : arguments) {
        command.push_back(argument.data());
    }
    command.push_back(nullptr);
    execv(command[0], command.data());
    std::perror("unable to start installer/install.sh");
    return 127;
}

void clear_screen() {
    std::cout << "\033[2J\033[H";
}

void draw_menu(std::size_t selected) {
    static const char* const entries[] = {
        "Start setup wizard",
        "Run read-only checks",
        "Use line-based installer",
        "Quit",
    };
    clear_screen();
    std::cout << "\033[3m  FSP\033[0m\n"
              << "  FABRIC SHORTCUT PROXY\n"
              << "========================================\n\n"
              << "  SSH-safe installer\n\n";
    for (std::size_t index = 0; index < 4; ++index) {
        std::cout << (index == selected ? "  > " : "    ") << entries[index] << '\n';
    }
    std::cout << "\n  Use Up/Down, Enter, or Q to quit.\n" << std::flush;
}

int interactive(const std::filesystem::path& script) {
    TerminalMode terminal;
    if (!terminal.enable()) {
        std::cerr << "Interactive terminal input is unavailable; using the shell installer.\n";
        return run_shell_installer(script, 1, nullptr);
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
        if (key == '\r' || key == '\n') {
            if (selected == 3) {
                return 0;
            }
            if (selected == 2) {
                return run_shell_installer(script, 1, nullptr);
            }
            if (selected == 1) {
                const char* check_args[] = {"install.sh", "--check", nullptr};
                return run_shell_installer(script, 2, const_cast<char**>(check_args));
            }
            return run_shell_installer(script, 1, nullptr);
        }
        if (key == '\033') {
            char sequence[2];
            if (read(STDIN_FILENO, sequence, sizeof(sequence)) == 2 && sequence[0] == '[') {
                if (sequence[1] == 'A' && selected > 0) {
                    --selected;
                } else if (sequence[1] == 'B' && selected < 3) {
                    ++selected;
                }
            }
        }
    }
}

} // namespace

int main(int argc, char** argv) {
    const auto script = script_path(argv[0]);
    if (argc > 1) {
        return run_shell_installer(script, argc, argv);
    }
    return interactive(script);
}
