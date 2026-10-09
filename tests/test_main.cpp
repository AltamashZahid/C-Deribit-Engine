#include <chrono>
#include <cstdio>
#include <cstring>
#include <cstdlib>
#include <exception>
#include <thread>

#include "test.hpp"

// Usage: unit_tests [substring]   - runs the tests whose name contains substring.
int main(int argc, char** argv) {
    // Network tests use blocking I/O; never let a bug hang CI forever.
    std::thread([] {
        std::this_thread::sleep_for(std::chrono::minutes(3));
        std::printf("watchdog: tests took longer than 3 minutes, aborting\n");
        std::fflush(stdout);
        std::_Exit(3);
    }).detach();
    const char* filter = argc > 1 ? argv[1] : nullptr;
    int run = 0;
    for (const auto& c : test::registry()) {
        if (filter && !std::strstr(c.name, filter)) continue;
        ++run;
        const int before = test::failures();
        const auto start = std::chrono::steady_clock::now();
        try {
            c.fn();
        } catch (const test::Abort&) {
        } catch (const std::exception& e) {
            ++test::failures();
            std::printf("  FAILED: unexpected exception: %s\n", e.what());
        }
        const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start).count();
        std::printf("[%s] %s (%lld ms)\n", test::failures() == before ? " OK " : "FAIL", c.name, static_cast<long long>(ms));
    }
    std::printf("\n%d test(s), %d failure(s)\n", run, test::failures());
    return test::failures() == 0 && run > 0 ? 0 : 1;
}
