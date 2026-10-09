#pragma once

// Minimal test harness: TEST(name) registers a function, CHECK* record failures and
// keep going, REQUIRE* abort the current test.

#include <cmath>
#include <cstdio>
#include <functional>
#include <sstream>
#include <string>
#include <vector>

namespace test {

struct Case {
    const char* name;
    void (*fn)();
};

inline std::vector<Case>& registry() {
    static std::vector<Case> cases;
    return cases;
}

inline int& failures() {
    static int n = 0;
    return n;
}

struct Registrar {
    Registrar(const char* name, void (*fn)()) { registry().push_back({name, fn}); }
};

struct Abort {};

template <class A, class B>
std::string describe(const A& a, const B& b) {
    std::ostringstream ss;
    ss << a << " vs " << b;
    return ss.str();
}

}  // namespace test

#define TEST(name)                                                  \
    static void name();                                             \
    static const ::test::Registrar name##_registrar(#name, &name);  \
    static void name()

#define DE_FAIL(msg)                                                            \
    do {                                                                        \
        ++::test::failures();                                                   \
        std::printf("  FAILED %s:%d: %s\n", __FILE__, __LINE__, std::string(msg).c_str()); \
    } while (0)

#define CHECK(cond) \
    do {            \
        if (!(cond)) DE_FAIL(#cond); \
    } while (0)

#define CHECK_EQ(a, b)                                                               \
    do {                                                                             \
        const auto va_ = (a);                                                        \
        const auto vb_ = (b);                                                        \
        if (!(va_ == vb_)) DE_FAIL(std::string(#a " == " #b " (") + ::test::describe(va_, vb_) + ")"); \
    } while (0)

#define CHECK_NEAR(a, b, eps)                                                         \
    do {                                                                              \
        const double va_ = (a), vb_ = (b);                                            \
        if (std::fabs(va_ - vb_) > (eps)) DE_FAIL(std::string(#a " ~= " #b " (") + ::test::describe(va_, vb_) + ")"); \
    } while (0)

#define REQUIRE(cond)              \
    do {                           \
        if (!(cond)) {             \
            DE_FAIL(#cond);        \
            throw ::test::Abort{}; \
        }                          \
    } while (0)
