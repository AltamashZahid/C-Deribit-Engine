cmake_minimum_required(VERSION 3.20)
project(DeribitEngine LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  set(CMAKE_BUILD_TYPE Release CACHE STRING "Build type" FORCE)
endif()

option(DE_ENABLE_ASAN "Build with AddressSanitizer + UBSan" OFF)
option(DE_ENABLE_TSAN "Build with ThreadSanitizer" OFF)

if(POLICY CMP0167)
  cmake_policy(SET CMP0167 NEW)  # use Boost's own BoostConfig.cmake
endif()
find_package(Boost 1.83 REQUIRED COMPONENTS json)
find_package(OpenSSL REQUIRED)
find_package(Threads REQUIRED)

add_library(engine_core STATIC
  src/common/config.cpp
  src/common/logger.cpp
  src/deribit/order_book.cpp
  src/deribit/book_feed.cpp
  src/deribit/client.cpp
  src/deribit/tls_roots.cpp
  src/server/book_message.cpp
  src/server/md_server.cpp
)
target_include_directories(engine_core PUBLIC src)
target_link_libraries(engine_core PUBLIC Boost::json OpenSSL::SSL OpenSSL::Crypto Threads::Threads)
target_compile_definitions(engine_core PUBLIC BOOST_ASIO_NO_DEPRECATED)

if(WIN32)
  target_compile_definitions(engine_core PUBLIC _WIN32_WINNT=0x0A00 WIN32_LEAN_AND_MEAN NOMINMAX)
  target_link_libraries(engine_core PUBLIC ws2_32 mswsock crypt32)
endif()

if(MSVC)
  target_compile_options(engine_core PUBLIC /W4 /bigobj /permissive-)
else()
  target_compile_options(engine_core PUBLIC -Wall -Wextra -Wpedantic)
  if(MINGW)
    # Beast/Asio translation units exceed the default COFF section limit.
    target_compile_options(engine_core PUBLIC -Wa,-mbig-obj)
    # Self-contained .exe files: no MSYS2 DLLs needed on PATH to run them.
    target_link_options(engine_core PUBLIC -static)
  endif()
endif()

if(DE_ENABLE_ASAN)
  target_compile_options(engine_core PUBLIC -fsanitize=address,undefined -fno-omit-frame-pointer)
  target_link_options(engine_core PUBLIC -fsanitize=address,undefined)
endif()
if(DE_ENABLE_TSAN)
  target_compile_options(engine_core PUBLIC -fsanitize=thread)
  target_link_options(engine_core PUBLIC -fsanitize=thread)
endif()

add_executable(deribit_engine src/app/main.cpp src/app/cli.cpp)
target_link_libraries(deribit_engine PRIVATE engine_core)

add_executable(engine_bench bench/engine_bench.cpp)
target_link_libraries(engine_bench PRIVATE engine_core)

add_executable(md_client_bench bench/md_client_bench.cpp)
target_link_libraries(md_client_bench PRIVATE engine_core)

enable_testing()
add_executable(unit_tests
  tests/test_main.cpp
  tests/test_common.cpp
  tests/test_order_book.cpp
  tests/test_md_server.cpp
  tests/test_client.cpp
)
target_link_libraries(unit_tests PRIVATE engine_core)
add_test(NAME unit_tests COMMAND unit_tests)
