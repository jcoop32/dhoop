cmake_minimum_required(VERSION 3.20)
project(whoop_worker CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

include(FetchContent)

# ── hiredis (required by redis-plus-plus) ─────────────────────────────────────
FetchContent_Declare(
    hiredis
    GIT_REPOSITORY https://github.com/redis/hiredis.git
    GIT_TAG        v1.2.0
)
FetchContent_MakeAvailable(hiredis)

# ── redis-plus-plus ───────────────────────────────────────────────────────────
FetchContent_Declare(
    redis_plus_plus
    GIT_REPOSITORY https://github.com/sewenew/redis-plus-plus.git
    GIT_TAG        1.3.12
)
set(REDIS_PLUS_PLUS_BUILD_TEST     OFF CACHE BOOL "" FORCE)
set(REDIS_PLUS_PLUS_BUILD_EXAMPLES OFF CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(redis_plus_plus)

# ── clickhouse-cpp ────────────────────────────────────────────────────────────
FetchContent_Declare(
    clickhouse_cpp
    GIT_REPOSITORY https://github.com/ClickHouse/clickhouse-cpp.git
    GIT_TAG        v2.5.1
)
set(BUILD_TESTS      OFF CACHE BOOL "" FORCE)
set(BUILD_BENCHMARKS OFF CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(clickhouse_cpp)

# ── Executable ────────────────────────────────────────────────────────────────
add_executable(whoop_worker main.cpp)

target_link_libraries(whoop_worker PRIVATE
    redis++
    hiredis
    clickhouse-cpp-lib
)

target_compile_options(whoop_worker PRIVATE -Wall -Wextra -O2)
