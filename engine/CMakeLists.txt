cmake_minimum_required(VERSION 3.16)
project(pulsestream_engine LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

add_executable(engine
  src/main.cpp
  src/server.cpp
  src/store.cpp
)

target_include_directories(engine PRIVATE
  ${CMAKE_CURRENT_SOURCE_DIR}/include
  ${CMAKE_CURRENT_SOURCE_DIR}/third_party
)

if (CMAKE_CXX_COMPILER_ID MATCHES "Clang|AppleClang|GNU")
  target_compile_options(engine PRIVATE -Wall -Wextra -Wpedantic)
endif()
