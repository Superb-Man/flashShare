cmake_minimum_required(VERSION 3.16)
project(flashshare VERSION 1.0.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()

set(CMAKE_CXX_FLAGS_RELEASE "-O2 -DNDEBUG")

# Find OpenSSL
find_package(OpenSSL REQUIRED)
find_package(Threads REQUIRED)

include_directories(${OPENSSL_INCLUDE_DIR})

add_executable(flashshare
    src/main.cpp
    src/cli/args.cpp
    src/cli/progress.cpp
    src/net/socket.cpp
    src/net/listener.cpp
    src/net/connection.cpp
    src/net/threadpool.cpp
    src/transfer/sender.cpp
    src/transfer/transfer_store.cpp
    src/transfer/receiver.cpp
    src/crypto/cipher.cpp
    src/util/logger.cpp
    src/util/sha256.cpp
    src/util/fs.cpp
)

target_include_directories(flashshare PRIVATE src)

target_link_libraries(flashshare PRIVATE
    ${OPENSSL_CRYPTO_LIBRARY}
    Threads::Threads
)

# GCC < 9 requires linking libstdc++fs explicitly for std::filesystem.
if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU" AND CMAKE_CXX_COMPILER_VERSION VERSION_LESS 9.0)
    target_link_libraries(flashshare PRIVATE stdc++fs)
endif()

# Windows: link Winsock libraries
if(WIN32)
    target_link_libraries(flashshare PRIVATE ws2_32 mswsock)
endif()

install(TARGETS flashshare DESTINATION bin)
