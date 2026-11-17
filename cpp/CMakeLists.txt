cmake_minimum_required(VERSION 3.24)
project(martinet_cpp LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE RelWithDebInfo)
endif()
add_compile_options(-Wall -Wextra -Wno-unused-parameter)

option(MARTINET_BUILD_POULS "Build the pouls stream processor (needs librdkafka)" ON)
option(MARTINET_BUILD_TESTS "Build GoogleTest suites" ON)
option(MARTINET_SANITIZE "Build with ASan + UBSan" OFF)
if(MARTINET_SANITIZE)
  add_compile_options(-fsanitize=address,undefined -fno-omit-frame-pointer)
  add_link_options(-fsanitize=address,undefined)
endif()

find_package(Threads REQUIRED)
find_package(PkgConfig)

# Homebrew / vcpkg ship CMake package configs; Debian/Ubuntu ship only
# pkg-config files for gRPC and an autotools protobuf. Support both.
find_package(protobuf CONFIG QUIET)
if(NOT protobuf_FOUND)
  find_package(Protobuf REQUIRED)  # FindProtobuf module -> protobuf::libprotobuf, protobuf::protoc
endif()

find_package(gRPC CONFIG QUIET)
if(gRPC_FOUND)
  set(GRPCPP_LIB gRPC::grpc++)
  set(GRPCPP_REFLECTION_LIB gRPC::grpc++_reflection)
  get_target_property(GRPC_CPP_PLUGIN gRPC::grpc_cpp_plugin LOCATION)
else()
  pkg_check_modules(GRPCPP REQUIRED IMPORTED_TARGET grpc++)
  pkg_check_modules(GRPCPP_REFL IMPORTED_TARGET grpc++_reflection)
  set(GRPCPP_LIB PkgConfig::GRPCPP)
  if(GRPCPP_REFL_FOUND)
    set(GRPCPP_REFLECTION_LIB PkgConfig::GRPCPP_REFL)
  endif()
  find_program(GRPC_CPP_PLUGIN grpc_cpp_plugin REQUIRED)
endif()

# ---------------------------------------------------------------- protos
set(PROTO_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/../proto)
set(GEN_DIR ${CMAKE_CURRENT_BINARY_DIR}/gen)
file(MAKE_DIRECTORY ${GEN_DIR})

function(martinet_proto name rel)
  set(src ${PROTO_ROOT}/${rel}.proto)
  set(outs ${GEN_DIR}/${rel}.pb.cc ${GEN_DIR}/${rel}.pb.h
           ${GEN_DIR}/${rel}.grpc.pb.cc ${GEN_DIR}/${rel}.grpc.pb.h)
  add_custom_command(
    OUTPUT ${outs}
    COMMAND protobuf::protoc
      --proto_path=${PROTO_ROOT}
      --cpp_out=${GEN_DIR}
      --grpc_out=${GEN_DIR}
      --plugin=protoc-gen-grpc=${GRPC_CPP_PLUGIN}
      ${src}
    DEPENDS ${src}
    COMMENT "protoc ${rel}.proto")
  add_library(${name} STATIC ${outs})
  target_include_directories(${name} PUBLIC ${GEN_DIR})
  target_link_libraries(${name} PUBLIC protobuf::libprotobuf ${GRPCPP_LIB})
  target_compile_options(${name} PRIVATE -w)
endfunction()

martinet_proto(carnet_proto carnet/v1/carnet)
martinet_proto(martinet_proto martinet/v1/martinet)

# ---------------------------------------------------------------- carnet
add_library(carnet STATIC
  carnet/src/bloom.cc
  carnet/src/crc32c.cc
  carnet/src/db.cc
  carnet/src/memtable.cc
  carnet/src/merging_iterator.cc
  carnet/src/sstable.cc
  carnet/src/wal.cc)
target_include_directories(carnet PUBLIC carnet/include)
target_link_libraries(carnet PUBLIC Threads::Threads)

add_executable(carnet-server carnet/src/server.cc)
target_link_libraries(carnet-server PRIVATE carnet carnet_proto)
if(GRPCPP_REFLECTION_LIB)
  target_link_libraries(carnet-server PRIVATE ${GRPCPP_REFLECTION_LIB})
  target_compile_definitions(carnet-server PRIVATE CARNET_WITH_REFLECTION=1)
endif()

add_executable(carnet-bench carnet/bench/carnet_bench.cc)
target_link_libraries(carnet-bench PRIVATE carnet)

# ---------------------------------------------------------------- pouls
if(MARTINET_BUILD_POULS)
  pkg_check_modules(RDKAFKA REQUIRED IMPORTED_TARGET rdkafka++)

  add_library(pouls_core STATIC
    pouls/src/window.cc
    pouls/src/breaker.cc)
  target_include_directories(pouls_core PUBLIC pouls/include)
  target_link_libraries(pouls_core PUBLIC martinet_proto)

  add_executable(pouls pouls/src/main.cc pouls/src/kafka.cc pouls/src/health_server.cc)
  target_link_libraries(pouls PRIVATE pouls_core PkgConfig::RDKAFKA ${GRPCPP_LIB})
endif()

# ---------------------------------------------------------------- tests
if(MARTINET_BUILD_TESTS)
  find_package(GTest REQUIRED)
  enable_testing()
  include(GoogleTest)

  add_executable(carnet_test carnet/tests/carnet_test.cc)
  target_link_libraries(carnet_test PRIVATE carnet GTest::gtest_main)
  gtest_discover_tests(carnet_test DISCOVERY_TIMEOUT 30)

  if(MARTINET_BUILD_POULS)
    add_executable(pouls_test pouls/tests/pouls_test.cc)
    target_link_libraries(pouls_test PRIVATE pouls_core GTest::gtest_main)
    gtest_discover_tests(pouls_test DISCOVERY_TIMEOUT 30)
  endif()
endif()
