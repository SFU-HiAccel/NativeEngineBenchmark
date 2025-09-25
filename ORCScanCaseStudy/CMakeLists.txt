cmake_minimum_required(VERSION 3.23)

project(orc_benchmark LANGUAGES CXX CUDA)

# Set C++ standard
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Set CUDA standard
set(CMAKE_CUDA_STANDARD 17)
set(CMAKE_CUDA_STANDARD_REQUIRED ON)

# Find required packages
find_package(CUDAToolkit REQUIRED)
find_package(cudf REQUIRED)
find_package(rmm REQUIRED)

# Create executable
add_executable(orc_benchmark orc_benchmark.cpp)

# Set target properties
set_target_properties(orc_benchmark PROPERTIES
    CUDA_SEPARABLE_COMPILATION ON
    CXX_STANDARD 17
    CXX_STANDARD_REQUIRED ON
)

# Link libraries
target_link_libraries(orc_benchmark
    PRIVATE
    cudf::cudf
    rmm::rmm
    CUDA::cudart
)

# Set CUDA compiler flags to allow unsupported compiler
set(CMAKE_CUDA_FLAGS "${CMAKE_CUDA_FLAGS} -allow-unsupported-compiler")

# Compiler-specific options
target_compile_options(orc_benchmark PRIVATE
    $<$<COMPILE_LANGUAGE:CXX>:-Wall -Wextra -O3>
    $<$<COMPILE_LANGUAGE:CUDA>:-O3 -allow-unsupported-compiler>
)
