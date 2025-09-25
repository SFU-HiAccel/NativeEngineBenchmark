#include <cudf/io/orc.hpp>
#include <cudf/table/table.hpp>
#include <cudf/column/column.hpp>
#include <cudf/reduction.hpp>
#include <cudf/aggregation.hpp>
#include <cudf/types.hpp>
#include <cudf/unary.hpp>
#include <rmm/mr/device/pool_memory_resource.hpp>
#include <rmm/mr/device/cuda_memory_resource.hpp>
#include <chrono>
#include <iostream>
#include <memory>
#include <vector>
#include <iomanip>
#include <cuda_runtime.h>
#include <filesystem>

// Profiling includes (conditionally compiled)
#ifdef ENABLE_PROFILING
#include <nvtx3/nvToolsExt.h>
#include <cuda_profiler_api.h>
#define NVTX_RANGE_PUSH(name) nvtxRangePushA(name)
#define NVTX_RANGE_POP() nvtxRangePop()
#define CUDA_PROFILER_START() cudaProfilerStart()
#define CUDA_PROFILER_STOP() cudaProfilerStop()
#else
#define NVTX_RANGE_PUSH(name) do {} while(0)
#define NVTX_RANGE_POP() do {} while(0)
#define CUDA_PROFILER_START() do {} while(0)
#define CUDA_PROFILER_STOP() do {} while(0)
#endif

class ORCStreamReader {
private:
    std::string filename_;
    size_t chunk_size_;
    std::vector<std::string> columns_to_read_;
    
    // CUDA events for precise timing
    cudaEvent_t io_start_, io_end_;
    cudaEvent_t gpu_start_, gpu_end_;
    cudaEvent_t transfer_start_, transfer_end_;
    
    // Helper function to get file size
    size_t get_file_size() const {
        try {
            return std::filesystem::file_size(filename_);
        } catch (const std::exception& e) {
            std::cerr << "Warning: Could not get file size: " << e.what() << std::endl;
            return 0;
        }
    }
    
    // Helper function to calculate uncompressed data size
    size_t calculate_uncompressed_size(const std::unique_ptr<cudf::table>& table) const {
        size_t total_size = 0;
        
        for (int i = 0; i < table->num_columns(); ++i) {
            const auto& column = table->get_column(i);
            
            // Calculate memory footprint based on data type
            switch (column.type().id()) {
                case cudf::type_id::INT8:
                case cudf::type_id::UINT8:
                    total_size += column.size() * 1;
                    break;
                case cudf::type_id::INT16:
                case cudf::type_id::UINT16:
                    total_size += column.size() * 2;
                    break;
                case cudf::type_id::INT32:
                case cudf::type_id::UINT32:
                case cudf::type_id::FLOAT32:
                    total_size += column.size() * 4;
                    break;
                case cudf::type_id::INT64:
                case cudf::type_id::UINT64:
                case cudf::type_id::FLOAT64:
                case cudf::type_id::TIMESTAMP_DAYS:
                case cudf::type_id::TIMESTAMP_SECONDS:
                case cudf::type_id::TIMESTAMP_MILLISECONDS:
                case cudf::type_id::TIMESTAMP_MICROSECONDS:
                case cudf::type_id::TIMESTAMP_NANOSECONDS:
                    total_size += column.size() * 8;
                    break;
                case cudf::type_id::STRING:
                    total_size += column.size() * 20; // Assume average 20 bytes per string
                    break;
                case cudf::type_id::BOOL8:
                    total_size += column.size() * 1;
                    break;
                default:
                    total_size += column.size() * 8;
                    break;
            }
            
            // Add null mask overhead if column has nulls
            if (column.has_nulls()) {
                total_size += (column.size() + 7) / 8; // Null mask bits
            }
        }
        
        return total_size;
    }
    
    float get_cuda_elapsed_time(cudaEvent_t start, cudaEvent_t end) {
        float elapsed_time;
        cudaEventElapsedTime(&elapsed_time, start, end);
        return elapsed_time;
    }
    
public:
    ORCStreamReader(const std::string& filename, size_t chunk_size = 1000000) 
        : filename_(filename), chunk_size_(chunk_size) {
        
        // Create CUDA events for precise timing
        cudaEventCreate(&io_start_);
        cudaEventCreate(&io_end_);
        cudaEventCreate(&gpu_start_);
        cudaEventCreate(&gpu_end_);
        cudaEventCreate(&transfer_start_);
        cudaEventCreate(&transfer_end_);
    }
    
    ~ORCStreamReader() {
        // Cleanup CUDA events
        cudaEventDestroy(io_start_);
        cudaEventDestroy(io_end_);
        cudaEventDestroy(gpu_start_);
        cudaEventDestroy(gpu_end_);
        cudaEventDestroy(transfer_start_);
        cudaEventDestroy(transfer_end_);
    }
    
    void set_columns(const std::vector<std::string>& columns) {
        columns_to_read_ = columns;
    }
    
    void benchmark_streaming_read() {
        // Start profiler
        CUDA_PROFILER_START();
        NVTX_RANGE_PUSH("ORC_Benchmark_Complete");
        
        std::cout << "=== ORC Streaming Read Benchmark (Profiling Enabled) ===" << std::endl;
        std::cout << "File: " << filename_ << std::endl;
        std::cout << "Chunk size: " << chunk_size_ << " rows" << std::endl;
        
        // Get compressed file size
        size_t compressed_file_size = get_file_size();
        std::cout << "Compressed file size: " << compressed_file_size / (1024.0 * 1024.0) << " MB" << std::endl;
        
        try {
            NVTX_RANGE_PUSH("Initialization");
            // Read first chunk to get basic info and calculate uncompressed size estimation
            auto first_chunk_options = cudf::io::orc_reader_options::builder(
                cudf::io::source_info{filename_})
                .skip_rows(0)
                .num_rows(std::min(chunk_size_, static_cast<size_t>(1000)));
            
            auto first_chunk = cudf::io::read_orc(first_chunk_options.build());
            size_t num_columns = first_chunk.tbl->num_columns();
            NVTX_RANGE_POP(); // End Initialization
            
            std::cout << "Detected columns: " << num_columns << std::endl;
            
            // Print column types for better understanding
            std::cout << "Column types: ";
            for (int i = 0; i < num_columns && i < 5; ++i) { // Show first 5 column types
                std::cout << cudf::type_to_name(first_chunk.tbl->get_column(i).type()) << " ";
            }
            if (num_columns > 5) std::cout << "...";
            std::cout << std::endl;
            
            std::cout << std::string(100, '=') << std::endl;
            
            // Enhanced header with detailed timing breakdown
            std::cout << std::setw(6) << "Chunk" 
                     << std::setw(10) << "Rows" 
                     << std::setw(12) << "File I/O(ms)" 
                     << std::setw(14) << "PCIe+Xfer(ms)"
                     << std::setw(14) << "GPU Proc(ms)"
                     << std::setw(12) << "Total(ms)"
                     << std::setw(15) << "Throughput(MB/s)"
                     << std::setw(15) << "Compression" << std::endl;
            std::cout << std::string(115, '-') << std::endl;
            
            auto start_total = std::chrono::high_resolution_clock::now();
            float total_io_time = 0.0;
            float total_transfer_time = 0.0;
            float total_gpu_time = 0.0;
            size_t chunks_processed = 0;
            size_t total_rows_processed = 0;
            size_t total_uncompressed_size = 0;
            
            // Process chunks until we can't read anymore (EOF detection)
            size_t offset = 0;
            bool has_more_data = true;
            
            NVTX_RANGE_PUSH("Main_Processing_Loop");
            while (has_more_data) {
                try {
                    auto chunk_result = process_chunk_detailed(offset, chunk_size_, chunks_processed + 1);
                    
                    if (chunk_result.rows_read == 0) {
                        break; // No more data to read
                    }
                    
                    total_io_time += chunk_result.io_time;
                    total_transfer_time += chunk_result.transfer_time;
                    total_gpu_time += chunk_result.gpu_time;
                    total_rows_processed += chunk_result.rows_read;
                    total_uncompressed_size += chunk_result.uncompressed_size;
                    chunks_processed++;
                    
                    // Calculate throughput based on uncompressed data
                    double chunk_size_mb = chunk_result.uncompressed_size / (1024.0 * 1024.0);
                    double total_chunk_time = chunk_result.io_time + chunk_result.transfer_time + chunk_result.gpu_time;
                    double throughput = chunk_size_mb / (total_chunk_time / 1000.0);
                    
                    // Calculate local compression ratio
                    double local_compression = 0.0;
                    if (chunk_result.uncompressed_size > 0) {
                        // Estimate compressed chunk size
                        double estimated_compressed = (chunk_result.uncompressed_size * compressed_file_size) / 
                                                    static_cast<double>(total_uncompressed_size > 0 ? total_uncompressed_size : chunk_result.uncompressed_size);
                        local_compression = chunk_result.uncompressed_size / estimated_compressed;
                    }
                    
                    std::cout << std::setw(6) << chunks_processed
                             << std::setw(10) << chunk_result.rows_read
                             << std::setw(12) << std::fixed << std::setprecision(2) << chunk_result.io_time
                             << std::setw(14) << std::fixed << std::setprecision(2) << chunk_result.transfer_time
                             << std::setw(14) << std::fixed << std::setprecision(2) << chunk_result.gpu_time
                             << std::setw(12) << std::fixed << std::setprecision(2) << total_chunk_time
                             << std::setw(15) << std::fixed << std::setprecision(1) << throughput
                             << std::setw(15) << std::fixed << std::setprecision(1) << local_compression << ":1" << std::endl;
                    
                    offset += chunk_size_;
                    
                    // Stop if we read fewer rows than requested (reached EOF)
                    if (chunk_result.rows_read < chunk_size_) {
                        has_more_data = false;
                    }
                    
                } catch (const std::exception& e) {
                    std::cerr << "Reached end of file or error: " << e.what() << std::endl;
                    break;
                }
            }
            NVTX_RANGE_POP(); // End Main_Processing_Loop
            
            auto end_total = std::chrono::high_resolution_clock::now();
            auto total_wall_time = std::chrono::duration_cast<std::chrono::milliseconds>(
                end_total - start_total).count();
            
            // Calculate compression ratio
            double compression_ratio = 0.0;
            double compression_percentage = 0.0;
            if (total_uncompressed_size > 0 && compressed_file_size > 0) {
                compression_ratio = static_cast<double>(total_uncompressed_size) / compressed_file_size;
                compression_percentage = (1.0 - static_cast<double>(compressed_file_size) / total_uncompressed_size) * 100.0;
            }
            
            // Detailed performance breakdown
            std::cout << std::string(115, '=') << std::endl;
            std::cout << "DETAILED PERFORMANCE BREAKDOWN:" << std::endl;
            std::cout << "Chunks processed: " << chunks_processed << std::endl;
            std::cout << "Total rows processed: " << total_rows_processed << std::endl;
            std::cout << "File I/O time: " << std::fixed << std::setprecision(2) 
                     << total_io_time << " ms (" << (total_io_time/total_wall_time)*100 << "%)" << std::endl;
            std::cout << "PCIe transfer time: " << std::fixed << std::setprecision(2) 
                     << total_transfer_time << " ms (" << (total_transfer_time/total_wall_time)*100 << "%)" << std::endl;
            std::cout << "GPU processing time: " << std::fixed << std::setprecision(2) 
                     << total_gpu_time << " ms (" << (total_gpu_time/total_wall_time)*100 << "%)" << std::endl;
            std::cout << "Total wall clock time: " << total_wall_time << " ms" << std::endl;
            
            // Bottleneck analysis
            std::cout << std::string(60, '-') << std::endl;
            std::cout << "BOTTLENECK ANALYSIS:" << std::endl;
            float max_time = std::max({total_io_time, total_transfer_time, total_gpu_time});
            if (max_time == total_io_time) {
                std::cout << "Primary bottleneck: File I/O (storage bandwidth)" << std::endl;
                std::cout << "Recommendation: Use faster storage (NVMe SSD)" << std::endl;
            } else if (max_time == total_transfer_time) {
                std::cout << "Primary bottleneck: PCIe transfer bandwidth" << std::endl;
                std::cout << "Recommendation: Use PCIe Gen4/5, reduce data movement" << std::endl;
            } else {
                std::cout << "Primary bottleneck: GPU processing" << std::endl;
                std::cout << "Recommendation: Use faster GPU or optimize algorithms" << std::endl;
            }
            
            // Print compression information
            std::cout << std::string(60, '-') << std::endl;
            std::cout << "COMPRESSION ANALYSIS:" << std::endl;
            std::cout << "Compressed file size: " << std::fixed << std::setprecision(2) 
                     << compressed_file_size / (1024.0 * 1024.0) << " MB" << std::endl;
            std::cout << "Estimated uncompressed size: " << std::fixed << std::setprecision(2) 
                     << total_uncompressed_size / (1024.0 * 1024.0) << " MB" << std::endl;
            std::cout << "Compression ratio: " << std::fixed << std::setprecision(2) 
                     << compression_ratio << ":1" << std::endl;
            std::cout << "Space saved: " << std::fixed << std::setprecision(1) 
                     << compression_percentage << "%" << std::endl;
            
        } catch (const std::exception& e) {
            std::cerr << "Error during benchmark: " << e.what() << std::endl;
        }
        
        NVTX_RANGE_POP(); // End ORC_Benchmark_Complete
        CUDA_PROFILER_STOP();
    }
    
private:
    struct DetailedChunkResult {
        float io_time;          // File I/O time (reading from storage)
        float transfer_time;    // PCIe transfer time (host to GPU)
        float gpu_time;         // GPU processing time (decompression)
        size_t rows_read;
        size_t uncompressed_size;
    };
    
    DetailedChunkResult process_chunk_detailed(size_t offset, size_t rows_to_read, int chunk_id) {
        DetailedChunkResult result;
        
        // Create unique NVTX range for this chunk
        std::string chunk_name = "Chunk_" + std::to_string(chunk_id) + 
                                "_offset_" + std::to_string(offset);
        NVTX_RANGE_PUSH(chunk_name.c_str());
        
        // Configure chunk reading options
        auto chunk_options = cudf::io::orc_reader_options::builder(
            cudf::io::source_info{filename_})
            .skip_rows(offset)
            .num_rows(rows_to_read);
        
        if (!columns_to_read_.empty()) {
            chunk_options.columns(columns_to_read_);
        }
        
        // Phase 1: File I/O (reading compressed data from storage)
        NVTX_RANGE_PUSH("File_IO_Phase");
        cudaEventRecord(io_start_);
        
        auto table = cudf::io::read_orc(chunk_options.build());
        
        cudaEventRecord(io_end_);
        cudaEventSynchronize(io_end_);
        NVTX_RANGE_POP(); // End File_IO_Phase
        
        // Phase 2: PCIe Transfer (moving data to GPU memory)
        NVTX_RANGE_PUSH("PCIe_Transfer_Phase");
        cudaEventRecord(transfer_start_);
        
        // Force memory operations to be visible
        cudaDeviceSynchronize();
        
        cudaEventRecord(transfer_end_);
        cudaEventSynchronize(transfer_end_);
        NVTX_RANGE_POP(); // End PCIe_Transfer_Phase
        
        // Phase 3: GPU Processing (decompression and data preparation)
        NVTX_RANGE_PUSH("GPU_Processing_Phase");
        cudaEventRecord(gpu_start_);
        
        // Ensure all GPU operations complete
        cudaDeviceSynchronize();
        
        cudaEventRecord(gpu_end_);
        cudaEventSynchronize(gpu_end_);
        NVTX_RANGE_POP(); // End GPU_Processing_Phase
        
        // Calculate results
        result.rows_read = table.tbl->num_rows();
        result.uncompressed_size = calculate_uncompressed_size(table.tbl);
        result.io_time = get_cuda_elapsed_time(io_start_, io_end_);
        result.transfer_time = get_cuda_elapsed_time(transfer_start_, transfer_end_);
        result.gpu_time = get_cuda_elapsed_time(gpu_start_, gpu_end_);
        
        NVTX_RANGE_POP(); // End chunk range
        
        return result;
    }
};

int main(int argc, char** argv) {
    if (argc != 2) {
        std::cerr << "Usage: " << argv[0] << " <orc_file_path>" << std::endl;
        std::cerr << std::endl;
        std::cerr << "For profiling, run with:" << std::endl;
        std::cerr << "  nsys profile --trace=cuda,nvtx --output=profile " << argv[0] << " <file>" << std::endl;
        std::cerr << "  ncu --set full --kernel-regex='.*' " << argv[0] << " <file>" << std::endl;
        return 1;
    }
    
    std::string orc_file = argv[1];
    
    try {
        // Initialize CUDA
        cudaSetDevice(0);
        
        //print_gpu_info();
        auto base_mr = std::make_shared<rmm::mr::cuda_memory_resource>();
        rmm::mr::pool_memory_resource<rmm::mr::cuda_memory_resource> pool_mr{
            base_mr.get(),
            8ULL * 1024 * 1024 * 1024,
            16ULL * 1024 * 1024 * 1024
        };
        rmm::mr::set_current_device_resource(&pool_mr);
        
        ORCStreamReader reader(orc_file, 1024*1024*512);
        
        std::cout << "Starting ORC file benchmark with profiling..." << std::endl;
        std::cout << "File: " << orc_file << std::endl;
        
        #ifdef ENABLE_PROFILING
        std::cout << "Profiling: ENABLED (NVTX markers active)" << std::endl;
        #else
        std::cout << "Profiling: DISABLED (compile with -DENABLE_PROFILING=1)" << std::endl;
        #endif
        std::cout << std::endl;
        
        // Run the main streaming benchmark
        reader.benchmark_streaming_read();
        
        std::cout << "\nBenchmark completed successfully!" << std::endl;
        #ifdef ENABLE_PROFILING
        std::cout << "Check profiler output for detailed PCIe and GPU timing breakdown." << std::endl;
        #endif
        
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }
    
    return 0;
}

//ncu --metrics pcie__read_throughput,pcie__write_throughput,gpu__time_duration ./orc_benchmark

//ncu --set full --kernel-regex=".*orc.*|.*decomp.*|.*memcpy.*" --target-processes all --export orc_kernels ./orc_benchmark /scratch/hpc-prf-haqc/haikai/orctpch/sf100/lineitem.orc
