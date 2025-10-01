/*
 * Hash Join Benchmark - C++ version matching Scala implementation
 * Fixed 64MB probe table, varying build table sizes (4KB - 256MB)
 */

#include <iostream>
#include <fstream>
#include <iomanip>
#include "velox/common/base/SelectivityInfo.h"
#include "velox/common/memory/MmapAllocator.h"
#include "velox/exec/HashTable.h"
#include "velox/exec/VectorHasher.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/vector/FlatVector.h"

#include <folly/Benchmark.h>
#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/init/Init.h>
#include <memory>

#include "velox/common/process/Profiler.h"

DEFINE_bool(profile, false, "Generate perf profiles and memory stats");

using namespace facebook::velox;
using namespace facebook::velox::exec;

namespace {

class HashTableBenchmark {
 public:
  HashTableBenchmark() : pool_(memory::memoryManager()->addLeafPool()) {}

  struct BenchmarkResult {
    int64_t targetHashTableSize;
    int64_t buildTableSize;
    int32_t numBuildTuples;
    int64_t actualHashTableSize;
    double buildTimeMs;
    double probeTimeMs;
    BaseHashTable::HashMode hashMode;
    int32_t numDistinct;
  };

  // Calculate build table size for target hash table size (matching Scala logic)
  int32_t calculateBuildTableSize(int64_t targetHashTableSize) {
    const double hashTableFillRate = 0.75;
    const int32_t tupleSize = 8;  // 4 bytes key + 4 bytes value
    const int32_t hashTableEntryOverhead = 16;  // overhead per entry
    
    int32_t bytesPerHashEntry = tupleSize + hashTableEntryOverhead;
    int32_t maxEntries = static_cast<int32_t>(
        (targetHashTableSize * hashTableFillRate) / bytesPerHashEntry);
    
    return maxEntries;
  }

  // Create build table with specified number of tuples
  void makeBuildTable(int32_t numTuples, int32_t keyRange) {
    buildBatches_.clear();
    
    // Create batches of 10000 tuples each
    const int32_t batchSize = 10000;
    int32_t remainingTuples = numTuples;
    int32_t sequence = 0;
    
    while (remainingTuples > 0) {
      int32_t currentBatchSize = std::min(batchSize, remainingTuples);
      auto batch = makeBatch(currentBatchSize, sequence, keyRange);
      buildBatches_.push_back(batch);
      remainingTuples -= currentBatchSize;
      sequence += currentBatchSize;
    }
  }

  // Create probe table (fixed at 64MB tuples)
  void makeProbeTable(int32_t numTuples, int32_t keyRange) {
    probeBatches_.clear();
    
    const int32_t batchSize = 10000;
    int32_t remainingTuples = numTuples;
    int32_t sequence = 0;
    
    while (remainingTuples > 0) {
      int32_t currentBatchSize = std::min(batchSize, remainingTuples);
      auto batch = makeBatch(currentBatchSize, sequence, keyRange);
      probeBatches_.push_back(batch);
      remainingTuples -= currentBatchSize;
      sequence += currentBatchSize;
    }
  }

  // Original copyVectorsToTable method from document 2
  void copyVectorsToTable(
      const std::vector<RowVectorPtr>& batches,
      BaseHashTable* table) {
    
    int32_t batchSize = batches[0]->size();
    raw_vector<uint64_t> dummy(batchSize);
    auto rowContainer = table->rows();
    auto& hashers = table->hashers();
    auto numKeys = hashers.size();
    std::vector<std::vector<DecodedVector>> decoded;
    SelectivityVector rows(batchSize);
    
    for (auto& batch : batches) {
      decoded.emplace_back(batch->childrenSize());
      auto& decoders = decoded.back();
      for (auto i = 0; i < batch->childrenSize(); ++i) {
        decoders[i].decode(*batch->childAt(i), rows);
        if (i < numKeys) {
          auto hasher = table->hashers()[i].get();
          hasher->decode(*batch->childAt(i), rows);
          if (table->hashMode() != BaseHashTable::HashMode::kHash &&
              hasher->mayUseValueIds()) {
            hasher->computeValueIds(rows, dummy);
          }
        }
      }
    }

    auto size = batchSize * batches.size();
    auto powerOfTwo = bits::nextPowerOfTwo(size);
    int32_t mask = powerOfTwo - 1;
    int32_t position = 0;
    int32_t delta = 1;
    auto nextOffset = rowContainer->nextOffset();

    for (auto count = 0; count < powerOfTwo; ++count) {
      if (position < size) {
        char* newRow = rowContainer->newRow();
        auto batchIndex = position / batchSize;
        auto rowIndex = position % batchSize;
        if (nextOffset) {
          *reinterpret_cast<char**>(newRow + nextOffset) = nullptr;
        }
        for (auto i = 0; i < batches[batchIndex]->type()->size(); ++i) {
          rowContainer->store(decoded[batchIndex][i], rowIndex, newRow, i);
        }
      }
      position = (position + delta) & mask;
      ++delta;
    }
  }

  // Build hash table and measure time
  BenchmarkResult buildAndProbe(
      int64_t targetHashTableSize,
      int32_t numBuildTuples,
      int32_t keyRange) {
    
    BenchmarkResult result;
    result.targetHashTableSize = targetHashTableSize;
    result.numBuildTuples = numBuildTuples;
    result.buildTableSize = numBuildTuples * 8;  // 8 bytes per tuple
    
    // Build phase
    std::vector<TypePtr> dependentTypes;
    TypePtr buildType = ROW({"customer_id"}, {BIGINT()});
    
    auto startBuild = std::chrono::high_resolution_clock::now();
    
    std::vector<std::unique_ptr<VectorHasher>> keyHashers;
    keyHashers.emplace_back(std::make_unique<VectorHasher>(
        buildType->childAt(0), 0));
    
    auto table = HashTable<true>::createForJoin(
        std::move(keyHashers),
        dependentTypes,
        true,
        false,
        1'000,
        pool_.get());
    
    // Use the robust copyVectorsToTable method
    copyVectorsToTable(buildBatches_, table.get());
    
    table->prepareJoinTable(
        {},
        BaseHashTable::kNoSpillInputStartPartitionBit,
        executor_.get());
    
    auto endBuild = std::chrono::high_resolution_clock::now();
    result.buildTimeMs = std::chrono::duration<double, std::milli>(
        endBuild - startBuild).count();
    result.actualHashTableSize = pool_->usedBytes();
    result.hashMode = table->hashMode();
    result.numDistinct = table->numDistinct();
    
    // Probe phase
    auto startProbe = std::chrono::high_resolution_clock::now();
    
    int32_t totalHits = 0;
    int32_t totalProbed = 0;
    
    for (auto& batch : probeBatches_) {
      int32_t batchSize = batch->size();
      
      auto lookup = std::make_unique<HashLookup>(table->hashers());
      lookup->reset(batchSize);
      
      SelectivityVector probeRows(batchSize);
      probeRows.setAll();
      
      // Decode and hash probe keys
      auto& hasher = table->hashers()[0];
      VectorHasher::ScratchMemory scratchMemory;
      
      if (table->hashMode() != BaseHashTable::HashMode::kHash) {
        hasher->lookupValueIds(
            *batch->childAt(0), probeRows, scratchMemory, lookup->hashes);
      } else {
        hasher->decode(*batch->childAt(0), probeRows);
        hasher->hash(probeRows, false, lookup->hashes);
      }
      
      // Set up rows to probe
      lookup->rows.clear();
      if (probeRows.isAllSelected()) {
        lookup->rows.resize(probeRows.size());
        std::iota(lookup->rows.begin(), lookup->rows.end(), 0);
      } else {
        constexpr int32_t kPadding = simd::kPadding / sizeof(int32_t);
        lookup->rows.resize(bits::roundUp(probeRows.size() + kPadding, kPadding));
        auto numRows = simd::indicesOfSetBits(
            probeRows.asRange().bits(), 0, batchSize, lookup->rows.data());
        lookup->rows.resize(numRows);
      }
      
      if (!lookup->rows.empty()) {
        totalProbed += lookup->rows.size();
        
        // Perform join probe
        table->joinProbe(*lookup);
        
        // Count hits
        for (auto i = 0; i < lookup->rows.size(); ++i) {
          auto key = lookup->rows[i];
          if (lookup->hits[key] != nullptr) {
            totalHits++;
          }
        }
      }
    }
    
    auto endProbe = std::chrono::high_resolution_clock::now();
    result.probeTimeMs = std::chrono::duration<double, std::milli>(
        endProbe - startProbe).count();
    
    std::cout << "  Probed: " << totalProbed << " rows, Hits: " << totalHits 
              << " (" << (100.0 * totalHits / totalProbed) << "%)" << std::endl;
    
    topTable_ = std::move(table);
    
    return result;
  }

 private:
  RowVectorPtr makeBatch(int32_t size, int32_t startSequence, int32_t keyRange) {
    auto keys = BaseVector::create<FlatVector<int64_t>>(
        BIGINT(), size, pool_.get());
    
    // Generate random keys within keyRange (for 20% selectivity)
    folly::Random::DefaultGenerator rng;
    rng.seed(startSequence);
    
    for (auto i = 0; i < size; ++i) {
      keys->set(i, folly::Random::rand32(rng) % keyRange);
    }
    
    std::vector<VectorPtr> children = {keys};
    TypePtr rowType = ROW({"customer_id"}, {BIGINT()});
    
    return std::make_shared<RowVector>(
        pool_.get(), rowType, nullptr, size, std::move(children));
  }

  std::shared_ptr<memory::MemoryPool> pool_;
  std::vector<RowVectorPtr> buildBatches_;
  std::vector<RowVectorPtr> probeBatches_;
  std::unique_ptr<HashTable<true>> topTable_;
  std::shared_ptr<folly::Executor> executor_{
      std::make_shared<folly::CPUThreadPoolExecutor>(
          std::thread::hardware_concurrency())};
};

} // namespace

int main(int argc, char** argv) {
  folly::Init init{&argc, &argv};
  memory::MemoryManagerOptions options;
  options.useMmapAllocator = true;
  options.allocatorCapacity = 10UL << 30;
  options.useMmapArena = true;
  options.mmapArenaCapacityRatio = 1;
  memory::MemoryManager::initialize(options);
  
  auto bm = std::make_unique<HashTableBenchmark>();
  
  // Target hash table sizes (4KB to 256MB)
  std::vector<int64_t> targetHashTableSizes = {
    4 * 1024, 8 * 1024, 16 * 1024, 32 * 1024, 64 * 1024, 
    128 * 1024, 256 * 1024, 512 * 1024, 1024 * 1024,
    2 * 1024 * 1024, 4 * 1024 * 1024, 8 * 1024 * 1024,
    16 * 1024 * 1024, 32 * 1024 * 1024, 64 * 1024 * 1024,
    128 * 1024 * 1024, 256 * 1024 * 1024
  };
  
  // Fixed probe table size: 64MB tuples
  const int32_t probeTableSize = 64 * 1024 * 1024;
  const int32_t probeKeyRange = probeTableSize / 5;  // 20% selectivity
  
  std::cout << "Generating probe table (64M tuples)..." << std::endl;
  bm->makeProbeTable(probeTableSize, probeKeyRange);
  std::cout << "Probe table generation complete." << std::endl;
  
  // Open output file
  std::ofstream outFile("hashTable.txt");
  std::vector<HashTableBenchmark::BenchmarkResult> results;
  
  std::cout << "\n=== HASH TABLE JOIN BENCHMARK ===" << std::endl;
  std::cout << "Probe table: 64M tuples" << std::endl;
  std::cout << "Build table sizes: 4KB to 256MB" << std::endl;
  std::cout << "Selectivity: 20%\n" << std::endl;
  
  // Run benchmarks for each target size
  for (auto targetSize : targetHashTableSizes) {
    int32_t numBuildTuples = bm->calculateBuildTableSize(targetSize);
    
    std::cout << std::string(70, '=') << std::endl;
    std::cout << "TARGET HASH TABLE SIZE: " << (targetSize / 1024) << " KB" << std::endl;
    std::cout << "Build Table Tuples: " << numBuildTuples << std::endl;
    
    // Generate build table
    bm->makeBuildTable(numBuildTuples, probeKeyRange);
    
    // Run benchmark
    auto result = bm->buildAndProbe(targetSize, numBuildTuples, probeKeyRange);
    results.push_back(result);
    
    // Write to file
    outFile << std::string(70, '=') << "\n";
    outFile << "TARGET HASH TABLE SIZE: " << (targetSize / 1024) 
            << " KB (" << targetSize << " bytes)\n";
    outFile << "Build Table Tuples: " << numBuildTuples << "\n";
    outFile << "Build Table Size: " << (result.buildTableSize / 1024) 
            << " KB (" << result.buildTableSize << " bytes)\n";
    outFile << "Build Time: " << std::fixed << std::setprecision(3) 
            << result.buildTimeMs << " ms\n";
    outFile << "Probe Time: " << std::fixed << std::setprecision(3) 
            << result.probeTimeMs << " ms\n";
    outFile << "Actual Hash Table Size: " << (result.actualHashTableSize / 1024) 
            << " KB\n\n";
    
    // Console output
    std::cout << "Build Time: " << std::fixed << std::setprecision(3) 
              << result.buildTimeMs << " ms" << std::endl;
    std::cout << "Probe Time: " << std::fixed << std::setprecision(3) 
              << result.probeTimeMs << " ms" << std::endl;
    std::cout << "Actual Memory: " << (result.actualHashTableSize / 1024) 
              << " KB" << std::endl;
  }
  
  // Summary table
  std::cout << "\n\n=== SUMMARY ===" << std::endl;
  std::cout << std::setw(15) << "Target Size" 
            << std::setw(15) << "Build Tuples"
            << std::setw(15) << "Build Time(ms)"
            << std::setw(15) << "Probe Time(ms)"
            << std::setw(15) << "Actual Mem(KB)" << std::endl;
  std::cout << std::string(75, '-') << std::endl;
  
  for (const auto& r : results) {
    std::cout << std::setw(13) << (r.targetHashTableSize / 1024) << " KB"
              << std::setw(15) << r.numBuildTuples
              << std::setw(15) << std::fixed << std::setprecision(3) << r.buildTimeMs
              << std::setw(15) << std::fixed << std::setprecision(3) << r.probeTimeMs
              << std::setw(15) << (r.actualHashTableSize / 1024) << std::endl;
  }
  
  outFile.close();
  std::cout << "\nResults written to hashTable.txt" << std::endl;
  
  return 0;
}
