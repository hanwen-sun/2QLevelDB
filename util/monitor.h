#ifndef STORAGE_LEVELDB_UTIL_MONITOR_H_
#define STORAGE_LEVELDB_UTIL_MONITOR_H_

#include <cstdint>
#include <cstdio>
#include <string>
#include <atomic>
#include <iostream>

namespace leveldb {

enum Operation_ {
  WAIT_IMM = 0,
  WRITE_LOG,
  INSERT_MEM,
  TOTAL_TIME,
  MAXOPTYPE
};

class Monitors {
 public:
    virtual void Report(Operation_ op, uint64_t latency) = 0;
    virtual void GenerateReport() = 0;
    virtual void Reset() = 0;
};

class LatencyMonitors : public Monitors {
  void Report(Operation_ op, uint64_t latency) override;
  void GenerateReport() override;
  void Reset() override;

 private:
  // std::atomic<uint> count_[MAXOPTYPE];
  std::atomic<uint64_t> latency_sum_[MAXOPTYPE];
};

inline void LatencyMonitors::Report(Operation_ op, uint64_t latency) {
    // std::cout << "report!" << std::endl;
    latency_sum_[op].fetch_add(latency, std::memory_order_relaxed);
    // latency_sum_[TOTAL_TIME].fetch_add(latency, std::memory_order_relaxed);
}

inline void LatencyMonitors::GenerateReport() {
    // fprintf(stderr, "%s", "monitor generate report!\n");
    for(int i = 0; i < MAXOPTYPE; i++) {
        uint64_t cnt = latency_sum_[i].load(std::memory_order_relaxed);
        //fprintf(stderr, "%zu\n", cnt);
        if(cnt == 0)
            continue;
        switch(i) {
            case 0:
                fprintf(stderr, "WAIT_TIME Latency: %zu millis\n", cnt);
                // std::cout << "WAIT_TIME Latency: " << cnt << "millis" << std::endl;
                break;
            case 1:
                fprintf(stderr, "LOG_WRITE Latency: %zu millis\n", cnt);
                //std::cout << "WRITE_LOG Latency: " << cnt << "millis" << std::endl;
                break;
            case 2:
                fprintf(stderr, "INSERT_MEM Latency: %zu millis\n", cnt);
                //std::cout << "INSERT_MEM Latency: " << cnt << "millis" << std::endl;
                break;
            case 3:
                //std::cout << "TOTAL Latency: " << cnt << "millis" << std::endl;
                fprintf(stderr, "TOTAL_TIME Latency: %zu millis\n", cnt);
        }
    }
}

inline void LatencyMonitors::Reset() {
    std::fill(std::begin(latency_sum_), std::end(latency_sum_), 0);
}

}

#endif