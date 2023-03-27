#ifndef STORAGE_LEVELDB_UTIL_TIMER_H_
#define STORAGE_LEVELDB_UTIL_TIMER_H_
#include <chrono>

namespace leveldb {

template <typename R, typename P = std::ratio<1>>
class Timer {
 public:
    void Start() {
        time_ = Clock::now();
    }

    R End() {
        Duration span;
        Clock::time_point t = Clock::now();
        span = std::chrono::duration_cast<Duration>(t - time_);
        return span.count();
    }

 private:
    using Duration = std::chrono::duration<R, P>;
    using Clock = std::chrono::high_resolution_clock;

    Clock::time_point time_;
};
}



#endif