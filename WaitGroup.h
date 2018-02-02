#pragma once
#include "coroutine.h"

class WaitGroup {
public:
    WaitGroup()
        : count(new std::atomic<uint64_t>(0))
    {
    }
    WaitGroup(uint64_t cnt)
        : count(new std::atomic<uint64_t>(cnt))
    {
    }
    WaitGroup(const WaitGroup& wg)
        : count(wg.count)
        , ch(wg.ch)
    {
    }
    void Add()
    {
        ++(*count);
    }
    void Done()
    {
        ch << true;
    }
    void Wait()
    {
        while (*count != 0) {
            bool tmp;
            ch >> tmp;
            --(*count);
        }
    }
    void Wait() const
    {
        while (*count != 0) {
            bool tmp;
            ch >> tmp;
            --(*count);
        }
    }

private:
    std::shared_ptr<std::atomic<uint64_t>> count;
    co_chan<bool> ch;
};