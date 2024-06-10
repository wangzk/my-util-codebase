#ifndef TIMER_H
#define TIMER_H

#include <chrono>

class Timer
{
private:
    std::chrono::time_point<std::chrono::system_clock> startTime;

public:

    void start()
    {
        this->startTime = std::chrono::system_clock::now();
    }
    /* return the elapsed seconds since start() */
    double stop()
    {
        auto endTime = std::chrono::system_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(endTime - this->startTime);
        return duration.count() / 1e6;
    }
};

#endif // TIMER_H
