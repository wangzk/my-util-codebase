#ifndef TIMER_H
#define TIMER_H


#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>

using namespace std;

class Timer {
private:

    timeval startTime;

public:

    void start(){
        gettimeofday(&startTime, NULL);
    }

    double stop(){
        timeval endTime;
        long seconds, useconds;
        double duration;

        gettimeofday(&endTime, NULL);

        seconds  = endTime.tv_sec  - startTime.tv_sec;
        useconds = endTime.tv_usec - startTime.tv_usec;

        duration = seconds + useconds/1000000.0;

        return duration;
    }

    static void printTime(double duration){
        printf("%5.6f seconds\n", duration);
    }
};

#endif // TIMER_H
