#ifndef RECORDPROCESSCOUNTERUTIL_H
#define RECORDPROCESSCOUNTERUTIL_H

/**
 * Version: 1.0 
 * Date: 2016.7.22
 */

#include <time.h>
#include <stdio.h>

/*
 * ====== PASA Util ========
 * Record Process Counter.
 * This counter will count the record number and their processing speed automatically.
 */
class PASARecordProcessCounter {
    long count; // the record count
    clock_t last_clock_tick, start_clock_tick; // to calculate the speed
    long process_report_interval; // print the debug info every `process_report_interval` records.
    FILE* output_file; // write the debug info to ?
    bool done; // whether all the records have processed.

public:
    /**
     * @param interval  Print the progress every `interval` records.
     * @param out Print the debug info to which FILE (the default is stderr).
     */ 
    PASARecordProcessCounter(long interval, FILE* out = NULL):count(0),last_clock_tick(clock()), process_report_interval(interval), output_file(stderr) {
        start_clock_tick = clock();
        done = false;
    }
    ~PASARecordProcessCounter() {
        if (!done)
            counter_done();
    }


    /**
     * Call this method every time a record has been processed.
     */
    void counter_add_one() {
        count++;
        if (count % process_report_interval == 0)
            print_current_process();
    }

    /**
     * Print current progress info. This method will be called every `process_report_interval` records has been processed.
     */
    void print_current_process() {
        float speed = (float)process_report_interval / ((clock() - last_clock_tick)/(float)CLOCKS_PER_SEC);
        last_clock_tick = clock();
        float elasped_time = (last_clock_tick - start_clock_tick)/(float)CLOCKS_PER_SEC;
        if (output_file == NULL)
            output_file = stderr;
        fprintf(output_file, "\r[%ld, %f record/s, elasped %f seconds.]", count, speed, elasped_time);
    }

    /**
     * The processing has been done! It will print a done info.
     */
    void counter_done() {
        print_current_process();
        fprintf(output_file,"...Done!\n");
        done = true;
    }

};
#endif // RECORDPROCESSCOUNTERUTIL_H
