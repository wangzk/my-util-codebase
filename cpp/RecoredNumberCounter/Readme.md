# Record Processing Counter
## Movitation
When the program are processing a group of records, you may want to know how many records the program have already processed and how fast the processing speed is.

You may write code to count the records the program has processed and calculate the speed by hand. But writing those util code is awkward.

Therefore, you can use this `PASARecordProcessCounter` util class. It will automatically count the record and print a debug line to report the progress.

## Usage
1. Include the `record_process_counter.h` in your source code.
2. New a counter in your program before processing a group of records.
3. Call `counter.counter_add_one();` method every time a record has been processed.
4. The counter will automatically print the progress info on the screen.

Here is a piece of example code:
    
```cpp
    #include <stdio.h> // to provide stderr FILE.
    #include "record_process_counter.h" // Record the .h file of the counter
    
    // ...... some code before processing records ....
    
    // Create a counter. Print the debug info every 1000 records to the stderr.
    PASARecordProcessCounter counter(1000, stderr); 
    
    for(....) { // for each record processing
        // ... process a record
        // ...
        counter.counter_add_one(); // +1
    }
    counter.counter_done(); // Done!
        
```