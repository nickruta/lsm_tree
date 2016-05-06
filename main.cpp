/**
 C++11 - GCC Compiler
 Main.cpp
 This file is used to test the LSM Tree. It contains tests to demonstrate the design and speed of the design using various settings, using the tunable parameters, in order to show the characteristics of the overall system design based on the design trade-offs made.
 @author N. Ruta
 @version 1.0 4/01/16
 */
#include "LsmTree.h"
#include "WorkerQueue.h"

#include <fstream>
#include <iomanip>
#include <ctype.h>
#include <iostream>
#include <cstdlib>
#include <thread>
#include <sstream>
#include <time.h>
#include <ctime>
#include <ctime>
#include <chrono>
#include <iostream>
#include <set>

int main()
{
    // HOW MANY VALUES TO USE FOR TESTING
    long N = 20000;
    long i;

    std::vector<long > data;
    
    // PROCESS ONE MILLION RANDOM VALUES IN A VECTOR FOR TESTING (I ALSO DID LARGER TESTS UP TO 256 MILLION)
    std::ifstream file("one_million.txt");
    
    std::string line;
    // Read one line at a time into the variable line:
    while(std::getline(file, line))
    {
        std::vector<long>   lineData;
        std::stringstream  lineStream(line);
        long value;

        while(lineStream >> value)
        {
            data.push_back(value);
        }
    }
 
    // START THE TIMER
    auto start = std::chrono::steady_clock::now();
    
    //1
    //XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
    //XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
    //XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
    //    //SINGLE THREAD
    //        LsmTree lsm_tree(false, 2, 5, 500000, 2, true, 1 ,1, 2, false); // 20k inserts
    //    
    //        // INSERT 10 VALUES
    //        for (i = 1; i < 11; i++)
    //        {
    //    
    //            dtype x;
    //            x.key = lsm_tree.getKeyCounter();
    //            x.value = i;
    //    
    //            cout << "INSERTING - " << x.value << endl;
    //            lsm_tree.insert_value(x);
    //        }
    //    
    //        // UPDATE 5 VALUES
    //        for (i = 1; i < 11; i++)
    //        {
    //    
    //            dtype new_value;
    //            new_value.key = lsm_tree.getKeyCounter();
    //            new_value.value = i + 50;
    //    
    //            dtype old_value;
    //            old_value.key = lsm_tree.getKeyCounter();
    //            old_value.value = i;
    //    
    //            cout << "UPDATING - " << "OLD VALUE - " << old_value.value << " NEW VALUE - " << new_value.value << endl;
    //            lsm_tree.update_value(old_value, new_value);
    //        }
    //    
    //        // DELETE 1 VALUE
    //            dtype x;
    //            x.key = lsm_tree.getKeyCounter();
    //            x.value = 59;
    //    
    //            cout << "DELETING - " << x.value << endl;
    //            lsm_tree.delete_value(x);
    //    
    //        // READ 10 VALUES
    //        for (i = 1; i < 11; i++)
    //        {
    //    
    //            dtype x;
    //            x.key = lsm_tree.getKeyCounter();
    //            x.value = i;
    //    
    //            lsm_tree.read_value(x);
    //        }
    //    
    //        dtype sixty;
    //        sixty.key = lsm_tree.getKeyCounter();
    //        sixty.value = 60;
    //        lsm_tree.read_value(sixty);
    //XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
    //XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
    //XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
    
    
    
    
    
    //2
    //XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
    //XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
    //XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
    //SINGLE THREAD
    
    //20k INSERTS
    
    //SCENARIO IS LEVELS ARE SMALLER VS LEVELS ARE LARGER
    
    //INSERTS up to c1 - 0.26 seconds
//        LsmTree lsm_tree(false, 2, 5, 500000, 2, true, 1 ,1, 2, false); // 20k inserts
//    
//        for (i = 0; i < N; i++)
//        {
//    
//            long random_x;
//            random_x = data.at(i);
//    
//            dtype x;
//            x.key = lsm_tree.getKeyCounter();
//            x.value = random_x;
//    
//            lsm_tree.insert_value(x);
//        }
    
    
    //INSERTS up to c5 - 4.21 seconds
//                LsmTree lsm_tree(false, 2, 5, 2000, 2, true, 1 ,1, 2, false); // 20k inserts
//            
//                    for (i = 0; i < N; i++)
//                    {
//            
//                        long random_x;
//                        random_x = data.at(i);
//            
//                        dtype x;
//                        x.key = lsm_tree.getKeyCounter();
//                        x.value = random_x;
//            
//                        lsm_tree.insert_value(x);
//                    }
    //XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
    //XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
    //XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
    
    
    
    
    
    
    
    //3
    //XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
    //XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
    //XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
    
    // FINDING THE PERFECT M
    
    //200k INSERTS
    
    // M = 5 - 1.88 seconds
    // M = 20 - 1.26 seconds
    // M = 40 - 1.43 seconds
    
//        LsmTree lsm_tree(false, 2, 5, 5000000, 2, true, 1 ,1, 2, false); // 20k inserts
//        
//        for (i = 0; i < 200000; i++)
//        {
//            
//            long random_x;
//            random_x = data.at(i);
//            
//            dtype x;
//            x.key = lsm_tree.getKeyCounter();
//            x.value = random_x;
//            
//            lsm_tree.insert_value(x);
//        }
    //XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
    //XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
    //XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
    
    
    //4
    //XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
    //XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
    //XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
    
    //SINGLE THREAD VS MULTITHREAD -20k INSERTS
    
    
    
        // MULTI-THREAD UPDATE LEVELS OFF
//        LsmTree lsm_tree(true, 2, 5, 2000, 2, true, 1 ,1, 2, false);
//        
//        for (i = 0; i < N; i++)
//        {
//            
//            long random_x;
//            random_x = data.at(i);
//            
//            dtype x;
//            x.key = lsm_tree.getKeyCounter();
//            x.value = random_x;
//            
//            //cout << "KEY IS " << x.key << endl;
//            //cout << "VALUE IS " << x.value << endl;
//            
//            
//            lsm_tree.insert_value(x);
//        }
    
    
    
         //MULTI-THREAD UPDATE LEVELS ON
//        LsmTree lsm_tree(true, 2, 5, 2000, 2, true, 1 ,1, 2, true);
//        
//        for (i = 0; i < N; i++)
//        {
//    
//            long random_x;
//            random_x = data.at(i);
//    
//            dtype x;
//            x.key = lsm_tree.getKeyCounter();
//            x.value = random_x;
//    
//            //cout << "KEY IS " << x.key << endl;
//            //cout << "VALUE IS " << x.value << endl;
//    
//            
//            lsm_tree.insert_value(x);
//        }
    //XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
    //XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
    //XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
    
    
    
    
    
    
    
    //5
    //XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
    //XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
    //XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX

        
    //  SINGLE THREAD VS MULTITHREAD
        
    //  2 threads - 7.3
    //  4 threads - 5.5
    //  8 threads - 5.4
    
        LsmTree lsm_tree(true, 2, 5, 5000000, 2, true, 1 ,1, 2, false);
        
        for (i = 0; i < 200000; i++)
        {
            long random_x;
            random_x = data.at(i);
    
            dtype x;
            x.key = lsm_tree.getKeyCounter();
            x.value = random_x;
    
            lsm_tree.insert_value(x);
        }
    
        
        typedef dtype item_type;
        distributor<item_type> process([&lsm_tree](item_type &item) {
            
            (lsm_tree).read_value(item);
            
        });
        
        //long i;
        for (i = 0; i < N; i++)
        {
            
            long random_x;
            
            random_x = data.at(i);
            
            dtype x;
            x.key = lsm_tree.getKeyCounter();
            x.value = random_x;
    
            process(std::move(x));
        }
    //XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
    //XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
    //XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
    
    // END THE TIMER
    auto end = std::chrono::steady_clock::now();
    
    // HOW MUCH TIME IT TOOK
    auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
    
    // MAKE THE MAIN THREAD WAIT FOR DETACHED THREADS BEFORE TERMINATING
    while (!lsm_tree.is_ready) {
        std::this_thread::yield();
    }
    
    // PRINT LSM TREE STATS AFTER TESTING
    lsm_tree.printStats();
    
    // DELETE THE c1-n.bin FILES AFTER THE PROGRAM RUNS
    for (int x = 1; x <= 10; x++)
    {
        int a = x;
        stringstream ss;
        ss << a;
        string str = ss.str();
        string fileName2 = "c" + str + ".bin";
        remove(fileName2.c_str());
    }
    
    // PRINT OUT THE TIME IT TOOK TO PROCESS THE TIMED DATA
    std::cout << "~~~IT TOOK " << double (elapsed.count() / 1000000.00) << " SECONDS TO PROCESS " << N << " VALUES~~~" << std::endl;
}