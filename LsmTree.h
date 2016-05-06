/**
 C++11 - GCC Compiler
 LsmTree.h
 Creates an LsmTree using data structures that are both memory and disk resident.
 Lsm Tree consists of several levels including at least one memory and one disk resident data structure.
 @author N. Ruta
 @version 1.0 4/01/16
 */
#ifndef LSMTREE_H
#define LSMTREE_H

#include <thread>

#include "LsmLevelDisk.h"
#include "LsmLevelMemory.h"

using namespace std;

// a struct to contain data for each disk level of the Lsm Tree
struct LsmLevel {
    int levelNumber;             // The level number --> for example c1 has a value of 1
    long fileSize;                // the level's associated file's size --> for example size of c1.bin
    long maxFileSize;            // the maximum size, in kb, of the level's associated file
    char fileName[10];           // the name of the disk resident file for the level --> for example c1.bin
    LsmLevelDisk *lsmLevelDisk;  // a pointer to the memory location of the BTree associated with this level
};

class LsmTree
{
public:
    
    // threads update the value of this atomic bool to determine if updateLevels process in a detached
    // thread has completed. This set to true allows the next thread to enter the updateLevels function
    static std::atomic<bool> is_ready;
    
    /**
     Constructor to initialize the Lsm Tree using tunable parameters.
     
     @param readOptimized boolean value to determine if read optimization is on or off
     @param c0DataStructure what data structure should be used at c0 - 1 for BTree or 2 for std::Vector
     @param numberOfLevels the number of levels for the lsm tree
     @param firstLevelFileSize what is the maximum size of the c1 level?
     @param sizeBetweenLevels what is the size increase for each additional level?
     @param copyAllFromC0 should we copy all of c0 to c1 on a rolling merge?
     @param c0_percentage_to_copy what percentage of c0 do you copy to c1 on a rolling merge?
     @param c0_percentage_of_c1 what percentage of c1 can c0 be before a rolling merge occurs?
     @param threadedRollingMerge should the threaded rolling merge occur in its own thread?
     
     // 1 FUTURE PLAN - for chunk all moves to where it can fit
     // 2 ONLY ONE FUNCTIONAL AT THIS TIME - for insert all here then take out whatever doesn't fit and move to next
     @param mergeStrategy LsmTree::updateLevels() determines strategy using this param
     
     */
    LsmTree(bool readOptimized, int c0DataStructure, int numberOfLevels, long firstLevelFileSize, int sizeBetweenLevels, bool copyAllFromC0, double c0_percentage_to_copy, double c0_percentage_of_c1, int mergeStrategy, bool threadedRollingMerge);
    virtual ~LsmTree();
    
    
    /**
     adds a key value pair to the Lsm Tree
     
     @param dtype the data to be inserted
     
     */
    void insert_value(dtype value);
    
    /**
     deletes a key value pair from the Lsm Tree
     
     @param dtype the data to be deleted
     
     */
    void delete_value(dtype value);
    
    /**
     Search for the value in the Lsm Tree.
     
     does a read request, if the value is found, the first time it is found...the function returns.
     
     @param dtype the data to be deleted
     
     */
    bool read_value(dtype value);
    
    /**
     Update the key value pair of the Lsm Tree
     
     @param old_value the data to be deleted
     @param new_value the new value to update
     
     */
    void update_value(dtype old_value, dtype new_value);
    
    // Functions to get the virtual and physical memory stats
    int parseLine(char* line);
    int getValueVirtualMemory();
    int getValuePhysicalMemory();
    
  
    // Global counter for keys
    long key_counter = 0;
    
    /**
     get the next unique value to use for the key of the most recent insert request
     
     @return the next key number for insert
     
     */
    long getKeyCounter() {
        return ++LsmTree::key_counter;
    }
    
    /**
     print the statistics for the LsmTree including the disk level information and memory level value count
     
     example output -
     
     C5 file size is - 670896
     LEVEL NUMBER - 5
     maxFileSize - 32000
     lsmLevelDisk - 0x7fff5fbf6f68
     fileName - c5.bin
     the values count in this level is 19370
     
     */
    void printStats();
    
protected:
private:
    
    // class member variables to represent the tunable parameters passsed in the constructor for usage during the
    // lifetime of the application
    int c0DataStructure;
    bool copyAllFromC0;
    int numberOfLevels;
    int mergeStrategy;
    long firstLevelFileSize;
    double c0_percentage_of_c1;
    long c0_max_size;
    double c0_percentage_to_copy;
    bool readOptimized;
    bool isThreadedRollingMerge;
    
    // the min and max values, when the value is a long, present in the lsm tree's data
    long minValueOfDataset;
    long maxValueOfDataset;
    
    // have the min and max range values of the dataset been set?
    bool isRangeSet;
    
    // a vector to contain all of the level structs
    vector<LsmLevel> levels;
    
    // an array to contain a maximum of 11 instances of the lsmLevelDisk class
    LsmLevelDisk lsmLevelDisks[11];
    
    // c0 instance of LsmLevelMemory
    LsmLevelMemory c0;
    
    // c0 as a vector
    std::vector<long> c0Vector;
    
    // create a tombstone vector to mark values as deleted
    std::vector<long> tombstoneVector;
    
    /**
     rollingMerge function is responsible for the rolling merge process once c0 fills up
     
     @copyAllFromc0 - should all of c0 be copied to c1 on a rolling merge
     
     */
    void rollingMerge(bool copyAllFromC0);
    
    /**
     the rolling merge strategy is determined here.
     
     once c1 is full, this function is called to move data between subsequent levels.
     
     @param c1_total_to_pass how many values to pass from c1 to c2
     @param c1 a pointer to the c1 disk Btree object
     @param numberOfLevels the number of disk resident levels for the Lsm Tree
     @param mergeStrategy tunable parameter for different merge strategies
     
     */
    void updateLevels(long c1_total_to_pass, LsmLevelDisk* c1, int numberOfLevels, int mergeStrategy);

    /**
     
     updateLevels operating in a detached thread 
     
     the rolling merge strategy is determined here.
     
     once c1 is full, this function is called to move data between subsequent levels.
     
     @param c1_total_to_pass how many values to pass from c1 to c2
     @param c1 a pointer to the c1 disk Btree object
     @param levels pointer to the levels vector that contains the c1-n levels information
     @param mergeStrategy tunable parameter for different merge strategies
     
     */
    static void updateLevelsThreaded(long* c1_total_to_pass, LsmLevelDisk* c1, int* numberOfLevels, int* mergeStrategy, vector<LsmLevel>* levels);
    
};

#endif // LSMTREE_H
