/**
 C++11 - GCC Compiler
 LsmTree.cpp
 Creates an LsmTree using data structures that are both memory and disk resident.
 Lsm Tree consists of several levels including at least one memory and one disk resident data structure.
 @author N. Ruta
 @version 1.0 4/01/16
 */
#include "LsmTree.h"
#include "LsmLevelDisk.h"
#include "LsmLevelMemory.h"

// includes added for detecting virtual and physical memory usage
#include "sys/types.h"
//#include "sys/sysinfo.h"

#include <typeinfo>
#include "stdlib.h"
#include "stdio.h"
#include "string.h"
#include <vector>
#include <string>
#include <iostream>
#include <sstream>

#include <mutex>

// threads update the value of this atomic bool to determine if updateLevels process in a detached
// thread has completed. This set to true allows the next thread to enter the updateLevels function
std::atomic<bool> LsmTree::is_ready(true);

// a mutex used to lock access to portions of read and write related to the Node objects and the root
// object of each BTree on disk
std::mutex m;

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
LsmTree::LsmTree(bool readOptimized, int c0DataStructure, int numberOfLevels, long firstLevelMaxFileSize, int sizeBetweenLevels, bool copyAllFromC0, double c0_percentage_to_copy, double c0_percentage_of_c1, int mergeStrategy, bool threadedRollingMerge)
{

    // class member variables to represent the tunable parameters passsed in the constructor for usage during the
    // lifetime of the application
    LsmTree::c0DataStructure = c0DataStructure;
    LsmTree::copyAllFromC0 = copyAllFromC0;
    LsmTree::numberOfLevels = numberOfLevels;
    LsmTree::mergeStrategy = mergeStrategy;
    LsmTree::firstLevelFileSize = firstLevelMaxFileSize;
    LsmTree::c0_percentage_of_c1 = c0_percentage_of_c1;
    LsmTree::c0_percentage_to_copy = c0_percentage_to_copy;
    LsmTree::c0_max_size = (long) (LsmTree::firstLevelFileSize * LsmTree::c0_percentage_of_c1) / 50;
    LsmTree::readOptimized = readOptimized;
    LsmTree::isThreadedRollingMerge = threadedRollingMerge;
    
    // if c0 is a vector
    if (c0DataStructure == 2)
    {
        // set the intial size for the vectors
        // the plan is to not resize and therefore get the optimization that comes with a set size array during
        // the lifetime of the application
        c0Vector.reserve(c0_max_size);
        tombstoneVector.reserve(c0_max_size);
    }
    
    // an int to represent the max file size
    // it is larger after each level creation in the for loop below by using 'sizeBetweenLevels'
    long levelMaxFileSize = firstLevelMaxFileSize;
    
    // create the LsmLevel objects for each level of the Disk resident BTrees
    for (int i = 1; i <= numberOfLevels; i++) {
        
        LsmLevel c_level;
        
        c_level.levelNumber = i;
        
        c_level.maxFileSize = levelMaxFileSize;
        
        // char to convert int to char for the filename
        char char1;
        
        char1 =  '0' + i ;
        
        // create the char array for the file name
        char FileName[10] =  { 'c',char1, '.', 'b', 'i', 'n', '\0' };
        
        // create a string of the char array to pass to the constructor of the Btree on disk for the level
        strncpy(c_level.fileName, FileName, sizeof(c_level.fileName)-1);
        
        // create an LsmLevelDisk object for each level, pass in the filename created above
        new (&lsmLevelDisks[i]) LsmLevelDisk(c_level.fileName);
        c_level.lsmLevelDisk = &lsmLevelDisks[i];
        
        // add the LsmLevel to the vector of levels representing the LsmTree's total levels
        levels.push_back(c_level);
        
        // set the max file size for the next level
        levelMaxFileSize = levelMaxFileSize * sizeBetweenLevels;
    }
}

LsmTree::~LsmTree()
{
    //dtor
}

// this counter is used to determine when c0 if full by comparing its value to LsmTree::c0_max_size
long c0_limit_counter;

/**
 adds a key value pair to the Lsm Tree
 
 @param dtype the data to be inserted
 
 */
void LsmTree::insert_value( dtype value )
{
    
    // if read optimization is enabled
    if (readOptimized)
    {
        // if the value is in the tombstone vector, remove it
        if ((std::find(tombstoneVector.begin(), tombstoneVector.end(), value.value) != tombstoneVector.end())) {
            
            // if the value is in the tombstone, remove it using the erase function
            tombstoneVector.erase(std::remove(tombstoneVector.begin(), tombstoneVector.end(), value.value), tombstoneVector.end());
        }
        
        // if the range for the dataset has not yet been set (this is the first call to insert_value)
        // then set the min and max values to this first insert
        if (isRangeSet == false)
        {
            LsmTree::minValueOfDataset = value.value;
            LsmTree::maxValueOfDataset = value.value;
            
            // the initial range is set
            isRangeSet = true;
        }
        
        // set the min and max value present in the dataset
        // change either value if this new value requires it
        if (value.value < minValueOfDataset)
        {
            LsmTree::minValueOfDataset = value.value;
        }
        
        if (value.value > maxValueOfDataset)
        {
            LsmTree::maxValueOfDataset = value.value;
        }
    }
    
    // c0 is a btree
    if (LsmTree::c0DataStructure == 1)
    {
        // if the max file size for c0 has not yet been reached, simply insert to c0 and increment the counter
        if (c0_limit_counter < LsmTree::c0_max_size)
        {
            
            c0.insert(value);
            c0_limit_counter++;
            
            // otherwise, c0 is now full, it is time to do a rolling merge
        } else {
            
            // copyAllFromC0 == true - copy entire c0 to c1 on rollingMerge
            rollingMerge(LsmTree::copyAllFromC0);
            
            // reset the counter for the next batch of inserts to c0
            c0_limit_counter = 0;
            
            // insert the value to c0 now that the rolling merge has been complete
            c0.insert(value);
        }
    }
    
    // c0 is a vector
    if (LsmTree::c0DataStructure == 2)
    {
        // if the max file size for c0 has not yet been reached, simply insert to c0 and increment the counter
        if (c0_limit_counter < LsmTree::c0_max_size)
        {
            c0Vector.push_back(value.value);
            c0_limit_counter++;
            
            // otherwise, c0 is now full, it is time to do a rolling merge
        } else {
            
            // copyAllFromC0 == true - copy entire c0 to c1 on rollingMerge
            rollingMerge(LsmTree::copyAllFromC0);
            
            // reset the counter for the next batch of inserts to c0
            c0_limit_counter = 0;
            
            
            // insert the value to c0 now that the rolling merge has been complete
            c0Vector.push_back(value.value);
        }
    }
}

/**
 deletes a key value pair from the Lsm Tree
 
 @param dtype the data to be deleted
 
 */
void LsmTree::delete_value(dtype value)
{
    // c0 is a BTREE
    if (LsmTree::c0DataStructure == 1)
    {
        // if read optimization is enabled, used the tombstone technique
        if (LsmTree::readOptimized == true) {
            
            tombstoneVector.push_back(value.value);
            
            // otherwise, use the blind delete technique
        } else {
            
            // blind delete at all levels
            
            c0.DelNode(value);
            
            for (int i = 0; i < numberOfLevels; ++i)
            {
                (*levels[i].lsmLevelDisk).DelNode(value);
            }
        }
    }
    
    // c0 is a vector
    if (LsmTree::c0DataStructure == 2)
    {
        // if read optimization is enabled, used the tombstone technique
        if (LsmTree::readOptimized == true) {
            
            tombstoneVector.push_back(value.value);
            
            // otherwise, use the blind delete technique
        } else {
            
            // blind delete at all levels
            
            c0Vector.erase(std::remove(c0Vector.begin(), c0Vector.end(), value.value), c0Vector.end());
            
            for (int i = 0; i < numberOfLevels; ++i)
            {
                (*levels[i].lsmLevelDisk).DelNode(value);
            }
        }
    }
}

/**
 Search for the value in the Lsm Tree.
 
 does a read request, if the value is found, the first time it is found...the function returns.
 
 @param dtype the data to be deleted
 
 */
bool LsmTree::read_value(dtype value)
{
    // if read optimization is enabled, use the min max range technique stop searches quickly when requested values are not in the
    // range of the LsmTree value set
    if (LsmTree::readOptimized == true)
    {
        if (!(value.value >= LsmTree::minValueOfDataset & value.value <= maxValueOfDataset))
        {
            // the value does not fall within the range contained by the dataset, return false and do not proceed to check the levels
            return false;
        }
    }
    
    // c0 is a BTREE
    if (LsmTree::c0DataStructure == 1)
    {
        
        if (LsmTree::readOptimized) {
            
            bool tombstone_search_result = std::find(tombstoneVector.begin(), tombstoneVector.end(), value.value) != tombstoneVector.end();
            
            // if value is marked as tombstone delete, exit search
            if (tombstone_search_result == true)
            {
                // return true and exit search
                return tombstone_search_result;
            }
        }
        
        // look for the value in c0 memory
        bool memory_search_result = c0.search_value(value);
        if (memory_search_result) { return memory_search_result; }
        
        // not found in memory, look for it at each level
        for (int i = 0; i < numberOfLevels; ++i)
        {
            bool disk_search_result = (*levels[i].lsmLevelDisk).search_value(value);
            if (disk_search_result) { return disk_search_result; }
        }
    }
    
    // c0 is a vector
    if (LsmTree::c0DataStructure == 2)
    {
        if (LsmTree::readOptimized == true) {
            
            bool tombstone_search_result = std::find(tombstoneVector.begin(), tombstoneVector.end(), value.value) != tombstoneVector.end();
            
            // if value is marked as tombstone delete, exit search
            if (tombstone_search_result == true)
            {
                // found in the tombstone vector, return false for search
                return false;
            }
        }
        bool memory_search_result = std::find(c0Vector.begin(), c0Vector.end(), value.value) != c0Vector.end();
        if (memory_search_result == true)
        {
            return true;
        }
        
        // not found in memory, look for it at each level
        for (int i = 0; i < numberOfLevels; ++i)
        {
            bool disk_search_result = (*levels[i].lsmLevelDisk).search_value(value);
            if (disk_search_result == true) {
                
                return disk_search_result;
            }
        }
    }
    return false;
}

/**
 Update the key value pair of the Lsm Tree
 
 @param old_value the data to be deleted
 @param new_value the new value to update
 
 */
void LsmTree::update_value(dtype old_value, dtype new_value)
{
    
    // if read optimization is enabled
    if (readOptimized == true)
    {
        // if the value is in the tombstone vector, remove it
        if ((std::find(tombstoneVector.begin(), tombstoneVector.end(), new_value.value) != tombstoneVector.end())) {
            tombstoneVector.erase(std::remove(tombstoneVector.begin(), tombstoneVector.end(), new_value.value), tombstoneVector.end());
        }
        
        // if the range for the dataset has not yet been set (this is the first call to insert_value
        // then set the min and max values to this first insert
        if (isRangeSet == false)
        {
            LsmTree::minValueOfDataset = new_value.value;
            LsmTree::maxValueOfDataset = new_value.value;
            
            // initial values set for the range
            isRangeSet = true;
        }
        
        // set the min and max value present in the dataset
        // change either value if this new value requires it
        if (new_value.value < minValueOfDataset)
        {
            
            LsmTree::minValueOfDataset = new_value.value;
        }
        
        if (new_value.value > maxValueOfDataset)
        {
            LsmTree::maxValueOfDataset = new_value.value;
        }
    }
    
    // c0 is a BTREE
    if (LsmTree::c0DataStructure == 1)
    {
        // use tombstone delete technique with read optimized
        if (LsmTree::readOptimized == true) {
            
            tombstoneVector.push_back(old_value.value);
            
            // insert new value to c0
            c0.insert(new_value);
            
        } else {
            
            // blind delete from memory level c0
            c0.DelNode(old_value);
            
            // not found in memory, blind delete it at at each level until found
            for (int i = 0; i < numberOfLevels; ++i)
            {
                (*levels[i].lsmLevelDisk).DelNode(old_value);
            }
            
            // insert new value to c0
            c0.insert(new_value);
        }
    }
    
    // c0 is a std::vector
    if (LsmTree::c0DataStructure == 2)
    {
        // c0 is small enough to contain the value
        if (c0_limit_counter < LsmTree::c0_max_size)
        {
            
            if (LsmTree::readOptimized == true)
            {
                
                tombstoneVector.push_back(old_value.value);
                
                c0Vector.push_back(new_value.value);
                
                c0_limit_counter++;
                
            } else {
                
                // blind delete at all levels
                
                c0Vector.erase(std::remove(c0Vector.begin(), c0Vector.end(), old_value.value), c0Vector.end());
                
                for (int i = 0; i < numberOfLevels; ++i)
                {
                    (*levels[i].lsmLevelDisk).DelNode(old_value);
                }
                
                // insert new value to c0
                c0Vector.push_back(new_value.value);
                
                c0_limit_counter++;
            }
            
            // c0 is full, do a rolling merge and then insert the value to memory
        } else {
            
            if (LsmTree::readOptimized == true) {
                
                // true - copy entire c0 to c1 on rollingMerge
                rollingMerge(LsmTree::copyAllFromC0);
                
                // reset the counter for the next batch of inserts to c0
                c0_limit_counter = 0;
                
                // insert the value to c0
                c0Vector.push_back(new_value.value);
                tombstoneVector.push_back(old_value.value);
                
            } else {
                
                // true - copy entire c0 to c1 on rollingMerge
                rollingMerge(LsmTree::copyAllFromC0);
                
                c0_limit_counter = 0;
                
                // blind delete at all levels
                c0Vector.erase(std::remove(c0Vector.begin(), c0Vector.end(), old_value.value), c0Vector.end());
                
                for (int i = 0; i < numberOfLevels; ++i)
                {
                    (*levels[i].lsmLevelDisk).DelNode(old_value);
                }
                
                // insert the value to c0
                c0Vector.push_back(new_value.value);
            }
        }
    }
}

// three simple functions to get the virtual and physical memory stats
// found at - http://stackoverflow.com/questions/2513505/how-to-get-available-memory-c-g

// parse lines for proc/self/status file
int LsmTree::parseLine(char* line){
    int i = strlen(line);
    while (*line < '0' || *line > '9') line++;
    line[i-3] = '\0';
    i = atoi(line);
    return i;
}

// Virtual Memory currently used by current process:
int LsmTree::getValueVirtualMemory(){ //Note: this value is in KB!
    FILE* file = fopen("/proc/self/status", "r");
    int result = -1;
    char line[128];
    
    while (fgets(line, 128, file) != NULL){
        if (strncmp(line, "VmSize:", 7) == 0){
            result = parseLine(line);
            break;
        }
    }
    fclose(file);
    return result;
}

// physical memory currently used by current process:
int LsmTree::getValuePhysicalMemory(){ //Note: this value is in KB!
    FILE* file = fopen("/proc/self/status", "r");
    int result = -1;
    char line[128];
    
    while (fgets(line, 128, file) != NULL){
        if (strncmp(line, "VmRSS:", 6) == 0){
            result = parseLine(line);
            break;
        }
    }
    fclose(file);
    return result;
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
void LsmTree::printStats() {
    
    //struct sysinfo memInfo;
    
    //sysinfo (&memInfo);
    //        long long totalVirtualMem = memInfo.totalram;
    //        //Add other values in next statement to avoid int overflow on right hand side...
    //        totalVirtualMem += memInfo.totalswap;
    //        totalVirtualMem *= memInfo.mem_unit;
    
    //       cout << "TOTAL VIRTUAL MEMORY - " << totalVirtualMem << endl;
    
    //        long long virtualMemUsed = memInfo.totalram - memInfo.freeram;
    //        //Add other values in next statement to avoid int overflow on right hand side...
    //        virtualMemUsed += memInfo.totalswap - memInfo.freeswap;
    //        virtualMemUsed *= memInfo.mem_unit;
    //
    //        cout << "TOTAL VIRTUAL MEMORY USED - " << virtualMemUsed << endl;
    //
    //        cout << "TOTAL AVAILABLE VIRTUAL MEMORY - " << totalVirtualMem - virtualMemUsed << endl;
    //
    //        // Virtual Memory currently used by current process:
    //        cout << "Virtual Memory currently used by current process: " << getValueVirtualMemory() << endl;
    //
    //        long long totalPhysMem = memInfo.totalram;
    //        //Multiply in next statement to avoid int overflow on right hand side...
    //        totalPhysMem *= memInfo.mem_unit;
    //
    //        cout << "TOTAL Physical MEMORY  - " << totalPhysMem << endl;
    //
    //        // PHYSICAL Memory currently used by current process:
    //        cout << "PHYSICAL Memory currently used by current process: " << getValuePhysicalMemory() << endl;
    //
    //
    //        long long physMemUsed = memInfo.totalram - memInfo.freeram;
    //        //Multiply in next statement to avoid int overflow on right hand side...
    //        physMemUsed *= memInfo.mem_unit;
    //
    //        cout << "TOTAL PHYSICAL MEMORY USED - " << physMemUsed << endl;
    //        cout << "TOTAL AVAILABLE PHYSICAL MEMORY - " << totalPhysMem - physMemUsed << endl;
    
    //cout << "C0 PRINT CONTENTS" << endl;
    //c0.print();
    
    /* ********************************************** DEBUG ******************************************************
     *************************************************************************************************************/
    cout << endl;
    cout << "C0 VALUES COUNT " << c0Vector.size() << endl;
    //c0.print();
    for (int a = 0; a < LsmTree::numberOfLevels; a++)
    {
        cout << "-------------------------------------------------------------------------------" << endl;
        cout << "C" << a+1 << " file size is - " << LsmLevelDisk::getFileSize(levels[a].fileName) << endl;
        cout <<  "LEVEL NUMBER - " << levels[a].levelNumber << endl;
        cout <<  "maxFileSize - " << levels[a].maxFileSize << endl;
        cout <<  "lsmLevelDisk - " << levels[a].lsmLevelDisk << endl;
        cout <<  "fileName - " << levels[a].fileName << endl;
        cout <<  "the values count in this level is " << (*levels[a].lsmLevelDisk).getValuesCount((*levels[a].lsmLevelDisk).root) << endl;
        //(*levels[a].lsmLevelDisk).print();
        cout << endl;
    }
}

/**
 the rolling merge strategy is determined here.
 
 once c1 is full, this function is called to move data between subsequent levels.
 
 @param c1_total_to_pass how many values to pass from c1 to c2
 @param c1 a pointer to the c1 disk Btree object
 @param numberOfLevels the number of disk resident levels for the Lsm Tree
 @param mergeStrategy tunable parameter for different merge strategies
 
 */
void LsmTree::updateLevels(long c1_total_to_pass, LsmLevelDisk* c1, int numberOfLevels, int mergeStrategy)
{
    
    cout << "UPDATE LEVELS START" << endl;
    
    // if mergeStrategy == 1 I WOULD LIKE TO HAVE COPY ENTIRE FILE IF NEXT LEVEL IS EMPTY
    // if mergeStrategy == 2 do fill each level and remainder to next level
    
    if (mergeStrategy == 1)
    {
        
    }
    
    // if mergeStrategy == 2 do fill each level and remainder to next level
    if (mergeStrategy == 2)
    {
        
        // get pointers to the c1 and c2 disk resident Btrees
        LsmLevelDisk* previous_level_pointer = c1;
        LsmLevelDisk* current_level_pointer;
        
        // for each level of the LSM Tree after C1
        for (int i = 1; i < numberOfLevels; ++i)
        {
            // long variables to represent the current level array cout and total to pass for subsequent levels after c1
            long active_array_count;
            long after_c1_total_to_pass;
            
            // if the level is c2, use the c1 total for the active array counter
            if (levels[i].levelNumber == 2)
            {
                active_array_count = c1_total_to_pass;
            }
            
            // the level is after c2, use the counter as the value passed from the previous level
            if (levels[i].levelNumber > 2) {
                active_array_count = after_c1_total_to_pass;
            }
            
            // get the max the current level can hold and the value count of the current level
            long current_level_max_values = (long)levels[i].maxFileSize / 50;
            long current_level_values_count = (*levels[i].lsmLevelDisk).getValuesCount((*levels[i].lsmLevelDisk).root);
            
            // get a pointer to the current level BTree
            current_level_pointer = levels[i].lsmLevelDisk;
            
            // get size of disk level
            std::string stringFileName(levels[i].fileName);
            
            //CAN THIS LEVEL CONTAIN THE ENTIRE ARRAY PASSED TO IT? NO
            bool canLevelContainAll = false;
            
            if (current_level_max_values - current_level_values_count > 0)
            {
                // yes, the current level can contain the values count passed to it
                if (current_level_values_count + active_array_count < current_level_max_values)
                {
                    canLevelContainAll = true;
                }
            }
            
            //CAN THIS LEVEL CONTAIN THE ENTIRE ARRAY PASSED TO IT? NO
            if (!canLevelContainAll)
            {
                // this level cannot contain all of the values, do a merge copy to the next level
                (*previous_level_pointer).diskLevelCopy(previous_level_pointer, current_level_pointer, ((*previous_level_pointer).root), active_array_count);
                
                // get the value count for the current level now that the copy was complete
                after_c1_total_to_pass = (*current_level_pointer).getValuesCount((*current_level_pointer).root) - current_level_max_values;
                
                // change the current pointer to be the preveious level pointer before examining the next level
                previous_level_pointer = current_level_pointer;
                
                
                //CAN THIS LEVEL CONTAIN THE ENTIRE ARRAY PASSED TO IT? YES
            } else {
                
                // copy all of the values from the previous level to this level and exit the update levels rolling merge process
                (*previous_level_pointer).diskLevelCopy(previous_level_pointer, current_level_pointer, ((*previous_level_pointer).root), active_array_count);
                
                return;
            }
        }
    }
}

// reset the counter for the next rolling merge process
long rollingMergeCounter = 0;

/**
 rollingMerge function is responsible for the rolling merge process once c0 fills up
 
 @copyAllFromc0 - should all of c0 be copied to c1 on a rolling merge
 
 */
void LsmTree::rollingMerge(bool copyAllFromC0)
{
    
    // c0 is a btree
    if (LsmTree::c0DataStructure == 1)
    {
        cout << "ROLLING MERGE START" << endl;
        
        // copy a portion of c0 to a new array
        // a pointer to the c0 memory level object
        LsmLevelMemory *cPoint = &c0;
        
        int levelCounter = 0;
        
        long current_level_max_values = (long)levels[levelCounter].maxFileSize / 50;
        
        // if all is to be copied from c0
        if (copyAllFromC0) {
            
            // set the counter to the entire size of the c0 level
            rollingMergeCounter = c0.getValuesCount();
            
            // copy the amount, less than all of c0, that is requested based on the tunable parameter
        } else {
            
            rollingMergeCounter = (long)c0.getValuesCount() * c0_percentage_to_copy;
            current_level_max_values = (long)(levels[levelCounter].maxFileSize / 50) * c0_percentage_to_copy;
        }
        
        // get size of disk level
        std::string stringFileName(levels[levelCounter].fileName);
        
        // can this level contain the entire array? YES
        if (current_level_max_values >= rollingMergeCounter)
        {
            
            // copy from c0 to c1
            LsmLevelDisk *c = levels[levelCounter].lsmLevelDisk;
            c0.memoryLevelCopy(c, cPoint, copyAllFromC0, LsmTree::c0_percentage_to_copy);
            
            // reset the rolling merge counter to 0
            rollingMergeCounter = 0;
            
            // // can this level contain the entire array? NO
        } else {
            
            // get a pointer to c1 BTree on disk
            LsmLevelDisk *c1 = levels[levelCounter].lsmLevelDisk;
            
            // copy values from c0 to c1
            c0.memoryLevelCopy(c1, cPoint, copyAllFromC0, LsmTree::c0_percentage_to_copy);
            
            // get the c1 max value size
            long current_level_max_values = (long)levels[levelCounter].maxFileSize / 50;
            
            // pass what cannot fit in c1 to c2
            long c1_total_to_pass = (*c1).getValuesCount((*c1).root) - current_level_max_values;
            
            // update the levels of the LSM Tree in a separate thread
            
            vector<LsmLevel>* levelsPtr = &levels;
            long* c1_total_to_passPtr = &c1_total_to_pass;
            int* mergeStrategyPtr = &mergeStrategy;
            int* numberOfLevelsPtr = &numberOfLevels;
            
            
            
            LsmLevelDisk* c1PtrPtr = c1;
            
            // if the bool is set for an update levels action in a new thread
            if (LsmTree::isThreadedRollingMerge)
            {
                cout << "UPDATING LEVELS IN A NEW THREAD" << endl;
                std::thread updateLevelsThread(updateLevelsThreaded,c1_total_to_passPtr, c1PtrPtr, numberOfLevelsPtr, mergeStrategyPtr, levelsPtr);
                
                // detach the new thread and let it finish the update levels
                updateLevelsThread.detach();
                
            // update levels in a single thread operation
            } else {
                LsmTree::updateLevels(c1_total_to_pass, c1, LsmTree::numberOfLevels, LsmTree::mergeStrategy);
            }
        }
        
        // reset the rolling merge counter for the next call
        rollingMergeCounter = 0;
        
        cout << "ROLLING MERGE END" << endl;
    }
    
    // c0 is a vector
    if (LsmTree::c0DataStructure == 2)
    {
        cout << "ROLLING MERGE START" << endl;

        // make this thread wait until the detached thread running update levels has completed and released the mutex
        while (!LsmTree::is_ready) {
            std::this_thread::yield();
        }
        
        // copy a portion of c0 to a new array
        // a pointer to the c0 memory level object
        //LsmLevelMemory *cPoint = &c0;
        
        int levelCounter = 0;
        
        long current_level_max_values = (long)levels[levelCounter].maxFileSize / 50;
        
        // if all is to be copied from c0
        if (copyAllFromC0) {
            rollingMergeCounter = c0Vector.size();
 
        } else {
            rollingMergeCounter = (long)c0Vector.size() * c0_percentage_to_copy;
            current_level_max_values = (long)(levels[levelCounter].maxFileSize / 50) * c0_percentage_to_copy;
        }
        
        // get size of disk level
        std::string stringFileName(levels[levelCounter].fileName);
        
        // can this level contain the entire array? YES
        if (current_level_max_values >= rollingMergeCounter)
        {
 
            LsmLevelDisk *c = levels[levelCounter].lsmLevelDisk;

            
            std::vector<long> c0VectorCopy(c0Vector);
            std::vector<long>* c0VectorCopyPtr = &c0VectorCopy;
            c0Vector.clear();
            
            c0.memoryLevelCopyVector(c0VectorCopyPtr, c, copyAllFromC0, LsmTree::c0_percentage_to_copy);
            
            c0VectorCopy.clear();

            // reset the rolling merge counter to 0
            rollingMergeCounter = 0;
            
            
        // can this level contain the entire array? NO
        } else {
            
            LsmLevelDisk *c1 = levels[levelCounter].lsmLevelDisk;
            
            std::vector<long> c0VectorCopy(c0Vector);
            std::vector<long>* c0VectorCopyPtr = &c0VectorCopy;
            c0Vector.clear();
            
            c0.memoryLevelCopyVector(c0VectorCopyPtr, c1, copyAllFromC0, LsmTree::c0_percentage_to_copy);
            
            c0VectorCopy.clear();
            
            long current_level_max_values = (long)levels[levelCounter].maxFileSize / 50;
            
            
            long c1_total_to_pass = (*c1).getValuesCount((*c1).root) - current_level_max_values;
            
            // update the levels of the LSM Tree in a new thread
            
            vector<LsmLevel>* levelsPtr = &levels;
            long* c1_total_to_passPtr = &c1_total_to_pass;
            int* mergeStrategyPtr = &mergeStrategy;
            int* numberOfLevelsPtr = &numberOfLevels;
            
            LsmLevelDisk* c1PtrPtr = c1;
            
            // if the bool is set for an update levels action in a new thread
            if (LsmTree::isThreadedRollingMerge)
            {
                cout << "UPDATING LEVELS IN A NEW THREAD" << endl;
                std::thread updateLevelsThread(updateLevelsThreaded,c1_total_to_passPtr, c1PtrPtr, numberOfLevelsPtr, mergeStrategyPtr, levelsPtr);

                // detach the new thread and let it finish the update levels
                updateLevelsThread.detach();
                
            } else {
                LsmTree::updateLevels(c1_total_to_pass, c1, LsmTree::numberOfLevels, LsmTree::mergeStrategy);
            }
        }
        rollingMergeCounter = 0;
        cout << "ROLLING MERGE END" << endl;
    }
}

/**
 
 updateLevels operating in a detached thread
 
 the rolling merge strategy is determined here.
 
 once c1 is full, this function is called to move data between subsequent levels.
 
 @param c1_total_to_pass how many values to pass from c1 to c2
 @param c1 a pointer to the c1 disk Btree object
 @param levels pointer to the levels vector that contains the c1-n levels information
 @param mergeStrategy tunable parameter for different merge strategies
 
 */
void LsmTree::updateLevelsThreaded(long* c1_total_to_pass, LsmLevelDisk* c1, int* numberOfLevels, int* mergeStrategy, vector<LsmLevel>* levels)
{
    cout << "THREADED UPDATE LEVELS START" << endl;

    // get a lock on the mutex for proceeding to the the update levels in the detached thread instructed to do so
    std::unique_lock<std::mutex> lock(m);
    
    // set the atomic variable so that future threads have to wait before entering here
    LsmTree::is_ready = false;
    
    // if mergeStrategy == 1 I WOULD LIKE TO HAVE COPY ENTIRE FILE IF NEXT LEVEL IS EMPTY
    // if mergeStrategy == 2 do fill each level and remainder to next level
    
    if ((*mergeStrategy) == 1)
    {
        
    }
    
    // if mergeStrategy == 2 do fill each level and remainder to next level
    if ((*mergeStrategy) == 2)
    {
        // get pointers to the c1 and c2 disk resident Btrees
        LsmLevelDisk* previous_level_pointer = c1;
        LsmLevelDisk* current_level_pointer;

        // for each level of the LSM Tree after C1
        for (int i = 1; i < (*numberOfLevels); ++i)
        {
            // long variables to represent the current level array cout and total to pass for subsequent levels after c1
            long active_array_count;
            long after_c1_total_to_pass;
            
            // if the level is c2, use the c1 total for the active array counter
            if ((*levels)[i].levelNumber == 2)
            {
                active_array_count = (*c1_total_to_pass);
                
            }
            // the level is after c2, use the counter as the value passed from the previous level
            if ((*levels)[i].levelNumber > 2) {
                active_array_count = after_c1_total_to_pass;
                
            }

            // get the max the current level can hold and the value count of the current level
            long current_level_max_values = (long)(*levels)[i].maxFileSize / 50;
            long current_level_values_count = (*(*levels)[i].lsmLevelDisk).getValuesCount((*(*levels)[i].lsmLevelDisk).root);
            
            // get a pointer to the current level BTree
            current_level_pointer = (*levels)[i].lsmLevelDisk;
            
            // get size of disk level
            std::string stringFileName((*levels)[i].fileName);
            
            //CAN THIS LEVEL CONTAIN THE ENTIRE ARRAY PASSED TO IT? NO
            bool canLevelContainAll = false;
            if (current_level_max_values - current_level_values_count > 0)
            {
                if (current_level_values_count + active_array_count < current_level_max_values)
                {
                    canLevelContainAll = true;
                }
                
            }
            
            //CAN THIS LEVEL CONTAIN THE ENTIRE ARRAY PASSED TO IT? NO
            if (!canLevelContainAll)
            {
                
                // this level cannot contain all of the values, do a merge copy to the next level
                (*previous_level_pointer).diskLevelCopy(previous_level_pointer, current_level_pointer, ((*previous_level_pointer).root), active_array_count);
                
                // get the value count for the current level now that the copy was complete
                after_c1_total_to_pass = (*current_level_pointer).getValuesCount((*current_level_pointer).root) - current_level_max_values;
                
                // change the current pointer to be the preveious level pointer before examining the next level
                previous_level_pointer = current_level_pointer;
                
                
            //CAN THIS LEVEL CONTAIN THE ENTIRE ARRAY PASSED TO IT? YES
            } else {

                // copy all of the values from the previous level to this level and exit the update levels rolling merge process
                (*previous_level_pointer).diskLevelCopy(previous_level_pointer, current_level_pointer, ((*previous_level_pointer).root), active_array_count);

                LsmTree::is_ready = true;
 
                return;
            }
        }
    }
    LsmTree::is_ready = true;
}