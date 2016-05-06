/**
 C++11 - GCC Compiler
 LsmLevelDisk.h
 
 Creates a BTree representation on disk for each level of the Lsm Tree from c1-n.
 It provides utility methods to access the disk level and print statistics.
 
 @author N. Ruta
 @version 1.0 4/01/16
 */
#ifndef LSMLEVELDISK_H
#define LSMLEVELDISK_H

#include "LsmLevelMemory.h"

// an enum to return the status of a disk operation used in several utility methods of the class
enum status_disk {Disk_InsertNotComplete, Disk_Success, Disk_DuplicateKey,
    Disk_Underflow, Disk_NotFound};

// struct to define the characteristics of a node
struct node_disk {
    int n;        // Number of items stored in a node (n < M)
    dtype k[M-1]; // an array of key value pairs, defined as a dtype , (only the first n in use)
    long p[M];    // an array of 'Pointers' to other nodes (n+1 in use)
};

class LsmLevelDisk {
public:
    LsmLevelDisk();
    
    // construct the LsmLevelDisk by passing a valid file name
    LsmLevelDisk(const char *TreeFileName);
    
    ~LsmLevelDisk();
    
    // a counter used when copying the data from level to level
    static long disk_total_values_count;
    
    /**
     insert the value to the BTree on disk
     
     @param dtype the data to be inserted
     
     */
    void insert(dtype x);
    
    /**
     Display the contents of the BTree for this level on disk
     
     */
    void print(){std::cout << "LsmLevelDisk - Contents:\n"; pr(root, 0);}
    
    /**
     Function to delete a value associated with a note in the BTree
     
     @param x the key value pair to be deleted
     */
    void DelNode(dtype x);
    
    /**
     function used to find the file size for each BTree on disk
     
     @param fileName the name of the file on disk to get the size of
     @return the file size
     
     */
    static long getFileSize(const std::string &fileName);
    
    /**
     function used to copy values from one disk level to the subsequent level
     
     @param previous_level_pointer pointer to the level to be copied from
     @param current_level_pointer pointer to the level to copy to
     @param r the root of the BTree
     @param total_disk_values_count how many values are in the previous level to be copied from
     @param total_to_pass_next_level the total amount to pass between levels
     
     */
    void getNValues(LsmLevelDisk* previous_level_pointer, LsmLevelDisk* current_level_pointer,long r, long total_disk_nodes_count, long disk_counter_limit);
    
    /**
     function used to copy values from one disk level to the subsequent level
     
     @param previous_level_pointer pointer to the level to be copied from
     @param current_level_pointer pointer to the level to copy to
     @param r the root of the BTree
     @param total_to_pass_next_level the total amount to pass between levels
     
     */
    void diskLevelCopy(LsmLevelDisk* previous_level_pointer, LsmLevelDisk* current_level_pointer, long r, long current_level_max_values);
    
    /**
     function used to get the values count for the BTree of a disk level
     
     @param r the root of the BTree
     @return the values count
     
     */
    long disk_calculateValuesCount(long r);
    
    /**
     function used to get the values count of a level
     
     @param r the root of the BTree
     @return the values count
     
     */
    long getValuesCount(long r);
    
    /**
     function used to search for a value in the BTree
     
     @param x the data value to search for
     
     */
    bool search_value(dtype x);
    
    // long values to represent the place of the root value of the BTree
    long root, FreeList;
    
private:
    
    enum {NIL=-1};
    
    // the root node object for the BTree
    node_disk RootNode;
    
    // represents the file associated with this BTree on disk
    std::fstream file;
    
    /**
     insert the value to the BTree on disk
     
     @param r the root for the BTree
     @param x the data type being inserted
     @param y a pointer to the value to be inserted
     @param q the pointer to the value to be inserted
     
     @return Disk_Success, Disk_DuplicateKey or Disk_InsertNotComplete
     
     */
    status_disk ins(long r, dtype x, dtype &y, long &u);
    
    /**
     function used to print the BTree to console
     
     @param r the root of the BTree
     @param nSpace the space, printing on the console, between each value used for visual quality
     
     */
    void pr(long r, int nSpace);
    
    /**
     function used to locate a node, based on binary search
     
     @param x the value to search for the node by
     @param a a pointer to the data type
     @param n the size of the node to search for
     @return the location of the node in the BTree
     
     */
    long NodeSearch(dtype x, const dtype  *a, long n)const;
    
    /**
     function used to delete the node
     
     @param x the value to delete
     @param r the root of the BTree
     @return the status of the attempt to delete from the BTree
     
     */
    status_disk del(long r, dtype x);
    
    /**
     function used to read a node
     
     @param r the root of the BTree
     @param Node the node to find on disk
     
     */
    void ReadNode(long r, node_disk &Node);
    
    /**
     function used to write a node
     
     @param r the root of the BTree
     @param Node the node to write on disk
     
     */
    void WriteNode(long r, const node_disk &Node);
    
    /**
     function used to read from the beginning of the BTree, the root
     */
    void ReadStart();
    
    /**
     function used to return the position of a newly created node
     
     @return long the node position
     
     */
    long GetNode();
    
    /**
     function used to pointer for root node
     
     @param r the root node
     
     */
    void FreeNode(long r);
    
};

#endif
