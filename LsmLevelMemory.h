/**
 C++11 - GCC Compiler
 LsmLevelMemory.h
 
 Creates a BTree representation in memory for the c0 level of the Lsm Tree.
 It provides utility methods to access the BTree and print statistics.
 
 @author N. Ruta
 @version 1.0 4/01/16
 */
#ifndef LSMLEVELMEMORY_H
#define LSMLEVELMEMORY_H

// M represents the node size
// This is called the order of the B-tree
// there are M fields in each node
#define M 20

// this struct represents the data of the lsm tree
// it is currently set as key value pairs
typedef struct {
    long key;
    long value;
} dtype;

#include <fstream>
#include <iomanip>
#include <vector>
#include <iostream>

// added to satisfy requirement to use NULL
#include <stdio.h>

#include "LsmLevelDisk.h"

// an enum to return the status of a disk operation used in several utility methods of the class
enum status {InsertNotComplete, Success, DuplicateKey,
    Underflow, NotFound};

// struct to define the characteristics of a node
struct node {
    int n;        // Number of items stored in a node (n < M)
    dtype k[M-1]; // an array of key value pairs, defined as a dtype , (only the first n in use)
    node *p[M];   // an array of 'Pointers' to other nodes (n+1 in use)
};

// establish a reference to LsmLevelDisk for subsequent calls to it from within this file
class LsmLevelDisk;

class LsmLevelMemory {
public:
    
    // a counter used when copying the data from level to level
    static long total_values_count;
    
    LsmLevelMemory(): root(NULL){}
    
    /**
     insert the value to the BTree in memory
     
     @param dtype the data to be inserted
     
     */
    void insert(dtype x);
    
    /**
     Display the contents of the BTree for this level on disk
     
     */
    void print()const{std::cout << "LsmLevelMemory - Contents:\n"; pr(root, 0);}
    
    /**
     Function to delete a value associated with a node in the BTree
     
     @param x the key value pair to be deleted
     */
    void DelNode(dtype x);
    
    /**
     function used to search for a value in the BTree
     
     @param x the data value to search for
     
     */
    bool search_value(dtype x)const;
    
    /**
     function used to copy values from c0 memory level to the c1 level
     
     @param c pointer to the level to be copied to
     @param c0 pointer to the level to copy from
     @param r the root of the BTree
     @param c0TotalValues the total values in the memory level BTree
     @param copyAllFromC0 boolean to determine if all of c0 should be copied
     @param c0_percentage_to_copy what percentage of c0 should be copied?
     
     */
    void getNValues(LsmLevelDisk* c, LsmLevelMemory* c0,const node *r, long c0TotalValues, bool copyAllFromC0, double c0_percentage_to_copy)const;
    
    /**
     function used to copy values from c0 memory level to the c1 level
     
     @param c pointer to the level to be copied to
     @param c0 pointer to the level to copy from
     @param copyAllFromC0 boolean to determine if all of c0 should be copied
     @param c0_percentage_to_copy what percentage of c0 should be copied?
     
     */
    void memoryLevelCopy(LsmLevelDisk* c, LsmLevelMemory *c0, bool copyAllFromC0, double c0_percentage_to_copy);
    
    /**
     function used to copy values from c0 memory level to the c1 level
     
     @param c0VectorPtr pointer to the vector to be copied from
     @param c pointer to the level to copy to
     @param copyAllFromC0 boolean to determine if all of c0 should be copied
     @param c0_percentage_to_copy what percentage of c0 should be copied?
     
     */
    void memoryLevelCopyVector(std::vector<long>* c0VectorPtr, LsmLevelDisk *c, bool copyAllFromC0, double c0_percentage_to_copy);
    
    /**
     function used to copy values from c0 memory level to the c1 level
     
     @param c0VectorPtr pointer to the vector to be copied from
     @param c pointer to the level to copy to
     @param c0TotalValues the total values to copy from c0 to c1
     @param copyAllFromC0 boolean to determine if all of c0 should be copied
     @param c0_percentage_to_copy what percentage of c0 should be copied?
     
     */
    void getNValuesVector(std::vector<long>* c0VectorPtr, LsmLevelDisk *c, long c0TotalValues, bool copyAllFromC0, double c0_percentage_to_copy)const;
    
    /**
     function used to get the values count of the c0 memory level
     
     @return the values count
     
     */
    long getValuesCount();
    
private:
    
    // pointer to the root node
    node *root;
    
    /**
     insert the value to the BTree in memory
     
     @param r pointer to the root node for the BTree
     @param x the data type being inserted
     @param y a pointer to the value to be inserted
     @param u the pointer to the value to be inserted
     
     @return Disk_Success, Disk_DuplicateKey or Disk_InsertNotComplete
     
     */
    status ins(node *r, dtype x, dtype &y, node* &u);
    
    /**
     function used to print the BTree to console
     
     @param r the root of the BTree
     @param nSpace the space, printing on the console, between each value used for visual quality
     
     */
    void pr(const node* r, int nSpace)const;
    
    /**
     function used to get the values count for the BTree of a disk level
     
     @param r the root of the BTree
     @return the values count
     
     */
    long calculateValuesCount(const node *r)const;
    
    /**
     function used to locate a node, based on binary search
     
     @param x the value to search for the node by
     @param a a pointer to the data type
     @param n the size of the node to search for
     @return the location of the node in the BTree
     
     */
    long NodeSearch(dtype x, const dtype *a, long n)const;
    
    /**
     function used to delete the node
     
     @param x the value to delete
     @param r the root of the BTree
     @return the status of the attempt to delete from the BTree
     
     */
    status del(node *r, dtype x);
};

#endif // LsmLevelMemory_H
