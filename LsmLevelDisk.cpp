/**
 C++11 - GCC Compiler
 LsmLevelDisk.cpp
 
 Creates a BTree representation on disk for each level of the Lsm Tree from c1-n.
 It provides utility methods to access the disk level and print statistics.
 
 @author N. Ruta
 @version 1.0 4/01/16
 */
#include "LsmLevelDisk.h"

// use a mutex to protect the read and write funnctionality for each disk level being simultaneously accessed by multiple threads
// the mutex allows a thread to get a lock on the node objects and the root of each btree
std::mutex Node_mutex;

using namespace std;

LsmLevelDisk::LsmLevelDisk()
{
    
}

/**
 Constructor to initialize the BTree for each level
 
 @param TreeFileName the file name used to create the file on disk for this level
 
 */
LsmLevelDisk::LsmLevelDisk(const char *TreeFileName)
{
    // use the file name to confirm the stream is successfully associated with it
    std::ifstream test(TreeFileName, std::ios::in);
    
    // check for error state flags set for the stream
    int NewFile = test.fail();
    
    // reset the state for the stream
    test.clear();
    
    // close the file
    test.close();
    
    // if we are working with a new file
    if (NewFile)
    {
        // open the file, passing in the TreeFileName associated with this level
        file.open(TreeFileName, std::ios::out | std::ios::in |
                  std::ios::trunc | std::ios::binary);
        // std::ios::binary required with MSDOS, but possibly
        // not accepted with other environments.
        
        // create the starting point of the new BTree by setting the root to NIL
        // FreeList is set to NIL to declare the list is empty
        // FreeList is used as a pointer to the first element of the list for the lifetime of the
        // application.
        root = FreeList = NIL;
        long start[2] = {NIL, NIL};
        
        // write to the file, from start to end, using one extra byte at the end to represent
        // the signature. This is used as verification in future reads of the file, if the signature
        // doesn't match, the file is rejected.
        file.write((char*)start, 2 * sizeof(long));
    }  else
    {
        // this isn't a new file, we open the file on disk and attempt to write new information
        long start[2];
        file.open(TreeFileName, std::ios::out | std::ios::in |
                  std::ios::binary); // See above note.
        
        // find the point in the file to write the new information to
        file.seekg(-1L, std::ios::end);
        char ch;
        
        // read the signature
        file.read(&ch, 1);
        file.seekg(0L, std::ios::beg);
        file.read((char *)start, 2 * sizeof(long));
        
        // verify the file format by checking the signature
        if (ch != sizeof(int))
        {
            // alert the file does not contain the signature
            std::cout << "Wrong file format.\n"; exit(1);
        }
        
        // set the root and FreeList variables to the starting position
        root = start[0];
        FreeList = start[1];
        
        // make a call to function ReadNode
        // set the number of values in the node to start at 0
        RootNode.n = 0;
        
        // call to read the node
        ReadNode(root, RootNode);
        print();
    }
}

LsmLevelDisk::~LsmLevelDisk()
{
    // create the long array to contain the root value and the FreeList pointer
    long start[2];
    
    // use seekp to find the beginning of the file for the BTree
    file.seekp(0L, std::ios::beg);
    
    start[0] = root;
    start[1] = FreeList;
    
    // If the current file length is an even number, a
    // signature is added; otherwise it is already there.
    file.write((char*)start, 2 * sizeof(long));
    
    // set the signature
    char ch = sizeof(int);
    file.seekg(0L, std::ios::end);
    if ((file.tellg() & 1) == 0)
    {
        file.write(&ch, 1);
    }
    
    // close the file
    file.close();
}

/**
 insert the value to the BTree on disk
 
 @param dtype the data to be inserted
 
 */
void LsmLevelDisk::insert(dtype x)
{
    
    // create new instances for the pointer and the value to be inserted
    long pNew;
    dtype xNew;
    
    // do the insert, return the status of the execution
    status_disk code = ins(root, x, xNew, pNew);
    
    // the BTree currently does not support duplicates, this is a debug statement to acknowledge
    // and attempt to insert a value already present in the tree
    //if (code == Disk_DuplicateKey)
    //std::cout << "DISK LEVEL INSERT - Duplicate key ignored.\n";
    
    // if the insert was not successful, get the node
    // call to GetNode will return the node or if there are no values, it returns the root
    if (code == Disk_InsertNotComplete)
    {
        long root0 = root;
        
        // call to GetNode, return the root or the value of the current node
        root = GetNode();
        
        // since there is a value in the node, set the n field to 1
        RootNode.n = 1;
        
        // set the root node first value
        RootNode.k[0] = xNew;
        
        // set the first pointer to the root value
        RootNode.p[0] = root0;
        
        // set the second pointer to the new value of the node
        RootNode.p[1] = pNew;
        
        // write the node to the BTree
        WriteNode(root, RootNode);
    }
}

/**
 insert the value to the BTree on disk
 
 @param r the root for the BTree
 @param x the data type being inserted
 @param y a pointer to the value to be inserted
 @param q the pointer to the value to be inserted
 
 @return Disk_Success, Disk_DuplicateKey or Disk_InsertNotComplete
 
 */
status_disk LsmLevelDisk::ins(long r, dtype x, dtype &y, long &q)
{
    
    // Insert x in pointer *this. If it doesn't complete with return of Disk_Success, the
    // data type y and the pointer q remain to be inserted.
    long pNew, pFinal;
    long i, j, n;
    dtype xNew, kFinal;
    status_disk code;
    
    // if the root is not set
    if (r == NIL)
    {
        // set the new value's pointer as the root
        q = NIL;
        
        // set the new value as the root
        y = x;
        
        return Disk_InsertNotComplete;
    }
    
    // create a new node object
    node_disk Node, NewNode;
    
    // read the node
    ReadNode(r, Node);
    
    // and set the number of values in the node
    n = Node.n;
    
    // use NodeSearch to locate the node in the BTree
    i = NodeSearch(x, Node.k, n);
    
    // if the new value to be inserted is already present in the node, return message for duplicate key
    if (i < n && x.value == Node.k[i].value) return Disk_DuplicateKey;
    
    // get the message for an attempt to insert the new value
    code = ins(Node.p[i], x, xNew, pNew);
    
    // return if the disk insert was not resulting in insert not being complete
    if (code != Disk_InsertNotComplete) return code;
    
    // Insertion in LsmLevelDisk did not completely succeed;
    // try to insert xNew and pNew in the current node:
    if (n < M - 1)
    {
        i = NodeSearch(xNew, Node.k, n);
        for (j=n; j>i; j--)
        {
            Node.k[j] = Node.k[j-1];
            Node.p[j+1] = Node.p[j];
        }
        
        Node.k[i] = xNew; Node.p[i+1] = pNew; ++Node.n;
        WriteNode(r, Node);
        return Disk_Success;
    }
    
    // The current node is full (n == M - 1) and will be split.
    if (i == M - 1)
    {
        kFinal = xNew;
        pFinal = pNew;
        
    } else {
        
        kFinal = Node.k[M-2];
        pFinal = Node.p[M-1];
        
        for (j=M-2; j>i; j--)
        {
            Node.k[j] = Node.k[j-1];
            Node.p[j+1] = Node.p[j];
        }
        
        Node.k[i] = xNew;
        Node.p[i+1] = pNew;
    }
    
    int h = (M - 1)/2;
    
    // Pass item k[h] in the middle of the augmented sequence back via parameter y, so that it
    // can move upward in the tree.
    // y and q are passed on
    y = Node.k[h];
    
    //pass a pointer to the newly created node back via parameter q:
    // next higher level in the tree
    // the values are on the left side, move values with the node
    q = GetNode();
    
    // The values p[0],k[0],p[1],...,k[h-1],p[h] belong to
    // the left of k[h] and are kept in *r: to maintain the correct order in the BTree
    Node.n = h;
    // p[h+1],k[h+1],p[h+2],...,k[M-2],p[M-1],kFinal,pFinal
    // belong to the right of k[h] and are moved to *q:
    NewNode.n = M - 1 - h;
    
    // if the node size and shifting is not in place, return Disk insert not complete
    // so that the insert function can continue to shift the data values
    for (j=0; j < NewNode.n; j++)
    {  NewNode.p[j] = Node.p[j + h + 1];
        NewNode.k[j] = (j < NewNode.n - 1 ? Node.k[j + h + 1] : kFinal);
    }
    
    NewNode.p[NewNode.n] = pFinal;
    WriteNode(r, Node); WriteNode(q, NewNode);
    return Disk_InsertNotComplete;
}


// a counter to keep track of how many values are passed to the next level
long getNValuesDiskCounter = 0;

/**
 function used to copy values from one disk level to the subsequent level
 
 @param previous_level_pointer pointer to the level to be copied from
 @param current_level_pointer pointer to the level to copy to
 @param r the root of the BTree
 @param total_disk_values_count how many values are in the previous level to be copied from
 @param total_to_pass_next_level the total amount to pass between levels
 
 */
void LsmLevelDisk::getNValues(LsmLevelDisk* previous_level_pointer, LsmLevelDisk* current_level_pointer, long r, long total_disk_values_count, long total_to_pass_next_level)
{
    // if the root is not empty
    if (r != NIL)
    {
        // get the root node using ReadNode by passing the root
        int i;
        node_disk Node; ReadNode(r, Node);
        
        int j;
        
        // for each value in the node
        for (i=0; i < Node.n; i++)
        {
            // if the counter has not exceeded its limit
            if (getNValuesDiskCounter <= total_to_pass_next_level) {
                
                // increment the counter by 1
                getNValuesDiskCounter += 1;
                
                // create a new dtype to represent the new key value pair
                dtype x;
                x.key = getNValuesDiskCounter;
                x.value = Node.k[i].value;
                
                // insert the new value to the current level
                (*current_level_pointer).insert(x);
                
                // delete the value from the previous level
                (*previous_level_pointer).DelNode(x);
                
            } else {
                // return since the counter has expired
                return;
            }
        }
        // this function is called recursively in order to get all values from each subsequent node
        // in the BTree until the counter has exceeded its limit
        for (j=0; j <= Node.n; j++)
        {
            LsmLevelDisk::getNValues(previous_level_pointer, current_level_pointer, Node.p[j], total_disk_values_count, total_to_pass_next_level);
        }
    }
}

/**
 function used to copy values from one disk level to the subsequent level
 
 @param previous_level_pointer pointer to the level to be copied from
 @param current_level_pointer pointer to the level to copy to
 @param r the root of the BTree
 @param total_to_pass_next_level the total amount to pass between levels
 
 */
void LsmLevelDisk::diskLevelCopy(LsmLevelDisk* previous_level_pointer, LsmLevelDisk* current_level_pointer, long r, long total_to_pass_next_level )
{
    // get the values count for the BTree to copy from
    long total_disk_values_count = getValuesCount(r);
    
    // make a call to getNValues in order to execute the copy from previous level to current level
    LsmLevelDisk::getNValues(previous_level_pointer, current_level_pointer, r, total_disk_values_count, total_to_pass_next_level);
    
    // set the counter back to 0 after the call to getNValues for the next time diskLevelCopy is called
    getNValuesDiskCounter = 0;
    
}


// a counter to get the total values count required for the function
long LsmLevelDisk::disk_total_values_count = 0;

/**
 function used to get the values count for the BTree of a disk level
 
 @param r the root of the BTree
 @return the values count
 
 */
long LsmLevelDisk::disk_calculateValuesCount(long r)
{
    // if the root is not empty
    if (r != NIL)
    {
        
        int i;
        // read the root node
        node_disk Node; ReadNode(r, Node);
        
        // and iterate through the nodes, incrementing the counter each time you add a value to
        // the total count
        for (i=0; i < Node.n; i++) disk_total_values_count++;
        
        // use recursion to find the total in each node
        for (i=0; i <= Node.n; i++) disk_calculateValuesCount(Node.p[i]);
    }
    
    // return the total once complete
    return disk_total_values_count;
    
}

/**
 function used to copy values from one disk level to the subsequent level
 
 @param r the root of the BTree
 @return the values count
 
 */
long LsmLevelDisk::getValuesCount(long r)
{
    long root = r;
    
    // set the counter to start at 0
    LsmLevelDisk::disk_total_values_count = 0;
    
    return disk_calculateValuesCount(root);
}

/**
 function used to print the BTree to console
 
 @param r the root of the BTree
 @param nSpace the space, printing on the console, between each value used for visual quality
 
 */
void LsmLevelDisk::pr(long r, int nSpace)
{  if (r != NIL)
    {
        int i;
        // set space before the first value
        std::cout << std::setw(nSpace) << "";
        
        // read the root node
        node_disk Node; ReadNode(r, Node);
        
        // for each value in the current node, print the value followed by a space
        for (i=0; i < Node.n; i++) std::cout << Node.k[i].value << " ";
        std::cout << std::endl;
        
        // call this function recursively until the entire BTree has been printed
        for (i=0; i <= Node.n; i++) pr(Node.p[i], nSpace+8);
    }
}

/**
 function used to locate a node, based on binary search
 
 @param x the value to search for the node by
 @param a a pointer to the data type
 @param n the size of the node to search for
 @return the location of the node in the BTree
 
 */
long LsmLevelDisk::NodeSearch(dtype x, const dtype *a, long n)const
{
    // navigate the BTree to find the node
    long middle, left=0, right = n - 1;
    
    if (x.value <= a[left].value) return 0;
    if (x.value > a[right].value) return n;
    
    // while there are still ndoes to navigate
    while (right - left > 1)
    {
        // find the middle point in the tree
        middle = (right + left)/2;
        // use a ternary operator to set to the middle value in the search
        (x.value <= a[middle].value ? right : left) = middle;
    }
    return right;
}

/**
 function used to delete the node
 
 @param x the value to delete
 
 */
void LsmLevelDisk::DelNode(dtype x)
{
    long root0;
    
    // switch statement to handle various cases associated with an attempt to delete
    switch (del(root, x))
    {
            // if the disk for the level cannot be found, break out
        case Disk_NotFound:
            //std::cout << x << " not found.\n";
            break;
        case Disk_Underflow:
            root0 = root;
            root = RootNode.p[0]; FreeNode(root0);
            if (root != NIL) ReadNode(root, RootNode);
            break;
    }
}

/**
 function used to delete the node
 
 @param x the value to delete
 @param r the root of the BTree
 @return the status of the attempt to delete from the BTree
 
 */
status_disk LsmLevelDisk::del(long r, dtype x)
{
    
    // if the root is empty, return
    if (r == NIL) return Disk_NotFound;
    
    
    node_disk Node;
    ReadNode(r, Node);
    long i, j, pivot, n = Node.n;
    
    // k[i] means Node.k[i]
    dtype *k = Node.k;
    
    // set the minimum n size
    // this represents the minimum number of data items
    // for any node except the root node
    const int nMin = (M - 1)/2;
    
    status_disk code;
    
    // p[i] means Node.p[i]
    // set a long pointer to the Node.p value in addition to the left and right p values
    long *p = Node.p, pL, pR;
    i = NodeSearch(x, k, n);
    
    // is the pointer not pointing to another node?
    // is this a leaf node?
    if (p[0] == NIL)
    {
        // we reached the end of the leaf node, return not found
        if (i == n || x.value < k[i].value) return Disk_NotFound;
        
        // move to the next node
        // shifting to the left
        for (j=i+1; j < n; j++)
        {
            k[j-1] = k[j];
            p[j] = p[j+1];
        }
        
        // we need to find if there is Underflow
        // Underflow is defined as a node containing 1 value
        // look for a transfer of a value from a nearby node to fill this void
        Node.n--;
        WriteNode(r, Node);
        return Node.n >= (r==root ? 1 : nMin) ?
        Disk_Success : Disk_Underflow;
    }
    
    // if *r is an interior node
    if (i < n && x.value == k[i].value)
    {
        // x found in an interior node. Go to left child
        // and follow a path all the way to a leaf,
        // using rightmost branches:
        long q = p[i], q1;
        int nq;
        node_disk Node1;
        
        // navigate the depth of the tree
        for (;;)
        {
            // read the next node
            ReadNode(q, Node1);
            
            // find the size of the node
            nq = Node1.n;
            
            // set pointer to current node's value pointer
            q1 = Node1.p[nq];
            
            // break out if the pointer does not point to a new node
            if (q1 == NIL) break;
            q = q1;
        }
        
        // Exchange k[i] (= x) with rightmost item in leaf:
        k[i] = Node1.k[nq-1];
        Node1.k[nq - 1] = x;
        WriteNode(r, Node);
        WriteNode(q, Node1);
    }
    
    // Delete x in leaf of suLsmLevelDisk with root p[i]:
    code = del(p[i], x);
    
    if (code != Disk_Underflow) return code;
    
    // There is underflow, too little values are present in the node;
    // borrow, and, if necessary, merge -
    
    // Too few data items in node *p[i]
    node_disk NodeL, NodeR;
    
    if (i > 0)
    {
        pivot = i - 1; pL = p[pivot]; ReadNode(pL, NodeL);
        
        // borrow from left sibling
        if (NodeL.n > nMin)
        {
            // k[pivot] between pL and pR:
            pR = p[i];
            
            // Increase contents of *pR, borrowing from *pL:
            ReadNode(pR, NodeR);
            NodeR.p[NodeR.n + 1] = NodeR.p[NodeR.n];
            
            for (j=NodeR.n; j>0; j--)
            {
                NodeR.k[j] = NodeR.k[j-1];
                NodeR.p[j] = NodeR.p[j-1];
            }
            NodeR.n++;
            NodeR.k[0] = k[pivot];
            NodeR.p[0] = NodeL.p[NodeL.n];
            k[pivot] = NodeL.k[--NodeL.n];
            WriteNode(pL, NodeL);
            WriteNode(pR, NodeR);
            WriteNode(r, Node);
            return Disk_Success;
        }
    }
    pivot = i;
    if (i < n)
    {
        pR = p[pivot+1];
        ReadNode(pR, NodeR);
        
        // repeat the procoess to borrow from the right sibling
        if (NodeR.n > nMin)
        {
            // k[pivot] between pL and pR:
            pL = p[pivot];
            ReadNode(pL, NodeL);
            // Increase contents of *pL, borrowing from *pR:
            NodeL.k[NodeL.n] = k[pivot];
            NodeL.p[NodeL.n + 1] = NodeR.p[0];
            k[pivot] = NodeR.k[0];
            NodeL.n++;
            NodeR.n--;
            for (j=0; j < NodeR.n; j++)
            {  NodeR.k[j] = NodeR.k[j+1];
                NodeR.p[j] = NodeR.p[j+1];
            }
            NodeR.p[NodeR.n] = NodeR.p[NodeR.n + 1];
            WriteNode(pL, NodeL);
            WriteNode(pR, NodeR);
            WriteNode(r, Node);
            return Disk_Success;
        }
    }
    
    // borrowing from left or right is not possible, do a merge
    pivot = (i == n ? i - 1 : i);
    pL = p[pivot];
    pR = p[pivot+1];
    
    // Add k[pivot] and *pR to *pL:
    ReadNode(pL, NodeL);
    ReadNode(pR, NodeR);
    NodeL.k[NodeL.n] = k[pivot];
    NodeL.p[NodeL.n + 1] = NodeR.p[0];
    
    for (j=0; j < NodeR.n; j++)
    {
        NodeL.k[NodeL.n + 1 + j] = NodeR.k[j];
        NodeL.p[NodeL.n + 2 + j] = NodeR.p[j+1];
    }
    
    NodeL.n += 1 + NodeR.n;
    FreeNode(pR);
    for (j=i+1; j < n; j++)
    {
        k[j-1] = k[j]; p[j] = p[j+1];
    }
    Node.n--;
    WriteNode(pL, NodeL);
    WriteNode(r, Node);
    return
    Node.n >= (r == root ? 1 : nMin) ? Disk_Success : Disk_Underflow;
}

/**
 function used to read a node
 
 @param r the root of the BTree
 @param Node the node to find on disk
 
 */
void LsmLevelDisk::ReadNode(long r, node_disk &Node)
{
    // place a lock on this point in the function
    // only allow one thread at a time to access the read node functionality
    // so that threads don't share access to the root r value and Node value
    std::lock_guard<std::mutex> guard(Node_mutex);
    
    if (r == NIL) return;
    
    // if the root is valid and not empty, the node is equal to the root node
    if (r == root && RootNode.n > 0) Node = RootNode; else
    {
        // start at the root node in the file
        file.seekg(r, std::ios::beg);
        
        // find the node in the file
        // pass the size of a node_disk struct in order to navigate the file
        file.read((char*)&Node, sizeof(node_disk));
    }
}

/**
 function used to write a node
 
 @param r the root of the BTree
 @param Node the node to find on disk
 
 */
void LsmLevelDisk::WriteNode(long r, const node_disk &Node)
{
    // place a lock on this point in the function
    // only allow one thread at a time to access the write node functionality
    // so that threads don't share access to the root r value and Node value
    std::lock_guard<std::mutex> guard(Node_mutex);
    
    // write the node to disk by finding the next position in the file
    // first see if the r value is the root
    if (r == root) RootNode = Node;
    file.seekp(r, std::ios::beg);
    file.write((char*)&Node, sizeof(node_disk));
}

/**
 function used to read from the beginning of the BTree, the root
 */
void LsmLevelDisk::ReadStart()
{  long start[2];
    file.seekg(0L, std::ios::beg);
    file.read((char *)start, 2 * sizeof(long));
    root = start[0];
    FreeList = start[1];
    ReadNode(root, RootNode);
}

long LsmLevelDisk::GetNode()  // Modified (see also the destructor ~LsmLevelDisk)
{  long r;
    node_disk Node;
    if (FreeList == NIL)
    {  file.seekp(0L, std::ios::end); // Allocate space on disk; if
        r = file.tellp() & ~1;    // file length is an odd number,
        WriteNode(r, Node);       // the new node will overwrite
    }  else                      // signature byte at end of file
    {  r = FreeList;
        ReadNode(r, Node);        // To update FreeList:
        FreeList = Node.p[0];     // Reduce the free list by 1
    }
    return r;
}


/**
 function used to pointer for root node
 
 @param r the root node
 
 */
void LsmLevelDisk::FreeNode(long r)
{
    node_disk Node;
    // find the root node
    ReadNode(r, Node);
    
    // set the root node pointer
    Node.p[0] = FreeList;
    FreeList = r;
    WriteNode(r, Node);
}

/**
 function used to search for a value in the BTree
 
 @param x the data value to search for
 
 */
bool LsmLevelDisk::search_value(dtype x)
{
    //std::cout << "Search path:\n";
    long i, j, n;
    long r = root;
    node_disk Node;
    
    while (r != NIL)
    {
        // find the root node
        ReadNode(r, Node);
        
        // get the size of the node
        n = Node.n;
        
        // print the node values as searched the tree
        //for (j=0; j<Node.n; j++) std::cout << " " << Node.k[j].value;
        //std::cout << std::endl;
        
        // find a node using binary search
        i = NodeSearch(x, Node.k, n);
        
        // if the value is found
        if (i < n && x.value == Node.k[i].value)
        {
            //std::cout << "Key " << x.value << " found in position " << i
            //<< " of last displayed node.\n";
            
            // return true for the search
            return true;
        }
        r = Node.p[i];
    }
    
    // the value was not found, return false
    //std::cout << "Key " << x.value << " not found.\n";
    return false;
}

/**
 function used to find the file size for each BTree on disk
 
 @param fileName the name of the file on disk to get the size of
 @return the file size
 
 */
long LsmLevelDisk::getFileSize(const std::string &fileName)
{
    // the files are binary, use std::ifstream::binary for the calculation
    std::ifstream file(fileName.c_str(), std::ifstream::in | std::ifstream::binary);
    
    // if the file cannot be opened, return -1
    if(!file.is_open())
    {
        return -1;
    }
    
    // check the file, from start to end, to calculate the size
    file.seekg(0, std::ios::end);
    long fileSize = file.tellg();
    file.close();
    
    return fileSize;
}
