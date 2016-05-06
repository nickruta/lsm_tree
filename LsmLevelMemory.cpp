/**
 C++11 - GCC Compiler
 LsmLevelMemory.h
 
 Creates a BTree representation in memory for the c0 level of the Lsm Tree.
 It provides utility methods to access the BTree and print statistics.
 
 @author N. Ruta
 @version 1.0 4/01/16
 */
#include "LsmLevelMemory.h"
#include "LsmTree.h"

#include <vector>

/**
 insert the value to the BTree in memory
 
 @param dtype the data to be inserted
 
 */
void LsmLevelMemory::insert(dtype x)
{
    
    // create new instances for the pointer and the value to be inserted
    node *pNew;
    dtype xNew;
    
    // do the insert, return the status of the execution
    status code = ins(root, x, xNew, pNew);
    
    //if (code == DuplicateKey)
    //cout << "Duplicate key ignored.\n";
    
    // if the insert was not successful, get the node
    // call  will return the node or if there are no values, it returns the root
    if (code == InsertNotComplete)
    {
        node *root0 = root;
        root = new node;
        // since there is a value in the node, set the n field to 1
        root->n = 1;
        
        // set the root node first value
        root->k[0] = xNew;
        
        // set the first pointer to the root value
        root->p[0] = root0;
        
        // set the second pointer to the new value of the node
        root->p[1] = pNew;
    }
}

/**
 insert the value to the BTree in memory
 
 @param r pointer to the root node for the BTree
 @param x the data type being inserted
 @param y a pointer to the value to be inserted
 @param u the pointer to the value to be inserted
 
 @return Disk_Success, Disk_DuplicateKey or Disk_InsertNotComplete
 
 */
status LsmLevelMemory::ins(node *r, dtype x, dtype &y, node* &q)
{
    // Insert x in *this. If not completely successful, the
    // integer y and the pointer q remain to be inserted.
    // Return value:
    //    Success, DuplicateKey or InsertNotComplete.
    node *pNew, *pFinal;
    
    long i, j, n;
    dtype xNew, kFinal;
    status code;
    
    // if the root is not set
    if (r == NULL)
    {
        // set the new value's pointer as the root
        q = NULL;
        
        // set the new value as the root
        y = x;
        
        return InsertNotComplete;
    }
    
    // and set the number of values in the node
    n = r->n;
    
    // use NodeSearch to locate the node in the BTree
    i = NodeSearch(x, r->k, n);
    
    // if the new value to be inserted is already present in the node, return message for duplicate key
    if (i < n && x.value == r->k[i].value) return DuplicateKey;
    
    // get the message for an attempt to insert the new value
    code = ins(r->p[i], x, xNew, pNew);
    
    // return if the disk insert was not resulting in insert not being complete
    if (code != InsertNotComplete) return code;
    
    // Insertion in subtree did not completely succeed;
    // try to insert xNew and pNew in the current node:
    if (n < M - 1)
    {
        
        i = NodeSearch(xNew, r->k, n);
        
        for (j=n; j>i; j--)
        {
            r->k[j] = r->k[j-1];
            r->p[j+1] = r->p[j];
        }
        
        r->k[i] = xNew;
        r->p[i+1] = pNew; ++r->n;
        
        return Success;
    }
    // Current node is full (n == M - 1) and will be split.
    // Pass item k[h] in the middle of the augmented
    // sequence back via parameter y, so that it
    // can move upward in the tree. Also, pass a pointer
    // to the newly created node back via parameter q:
    if (i == M - 1)
    {
        kFinal = xNew;
        pFinal = pNew;
    } else
    {
        kFinal = r->k[M-2];
        pFinal = r->p[M-1];
        
        for (j=M-2; j>i; j--)
        {
            r->k[j] = r->k[j-1];
            r->p[j+1] = r->p[j];
        }
        r->k[i] = xNew; r->p[i+1] = pNew;
    }
    int h = (M - 1)/2;
    
    // y and q are passed on to the
    y = r->k[h];
    
    // next higher level in the tree
    q = new node;
    
    // The values p[0],k[0],p[1],...,k[h-1],p[h] belong to
    // the left of k[h] and are kept in *r:
    r->n = h;
    
    // p[h+1],k[h+1],p[h+2],...,k[M-2],p[M-1],kFinal,pFinal
    // belong to the right of k[h] and are moved to *q:
    q->n = M - 1 - h;
    
    for (j=0; j < q->n; j++)
    {
        q->p[j] = r->p[j + h + 1];
        q->k[j] = (j < q->n - 1 ? r->k[j + h + 1] : kFinal);
    }
    
    q->p[q->n] = pFinal;
    
    return InsertNotComplete;
}

/**
 function used to print the BTree to console
 
 @param r the root of the BTree
 @param nSpace the space, printing on the console, between each value used for visual quality
 
 */
void LsmLevelMemory::pr(const node *r, int nSpace)const
{
    // if the root is valid and not empty
    if (r)
    {
        // print the values found in the first node
        int i;
        cout << setw(nSpace) << "";
        
        for (i=0; i < r->n; i++)
            cout << setw(3) << r->k[i].value << " ";
        cout << endl;
        
        // iterate through the entire tree, running this function recursively
        for (i=0; i <= r->n; i++) pr(r->p[i], nSpace+8);
    }
}

// a counter to keep track of how many values are passed to the next level
long getNValuesCounter = 0;

std::vector<long> getNValuesArray;

/**
 function used to copy values from c0 memory level to the c1 level
 
 @param c pointer to the level to be copied to
 @param c0 pointer to the level to copy from
 @param r the root of the BTree
 @param c0TotalValues the total values in the memory level BTree
 @param copyAllFromC0 boolean to determine if all of c0 should be copied
 @param c0_percentage_to_copy what percentage of c0 should be copied?
 
 */
void LsmLevelMemory::getNValues(LsmLevelDisk *c, LsmLevelMemory* c0, const node *r, long c0TotalValues, bool copyAllFromC0, double c0_percentage_to_copy)const
{
    
    // the limit set to only copy a certain amount from the level
    long counter_limit;
    
    // if all should be copied from c0
    if (copyAllFromC0)
    {
        counter_limit = c0TotalValues;
        
    // else only copy a certain percetage, defined as a tunable parameter, from c0
    } else {
        counter_limit = (long) c0TotalValues * c0_percentage_to_copy;
    }
    
    // if the root is valid
    if (r)
    {
        // loop through the root node and continue throughout the tree calling this function recursively
        long i;
        for (i=0; i < r->n; i++)
        {
            // get more values to copy and delete from the levels until the limit is reached
            if (getNValuesCounter < counter_limit)
            {
                
                dtype x;
                x.key = 0;
                x.value = r->k[i].value;
                
                (*c).insert(x);

                (*c0).DelNode(x);
                
                // increment the counter before exiting
                getNValuesCounter += 1;
                
            }
        }
        // run this function recursively until the total required has been reached
        for (i=0; i <= r->n; i++)
        {
            getNValues(c, c0, r->p[i], c0TotalValues, copyAllFromC0, c0_percentage_to_copy);
        }
    }
}


long getNValuesCounterVector = 0;
std::vector<long> getNValuesArrayVector;
/**
 function used to copy the vector of c0 to c1 on disk
 
 @param c0VectorPtr pointer to the copy of the memory level vector
 @param c a pointer to the c1 level BTree
 @param c0TotalValues the total values of the c0 vector to be copied to c1
 @param copyAllFromC0 should all of c0 be copied?
 @param c0_percentage_to_copy if not all of c0 should be copied, what percentage should be copied?
 
 */
void LsmLevelMemory::getNValuesVector(std::vector<long>* c0VectorPtr, LsmLevelDisk *c, long c0TotalValues, bool copyAllFromC0, double c0_percentage_to_copy)const
{
    
    // the counter to monitor the amount to copy from c0 vector to c1 BTree
    long counter_limit;
    
    // if copy all of c0 else copy only the percentage of c0 desired
    if (copyAllFromC0)
    {
        counter_limit = c0TotalValues;
    } else {
        counter_limit = (long) c0TotalValues * c0_percentage_to_copy;
    }
    
    // for each value of the vector at c0
    // insert the value to c1
    // stop when the counter limit has been reached
    long i;
    for (i=0; i < counter_limit; i++)
    {
        dtype x;
        x.key = getNValuesCounterVector;
        x.value = (*c0VectorPtr).at(i);
        
        // insert to c1
        (*c).insert(x);
        
        getNValuesCounterVector += 1;
        
        // c0 vector will be cleared, no need to delete from it here
    }
}


/**
 function used to copy the BTree of c0 to c1 on disk
 
 @param c0VectorPtr pointer to the copy of the memory level vector
 @param c a pointer to the c1 level BTree
 @param copyAllFromC0 should all of c0 be copied?
 @param c0_percentage_to_copy if not all of c0 should be copied, what percentage should be copied?
 
 */
void LsmLevelMemory::memoryLevelCopy(LsmLevelDisk *c, LsmLevelMemory *c0, bool copyAllFromC0, double c0_percentage_to_copy)
{
    node *r = root;
    long c0TotalValues = (*c0).getValuesCount();
    
    // all getNValues to make the copy
    getNValues(c, c0, r, c0TotalValues, copyAllFromC0, c0_percentage_to_copy);
    
    // clear the array to make way for the next call to memoryLevelCopy from LsmTree
    getNValuesArray.clear();
    
    // reset the counter for getNValues back to 0 for the next call to memoryLevelCOpy from LsmTree
    getNValuesCounter = 0;
}

/**
 function used to copy the vector of c0 to c1 on disk
 
 @param c0VectorPtr pointer to the copy of the memory level vector
 @param c a pointer to the c1 level BTree
 @param copyAllFromC0 should all of c0 be copied?
 @param c0_percentage_to_copy if not all of c0 should be copied, what percentage should be copied?
 
 */
void LsmLevelMemory::memoryLevelCopyVector(std::vector<long>* c0VectorPtr, LsmLevelDisk *c, bool copyAllFromC0, double c0_percentage_to_copy)
{
    
    // get the total values to be copied to c1 from the memory level vector
    long c0TotalValues = (*c0VectorPtr).size();
    
    // call to make the actual copy from c0 to c1
    getNValuesVector(c0VectorPtr, c, c0TotalValues, copyAllFromC0, c0_percentage_to_copy);
    
    
    // clear the array to make way for the next call to memoryLevelCopy from LsmTree
    getNValuesArray.clear();
    
    // reset the counter for getNValues back to 0 for the next call to memoryLevelCOpy from LsmTree
    getNValuesCounterVector = 0;
}


/**
 function used to get the total values count for the memory resident BTree
 
 @param r the root node pointer
 
 */
long LsmLevelMemory::total_values_count = 0;
long LsmLevelMemory::calculateValuesCount(const node *r)const
{
    if (r)
    {
        // use a recursive call to to aggregate the values count
        int i;
        for (i=0; i < r->n; i++) total_values_count++;
        for (i=0; i <= r->n; i++) calculateValuesCount(r->p[i]);
    }
    return total_values_count;
}


/**
 function used to get the values count of the c0 memory level
 
 @return the values count
 
 */
long LsmLevelMemory::getValuesCount()
{
    // get the root node to start
    node *r = root;
    
    // set the counter to start at 0
    LsmLevelMemory::total_values_count = 0;
    
    return calculateValuesCount(r);
}

/**
 function used to locate a node, based on binary search
 
 @param x the value to search for the node by
 @param a a pointer to the data type
 @param n the size of the node to search for
 @return the location of the node in the BTree
 
 */
long LsmLevelMemory::NodeSearch(dtype x, const dtype *a, long n)const
{
    // traverse the tree looking for the value 
    int i=0;
    while (i < n && x.value > a[i].value) i++;
    return i;
}

/**
 function used to search for a value in the BTree
 
 @param x the data value to search for
 
 */
bool LsmLevelMemory::search_value(dtype x)const
{  //cout << "Search path:\n";
    long i, j, n;
    node *r = root;
    while (r)
    {
        n = r->n;
        //for (j=0; j<r->n; j++) cout << " " << r->k[j].value;
        //cout << endl;
        i = NodeSearch(x, r->k, n);
        if (i < n && x.value == r->k[i].value)
        {
            // the key was found
            //cout << "Key " << x.value << " found in position " << i
            //<< " of last displayed node.\n";
            return true;
        }
        r = r->p[i];
    }
    // the key was not found
    //cout << "Key " << x.value << " not found.\n";
    return false;
}

/**
 Function to delete a value associated with a node in the BTree
 
 @param x the key value pair to be deleted
 */
void LsmLevelMemory::DelNode(dtype x)
{
    node *root0;
    
    // call to delete the value
    // uses switches for different cases as the return 'code'
    switch (del(root, x))
    {
        // if the value is not found, exit
        case NotFound:
            //cout << x.value << " not found.\n";
            break;
            
        // if removal causses underflow, the tree needs to be adjusted
        case Underflow:
            root0 = root;
            root = root->p[0];
            delete root0;
            break;
    }
}

/**
 function used to delete the node
 
 @param x the value to delete
 @param r the root of the BTree
 @return the status of the attempt to delete from the BTree
 
 */
status LsmLevelMemory::del(node *r, dtype x)
{
    // if the root is empty, return
    if (r == NULL) return NotFound;
    
    long i, j, pivot, n = r->n;
    
    // k[i] means r->k[i]
    dtype *k = r->k;
    
    // set the minimum n size
    // this represents the minimum number of data items
    // for any node except the root node
    const int nMin = (M - 1)/2;
    
    status code;
    
    // p[i] means r->p[i]
    // set a long pointer to the Node.p value in addition to the left and right p values
    node **p = r->p, *pL, *pR;
    i = NodeSearch(x, k, n);
    
    // is the pointer not pointing to another node?
    // is this a leaf node?
    if (p[0] == NULL)
    {
        // we reached the end of the leaf node, return not found
        if (i == n || x.value < k[i].value) return NotFound;
        
        // move to the next node
        // shifting to the left
        for (j=i+1; j < n; j++)
        {
            k[j-1] = k[j];
            p[j] = p[j+1];
        }
        return
        // we need to find if there is Underflow
        // Underflow is defined as a node containing 1 value
        // look for a transfer of a value from a nearby node to fill this void
        --r->n >= (r==root ? 1 : nMin) ? Success : Underflow;
    }
    // *r is an interior node
    if (i < n && x.value == k[i].value)
    {
        // x found in an interior node. Go to left child
        // *p[i] and follow a path all the way to a leaf,
        // using rightmost branches:
        node *q = p[i], *q1; int nq;
        
        // navigate the depth of the tree
        for (;;)
        {
            // find the size of the node
            nq = q->n;
            
            // set pointer to current node's value pointer
            q1 = q->p[nq];
            
            // break out if the pointer does not point to a new node
            if (q1 == NULL) break;
            q = q1;
        }
        // Exchange k[i] (= x) with rightmost item in leaf:
        k[i] = q->k[nq-1];
        q->k[nq - 1] = x;
    }
    
    // Delete x in leaf of subtree with root p[i]:
    code = del(p[i], x);
    
    // There is underflow, too little values are present in the node;
    // borrow, and, if necessary, merge -
    if (code != Underflow) return code;
    
    // There is underflow; borrow, and, if necessary, merge:
    // Too few data items in node *p[i]
    
    // Borrow from left sibling
    if (i > 0 && p[i-1]->n > nMin)
    {
        // k[pivot] between pL and pR:
        pivot = i - 1;
       
        pL = p[pivot];
        pR = p[i];
        
        // Increase contents of *pR, borrowing from *pL:
        pR->p[pR->n + 1] = pR->p[pR->n];
        
        for (j=pR->n; j>0; j--)
        {
            pR->k[j] = pR->k[j-1];
            pR->p[j] = pR->p[j-1];
        }
        
        pR->n++;
        pR->k[0] = k[pivot];
        pR->p[0] = pL->p[pL->n];
        k[pivot] = pL->k[--pL->n];
        return Success;
    }
    
    // repeat the procoess to borrow from the right sibling
    if (i<n && p[i+1]->n > nMin)
    {
        // k[pivot] between pL and pR:
        pivot = i;
        pL = p[pivot]; pR = p[pivot+1];
        
        // Increase contents of *pL, borrowing from *pR:
        pL->k[pL->n] = k[pivot];
        pL->p[pL->n + 1] = pR->p[0];
        k[pivot] = pR->k[0];
        pL->n++;
        pR->n--;
        for (j=0; j < pR->n; j++)
        {
            pR->k[j] = pR->k[j+1];
            pR->p[j] = pR->p[j+1];
        }
        pR->p[pR->n] = pR->p[pR->n + 1];
        
        // success, return code
        return Success;
    }
    // Merge; neither borrow left nor borrow right possible.
    pivot = (i == n ? i - 1 : i);
    pL = p[pivot];
    pR = p[pivot+1];
    // Add k[pivot] and *pR to *pL:
    pL->k[pL->n] = k[pivot];
    pL->p[pL->n + 1] = pR->p[0];
    
    for (j=0; j < pR->n; j++)
    {
        pL->k[pL->n + 1 + j] = pR->k[j];
        pL->p[pL->n + 2 + j] = pR->p[j+1];
    }
    pL->n += 1 + pR->n;
    delete pR;
    
    for (j=i+1; j < n; j++)
    {
        k[j-1] = k[j];
        p[j] = p[j+1];
    }
    return
    --r->n >= (r == root ? 1 : nMin) ? Success : Underflow;
}