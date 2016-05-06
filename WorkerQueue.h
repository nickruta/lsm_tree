/**
 C++11 - GCC Compiler
 WorkerQueue.h
 
 An implementation of a worker queue used for thread pooling in order to process reads and deletes in the LSM Tree. It makes use of the c++11 threading primitives, lambda function s and move semantics. 
 
 It is provided a function at constructure time which defines how to process one item of work. To pass work to the queue, the function operator of the object is called.
 
 When the destructor is called, when the object reaches the end of its scope, all remaining items are processed and background threads are joined.
 
 This implementation is based on that found at https://blind.guru/simple_cxx11_workqueue.html and was modified to use the dtype key value pair and read and delete functions for this LSM Tree application.
 
 @author N. Ruta
 @version 1.0 4/01/16
 */
#include <condition_variable>
#include <mutex>
#include <queue>
#include <stdexcept>
#include <thread>
#include <vector>

template <typename Type, typename Queue = std::queue<Type>>
class distributor: Queue, std::mutex, std::condition_variable {
    typename Queue::size_type capacity;
    bool done = false;
    std::vector<std::thread> threads;
    
public:
    template<typename Function>
    distributor( Function function
                , //unsigned int concurrency = std::thread::hardware_concurrency() to get the actual threads available to the system or set manually for testing
                unsigned int concurrency = 4
                , typename Queue::size_type max_items_per_thread = 1
                )
    : capacity{concurrency * max_items_per_thread}
    {
        
        // manage cases in which the queue is not properly initialized
        if (not concurrency)
            throw std::invalid_argument("Concurrency must be non-zero");
        if (not max_items_per_thread)
            throw std::invalid_argument("Max items per thread must be non-zero");
        
        for (unsigned int count {0}; count < concurrency; count += 1)
            threads.emplace_back(static_cast<void (distributor::*)(Function)>
                                 (&distributor::consume), this, function);
            }
    
    distributor(distributor &&) = default;
    distributor &operator=(distributor &&) = delete;
    
    // deconstructor call makes a call to thread.join() and notifies that work is complete using notify_all()
    ~distributor()
    {
        {
            std::lock_guard<std::mutex> guard(*this);
            done = true;
            notify_all();
        }
        for (auto &&thread: threads) thread.join();
    }
    
    void operator()(Type &&value)
    {
        std::unique_lock<std::mutex> lock(*this);
        while (Queue::size() == capacity) wait(lock);
        Queue::push(std::forward<Type>(value));
        notify_one();
    }
    
private:
    template <typename Function>
    void consume(Function process)
    {
        // get a lock,  pop off from the queue the work, and process the work based on the data type passed from the LSM Tree
        std::unique_lock<std::mutex> lock(*this);
        while (true) {
            if (not Queue::empty()) {
                Type item { std::move(Queue::front()) };
                Queue::pop();
                notify_one();
                lock.unlock();
                process(item);
                lock.lock();
            } else if (done) {
                break;
            } else {
                wait(lock);
            }
        }
    }
};