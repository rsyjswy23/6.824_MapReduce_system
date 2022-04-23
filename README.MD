## GOAL:
Implement a distributed MapReduce system based on [Google's MapReduce paper in 2004.](http://static.googleusercontent.com/media/research.google.com/en//archive/mapreduce-osdi04.pdf)


## My Inplementation & Design Diagram:
This MapReduce system consisting of 3 major partsss:

![Design Diagram](./diagram.svg)


# MapReduce functionality Example: (2 text files, nReduce = 2)

    file 1: Hello, my name is Sue, your name?
    file 2: Hello, your name is Tom.

    After perform Mapfunc() on each file:

    file 1: [{"Hello", 1}, {"my", 1}, {"name", 2}, {"is", 1}, {"Sue", 1}, {"your", 1}]
    file 2: [{"Hello", 1}, {"you", 1}, {"name", 1}, {"is", 1}, {"Tom", 1}]

    After partition: calculate (ihash(key) % nReduce) and pass intermediate kv pairs to the corresponding reduce machines:

    file 1: {"Hello", 1}, {"my", 1}, {"name", 2}, {"is", 1}, {"Sue", 1}, {"your", 1}
    file 2: {"Hello", 1}, {"name", 1}, {"is", 1}, {"Tom", 1}, {"your", 1}
    |------------reduce machine 1---------|------------reduce machine 2-----------| 

    {"Hello",[1,1]},{"my",[1]},{"name",[2,1]},{"is", [1,1]} ---> reduce machine 1
    {"Sue", 1}, {"your", [1,1]}, {"Tom", 1}                 ---> reduce machine 2

    After perform Reducefunc():
    reduce machine 1 ouput: 
    Hello 2
    my    1
    name  3
    is    2
    reduce machine 2 ouput: 
    Sue   1
    Tom   1
    your  2

# Rules:
- Use Go’s race detector to detect unsynchronised read/writes to the same memory and reports them as a failure (which it is).

- In a real system the workers would run on a bunch of different machines, but for this lab you'll run them all on a single machine.

- To ensure that nobody observes partially written files in the presence of crashes, the MapReduce paper mentions the trick of using a temporary file and atomically renaming it once it is completely written. Use ioutil.TempFile to create a temporary file and os.Rename to atomically rename it.

## Challenges & Optimizations:

1. The coordinator, as an RPC server, will be concurrent and need to use sync.Mutex to lock shared data(ToDoTask channel and status map).

2. In the paper, after each map task is done, both worker and coordinator can commit intermediate file. Originally I followed this idea and allowed worker and coordinator can locked and write the shared data. But both the data races and synchronization bugs occurred. 

Optimization: To simplify the design, minimize numbers of RPC calls and decouple the write and commit process, I only allow coordinator to commit.

3. Use switch--case to simplify RequestJob() logic.

4. The coordinator can’t reliably distinguish between crashed workers, workers that are alive but have stalled for some reason, and workers that are executing but too slowly to be useful. In the paper, chapter 3.6: 
    <i>"We have a general mechanism to alleviate the problem of stragglers. When a MapReduce operation is close to completion, the master schedules backup executions of the remaining in-progress tasks. The task is marked as completed whenever either the primary or the backup execution completes. We have tuned this mechanism so that it typically increases the computational resources
    used by the operation by no more than a few percent. We have found that this significantly reduces the time to complete large MapReduce operations. As an example, the sort program described in Section 5.3 takes 44%
    longer to complete when the backup task mechanism is
    disabled." </i>

One possible solution is to have the workers send hearbeat and task completion status to Coordinator, so Coordinator can resend this task to other worker when this task is close to completion. 

Optimization: To simplified the design, I added an attribute Deadline(10s after it sends to worker) of each task. When coordinator executes go routine to iterate every task and pass unassigned task to the ToDoTask channel, if the current task has not been completed by Deadline, then will mark this task uncomplete, pass it to ToDoTask channel, and wait for re-assginment to any work again.


## Notes & Highlight:
GO, concurrent programming, parallelism, mutual exclusions(mutexes), channel, RPC, [Go’s net/rpc package](https://ops.tips/gists/example-go-rpc-client-and-server/), thread, process, MapReduce...

- Why Go?
  good support for threads
  convenient RPC
  type safe
  garbage-collected (no use after freeing problems)
  threads + GC is particularly attractive!
  relatively simple

- Threads
  a useful structuring tool, but can be tricky
  Go calls them goroutines; everyone else calls them threads
  Thread = "thread of execution"
  threads allow one program to do many things at once
  each thread executes serially, just like an ordinary non-threaded program
  the threads share memory
  each thread includes some per-thread state:
    program counter, registers, stack

- Why threads?
  They express concurrency, which you need in distributed systems
  I/O concurrency
    Client sends requests to many servers in parallel and waits for replies.
    Server processes multiple client requests; each request may block.
    While waiting for the disk to read data for client X,
      process a request from client Y.
  Multicore performance
    Execute code in parallel on several cores.
  Convenience
    In background, once per second, check whether each worker is still alive.

- Threading challenges:
  shared data 
    e.g. what if two threads do n = n + 1 at the same time?
      or one thread reads while another increments?
    this is a "race" -- and is usually a bug
    -> use locks (Go's sync.Mutex)
    -> or avoid sharing mutable data
  coordination between threads
    e.g. one thread is producing data, another thread is consuming it
      how can the consumer wait (and release the CPU)?
      how can the producer wake up the consumer?
    -> use Go channels or sync.Cond or WaitGroup
  deadlock
    cycles via locks and/or communication (e.g. RPC or Go channels)