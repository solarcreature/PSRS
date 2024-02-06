#include <iostream>
#include <sys/time.h>
#include <queue>
//#define DEBUG
using namespace std;


// taken from https://blog.albertarmea.com/post/47089939939/using-pthreadbarrier-on-mac-os-x and modified a bit for correctness
#ifdef __APPLE__

#ifndef PTHREAD_BARRIER_H_
#define PTHREAD_BARRIER_H_

typedef int pthread_barrierattr_t;

typedef struct
{
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    int count;
    int tripCount;
} pthread_barrier_t;


int pthread_barrier_init(pthread_barrier_t *barrier, const pthread_barrierattr_t *attr, unsigned int count)
{
    if(count == 0)
    {
        errno = EINVAL;
        return -1;
    }
    if(pthread_mutex_init(&barrier->mutex, nullptr) != 0 || pthread_cond_init(&barrier->cond, nullptr) != 0) {
        return -1;
    }
    barrier->tripCount = count;
    barrier->count = 0;

    return 0;
}

int pthread_barrier_destroy(pthread_barrier_t *barrier)
{
    pthread_cond_destroy(&barrier->cond);
    pthread_mutex_destroy(&barrier->mutex);
    return 0;
}

int pthread_barrier_wait(pthread_barrier_t *barrier)
{
    pthread_mutex_lock(&barrier->mutex);
    ++(barrier->count);
    if(barrier->count >= barrier->tripCount)
    {
        barrier->count = 0;
        pthread_cond_broadcast(&barrier->cond);
        pthread_mutex_unlock(&barrier->mutex);
        return 1;
    }
    else
    {
        pthread_cond_wait(&barrier->cond, &(barrier->mutex));
        pthread_mutex_unlock(&barrier->mutex);
        return 0;
    }
}

#endif // PTHREAD_BARRIER_H_
#endif // __APPLE__

long arraySize;
int threadCount;
long * values;
vector<vector<pair<long, long>>> globalPartitions;
pthread_barrier_t mybarrier;

struct ThreadControlBlock {
    int id;
    long *localBlock;
    long blockSize;
    vector<pair<long, long>> regularSample;
    vector<pair<long, long>> pivots;
    vector<pair<long, long>> partitions;
    vector<pair<long, long>> truePartitions;
    vector<long> mergedPartition;
};

class QueueObject
{
public:
    long value;
    long start;
    long end;
    QueueObject(long value, long start, long end) {
        this->value = value;
        this->start = start;
        this->end = end;
    }
    long getValue() { return value; }
};

// simplified form of https://www.binarytides.com/get-time-difference-in-microtime-in-c/. Reinforced correctness by ChatGpt :)
long timeDiff (timeval tv2, timeval tv) {
    long microseconds = (tv2.tv_sec - tv.tv_sec) * 1000000 + ((int)tv2.tv_usec - (int)tv.tv_usec);
    return microseconds;
}


// https://stackoverflow.com/questions/17638499/using-qsort-to-sort-an-array-of-long-long-int-not-working-for-large-nos
int comparator(const void * p, const void * q) {
    long * l = (long *) p;
    long * r = (long *) q;

    if (*l > * r)
        return 1;
    else if (*l == * r)
        return 0;
    else
        return -1;
}

long binary_search(long *block, long size, long pivot) {
    long start = 0;
    long end = size - 1;
    long mid;

    while (start <= end) {
        mid = (start + end) / 2;
        if (block[mid] == pivot)
            return mid;
        else if (block[mid] < pivot)
            start = mid + 1;
        else
            end = mid - 1;
    }
    return end;
}

void *sortAndSampleLocal(void * arg) {
    auto * myTCB = (struct ThreadControlBlock *) arg;
    long * block = myTCB->localBlock;
    long size = myTCB->blockSize;

    qsort(block, size, sizeof(long), comparator);

    long w = arraySize / (threadCount * threadCount);

    for (int i = 0; i < threadCount; i++) {
        myTCB->regularSample.emplace_back(block[i * w], (i * floor(arraySize / threadCount)) + (i * w));
    }

    pthread_barrier_wait(&mybarrier);
    pthread_exit(nullptr);
}

void *createPartitions(void *arg) {
    auto *myTCB = (struct ThreadControlBlock *)arg;
    long *block = myTCB->localBlock;
    long start = 0;
    long end;
    for (auto pivot : myTCB->pivots) {
        end = binary_search(block, myTCB->blockSize, pivot.first);
        myTCB->partitions.emplace_back(start, end);
        myTCB->truePartitions.emplace_back(start + myTCB->id * floor(arraySize / threadCount), end + myTCB->id * floor(arraySize / threadCount));
        start = end + 1;
    }

    myTCB->partitions.emplace_back(start, myTCB->blockSize - 1);
    myTCB->truePartitions.emplace_back(start + myTCB->id * floor(arraySize / threadCount), myTCB->blockSize - 1 + myTCB->id * floor(arraySize / threadCount));

    pthread_barrier_wait(&mybarrier);
    pthread_exit(nullptr);
}

void *exchangePartitions(void *arg) {
    auto *myTCB = (struct ThreadControlBlock *)arg;
    for (int i = 0; i < threadCount; i++) {
        globalPartitions[i].push_back(myTCB->truePartitions[i]);
    }

    pthread_barrier_wait(&mybarrier);
    pthread_exit(nullptr);
}

// https://www.geeksforgeeks.org/implement-min-heap-using-stl/ for comparison
class minComp {
public:
    int operator() (QueueObject q1, QueueObject& q2) {
        return q1.getValue() > q2.getValue();
    }
};

// heavily inspired by https://medium.com/@vidyasagarr7/mastering-the-k-way-merge-algorithmic-pattern-for-technical-interviews-6db0e00a049f
void *successiveMerge(void *arg) {
    auto *myTCB = (struct ThreadControlBlock *)arg;

    priority_queue <QueueObject, vector<QueueObject>, minComp> pq;
    vector<long> result;
    for (auto & i : globalPartitions[myTCB->id]) {
        if (i.first <= i.second) {
            long t = *(values + i.first);
            pq.emplace(t, i.first, i.second);
        }
    }

    while (!pq.empty()) {
        QueueObject smallest = pq.top();
        pq.pop();
        result.push_back(smallest.getValue());
        long start = smallest.start;
        long end = smallest.end;
        if (start < end) {
            long t = *(values + start + 1);
            pq.emplace(t, start + 1, end);
        }
    }

    myTCB->mergedPartition.insert(myTCB->mergedPartition.end(), result.begin(), result.end());

    pthread_barrier_wait(&mybarrier);
    pthread_exit(nullptr);
}

int main(int argc, char ** argv) {

    if (argc < 3) {
        std::cout << "Missing required argument.\n"
                  << "Required arguments: <number of keys to sort> <number of threads>\n";
        return 1;
    }

    // Setting parameters
    arraySize = stol(argv[1]);
    threadCount = stoi(argv[2]);

    for (int i = 0; i < threadCount; i++) {
        globalPartitions.emplace_back();
    }

    values = new long [arraySize];
    struct ThreadControlBlock TCB[threadCount];
    pthread_t ThreadID[threadCount];
    long blockSize = floor(arraySize / threadCount);
    pthread_setconcurrency(threadCount);
    // "Randomly" generating data
    srandom((unsigned) time(nullptr));

#ifdef DEBUG
    cout << "Original ";
#endif
    for (long i = 0; i < arraySize; i++) {
        values[i] = (long) (random());
#ifdef DEBUG
        cout << values [i] << " ";
#endif
    }


    long *arr_copy = new long[arraySize];
    //cout << "Original ";
    for (long i = 0; i < arraySize; i++) {
        arr_copy[i] = values[i];
        //cout << values[i] << " ";
    }
    cout << endl;

    struct timeval sequential_start{};
    gettimeofday(&sequential_start, nullptr);
    sort(arr_copy, arr_copy + arraySize);
    struct timeval sequential_end{};
    gettimeofday(&sequential_end, nullptr);
    cout << "Time taken for sequential sort: " << timeDiff(sequential_end, sequential_start) << " microseconds" << endl;

#ifdef DEBUG
    cout << "Sorted ";
    for (long i = 0; i < arraySize; i++) {
        cout << arr_copy[i] << " ";
    }
    cout << endl;
#endif

    struct timeval start_time{};
    gettimeofday(&start_time, nullptr);

    pthread_barrier_init(&mybarrier, nullptr, threadCount);

    for (int i = 0; i < threadCount; i++) {
        TCB[i].id = i;
        TCB[i].localBlock = &values[i * blockSize];

        if (i == threadCount - 1) // Incase of uneven partitions
            TCB[i].blockSize = arraySize - (i * blockSize);
        else
            TCB[i].blockSize = blockSize;

        pthread_create(&(ThreadID[i]), nullptr, sortAndSampleLocal, (void *) &TCB[i]);
    }

    for (int i = 0; i < threadCount; i++) {
        pthread_join(ThreadID[i], nullptr);
    }

    pthread_barrier_destroy(&mybarrier);

    struct timeval phase_1{};
    gettimeofday(&phase_1, nullptr);
    cout << "Time taken for phase 1: " << timeDiff(phase_1, start_time) << " microseconds" << endl;

    vector<pair<long, long>> localSamples;

    for (int i = 0; i < threadCount; i++) {
#ifdef DEBUG
        cout << "Thread " << i << " Local Sorted" <<endl;
        for (long k = 0; k < TCB[i].blockSize; k++) {
            cout << TCB[i].localBlock[k] << " ";
        }
        cout << endl;

        cout << "Thread " << i << " Samples" <<endl;
#endif
        localSamples.insert(localSamples.end(), TCB[i].regularSample.begin(), TCB[i].regularSample.end());
#ifdef DEBUG
        for (int j = 0; j < threadCount; j++) {
            cout << TCB[i].regularSample[j].first <<  " " << "Index " << TCB[i].regularSample[j].second;
        }
        cout << endl << endl;
#endif
    }

    sort(localSamples.begin(), localSamples.end());

#ifdef DEBUG
    cout << "Sorted Gathered Sample  "<<endl;
    for (int i = 0 ; i < threadCount * threadCount; i++) {
        cout << localSamples[i].first << " " << "Index " << localSamples[i].second;
    }
    cout << endl;
    cout << endl;
#endif

    long rho = floor(threadCount / 2);
    vector<pair<long, long>> pivots;
#ifdef DEBUG
    cout << "Pivots "<<endl;
#endif
    for (int i = 1; i < threadCount; i++) {
#ifdef DEBUG
        cout << localSamples[i * threadCount + rho -1].first << " " << "Index " << localSamples[i * threadCount + rho -1].second ;
#endif
        pivots.push_back(localSamples[i * threadCount + rho - 1]);
    }

    pthread_barrier_init(&mybarrier, nullptr, threadCount);

    for (int i = 0; i < threadCount; i++) {
        TCB[i].pivots = pivots;
        pthread_create(&(ThreadID[i]), nullptr, createPartitions, (void *)&TCB[i]);
    }

    for (int i = 0; i < threadCount; i++) {
        pthread_join(ThreadID[i], nullptr);
    }

    pthread_barrier_destroy(&mybarrier);

#ifdef DEBUG
    for (int i = 0; i < threadCount; i++) {
        cout << "Partition thread " << i << endl;
        for (auto p: TCB[i].truePartitions) {
            for (long l = p.first; l <= p.second; l++) {
                cout<< values[l] << " ";
            }
            cout << endl;
        }
        cout << endl;
    }
#endif

    struct timeval phase_2{};
    gettimeofday(&phase_2, nullptr);
    cout << "Time taken for phase 2: " << timeDiff(phase_2, phase_1) << " microseconds" << endl;

    pthread_barrier_init(&mybarrier, nullptr, threadCount);
    for (int i = 0; i < threadCount; i++) {
        pthread_create(&(ThreadID[i]), nullptr, exchangePartitions, (void *)&TCB[i]);
    }

    for (int i = 0; i < threadCount; i++) {
        pthread_join(ThreadID[i], nullptr);
    }

    pthread_barrier_destroy(&mybarrier);

    struct timeval phase_3{};
    gettimeofday(&phase_3, nullptr);
    cout << "Time taken for phase 3: " << timeDiff(phase_3, phase_2) << " microseconds" << endl;

#ifdef DEBUG
    for (int i = 0; i < threadCount; i++) {
        cout << "Re assigned partitions: thread " << i << endl;
        for (auto p: globalPartitions[i]) {
            for (long j = p.first; j < p.second + 1; j++) {
                cout << values[j] << " ";
            }
            cout << endl;
        }
        cout << endl;
    }
#endif

    pthread_barrier_init(&mybarrier, nullptr, threadCount);
    for (int i = 0; i < threadCount; i++) {
        pthread_create(&(ThreadID[i]), nullptr, successiveMerge, (void *)&TCB[i]);
    }

    for (int i = 0; i < threadCount; i++) {
        pthread_join(ThreadID[i], nullptr);
    }

    pthread_barrier_destroy(&mybarrier);
    struct timeval phase_4{};
    gettimeofday(&phase_4, nullptr);
    cout << "Time taken for phase 4: " << timeDiff(phase_4, phase_3) << " microseconds" << endl;
    cout << "Time taken as a whole: " << timeDiff(phase_4, start_time) << " microseconds" << endl;
    vector<long> final_vector;
    for (int i = 0; i < threadCount; i++) {
        final_vector.insert(final_vector.end(), TCB[i].mergedPartition.begin(), TCB[i].mergedPartition.end());
    }

#ifdef DEBUG
    cout << "After Merge" <<endl;
    for (size_t i = 0; i < final_vector.size(); ++i) {
        cout << final_vector[i] << " ";
    }
    cout << endl;

    cout << "Sorted " << endl;
    for (long i = 0; i < arraySize; i++) {
        cout << arr_copy[i] << " ";
    }
    cout << endl;
#endif

    bool equivalent = true;
    if (final_vector.size() == arraySize) {

        for (size_t i = 0; i < final_vector.size(); ++i) {
                if (final_vector[i] != *(arr_copy + i)) {
                    equivalent = false;
                    break;
                }
        }
        cout << endl;
    }
    else {
        equivalent = false;
    }

    // Print the result
    if (equivalent) {
        std::cout << "The final output is equal to sequential sort result." << std::endl;
    } else {
        std::cout << "The final output is NOT equal to sequential sort result." << std::endl;
    }

    // free memory
    free(values);
    free(arr_copy);
}