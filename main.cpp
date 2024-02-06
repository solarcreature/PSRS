#include <iostream>
#include <sys/time.h>
using namespace std;

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
pthread_barrier_t mybarrier;

struct ThreadControlBlock {
    int id;
    long *localBlock;
    long blockSize;
    long *regularSample;
    vector<long> pivots;
    vector<pair<int, int>> partitions;
};

long timeDiff (timeval tv2, timeval tv) {
    long microseconds = (tv2.tv_sec - tv.tv_sec) * 1000000 + ((int)tv2.tv_usec - (int)tv.tv_usec);
    return microseconds;
}

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

void * sortAndSampleLocal(void * arg) {
    auto * myTCB = (struct ThreadControlBlock *) arg;
    long * block = myTCB->localBlock;
    long size = myTCB->blockSize;

    qsort(block, size, sizeof(long), comparator);

    long * regularSample = myTCB->regularSample;
    long w = arraySize / (threadCount * threadCount);

    for (int i = 0; i < threadCount; i++) {
        regularSample[i] = block[i * w];
    }

    pthread_barrier_wait(&mybarrier);
    pthread_exit(nullptr);
}

void *createPartitions(void *arg) {
    auto *myTCB = (struct ThreadControlBlock *)arg;
    long *block = myTCB->localBlock;
    long start = 0;
    long end;

    for (long pivot : myTCB->pivots) {
        end = binary_search(block, myTCB->blockSize, pivot);
        myTCB->partitions.emplace_back(start, end);
        start = end + 1;
    }

    myTCB->partitions.emplace_back(start, myTCB->blockSize - 1);

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
    auto * data = new long [arraySize];
    struct ThreadControlBlock TCB[threadCount];
    pthread_t ThreadID[threadCount];
    long blockSize = floor(arraySize / threadCount);
    pthread_setconcurrency(threadCount);
    // "Randomly" generating data
    srandom((unsigned) time(nullptr));
    for (long i = 0; i < arraySize; i++) {
        data[i] = (long) (random());
    }

    struct timeval start_time{};
    gettimeofday(&start_time, nullptr);

    pthread_barrier_init(&mybarrier, nullptr, threadCount);
    // Phase 1
    for (int i = 0; i < threadCount; i++) {
        TCB[i].id = i;
        TCB[i].localBlock = &data[i * blockSize];
        TCB[i].regularSample = new long[threadCount];

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

    vector<long> localSamples;

    for (int i = 0; i < threadCount; i++) {
        localSamples.insert(localSamples.end(), TCB[i].regularSample, TCB[i].regularSample + threadCount);
        delete TCB[i].regularSample;
    }

    sort(localSamples.begin(), localSamples.end());

    long rho = floor(threadCount / 2);
    vector<long> pivots;

    for (int i = 1; i < threadCount; i++) {
        pivots.push_back(localSamples[i * threadCount + rho]);
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

    struct timeval phase_2{};
    gettimeofday(&phase_2, nullptr);
    cout << "Time taken for phase 2: " << timeDiff(phase_2, phase_1) << " microseconds" << endl;
}