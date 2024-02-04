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

long int arraySize;
int threadCount;
pthread_barrier_t mybarrier;

struct ThreadControlBlock {
    int id;
    long int *localBlock;
    long int blockSize;
    long int *regularSample;
};

long int timeDiff (timeval tv2, timeval tv) {
    long int microseconds = (tv2.tv_sec - tv.tv_sec) * 1000000 + ((int)tv2.tv_usec - (int)tv.tv_usec);
    return microseconds;
}


int comparator(const void * p, const void * q) {
    long * l = (long int *) p;
    long * r = (long int *) q;

    if (*l > * r)
        return 1;
    else if (*l == * r)
        return 0;
    else
        return -1;
}

void * sortAndSampleLocal(void * arg) {
    auto * myTCB = (struct ThreadControlBlock *) arg;
    long int * block = myTCB->localBlock;
    long int size = myTCB->blockSize;

    qsort(block, size, sizeof(long int), comparator);

    long int * regularSample = myTCB->regularSample;
    long int w = arraySize / (threadCount * threadCount);

    for (int i = 0; i < threadCount; i++) {
        regularSample[i] = block[i * w];
        //std::cout << regularSample[i] << " ";
    }
    //std::cout <<endl;
    //printf("thread %d: I'm ready...\n", myTCB->id);
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
    auto * data = new long int[arraySize];
    struct ThreadControlBlock TCB[threadCount];
    pthread_t ThreadID[threadCount];
    long int blockSize = floor(arraySize / threadCount);
    pthread_setconcurrency(threadCount);
    // "Randomly" generating data
    srandom((unsigned) time(nullptr));
    for (long int i = 0; i < arraySize; i++) {
        data[i] = (long int) (random());
    }

    struct timeval start_time{};
    gettimeofday(&start_time, nullptr);

    pthread_barrier_init(&mybarrier, nullptr, threadCount);
    // Phase 1
    for (int i = 0; i < threadCount; i++) {
        TCB[i].id = i;
        TCB[i].localBlock = &data[i * blockSize];
        TCB[i].regularSample = new long int[threadCount];

        if (i == threadCount - 1)
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
}