#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <stdio.h>

#define IMMEDIATE_SHUTDOWN	1
#define GRACEFULL_SHUTDOWN	2

typedef struct threadpool_t threadpool_t;
typedef struct threadpool_task_t threadpool_task_t;

struct threadpool_t {
	pthread_mutex_t lock;
	pthread_cond_t notify;
	pthread_t *threads;
	int thread_count;
	int thread_started;
	threadpool_task_t *queue;
	int head, tail;
	int queue_size, task_count;
	int shutdown;
};

struct threadpool_task_t {
	void (*func)(void *);
	void *argu;
};

static void *threadpool_worker(void *argu);
int threadpool_destroy(threadpool_t *pool, int shutdown);

threadpool_t *
threadpool_create(int thread_count, int queue_size)
{
	int i;

	threadpool_t *pool = malloc(sizeof(threadpool_t));
	if (pool == NULL)
		return NULL;

	memset(pool, 0, sizeof(threadpool_t));
	pool->queue_size = queue_size;

	pool->threads = malloc(sizeof(pthread_t)*thread_count);
	if (pool->threads == NULL)
		goto free_pool;

	pool->queue = malloc(sizeof(threadpool_task_t)*queue_size);
	if (pool->queue == NULL)
		goto free_threads;
	
	if (pthread_mutex_init(&pool->lock, NULL) != 0)
		goto free_queue;
	if (pthread_cond_init(&pool->notify, NULL) != 0)
		goto destroy_lock;

	for (i = 0; i < thread_count; i++) {
		if (pthread_create(&pool->threads[i], NULL, threadpool_worker, pool) != 0) {
			threadpool_destroy(pool, IMMEDIATE_SHUTDOWN);
			return NULL;
		}
		pool->thread_count++;
		pool->thread_started++;
	}

	return pool;

destroy_lock:
	pthread_mutex_destroy(&pool->lock);
free_queue:
	free(pool->queue);
free_threads:
	free(pool->threads);
free_pool:
	free(pool);

	return NULL;
}

int threadpool_destroy(threadpool_t *pool, int flags)
{
	int i;
	int thread_err = 0;
	pthread_mutex_lock(&pool->lock);
	
	if (pool->shutdown) {
		pthread_mutex_unlock(&pool->lock);
		return 0;
	}
	pool->shutdown = (flags == 0 ? IMMEDIATE_SHUTDOWN : GRACEFULL_SHUTDOWN);
	if (pthread_cond_broadcast(&pool->notify) != 0)
		perror("pthread_cond_broadcast");
	pthread_mutex_unlock(&pool->lock);

	for (i = 0; i < pool->thread_count; i++) {
		if (pthread_join(pool->threads[i], NULL) != 0)
			thread_err++;
	}

	if (!thread_err) {
		pthread_cond_destroy(&pool->notify);
		pthread_mutex_destroy(&pool->lock);
		free(pool->queue);
		free(pool->threads);
		free(pool);
	}

	return thread_err;
}

static void *threadpool_worker(void *argu)
{
	threadpool_t *pool = argu;
	threadpool_task_t task;

	for (;;) {
		pthread_mutex_lock(&pool->lock);	

		while (pool->task_count == 0 && !pool->shutdown)
			pthread_cond_wait(&pool->notify, &pool->lock);
		if (pool->shutdown == IMMEDIATE_SHUTDOWN ||
			(pool->shutdown == GRACEFULL_SHUTDOWN && pool->task_count == 0)) {
			break;
		}

		task.func = pool->queue[pool->head].func;
		task.argu = pool->queue[pool->head].argu;
		if (++pool->head == pool->queue_size)
			pool->head = 0;
		pool->task_count--;

		pthread_mutex_unlock(&pool->lock);

		task.func(task.argu);
	}

	pool->thread_started--;
	pthread_mutex_unlock(&pool->lock);
	pthread_exit(NULL);
}

int threadpool_add(threadpool_t *pool, void (*func)(void *), void *argu)
{
	pthread_mutex_lock(&pool->lock);

	if (pool->task_count == pool->queue_size) {
		pthread_mutex_unlock(&pool->lock);
		return -1;
	}

	if (pool->shutdown) {
		pthread_mutex_unlock(&pool->lock);
		return -1;
	}

	pool->queue[pool->tail].func = func;
	pool->queue[pool->tail].argu = argu;
	if (++pool->tail == pool->queue_size)
		pool->tail = 0;
	pool->task_count++;

	pthread_cond_signal(&pool->notify);
	pthread_mutex_unlock(&pool->lock);

	return 0;
}
