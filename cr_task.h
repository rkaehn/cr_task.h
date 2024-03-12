#ifndef CR_TASK_H
#define CR_TASK_H

typedef struct cr_task_t cr_task_t;
typedef struct cr_executor_t cr_executor_t;

cr_task_t* cr_task_create(cr_executor_t* executor, void (*func)(void*), void* args);
void cr_task_retain(cr_task_t* task);
void cr_task_release(cr_task_t* task);
void cr_task_request_signal(cr_task_t* task, cr_task_t* task_signaling);
void cr_task_wait_request_signal(cr_task_t* task, cr_task_t* task_signaling);
void cr_task_wait(cr_task_t* task);
void cr_task_signal(cr_task_t* task);
void cr_task_run(cr_task_t* task);
void cr_task_sync(cr_task_t* task);
void cr_task_run_sync(cr_task_t* task);

cr_executor_t* cr_executor_create(int worker_count);

#ifdef CR_TASK_IMPL
#include <assert.h>
#include <threads.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define _cr_task_log(...) // do { printf("[cr_task]: "__VA_ARGS__); } while (0)

#define _CR_TASK_FLAG_W (1llu << 63)
#define _CR_TASK_FLAG_S (1llu << 31)
#define _CR_TASK_FLAG_SYNC (1llu << 30)
#define _CR_POOL_BATCH 8llu
#define _CR_POOL_ITEM 16llu
#define _CR_POOL_IDX_MASK ((1llu << (_CR_POOL_BATCH + _CR_POOL_ITEM)) - 1llu)

typedef struct _cr_sync_t _cr_sync_t;
typedef struct _cr_task_ref_t _cr_task_ref_t;
typedef struct _cr_pool_t _cr_pool_t;

struct _cr_sync_t {
    mtx_t mutex;
    cnd_t cond;
    int is_done;
};

struct _cr_task_ref_t {
    _Atomic uint32_t next_idx_pool;
    _Atomic uint32_t next_idx_list;
    cr_task_t* task;
};

struct _cr_pool_t {
    mtx_t mutex;
    void* batches[1llu << _CR_POOL_BATCH];
    uint64_t batch_count;
    uint64_t item_size;
    _Atomic uint64_t c;
};

struct cr_task_t {
    _Atomic uint32_t next_idx_pool;
    _Atomic uint32_t next_idx_list;
    uint64_t idx;
    cr_executor_t* executor;
    _cr_sync_t* sync;
    _Atomic uint64_t wait;
    _Atomic uint64_t signal;
    void (*func)(void*);
    void* args;
};
static_assert(sizeof(cr_task_t) == 64, "");

struct cr_executor_t {
    _cr_pool_t task_pool;
    _cr_pool_t task_ref_pool;
    _Atomic uint64_t c;
    mtx_t mutex;
    cnd_t cond;
    thrd_t* workers;
};

void _cr_pool_init(_cr_pool_t* pool, uint64_t item_size);
uint64_t _cr_pool_pop(_cr_pool_t* pool);
void _cr_pool_push(_cr_pool_t* pool, uint64_t idx);

void _cr_task_end_wait(cr_task_t* task);
void _cr_task_begin_signal(cr_task_t* task);

void _cr_executor_schedule_task(cr_executor_t* executor, cr_task_t* task);
int _cr_executor_worker_func(void* args);

void _cr_pool_init(_cr_pool_t* pool, uint64_t item_size) {
    mtx_init(&pool->mutex, mtx_plain);
    memset(pool->batches, 0, sizeof(pool->batches));
    pool->batch_count = 0;
    pool->item_size = item_size;
    atomic_store_explicit(&pool->c, _CR_POOL_IDX_MASK, memory_order_release);
}

uint64_t _cr_pool_pop(_cr_pool_t* pool) {
    _cr_task_log("pool pop (item size: %llu)\n", pool->item_size);
    uint64_t c = atomic_load_explicit(&pool->c, memory_order_acquire);
    uint64_t n, idx;
    do {
        idx = c & _CR_POOL_IDX_MASK;
        if (idx == _CR_POOL_IDX_MASK) {
            mtx_lock(&pool->mutex);
            c = atomic_load_explicit(&pool->c, memory_order_acquire);
            idx = c & _CR_POOL_IDX_MASK;
            if (idx == _CR_POOL_IDX_MASK) {
                _cr_task_log("pool alloc (item size: %llu)\n", pool->item_size);
                uint64_t batch_idx = pool->batch_count++;
                assert(batch_idx < (1llu << _CR_POOL_BATCH) && "out of memory");
                pool->batches[batch_idx] = malloc((1llu << _CR_POOL_ITEM) * pool->item_size);
                idx = batch_idx << _CR_POOL_ITEM;
                for (uint64_t i = 1; i < (1llu << _CR_POOL_ITEM) - 1llu; i++) {
                    void* item = (char*)(pool->batches[batch_idx]) + i * pool->item_size;
                    atomic_store_explicit((_Atomic uint32_t*)item, idx | (i + 1llu), memory_order_relaxed);
                }
                void* item = (char*)(pool->batches[batch_idx]) + ((1llu << _CR_POOL_ITEM) - 1llu) * pool->item_size;
                atomic_store_explicit((_Atomic uint32_t*)item, _CR_POOL_IDX_MASK, memory_order_relaxed);
                atomic_store_explicit(&pool->c, idx | 1llu, memory_order_release);
                mtx_unlock(&pool->mutex);
                return idx;
            }
            mtx_unlock(&pool->mutex);
        }
        uint64_t batch_idx = idx >> _CR_POOL_ITEM;
        uint64_t item_idx = idx & ((1llu << _CR_POOL_ITEM) - 1llu);
        void* item = (char*)(pool->batches[batch_idx]) + item_idx * pool->item_size;
        n = ((c + (1llu << 32)) & ~_CR_POOL_IDX_MASK) | atomic_load_explicit((_Atomic uint32_t*)item, memory_order_relaxed);
    } while (!atomic_compare_exchange_weak_explicit(&pool->c, &c, n, memory_order_acq_rel, memory_order_acquire));
    return idx;
}

void _cr_pool_push(_cr_pool_t* pool, uint64_t idx) {
    _cr_task_log("pool push (item size: %llu)\n", pool->item_size);
    uint64_t batch_idx = idx >> _CR_POOL_ITEM;
    uint64_t item_idx = idx & ((1llu << _CR_POOL_ITEM) - 1llu);
    void* item = (char*)(pool->batches[batch_idx]) + item_idx * pool->item_size;
    uint64_t c = atomic_load_explicit(&pool->c, memory_order_acquire);
    uint64_t n;
    do {
        atomic_store_explicit((_Atomic uint32_t*)item, c & _CR_POOL_IDX_MASK, memory_order_relaxed);
        n = ((c + (1llu << 32)) & ~_CR_POOL_IDX_MASK) | idx;
    } while (!atomic_compare_exchange_weak_explicit(&pool->c, &c, n, memory_order_acq_rel, memory_order_acquire));
}

cr_task_t* cr_task_create(cr_executor_t* executor, void (*func)(void*), void* args) {
    uint64_t idx = _cr_pool_pop(&executor->task_pool);
    uint64_t batch_idx = idx >> _CR_POOL_ITEM;
    uint64_t item_idx = idx & ((1llu << _CR_POOL_ITEM) - 1llu);
    cr_task_t* task = (cr_task_t*)executor->task_pool.batches[batch_idx] + item_idx;
    atomic_store_explicit(&task->next_idx_list, _CR_POOL_IDX_MASK, memory_order_relaxed);
    task->idx = idx;
    task->executor = executor;
    task->sync = NULL;
    task->func = func;
    task->args = args;
    atomic_store_explicit(&task->wait, 0, memory_order_release);
    atomic_store_explicit(&task->signal, _CR_POOL_IDX_MASK, memory_order_release);
    return task;
}

void cr_task_retain(cr_task_t* task) {
    uint64_t c = atomic_fetch_add_explicit(&task->signal, 1llu << 48, memory_order_relaxed);
    assert(c & _CR_TASK_FLAG_S ? (c >> 48) : 1 && "task no longer exists");
    assert((c >> 48) < 0xffff && "retain count too high");
}

void cr_task_release(cr_task_t* task) {
    uint64_t c = atomic_fetch_sub_explicit(&task->signal, 1llu << 48, memory_order_acq_rel);
    assert((c >> 48) > 0 && "unbalanced retain/release count");
    if ((c & _CR_TASK_FLAG_S) && (c >> 48) == 1) _cr_pool_push(&task->executor->task_pool, task->idx);
}

void cr_task_request_signal(cr_task_t* task, cr_task_t* task_signaling) {
    uint64_t c = atomic_load_explicit(&task_signaling->signal, memory_order_acquire);
    uint64_t idx = _cr_pool_pop(&task_signaling->executor->task_ref_pool);
    uint64_t batch_idx = idx >> _CR_POOL_ITEM;
    uint64_t item_idx = idx & ((1llu << _CR_POOL_ITEM) - 1llu);
    _cr_task_ref_t* task_ref = (_cr_task_ref_t*)task_signaling->executor->task_ref_pool.batches[batch_idx] + item_idx;
    task_ref->task = task;
    uint64_t n;
    do {
        assert(((c >> 32) & 0xffff) < 0xffff && "too many signal requests");
        if ((c & _CR_TASK_FLAG_S) == 0) {
            atomic_store_explicit(&task_ref->next_idx_list, c & _CR_POOL_IDX_MASK, memory_order_relaxed);
            n = ((c + (1llu << 32)) & ~_CR_POOL_IDX_MASK) | idx;
        } else break;
    } while (!atomic_compare_exchange_weak_explicit(&task_signaling->signal, &c, n, memory_order_acq_rel, memory_order_acquire));
    if (c & _CR_TASK_FLAG_S) {
        _cr_pool_push(&task_signaling->executor->task_ref_pool, idx);
        cr_task_signal(task);
    }
}

void cr_task_wait_request_signal(cr_task_t* task, cr_task_t* task_signaling) {
    cr_task_wait(task);
    cr_task_request_signal(task, task_signaling);
}

void cr_task_wait(cr_task_t* task) {
    uint64_t c = atomic_fetch_add_explicit(&task->wait, 1, memory_order_relaxed);
    assert(c != _CR_TASK_FLAG_W && "unexpected wait request");
}

void cr_task_signal(cr_task_t* task) {
    uint64_t c = atomic_fetch_sub_explicit(&task->wait, 1, memory_order_acq_rel);
    assert((c & ~_CR_TASK_FLAG_W) > 0 && "negative wait count");
    if (c == (_CR_TASK_FLAG_W | 1llu)) _cr_task_end_wait(task);
}

void cr_task_run(cr_task_t* task) {
    uint64_t c = atomic_fetch_or_explicit(&task->wait, _CR_TASK_FLAG_W, memory_order_acq_rel);
    assert((c & _CR_TASK_FLAG_W) == 0 && "tried to run more than once");
    if (c == 0) _cr_task_end_wait(task);
}

void cr_task_sync(cr_task_t* task) {
    _cr_sync_t sync;
    mtx_init(&sync.mutex, mtx_plain);
    cnd_init(&sync.cond);
    sync.is_done = 0;
    task->sync = &sync;
    uint64_t c = atomic_fetch_or_explicit(&task->signal, _CR_TASK_FLAG_SYNC, memory_order_acq_rel);
    assert((c & _CR_TASK_FLAG_SYNC) == 0 && "sync flag already set");
    if ((c & _CR_TASK_FLAG_S) == 0) {
        mtx_lock(&sync.mutex);
        while (!sync.is_done) {
            cnd_wait(&sync.cond, &sync.mutex);
        }
        mtx_unlock(&sync.mutex);
    }
}

void cr_task_run_sync(cr_task_t* task) {
    cr_task_retain(task);
    cr_task_run(task);
    cr_task_sync(task);
    cr_task_release(task);
}

void _cr_task_end_wait(cr_task_t* task) {
    if (task->func) {
        _cr_executor_schedule_task(task->executor, task);
    } else {
        _cr_task_begin_signal(task);
    }
}

void _cr_task_begin_signal(cr_task_t* task) {
    uint64_t c = atomic_fetch_or_explicit(&task->signal, _CR_TASK_FLAG_S, memory_order_acq_rel);
    assert((c & _CR_TASK_FLAG_S) == 0 && "tried to finalize more than once");
    if (c & _CR_TASK_FLAG_SYNC) {
        mtx_lock(&task->sync->mutex);
        task->sync->is_done = 1;
        cnd_signal(&task->sync->cond);
        mtx_unlock(&task->sync->mutex);
    }
    uint64_t waiting_count = (c >> 32) & 0xffff;
    uint64_t idx = c & _CR_POOL_IDX_MASK;
    for (uint64_t i = 0; i < waiting_count; i++) {
        assert(idx != _CR_POOL_IDX_MASK && "inconsistent state");
        uint64_t batch_idx = idx >> _CR_POOL_ITEM;
        uint64_t item_idx = idx & ((1llu << _CR_POOL_ITEM) - 1llu);
        _cr_task_ref_t* task_ref = (_cr_task_ref_t*)task->executor->task_ref_pool.batches[batch_idx] + item_idx;
        cr_task_t* next_task = task_ref->task;
        uint64_t next_idx = atomic_load_explicit(&task_ref->next_idx_list, memory_order_relaxed);
        _cr_pool_push(&task->executor->task_ref_pool, idx);
        cr_task_signal(next_task);
        idx = next_idx;
    }
    if ((c >> 48) == 0) _cr_pool_push(&task->executor->task_pool, task->idx);
}

cr_executor_t* cr_executor_create(int worker_count) {
    size_t aligned_size_executor = (sizeof(cr_executor_t) + 255) & ~(size_t)0xff;
    cr_executor_t* executor = malloc(aligned_size_executor + (size_t)worker_count * sizeof(thrd_t));
    _cr_pool_init(&executor->task_pool, sizeof(cr_task_t));
    _cr_pool_init(&executor->task_ref_pool, sizeof(_cr_task_ref_t));
    atomic_store_explicit(&executor->c, _CR_POOL_IDX_MASK, memory_order_release);
    mtx_init(&executor->mutex, mtx_plain);
    cnd_init(&executor->cond);
    executor->workers = (thrd_t*)((char*)executor + aligned_size_executor);
    for (int i = 0; i < worker_count; i++) {
        thrd_create(&executor->workers[i], _cr_executor_worker_func, executor);
    }
    return executor;
}

void _cr_executor_schedule_task(cr_executor_t* executor, cr_task_t* task) {
    uint64_t c = atomic_load_explicit(&executor->c, memory_order_acquire);
    uint64_t n;
    do {
        uint64_t idx = c & _CR_POOL_IDX_MASK;
        if (idx == _CR_POOL_IDX_MASK) {
            mtx_lock(&executor->mutex);
            c = atomic_load_explicit(&executor->c, memory_order_acquire);
            idx = c & _CR_POOL_IDX_MASK;
            if (idx == _CR_POOL_IDX_MASK) {
                atomic_store_explicit(&task->next_idx_list, _CR_POOL_IDX_MASK, memory_order_relaxed);
                atomic_store_explicit(&executor->c, task->idx, memory_order_release);
                cnd_signal(&executor->cond);
                mtx_unlock(&executor->mutex);
                return;
            }
            mtx_unlock(&executor->mutex);
        }
        atomic_store_explicit(&task->next_idx_list, (uint32_t)idx, memory_order_relaxed);
        n = ((c + (1llu << 32)) & ~_CR_POOL_IDX_MASK) | task->idx;
    } while (!atomic_compare_exchange_weak_explicit(&executor->c, &c, n, memory_order_acq_rel, memory_order_acquire));
    cnd_signal(&executor->cond);
}

int _cr_executor_worker_func(void* args) {
    cr_executor_t* executor = args;
    while (1) {
        cr_task_t* task;
        uint64_t c = atomic_load_explicit(&executor->c, memory_order_acquire);
        uint64_t n;
        do {
            uint64_t idx = c & _CR_POOL_IDX_MASK;
            if (idx == _CR_POOL_IDX_MASK) {
                mtx_lock(&executor->mutex);
                c = atomic_load_explicit(&executor->c, memory_order_acquire);
                idx = c & _CR_POOL_IDX_MASK;
                if (idx == _CR_POOL_IDX_MASK) {
                    cnd_wait(&executor->cond, &executor->mutex);
                    mtx_unlock(&executor->mutex);
                    goto next_task;
                }
                mtx_unlock(&executor->mutex);
            }
            uint64_t batch_idx = idx >> _CR_POOL_ITEM;
            uint64_t item_idx = idx & ((1llu << _CR_POOL_ITEM) - 1llu);
            task = (cr_task_t*)executor->task_pool.batches[batch_idx] + item_idx;
            n = ((c + (1llu << 32)) & ~_CR_POOL_IDX_MASK) | atomic_load_explicit(&task->next_idx_list, memory_order_relaxed);
        } while (!atomic_compare_exchange_weak_explicit(&executor->c, &c, n, memory_order_acq_rel, memory_order_acquire));
        task->func(task->args);
        _cr_task_begin_signal(task);
    next_task: ;
    }
    return 0;
}
#endif
#endif
