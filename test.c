#include <stdint.h>
#include <stdio.h>

#define CR_TASK_IMPL
#include "cr_task.h"

// 1  2  3  4  5  6  7  8   input
//    3     7     11    15  down
//          10          26  down
//          0           10  up
//    0     3     10    21  up
// 0  1  3  6  10 15 21 28  up

// 1  2  3  4  5  6  7  8  9  10  11  12  13  14  15  16
//    3     7     11    15    19      23      27      31
//          10          26            42              58
//                      36                            100
//                      0                             36
//          0           10            36              78
//    0     3     10    21    36      55      78      105
// 0  1  3  6  10 15 21 28 36 45  55  66  78  91  105 120

#define _log(...) // do { printf("[log]: "__VA_ARGS__); } while (0)

#define M 256 // length of each prefix sum
#define N 128 // number of concurrently running prefix sums

typedef uint8_t m_t;
static_assert(M <= (1llu << (sizeof(m_t) * 8llu)), "not enough bits");

static_assert(sizeof(void*) == sizeof(uint64_t), "args pointer used as uint64_t");

cr_executor_t* executor;
cr_task_t* m[N];
cr_task_t* end;
m_t c[M * N];
m_t s[M * N];

static void down(void* args) {
    uint64_t n = (uint64_t)args & 0xffff;
    uint64_t dist = ((uint64_t)args >> 16) & 0xffff;
    uint64_t i = (uint64_t)args >> 32;
    _log("down, %llu, %llu\n", dist, i);
    c[i * N + n] += c[(i - (dist >> 1)) * N + n];
}

static void up(void* args) {
    uint64_t n = (uint64_t)args & 0xffff;
    uint64_t dist = ((uint64_t)args >> 16) & 0xffff;
    uint64_t i = (uint64_t)args >> 32;
    _log("up, %llu, %llu\n", dist, i);
    m_t temp = i == dist - 1 ? 0 : c[i * N + n];
    c[i * N + n] = temp + c[(i - (dist >> 1)) * N + n];
    c[(i - (dist >> 1)) * N + n] = temp;
}

static void setup(void* args) {
    uint64_t n = (uint64_t)args & 0xffff;
    uint64_t dist = ((uint64_t)args >> 16) & 0xffff;
    uint64_t stage = (uint64_t)args >> 32;
    _log("setup, %llu, %llu, %llu\n", n, dist, stage);
    uint64_t dist_next;
    uint64_t stage_next = stage;
    if (stage == 0) {
        if (dist == 2) {
            m_t sr = 0;
            uint32_t r = 0;
            for (uint64_t i = 0; i < M; i++) {
                if (i % 32 == 0) r = rand();
                m_t v = r & 1;
                c[i * N + n] = v;
                s[i * N + n] = sr;
                sr += v;
                r >>= 1;
            }
        }
        dist_next = dist << 1;
        if (dist_next == M) {
            stage_next = 1;
        }
    } else {
        if (dist == 1) {
            for (uint64_t i = 0; i < M; i++) {
                if (c[i * N + n] != s[i * N + n]) abort();
            }
            _log("success\n");
            cr_task_request_signal(end, m[n]);
            return;
        }
        dist_next = dist >> 1;
    }
    cr_task_t* next = cr_task_create(executor, setup, (void*)((stage_next << 32) | (dist_next << 16) | n));
    for (uint64_t i = dist - 1; i < M; i += dist) {
        cr_task_t* t = cr_task_create(executor, stage ? up : down, (void*)((i << 32) | (dist << 16) | n));
        cr_task_wait_request_signal(t, m[n]);
        cr_task_wait_request_signal(next, t);
        cr_task_run(t);
    }
    cr_task_run(next);
    m[n] = next;
}

int main(void) {
    executor = cr_executor_create(4);
    while (1) {
        cr_task_t* start = cr_task_create(executor, NULL, NULL);
        end = cr_task_create(executor, NULL, NULL);
        for (uint64_t i = 0; i < N; i++) {
            cr_task_wait(end);
            cr_task_t* t = cr_task_create(executor, setup, (void*)((2llu << 16) | i));
            cr_task_wait_request_signal(t, start);
            cr_task_run(t);
            m[i] = t;
        }
        cr_task_run(start);
        cr_task_run_sync(end);
    }
}
