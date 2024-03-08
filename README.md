# cr_task.h

This library provides a minimal set of types and functions for an asynchronous task system in C. 
It was designed to be lock-free in the common case, with locks only needed for allocations of backing memory for the task pool and when the worker threads are starved for tasks to execute. It is written in standard C11 with no dependencies besides the C POSIX library.

To get started, create an executor with the desired number of worker threads and define a task.
```C
cr_executor_t* exec = cr_executor_create(4);
cr_task_t* task = cr_task_create(exec, func, args);
cr_task_run(task);
```

By default, the task is going to run immediately on `cr_task_run`. If you want it to run later, call `cr_task_wait` before `run`, and `cr_task_signal` later.

```C
cr_task_t* task = cr_task_create(exec, func, args);
cr_task_wait(task);
cr_task_run(task);
// Later... (possibly on a different thread or inside a task)
cr_task_signal(task);
```

Instead of calling `signal` yourself, you can also request a signal from another task, creating a dependency. Here, `task_1` has to finish before `task_2` can start.

```C
cr_task_t* task_1 = cr_task_create(exec, func, args);
cr_task_t* task_2 = cr_task_create(exec, func, args);
cr_task_wait(task_2);
cr_task_request_signal(task_2, task_1);
cr_task_run(task_2);
// task_2 does not execute yet
cr_task_run(task_1);
// task_1 executes, then task_2
```

Since telling one task to wait and requesting a signal from another is such a common operation, there is a shorthand function called `cr_task_wait_request_signal`. `wait` and `signal` calls must always be balanced, and as soon as the wait count hits zero and `run` has been called, the task executes.

```C
cr_task_t* task = cr_task_create(exec, func, args);
cr_task_wait_request_signal(task, task_dep_1);
cr_task_wait_request_signal(task, task_dep_2);
cr_task_run(task);
// task_dep_1 and task_dep_2 execute, then task
```

When you want to synchronously wait for a task to finish execution, call `cr_task_sync`. Since a task destroys itself automatically after it has run, you need to call `cr_task_retain` before, and `cr_task_release` after waiting. And again, because this pattern of `retain`, `run`, `sync`, and `release` is relatively common, there is a shorthand called `cr_task_run_sync`.

```C
cr_task_t* task = cr_task_create(exec, func, args);
cr_task_retain(task);
cr_task_run(task);
cr_task_sync(task);
cr_task_release(task);
// Or the shorthand...
cr_task_run_sync(task);
```
