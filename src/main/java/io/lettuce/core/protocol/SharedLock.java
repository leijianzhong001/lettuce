package io.lettuce.core.protocol;

import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

import io.lettuce.core.internal.LettuceAssert;

/**
 * Shared locking facade that supports shared and exclusive locking.
 * <p>
 * Multiple shared locks (writers) are allowed concurrently to process their work. If an exclusive lock is requested, the
 * exclusive lock requestor will wait until all shared locks are released and the exclusive worker is permitted.
 * <p>
 * Exclusive locking is reentrant. An exclusive lock owner is permitted to acquire and release shared locks. Shared/exclusive
 * lock requests by other threads than the thread which holds the exclusive lock, are forced to wait until the exclusive lock is
 * released.
 *
 * @author Mark Paluch
 */
class SharedLock {

    private static final AtomicLongFieldUpdater<SharedLock> WRITERS = AtomicLongFieldUpdater.newUpdater(SharedLock.class,
            "writers");

    private final Lock lock = new ReentrantLock();

    private final ThreadLocal<Integer> threadWriters = ThreadLocal.withInitial(() -> 0);

    private volatile long writers = 0;

    private volatile Thread exclusiveLockOwner;

    /**
     * Wait for stateLock and increment writers. Will wait if stateLock is locked and if writer counter is negative.
     * 等待stateLock和增量writers。如果stateLock被锁定并且写入器计数器为负，则将等待。
     */
    void incrementWriters() {
        // 可重入，所以如果是当前线程持有了锁，直接返回
        if (exclusiveLockOwner == Thread.currentThread()) {
            return;
        }

        lock.lock();
        try {
            for (;;) {
                // writers 是一个long类型，通过 AtomicLongFieldUpdater 来原子递增
                // this是当前 ShardLock 对象，一般而言，同一个连接的所有操作都是在同一个SharedLock对象上进行的
                // 获取 WRITERS 上保存的当前ShardLock对象的值，
                if (WRITERS.get(this) >= 0) {
                    // 递增一次写入
                    WRITERS.incrementAndGet(this);
                    // 将最新的写入计数结果保存到 threadWriters 中，这是个ThreadLocal
                    threadWriters.set(threadWriters.get() + 1);
                    return;
                }
                // 如果WRITERS.get返回负数，说明有线程持有了排他锁，当前线程需要等待
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Decrement writers without any wait.
     */
    void decrementWriters() {

        if (exclusiveLockOwner == Thread.currentThread()) {
            return;
        }

        WRITERS.decrementAndGet(this);
        threadWriters.set(threadWriters.get() - 1);
    }

    /**
     * Execute a {@link Runnable} guarded by an exclusive lock.
     *
     * @param runnable the runnable, must not be {@code null}.
     */
    void doExclusive(Runnable runnable) {

        LettuceAssert.notNull(runnable, "Runnable must not be null");

        doExclusive(() -> {
            runnable.run();
            return null;
        });
    }

    /**
     * Retrieve a value produced by a {@link Supplier} guarded by an exclusive lock.
     *
     * @param supplier the {@link Supplier}, must not be {@code null}.
     * @param <T> the return type
     * @return the return value
     */
    <T> T doExclusive(Supplier<T> supplier) {

        LettuceAssert.notNull(supplier, "Supplier must not be null");

        lock.lock();
        try {

            try {

                lockWritersExclusive();
                return supplier.get();
            } finally {
                unlockWritersExclusive();
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Wait for stateLock and no writers. Must be used in an outer {@code synchronized} block to prevent interleaving with other
     * methods using writers. Sets writers to a negative value to create a lock for {@link #incrementWriters()}.
     */
    private void lockWritersExclusive() {

        if (exclusiveLockOwner == Thread.currentThread()) {
            WRITERS.decrementAndGet(this);
            return;
        }

        lock.lock();
        try {
            for (;;) {

                // allow reentrant exclusive lock by comparing writers count and threadWriters count
                if (WRITERS.compareAndSet(this, threadWriters.get(), -1)) {
                    exclusiveLockOwner = Thread.currentThread();
                    return;
                }
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Unlock writers.
     */
    private void unlockWritersExclusive() {

        if (exclusiveLockOwner == Thread.currentThread()) {
            // check exclusive look not reentrant first
            if (WRITERS.compareAndSet(this, -1, threadWriters.get())) {
                exclusiveLockOwner = null;
                return;
            }
            // otherwise unlock until no more reentrant left
            WRITERS.incrementAndGet(this);
        }
    }

}
