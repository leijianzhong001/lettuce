package io.lettuce.core.support;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisConnectionException;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.internal.Exceptions;
import io.lettuce.core.internal.LettuceAssert;
import io.lettuce.core.support.ConnectionWrapping.HasTargetConnection;
import io.lettuce.core.support.ConnectionWrapping.Origin;

/**
 * Asynchronous connection pool support for {@link BoundedAsyncPool}. Connection pool creation requires a {@link Supplier} that
 * connects asynchronously to Redis. The pool can allocate either wrapped or direct connections.
 * <ul>
 * <li>Wrapped instances will return the connection back to the pool when called {@link StatefulConnection#close()}/
 * {@link StatefulConnection#closeAsync()}.</li>
 * <li>Regular connections need to be returned to the pool with {@link AsyncPool#release(Object)}</li>
 * </ul>
 * <p>
 * Lettuce connections are designed to be thread-safe so one connection can be shared amongst multiple threads and Lettuce
 * connections {@link ClientOptions#isAutoReconnect() auto-reconnect} by default. Connection pooling with Lettuce can be
 * required when you're invoking Redis operations in multiple threads and you use
 * <ul>
 * <li>blocking commands such as {@code BLPOP}.</li>
 * <li>transactions {@code BLPOP}.</li>
 * <li>{@link StatefulConnection#setAutoFlushCommands(boolean) command batching}.</li>
 * </ul>
 *
 * Transactions and command batching affect connection state. Blocking commands won't propagate queued commands to Redis until
 * the blocking command is completed.
 *
 * <h3>Example</h3>
 *
 * <pre class="code">
 *
 * // application initialization
 * RedisClusterClient clusterClient = RedisClusterClient.create(RedisURI.create(host, port));
 *
 * AsyncPool&lt;StatefulRedisConnection&lt;String, String&gt;&gt; pool = AsyncConnectionPoolSupport
 *         .createBoundedObjectPool(() -&gt; clusterClient.connectAsync(), BoundedPoolConfig.create());
 *
 * // executing work
 * CompletableFuture&lt;String&gt; pingResponse = pool.acquire().thenCompose(c -&gt; {
 *
 *     return c.async().ping().whenComplete((s, throwable) -&gt; pool.release(c));
 * });
 *
 * // terminating
 * CompletableFuture&lt;Void&gt; poolClose = pool.closeAsync();
 *
 * // after poolClose completes:
 * CompletableFuture&lt;Void&gt; closeFuture = clusterClient.shutdown();
 * </pre>
 *
 * @author Mark Paluch
 * @since 5.1
 */
public abstract class AsyncConnectionPoolSupport {

    private AsyncConnectionPoolSupport() {
    }

    /**
     * Create and initialize asynchronously a new {@link BoundedAsyncPool} using the {@link Supplier}. Allocated instances are
     * wrapped and must not be returned with {@link AsyncPool#release(Object)}.
     * <p>
     * Since Lettuce 6, this method is blocking as it awaits pool initialization (creation of idle connections).Use
     * {@link #createBoundedObjectPoolAsync(Supplier, BoundedPoolConfig)} to obtain a {@link CompletionStage} for non-blocking
     * synchronization.
     *
     * @param connectionSupplier must not be {@code null}.
     * @param config must not be {@code null}.
     * @param <T> connection type.
     * @return the connection pool.
     */
    public static <T extends StatefulConnection<?, ?>> BoundedAsyncPool<T> createBoundedObjectPool(
            Supplier<CompletionStage<T>> connectionSupplier, BoundedPoolConfig config) {
        return createBoundedObjectPool(connectionSupplier, config, true);
    }

    /**
     * Create and initialize asynchronously a new {@link BoundedAsyncPool} using the {@link Supplier}.
     * <p>
     * Since Lettuce 6, this method is blocking as it awaits pool initialization (creation of idle connections).Use
     * {@link #createBoundedObjectPoolAsync(Supplier, BoundedPoolConfig, boolean)} to obtain a {@link CompletionStage} for
     * non-blocking synchronization.
     *
     * @param connectionSupplier must not be {@code null}.
     * @param config must not be {@code null}.
     * @param wrapConnections {@code false} to return direct connections that need to be returned to the pool using
     *        {@link AsyncPool#release(Object)}. {@code true} to return wrapped connection that are returned to the pool when
     *        invoking {@link StatefulConnection#close()}/{@link StatefulConnection#closeAsync()}.
     * @param <T> connection type.
     * @return the connection pool.
     */
    public static <T extends StatefulConnection<?, ?>> BoundedAsyncPool<T> createBoundedObjectPool(
            Supplier<CompletionStage<T>> connectionSupplier, BoundedPoolConfig config, boolean wrapConnections) {

        try {
            return createBoundedObjectPoolAsync(connectionSupplier, config, wrapConnections).toCompletableFuture().join();
        } catch (Exception e) {
            throw Exceptions.bubble(Exceptions.unwrap(e));
        }
    }

    /**
     * Create and initialize asynchronously a new {@link BoundedAsyncPool} using the {@link Supplier}. Allocated instances are
     * wrapped and must not be returned with {@link AsyncPool#release(Object)}.
     *
     * @param connectionSupplier must not be {@code null}.
     * @param config must not be {@code null}.
     * @param <T> connection type.
     * @return {@link CompletionStage} emitting the connection pool upon completion.
     * @since 5.3.3
     */
    public static <T extends StatefulConnection<?, ?>> CompletionStage<BoundedAsyncPool<T>> createBoundedObjectPoolAsync(
            Supplier<CompletionStage<T>> connectionSupplier, BoundedPoolConfig config) {
        return createBoundedObjectPoolAsync(connectionSupplier, config, true);
    }

    /**
     * Create and initialize asynchronously a new {@link BoundedAsyncPool} using the {@link Supplier}.
     *
     * @param connectionSupplier must not be {@code null}.
     * @param config must not be {@code null}.
     * @param wrapConnections {@code false} to return direct connections that need to be returned to the pool using
     *        {@link AsyncPool#release(Object)}. {@code true} to return wrapped connection that are returned to the pool when
     *        invoking {@link StatefulConnection#close()}/{@link StatefulConnection#closeAsync()}.
     * @param <T> connection type.
     * @return {@link CompletionStage} emitting the connection pool upon completion.
     * @since 5.3.3
     */
    public static <T extends StatefulConnection<?, ?>> CompletionStage<BoundedAsyncPool<T>> createBoundedObjectPoolAsync(
            Supplier<CompletionStage<T>> connectionSupplier, BoundedPoolConfig config, boolean wrapConnections) {

        BoundedAsyncPool<T> pool = doCreatePool(connectionSupplier, config, wrapConnections);
        CompletableFuture<BoundedAsyncPool<T>> future = new CompletableFuture<>();

        pool.createIdle().whenComplete((v, throwable) -> {

            if (throwable == null) {
                future.complete(pool);
            } else {
                pool.closeAsync().whenComplete((v1, throwable1) -> {
                    future.completeExceptionally(new RedisConnectionException("Could not create pool", throwable));
                });
            }
        });

        return future;
    }

    protected static <T extends StatefulConnection<?, ?>> BoundedAsyncPool<T> doCreatePool(
            Supplier<CompletionStage<T>> connectionSupplier, BoundedPoolConfig config, boolean wrapConnections) {

        LettuceAssert.notNull(connectionSupplier, "Connection supplier must not be null");
        LettuceAssert.notNull(config, "BoundedPoolConfig must not be null");

        AtomicReference<Origin<T>> poolRef = new AtomicReference<>();
        // 一个内置的连接池
        // RedisPooledObjectFactory 提供了连接的创建、销毁、校验等操作
        BoundedAsyncPool<T> pool = new BoundedAsyncPool<T>(new RedisPooledObjectFactory<T>(connectionSupplier), config, false) {

            // 从连接池中借出一个连接
            @Override
            public CompletableFuture<T> acquire() {
                // 1、先调用父类的 acquire 方法，获取一个未经过包装的连接，此方法中如果没有空闲连接，会调用连接工厂的 create 方法来获得一个连接
                CompletableFuture<T> acquire = super.acquire();

                if (wrapConnections) {
                    // 默认会使用包装的连接，所以这里会对连接进行包装。wrapConnection 方法和同步连接池中的 wrapConnection 方法并无不同
                    return acquire.thenApply(it -> ConnectionWrapping.wrapConnection(it, poolRef.get()));
                }

                return acquire;
            }

            @Override
            @SuppressWarnings("unchecked")
            public CompletableFuture<Void> release(T object) {

                if (wrapConnections && object instanceof HasTargetConnection) {
                    // 重写归还连接的方法，默认情况下， wrapConnections 为 true，包装以后得连接都有 HasTargetConnection 这个父接口，所以走这里。
                    return super.release((T) ((HasTargetConnection) object).getTargetConnection());
                }

                return super.release(object);
            }

        };

        poolRef.set(new AsyncPoolWrapper<>(pool));
        return pool;
    }

    /**
     * @author Mark Paluch
     * @since 5.1
     */
    private static class RedisPooledObjectFactory<T extends StatefulConnection<?, ?>> implements AsyncObjectFactory<T> {

        private final Supplier<CompletionStage<T>> connectionSupplier;

        RedisPooledObjectFactory(Supplier<CompletionStage<T>> connectionSupplier) {
            this.connectionSupplier = connectionSupplier;
        }

        @Override
        public CompletableFuture<T> create() {
            return connectionSupplier.get().toCompletableFuture();
        }

        @Override
        public CompletableFuture<Void> destroy(T object) {
            return object.closeAsync();
        }

        @Override
        public CompletableFuture<Boolean> validate(T object) {
            return CompletableFuture.completedFuture(object.isOpen());
        }

    }

    private static class AsyncPoolWrapper<T> implements Origin<T> {

        private final AsyncPool<T> pool;

        AsyncPoolWrapper(AsyncPool<T> pool) {
            this.pool = pool;
        }

        @Override
        public void returnObject(T o) {
            returnObjectAsync(o).join();
        }

        @Override
        public CompletableFuture<Void> returnObjectAsync(T o) {
            return pool.release(o);
        }

    }

}
