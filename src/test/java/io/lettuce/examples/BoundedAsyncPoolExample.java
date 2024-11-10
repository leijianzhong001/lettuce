package io.lettuce.examples;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.TransactionResult;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.support.AsyncConnectionPoolSupport;
import io.lettuce.core.support.BoundedAsyncPool;
import io.lettuce.core.support.BoundedPoolConfig;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;


public class BoundedAsyncPoolExample {
    public static void main(String[] args) throws Exception {
        RedisClient client = RedisClient.create();

        CompletionStage<BoundedAsyncPool<StatefulRedisConnection<String, String>>> poolFuture =
                AsyncConnectionPoolSupport.createBoundedObjectPoolAsync(
                        () -> client.connectAsync(StringCodec.UTF8, RedisURI.create("10.243.7.12", 6379)),
                        BoundedPoolConfig.create());

        // await poolFuture initialization to avoid NoSuchElementException: Pool exhausted when starting your application
        BoundedAsyncPool<StatefulRedisConnection<String, String>> pool = poolFuture.toCompletableFuture().join();

        // execute work
        CompletableFuture<TransactionResult> transactionResult = pool.acquire().thenCompose(connection -> {
            RedisAsyncCommands<String, String> async = connection.async();

            async.multi();
            async.set("key", "value");
            async.set("key2", "value2");
            return async.exec().whenComplete((s, throwable) -> pool.release(connection));
        });

        // terminating
        pool.closeAsync();
        // after pool completion
        client.shutdownAsync();
    }
}
