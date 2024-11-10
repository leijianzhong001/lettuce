package io.lettuce.examples;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.support.ConnectionPoolSupport;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;


public class GenericObjectPoolExample {
    public static void main(String[] args) throws Exception {
        RedisClient client = RedisClient.create(RedisURI.create("10.243.7.12", 6379));

        GenericObjectPool<StatefulRedisConnection<String, String>> pool = ConnectionPoolSupport
                .createGenericObjectPool(client::connect, new GenericObjectPoolConfig<>());
        // executing work
        StatefulRedisConnection<String, String> connection = pool.borrowObject();
        RedisCommands<String, String> commands = connection.sync();
        commands.multi();
        commands.set("key", "value");
        commands.set("key2", "value2");
        commands.exec();
        connection.close();

        // terminating
        pool.close();
        client.shutdown();
    }
}
