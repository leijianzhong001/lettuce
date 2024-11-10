/*
 * Copyright 2011-Present, Redis Ltd. and Contributors
 * All rights reserved.
 *
 * Licensed under the MIT License.
 *
 * This file contains contributions from third-party contributors
 * licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.lettuce.examples;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.resource.ClientResources;
import io.lettuce.core.resource.DefaultClientResources;

/**
 * @author Mark Paluch
 * @author Tugdual Grall
 */
public class ConnectToRedis {

    public static void main(String[] args) {

        // 自定义IO线程数量为1，因为多了也用不了
        ClientResources clientResources = DefaultClientResources.builder()
                .ioThreadPoolSize(1)
                .build();
        // 多个客户端共享一个ClientResources，这样每个客户端的IO线程数量都可以保持在1
        RedisClient redisClient = RedisClient.create(clientResources, "redis://10.243.7.12:6379/0");
        // 每次 connect，都会创建一个Bootstrap，然后创建一个Channel，然后连接到Redis, 所以前面要保持IO线程为1个，否则就会浪费
        StatefulRedisConnection<String, String> connection = redisClient.connect();
        // RedisAsyncCommandsImpl对象，内部包含了连接等信息
        RedisAsyncCommands<String, String> async = connection.async();
        RedisFuture<String> key = async.set("key", "Hello, Redis!");
        key.thenAccept(value -> System.out.println("key: " + value));
        System.out.println("Connected to Redis");

        connection.close();
        redisClient.shutdown();
        // 注意共享的ClientResources在应用停止时是需要主动关闭的
        clientResources.shutdown();
    }

}
