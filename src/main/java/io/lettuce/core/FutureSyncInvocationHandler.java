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
package io.lettuce.core;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.TimeUnit;

import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.internal.AbstractInvocationHandler;
import io.lettuce.core.internal.Futures;
import io.lettuce.core.internal.TimeoutProvider;
import io.lettuce.core.protocol.CommandType;
import io.lettuce.core.protocol.ProtocolKeyword;
import io.lettuce.core.protocol.RedisCommand;

/**
 * Invocation-handler to synchronize API calls which use Futures as backend. This class leverages the need to implement a full
 * sync class which just delegates every request.
 *
 * @author Mark Paluch
 * @author Tz Zhuo
 * @since 3.0
 */
class FutureSyncInvocationHandler extends AbstractInvocationHandler {

    private final StatefulConnection<?, ?> connection;

    private final TimeoutProvider timeoutProvider;

    private final Object asyncApi;

    private final MethodTranslator translator;

    FutureSyncInvocationHandler(StatefulConnection<?, ?> connection, Object asyncApi, Class<?>[] interfaces) {
        this.connection = connection;
        this.timeoutProvider = new TimeoutProvider(() -> connection.getOptions().getTimeoutOptions(),
                () -> connection.getTimeout().toNanos());
        // 是 StatefulRedisConnectionImpl.async对象，其实现是 RedisAsyncCommandsImpl
        this.asyncApi = asyncApi;
        // 可以认为这是一个缓存，缓存了内容为原始方法->代理对象方法的映射，这样就避免了每次调用都要去遍历一遍被代理对象的方法列表
        this.translator = MethodTranslator.of(asyncApi.getClass(), interfaces);
    }

    @Override
    protected Object handleInvocation(Object proxy, Method method, Object[] args) throws Throwable {
        // proxy:　 - 指代我们所代理的那个真实对象,其实就是异步 api RedisAsyncCommandsImpl
        // method:　- 指代的是我们所要调用真实对象的某个方法的Method对象
        // args:　　- 指代的是调用真实对象某个方法时接受的参数
        try {

            // 获取到需要执行的代理对象方法（异步api中的响应方法）
            Method targetMethod = this.translator.get(method);
            // 执行方法调用
            Object result = targetMethod.invoke(asyncApi, args);

            if (result instanceof RedisFuture<?>) {

                RedisFuture<?> command = (RedisFuture<?>) result;

                if (!isTxControlMethod(method.getName(), args) && isTransactionActive(connection)) {
                    return null;
                }
                // 获取超时时间
                long timeout = getTimeoutNs(command);
                // 在这里异步转同步，阻塞等待结果
                return Futures.awaitOrCancel(command, timeout, TimeUnit.NANOSECONDS);
            }

            return result;
        } catch (InvocationTargetException e) {
            throw e.getTargetException();
        }
    }

    private long getTimeoutNs(RedisFuture<?> command) {

        if (command instanceof RedisCommand) {
            return timeoutProvider.getTimeoutNs((RedisCommand) command);
        }

        return connection.getTimeout().toNanos();
    }

    private static boolean isTransactionActive(StatefulConnection<?, ?> connection) {
        return connection instanceof StatefulRedisConnection && ((StatefulRedisConnection) connection).isMulti();
    }

    private static boolean isTxControlMethod(String methodName, Object[] args) {

        if (methodName.equals("exec") || methodName.equals("multi") || methodName.equals("discard")) {
            return true;
        }

        if (methodName.equals("dispatch") && args.length > 0 && args[0] instanceof ProtocolKeyword) {

            ProtocolKeyword keyword = (ProtocolKeyword) args[0];
            if (keyword.toString().equals(CommandType.MULTI.name()) || keyword.toString().equals(CommandType.EXEC.name())
                    || keyword.toString().equals(CommandType.DISCARD.name())) {
                return true;
            }
        }

        return false;
    }

}
