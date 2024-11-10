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
package io.lettuce.core.protocol;

import io.lettuce.core.internal.LettuceAssert;
import io.lettuce.core.output.CommandOutput;
import io.netty.buffer.ByteBuf;

/**
 * A Redis command with a {@link ProtocolKeyword command type}, {@link CommandArgs arguments} and an optional
 * {@link CommandOutput output}. All successfully executed commands will eventually return a {@link CommandOutput} object.
 *
 * @param <K> Key type.
 * @param <V> Value type.
 * @param <T> Command output type.
 *
 * @author Will Glozer
 * @author Mark Paluch
 */
public class Command<K, V, T> implements RedisCommand<K, V, T> {

    protected static final byte ST_INITIAL = 0;

    protected static final byte ST_COMPLETED = 1;

    protected static final byte ST_CANCELLED = 2;

    private final ProtocolKeyword type;

    protected CommandArgs<K, V> args;

    protected CommandOutput<K, V, T> output;

    protected Throwable exception;

    protected volatile byte status = ST_INITIAL;

    /**
     * Create a new command with the supplied type.
     *
     * @param type Command type, must not be {@code null}.
     * @param output Command output, can be {@code null}.
     */
    public Command(ProtocolKeyword type, CommandOutput<K, V, T> output) {
        this(type, output, null);
    }

    /**
     * Create a new command with the supplied type and args.
     *
     * @param type Command type, must not be {@code null}.
     * @param output Command output, can be {@code null}.
     * @param args Command args, can be {@code null}
     */
    public Command(ProtocolKeyword type, CommandOutput<K, V, T> output, CommandArgs<K, V> args) {
        LettuceAssert.notNull(type, "Command type must not be null");
        this.type = type;
        this.output = output;
        this.args = args;
    }

    /**
     * Get the object that holds this command's output.
     *
     * @return The command output object.
     */
    @Override
    public CommandOutput<K, V, T> getOutput() {
        return output;
    }

    @Override
    public boolean completeExceptionally(Throwable throwable) {
        if (output != null) {
            output.setError(throwable.getMessage());
        }

        exception = throwable;
        this.status = ST_COMPLETED;
        return true;
    }

    /**
     * Mark this command complete and notify all waiting threads.
     */
    @Override
    public void complete() {
        this.status = ST_COMPLETED;
    }

    @Override
    public void cancel() {
        this.status = ST_CANCELLED;
    }

    /**
     * Encode and write this command to the supplied buffer using the new <a href="https://redis.io/topics/protocol">Unified
     * Request Protocol</a>.
     *
     * @param buf Buffer to write to.
     */
    public void encode(ByteBuf buf) {
        // touch方法可以记录ByteBuf对象的当前访问位置，以及添加用于调试的任意附加信息。
        // 如果确定此ByteBuf对象被泄露，则此操作记录的信息将通过{@link ResourceLeakDetector}提供给用户。
        buf.touch("Command.encode(…)");

        // resp协议的命令格式为：*<number of arguments>\r\n$<number of bytes of argument 1>\r\n<argument data>\r\n
        // 例如：SET key value江北表达为：*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n

        // 1、写入数组长度， 也即是参数的个数
        buf.writeByte('*');
        CommandArgs.IntegerArgument.writeInteger(buf, 1 + (args != null ? args.count() : 0));

        buf.writeBytes(CommandArgs.CRLF);

        // 2、写入具体的命令，比如 SET
        CommandArgs.BytesArgument.writeBytes(buf, type.getBytes());

        if (args != null) {
            // 3、写入命令参数
            args.encode(buf);
        }
    }

    public String getError() {
        return output.getError();
    }

    @Override
    public CommandArgs<K, V> getArgs() {
        return args;
    }

    /**
     *
     * @return the resut from the output.
     */
    public T get() {
        if (output != null) {
            return output.get();
        }
        return null;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append(getClass().getSimpleName());
        sb.append(" [type=").append(type);
        sb.append(", output=").append(output);
        sb.append(']');
        return sb.toString();
    }

    public void setOutput(CommandOutput<K, V, T> output) {
        if (this.status != ST_INITIAL) {
            throw new IllegalStateException("Command is completed/cancelled. Cannot set a new output");
        }
        this.output = output;
    }

    @Override
    public ProtocolKeyword getType() {
        return type;
    }

    @Override
    public boolean isCancelled() {
        return status == ST_CANCELLED;
    }

    @Override
    public boolean isDone() {
        return status != ST_INITIAL;
    }

}
