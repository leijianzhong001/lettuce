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

import java.net.SocketAddress;
import java.net.SocketOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

import jdk.net.ExtendedSocketOptions;
import reactor.core.publisher.Mono;
import io.lettuce.core.internal.LettuceAssert;
import io.lettuce.core.protocol.CommandEncoder;
import io.lettuce.core.protocol.CommandHandler;
import io.lettuce.core.protocol.ConnectionInitializer;
import io.lettuce.core.protocol.ConnectionWatchdog;
import io.lettuce.core.protocol.Endpoint;
import io.lettuce.core.protocol.ReconnectionListener;
import io.lettuce.core.protocol.RedisHandshakeHandler;
import io.lettuce.core.resource.ClientResources;
import io.lettuce.core.resource.EpollProvider;
import io.lettuce.core.resource.IOUringProvider;
import io.lettuce.core.resource.KqueueProvider;
import io.lettuce.core.resource.Transports;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.socket.nio.NioChannelOption;
import io.netty.util.AttributeKey;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

/**
 * Connection builder for connections. This class is part of the internal API.
 *
 * @author Mark Paluch
 * @author Bodong Ybd
 */
public class ConnectionBuilder {

    private static final InternalLogger logger = InternalLoggerFactory.getInstance(ConnectionBuilder.class);

    public static final AttributeKey<String> REDIS_URI = AttributeKey.valueOf("RedisURI");

    public static final AttributeKey<Throwable> INIT_FAILURE = AttributeKey.valueOf("ConnectionBuilder.INIT_FAILURE");

    private Mono<SocketAddress> socketAddressSupplier;

    private ConnectionEvents connectionEvents;

    // 这个连接就是我们通过Client.connect()得到的StatefulRedisConnectionImpl对象
    private RedisChannelHandler<?, ?> connection;

    private Endpoint endpoint;

    private Supplier<CommandHandler> commandHandlerSupplier;

    private ChannelGroup channelGroup;

    private Bootstrap bootstrap;

    private ClientOptions clientOptions;

    private Duration timeout;

    private ClientResources clientResources;

    private ConnectionInitializer connectionInitializer;

    private ReconnectionListener reconnectionListener = ReconnectionListener.NO_OP;

    private ConnectionWatchdog connectionWatchdog;

    private RedisURI redisURI;

    public static ConnectionBuilder connectionBuilder() {
        return new ConnectionBuilder();
    }

    /**
     * Apply settings from {@link RedisURI}
     *
     * @param redisURI
     */
    public void apply(RedisURI redisURI) {
        this.redisURI = redisURI;
        timeout(redisURI.getTimeout());

        bootstrap.attr(REDIS_URI, redisURI.toString());
    }

    protected List<ChannelHandler> buildHandlers() {

        LettuceAssert.assertState(channelGroup != null, "ChannelGroup must be set");
        LettuceAssert.assertState(connectionEvents != null, "ConnectionEvents must be set");
        LettuceAssert.assertState(connection != null, "Connection must be set");
        LettuceAssert.assertState(clientResources != null, "ClientResources must be set");
        LettuceAssert.assertState(endpoint != null, "Endpoint must be set");
        LettuceAssert.assertState(connectionInitializer != null, "ConnectionInitializer must be set");

        List<ChannelHandler> handlers = new ArrayList<>();

        // 这个连接就是我们通过Client.connect()得到的StatefulRedisConnectionImpl对象
        connection.setOptions(clientOptions);
        // 入站handler: 这是一个简单的handler，用于将channel添加到channelGroup中，或者在断开连接时从channelGroup中移除channel
        handlers.add(new ChannelGroupListener(channelGroup, clientResources.eventBus()));
        // 出站handler: 将 RedisCommand 编码为ByteBuf对象以进行网络传输，是一个 MessageToByteEncoder
        handlers.add(new CommandEncoder());
        // 入站handler: 在Channel进入活动状态时，会调用connectionInitializer初始化这个连接，主要包括：
        // 1.1、ping命令检测连通性；
        // 1.2、select 命令选择数据库；
        // 1.3、client setname 命令设置客户端名称；
        // 1.4、client setinfo 命令设置客户端库的名称和版本
        handlers.add(getHandshakeHandler());
        // commandHandlerSupplier 的实现是在 RedisClient.connectStandaloneAsync 中, 实际实现是CommandHandler这个类。
        // 入站出站handler：CommandHandler 这个类会在channelActive的时候，将channel交给DefaultEndpoint, 这样我们所有的写出命令操作都会通过 DefaultEndpoint 进行
        handlers.add(commandHandlerSupplier.get());

        // 入站handler: 用于在各个环节向事件总线发送事件
        handlers.add(new ConnectionEventTrigger(connectionEvents, connection, clientResources.eventBus()));

        if (clientOptions.isAutoReconnect()) {
            // 入站handler: 如果开启了自动重连，那么就会创建一个ConnectionWatchdog对象，用于监听连接状态，在连接失败的时候进行重连。该配置默认是打开的
            handlers.add(createConnectionWatchdog());
        }

        return handlers;
    }

    protected ChannelHandler getHandshakeHandler() {
        return new RedisHandshakeHandler(connectionInitializer, clientResources, timeout);
    }

    protected ConnectionWatchdog createConnectionWatchdog() {

        if (connectionWatchdog != null) {
            return connectionWatchdog;
        }

        LettuceAssert.assertState(bootstrap != null, "Bootstrap must be set for autoReconnect=true");
        LettuceAssert.assertState(socketAddressSupplier != null, "SocketAddressSupplier must be set for autoReconnect=true");

        ConnectionWatchdog watchdog = new ConnectionWatchdog(clientResources.reconnectDelay(), clientOptions, bootstrap,
                clientResources.timer(), clientResources.eventExecutorGroup(), socketAddressSupplier, reconnectionListener,
                connection, clientResources.eventBus(), endpoint);

        endpoint.registerConnectionWatchdog(watchdog);

        connectionWatchdog = watchdog;
        return watchdog;
    }

    public ChannelInitializer<Channel> build(SocketAddress socketAddress) {
        // PlainChannelInitializer中的initChannel实现会使用buildHandlers指定的handler列表初始化channel
        return new PlainChannelInitializer(this::buildHandlers, clientResources);
    }

    public ConnectionBuilder socketAddressSupplier(Mono<SocketAddress> socketAddressSupplier) {
        this.socketAddressSupplier = socketAddressSupplier;
        return this;
    }

    public Mono<SocketAddress> socketAddress() {
        LettuceAssert.assertState(socketAddressSupplier != null, "SocketAddressSupplier must be set");
        return socketAddressSupplier;
    }

    public ConnectionBuilder timeout(Duration timeout) {
        this.timeout = timeout;
        return this;
    }

    public Duration getTimeout() {
        return timeout;
    }

    public ConnectionBuilder reconnectionListener(ReconnectionListener reconnectionListener) {

        LettuceAssert.notNull(reconnectionListener, "ReconnectionListener must not be null");
        this.reconnectionListener = reconnectionListener;
        return this;
    }

    public ConnectionBuilder clientOptions(ClientOptions clientOptions) {
        this.clientOptions = clientOptions;
        return this;
    }

    public ConnectionBuilder connectionEvents(ConnectionEvents connectionEvents) {
        this.connectionEvents = connectionEvents;
        return this;
    }

    public ConnectionBuilder connection(RedisChannelHandler<?, ?> connection) {
        this.connection = connection;
        return this;
    }

    public ConnectionBuilder channelGroup(ChannelGroup channelGroup) {
        this.channelGroup = channelGroup;
        return this;
    }

    public ConnectionBuilder commandHandler(Supplier<CommandHandler> supplier) {
        this.commandHandlerSupplier = supplier;
        return this;
    }

    public ConnectionBuilder bootstrap(Bootstrap bootstrap) {
        this.bootstrap = bootstrap;
        return this;
    }

    public ConnectionBuilder endpoint(Endpoint endpoint) {
        this.endpoint = endpoint;
        return this;
    }

    public ConnectionBuilder clientResources(ClientResources clientResources) {
        this.clientResources = clientResources;
        return this;
    }

    public ConnectionBuilder connectionInitializer(ConnectionInitializer connectionInitializer) {
        this.connectionInitializer = connectionInitializer;
        return this;
    }

    /**
     * Initialize the {@link Bootstrap}.
     *
     * @since 6.1
     */
    public void configureBootstrap(boolean domainSocket,
            Function<Class<? extends EventLoopGroup>, EventLoopGroup> eventLoopGroupProvider) {

        LettuceAssert.assertState(bootstrap != null, "Bootstrap must be set");
        LettuceAssert.assertState(clientOptions != null, "ClientOptions must be set");

        // 如果没开启native的话，就是 NioEventLoopGroup
        Class<? extends EventLoopGroup> eventLoopGroupClass = Transports.eventLoopGroupClass();

        // 如果没开启native的话，就是NioSocketChannel
        Class<? extends Channel> channelClass = Transports.socketChannelClass();

        if (domainSocket) {
            // 如果打开了unix域套接字
            Transports.NativeTransports.assertDomainSocketAvailable();
            eventLoopGroupClass = Transports.NativeTransports.eventLoopGroupClass(true);
            channelClass = Transports.NativeTransports.domainSocketChannelClass();
        } else {
            // 1、设置域名解析器，如果指定的redis地址是一个主机名或者域名的话，可能需要通过域名解析器解析
            bootstrap.resolver(clientResources.addressResolverGroup());
        }

        // 从ClientOptions中获取SocketOptions
        SocketOptions options = clientOptions.getSocketOptions();
        // NioEventLoopGroup
        EventLoopGroup eventLoopGroup = eventLoopGroupProvider.apply(eventLoopGroupClass);

        // 2、设置Socket的超时时间
        bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, Math.toIntExact(options.getConnectTimeout().toMillis()));

        if (!domainSocket) {
            // 3、根据clientOptions中的定义，设置是否关闭tcp心跳检测，是否关闭nagle算法，默认情况下这两个都是关的
            bootstrap.option(ChannelOption.SO_KEEPALIVE, options.isKeepAlive());
            bootstrap.option(ChannelOption.TCP_NODELAY, options.isTcpNoDelay());

            if (options.isEnableTcpUserTimeout()) {

                SocketOptions.TcpUserTimeoutOptions tcpUserTimeoutOptions = options.getTcpUserTimeout();

                if (IOUringProvider.isAvailable()) {
                    IOUringProvider.applyTcpUserTimeout(bootstrap, tcpUserTimeoutOptions.getTcpUserTimeout());
                } else if (io.lettuce.core.resource.EpollProvider.isAvailable()) {
                    EpollProvider.applyTcpUserTimeout(bootstrap, tcpUserTimeoutOptions.getTcpUserTimeout());
                } else {
                    logger.warn("Cannot apply TCP User Timeout options to channel type " + channelClass.getName());
                }
            }
        }

        // 4、设置channel的实现为NioSocketChannel，
        // 5、设置eventLoopGroup为前面创建的NioEventLoopGroup
        bootstrap.channel(channelClass).group(eventLoopGroup);

        if (options.isKeepAlive() && options.isExtendedKeepAlive()) {

            SocketOptions.KeepAliveOptions keepAlive = options.getKeepAlive();

            if (IOUringProvider.isAvailable()) {
                IOUringProvider.applyKeepAlive(bootstrap, keepAlive.getCount(), keepAlive.getIdle(), keepAlive.getInterval());
            } else if (io.lettuce.core.resource.EpollProvider.isAvailable()) {
                EpollProvider.applyKeepAlive(bootstrap, keepAlive.getCount(), keepAlive.getIdle(), keepAlive.getInterval());
            } else if (ExtendedNioSocketOptions.isAvailable() && !KqueueProvider.isAvailable()) {
                ExtendedNioSocketOptions.applyKeepAlive(bootstrap, keepAlive.getCount(), keepAlive.getIdle(),
                        keepAlive.getInterval());
            } else {
                logger.warn("Cannot apply extended TCP keepalive options to channel type " + channelClass.getName());
            }
        }

    }

    public RedisChannelHandler<?, ?> connection() {
        return connection;
    }

    public Bootstrap bootstrap() {
        return bootstrap;
    }

    public ClientOptions clientOptions() {
        return clientOptions;
    }

    public ClientResources clientResources() {
        return clientResources;
    }

    public Endpoint endpoint() {
        return endpoint;
    }

    public RedisURI getRedisURI() {
        return redisURI;
    }

    static class PlainChannelInitializer extends ChannelInitializer<Channel> {

        private final Supplier<List<ChannelHandler>> handlers;

        private final ClientResources clientResources;

        PlainChannelInitializer(Supplier<List<ChannelHandler>> handlers, ClientResources clientResources) {
            this.handlers = handlers;
            this.clientResources = clientResources;
        }

        @Override
        protected void initChannel(Channel channel) {
            doInitialize(channel);
        }

        private void doInitialize(Channel channel) {

            for (ChannelHandler handler : handlers.get()) {
                channel.pipeline().addLast(handler);
            }

            clientResources.nettyCustomizer().afterChannelInitialized(channel);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            ctx.channel().attr(INIT_FAILURE).set(cause);
            super.exceptionCaught(ctx, cause);
        }

    }

    /**
     * Utility to support Java 11 {@link ExtendedSocketOptions extended keepalive options}.
     */
    @SuppressWarnings("unchecked")
    static class ExtendedNioSocketOptions {

        private static final SocketOption<Integer> TCP_KEEPCOUNT;

        private static final SocketOption<Integer> TCP_KEEPIDLE;

        private static final SocketOption<Integer> TCP_KEEPINTERVAL;

        static {

            SocketOption<Integer> keepCount = null;
            SocketOption<Integer> keepIdle = null;
            SocketOption<Integer> keepInterval = null;
            try {

                keepCount = (SocketOption<Integer>) ExtendedSocketOptions.class.getDeclaredField("TCP_KEEPCOUNT").get(null);
                keepIdle = (SocketOption<Integer>) ExtendedSocketOptions.class.getDeclaredField("TCP_KEEPIDLE").get(null);
                keepInterval = (SocketOption<Integer>) ExtendedSocketOptions.class.getDeclaredField("TCP_KEEPINTERVAL")
                        .get(null);
            } catch (ReflectiveOperationException e) {
                logger.trace("Cannot extract ExtendedSocketOptions for KeepAlive", e);
            }

            TCP_KEEPCOUNT = keepCount;
            TCP_KEEPIDLE = keepIdle;
            TCP_KEEPINTERVAL = keepInterval;
        }

        public static boolean isAvailable() {
            return TCP_KEEPCOUNT != null && TCP_KEEPIDLE != null && TCP_KEEPINTERVAL != null;
        }

        /**
         * Apply Keep-Alive options.
         *
         */
        public static void applyKeepAlive(Bootstrap bootstrap, int count, Duration idle, Duration interval) {

            bootstrap.option(NioChannelOption.of(TCP_KEEPCOUNT), count);
            bootstrap.option(NioChannelOption.of(TCP_KEEPIDLE), Math.toIntExact(idle.getSeconds()));
            bootstrap.option(NioChannelOption.of(TCP_KEEPINTERVAL), Math.toIntExact(interval.getSeconds()));
        }

    }

}
