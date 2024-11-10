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

import java.io.Closeable;
import java.net.SocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import reactor.core.publisher.Mono;
import io.lettuce.core.event.command.CommandListener;
import io.lettuce.core.event.connection.ConnectEvent;
import io.lettuce.core.event.connection.ConnectionCreatedEvent;
import io.lettuce.core.event.jfr.EventRecorder;
import io.lettuce.core.internal.AsyncCloseable;
import io.lettuce.core.internal.Exceptions;
import io.lettuce.core.internal.Futures;
import io.lettuce.core.internal.LettuceAssert;
import io.lettuce.core.internal.LettuceStrings;
import io.lettuce.core.protocol.ConnectionWatchdog;
import io.lettuce.core.protocol.RedisHandshakeHandler;
import io.lettuce.core.resource.ClientResources;
import io.lettuce.core.resource.DefaultClientResources;
import io.lettuce.core.resource.Transports;
import io.lettuce.core.resource.Transports.NativeTransports;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.EventExecutorGroup;
import io.netty.util.concurrent.Future;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

/**
 * Base Redis client. This class holds the netty infrastructure, {@link ClientOptions} and the basic connection procedure. This
 * class creates the netty {@link EventLoopGroup}s for NIO ({@link NioEventLoopGroup}) and EPoll (
 * {@link io.netty.channel.epoll.EpollEventLoopGroup}) with a default of {@code Runtime.getRuntime().availableProcessors() * 4}
 * threads. Reuse the instance as much as possible since the {@link EventLoopGroup} instances are expensive and can consume a
 * huge part of your resources, if you create multiple instances.
 * <p>
 * You can set the number of threads per {@link NioEventLoopGroup} by setting the {@code io.netty.eventLoopThreads} system
 * property to a reasonable number of threads.
 * </p>
 *
 * @author Mark Paluch
 * @author Jongyeol Choi
 * @author Poorva Gokhale
 * @since 3.0
 * @see ClientResources
 */
public abstract class AbstractRedisClient implements AutoCloseable {

    private static final InternalLogger logger = InternalLoggerFactory.getInstance(AbstractRedisClient.class);

    private static final int EVENTLOOP_ACQ_INACTIVE = 0;

    private static final int EVENTLOOP_ACQ_ACTIVE = 1;

    private final AtomicInteger eventLoopGroupCas = new AtomicInteger();

    protected final ConnectionEvents connectionEvents = new ConnectionEvents();

    protected final Set<Closeable> closeableResources = ConcurrentHashMap.newKeySet();

    protected final ChannelGroup channels;

    private final ClientResources clientResources;

    private final List<CommandListener> commandListeners = new ArrayList<>();

    private final Map<Class<? extends EventLoopGroup>, EventLoopGroup> eventLoopGroups = new ConcurrentHashMap<>(2);

    private final boolean sharedResources;

    private final AtomicBoolean shutdown = new AtomicBoolean();

    private volatile ClientOptions clientOptions = ClientOptions.create();

    private volatile Duration defaultTimeout = RedisURI.DEFAULT_TIMEOUT_DURATION;

    /**
     * Create a new instance with client resources.
     *
     * @param clientResources the client resources. If {@code null}, the client will create a new dedicated instance of client
     *        resources and keep track of them.
     */
    protected AbstractRedisClient(ClientResources clientResources) {

        if (clientResources == null) {
            this.sharedResources = false;
            this.clientResources = DefaultClientResources.create();
        } else {
            this.sharedResources = true;
            this.clientResources = clientResources;
        }

        // DefaultChannelGroup 的底层通过一个 ConcurrentMap 集合来给管理所有的通道,并从计算线程池中获取一个线程来处理通道的事件
        this.channels = new DefaultChannelGroup(this.clientResources.eventExecutorGroup().next());
    }

    protected int getChannelCount() {
        return channels.size();
    }

    /**
     * Returns the default {@link Duration timeout} for commands.
     *
     * @return the default {@link Duration timeout} for commands.
     * @deprecated since 6.2, use {@link RedisURI#getTimeout()} to control timeouts.
     */
    @Deprecated
    public Duration getDefaultTimeout() {
        return defaultTimeout;
    }

    /**
     * Set the default timeout for connections created by this client. The timeout applies to connection attempts and
     * non-blocking commands.
     *
     * @param timeout default connection timeout, must not be {@code null}.
     * @since 5.0
     * @deprecated since 6.2, use {@link RedisURI#getTimeout()} to control timeouts.
     */
    @Deprecated
    public void setDefaultTimeout(Duration timeout) {

        LettuceAssert.notNull(timeout, "Timeout duration must not be null");
        LettuceAssert.isTrue(!timeout.isNegative(), "Timeout duration must be greater or equal to zero");

        this.defaultTimeout = timeout;
    }

    /**
     * Set the default timeout for connections created by this client. The timeout applies to connection attempts and
     * non-blocking commands.
     *
     * @param timeout Default connection timeout.
     * @param unit Unit of time for the timeout.
     * @deprecated since 6.2, use {@link RedisURI#getTimeout()} to control timeouts.
     */
    @Deprecated
    public void setDefaultTimeout(long timeout, TimeUnit unit) {
        setDefaultTimeout(Duration.ofNanos(unit.toNanos(timeout)));
    }

    /**
     * Returns the {@link ClientOptions} which are valid for that client. Connections inherit the current options at the moment
     * the connection is created. Changes to options will not affect existing connections.
     *
     * @return the {@link ClientOptions} for this client
     */
    public ClientOptions getOptions() {
        return clientOptions;
    }

    /**
     * Set the {@link ClientOptions} for the client.
     *
     * @param clientOptions client options for the client and connections that are created after setting the options
     */
    protected void setOptions(ClientOptions clientOptions) {
        LettuceAssert.notNull(clientOptions, "ClientOptions must not be null");
        this.clientOptions = clientOptions;
    }

    /**
     * Returns the {@link ClientResources} which are used with that client.
     *
     * @return the {@link ClientResources} for this client.
     * @since 6.0
     *
     */
    public ClientResources getResources() {
        return clientResources;
    }

    protected int getResourceCount() {
        return closeableResources.size();
    }

    /**
     * Add a listener for the RedisConnectionState. The listener is notified every time a connect/disconnect/IO exception
     * happens. The listeners are not bound to a specific connection, so every time a connection event happens on any
     * connection, the listener will be notified. The corresponding netty channel handler (async connection) is passed on the
     * event.
     *
     * @param listener must not be {@code null}.
     */
    public void addListener(RedisConnectionStateListener listener) {

        LettuceAssert.notNull(listener, "RedisConnectionStateListener must not be null");
        connectionEvents.addListener(listener);
    }

    /**
     * Removes a listener.
     *
     * @param listener must not be {@code null}.
     */
    public void removeListener(RedisConnectionStateListener listener) {

        LettuceAssert.notNull(listener, "RedisConnectionStateListener must not be null");
        connectionEvents.removeListener(listener);
    }

    /**
     * Add a listener for Redis Command events. The listener is notified on each command start/success/failure.
     *
     * @param listener must not be {@code null}.
     * @since 6.1
     */
    public void addListener(CommandListener listener) {

        LettuceAssert.notNull(listener, "CommandListener must not be null");
        commandListeners.add(listener);
    }

    /**
     * Removes a listener.
     *
     * @param listener must not be {@code null}.
     * @since 6.1
     */
    public void removeListener(CommandListener listener) {

        LettuceAssert.notNull(listener, "CommandListener must not be null");
        commandListeners.remove(listener);
    }

    protected List<CommandListener> getCommandListeners() {
        return commandListeners;
    }

    /**
     * Populate connection builder with necessary resources.
     *
     * @param socketAddressSupplier address supplier for initial connect and re-connect
     * @param connectionBuilder connection builder to configure the connection
     * @param redisURI URI of the Redis instance
     */
    protected void connectionBuilder(Mono<SocketAddress> socketAddressSupplier, ConnectionBuilder connectionBuilder,
            RedisURI redisURI) {
        connectionBuilder(socketAddressSupplier, connectionBuilder, connectionEvents, redisURI);
    }

    /**
     * Populate connection builder with necessary resources.
     * 用必要的资源填充connectionBuilder
     *
     * @param socketAddressSupplier address supplier for initial connect and re-connect
     * @param connectionBuilder connection builder to configure the connection
     * @param connectionEvents connection events dispatcher
     * @param redisURI URI of the Redis instance
     * @since 6.2
     */
    protected void connectionBuilder(Mono<SocketAddress> socketAddressSupplier, ConnectionBuilder connectionBuilder,
            ConnectionEvents connectionEvents, RedisURI redisURI) {

        // 创建Netty客户端需要的Bootstrap对象，下面的主要过程其实都是在配置这个 Bootstrap
        Bootstrap redisBootstrap = new Bootstrap();
        // 1、配置内存分配器，这里配置的是默认的内存分配器，即PooledByteBufAllocator.DEFAULT
        redisBootstrap.option(ChannelOption.ALLOCATOR, ByteBufAllocator.DEFAULT);

        connectionBuilder.bootstrap(redisBootstrap);
        // 2、设置一个名为 RedisURI 的属性到 Bootstrap 中，这个属性最终会给到创建出来的Channel上
        connectionBuilder.apply(redisURI);
        // 3、为这个Bootstrap配置Channel的类型，这里的Channel类型是NioSocketChannel
        // 4、为这个Bootstrap配置EventLoopGroup，这里的EventLoopGroup是NioEventLoopGroup
        connectionBuilder.configureBootstrap(!LettuceStrings.isEmpty(redisURI.getSocket()), this::getEventLoopGroup);
        connectionBuilder.channelGroup(channels).connectionEvents(connectionEvents == this.connectionEvents ? connectionEvents
                : ConnectionEvents.of(this.connectionEvents, connectionEvents));
        connectionBuilder.socketAddressSupplier(socketAddressSupplier);
    }

    protected void channelType(ConnectionBuilder connectionBuilder, ConnectionPoint connectionPoint) {

        LettuceAssert.notNull(connectionPoint, "ConnectionPoint must not be null");

        boolean domainSocket = LettuceStrings.isNotEmpty(connectionPoint.getSocket());
        connectionBuilder.bootstrap().group(getEventLoopGroup(
                domainSocket ? NativeTransports.eventLoopGroupClass(true) : Transports.eventLoopGroupClass()));

        if (connectionPoint.getSocket() != null) {
            NativeTransports.assertDomainSocketAvailable();
            connectionBuilder.bootstrap().channel(NativeTransports.domainSocketChannelClass());
        } else {
            connectionBuilder.bootstrap().channel(Transports.socketChannelClass());
        }
    }

    private EventLoopGroup getEventLoopGroup(Class<? extends EventLoopGroup> eventLoopGroupClass) {

        // 默认情况下， eventLoopGroup的类型是 NioEventLoopGroup
        for (;;) {
            if (!eventLoopGroupCas.compareAndSet(EVENTLOOP_ACQ_INACTIVE, EVENTLOOP_ACQ_ACTIVE)) {
                continue;
            }

            try {
                // eventLoopGroups是EventLoopGroup 的类型->EventLoopGroupProvider
                // computeIfAbsent方法中指定的lambda表达式在没有key的情况下立即调用。其中：
                // 1、clientResources默认的EventLoopGroupProvider是 DefaultEventLoopGroupProvider，所以调用的是 DefaultEventLoopGroupProvider 的 allocate 方法
                // 2、DefaultEventLoopGroupProvider 的 allocate 方法中会根据传入的参数（即当前的key NioEventLoopGroup）创建一个 NioEventLoopGroup 对象
                return eventLoopGroups.computeIfAbsent(eventLoopGroupClass,
                        it -> clientResources.eventLoopGroupProvider().allocate(it));
            } finally {
                eventLoopGroupCas.set(EVENTLOOP_ACQ_INACTIVE);
            }
        }
    }

    /**
     * Retrieve the connection from {@link ConnectionFuture}. Performs a blocking {@link ConnectionFuture#get()} to synchronize
     * the channel/connection initialization. Any exception is rethrown as {@link RedisConnectionException}.
     *
     * @param connectionFuture must not be null.
     * @param <T> Connection type.
     * @return the connection.
     * @throws RedisConnectionException in case of connection failures.
     * @since 4.4
     */
    protected <T> T getConnection(ConnectionFuture<T> connectionFuture) {

        try {
            // 异步转同步
            return connectionFuture.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw RedisConnectionException.create(connectionFuture.getRemoteAddress(), e);
        } catch (Exception e) {
            throw RedisConnectionException.create(connectionFuture.getRemoteAddress(), Exceptions.unwrap(e));
        }
    }

    /**
     * Retrieve the connection from {@link ConnectionFuture}. Performs a blocking {@link ConnectionFuture#get()} to synchronize
     * the channel/connection initialization. Any exception is rethrown as {@link RedisConnectionException}.
     *
     * @param connectionFuture must not be null.
     * @param <T> Connection type.
     * @return the connection.
     * @throws RedisConnectionException in case of connection failures.
     * @since 5.0
     */
    protected <T> T getConnection(CompletableFuture<T> connectionFuture) {

        try {
            return connectionFuture.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw RedisConnectionException.create(e);
        } catch (Exception e) {
            throw RedisConnectionException.create(Exceptions.unwrap(e));
        }
    }

    /**
     * Connect and initialize a channel from {@link ConnectionBuilder}.
     *
     * @param connectionBuilder must not be {@code null}.
     * @return the {@link ConnectionFuture} to synchronize the connection process.
     * @since 4.4
     */
    @SuppressWarnings("unchecked")
    protected <K, V, T extends RedisChannelHandler<K, V>> ConnectionFuture<T> initializeChannelAsync(
            ConnectionBuilder connectionBuilder) {

        // 订阅这个Mono可以返回用户在参数中指定的redis的地址和端口
        Mono<SocketAddress> socketAddressSupplier = connectionBuilder.socketAddress();

        if (clientResources.eventExecutorGroup().isShuttingDown()) {
            // 计算线程池已经关闭，不能再创建新的连接了
            throw new IllegalStateException("Cannot connect, Event executor group is terminated.");
        }

        CompletableFuture<SocketAddress> socketAddressFuture = new CompletableFuture<>();
        CompletableFuture<Channel> channelReadyFuture = new CompletableFuture<>();

        String uriString = connectionBuilder.getRedisURI().toString();

        EventRecorder.getInstance().record(new ConnectionCreatedEvent(uriString, connectionBuilder.endpoint().getId()));
        EventRecorder.RecordableEvent event = EventRecorder.getInstance()
                .start(new ConnectEvent(uriString, connectionBuilder.endpoint().getId()));

        channelReadyFuture.whenComplete((channel, throwable) -> {
            // 连接就绪之后，产生一个ConnectEvent事件通知
            event.record();
        });

        // 1、实际的创建连接的操作放在了这个Mono的subscribe方法中
        // subscribe方法会阻塞当前线程，直到subscribe中的操作完成， 但是initializeChannelAsync0中的操作是异步的，也不会阻塞
        socketAddressSupplier.doOnError(socketAddressFuture::completeExceptionally).doOnNext(socketAddressFuture::complete)
                .subscribe(redisAddress -> {

                    if (channelReadyFuture.isCancelled()) {
                        return;
                    }
                    // 2、真正的创建连接的操作
                    initializeChannelAsync0(connectionBuilder, channelReadyFuture, redisAddress);
                }, channelReadyFuture::completeExceptionally);

        // 3、前面的subscribe方法是异步的，所以这里返回一个ConnectionFuture对象，用于同步等待连接的建立
        // channelReadyFuture这个future会在连接就绪之后设置为完成。完成之后，下面的thenApply方法会执行，返回一个连接
        return new DefaultConnectionFuture<>(socketAddressFuture,
                channelReadyFuture.thenApply(channel -> (T) connectionBuilder.connection()));
    }

    private void initializeChannelAsync0(ConnectionBuilder connectionBuilder, CompletableFuture<Channel> channelReadyFuture,
            SocketAddress redisAddress) {

        logger.debug("Connecting to Redis at {}", redisAddress);

        // 客户端的Bootstrap对象
        Bootstrap redisBootstrap = connectionBuilder.bootstrap();

        // 1、向客户端的pipeline中添加handler定义
        ChannelInitializer<Channel> initializer = connectionBuilder.build(redisAddress);
        redisBootstrap.handler(initializer);

        // 用户可以通过自己实现NettyCustomizer接口来针对redisBootstrap做一些额外的配置
        clientResources.nettyCustomizer().afterBootstrapInitialized(redisBootstrap);
        // 2、连接到redis服务器，这个方法里面中几个关键操作
        // 2.1、创建一个 NioSocketChannel 并找bootStrap中指定的配置进行初始化，包括创建这个channel对应的pipeline；
        // 2.2、将前 redisBootstrap 中指定的handler添加到这个channel的pipeline中；
        // 2.3、将当前的`NioSocketChannel`注册到`EventLoopGroup`中的某个`NioEventLoop`的`Selector`上,这样这个Selector就可以select到这个channel上发生的事件了。
        // 2.4、调用 java nio的socketChannel.connect方法，实际连接远程地址。
        ChannelFuture connectFuture = redisBootstrap.connect(redisAddress);

        channelReadyFuture.whenComplete((c, t) -> {

            if (t instanceof CancellationException) {
                connectFuture.cancel(true);
            }
        });

        connectFuture.addListener(future -> {

            Channel channel = connectFuture.channel();
            if (!future.isSuccess()) {

                Throwable cause = future.cause();
                Throwable detail = channel.attr(ConnectionBuilder.INIT_FAILURE).get();

                if (detail != null) {
                    detail.addSuppressed(cause);
                    cause = detail;
                }

                logger.debug("Connecting to Redis at {}: {}", redisAddress, cause);
                connectionBuilder.endpoint().initialState();
                channelReadyFuture.completeExceptionally(cause);
                return;
            }

            RedisHandshakeHandler handshakeHandler = channel.pipeline().get(RedisHandshakeHandler.class);

            if (handshakeHandler == null) {
                channelReadyFuture.completeExceptionally(new IllegalStateException("RedisHandshakeHandler not registered"));
                return;
            }

            // 3、添加一个监听，在channel初始化完成的时候,让 channelReadyFuture 完成，这样外部就可以通过这个future来获取到channel了
            handshakeHandler.channelInitialized().whenComplete((success, throwable) -> {

                if (throwable == null) {

                    logger.debug("Connecting to Redis at {}: Success", redisAddress);
                    RedisChannelHandler<?, ?> connection = connectionBuilder.connection();
                    connection.registerCloseables(closeableResources, connection);
                    // 4、channel初始化完成之后，将channelReadyFuture标记为完成,证明连接已经就绪了，并且指定这个channelReadyFuture的结果为创建的channel
                    channelReadyFuture.complete(channel);
                    return;
                }

                logger.debug("Connecting to Redis at {}, initialization: {}", redisAddress, throwable);
                connectionBuilder.endpoint().initialState();
                channelReadyFuture.completeExceptionally(throwable);
            });
        });
    }

    /**
     * Shutdown this client and close all open connections once this method is called. Once all connections are closed, the
     * associated {@link ClientResources} are shut down/released gracefully considering quiet time and the shutdown timeout. The
     * client should be discarded after calling shutdown. The shutdown is executed without quiet time and a timeout of 2
     * {@link TimeUnit#SECONDS}.
     *
     * @see EventExecutorGroup#shutdownGracefully(long, long, TimeUnit)
     */
    public void shutdown() {
        shutdown(0, 2, TimeUnit.SECONDS);
    }

    @Override
    public void close() {
        shutdown();
    }

    /**
     * Shutdown this client and close all open connections once this method is called. Once all connections are closed, the
     * associated {@link ClientResources} are shut down/released gracefully considering quiet time and the shutdown timeout. The
     * client should be discarded after calling shutdown.
     *
     * @param quietPeriod the quiet period to allow the executor gracefully shut down.
     * @param timeout the maximum amount of time to wait until the backing executor is shutdown regardless if a task was
     *        submitted during the quiet period.
     * @since 5.0
     * @see EventExecutorGroup#shutdownGracefully(long, long, TimeUnit)
     */
    public void shutdown(Duration quietPeriod, Duration timeout) {
        shutdown(quietPeriod.toNanos(), timeout.toNanos(), TimeUnit.NANOSECONDS);
    }

    /**
     * Shutdown this client and close all open connections once this method is called. Once all connections are closed, the
     * associated {@link ClientResources} are shut down/released gracefully considering quiet time and the shutdown timeout. The
     * client should be discarded after calling shutdown.
     *
     * @param quietPeriod the quiet period to allow the executor gracefully shut down.
     * @param timeout the maximum amount of time to wait until the backing executor is shutdown regardless if a task was
     *        submitted during the quiet period.
     * @param timeUnit the unit of {@code quietPeriod} and {@code timeout}.
     * @see EventExecutorGroup#shutdownGracefully(long, long, TimeUnit)
     */
    public void shutdown(long quietPeriod, long timeout, TimeUnit timeUnit) {

        try {
            shutdownAsync(quietPeriod, timeout, timeUnit).get();
        } catch (Exception e) {
            throw Exceptions.bubble(e);
        }
    }

    /**
     * Shutdown this client and close all open connections asynchronously. Once all connections are closed, the associated
     * {@link ClientResources} are shut down/released gracefully considering quiet time and the shutdown timeout. The client
     * should be discarded after calling shutdown. The shutdown is executed without quiet time and a timeout of 2
     * {@link TimeUnit#SECONDS}.
     *
     * @since 4.4
     * @see EventExecutorGroup#shutdownGracefully(long, long, TimeUnit)
     */
    public CompletableFuture<Void> shutdownAsync() {
        return shutdownAsync(0, 2, TimeUnit.SECONDS);
    }

    /**
     * Shutdown this client and close all open connections asynchronously. Once all connections are closed, the associated
     * {@link ClientResources} are shut down/released gracefully considering quiet time and the shutdown timeout. The client
     * should be discarded after calling shutdown.
     *
     * @param quietPeriod the quiet period to allow the executor gracefully shut down.
     * @param timeout the maximum amount of time to wait until the backing executor is shutdown regardless if a task was
     *        submitted during the quiet period.
     * @param timeUnit the unit of {@code quietPeriod} and {@code timeout}.
     * @since 4.4
     * @see EventExecutorGroup#shutdownGracefully(long, long, TimeUnit)
     */
    public CompletableFuture<Void> shutdownAsync(long quietPeriod, long timeout, TimeUnit timeUnit) {

        if (shutdown.compareAndSet(false, true)) {

            logger.debug("Initiate shutdown ({}, {}, {})", quietPeriod, timeout, timeUnit);
            return closeResources().thenCompose((value) -> closeClientResources(quietPeriod, timeout, timeUnit));
        }

        return CompletableFuture.completedFuture(null);
    }

    private CompletableFuture<Void> closeResources() {

        List<CompletionStage<Void>> closeFutures = new ArrayList<>();
        List<Closeable> closeableResources = new ArrayList<>(this.closeableResources);

        for (Closeable closeableResource : closeableResources) {

            if (closeableResource instanceof AsyncCloseable) {

                closeFutures.add(((AsyncCloseable) closeableResource).closeAsync());
            } else {
                try {
                    closeableResource.close();
                } catch (Exception e) {
                    logger.debug("Exception on Close: " + e.getMessage(), e);
                }
            }
            this.closeableResources.remove(closeableResource);
        }

        for (Channel c : channels.toArray(new Channel[0])) {

            if (c == null) {
                continue;
            }

            ChannelPipeline pipeline = c.pipeline();

            ConnectionWatchdog commandHandler = pipeline.get(ConnectionWatchdog.class);
            if (commandHandler != null) {
                commandHandler.setListenOnChannelInactive(false);
            }
        }

        try {
            closeFutures.add(Futures.toCompletionStage(channels.close()));
        } catch (Exception e) {
            logger.debug("Cannot close channels", e);
        }

        return Futures.allOf(closeFutures);
    }

    private CompletableFuture<Void> closeClientResources(long quietPeriod, long timeout, TimeUnit timeUnit) {
        List<CompletionStage<?>> groupCloseFutures = new ArrayList<>();
        if (!sharedResources) {
            Future<?> groupCloseFuture = clientResources.shutdown(quietPeriod, timeout, timeUnit);
            groupCloseFutures.add(Futures.toCompletionStage(groupCloseFuture));
        } else {
            for (EventLoopGroup eventExecutors : eventLoopGroups.values()) {
                Future<?> groupCloseFuture = clientResources.eventLoopGroupProvider().release(eventExecutors, quietPeriod,
                        timeout, timeUnit);
                groupCloseFutures.add(Futures.toCompletionStage(groupCloseFuture));
            }
        }
        return Futures.allOf(groupCloseFutures);
    }

    protected RedisHandshake createHandshake(ConnectionState state) {
        return new RedisHandshake(clientOptions.getConfiguredProtocolVersion(), clientOptions.isPingBeforeActivateConnection(),
                state);
    }

}
