package io.lettuce.core.protocol;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import io.lettuce.core.RedisCommandTimeoutException;
import io.lettuce.core.RedisConnectionException;
import io.lettuce.core.internal.ExceptionFactory;
import io.lettuce.core.resource.ClientResources;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.Timeout;

/**
 * Handler to initialize a Redis Connection using a {@link ConnectionInitializer}.
 *
 * @author Mark Paluch
 * @since 6.0
 */
public class RedisHandshakeHandler extends ChannelInboundHandlerAdapter {

    private final ConnectionInitializer connectionInitializer;

    private final ClientResources clientResources;

    private final Duration initializeTimeout;

    private final CompletableFuture<Void> handshakeFuture = new CompletableFuture<>();

    private volatile boolean timedOut = false;

    public RedisHandshakeHandler(ConnectionInitializer connectionInitializer, ClientResources clientResources,
            Duration initializeTimeout) {
        this.connectionInitializer = connectionInitializer;
        this.clientResources = clientResources;
        this.initializeTimeout = initializeTimeout;
    }

    @Override
    public void channelRegistered(ChannelHandlerContext ctx) throws Exception {

        Runnable timeoutGuard = () -> {

            timedOut = true;
            if (handshakeFuture.isDone()) {
                return;
            }

            fail(ctx, new RedisCommandTimeoutException(
                    "Connection initialization timed out after " + ExceptionFactory.formatTimeout(initializeTimeout)));
        };

        Timeout timeoutHandle = clientResources.timer().newTimeout(t -> {

            if (clientResources.eventExecutorGroup().isShuttingDown()) {
                timeoutGuard.run();
                return;
            }

            clientResources.eventExecutorGroup().submit(timeoutGuard);
        }, initializeTimeout.toNanos(), TimeUnit.NANOSECONDS);

        handshakeFuture.thenAccept(ignore -> {
            timeoutHandle.cancel();
        });

        super.channelRegistered(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {

        if (shouldFail()) {
            fail(ctx, new RedisConnectionException("Connection closed prematurely"));
        }

        super.channelInactive(ctx);
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {

        // 1、主要逻辑就是使用这个方法初始化连接, 主要包括：
        // 1.1、ping命令检测连通性；
        // 1.2、select 命令选择数据库；
        // 1.3、client setname 命令设置客户端名称；
        // 1.4、client setinfo 命令设置客户端库的名称和版本
        CompletionStage<Void> future = connectionInitializer.initialize(ctx.channel());

        future.whenComplete((ignore, throwable) -> {

            if (throwable != null) {

                if (shouldFail()) {
                    fail(ctx, throwable);
                }
            } else {
                ctx.fireChannelActive();
                succeed();
            }
        });
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {

        if (shouldFail()) {
            fail(ctx, cause);
        }

        super.exceptionCaught(ctx, cause);
    }

    /**
     * Complete the handshake future successfully.
     */
    protected void succeed() {
        handshakeFuture.complete(null);
    }

    /**
     * Complete the handshake future with an error and close the channel..
     */
    protected void fail(ChannelHandlerContext ctx, Throwable cause) {

        ctx.close().addListener(closeFuture -> {
            handshakeFuture.completeExceptionally(cause);
        });
    }

    /**
     * @return future to synchronize channel initialization. Returns a new future for every reconnect.
     */
    public CompletionStage<Void> channelInitialized() {
        return handshakeFuture;
    }

    private boolean shouldFail() {
        return !handshakeFuture.isDone() && !timedOut;
    }

}
