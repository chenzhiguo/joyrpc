package io.joyrpc.transport.netty4.transport;

/*-
 * #%L
 * joyrpc
 * %%
 * Copyright (C) 2019 joyrpc.io
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import io.joyrpc.constants.Constants;
import io.joyrpc.exception.ConnectionException;
import io.joyrpc.exception.SslException;
import io.joyrpc.extension.URL;
import io.joyrpc.transport.channel.Channel;
import io.joyrpc.transport.channel.ChannelManager.Connector;
import io.joyrpc.transport.heartbeat.HeartbeatStrategy.HeartbeatMode;
import io.joyrpc.transport.netty4.binder.HandlerBinder;
import io.joyrpc.transport.netty4.channel.NettyClientChannel;
import io.joyrpc.transport.netty4.channel.NettyQuicClientChannel;
import io.joyrpc.transport.netty4.channel.NettyQuicStreamClientChannel;
import io.joyrpc.transport.netty4.handler.ConnectionChannelHandler;
import io.joyrpc.transport.netty4.handler.IdleHeartbeatHandler;
import io.joyrpc.transport.netty4.ssl.SslContextManager;
import io.joyrpc.transport.transport.AbstractClientTransport;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.socket.ChannelInputShutdownReadComplete;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.proxy.Socks5ProxyHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.incubator.codec.quic.*;
import io.netty.util.CharsetUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static io.joyrpc.constants.Constants.*;
import static io.joyrpc.transport.netty4.Plugin.HANDLER_BINDER;

/**
 * Netty客户端连接
 */
public class NettyClientTransport extends AbstractClientTransport {
    private static final Logger logger = LoggerFactory.getLogger(NettyClientTransport.class);
    /**
     * 构造函数
     *
     * @param url
     */
    public NettyClientTransport(URL url) {
        super(url);
    }

    @Override
    public Connector getConnector() {
        return this::connect;
    }

    /**
     * 创建channel
     */
    protected CompletableFuture<Channel> connect() {
        CompletableFuture<Channel> future = new CompletableFuture<>();
        //consumer不会为空
        if (codec == null) {
            future.completeExceptionally(error("codec can not be null!"));
        } else {
            final EventLoopGroup[] ioGroups = new EventLoopGroup[1];
            final Channel[] channels = new Channel[1];
            try {
                ioGroups[0] = EventLoopGroupFactory.getClientGroup(url);
                //获取SSL上下文
                SslContext sslContext = SslContextManager.getClientSslContext(url);
                //TODO 考虑根据不同的参数，创建不同的连接
                if (true) {
                    QuicSslContext context = QuicSslContextBuilder.forClient().trustManager(InsecureTrustManagerFactory.INSTANCE).
                            applicationProtocols("http/0.9").build();
                    ChannelHandler quicChannelHandler = new QuicClientCodecBuilder().sslContext(context)
                            .maxIdleTimeout(5000, TimeUnit.MILLISECONDS)
                            // .maxUdpPayloadSize(Quic.MAX_DATAGRAM_SIZE)
                            .initialMaxData(10000000)
                            .initialMaxStreamDataBidirectionalLocal(1000000)
                            .initialMaxStreamDataBidirectionalRemote(1000000)
                            .initialMaxStreamsBidirectional(100)
                            .initialMaxStreamsUnidirectional(100)
                            .build();

                    Bootstrap bs = new Bootstrap();
                    io.netty.channel.Channel channel = bs.group(ioGroups[0])
                            .channel(NioDatagramChannel.class)
                            .handler(quicChannelHandler)
                            .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, url.getPositiveInt(Constants.CONNECT_TIMEOUT_OPTION))
                            .
                            //option(ChannelOption.SO_TIMEOUT, url.getPositiveInt(Constants.SO_TIMEOUT_OPTION)).
                            //         option(ChannelOption.TCP_NODELAY, url.getBoolean(TCP_NODELAY)).
                            //         option(ChannelOption.SO_KEEPALIVE, url.getBoolean(Constants.SO_KEEPALIVE_OPTION)).
                                    option(ChannelOption.ALLOCATOR, BufAllocator.create(url)).
                                    option(ChannelOption.WRITE_BUFFER_WATER_MARK,
                                            new WriteBufferWaterMark(
                                                    url.getPositiveInt(Constants.WRITE_BUFFER_LOW_WATERMARK_OPTION),
                                                    url.getPositiveInt(Constants.WRITE_BUFFER_HIGH_WATERMARK_OPTION))).
                                    option(ChannelOption.RCVBUF_ALLOCATOR, AdaptiveRecvByteBufAllocator.DEFAULT)
                            .bind(0)
                            // .addListener((ChannelFutureListener) f -> {
                            //     if (f.isSuccess()) {
                            //         myConsumer.accept(new AsyncResult<>(channels[0]));
                            //     } else {
                            //         logger.error("Error:", f.cause());
                            //         myConsumer.accept(new AsyncResult<>(new NettyClientChannel(f.channel(), ioGroups[0]), error(f.cause())));
                            //     }
                            // })
                            .sync().channel();

                    QuicChannel.newBootstrap(channel)
                            .handler(new ChannelInitializer<io.netty.channel.Channel>() {
                                @Override
                                protected void initChannel(final io.netty.channel.Channel ch) {
                                    try {
                                        //及时发送 与 缓存发送
                                        channels[0] = new NettyQuicClientChannel(ch, ioGroups[0], quicChannel -> {
                                            try {
                                                return ((QuicChannel)quicChannel).createStream(QuicStreamType.BIDIRECTIONAL,
                                                        new ChannelInitializer<io.netty.channel.Channel>() {
                                                            @Override
                                                            protected void initChannel(final io.netty.channel.Channel ch) {
                                                                try {
                                                                    Channel streamChannel = new NettyQuicStreamClientChannel(ch, ioGroups[0]);
                                                                    //设置
                                                                    streamChannel.
                                                                            setAttribute(Channel.PAYLOAD, url.getPositiveInt(Constants.PAYLOAD)).
                                                                            setAttribute(Channel.BIZ_THREAD_POOL, bizThreadPool, (k, v) -> v != null);
                                                                    //添加连接事件监听
                                                                    ch.pipeline().addLast("connection", new ConnectionChannelHandler(streamChannel, publisher));
                                                                    //添加编解码和处理链
                                                                    HandlerBinder binder = Plugin.HANDLER_BINDER.get(codec.binder());
                                                                    binder.bind(ch.pipeline(), codec, handlerChain, streamChannel);
                                                                    //若配置idle心跳策略，配置心跳handler
                                                                    if (heartbeatStrategy != null && heartbeatStrategy.getHeartbeatMode() == HeartbeatMode.IDLE) {
                                                                        ch.pipeline().
                                                                                addLast("idleState", new IdleStateHandler(0, heartbeatStrategy.getInterval(), 0, TimeUnit.MILLISECONDS)).
                                                                                addLast("idleHeartbeat", new IdleHeartbeatHandler());
                                                                    }
                                                                    if (sslContext != null) {
                                                                        ch.pipeline().addFirst("ssl", sslContext.newHandler(ch.alloc()));
                                                                    }
                                                                    //若开启了ss5代理，添加ss5
                                                                    if (url.getBoolean(SS5_ENABLE)) {
                                                                        String host = url.getString(SS5_HOST);
                                                                        if (host != null && !host.isEmpty()) {
                                                                            InetSocketAddress ss5Address = new InetSocketAddress(host, url.getInteger(SS5_PORT));
                                                                            ch.pipeline().addFirst("ss5", new Socks5ProxyHandler(ss5Address, url.getString(SS5_USER), url.getString(SS5_PASSWORD)));
                                                                        }
                                                                    }
                                                                } catch (Exception e) {
                                                                    logger.error("Error:", e);
                                                                }
                                                            }
                                                        }).sync().getNow();
                                            } catch (InterruptedException e) {
                                                e.printStackTrace();
                                            }
                                            return null;
                                        }
                                        );
                                        //设置
                                        channels[0].
                                                setAttribute(Channel.PAYLOAD, url.getPositiveInt(PAYLOAD)).
                                                setAttribute(Channel.BIZ_THREAD_POOL, bizThreadPool, (k, v) -> v != null);
                                        //添加连接事件监听
                                        // ch.pipeline().addLast("connection", new ConnectionChannelHandler(channels[0], publisher));
                                        //添加编解码和处理链
                                        // HandlerBinder binder = Plugin.HANDLER_BINDER.get(codec.binder());
                                        // binder.bind(ch.pipeline(), codec, handlerChain, channels[0]);
                                        //若配置idle心跳策略，配置心跳handler
                                        // if (heartbeatStrategy != null && heartbeatStrategy.getHeartbeatMode() == HeartbeatMode.IDLE) {
                                        //     ch.pipeline().
                                        //             addLast("idleState", new IdleStateHandler(0, heartbeatStrategy.getInterval(), 0, TimeUnit.MILLISECONDS)).
                                        //             addLast("idleHeartbeat", new IdleHeartbeatHandler());
                                        // }
                                        // if (sslContext != null) {
                                        //     ch.pipeline().addFirst("ssl", sslContext.newHandler(ch.alloc()));
                                        // }
                                        // //若开启了ss5代理，添加ss5
                                        // if (url.getBoolean(SS5_ENABLE)) {
                                        //     String host = url.getString(SS5_HOST);
                                        //     if (host != null && !host.isEmpty()) {
                                        //         InetSocketAddress ss5Address = new InetSocketAddress(host, url.getInteger(SS5_PORT));
                                        //         ch.pipeline().addFirst("ss5", new Socks5ProxyHandler(ss5Address, url.getString(SS5_USER), url.getString(SS5_PASSWORD)));
                                        //     }
                                        // }
                                    } catch (Exception e) {
                                        logger.error("Error:", e);
                                    }
                                }
                            })
                            .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, url.getPositiveInt(CONNECT_TIMEOUT_OPTION)).
                            //option(ChannelOption.SO_TIMEOUT, url.getPositiveInt(Constants.SO_TIMEOUT_OPTION)).
                            //         option(ChannelOption.TCP_NODELAY, url.getBoolean(TCP_NODELAY)).
                            //         option(ChannelOption.SO_KEEPALIVE, url.getBoolean(SO_KEEPALIVE_OPTION)).
                                    option(ChannelOption.ALLOCATOR, BufAllocator.create(url)).
                                    option(ChannelOption.WRITE_BUFFER_WATER_MARK,
                                            new WriteBufferWaterMark(
                                                    url.getPositiveInt(WRITE_BUFFER_LOW_WATERMARK_OPTION),
                                                    url.getPositiveInt(WRITE_BUFFER_HIGH_WATERMARK_OPTION))).
                                    option(ChannelOption.RCVBUF_ALLOCATOR, AdaptiveRecvByteBufAllocator.DEFAULT)
                            // .streamHandler(new ChannelInboundHandlerAdapter() {
                            //     @Override
                            //     public void channelActive(ChannelHandlerContext ctx) {
                            //         // We don't want to handle streams created by the server side, just close the
                            //         // stream and so send a fin.
                            //         ctx.close();
                            //     }
                            //
                            //     @Override
                            //     public void channelInactive(ChannelHandlerContext ctx) throws Exception {
                            //         super.channelInactive(ctx);
                            //         ((QuicChannel) ctx.channel().parent()).close(true, 0,
                            //                 ctx.alloc().directBuffer(16)
                            //                         .writeBytes(new byte[]{'k', 't', 'h', 'x', 'b', 'y', 'e'}));
                            //     }
                            // })
                            .remoteAddress(new InetSocketAddress(url.getHost(), url.getPort()))
                            .connect()
                            .addListener(future -> {
                                if (future.isSuccess()) {
                                    myConsumer.accept(new AsyncResult<>(channels[0]));
                                } else {
                                    logger.error("Error:", future.cause());
                                    myConsumer.accept(new AsyncResult<>(new NettyClientChannel((io.netty.channel.Channel) future.getNow(), ioGroups[0]), error(future.cause())));
                                }
                            })
                            // .addListener((ChannelFutureListener) f -> {
                            //     if (f.isSuccess()) {
                            //         myConsumer.accept(new AsyncResult<>(channels[0]));
                            //     } else {
                            //         logger.error("Error:", f.cause());
                            //         myConsumer.accept(new AsyncResult<>(new NettyClientChannel(f.channel(), ioGroups[0]), error(f.cause())));
                            //     }
                            // })
                            .get();

                    // QuicStreamChannel streamChannel = quicChannel.createStream(QuicStreamType.BIDIRECTIONAL,
                    //         new ChannelInboundHandlerAdapter() {
                    //             @Override
                    //             public void channelRead(ChannelHandlerContext ctx, Object msg) {
                    //                 ByteBuf byteBuf = (ByteBuf) msg;
                    //                 System.err.println(byteBuf.toString(CharsetUtil.US_ASCII));
                    //                 byteBuf.release();
                    //             }
                    //
                    //             @Override
                    //             public void channelInactive(ChannelHandlerContext ctx) {
                    //                 // Close the connection once the remote peer did close this stream.
                    //                 ((QuicChannel) ctx.channel().parent()).close(true, 0,
                    //                         ctx.alloc().directBuffer(16)
                    //                                 .writeBytes(new byte[]{'k', 't', 'h', 'x', 'b', 'y', 'e'}));
                    //             }
                    //         }
                    // new ChannelInitializer<io.netty.channel.Channel>() {
                    //     @Override
                    //     protected void initChannel(final io.netty.channel.Channel ch) {
                    //         try {
                    //             //及时发送 与 缓存发送
                    //             channels[0] = new NettyClientChannel(ch, ioGroups[0]);
                    //             //设置
                    //             channels[0].
                    //                     setAttribute(Channel.PAYLOAD, url.getPositiveInt(Constants.PAYLOAD)).
                    //                     setAttribute(Channel.BIZ_THREAD_POOL, bizThreadPool, (k, v) -> v != null);
                    //             //添加连接事件监听
                    //             ch.pipeline().addLast("connection", new ConnectionChannelHandler(channels[0], publisher));
                    //             //添加编解码和处理链
                    //             HandlerBinder binder = Plugin.HANDLER_BINDER.get(codec.binder());
                    //             binder.bind(ch.pipeline(), codec, handlerChain, channels[0]);
                    //             //若配置idle心跳策略，配置心跳handler
                    //             if (heartbeatStrategy != null && heartbeatStrategy.getHeartbeatMode() == HeartbeatMode.IDLE) {
                    //                 ch.pipeline().
                    //                         addLast("idleState", new IdleStateHandler(0, heartbeatStrategy.getInterval(), 0, TimeUnit.MILLISECONDS)).
                    //                         addLast("idleHeartbeat", new IdleHeartbeatHandler());
                    //             }
                    //             if (sslContext != null) {
                    //                 ch.pipeline().addFirst("ssl", sslContext.newHandler(ch.alloc()));
                    //             }
                    //             //若开启了ss5代理，添加ss5
                    //             if (url.getBoolean(SS5_ENABLE)) {
                    //                 String host = url.getString(SS5_HOST);
                    //                 if (host != null && !host.isEmpty()) {
                    //                     InetSocketAddress ss5Address = new InetSocketAddress(host, url.getInteger(SS5_PORT));
                    //                     ch.pipeline().addFirst("ss5", new Socks5ProxyHandler(ss5Address, url.getString(SS5_USER), url.getString(SS5_PASSWORD)));
                    //                 }
                    //             }
                    //         } catch (Exception e) {
                    //             logger.error("Error:", e);
                    //         }
                    //     }
                    // }
                    //         )
                    //         .addListener(future -> {
                    //             if (future.isSuccess()) {
                    //                 myConsumer.accept(new AsyncResult<>(channels[0]));
                    //             } else {
                    //                 logger.error("Error:", future.cause());
                    //                 myConsumer.accept(new AsyncResult<>(new NettyClientChannel((io.netty.channel.Channel) future.getNow(), ioGroups[0]), error(future.cause())));
                    //             }
                    //         }
                    // ).sync().getNow();
                    // ByteBuf buffer = Unpooled.directBuffer();
                    // buffer.writeCharSequence("GET /\r\n", CharsetUtil.US_ASCII);
                    // streamChannel.writeAndFlush(buffer);

                    // Wait for the stream channel and quic channel to be closed. After this is done we will
                    // close the underlying datagram channel.
                    // streamChannel.closeFuture().sync();
                    // quicChannel.closeFuture().sync();
                    // channel.close().sync();
                } else {
                    Bootstrap bootstrap = configure(new Bootstrap(), ioGroups[0], channels, sslContext);
                    // Bind and start to accept incoming connections.
                    bootstrap.connect(url.getHost(), url.getPort()).addListener((ChannelFutureListener) f -> {
                        if (f.isSuccess()) {
                            future.complete(channels[0]);
                        } else {
                            future.completeExceptionally(error(f.cause()));
                        }
                    });
                }
            } catch (SslException e) {
                future.completeExceptionally(e);
            } catch (ConnectionException e) {
                future.completeExceptionally(e);
            } catch (Throwable e) {
                //捕获Throwable，防止netty报错
                future.completeExceptionally(error(e));
            }
        }
        return future;
    }

    /**
     * 配置
     *
     * @param bootstrap  bootstrap
     * @param ioGroup    线程池
     * @param channels   通道
     * @param sslContext ssl上下文
     */
    protected Bootstrap configure(final Bootstrap bootstrap,
                                  final EventLoopGroup ioGroup,
                                  final Channel[] channels,
                                  final SslContext sslContext) {
        //Unknown channel option 'SO_BACKLOG' for channel
        bootstrap.group(ioGroup).channel(Constants.isUseEpoll(url) ? EpollSocketChannel.class : NioSocketChannel.class).
                option(ChannelOption.CONNECT_TIMEOUT_MILLIS, url.getPositiveInt(Constants.CONNECT_TIMEOUT_OPTION)).
                //option(ChannelOption.SO_TIMEOUT, url.getPositiveInt(Constants.SO_TIMEOUT_OPTION)).
                        option(ChannelOption.TCP_NODELAY, url.getBoolean(TCP_NODELAY)).
                option(ChannelOption.SO_KEEPALIVE, url.getBoolean(Constants.SO_KEEPALIVE_OPTION)).
                option(ChannelOption.ALLOCATOR, BufAllocator.create(url)).
                option(ChannelOption.WRITE_BUFFER_WATER_MARK,
                        new WriteBufferWaterMark(
                                url.getPositiveInt(Constants.WRITE_BUFFER_LOW_WATERMARK_OPTION),
                                url.getPositiveInt(Constants.WRITE_BUFFER_HIGH_WATERMARK_OPTION))).
                option(ChannelOption.RCVBUF_ALLOCATOR, AdaptiveRecvByteBufAllocator.DEFAULT).
                handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(final SocketChannel ch) {
                        //及时发送 与 缓存发送
                        channels[0] = new NettyClientChannel(ch, ioGroup);
                        //设置
                        channels[0].
                                setAttribute(Channel.PAYLOAD, url.getPositiveInt(Constants.PAYLOAD)).
                                setAttribute(Channel.BIZ_THREAD_POOL, bizThreadPool, (k, v) -> v != null);
                        //添加连接事件监听
                        ch.pipeline().addLast("connection", new ConnectionChannelHandler(channels[0], publisher));
                        //添加编解码和处理链
                        HandlerBinder binder = HANDLER_BINDER.get(codec.binder());
                        binder.bind(ch.pipeline(), codec, handlerChain, channels[0]);
                        //若配置idle心跳策略，配置心跳handler
                        if (heartbeatStrategy != null && heartbeatStrategy.getHeartbeatMode() == HeartbeatMode.IDLE) {
                            ch.pipeline().
                                    addLast("idleState", new IdleStateHandler(0, heartbeatStrategy.getInterval(), 0, TimeUnit.MILLISECONDS)).
                                    addLast("idleHeartbeat", new IdleHeartbeatHandler());
                        }
                        if (sslContext != null) {
                            ch.pipeline().addFirst("ssl", sslContext.newHandler(ch.alloc()));
                        }
                        //若开启了ss5代理，添加ss5
                        if (url.getBoolean(SS5_ENABLE)) {
                            String host = url.getString(SS5_HOST);
                            if (host != null && !host.isEmpty()) {
                                InetSocketAddress ss5Address = new InetSocketAddress(host, url.getInteger(SS5_PORT));
                                ch.pipeline().addFirst("ss5", new Socks5ProxyHandler(ss5Address, url.getString(SS5_USER), url.getString(SS5_PASSWORD)));
                            }
                        }
                    }
                });
        return bootstrap;
    }

    /**
     * 连接异常
     *
     * @param message 异常消息
     * @return 异常
     */
    protected Throwable error(final String message) {
        return message == null || message.isEmpty() ? new ConnectionException("Unknown error.") : new ConnectionException(message);
    }

    /**
     * 异常转换
     *
     * @param throwable 异常
     * @return 异常
     */
    protected Throwable error(final Throwable throwable) {
        return throwable == null ?
                new ConnectionException("Unknown error.") :
                new ConnectionException(throwable.getMessage(), throwable);
    }

}
