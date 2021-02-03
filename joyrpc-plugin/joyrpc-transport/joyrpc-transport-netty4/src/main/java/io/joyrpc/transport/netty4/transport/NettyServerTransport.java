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
import io.joyrpc.extension.URL;
import io.joyrpc.transport.channel.Channel;
import io.joyrpc.transport.codec.DeductionContext;
import io.joyrpc.transport.codec.ProtocolDeduction;
import io.joyrpc.transport.netty4.channel.NettyChannel;
import io.joyrpc.transport.netty4.channel.NettyServerChannel;
import io.joyrpc.transport.netty4.codec.ProtocolDeductionContext;
import io.joyrpc.transport.netty4.handler.ConnectionChannelHandler;
import io.joyrpc.transport.netty4.handler.ProtocolDeductionAdapter;
import io.joyrpc.transport.netty4.ssl.SslContextManager;
import io.joyrpc.transport.transport.AbstractServerTransport;
import io.joyrpc.transport.transport.ChannelTransport;
import io.joyrpc.transport.transport.ServerTransport;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.ssl.SslContext;
import io.netty.incubator.codec.quic.InsecureQuicTokenHandler;
import io.netty.incubator.codec.quic.Quic;
import io.netty.incubator.codec.quic.QuicChannel;
import io.netty.incubator.codec.quic.QuicServerCodecBuilder;
import io.netty.util.CharsetUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

import static io.joyrpc.transport.codec.ProtocolDeduction.PROTOCOL_DEDUCTION_HANDLER;

/**
 * 服务端
 *
 * @date: 2019/2/21
 */
public class NettyServerTransport extends AbstractServerTransport {

    protected final BiFunction<Channel, URL, ChannelTransport> function;

    protected final Supplier<List<Channel>> supplier = this::getChannels;

    private static final Logger logger = LoggerFactory.getLogger(NettyServerTransport.class);

    /**
     * 构造函数
     *
     * @param url
     * @param function
     */
    public NettyServerTransport(final URL url,
                                final BiFunction<Channel, URL, ChannelTransport> function) {
        super(url);
        this.function = function;
    }

    /**
     * 构造函数
     *
     * @param url
     * @param beforeOpen
     * @param afterClose
     * @param function
     */
    public NettyServerTransport(final URL url,
                                final Function<ServerTransport, CompletableFuture<Void>> beforeOpen,
                                final Function<ServerTransport, CompletableFuture<Void>> afterClose,
                                final BiFunction<Channel, URL, ChannelTransport> function) {
        super(url, beforeOpen, afterClose);
        this.function = function;
    }

    @Override
    protected CompletableFuture<Channel> bind(final String host, final int port) {
        CompletableFuture<Channel> future = new CompletableFuture<>();
        //消费者不会为空
        if (codec == null && deduction == null) {
            future.completeExceptionally(new ConnectionException(
                    String.format("Failed binding server at %s:%d, caused by codec or adapter can not be null!",
                            host, port)));
        } else {
            try {
                SslContext sslContext = SslContextManager.getServerSslContext(url);
                EventLoopGroup bossGroup = EventLoopGroupFactory.getBossGroup(url);
                EventLoopGroup workerGroup = EventLoopGroupFactory.getWorkerGroup(url);
                // if (url.getBoolean("quic")) {
                if (true) {
                    byte[] proto = new byte[] {
                            0x05, 'h', 'q', '-', '2', '9',
                            0x05, 'h', 'q', '-', '2', '8',
                            0x05, 'h', 'q', '-', '2', '7',
                            0x08, 'h', 't', 't', 'p', '/', '0', '.', '9'
                    };
                    ChannelHandler channelHandler = new QuicServerCodecBuilder()
                            .certificateChain("/Users/Silence/Projects/joyrpc/joyrpc-plugin/joyrpc-transport/joyrpc-transport-netty4/src/main/resources/cert.crt")
                            .privateKey("/Users/Silence/Projects/joyrpc/joyrpc-plugin/joyrpc-transport/joyrpc-transport-netty4/src/main/resources/cert.key")
                            .applicationProtocols(proto)
                            .maxIdleTimeout(5000, TimeUnit.MILLISECONDS)
                            // .maxUdpPayloadSize(Quic.MAX_DATAGRAM_SIZE)
                            .initialMaxData(10000000)
                            .initialMaxStreamDataBidirectionalLocal(1000000)
                            .initialMaxStreamDataBidirectionalRemote(1000000)
                            .initialMaxStreamsBidirectional(100)
                            .initialMaxStreamsUnidirectional(100)
                            .tokenHandler(InsecureQuicTokenHandler.INSTANCE)
                            // ChannelHandler that is added into QuicChannel pipeline.
                            .handler(new ChannelInboundHandlerAdapter() {
                                @Override
                                public void channelActive(ChannelHandlerContext ctx) {
                                    QuicChannel channel = (QuicChannel) ctx.channel();
                                    // Create streams etc..
                                }

                                @Override
                                public void channelInactive(ChannelHandlerContext ctx) {
                                    // log.info("Connection closed: {}",
                                    //         ((QuicChannel) ctx.channel()).collectStats().getNow());
                                }

                                @Override
                                public boolean isSharable() {
                                    return true;
                                }
                            })
                            .streamHandler(new ChannelInboundHandlerAdapter() {
                                @Override
                                public void channelRead(ChannelHandlerContext ctx, Object msg) {
                                    ByteBuf byteBuf = (ByteBuf) msg;
                                    try {
                                        if (byteBuf.toString(CharsetUtil.US_ASCII).trim().equals("GET /")) {
                                            ByteBuf buffer = ctx.alloc().directBuffer();
                                            buffer.writeCharSequence("Hello World!\r\n", CharsetUtil.US_ASCII);

                                            // Write the buffer and close the stream once the write completes.
                                            ctx.writeAndFlush(buffer).addListener(ChannelFutureListener.CLOSE);
                                        }
                                    } finally {
                                        byteBuf.release();
                                    }
                                }

                                @Override
                                public boolean isSharable() {
                                    return true;
                                }
                            })
                            .build();
                    Bootstrap bootstrap = new Bootstrap();
                    // io.netty.channel.Channel channel1 =
                            bootstrap.group(bossGroup).channel(NioDatagramChannel.class).handler(channelHandler)
                            .bind(new InetSocketAddress(port))
                                    .addListener((ChannelFutureListener) f -> {
                                NettyServerChannel channel = new NettyServerChannel(f.channel(), bossGroup, workerGroup, supplier);
                                if (f.isSuccess()) {
                                    consumer.accept(new AsyncResult<>(channel));
                                } else {
                                    //自动解绑
                                    logger.error("Error:", f.cause());
                                    Throwable error = f.cause();
                                    channel.close(o -> consumer.accept(new AsyncResult<>(
                                            new ConnectionException(
                                                    String.format("Failed binding server at %s:%d, caused by %s",
                                                            host, port, error.getMessage()), error))));

                                }
                            }
                    ).sync().channel();
                    // channel1.closeFuture().sync();
                } else {
                    ServerBootstrap bootstrap = configure(new ServerBootstrap().group(bossGroup, workerGroup), sslContext);
                    bootstrap.bind(new InetSocketAddress(host, port)).addListener((ChannelFutureListener) f -> {
                        NettyServerChannel channel = new NettyServerChannel(f.channel(), bossGroup, workerGroup, supplier);
                        if (f.isSuccess()) {
                            future.complete(channel);
                        } else {
                            //自动解绑
                            Throwable error = f.cause();
                            channel.close().whenComplete((v, e) -> future.completeExceptionally(new ConnectionException(
                                    String.format("Failed binding server at %s:%d, caused by %s",
                                            host, port, error.getMessage()), error)));
                        }
                    });
                }
            } catch (Throwable e) {
                future.completeExceptionally(e);
            }
        }
        return future;
    }

    /**
     * 配置
     *
     * @param bootstrap  启动
     * @param sslContext ssl上下文
     */
    protected ServerBootstrap configure(final ServerBootstrap bootstrap, final SslContext sslContext) {
        //io.netty.bootstrap.Bootstrap - Unknown channel option 'SO_BACKLOG' for channel
        bootstrap.channel(Constants.isUseEpoll(url) ? EpollServerSocketChannel.class : NioServerSocketChannel.class)
                .childHandler(new MyChannelInitializer(url, sslContext))
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, url.getPositiveInt(Constants.CONNECT_TIMEOUT_OPTION))
                .option(ChannelOption.SO_REUSEADDR, url.getBoolean(Constants.SO_REUSE_PORT_OPTION))
                .option(ChannelOption.SO_BACKLOG, url.getPositiveInt(Constants.SO_BACKLOG_OPTION))
                .option(ChannelOption.RCVBUF_ALLOCATOR, AdaptiveRecvByteBufAllocator.DEFAULT)
                .option(ChannelOption.WRITE_BUFFER_WATER_MARK, new WriteBufferWaterMark(url.getPositiveInt(Constants.WRITE_BUFFER_LOW_WATERMARK_OPTION),
                        url.getPositiveInt(Constants.WRITE_BUFFER_HIGH_WATERMARK_OPTION)))
                .childOption(ChannelOption.SO_RCVBUF, url.getPositiveInt(Constants.SO_RECEIVE_BUF_OPTION))
                .childOption(ChannelOption.SO_SNDBUF, url.getPositiveInt(Constants.SO_SEND_BUF_OPTION))
                .childOption(ChannelOption.SO_KEEPALIVE, url.getBoolean(Constants.SO_KEEPALIVE_OPTION))
                .childOption(ChannelOption.TCP_NODELAY, url.getBoolean(Constants.TCP_NODELAY))
                .childOption(ChannelOption.ALLOCATOR, BufAllocator.create(url));

        return bootstrap;
    }

    /**
     * 通道初始化
     */
    protected class MyChannelInitializer extends ChannelInitializer<SocketChannel> {
        /**
         * URL
         */
        protected URL url;
        /**
         * SSL上下文
         */
        protected SslContext sslContext;

        /**
         * 构造函数
         *
         * @param url
         * @param sslContext
         */
        public MyChannelInitializer(URL url, SslContext sslContext) {
            this.url = url;
            this.sslContext = sslContext;
        }

        @Override
        protected void initChannel(final SocketChannel ch) {
            //及时发送 与 缓存发送
            Channel channel = new NettyChannel(ch, true);
            //设置payload,添加业务线程池到channel
            channel.setAttribute(Channel.PAYLOAD, url.getPositiveInt(Constants.PAYLOAD)).
                    setAttribute(Channel.BIZ_THREAD_POOL, bizThreadPool, (k, v) -> v != null);
            if (sslContext != null) {
                ch.pipeline().addFirst("ssl", sslContext.newHandler(ch.alloc()));
            }
            ch.pipeline().addLast("connection", new ConnectionChannelHandler(channel, publisher) {
                @Override
                public void channelInactive(final ChannelHandlerContext ctx) throws Exception {
                    removeChannel(channel);
                    super.channelInactive(ctx);
                    logger.info(String.format("disconnect %s", ctx.channel().remoteAddress()));
                }
            });

            if (deduction != null) {
                ch.pipeline().addLast(PROTOCOL_DEDUCTION_HANDLER, new ProtocolDeductionAdapter(deduction, channel));
            } else {
                DeductionContext context = new ProtocolDeductionContext(channel, ch.pipeline());
                context.bind(codec, chain);
            }

            ChannelTransport transport = function.apply(channel, url);
            channel.setAttribute(Channel.CHANNEL_TRANSPORT, transport);
            channel.setAttribute(Channel.SERVER_CHANNEL, getServerChannel());
            addChannel(channel, transport);
        }
    }

}
