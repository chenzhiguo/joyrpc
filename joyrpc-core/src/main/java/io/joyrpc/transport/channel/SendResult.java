package io.joyrpc.transport.channel;

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

import java.net.SocketAddress;

/**
 * 发送结果
 */
public class SendResult {
    /**
     * 是否成功
     */
    private boolean success;
    /**
     * 异常
     */
    private Throwable throwable;
    /**
     * 本地地址
     */
    private SocketAddress localAddress;
    /**
     * 远端地址
     */
    private SocketAddress remoteAddress;
    /**
     * 请求
     */
    private Object request;
    /**
     * 通道
     */
    private Channel channel;

    public SendResult(boolean success, Channel channel) {
        this.success = success;
        this.localAddress = channel.getLocalAddress();
        this.remoteAddress = channel.getRemoteAddress();
        this.channel = channel;
    }

    public SendResult(boolean success, Channel channel, Object request) {
        this(success, channel);
        this.request = request;
    }

    public SendResult(Throwable throwable, Channel channel) {
        this.success = false;
        this.throwable = throwable;
        this.localAddress = channel.getLocalAddress();
        this.remoteAddress = channel.getRemoteAddress();
        this.channel = channel;
    }

    public SendResult(Throwable throwable, Channel channel, Object request) {
        this(throwable, channel);
        this.request = request;
    }

    public boolean isSuccess() {
        return success;
    }

    public Throwable getThrowable() {
        return throwable;
    }

    public SocketAddress getLocalAddress() {
        return localAddress;
    }

    public SocketAddress getRemoteAddress() {
        return remoteAddress;
    }

    public Object getRequest() {
        return request;
    }

    public Channel getChannel() {
        return channel;
    }

    @Override
    public String toString() {
        return "SendEvent{" +
                "success=" + success +
                ", throwable=" + throwable +
                '}';
    }
}
