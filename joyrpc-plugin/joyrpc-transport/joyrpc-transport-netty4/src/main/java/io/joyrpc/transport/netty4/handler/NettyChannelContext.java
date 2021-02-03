package io.joyrpc.transport.netty4.handler;

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

import io.joyrpc.transport.channel.Channel;
import io.joyrpc.transport.channel.ChannelContext;

/**
 * Netty连接通道上下文
 */
public class NettyChannelContext implements ChannelContext {
    /**
     * 连接通道
     */
    protected final Channel channel;
    /**
     * 结束标识
     */
    protected boolean end;

    public NettyChannelContext(final Channel channel) {
        this.channel = channel;
    }

    @Override
    public Channel getChannel() {
        return channel;
    }

    @Override
    public void end() {
        end = true;
    }

    @Override
    public boolean isEnd() {
        return end;
    }
}
