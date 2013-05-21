/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.thinkaurelius.thrift;

import java.nio.channels.SelectionKey;

import com.lmax.disruptor.WorkHandler;

public abstract class MessageHandler implements WorkHandler<Message.Event>
{
    @Override
    public void onEvent(Message.Event event) throws Exception
    {
        SelectionKey key = event.getKey();

        assert key != null;

        Message message = (Message) key.attachment();

        if (!key.isValid())
        {
            cleanup(key);
            return;
        }

        int interest = key.interestOps();

        if (interest == SelectionKey.OP_READ)
            handleRead(key, message);
        else if (interest == SelectionKey.OP_WRITE)
            handleWrite(key, message);
    }

    protected void handleRead(SelectionKey key, Message message)
    {
        if (!message.read())
        {
            cleanup(key, message);
            return;
        }

        // if the buffer's frame read is complete, invoke the method.
        if (message.isFrameFullyRead())
            handleInvoke(message);
    }

    protected void handleWrite(SelectionKey key, Message message)
    {
        if (!message.write())
            cleanup(key, message);
    }

    /**
     * Invocation handler, called after message has been fully read.
     * Could be implemented using ExecutorService or queue, example.
     *
     * @param message The client connection in invoke ready state.
     */
    protected abstract void handleInvoke(Message message);

    protected void cleanup(SelectionKey key)
    {
        cleanup(key, (Message) key.attachment());
    }

    protected void cleanup(SelectionKey key, Message message)
    {
        if (message != null)
            message.close();

        // cancel the selection key (aka close connection)
        key.cancel();
    }
}
