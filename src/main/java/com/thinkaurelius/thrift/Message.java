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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TNonblockingTransport;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lmax.disruptor.EventFactory;
import com.thinkaurelius.thrift.util.ThriftFactories;
import com.thinkaurelius.thrift.util.mem.Buffer;
import com.thinkaurelius.thrift.util.mem.FastMemoryOutputTransport;
import com.thinkaurelius.thrift.util.mem.TMemoryInputTransport;

/**
 * Possible states for the Message state machine.
 */
enum State
{
    READY_TO_READ_FRAME_SIZE,
    READING_FRAME_SIZE,

    READY_TO_READ_FRAME,
    READING_FRAME,
    READ_FRAME_COMPLETE,

    READY_TO_WRITE,
    WRITING,

    AWAITING_CLOSE
}

/**
 * Class that implements a sort of state machine around the interaction with a
 * client and an invoker. It manages reading the frame size and frame data,
 * getting it handed off as wrapped transports, and then the writing of
 * response data back to the client. In the process it manages flipping the
 * read and write bits on the selection key for its client.
 */
public class Message
{
    private static final Logger logger = LoggerFactory.getLogger(Message.class);

    public static class Invocation
    {
        public static final EventFactory<Invocation> FACTORY = new EventFactory<Invocation>()
        {
            @Override
            public Invocation newInstance()
            {
                return new Invocation();
            }
        };

        public Message message;

        public void setMessage(Message message)
        {
            this.message = message;
        }

        public Message getMessage()
        {
            return message;
        }

        public void execute()
        {
            message.invoke();
        }
    }

    // the actual transport hooked up to the client.
    public final TNonblockingTransport transport;
    public final ThriftFactories thriftFactories;

    // the SelectionKey that corresponds to our transport
    private final SelectionKey selectionKey;

    // where in the process of reading/writing are we?
    private State state = State.READY_TO_READ_FRAME_SIZE;

    private Buffer dataBuffer, frameSizeBuffer;

    private FastMemoryOutputTransport response;

    private final boolean useHeapBasedAllocation, alwaysReallocateBuffers;

    public Message(TNonblockingTransport trans,
                   SelectionKey key,
                   ThriftFactories factories,
                   boolean heapBasedAllocation,
                   boolean reallocateBuffers)
    {
        frameSizeBuffer = Buffer.allocate(4, heapBasedAllocation);
        transport = trans;
        selectionKey = key;
        thriftFactories = factories;
        useHeapBasedAllocation = heapBasedAllocation;
        alwaysReallocateBuffers = reallocateBuffers;
    }

    public boolean isReadyToRead()
    {
        return (state == State.READY_TO_READ_FRAME_SIZE || state == State.READY_TO_READ_FRAME) && selectionKey.isReadable();
    }

    public boolean isReadyToWrite()
    {
        return state == State.READY_TO_WRITE && selectionKey.isWritable();
    }

    /**
     * Give this Message a chance to read. The selector loop should have
     * received a read event for this Message.
     *
     * @return true if the connection should live on, false if it should be
     *         closed
     */
    public boolean read()
    {
        if (state == State.READING_FRAME_SIZE)
        {
            // try to read the frame size completely
            if (!internalRead(frameSizeBuffer))
                return false;

            // if the frame size has been read completely, then prepare to read the
            // actual frame.
            if (frameSizeBuffer.remaining() == 0)
            {
                // pull out the frame size as an integer.
                int frameSize = frameSizeBuffer.getInt(0);

                if (frameSize <= 0)
                {
                    logger.error("Read an invalid frame size of " + frameSize + ". Are you using TFramedTransport on the client side?");
                    return false;
                }

                if (frameSize > thriftFactories.maxFrameSizeInBytes)
                {
                    logger.error("Invalid frame size got (" + frameSize + "), maximum expected " + thriftFactories.maxFrameSizeInBytes);
                    return false;
                }


                // reallocate to match frame size (if needed)
                reallocateDataBuffer(frameSize);

                frameSizeBuffer.clear(); // prepare it to the next round of reading (if any)

                state = State.READING_FRAME;
            }
            else
            {
                // this skips the check of READING_FRAME state below, since we can't
                // possibly go on to that state if there's data left to be read at
                // this one.
                state = State.READY_TO_READ_FRAME_SIZE;
                return true;
            }
        }

        // it is possible to fall through from the READING_FRAME_SIZE section
        // to READING_FRAME if there's already some frame data available once
        // READING_FRAME_SIZE is complete.

        if (state == State.READING_FRAME)
        {
            if (!internalRead(dataBuffer))
                return false;

            state = (dataBuffer.remaining() == 0)
                     ? State.READ_FRAME_COMPLETE
                     : State.READY_TO_READ_FRAME;

            // Do not read until we finish processing request.
            if (state == State.READ_FRAME_COMPLETE)
            {
                switchMode(State.READ_FRAME_COMPLETE);
            }

            return true;
        }

        // if we fall through to this point, then the state must be invalid.
        logger.error("Read was called but state is invalid (" + state + ")");
        return false;
    }

    public boolean isFrameFullyRead()
    {
        return state == State.READ_FRAME_COMPLETE;
    }

    /**
     * Give this Message a chance to write its output to the final client.
     */
    public boolean write()
    {
        assert state == State.WRITING;

        boolean writeFailed = false;

        try
        {
            if (response.streamTo(transport) < 0)
            {
                writeFailed = true;
                return false;
            }
            else if (!response.isFullyStreamed())
            {
                // if socket couldn't accommodate whole write buffer,
                // continue writing when we get next write signal.

                switchToWrite();
                return true;
            }
        }
        catch (IOException e)
        {
            logger.error("Got an IOException during write!", e);
            writeFailed = true;
            return false;
        }
        finally
        {
            if (writeFailed || response.isFullyStreamed())
                response.close();
        }



        // we're done writing. Now we need to switch back to reading.
        switchToRead();

        return true;
    }

    /**
     * Give this Message a chance to change its interests.
     */
    public void changeSelectInterests()
    {
        switch (state)
        {
            case READY_TO_WRITE: // set the OP_WRITE interest
                state = State.WRITING;
                break;

            case READY_TO_READ_FRAME_SIZE:
                state = State.READING_FRAME_SIZE;
                break;

            case READY_TO_READ_FRAME:
                state = State.READING_FRAME;
                break;

            case AWAITING_CLOSE:
                close();
                selectionKey.cancel();
                break;

            default:
                logger.error("changeSelectInterest was called, but state is invalid (" + state + ")");
        }
    }

    public void responseReady()
    {
        if (response.size() == 0)
        {
            // go straight to reading again. this was probably an one way method
            switchToRead();
            // we can close response stream as it wouldn't be used and we want to reclaim resources
            response.close();

        }
        else
        {
            switchToWrite();
        }
    }

    /**
     * Actually invoke the method signified by this Message.
     */
    public void invoke()
    {
        assert state == State.READ_FRAME_COMPLETE : "Invoke called in invalid state: " + state;

        TTransport inTrans = getInputTransport();
        TProtocol inProt = thriftFactories.inputProtocolFactory.getProtocol(inTrans);
        TProtocol outProt = thriftFactories.outputProtocolFactory.getProtocol(getOutputTransport());

        try
        {
            thriftFactories.processorFactory.getProcessor(inTrans).process(inProt, outProt);
            responseReady();
            return;
        }
        catch (TException te)
        {
            logger.warn("Exception while invoking!", te);
        }
        catch (Throwable t)
        {
            logger.error("Unexpected throwable while invoking!", t);
        }

        // This will only be reached when there is a throwable.
        state = State.AWAITING_CLOSE;
        changeSelectInterests();
    }

    /**
     * Wrap the read dataBuffer in a memory-based transport so a processor can read
     * the data it needs to handle an invocation.
     */
    private TTransport getInputTransport()
    {
        return dataBuffer.getInputTransport();
    }

    /**
     * Get the transport that should be used by the invoker for responding.
     */
    private TTransport getOutputTransport()
    {
        response = new FastMemoryOutputTransport(32, useHeapBasedAllocation);
        return thriftFactories.outputTransportFactory.getTransport(response);
    }

    /**
     * Perform a read into dataBuffer.
     *
     * @return true if the read succeeded, false if there was an error or the
     *         connection closed.
     */
    private boolean internalRead(Buffer buffer)
    {
        try
        {
            return !(buffer.readFrom(transport) < 0);
        }
        catch (IOException e)
        {
            logger.warn("Got an IOException in internalRead!", e);
            return false;
        }
    }

    private void switchToRead()
    {
        switchMode(State.READY_TO_READ_FRAME_SIZE);
    }

    private void switchToWrite()
    {
        switchMode(State.READY_TO_WRITE);
    }

    private void switchMode(State newState)
    {
        state = newState;

        switch (newState)
        {
            case READY_TO_READ_FRAME_SIZE:
                selectionKey.interestOps(SelectionKey.OP_READ);
                break;

            case READY_TO_WRITE:
                selectionKey.interestOps(SelectionKey.OP_WRITE);
                break;

            case READ_FRAME_COMPLETE:
                selectionKey.interestOps(0);
                break;

            default:
                throw new IllegalArgumentException("Illegal state: " + newState);
        }

        selectionKey.selector().wakeup();
    }

    private void freeDataBuffer()
    {
        if (dataBuffer == null)
            return;

        dataBuffer.free();
        dataBuffer = null;
    }

    private void reallocateDataBuffer(int newSize)
    {
        if (shouldReallocateBuffer(newSize))
            freeDataBuffer();

        if (dataBuffer == null)
            dataBuffer = Buffer.allocate(newSize, useHeapBasedAllocation);

        dataBuffer.clear();
    }

    private boolean shouldReallocateBuffer(int newSize) {
        return alwaysReallocateBuffers || (dataBuffer != null && dataBuffer.size() != newSize);
    }

    /**
     * Shut the connection down.
     */
    public void close()
    {
        freeDataBuffer();
        frameSizeBuffer.free();
        transport.close();

        if (response != null)
            response.close();
    }

    public void cancel()
    {
        close();
        selectionKey.cancel();
    }
}