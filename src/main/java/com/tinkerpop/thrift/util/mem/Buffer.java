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
package com.tinkerpop.thrift.util.mem;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.apache.thrift.transport.TNonblockingTransport;
import org.apache.thrift.transport.TTransport;

public abstract class Buffer
{
    public static Buffer allocate(int size, boolean onHeapAllocation)
    {
        return (onHeapAllocation) ? new OnHeapBuffer(size) : new OffHeapBuffer(size);
    }

    protected ByteBuffer buffer;

    protected void setBuffer(ByteBuffer newBuffer)
    {
        buffer = newBuffer;
    }

    protected void setBuffer(ByteBuffer newBuffer, ByteOrder order)
    {
        setBuffer(newBuffer);

        if (buffer != null)
            buffer.order(order);
    }

    public void clear()
    {
        buffer.clear();
    }

    public int getInt(int atIndex)
    {
        return buffer.getInt(atIndex);
    }

    public byte get(int index)
    {
        return buffer.get(index);
    }

    public void put(int index, byte value)
    {
        buffer.put(index, value);
    }

    /**
     * Buffers should only be accessed by absolute positions,
     * this method is for convenience when buffer is passed to TTransport implementation
     *
     * @return The size of empty space in the buffer
     */
    public int remaining()
    {
        return buffer.remaining();
    }

    public int size()
    {
        return buffer.capacity();
    }

    public abstract void reallocate(int newSize);
    public abstract void free();

    public TTransport getInputTransport()
    {
        return new TMemoryInputTransport(buffer);
    }

    public int readFrom(TNonblockingTransport transport) throws IOException
    {
        return transport.read(buffer);
    }

    public int writeTo(TNonblockingTransport transport, int start, int count) throws IOException
    {
        ByteBuffer dup = buffer.duplicate();
        dup.position(start).limit(start + count);

        return transport.write(dup);
    }

    private static class OffHeapBuffer extends Buffer
    {
        private final Memory rawMemory;

        public OffHeapBuffer(int size)
        {
            rawMemory = Memory.allocate(size);
            setBuffer(rawMemory, ByteOrder.BIG_ENDIAN);
        }

        @Override
        public void reallocate(int newSize)
        {
            setBuffer(rawMemory.reallocate(newSize), ByteOrder.BIG_ENDIAN);
        }

        @Override
        public void free()
        {
            if (rawMemory.getPeer() != 0)
                rawMemory.free();

            setBuffer(null);
        }

        private void setBuffer(Memory memory, ByteOrder order)
        {
            setBuffer(memory.toByteBuffer(), order);
        }
    }

    private static class OnHeapBuffer extends Buffer
    {
        public OnHeapBuffer(int size)
        {
            setBuffer(ByteBuffer.allocate(size));
        }

        @Override
        public void reallocate(int newSize)
        {
            assert buffer.hasArray();

            if (newSize < buffer.capacity())
            {
                buffer.clear().limit(newSize);
                return;
            }

            if (buffer.capacity() == newSize)
                return;

            ByteBuffer extended = ByteBuffer.allocate(newSize);
            System.arraycopy(buffer.array(), 0, extended.array(), 0, buffer.capacity());

            setBuffer(extended);
        }

        @Override
        public void free()
        {
            setBuffer(null); // we can't deallocate buffer itself, GC should do that for us
        }
    }
}
