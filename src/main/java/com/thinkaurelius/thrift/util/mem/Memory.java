/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.thinkaurelius.thrift.util.mem;

import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;

import com.sun.jna.Pointer;
import sun.misc.Unsafe;

/**
 * An off-heap region of memory that must be manually free'd when no longer needed.
 */
class Memory
{
    private static final Unsafe unsafe;

    static
    {
        try
        {
            Field field = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            unsafe = (sun.misc.Unsafe) field.get(null);
        }
        catch (Exception e)
        {
            throw new AssertionError(e);
        }
    }

    protected long peer, size; // size could be changed by 'reallocate'

    protected Memory(long size)
    {
        this(unsafe.allocateMemory(size), size);
    }

    protected Memory(long peer, long size)
    {
        this.peer = peer;
        this.size = size;
    }

    public static Memory allocate(long bytes)
    {
        if (bytes < 0)
            throw new IllegalArgumentException();

        return new Memory(bytes);
    }

    public static Memory wrap(long peer, int limit)
    {
        return new Memory(peer, limit);
    }

    public void setByte(long offset, byte b)
    {
        checkPosition(offset);
        setByteUnsafe(offset, b);
    }

    void setByteUnsafe(long offset, byte b)
    {
        unsafe.putByte(peer + offset, b);
    }

    public void setMemory(long offset, long bytes, byte b)
    {
        // check if the last element will fit into the memory
        checkPosition(offset + bytes - 1);
        unsafe.setMemory(peer + offset, bytes, b);
    }

    void copyMemory(long offset, long dstAddr, long bytes)
    {
        if (bytes > size - offset)
            throw new IllegalArgumentException();

        unsafe.copyMemory(peer + offset, dstAddr, bytes);
    }

    public void setLong(long offset, long l)
    {
        checkPosition(offset);
        unsafe.putLong(peer + offset, l);
    }

    /**
     * Transfers count bytes from buffer to Memory
     *
     * @param memoryOffset start offset in the memory
     * @param buffer the data buffer
     * @param bufferOffset start offset of the buffer
     * @param count number of bytes to transfer
     */
    public void setBytes(long memoryOffset, byte[] buffer, int bufferOffset, int count)
    {
        if (buffer == null)
            throw new NullPointerException();
        else if (bufferOffset < 0
                 || count < 0
                 || bufferOffset + count > buffer.length)
            throw new IndexOutOfBoundsException();
        else if (count == 0)
            return;

        checkPosition(memoryOffset);
        long end = memoryOffset + count;
        checkPosition(end - 1);
        while (memoryOffset < end)
        {
            unsafe.putByte(peer + memoryOffset, buffer[bufferOffset]);
            memoryOffset++;
            bufferOffset++;
        }
    }

    /**
     * Transfers count bytes from buffer to Memory
     *
     * @param memoryOffset start offset in the memory
     * @param buffer the data buffer
     */
    public void setBytes(long memoryOffset, ByteBuffer buffer)
    {
        if (buffer == null)
            throw new NullPointerException();
        else if (buffer.remaining() == 0)
            return;

        int bufferOffset = buffer.position();

        checkPosition(memoryOffset);
        long end = memoryOffset + buffer.remaining();
        checkPosition(end - 1);
        while (memoryOffset < end)
        {
            unsafe.putByte(peer + memoryOffset, buffer.get(bufferOffset));
            memoryOffset++;
            bufferOffset++;
        }
    }

    public byte getByte(long offset)
    {
        checkPosition(offset);
        return getByteUnsafe(offset);
    }

    byte getByteUnsafe(long offset)
    {
        return unsafe.getByte(peer + offset);
    }

    public long getLong(long offset)
    {
        checkPosition(offset);
        return unsafe.getLong(peer + offset);
    }

    /**
     * Transfers count bytes from Memory starting at memoryOffset to buffer starting at bufferOffset
     *
     * @param memoryOffset start offset in the memory
     * @param buffer the data buffer
     * @param bufferOffset start offset of the buffer
     * @param count number of bytes to transfer
     */
    public void getBytes(long memoryOffset, byte[] buffer, int bufferOffset, int count)
    {
        if (buffer == null)
            throw new NullPointerException();
        else if (bufferOffset < 0 || count < 0 || count > buffer.length - bufferOffset)
            throw new IndexOutOfBoundsException();
        else if (count == 0)
            return;

        checkPosition(memoryOffset);
        long end = memoryOffset + count;
        checkPosition(end - 1);
        while (memoryOffset < end)
        {
            buffer[bufferOffset++] = unsafe.getByte(peer + memoryOffset);
            memoryOffset++;
        }
    }

    private void checkPosition(long offset)
    {
        assert peer != 0 : "Memory was freed";
        assert offset >= 0 && offset < size : "Illegal offset: " + offset + ", size: " + size;
    }

    public long getPeer()
    {
        return peer;
    }

    public Memory reallocate(int newSize)
    {
        peer = unsafe.reallocateMemory(peer, newSize);
        size = newSize;

        return this;
    }

    public void free()
    {
        assert peer != 0;
        unsafe.freeMemory(peer);
        peer = 0;
    }

    public long size()
    {
        return size;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;
        if (!(o instanceof Memory))
            return false;
        Memory b = (Memory) o;
        if (peer == b.peer && size == b.size)
            return true;
        return false;
    }

    public String toString()
    {
        return new String(toBytes());
    }

    public String toString(String enc) throws UnsupportedEncodingException
    {
        return new String(toBytes(), enc);
    }

    public byte[] toBytes()
    {
        return toBytes((int) size);
    }

    public byte[] toBytes(int size)
    {
        assert size <= (int) this.size;

        byte[] buf = new byte[size];

        for (int i = 0; i < size; i++)
            buf[i] = getByte(i);

        return buf;
    }

    public ByteBuffer toByteBuffer()
    {
        ByteBuffer buffer = new Pointer(peer).getByteBuffer(0, size);

        if (buffer != null)
            return buffer;

        buffer = ByteBuffer.allocate((int) size);

        for (int i = 0; i < size; i++)
            buffer.put(getByteUnsafe(i));

        buffer.flip();

        return buffer;
    }
}

