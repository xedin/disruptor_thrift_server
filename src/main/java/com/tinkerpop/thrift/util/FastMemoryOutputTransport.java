/**
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.tinkerpop.thrift.util;

import java.nio.ByteBuffer;

import com.sun.jna.Pointer;
import com.tinkerpop.thrift.util.Memory;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

public class FastMemoryOutputTransport extends TTransport
{
    /**
     * The byte array containing the bytes written.
     */
    protected Memory buf;

    /**
     * The number of bytes written.
     */
    protected int count;

    /**
     * Constructs a new {@code FastMemoryOutputTransport} with a default size of 32 bytes.
     * If more than 32 bytes are written to this instance, the underlying byte buffer will be expanded.
     */
    public FastMemoryOutputTransport()
    {
        this(32);
    }

    @Override
    public boolean isOpen()
    {
        return true;
    }

    @Override
    public void open() throws TTransportException
    {}

    /**
     * Constructs a new {@code FastMemoryOutputTransport} with a default size of
     * {@code size} bytes. If more than {@code size} bytes are written to this
     * instance, the underlying byte buffer will expanded.
     *
     * @param size initial size for the underlying byte buffer, must be non-negative.
     *
     * @throws IllegalArgumentException if {@code size} < 0.
     */
    public FastMemoryOutputTransport(int size)
    {
        if (size <= 0)
            throw new IllegalArgumentException();

        buf = Memory.allocate(size);
    }

    /**
     * Closes this transport. This releases system resources used for this stream.
     */
    @Override
    public void close()
    {
        if (buf.getPeer() == 0) // paranoia
            return;

        buf.free();
    }

    @Override
    public int read(byte[] buf, int off, int len) throws TTransportException
    {
        throw new UnsupportedOperationException();
    }

    private void expand(int i)
    {
        /* Can the buffer handle @i more bytes, if not expand it */
        if (count + i <= buf.size())
            return;

        buf.reallocate((count + i) * 2);
    }

    /**
     * Returns the total number of bytes written to this transport so far.
     *
     * @return the number of bytes written to this stream.
     */
    public int size()
    {
        return count;
    }

    public ByteBuffer toByteBuffer()
    {
        return new Pointer(buf.peer).getByteBuffer(0, count);
    }

    /**
     * Returns the contents of this ByteArrayOutputStream as a string. Any
     * changes made to the receiver after returning will not be reflected in the
     * string returned to the caller.
     *
     * @return this stream's current contents as a string.
     */

    @Override
    public String toString()
    {
        return buf.toString();
    }

    /**
     * Writes {@code count} bytes from the byte buffer {@code buffer} starting at
     * offset {@code index} to this stream.
     *
     * @param buffer
     *            the buffer to be written.
     * @param offset
     *            the initial position in {@code buffer} to retrieve bytes.
     * @param len
     *            the number of bytes of {@code buffer} to write.
     * @throws NullPointerException
     *             if {@code buffer} is {@code null}.
     * @throws IndexOutOfBoundsException
     *             if {@code offset < 0} or {@code len < 0}, or if
     *             {@code offset + len} is greater than the length of
     *             {@code buffer}.
     */
    @Override
    public void write(byte[] buffer, int offset, int len)
    {
        // avoid int overflow
        if (offset < 0 || offset > buffer.length || len < 0 || len > buffer.length - offset)
            throw new IndexOutOfBoundsException();

        if (len == 0)
            return;

        /* Expand if necessary */
        expand(len);

        for (int i = count, j = offset; j < offset + len; i++, j++)
            buf.setByteUnsafe(i, buffer[j]);

        count += len;
    }
}
