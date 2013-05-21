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
package com.thinkaurelius.thrift.util.mem;

import java.nio.ByteBuffer;

import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

public final class TMemoryInputTransport extends TTransport
{
    private ByteBuffer buffer;
    private int position, limit;

    public TMemoryInputTransport(ByteBuffer buf)
    {
        buffer = buf;
        position = 0;
        limit = buffer.capacity();
    }

    @Override
    public void close()
    {
        // Message takes care of freeing buffer space once connection is closed
    }

    @Override
    public boolean isOpen()
    {
        return true;
    }

    @Override
    public void open() throws TTransportException
    {}

    @Override
    public int read(byte[] buf, int off, int len) throws TTransportException
    {
        int bytesRemaining = getBytesRemainingInBuffer();
        int amtToRead = (len > bytesRemaining ? bytesRemaining : len);
        if (amtToRead > 0) {
            for (int i = 0; i < amtToRead; i++)
            {
                buf[off] = buffer.get(position + i);
                off++;
            }

            consumeBuffer(amtToRead);
        }

        return amtToRead;
    }

    @Override
    public void write(byte[] buf, int off, int len) throws TTransportException
    {
        throw new UnsupportedOperationException("No writing allowed!");
    }

    @Override
    public byte[] getBuffer()
    {
        throw new UnsupportedOperationException();
    }

    public int getBufferPosition()
    {
        return position;
    }

    public int getBytesRemainingInBuffer()
    {
        return limit - position;
    }

    public void consumeBuffer(int len)
    {
        position += len;
    }

    public int read()
    {
        int temp = ((int) buffer.get(position)) & 0xff;
        consumeBuffer(1);
        return temp;
    }

    public boolean readBoolean()
    {
        int temp = this.read();
        if (temp < 0) {
            throw new IllegalStateException();
        }
        return temp != 0;
    }

    public byte readByte()
    {
        int temp = this.read();
        if (temp < 0) {
            throw new IllegalStateException();
        }
        return (byte) temp;
    }

    public short readShort()
    {
        int ch1 = this.read();
        int ch2 = this.read();
        if ((ch1 | ch2) < 0)
            throw new IllegalStateException();
        return (short)((ch1 << 8) + (ch2 << 0));
    }

    public int readInt()
    {
        int ch1 = this.read();
        int ch2 = this.read();
        int ch3 = this.read();
        int ch4 = this.read();
        if ((ch1 | ch2 | ch3 | ch4) < 0)
            throw new IllegalStateException();
        return ((ch1 << 24) + (ch2 << 16) + (ch3 << 8) + (ch4 << 0));
    }

    public long readLong()
    {
        return ((long)(readInt()) << 32) + (readInt() & 0xFFFFFFFFL);
    }

    public double readDouble()
    {
        return Double.longBitsToDouble(readLong());
    }

    public ByteBuffer readBytes(int size) throws TException
    {
        if (size > getBytesRemainingInBuffer())
            throw new TException("Requested " + size + ", available only " + buffer.remaining());

        ByteBuffer dup = buffer.duplicate();
        dup.position(position).limit(position + size);

        consumeBuffer(size);

        return dup.slice();
    }

    public void readFully(byte[] buffer) throws TException
    {
        readFully(buffer, 0, buffer.length);
    }

    public void readFully(byte[] buf, int offset, int count) throws TException
    {
        if (buffer == null) {
            throw new NullPointerException();
        }

        // avoid int overflow
        if (offset < 0 || offset > buf.length || count < 0 || count > buf.length - offset)
            throw new IndexOutOfBoundsException();

        while (count > 0)
        {
            int result = read(buf, offset, count);
            if (result < 0)
                throw new IllegalStateException();

            offset += result;
            count -= result;
        }
    }
}
