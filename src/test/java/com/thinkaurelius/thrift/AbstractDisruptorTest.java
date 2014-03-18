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

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;

import com.thinkaurelius.thrift.test.*;
import com.thinkaurelius.thrift.util.TBinaryProtocol;
import org.apache.thrift.TException;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.*;

import org.junit.AfterClass;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public abstract class AbstractDisruptorTest
{
    private static TServer TEST_SERVICE;
    private static final Random RANDOM = new Random();

    protected static final String HOST;

    static
    {
        try
        {
            HOST = InetAddress.getLocalHost().getHostName();
        }
        catch (UnknownHostException e)
        {
            throw new AssertionError(e);
        }
    }

    public static void prepareTest(boolean onHeapBuffers, int port) throws Exception
    {
        prepareTest(onHeapBuffers, false, port);
    }

    public static void prepareTest(boolean onHeapBuffers, boolean shouldRellocateBuffers, int port) throws Exception
    {
        final TNonblockingServerTransport socket = new TNonblockingServerSocket(new InetSocketAddress(HOST, port));
        final TBinaryProtocol.Factory protocol = new TBinaryProtocol.Factory();

        TDisruptorServer.Args args = new TDisruptorServer.Args(socket)
                                                         .inputTransportFactory(new TFramedTransport.Factory())
                                                         .outputTransportFactory(new TFramedTransport.Factory())
                                                         .inputProtocolFactory(protocol)
                                                         .outputProtocolFactory(protocol)
                                                         .processor(new TestService.Processor<TestService.Iface>(new Service()))
                                                         .useHeapBasedAllocation(onHeapBuffers)
                                                         .alwaysReallocateBuffers(shouldRellocateBuffers);

        TEST_SERVICE = new CustomTDisruptorServer(args);

        new Thread()
        {
            public void run()
            {
                TEST_SERVICE.serve();
            }
        }.start();
    }

    @AfterClass
    public static void shutdownTest()
    {
        TEST_SERVICE.stop();
    }

    protected TTransport getNewTransport() throws TTransportException
    {
        return new TFramedTransport(new TSocket(HOST, getServerPort()));
    }

    public abstract int getServerPort();

    protected TestService.Client getNewClient(TTransport transport) throws TTransportException
    {
        if (!transport.isOpen())
            transport.open();

        return new TestService.Client(new org.apache.thrift.protocol.TBinaryProtocol(transport, true, true));
    }

    protected void invokeRequests(TestService.Client client, int startId, int arg1, int arg2) throws TException
    {
        Response responseAdd = client.invoke(getRequest(startId + 0, arg1, arg2, OperationType.ADD));
        Response responseSub = client.invoke(getRequest(startId + 1, arg1, arg2, OperationType.SUB));
        Response responseMul = client.invoke(getRequest(startId + 2, arg1, arg2, OperationType.MUL));
        Response responseDiv = client.invoke(getRequest(startId + 3, arg1, arg2, OperationType.DIV));

        int resultAdd = toInteger(responseAdd.bufferForResult());
        int resultSub = toInteger(responseSub.bufferForResult());
        int resultMul = toInteger(responseMul.bufferForResult());
        int resultDiv = toInteger(responseDiv.bufferForResult());

        assertEquals(responseAdd.getId(), startId);
        assertEquals(responseSub.getId(), startId + 1);
        assertEquals(responseMul.getId(), startId + 2);
        assertEquals(responseDiv.getId(), startId + 3);

        assertEquals(arg1 + arg2, resultAdd);
        assertEquals(arg1 - arg2, resultSub);
        assertEquals(arg1 * arg2, resultMul);
        assertEquals(arg1 / arg2, resultDiv);

        assertEquals(ArgType.INT, responseAdd.getResType());
        assertEquals(ArgType.INT, responseSub.getResType());
        assertEquals(ArgType.INT, responseMul.getResType());
        assertEquals(ArgType.INT, responseDiv.getResType());

        Response responseEmpty = client.invoke(new Request().setId(startId + 4)
                                                            .setArg1(ByteBuffer.allocate(0))
                                                            .setArg2(ByteBuffer.allocate(0))
                                                            .setArgType(ArgType.LONG)
                                                            .setOperationType(OperationType.DIV));

        assertNull(responseEmpty.getResult());
        assertNull(responseEmpty.getResType());
    }

    protected static class CustomTDisruptorServer extends TDisruptorServer
    {
        public CustomTDisruptorServer(Args args)
        {
            super(args);
        }

        @Override
        protected void beforeInvoke(Message message)
        {}
    }

    protected static class Service implements TestService.Iface
    {
        @Override
        public Response invoke(Request req) throws TException
        {
            if (req.getArgType() == ArgType.INT)
            {
                int arg1 = toInteger(req.arg1);
                int arg2 = toInteger(req.arg2);

                int result = -1;

                switch (req.getOperationType())
                {
                    case ADD:
                        result = arg1 + arg2;
                        break;

                    case SUB:
                        result = arg1 - arg2;
                        break;

                    case MUL:
                        result = arg1 * arg2;
                        break;

                    case DIV:
                        result = arg1 / arg2;
                        break;
                }

                return new Response().setId(req.id)
                                     .setResType(ArgType.INT)
                                     .setResult(toByteBuffer(result));
            }

            return new Response();
        }

        @Override
        public void ping() throws TException
        {}
    }

    protected class Work implements Callable<Request>
    {
        private final CountDownLatch latch;
        private final int id, arg1, arg2;
        private final OperationType op;

        public Work(CountDownLatch latch, int id, int arg1, int arg2, OperationType op)
        {
            this.latch = latch;
            this.id = id;
            this.arg1 = arg1;
            this.arg2 = arg2;
            this.op = op;
        }

        @Override
        public Request call() throws Exception
        {
            TTransport transport = getNewTransport();

            try
            {
                TestService.Client client = getNewClient(transport);

                Response res = client.invoke(getRequest(id, arg1, arg2, op));


                switch (op)
                {
                    case ADD:
                        assertEquals(id, res.getId());
                        assertEquals(ArgType.INT, res.getResType());
                        assertEquals(arg1 + arg2, toInteger(res.bufferForResult()));
                        break;

                    default:
                        throw new IllegalStateException();
                }
            }
            finally
            {
                transport.close();
            }

            latch.countDown();
            return null;
        }
    }

    protected static ByteBuffer toByteBuffer(int integer)
    {
        ByteBuffer b = ByteBuffer.allocate(4).putInt(integer);
        b.clear();

        return b;
    }

    protected static int toInteger(ByteBuffer buffer)
    {
        return buffer.getInt();
    }

    protected static int getRandomArgument()
    {
        int n = RANDOM.nextInt(50000);
        return n == 0 ? 1 : n;
    }

    private static Request getRequest(int id, int arg1, int arg2, OperationType op)
    {
        return new Request().setId(id)
                            .setArg1(toByteBuffer(arg1))
                            .setArg2(toByteBuffer(arg2))
                            .setArgType(ArgType.INT)
                            .setOperationType(op);
    }
}
