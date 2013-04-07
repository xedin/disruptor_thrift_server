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
package com.tinkerpop.thrift;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import com.tinkerpop.thrift.test.*;
import com.tinkerpop.thrift.util.MessageFrameBuffer;
import com.tinkerpop.thrift.util.TBinaryProtocol;
import org.apache.thrift.TException;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.*;
import org.junit.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class MultiConnectionTest
{
    private volatile static TServer TEST_SERVICE;

    private static final String HOST;
    private static final int SERVER_PORT = 9161, CONNECTIONS = 250;

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

    @BeforeClass
    public static void prepareTest() throws Exception
    {
        final TNonblockingServerTransport socket = new TNonblockingServerSocket(new InetSocketAddress(HOST, SERVER_PORT));
        final TBinaryProtocol.Factory protocol = new TBinaryProtocol.Factory();

        TDisruptorServer.Args args = new TDisruptorServer.Args(socket)
                                               .inputTransportFactory(new TFramedTransport.Factory())
                                               .outputTransportFactory(new TFramedTransport.Factory())
                                               .inputProtocolFactory(protocol)
                                               .outputProtocolFactory(protocol)
                                               .processor(new TestService.Processor<TestService.Iface>(new Service()));

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

    @Test
    public void multipleConnectionsTest() throws Exception
    {
        ExecutorService service = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

        List<TTransport> transports = new ArrayList<>();

        final AtomicInteger ids = new AtomicInteger(1);
        final CountDownLatch latch = new CountDownLatch(CONNECTIONS);

        for (int i = 0; i < CONNECTIONS; i++)
        {
            final TTransport transport = getNewTransport();

            transports.add(transport);

            service.submit(new Callable<Object>()
            {
                @Override
                public Object call() throws Exception
                {
                    int id = ids.incrementAndGet();

                    ByteBuffer num = toByteBuffer(id);

                    TestService.Client client = getNewClient(transport);

                    Response responseAdd = client.invoke(new Request().setId(ids.incrementAndGet())
                                                                      .setArg1(num.duplicate())
                                                                      .setArg2(num.duplicate())
                                                                      .setArgType(ArgType.INT)
                                                                      .setOperationType(OperationType.ADD));

                    Response responseSub = client.invoke(new Request().setId(ids.incrementAndGet())
                                                                      .setArg1(num.duplicate())
                                                                      .setArg2(num.duplicate())
                                                                      .setArgType(ArgType.INT)
                                                                      .setOperationType(OperationType.SUB));

                    Response responseMul = client.invoke(new Request().setId(ids.incrementAndGet())
                                                                      .setArg1(num.duplicate())
                                                                      .setArg2(num.duplicate())
                                                                      .setArgType(ArgType.INT)
                                                                      .setOperationType(OperationType.MUL));

                    Response responseDiv = client.invoke(new Request().setId(ids.incrementAndGet())
                                                                      .setArg1(num.duplicate())
                                                                      .setArg2(num.duplicate())
                                                                      .setArgType(ArgType.INT)
                                                                      .setOperationType(OperationType.DIV));

                    int resultAdd = toInteger(ByteBuffer.wrap(responseAdd.getResult()));
                    int resultSub = toInteger(ByteBuffer.wrap(responseSub.getResult()));
                    int resultMul = toInteger(ByteBuffer.wrap(responseMul.getResult()));
                    int resultDiv = toInteger(ByteBuffer.wrap(responseDiv.getResult()));

                    assertEquals(id + id, resultAdd);
                    assertEquals(id - id, resultSub);
                    assertEquals(id * id, resultMul);
                    assertEquals(id / id, resultDiv);

                    assertEquals(ArgType.INT, responseAdd.getResType());
                    assertEquals(ArgType.INT, responseSub.getResType());
                    assertEquals(ArgType.INT, responseMul.getResType());
                    assertEquals(ArgType.INT, responseDiv.getResType());

                    Response responseEmpty = client.invoke(new Request().setId(ids.incrementAndGet())
                                                                        .setArg1(ByteBuffer.allocate(0))
                                                                        .setArg2(ByteBuffer.allocate(0))
                                                                        .setArgType(ArgType.LONG)
                                                                        .setOperationType(OperationType.DIV));

                    assertNull(responseEmpty.getResult());
                    assertNull(responseEmpty.getResType());

                    client.ping();

                    latch.countDown();

                    return null;
                }
            });
        }

        latch.await();

        for (int i = 0; i < CONNECTIONS; i++)
            transports.get(i).close();
    }

    private TTransport getNewTransport() throws TTransportException
    {
        return new TFramedTransport(new TSocket(HOST, SERVER_PORT));
    }

    private TestService.Client getNewClient(TTransport transport) throws TTransportException
    {
        if (!transport.isOpen())
            transport.open();

        return new TestService.Client(new org.apache.thrift.protocol.TBinaryProtocol(transport, true, true));
    }

    private static class CustomTDisruptorServer extends TDisruptorServer
    {
        public CustomTDisruptorServer(Args args)
        {
            super(args);
        }

        @Override
        protected void beforeInvoke(MessageFrameBuffer buffer)
        {}
    }

    private static class Service implements TestService.Iface
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

                return new Response().setResType(ArgType.INT).setResult(toByteBuffer(result));
            }

            return new Response();
        }

        @Override
        public void ping() throws TException
        {}
    }

    private static ByteBuffer toByteBuffer(int integer)
    {
        ByteBuffer b = ByteBuffer.allocate(4).putInt(integer);
        b.clear();

        return b;
    }

    private static int toInteger(ByteBuffer buffer)
    {
        return buffer.getInt();
    }
}
