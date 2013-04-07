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

import com.tinkerpop.thrift.test.*;
import com.tinkerpop.thrift.util.MessageFrameBuffer;
import com.tinkerpop.thrift.util.TBinaryProtocol;
import org.apache.thrift.TException;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.*;

import org.junit.AfterClass;
import org.junit.BeforeClass;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class AbstractDisruptorTest
{
    private static TServer TEST_SERVICE;

    protected static final String HOST;
    protected static final int SERVER_PORT = 9161;

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

    protected TTransport getNewTransport() throws TTransportException
    {
        return new TFramedTransport(new TSocket(HOST, SERVER_PORT));
    }

    protected TestService.Client getNewClient(TTransport transport) throws TTransportException
    {
        if (!transport.isOpen())
            transport.open();

        return new TestService.Client(new org.apache.thrift.protocol.TBinaryProtocol(transport, true, true));
    }

    protected void invokeRequests(TestService.Client client, int startId, int arg1, int arg2) throws TException
    {
        ByteBuffer rawArg1 = toByteBuffer(arg1);
        ByteBuffer rawArg2 = toByteBuffer(arg2);

        Response responseAdd = client.invoke(new Request().setId(startId)
                                                          .setArg1(rawArg1.duplicate())
                                                          .setArg2(rawArg2.duplicate())
                                                          .setArgType(ArgType.INT)
                                                          .setOperationType(OperationType.ADD));

        Response responseSub = client.invoke(new Request().setId(startId + 1)
                                                          .setArg1(rawArg1.duplicate())
                                                          .setArg2(rawArg2.duplicate())
                                                          .setArgType(ArgType.INT)
                                                          .setOperationType(OperationType.SUB));

        Response responseMul = client.invoke(new Request().setId(startId + 2)
                                                          .setArg1(rawArg1.duplicate())
                                                          .setArg2(rawArg2.duplicate())
                                                          .setArgType(ArgType.INT)
                                                          .setOperationType(OperationType.MUL));

        Response responseDiv = client.invoke(new Request().setId(startId + 3)
                                                          .setArg1(rawArg1.duplicate())
                                                          .setArg2(rawArg2.duplicate())
                                                          .setArgType(ArgType.INT)
                                                          .setOperationType(OperationType.DIV));

        int resultAdd = toInteger(ByteBuffer.wrap(responseAdd.getResult()));
        int resultSub = toInteger(ByteBuffer.wrap(responseSub.getResult()));
        int resultMul = toInteger(ByteBuffer.wrap(responseMul.getResult()));
        int resultDiv = toInteger(ByteBuffer.wrap(responseDiv.getResult()));

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
        protected void beforeInvoke(MessageFrameBuffer buffer)
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
}
