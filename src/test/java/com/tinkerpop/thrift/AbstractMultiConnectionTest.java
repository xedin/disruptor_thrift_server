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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import com.tinkerpop.thrift.test.*;
import org.apache.thrift.transport.TTransport;

import org.junit.Test;

public abstract class AbstractMultiConnectionTest extends AbstractDisruptorTest
{
    private static final int CONNECTIONS = 250;

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

                    TestService.Client client = getNewClient(transport);

                    invokeRequests(client, id, getRandomArgument(), getRandomArgument());
                    client.ping();

                    latch.countDown();

                    return null;
                }
            });
        }

        latch.await();

        service.shutdown();

        for (int i = 0; i < CONNECTIONS; i++)
            transports.get(i).close();
    }

    @Test
    public void producerConsumerTest() throws Exception
    {
        final CountDownLatch latch = new CountDownLatch(CONNECTIONS);
        final SynchronousQueue<Work> queue = new SynchronousQueue<>();
        final ExecutorService service = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

        new Thread()
        {
            public void run()
            {
                int count = 0;
                while (count < CONNECTIONS)
                {
                    Work newWork = queue.poll();

                    if (newWork == null)
                        continue;

                    service.submit(newWork);
                    count++;
                }
            }
        }.start();

        for (int i = 0; i < CONNECTIONS; i++)
            queue.put(new Work(latch, i, getRandomArgument(), getRandomArgument(), OperationType.ADD));

        latch.await();
        service.shutdown();
    }
}
