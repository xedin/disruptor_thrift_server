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

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.thinkaurelius.thrift.test.TestService;
import org.apache.thrift.transport.TTransport;

import org.junit.Test;

public abstract class AbstractMultiRequestTest extends AbstractDisruptorTest
{
    private static final int REQUESTS = 500;

    @Test
    public void multiRequestTest() throws Exception
    {
        TTransport transport = getNewTransport();

        try
        {
            TestService.Client client = getNewClient(transport);

            for (int i = 0; i < REQUESTS; i += 4)
                invokeRequests(client, i, getRandomArgument(), getRandomArgument());
        }
        finally
        {
            transport.close();
        }
    }

    @Test
    public void concurrentMultiRequestTest() throws Exception
    {
        final ExecutorService service = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

        TTransport transport = getNewTransport();

        try
        {
            final TestService.Client client = getNewClient(transport);
            final AtomicInteger id = new AtomicInteger(0);

            final Lock lock = new ReentrantLock();
            final CountDownLatch latch = new CountDownLatch(REQUESTS);

            for (int i = 0; i < REQUESTS; i++)
            {
                service.submit(new Callable<Object>()
                {
                    @Override
                    public Object call() throws Exception
                    {
                        lock.lock();

                        try
                        {
                            invokeRequests(client, id.incrementAndGet(), getRandomArgument(), getRandomArgument());
                        }
                        finally
                        {
                            lock.unlock();
                        }

                        latch.countDown();
                        return null;
                    }
                });
            }

            latch.await();
        }
        finally
        {
            service.shutdown();
            transport.close();
        }
    }
}
