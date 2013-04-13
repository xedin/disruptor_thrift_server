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

import java.io.IOException;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.SelectionKey;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lmax.disruptor.*;
import com.tinkerpop.thrift.util.TBinaryProtocol;
import com.tinkerpop.thrift.util.ThriftFactories;
import org.apache.thrift.server.AbstractNonblockingServer;
import org.apache.thrift.server.TNonblockingServer;
import org.apache.thrift.transport.TNonblockingServerTransport;
import org.apache.thrift.transport.TNonblockingTransport;
import org.apache.thrift.transport.TTransportException;

public abstract class TDisruptorServer extends TNonblockingServer
{
    private static final Logger logger = LoggerFactory.getLogger(TDisruptorServer.class);

    public static class Args extends AbstractNonblockingServer.AbstractNonblockingServerArgs<Args>
    {
        private Integer numSelectors, numWorkers, ringSize;
        private ExecutorService invoker;

        public Args(TNonblockingServerTransport transport)
        {
            super(transport);
        }

        @SuppressWarnings("unused")
        public Args numSelectors(int numSelectors)
        {
            this.numSelectors = numSelectors;
            return this;
        }

        @SuppressWarnings("unused")
        public Args numWorkers(int numWorkers)
        {
            this.numWorkers = numWorkers;
            return this;
        }

        @SuppressWarnings("unused")
        public Args ringSize(int ringSize)
        {
            this.ringSize = ringSize;
            return this;
        }

        @SuppressWarnings("unused")
        public Args invokerService(ExecutorService service)
        {
            this.invoker = service;
            return this;
        }
    }

    private final SelectorThread[] selectorThreads;
    private final ExecutorService invoker;

    private final ThriftFactories thriftFactories;
    private volatile boolean stopped;

    public TDisruptorServer(Args args)
    {
        super(args);

        final int numCores = Runtime.getRuntime().availableProcessors();

        // by default, setting selectors to 1/4 of available CPUs (or 1 if CPU count is less than or equal to 4)
        // because we just want to split the iterative work (accept, put-to-ring)
        final int numSelectors = (args.numSelectors == null)
                                   ? (numCores < 4) ? 1 : numCores / 4
                                   : args.numSelectors;

        // by default, setting number of workers to match number of available CPUs
        final int numWorkers = (args.numWorkers == null)
                                   ? numCores
                                   : args.numWorkers;

        // by default, setting size of the ring buffer so each worker has 1024 slots available,
        // rounded to the nearest power 2 (as requirement of Disruptor).
        final int ringSize = (args.ringSize == null)
                                   ? nearestPowerOf2(1024 * numCores)
                                   : args.ringSize;

        // unfortunate fact that Thrift transports still rely on byte arrays forces us to do this :(
        if (!(inputProtocolFactory_ instanceof TBinaryProtocol.Factory) || !(outputProtocolFactory_ instanceof TBinaryProtocol.Factory))
            throw new IllegalArgumentException("Please use " + TBinaryProtocol.Factory.class.getCanonicalName() + " or it's subclass as protocol factories.");

        invoker = (args.invoker == null)
                    ? Executors.newFixedThreadPool(numCores)
                    : args.invoker;

        /**
         * YieldingWaitStrategy claims to be better compromise between throughput/latency and CPU usage comparing to
         * BlockingWaitStrategy, but actual tests show quite the opposite, where YieldingWaitStrategy just constantly
         * burns CPU cycles with no performance benefit when coupled with networking.
         */
        RingBuffer<Message.Event> ringBuffer = RingBuffer.createMultiProducer(Message.Event.FACTORY, ringSize, new BlockingWaitStrategy());

        thriftFactories = new ThriftFactories(inputTransportFactory_, outputTransportFactory_,
                                              inputProtocolFactory_,  outputProtocolFactory_,
                                              processorFactory_);

        MessageHandler handlers[] = new MessageHandler[numWorkers];

        for (int i = 0; i < numWorkers; i++)
            handlers[i] = new DisruptorMessageHandler();

        WorkerPool<Message.Event> workers = new WorkerPool<>(ringBuffer,
                                                            ringBuffer.newBarrier(),
                                                            new FatalExceptionHandler(),
                                                            handlers);

        workers.start(Executors.newFixedThreadPool(numWorkers));

        try
        {
            selectorThreads = new SelectorThread[numSelectors];

            for (int i = 0; i < numSelectors; i++)
            {
                selectorThreads[i] = new SelectorThread("Thrift-Selector_" + i,
                                                        ringBuffer,
                                                        (TNonblockingServerTransport) serverTransport_);
            }
        }
        catch (IOException e)
        {
            logger.error("Could not create selector thread: {}", e.getMessage());
            throw new AssertionError(e);
        }
    }

    @Override
    protected boolean startThreads()
    {
        stopped = false;
        for (int i = 0; i < selectorThreads.length; i++)
        {
            selectorThreads[i].start(); // start the selector
            logger.debug("Thrift Selector thread {} is started.", i);
        }
        return true;
    }

    @Override
    protected void waitForShutdown()
    {
        joinSelector();
        gracefullyShutdownInvokerPool();
    }

    @Override
    protected void joinSelector()
    {
        try
        {
            for (int i = 0; i < selectorThreads.length; i++)
                selectorThreads[i].join(); // wait until the selector thread exits
        }
        catch (InterruptedException e)
        {
            logger.error("Interruption: " + e.getMessage());
            e.printStackTrace();
        }
    }

    protected void gracefullyShutdownInvokerPool()
    {
        // try to gracefully shut down the executor service
        invoker.shutdown();

        // Loop until awaitTermination finally does return without a interrupted
        // exception. If we don't do this, then we'll shut down prematurely. We want
        // to let the executorService clear it's task queue, closing client sockets
        // appropriately.
        long timeoutMS = 150; // milliseconds
        long now = System.currentTimeMillis();

        while (timeoutMS >= 0)
        {
            try
            {
                invoker.awaitTermination(timeoutMS, TimeUnit.MILLISECONDS);
                break;
            }
            catch (InterruptedException ix)
            {
                long newNow = System.currentTimeMillis();
                timeoutMS -= (newNow - now);
                now = newNow;
            }
        }
    }

    @Override
    public void stop()
    {
        stopped = true;

        for (int i = 0; i < selectorThreads.length; i++)
            selectorThreads[i].wakeupSelector();
    }

    @Override
    public boolean isStopped()
    {
        for (int i = 0; i < selectorThreads.length; i++)
        {
            if (!selectorThreads[i].isStopped())
                return false;
        }

        return true;
    }

    @Override
    protected boolean requestInvoke(FrameBuffer frameBuffer)
    {
        throw new UnsupportedOperationException();
    }

    /**
     * All implementations should use this method to provide custom behaviour in pre-invoke stage.
     *
     * @param message The "message" or buffer associated with request in pre-invoke state.
     */
    protected abstract void beforeInvoke(Message message);

    protected void dispatchInvoke(final Message message)
    {
        invoker.submit(new Runnable()
        {
            @Override
            public void run()
            {
                beforeInvoke(message);
                message.invoke();
            }
        });
    }

    protected class SelectorThread extends SelectAcceptThread
    {
        private final RingBuffer<Message.Event> ringBuffer;

        /**
         * Set up the thread that will handle the non-blocking accepts, reads, and
         * writes.
         */
        public SelectorThread(String name,
                              RingBuffer<Message.Event> ringBuffer,
                              TNonblockingServerTransport serverTransport) throws IOException
        {
            super(serverTransport);
            setName(name);
            serverTransport.registerSelector(selector);

            this.ringBuffer = ringBuffer;
        }

        @Override
        public void run()
        {
            try
            {
                while (!isStopped())
                    select();

                selector.close();
            }
            catch (Throwable t)
            {
                logger.error("run() exiting due to uncaught error", t);
            }
            finally
            {
                stopped = true;
            }
        }

        @Override
        public boolean isStopped()
        {
            return stopped;
        }

        private void select()
        {
            try
            {
                // wait for io events.
                if (selector.select() == 0)
                    return;

                // process the io events we received
                Iterator<SelectionKey> selectedKeys = selector.selectedKeys().iterator();

                while (!isStopped() && selectedKeys.hasNext())
                {
                    SelectionKey key = selectedKeys.next();
                    selectedKeys.remove();

                    // skip if not valid
                    if (!key.isValid())
                    {
                        cleanupSelectionKey(key);
                        continue;
                    }

                    // if the key is marked Accept, then it has to be the server transport.
                    if (key.isAcceptable())
                    {
                        handleAccept();
                        continue;
                    }

                    Message message = (Message) key.attachment();

                    if (isReadable(key, message) || isWritable(key, message))
                    {
                        message.changeSelectInterests();
                        dispatch(key);
                    }
                }
            }
            catch (IOException e)
            {
                logger.warn("Got an IOException while selecting: {}!", e);
            }
            catch (CancelledKeyException e)
            {
                logger.debug("Non-fatal exception in select loop (probably somebody closed the channel)...", e);
            }
        }

        private boolean isReadable(SelectionKey key, Message message)
        {
            return message.isReadyToRead() && key.isReadable();
        }

        private boolean isWritable(SelectionKey key, Message message)
        {
            return message.isReadyToWrite() && key.isWritable();
        }

        private void handleAccept() throws IOException
        {
            final SelectionKey clientKey;
            final TNonblockingTransport client;

            try
            {
                // accept the connection
                client = (TNonblockingTransport) serverTransport_.accept();
                clientKey = client.registerSelector(selector, SelectionKey.OP_READ);
                clientKey.attach(new Message(client, clientKey, thriftFactories));
            }
            catch (TTransportException tte)
            {
                // accept() shouldn't be NULL if fine because are are raising for a socket
                logger.debug("Non-fatal exception trying to accept!", tte);
            }
        }

        private void dispatch(final SelectionKey key)
        {
            ringBuffer.publishEvent(new EventTranslator<Message.Event>()
            {
                @Override
                public void translateTo(Message.Event event, long sequence)
                {
                    event.setKey(key);
                }
            });
        }

        @Override
        protected void cleanupSelectionKey(SelectionKey key)
        {
            // remove the records from the two maps
            Message message = (Message) key.attachment();

            if (message != null)
                message.close();

            // cancel the selection key
            key.cancel();
        }

        @Override
        protected void handleRead(SelectionKey key)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        protected void handleWrite(SelectionKey key)
        {
            throw new UnsupportedOperationException();
        }
    }

    private class DisruptorMessageHandler extends MessageHandler
    {
        @Override
        protected void handleInvoke(Message message)
        {
            dispatchInvoke(message);
        }
    }

    private static int nearestPowerOf2(int v)
    {
        v--;
        v |= v >> 1;
        v |= v >> 2;
        v |= v >> 4;
        v |= v >> 8;
        v |= v >> 16;
        v++;

        return v;
    }
}