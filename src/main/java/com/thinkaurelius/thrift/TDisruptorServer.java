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

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.SelectionKey;
import java.util.Iterator;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lmax.disruptor.*;
import com.thinkaurelius.thrift.util.TBinaryProtocol;
import com.thinkaurelius.thrift.util.ThriftFactories;
import org.apache.thrift.server.AbstractNonblockingServer;
import org.apache.thrift.server.TNonblockingServer;
import org.apache.thrift.transport.TNonblockingServerTransport;
import org.apache.thrift.transport.TNonblockingTransport;
import org.apache.thrift.transport.TTransportException;

public abstract class TDisruptorServer extends TNonblockingServer implements TDisruptorServerMBean
{
    private static final Logger logger = LoggerFactory.getLogger(TDisruptorServer.class);

    private static final boolean isJNAPresent;

    static
    {
        boolean jna = false;

        try
        {
            new com.sun.jna.Pointer(0);
            jna = true;
        }
        catch (NoClassDefFoundError e)
        {
            // can't do anything about it, going to use on-heap buffers instead
        }

        isJNAPresent = jna;
    }

    public static final String MBEAN_NAME = "com.thinkaurelius.thrift.server:type=TDisruptorServer";

    public static class Args extends AbstractNonblockingServer.AbstractNonblockingServerArgs<Args>
    {
        private Integer numSelectors, numWorkersPerSelector, ringSize;
        private boolean useHeapBasedAllocation;

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
        public Args numWorkersPerSelector(int numWorkers)
        {
            this.numWorkersPerSelector = numWorkers;
            return this;
        }

        @SuppressWarnings("unused")
        public Args ringSizePerSelector(int ringSize)
        {
            this.ringSize = ringSize;
            return this;
        }

        @SuppressWarnings("unused")
        public Args useHeapBasedAllocation(boolean flag)
        {
            this.useHeapBasedAllocation = flag;
            return this;
        }
    }

    private final SelectorThread[] selectorThreads;
    private final ThriftFactories thriftFactories;

    private volatile boolean useHeapBasedAllocation, isStopped;

    public TDisruptorServer(Args args)
    {
        super(args);

        final int numCores = Runtime.getRuntime().availableProcessors();

        final int numSelectors = (args.numSelectors == null)
                                   ? numCores
                                   : args.numSelectors;

        // by default, setting number of workers to 2 as we have selector per core
        final int numWorkersPerSelector = (args.numWorkersPerSelector == null)
                                           ? 2
                                           : args.numWorkersPerSelector;

        // by default, setting size of the ring buffer so each worker has 2048 slots available,
        // rounded to the nearest power 2 (as requirement of Disruptor).
        final int ringSize = (args.ringSize == null)
                                   ? 2048
                                   : args.ringSize;

        // unfortunate fact that Thrift transports still rely on byte arrays forces us to do this :(
        if (!(inputProtocolFactory_ instanceof TBinaryProtocol.Factory) || !(outputProtocolFactory_ instanceof TBinaryProtocol.Factory))
            throw new IllegalArgumentException("Please use " + TBinaryProtocol.Factory.class.getCanonicalName() + " or it's subclass as protocol factories.");

        // there is no need to force people to have JNA in classpath,
        // let's just warn them that it's not there and we are switching to on-heap allocation.
        if (!args.useHeapBasedAllocation && !isJNAPresent)
        {
            logger.warn("Off-heap allocation couldn't be used as JNA is not present in classpath or broken, using on-heap instead.");
            args.useHeapBasedAllocation = true;
        }

        useHeapBasedAllocation = args.useHeapBasedAllocation;

        thriftFactories = new ThriftFactories(inputTransportFactory_, outputTransportFactory_,
                                              inputProtocolFactory_,  outputProtocolFactory_,
                                              processorFactory_);

        try
        {
            selectorThreads = new SelectorThread[numSelectors];

            for (int i = 0; i < numSelectors; i++)
            {
                selectorThreads[i] = new SelectorThread("Thrift-Selector_" + i,
                                                        (TNonblockingServerTransport) serverTransport_,
                                                        nextPowerOfTwo(ringSize),
                                                        numWorkersPerSelector);
            }
        }
        catch (IOException e)
        {
            logger.error("Could not create selector thread: {}", e.getMessage());
            throw new AssertionError(e);
        }

        /* Register JXM listener */

        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();

        try
        {
            mbs.registerMBean(this, new ObjectName(MBEAN_NAME));
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected boolean startThreads()
    {
        isStopped = false;
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
            for (SelectorThread selector : selectorThreads)
                selector.join(); // wait until the selector thread exits
        }
        catch (InterruptedException e)
        {
            logger.error("Interruption: " + e.getMessage());
            e.printStackTrace();
        }
    }

    protected void gracefullyShutdownInvokerPool()
    {
        for (SelectorThread selector : selectorThreads)
            selector.shutdown();
    }

    @Override
    public void stop()
    {
        isStopped = true;

        for (SelectorThread selector : selectorThreads)
            selector.wakeupSelector();
    }

    @Override
    public boolean isStopped()
    {
        for (SelectorThread selector : selectorThreads)
        {
            if (!selector.isStopped())
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

    protected class SelectorThread extends SelectAcceptThread
    {
        private final RingBuffer<Message.Invocation> ringBuffer;
        private final WorkerPool<Message.Invocation> workerPool;
        /**
         * Set up the thread that will handle the non-blocking accepts, reads, and
         * writes.
         */
        public SelectorThread(String name,
                              TNonblockingServerTransport serverTransport,
                              int ringSize,
                              int numWorkers) throws IOException
        {
            super(serverTransport);

            setName(name);

            InvocationHandler handlers[] = new InvocationHandler[numWorkers];

            for (int i = 0; i < handlers.length; i++)
                handlers[i] = new InvocationHandler();

            /**
             * YieldingWaitStrategy claims to be better compromise between throughput/latency and CPU usage comparing to
             * BlockingWaitStrategy, but actual tests show quite the opposite, where YieldingWaitStrategy just constantly
             * burns CPU cycles with no performance benefit when coupled with networking.
             */
            ringBuffer = RingBuffer.createSingleProducer(Message.Invocation.FACTORY, ringSize, new BlockingWaitStrategy());
            workerPool = new WorkerPool<>(ringBuffer, ringBuffer.newBarrier(), new FatalExceptionHandler(), handlers);
            workerPool.start((numWorkers == 1)
                              ? Executors.newSingleThreadExecutor()
                              : Executors.newFixedThreadPool(numWorkers));
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
                isStopped = true;
            }
        }

        @Override
        public boolean isStopped()
        {
            return isStopped;
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

                    if (message.isReadyToRead())
                        handleRead(message);
                    else if (message.isReadyToWrite())
                        handleWrite(message);
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

        private void handleAccept() throws IOException
        {
            try
            {
                // accept the connection
                TNonblockingTransport client = (TNonblockingTransport) serverTransport_.accept();
                SelectionKey clientKey = client.registerSelector(selector, SelectionKey.OP_READ);
                clientKey.attach(new Message(client, clientKey, thriftFactories, useHeapBasedAllocation));
            }
            catch (TTransportException tte)
            {
                // accept() shouldn't be NULL if fine because are are raising for a socket
                logger.debug("Non-fatal exception trying to accept!", tte);
            }
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

        protected void handleRead(Message message)
        {
            message.changeSelectInterests();

            if (!message.read())
                message.cancel();
            else if (message.isFrameFullyRead())
                dispatchInvoke(message);
        }

        protected void handleWrite(Message message)
        {
            message.changeSelectInterests();

            if (!message.write())
                message.cancel();
        }

        protected void dispatchInvoke(final Message message)
        {
            boolean success = ringBuffer.tryPublishEvent(new EventTranslator<Message.Invocation>()
            {
                @Override
                public void translateTo(Message.Invocation invocation, long sequence)
                {
                    invocation.setMessage(message);
                }
            });

            if (!success)
            {  // looks like we are overloaded, let's cancel this request (connection) and drop warn in the log
                logger.warn(this + " ring buffer is full, dropping client message.");
                message.cancel();
            }
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

        public void shutdown()
        {
            workerPool.drainAndHalt();
        }

        public int getRingBufferSize()
        {
            return ringBuffer.getBufferSize();
        }
    }

    private static int nextPowerOfTwo(int v)
    {
        return 1 << (32 - Integer.numberOfLeadingZeros(v - 1));
    }

    /* JMX section */

    @Override
    public int getRingBufferSize()
    {
        return selectorThreads[0].getRingBufferSize();
    }

    @Override
    public int getNumberOfSelectors()
    {
        return selectorThreads.length;
    }

    @Override
    public boolean isHeapBasedAllocationUsed()
    {
        return useHeapBasedAllocation;
    }

    @Override
    public void useHeapBasedAllocation(boolean flag)
    {
        if (!flag && !isJNAPresent)
            throw new IllegalArgumentException("Off-Heap allocation method could not be used because JNA is missing.");

        useHeapBasedAllocation = flag;
    }

    public class InvocationHandler implements WorkHandler<Message.Invocation>
    {
        @Override
        public void onEvent(Message.Invocation invocation) throws Exception
        {
            beforeInvoke(invocation.getMessage());
            invocation.execute();
        }
    }
}