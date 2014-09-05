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

import javax.management.*;
import java.io.Closeable;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.spi.SelectorProvider;
import java.util.Iterator;
import java.util.concurrent.*;

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
        private Integer numAcceptors, numSelectors, numWorkersPerSelector, ringSize, maxFrameSizeInBytes = 16384000;
        private boolean useHeapBasedAllocation = true, alwaysReallocateBuffers = true;
        private ThreadPoolExecutor invocationExecutor;

        public Args(TNonblockingServerTransport transport)
        {
            super(transport);
        }

        @SuppressWarnings("unused")
        public Args numAcceptors(int numAcceptors)
        {
            this.numAcceptors = numAcceptors;
            return this;
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

        @SuppressWarnings("unused")
        public Args maxFrameSizeInBytes(int maxFrameSizeInBytes)
        {
            this.maxFrameSizeInBytes = maxFrameSizeInBytes;
            return this;
        }

        @SuppressWarnings("unused")
        public Args alwaysReallocateBuffers(boolean flag) {
            this.alwaysReallocateBuffers = flag;
            return this;
        }

        /**
         * Executor if set takes precedence over numWorkersPerSelector.
         *
         * @param executor The executor to use for message execution.
         *
         * @return self.
         */
        @SuppressWarnings("unused")
        public Args invocationExecutor(ThreadPoolExecutor executor) {
            this.invocationExecutor = executor;
            return this;
        }
    }

    private final AcceptorThread[] acceptorThreads;
    private final SelectorThread[] selectorThreads;
    private final SelectorLoadBalancer selectorLoadBalancer;
    private final ThreadPoolExecutor globalInvoker;

    private final ThriftFactories thriftFactories;

    private volatile boolean useHeapBasedAllocation, alwaysReallocateBuffers, isStopped;

    public TDisruptorServer(Args args)
    {
        super(args);

        final int numCores = Runtime.getRuntime().availableProcessors();

        final int numAcceptors = (args.numAcceptors == null)
                                   ? 2
                                   : args.numAcceptors;

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
        alwaysReallocateBuffers = args.alwaysReallocateBuffers;

        thriftFactories = new ThriftFactories(inputTransportFactory_, outputTransportFactory_,
                                              inputProtocolFactory_,  outputProtocolFactory_,
                                              processorFactory_,
                                              args.maxFrameSizeInBytes);

        try
        {
            acceptorThreads = new AcceptorThread[numAcceptors];

            for (int i = 0; i < numAcceptors; i++)
                acceptorThreads[i] = new AcceptorThread("Thrift-Acceptor_" + i, (TNonblockingServerTransport) serverTransport_);
        }
        catch (IOException e)
        {
            throw new RuntimeException("Could not create acceptor threads", e);
        }

        if (args.invocationExecutor != null)
            logger.info(String.format("Going to use %s for all of the Selector threads..", args.invocationExecutor));

        try
        {
            selectorThreads = new SelectorThread[numSelectors];

            globalInvoker = (args.invocationExecutor != null) ? args.invocationExecutor : null;
            int numHandlers = (globalInvoker != null)
                                ? globalInvoker.getMaximumPoolSize() / numSelectors
                                : numWorkersPerSelector;

            for (int i = 0; i < numSelectors; i++)
            {
                ThreadPoolExecutor invoker = (globalInvoker != null)
                                              ? globalInvoker
                                              : new ThreadPoolExecutor(numWorkersPerSelector, numWorkersPerSelector,
                                                                       0L, TimeUnit.MILLISECONDS,
                                                                       new LinkedBlockingQueue<Runnable>());

                selectorThreads[i] = new SelectorThread("Thrift-Selector_" + i,
                                                        nextPowerOfTwo(ringSize),
                                                        invoker,
                                                        numHandlers == 0 ? 1 : numHandlers,
                                                        (globalInvoker != null));
            }
        }
        catch (IOException e)
        {
            throw new RuntimeException("Could not create selector threads", e);
        }

        selectorLoadBalancer = new RandomSelectorLoadBalancer(selectorThreads);

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

        for (int i = 0; i < acceptorThreads.length; i++)
        {
            acceptorThreads[i].start();
            logger.debug("Thrift Acceptor thread {} is started.", i);
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

        if (globalInvoker != null)
            globalInvoker.shutdown();
    }

    @Override
    public void stop()
    {
        isStopped = true;

        for (AcceptorThread acceptor : acceptorThreads)
            acceptor.wakeupSelector();

        for (SelectorThread selector : selectorThreads)
            selector.wakeupSelector();

        unregisterMBean();
    }

    void unregisterMBean()
    {
        try
        {
            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            ObjectName nameObj = new ObjectName(MBEAN_NAME);

            if (mbs.isRegistered(nameObj))
                mbs.unregisterMBean(nameObj);
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
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

    protected abstract class AbstractSelectorThread extends Thread implements Closeable
    {
        protected final Selector selector = SelectorProvider.provider().openSelector();

        public AbstractSelectorThread(String name) throws IOException
        {
            super(name);

        }

        public boolean isStopped()
        {
            return isStopped;
        }

        @Override
        public void run()
        {
            try
            {
                while (!isStopped())
                    select();
                
                for (SelectionKey key: selector.keys())
                    cleanupSelectionKey(key);

            }
            catch (Throwable t)
            {
                logger.error("run() exiting due to uncaught error", t);
            }
            finally
            {
                isStopped = true;

                close();
            }
        }

        private void select()
        {
            try
            {
                // wait for io events.
                selector.select();

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

                    processKey(key);
                }

                selectorIterationComplete();
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

        /**
         * Process each individual key that reported ready state
         *
         * @param key The key to process
         *
         * @throws IOException any I/O related error
         */
        protected abstract void processKey(SelectionKey key) throws IOException;

        /**
         * Called after each selector event loop completion, useful for booking (e.g. registering new connections)
         * @throws IOException any I/O related error
         */
        protected abstract void selectorIterationComplete() throws IOException;

        protected void cleanupSelectionKey(SelectionKey key)
        {
            // remove the records from the two maps
            Message message = (Message) key.attachment();

            if (message != null)
            {
                beforeClose(message);
                message.close();
            }

            // cancel the selection key
            key.cancel();
        }

        public void wakeupSelector()
        {
            selector.wakeup();
        }

        public void close()
        {
            try
            {
                selector.close();
            }
            catch (IOException e)
            {
                logger.warn("Failed to close selector: {}.", getName(), e);
            }
        }
    }

    protected class AcceptorThread extends AbstractSelectorThread
    {
        private final TNonblockingServerTransport serverTransport;

        /**
         * Set up the thread that will handle the non-blocking accepts.
         */
        public AcceptorThread(String name, TNonblockingServerTransport serverTransport) throws IOException {
            super(name);
            this.serverTransport = serverTransport;
            this.serverTransport.registerSelector(selector); // registers with OP_ACCEPT interests
        }

        @Override
        protected void processKey(SelectionKey key) throws IOException
        {
            if (!key.isAcceptable())
                return;

            try
            {
                // accept the connection
                SelectorThread selector = selectorLoadBalancer.nextSelector();
                selector.subscribe((TNonblockingTransport) serverTransport.accept());
                selector.wakeupSelector();
            }
            catch (TTransportException tte)
            {
                // accept() shouldn't be NULL if fine because are are raising for a socket
                logger.debug("Non-fatal exception trying to accept!", tte);
            }
        }

        @Override
        protected void selectorIterationComplete() {
            // no post-processing is required
        }
    }

    protected class SelectorThread extends AbstractSelectorThread
    {
        private final ThreadPoolExecutor invoker;
        private final boolean isSharedInvoker;

        private final RingBuffer<Message.Invocation> ringBuffer;
        private final WorkerPool<Message.Invocation> workerPool;
        private final ConcurrentLinkedQueue<TNonblockingTransport> newConnections;

        /**
         * Set up the thread that will handle the non-blocking reads, and writes.
         */
        public SelectorThread(String name, int ringSize, ThreadPoolExecutor invoker, int numHandlers, boolean isSharedInvoker) throws IOException
        {
            super(name);

            this.newConnections = new ConcurrentLinkedQueue<>();

            InvocationHandler handlers[] = new InvocationHandler[numHandlers];

            for (int i = 0; i < handlers.length; i++)
                handlers[i] = new InvocationHandler();

            this.invoker = invoker;
            this.isSharedInvoker = isSharedInvoker;

            /**
             * YieldingWaitStrategy claims to be better compromise between throughput/latency and CPU usage comparing to
             * BlockingWaitStrategy, but actual tests show quite the opposite, where YieldingWaitStrategy just constantly
             * burns CPU cycles with no performance benefit when coupled with networking.
             */
            ringBuffer = RingBuffer.createSingleProducer(Message.Invocation.FACTORY, ringSize, new BlockingWaitStrategy());
            workerPool = new WorkerPool<>(ringBuffer, ringBuffer.newBarrier(), new FatalExceptionHandler(), handlers);
            workerPool.start(invoker);
        }

        @Override
        protected void processKey(SelectionKey key)
        {
            Message message = (Message) key.attachment();

            if (message.isReadyToRead())
                handleRead(message);
            else if (message.isReadyToWrite())
                handleWrite(message);
        }

        @Override
        protected void selectorIterationComplete() throws IOException
        {
            TNonblockingTransport newClient;

            while ((newClient = newConnections.poll()) != null)
            {
                SelectionKey clientKey = newClient.registerSelector(selector, SelectionKey.OP_READ);
                clientKey.attach(new Message(newClient, clientKey, thriftFactories, useHeapBasedAllocation, alwaysReallocateBuffers));
            }
        }

        protected void handleRead(Message message)
        {
            message.changeSelectInterests();

            if (!message.read())
                cancelMessage(message);
            else if (message.isFrameFullyRead())
                dispatchInvoke(message);
        }

        protected void handleWrite(Message message)
        {
            message.changeSelectInterests();

            if (!message.write())
                cancelMessage(message);
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
                cancelMessage(message);
            }
        }

        private void cancelMessage(Message message)
        {
            beforeClose(message);
            message.cancel();
        }

        public void subscribe(TNonblockingTransport newClient)
        {
            newConnections.add(newClient); // always returns true as queue is unbounded
        }

        public void shutdown()
        {
            workerPool.drainAndHalt();

            if (!isSharedInvoker)
                invoker.shutdown();
        }

        public int getRingBufferSize()
        {
            return ringBuffer.getBufferSize();
        }
    }

    /**
     * Allows derived classes to react when a connection is closed.
     */
    protected void beforeClose(@SuppressWarnings("unused") Message buffer)
    {
        // NOP by default
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

    /* Helper classes */

    public class InvocationHandler implements WorkHandler<Message.Invocation>
    {
        @Override
        public void onEvent(Message.Invocation invocation) throws Exception
        {
            beforeInvoke(invocation.getMessage());
            invocation.execute();
        }
    }

    public static interface SelectorLoadBalancer
    {
        public SelectorThread nextSelector();
    }

    public static class RandomSelectorLoadBalancer implements SelectorLoadBalancer
    {
        private final SelectorThread[] selectors;

        public RandomSelectorLoadBalancer(SelectorThread[] selectorThreads)
        {
            this.selectors = selectorThreads;
        }

        @Override
        public SelectorThread nextSelector()
        {
            return selectors[ThreadLocalRandom.current().nextInt(selectors.length)];
        }
    }
}