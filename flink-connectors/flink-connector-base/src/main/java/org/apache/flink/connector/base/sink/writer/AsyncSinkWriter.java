/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.base.sink.writer;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.operators.ProcessingTimeService;
import org.apache.flink.api.connector.sink2.StatefulSinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.connector.base.sink.writer.config.AsyncSinkWriterConfiguration;
import org.apache.flink.connector.base.sink.writer.strategy.BasicRequestInfo;
import org.apache.flink.connector.base.sink.writer.strategy.BasicResultInfo;
import org.apache.flink.connector.base.sink.writer.strategy.RateLimitingStrategy;
import org.apache.flink.connector.base.sink.writer.strategy.RequestInfo;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.ListIterator;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * A generic sink writer that handles the general behaviour of a sink such as batching and flushing,
 * and allows extenders to implement the logic for persisting individual request elements, with
 * allowance for retries.
 *
 * <p>At least once semantics is supported through {@code prepareCommit} as outstanding requests are
 * flushed or completed prior to checkpointing.
 *
 * <p>Designed to be returned at {@code createWriter} time by an {@code AsyncSinkBase}.
 *
 * <p>There are configuration options to customize the buffer size etc.
 */
@PublicEvolving
public abstract class AsyncSinkWriter<InputT, RequestEntryT extends Serializable>
        implements StatefulSinkWriter<InputT, BufferedRequestState<RequestEntryT>> {

    private final MailboxExecutor mailboxExecutor;
    private final ProcessingTimeService timeService;

    /* The timestamp of the previous batch of records was sent from this sink. */
    private long lastSendTimestamp = 0;

    /* The timestamp of the response to the previous request from this sink. */
    private long ackTime = Long.MAX_VALUE;

    /* The sink writer metric group. */
    private final SinkWriterMetricGroup metrics;

    /* Counter for number of bytes this sink has attempted to send to the destination. */
    private final Counter numBytesOutCounter;

    /* Counter for number of records this sink has attempted to send to the destination. */
    private final Counter numRecordsOutCounter;

    private final RateLimitingStrategy rateLimitingStrategy;

    private final int maxBatchSize;
    private final int maxBufferedRequests;

    /**
     * Threshold in bytes to trigger a flush from the buffer.
     *
     * <p>This is derived from {@code maxBatchSizeInBytes} in the configuration, but is only used
     * here to decide when the buffer should be flushed. The actual batch size limit is now enforced
     * by the {@link BatchCreator}.
     */
    private final long flushThresholdBytes;

    private final long maxTimeInBufferMS;
    private final long maxRecordSizeInBytes;

    private final long requestTimeoutMS;
    private final boolean failOnTimeout;

    /**
     * The ElementConverter provides a mapping between for the elements of a stream to request
     * entries that can be sent to the destination.
     *
     * <p>The resulting request entry is buffered by the AsyncSinkWriter and sent to the destination
     * when the {@code submitRequestEntries} method is invoked.
     */
    private final ElementConverter<InputT, RequestEntryT> elementConverter;

    /**
     * Buffer to hold request entries that should be persisted into the destination, along with its
     * size in bytes.
     *
     * <p>A request entry contain all relevant details to make a call to the destination. Eg, for
     * Kinesis Data Streams a request entry contains the payload and partition key.
     *
     * <p>It seems more natural to buffer InputT, ie, the events that should be persisted, rather
     * than RequestEntryT. However, in practice, the response of a failed request call can make it
     * very hard, if not impossible, to reconstruct the original event. It is much easier, to just
     * construct a new (retry) request entry from the response and add that back to the queue for
     * later retry.
     */
    private final RequestBuffer<RequestEntryT> bufferedRequestEntries;

    /**
     * Batch component responsible for forming a batch of request entries from the buffer when the
     * sink is ready to flush. This determines the logic of including entries in a batch from the
     * buffered requests.
     */
    private final BatchCreator<RequestEntryT> batchCreator;

    /**
     * Tracks all pending async calls that have been executed since the last checkpoint. Calls that
     * completed (successfully or unsuccessfully) are automatically decrementing the counter. Any
     * request entry that was not successfully persisted needs to be handled and retried by the
     * logic in {@code submitRequestsToApi}.
     *
     * <p>To complete a checkpoint, we need to make sure that no requests are in flight, as they may
     * fail, which could then lead to data loss.
     */
    private int inFlightRequestsCount;

    private boolean existsActiveTimerCallback = false;

    /**
     * The {@code accept} method should be called on this Consumer if the processing of the {@code
     * requestEntries} raises an exception that should not be retried. Specifically, any action that
     * we are sure will result in the same exception no matter how many times we retry should raise
     * a {@code RuntimeException} here. For example, wrong user credentials. However, it is possible
     * intermittent failures will recover, e.g. flaky network connections, in which case, some other
     * mechanism may be more appropriate.
     */
    private final Consumer<Exception> fatalExceptionCons;

    /**
     * This method specifies how to persist buffered request entries into the destination. It is
     * implemented when support for a new destination is added.
     *
     * <p>The method is invoked with a set of request entries according to the buffering hints (and
     * the valid limits of the destination). The logic then needs to create and execute the request
     * asynchronously against the destination (ideally by batching together multiple request entries
     * to increase efficiency). The logic also needs to identify individual request entries that
     * were not persisted successfully and resubmit them using the {@code requestToRetry} callback.
     *
     * <p>From a threading perspective, the mailbox thread will call this method and initiate the
     * asynchronous request to persist the {@code requestEntries}. NOTE: The client must support
     * asynchronous requests and the method called to persist the records must asynchronously
     * execute and return a future with the results of that request. A thread from the destination
     * client thread pool should complete the request and trigger the {@code resultHandler} to
     * complete the processing of the request entries. The {@code resultHandler} actions will run on
     * the mailbox thread.
     *
     * <p>An example implementation of this method is included:
     *
     * <pre>{@code
     * @Override
     * protected void submitRequestEntries
     *   (List<RequestEntryT> records, ResultHandler<RequestEntryT> resultHandler) {
     *     Future<Response> response = destinationClient.putRecords(records);
     *     response.whenComplete(
     *         (response, error) -> {
     *             if(error != null && isFatal(error)){
     *                  resultHandler.completeExceptionally(error);
     *             }else if(error != null){
     *                 List<RequestEntryT> retryableFailedRecords = getRetryableFailed(response);
     *                 resultHandler.retryForEntries(retryableFailedRecords);
     *             }else{
     *                 resultHandler.complete();
     *             }
     *         }
     *     );
     * }
     *
     * }</pre>
     *
     * <p>During checkpointing, the sink needs to ensure that there are no outstanding in-flight
     * requests.
     *
     * @param requestEntries a set of request entries that should be sent to the destination
     * @param resultHandler the {@code complete} method should be called on this ResultHandler once
     *     the processing of the {@code requestEntries} are complete. Any entries that encountered
     *     difficulties in persisting should be re-queued through {@code retryForEntries} by
     *     including that element in the collection of {@code RequestEntryT}s passed to the {@code
     *     retryForEntries} method. All other elements are assumed to have been successfully
     *     persisted. In case of encountering fatal exceptions, the {@code completeExceptionally}
     *     method should be called.
     */
    protected void submitRequestEntries(
            List<RequestEntryT> requestEntries, ResultHandler<RequestEntryT> resultHandler) {
        throw new UnsupportedOperationException("Please override the method.");
    }

    /**
     * This method allows the getting of the size of a {@code RequestEntryT} in bytes. The size in
     * this case is measured as the total bytes that is written to the destination as a result of
     * persisting this particular {@code RequestEntryT} rather than the serialized length (which may
     * be the same).
     *
     * @param requestEntry the requestEntry for which we want to know the size
     * @return the size of the requestEntry, as defined previously
     */
    protected abstract long getSizeInBytes(RequestEntryT requestEntry);

    /**
     * This constructor is deprecated. Users should use {@link #AsyncSinkWriter(ElementConverter,
     * WriterInitContext, AsyncSinkWriterConfiguration, Collection, BatchCreator, RequestBuffer)}.
     */
    @Deprecated
    public AsyncSinkWriter(
            ElementConverter<InputT, RequestEntryT> elementConverter,
            WriterInitContext context,
            AsyncSinkWriterConfiguration configuration,
            Collection<BufferedRequestState<RequestEntryT>> states) {
        this(
                elementConverter,
                context,
                configuration,
                states,
                new SimpleBatchCreator<>(configuration.getMaxBatchSizeInBytes()),
                new DequeRequestBuffer<>());
    }

    public AsyncSinkWriter(
            ElementConverter<InputT, RequestEntryT> elementConverter,
            WriterInitContext context,
            AsyncSinkWriterConfiguration configuration,
            Collection<BufferedRequestState<RequestEntryT>> states,
            BatchCreator<RequestEntryT> batchCreator,
            RequestBuffer<RequestEntryT> bufferedRequestEntries) {
        this.elementConverter = elementConverter;
        this.mailboxExecutor = context.getMailboxExecutor();
        this.timeService = context.getProcessingTimeService();

        Preconditions.checkNotNull(elementConverter);
        Preconditions.checkArgument(configuration.getMaxBatchSize() > 0);
        Preconditions.checkArgument(configuration.getMaxBufferedRequests() > 0);
        Preconditions.checkArgument(configuration.getMaxBatchSizeInBytes() > 0);
        Preconditions.checkArgument(configuration.getMaxTimeInBufferMS() > 0);
        Preconditions.checkArgument(configuration.getMaxRecordSizeInBytes() > 0);
        Preconditions.checkArgument(
                configuration.getMaxBufferedRequests() > configuration.getMaxBatchSize(),
                "The maximum number of requests that may be buffered should be strictly"
                        + " greater than the maximum number of requests per batch.");
        Preconditions.checkArgument(
                configuration.getMaxBatchSizeInBytes() >= configuration.getMaxRecordSizeInBytes(),
                "The maximum allowed size in bytes per flush must be greater than or equal to the"
                        + " maximum allowed size in bytes of a single record.");
        Preconditions.checkNotNull(configuration.getRateLimitingStrategy());
        Preconditions.checkNotNull(
                batchCreator, "batchCreator must not be null; required for creating batches.");
        Preconditions.checkNotNull(
                bufferedRequestEntries,
                "bufferedRequestEntries must not be null; holds pending request data.");
        this.maxBatchSize = configuration.getMaxBatchSize();
        this.maxBufferedRequests = configuration.getMaxBufferedRequests();
        this.flushThresholdBytes = configuration.getMaxBatchSizeInBytes();
        this.maxTimeInBufferMS = configuration.getMaxTimeInBufferMS();
        this.maxRecordSizeInBytes = configuration.getMaxRecordSizeInBytes();
        this.rateLimitingStrategy = configuration.getRateLimitingStrategy();
        this.requestTimeoutMS = configuration.getRequestTimeoutMS();
        this.failOnTimeout = configuration.isFailOnTimeout();
        this.inFlightRequestsCount = 0;
        this.metrics = context.metricGroup();
        this.metrics.setCurrentSendTimeGauge(() -> this.ackTime - this.lastSendTimestamp);
        this.numBytesOutCounter = this.metrics.getIOMetricGroup().getNumBytesOutCounter();
        this.numRecordsOutCounter = this.metrics.getIOMetricGroup().getNumRecordsOutCounter();
        this.batchCreator = batchCreator;
        this.bufferedRequestEntries = bufferedRequestEntries;
        this.fatalExceptionCons =
                exception ->
                        mailboxExecutor.execute(
                                () -> {
                                    throw exception;
                                },
                                "A fatal exception occurred in the sink that cannot be recovered from or should not be retried.");

        elementConverter.open(context);
        initializeState(states);
    }

    private void registerCallback() {
        ProcessingTimeService.ProcessingTimeCallback ptc =
                instant -> {
                    existsActiveTimerCallback = false;
                    while (!bufferedRequestEntries.isEmpty()) {
                        flush();
                    }
                };
        timeService.registerTimer(timeService.getCurrentProcessingTime() + maxTimeInBufferMS, ptc);
        existsActiveTimerCallback = true;
    }

    @Override
    public void write(InputT element, Context context) throws IOException, InterruptedException {
        while (bufferedRequestEntries.size() >= maxBufferedRequests) {
            flush();
        }

        addEntryToBuffer(elementConverter.apply(element, context), false);

        nonBlockingFlush();
    }

    /**
     * Determines if a call to flush will be non-blocking (i.e. {@code inFlightRequestsCount} is
     * strictly smaller than {@code maxInFlightRequests}). Also requires one of the following
     * requirements to be met:
     *
     * <ul>
     *   <li>The number of elements buffered is greater than or equal to the {@code maxBatchSize}
     *   <li>The sum of the size in bytes of all records in the buffer is greater than or equal to
     *       {@code maxBatchSizeInBytes}
     * </ul>
     */
    private void nonBlockingFlush() throws InterruptedException {
        while (!rateLimitingStrategy.shouldBlock(createRequestInfo())
                && (bufferedRequestEntries.size() >= getNextBatchSizeLimit()
                        || bufferedRequestEntries.totalSizeInBytes() >= flushThresholdBytes)) {
            flush();
        }
    }

    private BasicRequestInfo createRequestInfo() {
        int batchSize = getNextBatchSize();
        return new BasicRequestInfo(batchSize);
    }

    /**
     * Persists buffered RequestsEntries into the destination by invoking {@code
     * submitRequestEntries} with batches according to the user specified buffering hints.
     *
     * <p>The method checks with the {@code rateLimitingStrategy} to see if it should block the
     * request.
     */
    private void flush() throws InterruptedException {
        RequestInfo requestInfo = createRequestInfo();
        while (rateLimitingStrategy.shouldBlock(requestInfo)) {
            mailboxExecutor.yield();
            requestInfo = createRequestInfo();
        }

        Batch<RequestEntryT> batchCreationResult =
                batchCreator.createNextBatch(requestInfo, bufferedRequestEntries);
        List<RequestEntryT> batch = batchCreationResult.getBatchEntries();
        numBytesOutCounter.inc(batchCreationResult.getSizeInBytes());
        numRecordsOutCounter.inc(batchCreationResult.getRecordCount());

        if (batch.isEmpty()) {
            return;
        }

        long requestTimestamp = System.currentTimeMillis();

        rateLimitingStrategy.registerInFlightRequest(requestInfo);
        inFlightRequestsCount++;
        submitRequestEntries(
                batch, new AsyncSinkWriterResultHandler(requestTimestamp, batch, requestInfo));
    }

    private int getNextBatchSize() {
        return Math.min(getNextBatchSizeLimit(), bufferedRequestEntries.size());
    }

    /**
     * Marks an in-flight request as completed and prepends failed requestEntries back to the
     * internal requestEntry buffer for later retry.
     *
     * @param failedRequestEntries requestEntries that need to be retried
     */
    private void completeRequest(
            List<RequestEntryT> failedRequestEntries, int batchSize, long requestStartTime)
            throws InterruptedException {
        lastSendTimestamp = requestStartTime;
        ackTime = System.currentTimeMillis();

        inFlightRequestsCount--;
        rateLimitingStrategy.registerCompletedRequest(
                new BasicResultInfo(failedRequestEntries.size(), batchSize));

        ListIterator<RequestEntryT> iterator =
                failedRequestEntries.listIterator(failedRequestEntries.size());
        while (iterator.hasPrevious()) {
            addEntryToBuffer(iterator.previous(), true);
        }
        nonBlockingFlush();
    }

    private void addEntryToBuffer(RequestEntryT entry, boolean insertAtHead) {
        addEntryToBuffer(new RequestEntryWrapper<>(entry, getSizeInBytes(entry)), insertAtHead);
    }

    private void addEntryToBuffer(RequestEntryWrapper<RequestEntryT> entry, boolean insertAtHead) {
        if (bufferedRequestEntries.isEmpty() && !existsActiveTimerCallback) {
            registerCallback();
        }

        if (entry.getSize() > maxRecordSizeInBytes) {
            throw new IllegalArgumentException(
                    String.format(
                            "The request entry sent to the buffer was of size [%s], when the maxRecordSizeInBytes was set to [%s].",
                            entry.getSize(), maxRecordSizeInBytes));
        }

        bufferedRequestEntries.add(entry, insertAtHead);
    }

    /**
     * In flight requests will be retried if the sink is still healthy. But if in-flight requests
     * fail after a checkpoint has been triggered and Flink needs to recover from the checkpoint,
     * the (failed) in-flight requests are gone and cannot be retried. Hence, there cannot be any
     * outstanding in-flight requests when a commit is initialized.
     *
     * <p>To this end, all in-flight requests need to completed before proceeding with the commit.
     */
    @Override
    public void flush(boolean flush) throws InterruptedException {
        while (inFlightRequestsCount > 0 || (!bufferedRequestEntries.isEmpty() && flush)) {
            yieldIfThereExistsInFlightRequests();
            if (flush) {
                flush();
            }
        }
    }

    private void yieldIfThereExistsInFlightRequests() throws InterruptedException {
        if (inFlightRequestsCount > 0) {
            mailboxExecutor.yield();
        }
    }

    /**
     * All in-flight requests that are relevant for the snapshot have been completed, but there may
     * still be request entries in the internal buffers that are yet to be sent to the endpoint.
     * These request entries are stored in the snapshot state so that they don't get lost in case of
     * a failure/restart of the application.
     */
    @Override
    public List<BufferedRequestState<RequestEntryT>> snapshotState(long checkpointId) {
        return Collections.singletonList(new BufferedRequestState<>((bufferedRequestEntries)));
    }

    private void initializeState(Collection<BufferedRequestState<RequestEntryT>> states) {
        for (BufferedRequestState<RequestEntryT> state : states) {
            for (RequestEntryWrapper<RequestEntryT> wrapper : state.getBufferedRequestEntries()) {
                addEntryToBuffer(wrapper, false);
            }
        }
    }

    @Override
    public void close() {}

    private int getNextBatchSizeLimit() {
        return Math.min(maxBatchSize, rateLimitingStrategy.getMaxBatchSize());
    }

    protected Consumer<Exception> getFatalExceptionCons() {
        return fatalExceptionCons;
    }

    /** An implementation of {@link ResultHandler} that supports timeout. */
    private class AsyncSinkWriterResultHandler implements ResultHandler<RequestEntryT> {
        private final ScheduledFuture<?> scheduledFuture;
        private final long requestTimestamp;
        private final int batchSize;
        private final AtomicBoolean isCompleted = new AtomicBoolean(false);
        private final List<RequestEntryT> batchEntries;

        public AsyncSinkWriterResultHandler(
                long requestTimestamp, List<RequestEntryT> batchEntries, RequestInfo requestInfo) {
            this.scheduledFuture =
                    timeService.registerTimer(
                            timeService.getCurrentProcessingTime() + requestTimeoutMS,
                            instant -> this.timeout());
            this.requestTimestamp = requestTimestamp;
            this.batchEntries = batchEntries;
            this.batchSize = requestInfo.getBatchSize();
        }

        @Override
        public void complete() {
            if (isCompleted.compareAndSet(false, true)) {
                scheduledFuture.cancel(false);
                mailboxExecutor.execute(
                        () -> completeRequest(Collections.emptyList(), batchSize, requestTimestamp),
                        "Mark in-flight request as completed successfully for batch with size %d",
                        batchSize);
            }
        }

        @Override
        public void completeExceptionally(Exception e) {
            if (isCompleted.compareAndSet(false, true)) {
                scheduledFuture.cancel(false);
                mailboxExecutor.execute(
                        () -> getFatalExceptionCons().accept(e),
                        "Mark in-flight request as failed with fatal exception %s",
                        e.getMessage());
            }
        }

        @Override
        public void retryForEntries(List<RequestEntryT> requestEntriesToRetry) {
            if (isCompleted.compareAndSet(false, true)) {
                scheduledFuture.cancel(false);
                mailboxExecutor.execute(
                        () -> completeRequest(requestEntriesToRetry, batchSize, requestTimestamp),
                        "Mark in-flight request as completed with %d failed request entries",
                        requestEntriesToRetry.size());
            }
        }

        public void timeout() {
            if (isCompleted.compareAndSet(false, true)) {
                mailboxExecutor.execute(
                        () -> {
                            if (failOnTimeout) {
                                getFatalExceptionCons()
                                        .accept(
                                                new TimeoutException(
                                                        "Request timed out after "
                                                                + requestTimeoutMS
                                                                + "ms with failOnTimeout set to true."));
                            } else {
                                // Retry the request on timeout
                                completeRequest(batchEntries, batchSize, requestTimestamp);
                            }
                        },
                        "Mark in-flight request as completed with timeout after %l",
                        requestTimeoutMS);
            }
        }
    }
}
