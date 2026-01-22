/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.opensearch.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.function.ThrowingRunnable;

import org.apache.hc.client5.http.auth.AuthScope;
import org.apache.hc.client5.http.auth.UsernamePasswordCredentials;
import org.apache.hc.client5.http.impl.auth.BasicCredentialsProvider;
import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManager;
import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManagerBuilder;
import org.apache.hc.client5.http.ssl.ClientTlsStrategyBuilder;
import org.apache.hc.client5.http.ssl.NoopHostnameVerifier;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.http.nio.ssl.TlsStrategy;
import org.apache.hc.core5.ssl.SSLContextBuilder;
import org.opensearch.client.opensearch.OpenSearchAsyncClient;
import org.opensearch.client.opensearch.core.BulkRequest;
import org.opensearch.client.opensearch.core.BulkResponse;
import org.opensearch.client.opensearch.core.bulk.BulkResponseItem;
import org.opensearch.client.transport.OpenSearchTransport;
import org.opensearch.client.transport.httpclient5.ApacheHttpClient5TransportBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.flink.util.ExceptionUtils.firstOrSuppressed;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** OpenSearch 3.x writer using the official opensearch-java client. */
@Internal
class Opensearch3Writer<IN> implements SinkWriter<IN> {

    private static final Logger LOG = LoggerFactory.getLogger(Opensearch3Writer.class);

    private final Opensearch3Emitter<? super IN> emitter;
    private final MailboxExecutor mailboxExecutor;
    private final boolean flushOnCheckpoint;
    private final BulkProcessorConfig bulkProcessorConfig;
    private final OpenSearchAsyncClient client;
    private final OpenSearchTransport transport;
    private final DefaultRequestIndexer requestIndexer;
    private final Counter numBytesOutCounter;
    private final Opensearch3BulkResponseInspector bulkResponseInspector;

    private final List<BulkOperation> bufferedOperations;
    private final AtomicLong bufferedBytes;

    private final ScheduledExecutorService scheduler;
    @Nullable private ScheduledFuture<?> scheduledFlush;

    private long pendingActions = 0;
    private boolean checkpointInProgress = false;
    private volatile long lastSendTime = 0;
    private volatile long ackTime = Long.MAX_VALUE;
    private volatile boolean closed = false;

    /**
     * Constructor creating an OpenSearch 3.x writer.
     *
     * @param hosts the reachable OpenSearch cluster nodes
     * @param emitter converting incoming records to OpenSearch operations
     * @param flushOnCheckpoint if true all buffered operations are flushed on checkpoint
     * @param bulkProcessorConfig configuration for bulk processing
     * @param networkClientConfig configuration for the network client
     * @param metricGroup for the sink writer
     * @param mailboxExecutor Flink's mailbox executor
     * @param bulkResponseInspector inspector for bulk responses
     */
    Opensearch3Writer(
            List<HttpHost> hosts,
            Opensearch3Emitter<? super IN> emitter,
            boolean flushOnCheckpoint,
            BulkProcessorConfig bulkProcessorConfig,
            NetworkClientConfig networkClientConfig,
            SinkWriterMetricGroup metricGroup,
            MailboxExecutor mailboxExecutor,
            Opensearch3BulkResponseInspector bulkResponseInspector) {
        this.emitter = checkNotNull(emitter);
        this.flushOnCheckpoint = flushOnCheckpoint;
        this.mailboxExecutor = checkNotNull(mailboxExecutor);
        this.bulkProcessorConfig = checkNotNull(bulkProcessorConfig);
        this.bulkResponseInspector = checkNotNull(bulkResponseInspector);

        this.bufferedOperations = new ArrayList<>();
        this.bufferedBytes = new AtomicLong(0);

        this.transport = createTransport(hosts, networkClientConfig);
        this.client = new OpenSearchAsyncClient(transport);
        this.requestIndexer = new DefaultRequestIndexer(metricGroup.getNumRecordsSendCounter());

        checkNotNull(metricGroup);
        metricGroup.setCurrentSendTimeGauge(() -> ackTime - lastSendTime);
        this.numBytesOutCounter = metricGroup.getIOMetricGroup().getNumBytesOutCounter();

        this.scheduler = Executors.newSingleThreadScheduledExecutor();
        if (bulkProcessorConfig.getBulkFlushInterval() > 0) {
            this.scheduledFlush =
                    scheduler.scheduleWithFixedDelay(
                            this::flushIfNotEmpty,
                            bulkProcessorConfig.getBulkFlushInterval(),
                            bulkProcessorConfig.getBulkFlushInterval(),
                            TimeUnit.MILLISECONDS);
        }

        try {
            emitter.open();
        } catch (Exception e) {
            throw new FlinkRuntimeException("Failed to open the Opensearch3Emitter", e);
        }
    }

    private OpenSearchTransport createTransport(
            List<HttpHost> hosts, NetworkClientConfig config) {
        HttpHost[] hostArray = hosts.toArray(new HttpHost[0]);

        ApacheHttpClient5TransportBuilder builder =
                ApacheHttpClient5TransportBuilder.builder(hostArray);

        builder.setHttpClientConfigCallback(
                httpClientBuilder -> {
                    // Configure credentials if provided
                    if (config.getUsername() != null && config.getPassword() != null) {
                        BasicCredentialsProvider credentialsProvider =
                                new BasicCredentialsProvider();
                        credentialsProvider.setCredentials(
                                new AuthScope(null, -1),
                                new UsernamePasswordCredentials(
                                        config.getUsername(),
                                        config.getPassword().toCharArray()));
                        httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }

                    // Configure SSL if allow insecure
                    if (config.isAllowInsecure().orElse(false)) {
                        try {
                            TlsStrategy tlsStrategy =
                                    ClientTlsStrategyBuilder.create()
                                            .setSslContext(
                                                    SSLContextBuilder.create()
                                                            .loadTrustMaterial(null, (chain, authType) -> true)
                                                            .build())
                                            .setHostnameVerifier(NoopHostnameVerifier.INSTANCE)
                                            .build();
                            PoolingAsyncClientConnectionManager connectionManager =
                                    PoolingAsyncClientConnectionManagerBuilder.create()
                                            .setTlsStrategy(tlsStrategy)
                                            .build();
                            httpClientBuilder.setConnectionManager(connectionManager);
                        } catch (NoSuchAlgorithmException | KeyManagementException | KeyStoreException e) {
                            throw new IllegalStateException("Unable to create custom SSL context", e);
                        }
                    }

                    return httpClientBuilder;
                });

        return builder.build();
    }

    @Override
    public void write(IN element, Context context) throws IOException, InterruptedException {
        // do not allow new bulk writes until all actions are flushed
        while (checkpointInProgress) {
            mailboxExecutor.yield();
        }
        emitter.emit(element, context, requestIndexer);
        maybeFlush();
    }

    private synchronized void maybeFlush() {
        if (shouldFlush()) {
            flush();
        }
    }

    private boolean shouldFlush() {
        int maxActions = bulkProcessorConfig.getBulkFlushMaxActions();
        int maxMb = bulkProcessorConfig.getBulkFlushMaxMb();
        long maxBytes = maxMb > 0 ? maxMb * 1024L * 1024L : -1;

        return (maxActions > 0 && bufferedOperations.size() >= maxActions)
                || (maxBytes > 0 && bufferedBytes.get() >= maxBytes);
    }

    private synchronized void flushIfNotEmpty() {
        if (!bufferedOperations.isEmpty()) {
            flush();
        }
    }

    private synchronized void flush() {
        if (bufferedOperations.isEmpty()) {
            return;
        }

        List<BulkOperation> operationsToSend = new ArrayList<>(bufferedOperations);
        bufferedOperations.clear();
        long bytesToSend = bufferedBytes.getAndSet(0);
        int actionsCount = operationsToSend.size();

        LOG.info("Sending bulk of {} actions to OpenSearch.", actionsCount);
        lastSendTime = System.currentTimeMillis();
        numBytesOutCounter.inc(bytesToSend);

        BulkRequest.Builder bulkRequestBuilder = new BulkRequest.Builder();
        for (BulkOperation op : operationsToSend) {
            bulkRequestBuilder.operations(op.toClientBulkOperation());
        }

        CompletableFuture<BulkResponse> future;
        try {
            future = client.bulk(bulkRequestBuilder.build());
        } catch (IOException e) {
            throw new FlinkRuntimeException("Failed to execute bulk request", e);
        }
        pendingActions += actionsCount;

        future.whenComplete(
                (response, throwable) -> {
                    ackTime = System.currentTimeMillis();
                    if (throwable != null) {
                        enqueueActionInMailbox(
                                () -> {
                                    throw new FlinkRuntimeException(
                                            "Complete bulk has failed.", throwable);
                                },
                                "opensearchErrorCallback");
                    } else {
                        enqueueActionInMailbox(
                                () -> extractFailures(operationsToSend, response),
                                "opensearchSuccessCallback");
                    }
                });
    }

    private void extractFailures(List<BulkOperation> operations, BulkResponse response) {
        bulkResponseInspector.inspect(operations, response);
        pendingActions -= operations.size();
    }

    @Override
    public void flush(boolean endOfInput) throws IOException, InterruptedException {
        checkpointInProgress = true;
        flush();
        while (pendingActions != 0 && (flushOnCheckpoint || endOfInput)) {
            LOG.info("Waiting for the response of {} pending actions.", pendingActions);
            mailboxExecutor.yield();
        }
        checkpointInProgress = false;
    }

    @VisibleForTesting
    void blockingFlushAllActions() throws InterruptedException {
        flush();
        while (pendingActions != 0) {
            LOG.info("Waiting for the response of {} pending actions.", pendingActions);
            mailboxExecutor.yield();
        }
    }

    @Override
    public void close() throws Exception {
        closed = true;
        emitter.close();
        if (scheduledFlush != null) {
            scheduledFlush.cancel(false);
        }
        scheduler.shutdown();
        transport.close();
    }

    private void enqueueActionInMailbox(
            ThrowingRunnable<? extends Exception> action, String actionName) {
        if (isClosed()) {
            return;
        }
        mailboxExecutor.execute(action, actionName);
    }

    private boolean isClosed() {
        if (closed) {
            LOG.warn("Writer was closed before all records were acknowledged by OpenSearch.");
        }
        return closed;
    }

    private class DefaultRequestIndexer implements Opensearch3RequestIndexer {

        private final Counter numRecordsSendCounter;

        DefaultRequestIndexer(Counter numRecordsSendCounter) {
            this.numRecordsSendCounter = checkNotNull(numRecordsSendCounter);
        }

        @Override
        public void addIndexRequest(String index, @Nullable String id, Map<String, Object> document) {
            BulkOperation operation = BulkOperation.index(index, id, document);
            addOperation(operation);
        }

        @Override
        public void addDeleteRequest(String index, String id) {
            BulkOperation operation = BulkOperation.delete(index, id);
            addOperation(operation);
        }

        @Override
        public void addUpdateRequest(String index, String id, Map<String, Object> document) {
            BulkOperation operation = BulkOperation.update(index, id, document, null);
            addOperation(operation);
        }

        @Override
        public void addUpsertRequest(
                String index,
                String id,
                Map<String, Object> document,
                Map<String, Object> upsertDocument) {
            BulkOperation operation = BulkOperation.update(index, id, document, upsertDocument);
            addOperation(operation);
        }

        private void addOperation(BulkOperation operation) {
            numRecordsSendCounter.inc();
            synchronized (Opensearch3Writer.this) {
                bufferedOperations.add(operation);
                bufferedBytes.addAndGet(operation.estimateSizeInBytes());
            }
        }
    }

    /** Interface for inspecting bulk responses and handling failures. */
    @Internal
    interface Opensearch3BulkResponseInspector {
        void inspect(List<BulkOperation> operations, BulkResponse response);
    }

    /** Default implementation that throws on failures. */
    static class DefaultBulkResponseInspector implements Opensearch3BulkResponseInspector {

        private final Opensearch3FailureHandler failureHandler;

        DefaultBulkResponseInspector() {
            this(new DefaultFailureHandler());
        }

        DefaultBulkResponseInspector(Opensearch3FailureHandler failureHandler) {
            this.failureHandler = checkNotNull(failureHandler);
        }

        @Override
        public void inspect(List<BulkOperation> operations, BulkResponse response) {
            if (!response.errors()) {
                return;
            }

            Throwable chainedFailures = null;
            List<BulkResponseItem> items = response.items();
            for (int i = 0; i < items.size(); i++) {
                BulkResponseItem item = items.get(i);
                if (item.error() != null) {
                    BulkOperation operation = i < operations.size() ? operations.get(i) : null;
                    Throwable failure =
                            new FlinkRuntimeException(
                                    String.format(
                                            "Bulk item %d failed: %s - %s",
                                            i,
                                            item.error().type(),
                                            item.error().reason()));
                    chainedFailures = firstOrSuppressed(failure, chainedFailures);
                }
            }

            if (chainedFailures != null) {
                failureHandler.onFailure(chainedFailures);
            }
        }
    }

    /** Interface for handling bulk operation failures. */
    @Internal
    interface Opensearch3FailureHandler {
        void onFailure(Throwable failure);
    }

    /** Default failure handler that throws runtime exceptions. */
    static class DefaultFailureHandler implements Opensearch3FailureHandler, Serializable {

        private static final long serialVersionUID = 1L;

        @Override
        public void onFailure(Throwable failure) {
            if (failure instanceof FlinkRuntimeException) {
                throw (FlinkRuntimeException) failure;
            }
            throw new FlinkRuntimeException(failure);
        }
    }
}
