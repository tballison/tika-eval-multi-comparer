/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.tallison.tika.eval.multicomparer;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.serialization.JsonMetadataList;
import org.apache.tika.pipes.FetchEmitTuple;
import org.apache.tika.pipes.emitter.EmitData;
import org.apache.tika.pipes.emitter.EmitKey;
import org.apache.tika.pipes.emitter.Emitter;
import org.apache.tika.pipes.emitter.EmitterManager;
import org.apache.tika.pipes.fetcher.FetchKey;
import org.apache.tika.pipes.fetcher.Fetcher;
import org.apache.tika.pipes.fetcher.FetcherManager;
import org.apache.tika.pipes.pipesiterator.CallablePipesIterator;
import org.apache.tika.pipes.pipesiterator.PipesIterator;

public class MultiProfiler {

    private static final Logger LOGGER = LoggerFactory.getLogger(MultiProfiler.class);
    private static final EmitData EMIT_DATA_STOP_SEMAPHORE = new EmitData(null, null);
    private static final int MAX_EMIT_QUEUE = 10000;

    private static final String PATHS_EMITTER_AND_FETCHER_NAME = "paths";
    public static void main(String[] args) throws Exception {
        if (Files.exists(Paths.get("/home/tallison/data/tst.db"))) {
            Files.delete(Paths.get("/home/tallison/data/tst.db"));
        }
        Config config = Config.load(Paths.get(args[0]));
        EmitterManager emitterManager = EmitterManager.load(Paths.get(config.tikaConfig));
        injectPathTable(config, emitterManager);
        for (FetchEmitterTuple t : config.fetchEmitterTuples) {
            processTable(t, config, emitterManager);
        }
        closeEmitters(emitterManager);
    }

    private static void injectPathTable(Config config, EmitterManager emitterManager)
            throws TikaException, IOException {
        Emitter emitter = emitterManager.getEmitter(PATHS_EMITTER_AND_FETCHER_NAME);
        Fetcher fetcher = FetcherManager.load(Paths.get(config.tikaConfig)).getFetcher(PATHS_EMITTER_AND_FETCHER_NAME);
        PipesIterator pipesIterator = PipesIterator.build(Paths.get(config.tikaConfig));
        List<EmitData> data = new ArrayList<>();
        for (FetchEmitTuple t : pipesIterator) {
            long length = getLength(fetcher, t.getFetchKey());
            Metadata m = new Metadata();
            m.set("length", Long.toString(length));
            data.add(new EmitData(new EmitKey(PATHS_EMITTER_AND_FETCHER_NAME, t.getEmitKey().getEmitKey()),
                    List.of(m)));
            //LOGGER.info("adding " + t.getEmitKey().getEmitKey());
            if (data.size() >= 1000) {
                long start = System.currentTimeMillis();
                LOGGER.info("emitting: " + data.size());
                emitter.emit(data);
                LOGGER.info("emitted: " + data.size() + " elapsed: " + (System.currentTimeMillis() - start));
                data.clear();
            }
        }
        if (!data.isEmpty()) {
            emitter.emit(data);
        }
    }

    private static long getLength(Fetcher fetcher, FetchKey fetchKey) {
        Metadata m = new Metadata();
        try (InputStream is = fetcher.fetch(fetchKey.getFetchKey(), m)) {
            String len = m.get(Metadata.CONTENT_LENGTH);
            if (!StringUtils.isAllBlank(len)) {
                try {
                    return Long.parseLong(len);
                } catch (NumberFormatException e) {
                    //do nothing
                }
            }
        } catch (IOException | TikaException e) {
            LOGGER.warn("io exception on " + fetchKey.getFetchKey(), e);
        }
        return -1l;
    }

    private static void processTable(FetchEmitterTuple t, Config config, EmitterManager emitterManager)
            throws TikaException, IOException {

        ArrayBlockingQueue<EmitData> emitDataQueue = new ArrayBlockingQueue<>(1000);
        ArrayBlockingQueue<FetchEmitTuple> fetchEmitTuples = new ArrayBlockingQueue<>(1000);
        Path tikaConfigPath = Paths.get(config.tikaConfig);
        Emitter emitter = emitterManager.getEmitter(t.emitter);
        ExecutorService executorService = Executors.newFixedThreadPool(config.numThreads + 2);
        ExecutorCompletionService<Long> executorCompletionService =
                new ExecutorCompletionService<>(executorService);

        executorCompletionService.submit(new Writer(emitter, emitDataQueue));

        CallablePipesIterator callablePipesIterator =
                new CallablePipesIterator(PipesIterator.build(tikaConfigPath),
                fetchEmitTuples, 600000, config.numThreads);

        executorCompletionService.submit(callablePipesIterator);

        for (int i = 0; i < config.numThreads; i++) {
            Fetcher fetcher = FetcherManager.load(tikaConfigPath).getFetcher(t.fetcher);
            executorCompletionService.submit(new Worker(fetchEmitTuples, emitDataQueue, fetcher));
        }

        try {
            int finished = 0;
            while (finished < config.numThreads + 1) {
                //hang forever
                Future<Long> future = executorCompletionService.take();
                future.get();
                finished++;
            }

            //hang forever until you can tell the writer to stop
            emitDataQueue.put(EMIT_DATA_STOP_SEMAPHORE);
            //hang forever
            Future<Long> future = executorCompletionService.take();
            future.get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            executorService.shutdownNow();
        }
    }

    private static class Writer implements Callable<Long> {
        private final Emitter emitter;
        private final ArrayBlockingQueue<EmitData> evalEmitData;

        public Writer(Emitter emitter, ArrayBlockingQueue<EmitData> evalEmitData) {
            this.emitter = emitter;
            this.evalEmitData = evalEmitData;
        }

        @Override
        public Long call() throws Exception {
            List<EmitData> emitDataList = new ArrayList<>();
            while (true) {
                EmitData emitData = evalEmitData.take();
                if (emitData == EMIT_DATA_STOP_SEMAPHORE) {
                    if (! emitDataList.isEmpty()) {
                        emitter.emit(emitDataList);
                    }
                    if (emitter instanceof Closeable) {
                        ((Closeable)emitter).close();
                    }
                    return 1l;
                }
                emitDataList.add(emitData);
                if (emitDataList.size() >= MAX_EMIT_QUEUE) {
                    emitter.emit(emitDataList);
                    emitDataList.clear();
                }
            }
        }
    }

    private static void closeEmitters(EmitterManager emitterManager) throws Exception {
        for (String emitterName : emitterManager.getSupported()) {
            Emitter emitter = emitterManager.getEmitter(emitterName);
            if (emitter instanceof Closeable) {
                ((Closeable)emitter).close();
            }
        }
    }

    private static class Worker implements Callable<Long> {
        private final ExtractEnricher extractEnricher = new ExtractEnricher();

        private final ArrayBlockingQueue<FetchEmitTuple> fetchEmitTuples;
        private final ArrayBlockingQueue<EmitData> evalEmitData;

        private final Fetcher fetcher;

        public Worker(ArrayBlockingQueue<FetchEmitTuple> fetchEmitTuples,
                      ArrayBlockingQueue<EmitData> evalEmitData, Fetcher fetcher) {
            this.fetchEmitTuples = fetchEmitTuples;
            this.evalEmitData = evalEmitData;
            this.fetcher = fetcher;
        }

        @Override
        public Long call() throws Exception {
            while (true) {
                //hang forever
                FetchEmitTuple t = fetchEmitTuples.take();
                if (t == PipesIterator.COMPLETED_SEMAPHORE) {
                    return 1l;
                }
                List<Metadata> metadataList = null;
                try (Reader reader =
                             new BufferedReader(
                                     new InputStreamReader(
                                             fetcher.fetch(t.getFetchKey().getFetchKey() + ".json",
                                                     new Metadata()), StandardCharsets.UTF_8))) {
                    metadataList = JsonMetadataList.fromJson(reader);
                } catch (IOException e) {
                    metadataList = List.of(new Metadata());
                }
                //hang forever
                evalEmitData.put(new EmitData(t.getEmitKey(),
                        extractEnricher.enrich(metadataList)));
            }
        }
    }

    private static class Config {

        private static Config load(Path p) throws IOException {
            ObjectMapper mapper = new ObjectMapper();
            return mapper.readValue(p.toFile(), Config.class);
        }


        @JsonProperty
        int numThreads;
        @JsonProperty
        String tikaConfig;
        @JsonProperty
        List<FetchEmitterTuple> fetchEmitterTuples;
    }

    private static class FetchEmitterTuple {
        @JsonProperty
        String fetcher;
        @JsonProperty
        String emitter;
    }
}
