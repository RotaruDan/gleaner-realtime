/**
 * Copyright (C) 2016 e-UCM (http://www.e-ucm.es/)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package es.eucm.rage.realtime.states.elasticsearch;

import es.eucm.rage.realtime.AbstractAnalysis;
import es.eucm.rage.realtime.utils.Document;
import es.eucm.rage.realtime.utils.ESUtils;
import org.apache.storm.task.IMetricsContext;
import org.apache.storm.topology.ReportedFailedException;
import org.apache.storm.trident.state.State;
import org.apache.storm.trident.state.StateFactory;
import org.apache.storm.trident.tuple.TridentTuple;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class EsState implements State {

    private static final Logger LOG = LoggerFactory.getLogger(EsState.class);

    private final String writingIndex;
    private final String resultsIndex;

    @Override
    public void beginCommit(Long aLong) {

    }

    @Override
    public void commit(Long aLong) {

    }

    private final TransportClient _client;

    public EsState(TransportClient client, String writingIndex,
                   String resultsIndex) {
        _client = client;
        this.writingIndex = writingIndex;
        this.resultsIndex = resultsIndex;
    }

    public void bulkUpdateIndices(List<TridentTuple> inputs) {

        BulkRequestBuilder bulkRequest = _client.prepareBulk();
        for (TridentTuple input : inputs) {
            Document<Map> doc = (Document<Map>) input.get(0);
            Map source = doc.getSource();


            String index = writingIndex;
            String indexPrefix = doc.getIndexPrefix();
            if (indexPrefix != null) {
                index = indexPrefix + "-" + index;
            }

            String type = doc.getType();
            if (type == null) {
                type = ESUtils.getTracesType();
            }

            IndexRequestBuilder request = _client.prepareIndex(index,
                    type, doc.getId()).setSource(source);

            bulkRequest.add(request);
        }

        if (bulkRequest.numberOfActions() > 0) {
            try {
                BulkResponse response = bulkRequest.execute().actionGet();
                if (response.hasFailures()) {
                    LOG.error("BulkResponse has failures : {}",
                            response.buildFailureMessage());

                }
            } catch (Exception e) {
                LOG.error(
                        "error while executing bulk request to elasticsearch, "
                                + "failed to store data into elasticsearch", e);
            }
        }
    }

    public void setProperty(String gameplayId, String key, Object value) {

        try {
            XContentBuilder xContentBuilder = jsonBuilder().startObject();

            String[] keys = key.split("\\.");
            for (int i = 0; i < keys.length - 1; ++i) {
                xContentBuilder = xContentBuilder.startObject(keys[i]);
            }
            xContentBuilder.field(keys[keys.length - 1], value);
            for (int i = 0; i < keys.length - 1; ++i) {
                xContentBuilder = xContentBuilder.endObject();
            }
            UpdateRequest updateRequest = new UpdateRequest(resultsIndex,
                    ESUtils.getResultsType(), gameplayId).doc(
                    xContentBuilder.endObject()).docAsUpsert(true);
            _client.update(updateRequest).get();

        } catch (Exception e) {
            LOG.error("Set Property has failures : {}", e);
            checkElasticsearchException(e);
        }

    }

    public Map<String, Object> getFromIndex(String index, String type, String id) {
        try {
            GetResponse response = _client.prepareGet(index, type, id)
                    .setOperationThreaded(false).get();
            return response.getSourceAsMap();
        } catch (IndexNotFoundException ex) {
            return null;
        }
    }

    public void setOnIndex(String index, String type, String id, Map source) {
        _client.prepareIndex(index, type, id).setSource(source).get();
    }

    private void checkElasticsearchException(Exception e) {
        if (e instanceof ReportedFailedException) {
            throw (ReportedFailedException) e;
        } else {
            throw new RuntimeException(e);
        }
    }

    public static StateFactory opaque() {
        return new Factory();
    }

    public static class Factory implements StateFactory {

        public Factory() {
        }

        @Override
        public State makeState(Map conf, IMetricsContext context,
                               int partitionIndex, int numPartitions) {
            EsState s;
            try {
                String esHost = (String) conf
                        .get(AbstractAnalysis.ELASTICSEARCH_URL_FLUX_PARAM);
                String sessionId = (String) conf
                        .get(AbstractAnalysis.SESSION_ID_FLUX_PARAM);
                String writingIndex = ESUtils.getTracesIndex(sessionId);
                String resultsIndex = ESUtils.getResultsIndex((String) conf
                        .get(AbstractAnalysis.SESSION_ID_FLUX_PARAM));
                s = new EsState(
                        makeElasticsearchClient(Arrays.asList(InetAddress
                                .getByName(esHost))), writingIndex,
                        resultsIndex);
            } catch (UnknownHostException e) {
                throw new RuntimeException(e);
            }
            return s;
        }

        /**
         * Constructs a java elasticsearch 5 client for the host..
         *
         * @return {@link TransportClient} to read/write to the hash ring of the
         *         servers..
         */
        public TransportClient makeElasticsearchClient(
                List<InetAddress> endpoints) throws UnknownHostException {
            TransportClient client = new PreBuiltTransportClient(Settings.EMPTY);

            for (InetAddress endpoint : endpoints) {
                client.addTransportAddress(new InetSocketTransportAddress(
                        endpoint, 9300));
            }
            return client;
        }
    }
}
