/**
 * Copyright © 2016 e-UCM (http://www.e-ucm.es/)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package es.eucm.rage.realtime.states.elasticsearch;

import com.google.gson.Gson;
import es.eucm.rage.realtime.AbstractAnalysis;
import es.eucm.rage.realtime.topologies.TopologyBuilder;
import es.eucm.rage.realtime.utils.Document;
import es.eucm.rage.realtime.utils.ESUtils;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpStatus;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.apache.storm.task.IMetricsContext;
import org.apache.storm.topology.ReportedFailedException;
import org.apache.storm.trident.state.State;
import org.apache.storm.trident.state.StateFactory;
import org.apache.storm.trident.tuple.TridentTuple;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.single.instance.InstanceShardOperationRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class EsState implements State {

    private static final Logger LOG = LoggerFactory.getLogger(EsState.class);

    private final RestHighLevelClient hClient;

    private Gson gson = new Gson();

    @Override
    public void beginCommit(Long aLong) {

    }

    @Override
    public void commit(Long aLong) {

    }

    private RestClient _client;

    public EsState(RestClient client, RestHighLevelClient hClient) {
        _client = client;
        this.hClient = hClient;
    }

    public void bulkUpdateIndices(List<TridentTuple> inputs) {

        try {
            BulkRequest request = new BulkRequest();

            for (TridentTuple input : inputs) {
                Document<Map> doc = (Document<Map>) input.get(0);
                Map source = doc.getSource();

                String index = ESUtils.getTracesIndex(doc.getIndex());
                String indexPrefix = doc.getIndexPrefix();
                if (indexPrefix != null) {
                    index = indexPrefix + "-" + index;
                }

                String type = doc.getType();
                if (type == null) {
                    type = ESUtils.getTracesType();
                }

                DocWriteRequest req;
                Object uuidv4 = source.get(TopologyBuilder.UUIDV4);
                if (uuidv4 != null) {
                    req = new UpdateRequest(index, type, uuidv4.toString())
                            .docAsUpsert(true).doc(source).retryOnConflict(10);
                } else {
                    req = new IndexRequest(index, type).source(source);
                }

                request.add(req);
            }


            BulkResponse bulkResponse = hClient.bulk(request);

            if (bulkResponse.hasFailures()) {
                LOG.error("BULK hasFailures proceeding to re-bulk");
                for (BulkItemResponse bulkItemResponse : bulkResponse) {
                    if (bulkItemResponse.isFailed()) {
                        BulkItemResponse.Failure failure = bulkItemResponse
                                .getFailure();
                        LOG.error("Failure " + failure.getCause()
                                + ", response: "
                                + gson.toJson(bulkItemResponse));

                        BulkResponse bulkResponse2 = hClient.bulk(request);
                        if (bulkResponse2.hasFailures()) {
                            LOG.error("BULK hasFailures proceeding to re-bulk");
                            for (BulkItemResponse bulkItemResponse2 : bulkResponse2) {
                                if (bulkItemResponse2.isFailed()) {
                                    BulkItemResponse.Failure failure2 = bulkItemResponse2
                                            .getFailure();
                                    LOG.error("Failure " + failure2.getCause()
                                            + ", response: "
                                            + gson.toJson(bulkItemResponse2));
                                }

                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            LOG.error("error while executing bulk request to elasticsearch, "
                    + "failed to store data into elasticsearch", e);
        }
    }

    public void setProperty(String activityId, String gameplayId, String key,
                            Object value) {

        try {

            Map<String, Object> map = new HashMap<>();
            String[] keys = key.split("\\.");
            Map nested = map;
            for (int i = 0; i < keys.length - 1; ++i) {
                Map<String, Object> keymap = new HashMap<>();
                nested.put(keys[i], keymap);
                nested = keymap;
            }

            nested.put(keys[keys.length - 1], value);

            hClient.update(new UpdateRequest(ESUtils
                    .getResultsIndex(activityId), ESUtils.getResultsType(),
                    gameplayId).docAsUpsert(true).doc(map).retryOnConflict(50));

        } catch (Exception e) {
            LOG.error("Set Property has failures : {}", e);
        }
    }

    public Map<String, Object> getFromIndex(String index, String type, String id) {
        Map ret;
        try {
            GetRequest getRequest = new GetRequest(index, type, id);

            GetResponse resp = hClient.get(getRequest);

            ret = resp.getSourceAsMap();
        } catch (Exception e) {
            LOG.error(
                    "error while executing getFromIndex request from elasticsearch, "
                            + "failed to store data into elasticsearch", e);
            ret = null;
        }
        return ret;
    }

    public void setOnIndex(String index, String type, String id, Map source) {
        try {

            hClient.update(new UpdateRequest(index, type, id).docAsUpsert(true)
                    .doc(source).retryOnConflict(50));
        } catch (Exception e) {
            LOG.error(
                    "error while executing setOnIndex request to elasticsearch, "
                            + "failed to store data into elasticsearch", e);

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

            String esHost = (String) conf
                    .get(AbstractAnalysis.ELASTICSEARCH_URL_FLUX_PARAM);
            RestClient client = makeElasticsearchClient(new HttpHost(esHost,
                    9200));
            EsState s = new EsState(client, new RestHighLevelClient(client));

            return s;
        }

        /**
         * Constructs a java elasticsearch 5 client for the host..
         *
         * @return {@link RestClient} to read/write to the hash ring of the
         *         servers..
         */
        public RestClient makeElasticsearchClient(HttpHost... endpoints) {
            return RestClient.builder(endpoints).build();
        }
    }
}
