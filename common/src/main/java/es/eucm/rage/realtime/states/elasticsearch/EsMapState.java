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
import es.eucm.rage.realtime.JSONOpaqueSerializerString;
import es.eucm.rage.realtime.utils.ESUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpStatus;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.apache.storm.Config;
import org.apache.storm.metric.api.CountMetric;
import org.apache.storm.task.IMetricsContext;
import org.apache.storm.trident.state.*;
import org.apache.storm.trident.state.map.*;
import org.apache.storm.tuple.Values;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;

public class EsMapState<T> implements IBackingMap<T> {

	private static final Logger LOG = LoggerFactory.getLogger(EsMapState.class);
	private final RestHighLevelClient hClient;

	public static class Options implements Serializable {

		public int localCacheSize = 1000;
		public String globalKey = "$GLOBAL$";
	}

	public static StateFactory opaque() {
		return opaque(new Options());
	}

	public static StateFactory opaque(Options opts) {
		return new Factory(opts);
	}

	public static class Factory implements StateFactory {
		protected Options _opts;

		public Factory() {
			this(new Options());
		}

		public Factory(Options options) {
			_opts = options;
		}

		@Override
		public State makeState(Map conf, IMetricsContext context,
				int partitionIndex, int numPartitions) {

			String esHost = (String) conf
					.get(AbstractAnalysis.ELASTICSEARCH_URL_FLUX_PARAM);
			RestClient client = makeElasticsearchClient(new HttpHost(esHost,
					9200));
			EsMapState s = new EsMapState(client, new RestHighLevelClient(
					client), _opts);

			CachedMap c = new CachedMap(s, _opts.localCacheSize);
			MapState ms = OpaqueMap.build(c);

			return new SnapshottableMap(ms, new Values(_opts.globalKey));
		}

		/**
		 * Constructs a java elasticsearch 5 client for the host..
		 * 
		 * @param endpoints
		 *            list of {@code InetAddress} for all the elasticsearch 5
		 *            servers
		 * @return {@link RestClient} to read/write to the hash ring of the
		 *         servers..
		 */
		public RestClient makeElasticsearchClient(HttpHost... endpoints) {
			return RestClient.builder(endpoints).build();
		}
	}

	private JSONOpaqueSerializerString ser = new JSONOpaqueSerializerString();
	private final RestClient _client;
	private Options _opts;
	CountMetric _mreads;
	CountMetric _mwrites;
	CountMetric _mexceptions;
	private Gson gson = new Gson();

	public EsMapState(RestClient client, RestHighLevelClient hClient,
			Options opts) {
		_client = client;
		_opts = opts;
		this.hClient = hClient;
	}

	/**
	 * MapState implementation
	 **/

	@Override
	public List<T> multiGet(List<List<Object>> keys) {

		String mgetJson = "";
		try {

			Map<String, List> body = new HashMap<>();
			List<Map> query = new ArrayList<>();
			body.put("docs", query);

			for (List<Object> key : keys) {

				String index = key.get(0).toString();
				String type = ESUtils.getOpaqueValuesType();
				String id = toSingleKey(key);

				Map<String, String> doc = new HashMap<>();
				query.add(doc);

				doc.put("_index", index);
				doc.put("_type", type);
				doc.put("_id", id);
			}
			List<T> ret = new ArrayList(keys.size());

			mgetJson = gson.toJson(body, Map.class);
			HttpEntity entity = new NStringEntity(mgetJson,
					ContentType.APPLICATION_JSON);
			Response response = _client.performRequest("GET", "_mget",
					Collections.emptyMap(), entity);

			int status = response.getStatusLine().getStatusCode();
			if (status > HttpStatus.SC_ACCEPTED) {
				LOG.info("There was an MGET error, mget JSON " + mgetJson);
				LOG.error("MGET error, status is " + status);
				return ret;
			}

			String responseString = EntityUtils.toString(response.getEntity());
			Map<String, Object> responseDocs = gson.fromJson(responseString,
					Map.class);

			List<Object> docs = (List) responseDocs.get("docs");
			for (Object doc : docs) {
				Map<String, Object> docMap = (Map) doc;
				T resDoc = (T) docMap.get("_source");

				ret.add(resDoc);
			}

			if (_mreads != null) {
				_mreads.incrBy(ret.size());
			}
			return ret;
		} catch (Exception e) {
			LOG.info("There was an MGET error, mget JSON " + mgetJson);
			LOG.error("Exception while mget", e);
		}

		return null;
	}

	@Override
	public void multiPut(List<List<Object>> keys, List<T> vals) {

		try {
			BulkRequest request = new BulkRequest();

			for (int i = 0; i < keys.size(); i++) {

				// Update the result
				List<Object> key = keys.get(i);
				OpaqueValue val = (OpaqueValue) vals.get(i);

				String activityId = (String) key.get(0);
				String gameplayId = (String) key.get(1);
				setProperty(activityId, gameplayId, key.subList(2, key.size()),
						val.getCurr());

				String keyId = toSingleKey(keys.get(i));
				String serialized = ser.serialize(val);

				String index = ESUtils.getOpaqueValuesIndex(activityId);
				String type = ESUtils.getOpaqueValuesType();
				String id = keyId;
				String source = serialized;
				request.add(new UpdateRequest(index, type, id)
						.docAsUpsert(true).doc(source, XContentType.JSON)
						.retryOnConflict(50));
			}

			BulkResponse bulkResponse = hClient.bulk(request);

			if (bulkResponse.hasFailures()) {
				LOG.error("BULK hasFailures proceeding to re-bulk");
				for (BulkItemResponse bulkItemResponse : bulkResponse) {
					if (bulkItemResponse.isFailed()) {
						BulkItemResponse.Failure failure = bulkItemResponse
								.getFailure();
						LOG.error("Failure " + failure.getCause());
					}
				}
			}

		} catch (Exception e) {
			LOG.error("MULTI PUT error", e);
		}
	}

	private void setProperty(String activityId, String gameplayId,
			List<Object> keys, Object value) {

		try {

			Map<String, Object> doc = new HashMap<>();

			Map<String, Object> map = doc;
			for (int i = 0; i < keys.size() - 1; ++i) {
				Map<String, Object> keymap = new HashMap<>();
				map.put(keys.get(i).toString(), keymap);
				map = keymap;
			}
			map.put(keys.get(keys.size() - 1).toString(), value);

			hClient.update(new UpdateRequest(ESUtils
					.getResultsIndex(activityId), ESUtils.getResultsType(),
					gameplayId).docAsUpsert(true).doc(doc).retryOnConflict(50));

		} catch (Exception e) {
			LOG.error("Set Property has failures : {}", e);
		}

	}

	private String toSingleKey(List<Object> key) {
		String result = "";
		for (Object o : key) {
			result += o;
		}
		return result.toString();
	}

}
