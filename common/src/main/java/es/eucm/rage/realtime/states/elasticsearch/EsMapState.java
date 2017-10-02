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
import org.apache.storm.topology.ReportedFailedException;
import org.apache.storm.trident.state.*;
import org.apache.storm.trident.state.map.*;
import org.apache.storm.tuple.Values;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;

public class EsMapState<T> implements IBackingMap<T> {

	private static final Logger LOG = LoggerFactory.getLogger(EsMapState.class);

	public static class Options implements Serializable {

		public int localCacheSize = 1000;
		public String globalKey = "$GLOBAL$";

		public String opaqueIndex;
		public String resultsIndex;
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
			String sessionId = (String) conf
					.get(AbstractAnalysis.SESSION_ID_FLUX_PARAM);
			_opts.opaqueIndex = ESUtils.getOpaqueValuesIndex(sessionId);
			_opts.resultsIndex = ESUtils.getResultsIndex(sessionId);
			EsMapState s = new EsMapState(makeElasticsearchClient(new HttpHost(
					esHost, 9200)), _opts);

			s.registerMetrics(conf, context);
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

	public EsMapState(RestClient client, Options opts) {
		_client = client;
		_opts = opts;
	}

	/**
	 * MapState implementation
	 **/

	@Override
	public List<T> multiGet(List<List<Object>> keys) {

		String mgetJson = "";
		try {
			LinkedList<String> singleKeys = new LinkedList();
			for (List<Object> key : keys) {
				singleKeys.add(toSingleKey(key));
			}
			List<T> ret = new ArrayList(singleKeys.size());

			Map<String, List> body = new HashMap<>();
			List<Map> query = new ArrayList<>();
			body.put("docs", query);

			while (!singleKeys.isEmpty()) {

				Map<String, String> doc = new HashMap<>();
				query.add(doc);

				doc.put("_type", ESUtils.getOpaqueValuesType());
				doc.put("_id", singleKeys.removeFirst());
			}

			mgetJson = gson.toJson(body, Map.class);
			LOG.info("MGET JSON " + mgetJson);
			HttpEntity entity = new NStringEntity(mgetJson,
					ContentType.APPLICATION_JSON);

			Response response = _client.performRequest("GET", "/"
					+ _opts.opaqueIndex + "/_mget", Collections.emptyMap(),
					entity);

			int status = response.getStatusLine().getStatusCode();
			if (status > HttpStatus.SC_ACCEPTED) {
				LOG.info("There was an MGET error, mget JSON " + mgetJson);
				LOG.error("MGET error, status is " + status);
				return ret;
			}

			String responseString = EntityUtils.toString(response.getEntity());
			System.out.println("responseString multiGet = " + responseString);
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

			// checkElasticsearchException(e);
		}

		return null;
	}

	@Override
	public void multiPut(List<List<Object>> keys, List<T> vals) {

		try {
			StringBuilder bulkRequestBody = new StringBuilder();

			for (int i = 0; i < keys.size(); i++) {

				// Update the result
				List<Object> key = keys.get(i);
				OpaqueValue val = (OpaqueValue) vals.get(i);

				String gameplayId = (String) key.get(1);
				setProperty(gameplayId, key.subList(2, key.size()),
						val.getCurr());

				String keyId = toSingleKey(keys.get(i));
				String serialized = ser.serialize(val);

				String index = _opts.opaqueIndex;
				String type = ESUtils.getOpaqueValuesType();
				String id = keyId;
				String source = serialized;

				String actionMetaData = String
						.format("{ \"index\" : { \"_index\" : \"%s\", \"_type\" : \"%s\", \"_id\" : \"%s\" } }%n",
								index, type, id);
				bulkRequestBody.append(actionMetaData);
				bulkRequestBody.append(source);
				bulkRequestBody.append("\n");
			}

			HttpEntity entity = new NStringEntity(bulkRequestBody.toString(),
					ContentType.APPLICATION_JSON);

			Response response = _client.performRequest("POST", "/_bulk",
					Collections.emptyMap(), entity);
			int status = response.getStatusLine().getStatusCode();
			if (status > HttpStatus.SC_ACCEPTED) {
				LOG.error("BULK error, status is", status);
			}

		} catch (Exception e) {
			LOG.error("MULTI PUT error", e);
		}
	}

	public void setProperty(String gameplayId, List<Object> keys, Object value) {

		try {

			Map<String, Object> doc = new HashMap<>();
			doc.put("doc_as_upsert", true);

			Map<String, Object> map = new HashMap<>();
			doc.put("doc", map);
			for (int i = 0; i < keys.size() - 1; ++i) {
				Map<String, Object> keymap = new HashMap<>();
				map.put(keys.get(i).toString(), keymap);
				map = keymap;
			}
			map.put(keys.get(keys.size() - 1).toString(), value);

			HttpEntity entity = new NStringEntity(gson.toJson(doc, Map.class),
					ContentType.APPLICATION_JSON);

			Response response = _client.performRequest("POST", "/"
					+ _opts.resultsIndex + "/" + ESUtils.getResultsType() + "/"
					+ gameplayId + "/_update?retry_on_conflict=50",
					Collections.emptyMap(), entity);
			int status = response.getStatusLine().getStatusCode();
			if (status > HttpStatus.SC_ACCEPTED) {
				LOG.error("UPDATE error, status is" + status);
			}

		} catch (Exception e) {
			LOG.error("Set Property has failures : {}", e);
		}

	}

	/*
	 * private void checkElasticsearchException(Exception e) { if (_mexceptions
	 * != null) { _mexceptions.incr(); } if (e instanceof
	 * ReportedFailedException) { throw (ReportedFailedException) e; } else {
	 * throw new RuntimeException(e); } }
	 */
	private void registerMetrics(Map conf, IMetricsContext context) {

		Long longBucketSize = (Long) (conf
				.get(Config.TOPOLOGY_BUILTIN_METRICS_BUCKET_SIZE_SECS));
		String uniqueId = (String) conf
				.get(AbstractAnalysis.SESSION_ID_FLUX_PARAM);
		int bucketSize = longBucketSize.intValue();
		String stateUniqueId = this.toString();
		_mreads = context.registerMetric(stateUniqueId
				+ "/elasticsearch/readCount/" + uniqueId, new CountMetric(),
				bucketSize);
		_mwrites = context.registerMetric(stateUniqueId
				+ "/elasticsearch/writeCount/ " + uniqueId, new CountMetric(),
				bucketSize);
		_mexceptions = context.registerMetric(stateUniqueId
				+ "/elasticsearch/exceptionCount/" + uniqueId,
				new CountMetric(), bucketSize);
	}

	private String toSingleKey(List<Object> key) {
		String result = "";
		for (Object o : key) {
			result += o;
		}
		return result.toString();
	}

}
