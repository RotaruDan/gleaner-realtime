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
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class EsState implements State {

	private static final Logger LOG = LoggerFactory.getLogger(EsState.class);

	private final String writingIndex;
	private final String resultsIndex;

	private Gson gson = new Gson();

	@Override
	public void beginCommit(Long aLong) {

	}

	@Override
	public void commit(Long aLong) {

	}

	private RestClient _client;

	public EsState(RestClient client, String writingIndex, String resultsIndex) {
		_client = client;
		this.writingIndex = writingIndex;
		this.resultsIndex = resultsIndex;
	}

	public void bulkUpdateIndices(List<TridentTuple> inputs) {

		StringBuilder bulkRequestBody = new StringBuilder();
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

			String jsonSource = gson.toJson(source, Map.class);

			String actionMetaData = String
					.format("{ \"index\" : { \"_index\" : \"%s\", \"_type\" : \"%s\" } }%n",
							index, type);
			bulkRequestBody.append(actionMetaData);
			bulkRequestBody.append(jsonSource);
			bulkRequestBody.append("\n");
		}
		HttpEntity entity = new NStringEntity(bulkRequestBody.toString(),
				ContentType.APPLICATION_JSON);
		try {
			Response response = _client.performRequest("POST", "/_bulk",
					Collections.emptyMap(), entity);
			int status = response.getStatusLine().getStatusCode();
			if (status > HttpStatus.SC_ACCEPTED) {
				LOG.error("BULK error, status is " + status);
			}
		} catch (Exception e) {
			LOG.error("error while executing bulk request to elasticsearch, "
					+ "failed to store data into elasticsearch", e);
		}
	}

	public void setProperty(String gameplayId, String key, Object value) {

		try {

			Map<String, Object> doc = new HashMap<>();
			doc.put("doc_as_upsert", true);

			Map<String, Object> map = new HashMap<>();
			doc.put("doc", map);
			String[] keys = key.split("\\.");
			for (int i = 0; i < keys.length - 1; ++i) {
				Map<String, Object> keymap = new HashMap<>();
				map.put(keys[i], keymap);
				map = keymap;
			}

			map.put(keys[keys.length - 1], value);

			HttpEntity entity = new NStringEntity(gson.toJson(doc, Map.class),
					ContentType.APPLICATION_JSON);

			Response response = _client.performRequest("POST", "/"
					+ resultsIndex + "/" + ESUtils.getResultsType() + "/"
					+ gameplayId + "/_update?retry_on_conflict=50",
					Collections.emptyMap(), entity);
			int status = response.getStatusLine().getStatusCode();
			if (status > HttpStatus.SC_ACCEPTED) {
				LOG.error("UPDATE error, status is " + status);
			}

		} catch (Exception e) {
			LOG.error("Set Property has failures : {}", e);
		}

	}

	public Map<String, Object> getFromIndex(String index, String type, String id) {
		Map ret;
		try {
			Response response = _client.performRequest("GET", "/" + index + "/"
					+ type + "/" + id);

			int status = response.getStatusLine().getStatusCode();
			if (status > HttpStatus.SC_ACCEPTED) {
				LOG.error("GET on index error, status is " + status);
				return null;
			}

			String stringRes = EntityUtils.toString(response.getEntity());
			System.out.println("stringRes = " + stringRes);
			ret = (Map) gson.fromJson(stringRes, Map.class).get("_source");
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

			HttpEntity entity = new NStringEntity(
					gson.toJson(source, Map.class),
					ContentType.APPLICATION_JSON);
			Response response = _client.performRequest("POST", "/" + index
					+ "/" + type + "/" + id, Collections.emptyMap(), entity);

			int status = response.getStatusLine().getStatusCode();
			if (status > HttpStatus.SC_ACCEPTED) {
				LOG.error("POST on index error, status is " + status);
			}
		} catch (Exception e) {
			LOG.error("error while executing bulk request to elasticsearch, "
					+ "failed to store data into elasticsearch", e);

		}
	}

	/*
	 * private void checkElasticsearchException(Exception e) { if (e instanceof
	 * ReportedFailedException) { throw (ReportedFailedException) e; } else {
	 * throw new RuntimeException(e); } }
	 */

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
			String sessionId = (String) conf
					.get(AbstractAnalysis.SESSION_ID_FLUX_PARAM);
			String writingIndex = ESUtils.getTracesIndex(sessionId);
			String resultsIndex = ESUtils.getResultsIndex((String) conf
					.get(AbstractAnalysis.SESSION_ID_FLUX_PARAM));
			EsState s = new EsState(makeElasticsearchClient(new HttpHost(
					esHost, 9200)), writingIndex, resultsIndex);

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
