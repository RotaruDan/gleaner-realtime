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
package es.eucm.gleaner.realtime.states;

import es.eucm.gleaner.realtime.utils.DBUtils;
import es.eucm.gleaner.realtime.utils.Document;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHitField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.state.OpaqueValue;
import storm.trident.tuple.TridentTuple;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.*;

public class ESGameplayState extends GameplayState {

	public static final String STORED_KEY = "stored";
	public static final String RAGE_OPAQUE_VALUES_DOCUMENT_TYPE = "opaquevalues";
	public static final String RAGE_TRACES_DOCUMENT_TYPE = "traces";
	public static final String RAGE_RESULTS_DOCUMENT_TYPE = "results";

	private static final Logger LOG = LoggerFactory
			.getLogger(ESGameplayState.class);

	private final TransportClient client;
	private String opaqueValuesIndex, resultsIndex;

	public ESGameplayState(TransportClient client, String sessionId) {
		this.client = client;
		opaqueValuesIndex = DBUtils.getOpaqueValuesIndex(sessionId);
		resultsIndex = DBUtils.getResultsIndex(sessionId);
	}

	@Override
	public void setProperty(String versionId, String gameplayId, String key,
			Object value) {

		try {
			key = key.replace(".", "-");
			UpdateRequest updateRequest = new UpdateRequest(resultsIndex,
					RAGE_RESULTS_DOCUMENT_TYPE, gameplayId).doc(
					jsonBuilder().startObject().field(key, value)
							.field(STORED_KEY, new Date())).docAsUpsert(true);
			client.update(updateRequest).get();

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void setOpaqueValue(String versionId, String gameplayId,
			List<Object> keys, OpaqueValue value) {

		setProperty(versionId, gameplayId, buildKey(keys), value.getCurr());

		String key = toKey(gameplayId, keys);

		try {
			UpdateRequest updateRequest = new UpdateRequest(opaqueValuesIndex,
					RAGE_OPAQUE_VALUES_DOCUMENT_TYPE, key).doc(
					jsonBuilder().startObject().field(KEY_KEY, key)
							.field(VALUE_KEY, toDBObject(value))).docAsUpsert(
					true);
			client.update(updateRequest).get();
		} catch (Exception e) {
			LOG.error("Error setting property " + key + "=" + value, e);
		}
	}

	@Override
	public OpaqueValue getOpaqueValue(String versionId, String gameplayId,
			List<Object> keys) {

		String key = toKey(gameplayId, keys);

		try {
			SearchResponse response = client.prepareSearch(opaqueValuesIndex)
					.setTypes(RAGE_OPAQUE_VALUES_DOCUMENT_TYPE)
					.setQuery(QueryBuilders.termQuery(KEY_KEY, key))
					// Query
					.setFrom(0).setSize(1).setExplain(true).execute()
					.actionGet();

			Map<String, SearchHitField> fields = response.getHits().getAt(0)
					.getFields();

			return fields == null ? null : toOpaqueValue(fields);

		} catch (Exception e) {
			LOG.error("Error querying opaque value " + key, e);

			return null;
		}
	}

	private String toKey(String gameplayId, List<Object> key) {
		String result = gameplayId;
		for (Object o : key) {
			result += o;
		}
		return result;
	}

	private Map toDBObject(OpaqueValue value) {
		Map dbObject = new HashMap();
		dbObject.put(TRANSACTION_ID, value.getCurrTxid());
		dbObject.put(PREVIOUS_VALUE_ID, value.getPrev());
		dbObject.put(CURRENT_VALUE_ID, value.getCurr());
		return dbObject;
	}

	private String buildKey(List<Object> keys) {
		String result = "";
		for (Object key : keys) {
			result += key + ".";
		}
		return result.substring(0, result.length() - 1);
	}

	private OpaqueValue toOpaqueValue(Map<String, SearchHitField> dbObject) {
		Map opaqueValue = dbObject.get(VALUE_KEY).value();
		return new OpaqueValue((Long) opaqueValue.get(TRANSACTION_ID),
				opaqueValue.get(CURRENT_VALUE_ID),
				opaqueValue.get(PREVIOUS_VALUE_ID));
	}

	public void bulkUpdateIndices(List<TridentTuple> inputs) {

		BulkRequestBuilder bulkRequest = client.prepareBulk();
		for (TridentTuple input : inputs) {
			Document<Map> doc = (Document<Map>) input
					.getValueByField("document");
			Map source = doc.getSource();
			IndexRequestBuilder request = client.prepareIndex(doc.getName(),
					doc.getType(), doc.getId()).setSource(source);

			if (doc.getParentId() != null) {
				request.setParent(doc.getParentId());
			}
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
}
