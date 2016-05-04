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
package es.eucm.gleaner.realtime.utils;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.client.transport.TransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.UnknownHostException;
import java.util.Map;

public class DBUtils {

	private static final Logger LOG = LoggerFactory.getLogger(DBUtils.class);
	private static TransportClient client;

	public static DB getMongoDB(Map<String, Object> conf) {
		try {
			return new MongoClient((String) conf.get("mongoHost"),
					((Number) conf.get("mongoPort")).intValue())
					.getDB((String) conf.get("mongoDB"));
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		return null;
	}

	public static DBCollection getMongoCollection(Map conf,
			String collectionName) {
		return getMongoDB(conf).getCollection(collectionName);
	}

	public static DBCollection getRealtimeResults(DB db, String version) {
		return db.getCollection("session" + version);
	}

	public static DBCollection getOpaqueValues(DB db, String version) {
		return db.getCollection("session_opaque_values_" + version);
	}

	public static void startRealtime(DB db, String sessionId) {

		// Mongo DB Collection Drop
		getOpaqueValues(db, sessionId).drop();
		getOpaqueValues(db, sessionId).createIndex(new BasicDBObject("key", 1));

		// ElasticSearch Index Deletion
		if (client != null) {
			String opaqueValuesIndex = getOpaqueValuesIndex(sessionId);
			DeleteIndexResponse delete = client.admin().indices()
					.delete(new DeleteIndexRequest(opaqueValuesIndex))
					.actionGet();
			if (!delete.isAcknowledged()) {
				LOG.error("Index wasn't deleted for session " + sessionId);
			}

			client.admin().indices()
					.flush(new FlushRequest(opaqueValuesIndex).force(true))
					.actionGet();
		}
	}

	public static TransportClient getClient(EsConfig config) {
		if (client == null) {
			client = new StormElasticSearchClient(config).construct();
		}
		return client;
	}

	public static String getResultsIndex(String sessionId) {
		return "results-" + sessionId.toLowerCase();
	}

	public static String getTracesIndex(String sessionId) {
		return "traces-" + sessionId.toLowerCase();
	}

	public static String getOpaqueValuesIndex(String sessionId) {
		return "opaque-values-" + sessionId.toLowerCase();
	}
}
