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
package es.eucm.rage.realtime.utils;

import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.client.transport.TransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helpful methods for connecting with the ElasticSearch database.
 */
public class DBUtils {

	private static final Logger LOG = LoggerFactory.getLogger(DBUtils.class);
	private static TransportClient client;

	/**
	 * Every time a new realtime session is started the auxiliary (opaque
	 * values) ElasticSearch index must be cleaned.
	 * 
	 * @param sessionId
	 */
	public static void startRealtime(String sessionId) {
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

	/**
	 * Return a simple ElasticSearch client on port 9300.
	 * 
	 * @param config
	 * @return
	 */
	public static TransportClient getClient(EsConfig config) {
		if (client == null) {
			client = new StormElasticSearchClient(config).construct();
		}
		return client;
	}

	/**
	 * Returns the name if the ElasticSearch index used to store the game play
	 * state per player used to display Alerts and Warnings to the teacher.
	 * 
	 * @param sessionId
	 * @return
	 */
	public static String getResultsIndex(String sessionId) {
		return "results-" + getTracesIndex(sessionId);
	}

	/**
	 * Returns the name of the ElasticSearch index used to store the "sanitized"
	 * traces used to display Kibana visualizations.
	 * 
	 * @param sessionId
	 * @return
	 */
	public static String getTracesIndex(String sessionId) {
		return sessionId.toLowerCase();
	}

	/**
	 * Returns the name of an auxiliary ElasticSearch index used to store the
	 * state of the topology for a correct functioning of the Storm Trident
	 * Opaque API.
	 * 
	 * @param sessionId
	 * @return
	 */
	public static String getOpaqueValuesIndex(String sessionId) {
		return "opaque-values-" + sessionId.toLowerCase();
	}
}
