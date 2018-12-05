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
package es.eucm.rage.realtime.utils;

import org.apache.http.HttpStatus;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.Map;

/**
 * Helpful methods for connecting with the ElasticSearch database.
 */
public class ESUtils {

	/**
	 * @return the EslasticSearch type of the traces stored inside the
	 *         {@link ESUtils#getTracesIndex(String)}
	 */
	public static String getTracesType() {
		return "traces";
	}

	/**
	 * @return the EslasticSearch type of the documents stored inside the
	 *         {@link ESUtils#getResultsIndex(String)}
	 */
	public static String getResultsType() {
		return "results";
	}

	/**
	 * @return the EslasticSearch type of the documents stored inside the
	 *         {@link ESUtils#getOpaqueValuesIndex(String)}
	 */
	public static String getOpaqueValuesType() {
		return "opaquevalues";
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
	 * Returns the name if the ElasticSearch index used to store all the
	 * Analytics for the current GLP
	 * 
	 * @param rootGlpId
	 *            is the id of the root element of the GLP
	 * @return
	 */
	public static String getAnalyticsGLPIndex(String rootGlpId) {
		return "analytics-" + rootGlpId;
	}

	/**
	 * Returns the ID of the root object in the glpIndex (does the opposite of
	 * ESUtils#getAnalyticsGLPIndex
	 * 
	 * @param glpIndex
	 *            is the id of the index where the analyticks tree is stored
	 * @return
	 */
	public static String getRootGLPId(String glpIndex) {
		String ret = glpIndex;
		if (ret.startsWith("analytics-")) {
			ret = ret.substring("analytics-".length());
		}
		return ret;
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

	/**
	 * Tries to reopen the index if the exception is of type
	 * index_closed_exception
	 */
	public static void reopenIndexIfNeeded(ElasticsearchStatusException e,
			String index, Logger LOG, RestClient _client) {
		if (e.getDetailedMessage().contains("index_closed_exception")) {
			LOG.error("ElasticsearchStatusException", e);
			try {
				Response resultResponse = _client.performRequest("POST", "/"
						+ index + "/_open");
				int resultStatus = resultResponse.getStatusLine()
						.getStatusCode();

				LOG.info("OPENING!!!!!!!!! " + resultStatus);

				if (resultStatus != HttpStatus.SC_OK) {
					LOG.error("Could not open the closed index " + index
							+ ", status " + resultStatus);
				}
			} catch (IOException e1) {
				LOG.error("Could not open the closed index ", e1);
			}
		}
	}
}
