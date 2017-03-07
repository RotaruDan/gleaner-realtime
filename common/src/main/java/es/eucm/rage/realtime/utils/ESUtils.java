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
