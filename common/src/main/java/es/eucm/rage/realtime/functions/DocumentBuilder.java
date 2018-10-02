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
package es.eucm.rage.realtime.functions;

import es.eucm.rage.realtime.topologies.TopologyBuilder;
import es.eucm.rage.realtime.utils.Document;
import org.apache.storm.trident.operation.Function;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.tuple.TridentTuple;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Storm Trident function for building a {@link Document} object with the
 * information required to display Kibana bisualization.
 */
public class DocumentBuilder implements Function {
	private static final Logger LOG = Logger.getLogger(DocumentBuilder.class
			.getName());

	private final String defaultTraceKey;
	private final String indexIdKey;

	/**
	 * Builds a {@link Document} from a TridentTouple. The trace is sanitized
	 * before being persisted. The {@link Document} is designed to be persisted
	 * in ElasticSearch.
	 * 
	 * @param defaultTraceKey
	 */
	public DocumentBuilder(String defaultTraceKey) {
		this(defaultTraceKey, TopologyBuilder.ACTIVITY_ID_KEY);
	}

	/**
	 * Builds a {@link Document} from a TridentTouple. The trace is sanitized
	 * before being persisted. The {@link Document} is designed to be persisted
	 * in ElasticSearch.
	 * 
	 * @param defaultTraceKey
	 */
	public DocumentBuilder(String defaultTraceKey, String indexIdKey) {
		this.indexIdKey = indexIdKey;
		this.defaultTraceKey = defaultTraceKey;
	}

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		try {
			Map trace = (Map) tuple.getValueByField(defaultTraceKey);

			Map resultTraces = buildTrace(trace);

			if (resultTraces == null) {
				return;
			}

			String index = null;

			Object indexObj = trace.get(indexIdKey);

			if (indexObj != null && (indexObj instanceof String)) {
				index = indexObj.toString();
			}

			Document<Map> doc = new Document(resultTraces, null, null, null,
					index);

			ArrayList<Object> object = new ArrayList<Object>(1);
			object.add(doc);

			collector.emit(object);
		} catch (Exception ex) {
			LOG.info("Error unexpected exception, discarding" + ex.toString());
		}
	}

	/**
	 * Sanitizes some fields ads basic trace values useful for the Kibana
	 * visualizations:
	 * <p>
	 * -> "stored": timestamp, -> sanitizes "score" to be a float field that can
	 * be used in the Y-axis of Kibana visualizations -> sanitizes "progress" to
	 * be a float field that can be used in the Y-axis of Kibana visualizations
	 * -> sanitizes "health" to be a float field that can be used in the Y-axis
	 * of Kibana visualizations -> sanitizes "success" to be a boolean field ->
	 * adds hash codes for "gameplayId", "event", "type" and "target" in case
	 * they are needed to be used in the Y-axis of Kibana visualizations
	 * 
	 * @param inputTrace
	 * @return
	 */
	private Map buildTrace(Map inputTrace) {

		Object out = inputTrace.get(TopologyBuilder.OUT_KEY);

		if (out == null) {
			return null;
		}

		if (!(out instanceof Map)) {
			return null;
		}

		Map outMap = (Map) out;

		Map trace = new HashMap(outMap);
		trace.put(TopologyBuilder.TridentTraceKeys.STORED, new Date());

		Object score = trace.get(TopologyBuilder.TridentTraceKeys.SCORE);
		if (score != null) {
			if (score instanceof String) {
				try {
					float finalScore = Float.valueOf(score.toString());
					trace.put(TopologyBuilder.TridentTraceKeys.SCORE,
							finalScore);
				} catch (NumberFormatException numberFormatException) {
					LOG.info("Error parsing score to float: "
							+ numberFormatException.getMessage());
				}
			}
		}

		Object progress = trace.get("progress");
		if (progress != null) {
			if (progress instanceof String) {
				try {
					float finalProgress = Float.valueOf(progress.toString());
					trace.put("progress", finalProgress);
				} catch (NumberFormatException numberFormatException) {
					LOG.info("Error parsing progress to float: "
							+ numberFormatException.getMessage());
				}
			} else if (progress instanceof Long) {
				try {
					float finalProgress = Float.valueOf((Long) progress);
					trace.put("progress", finalProgress);
				} catch (NumberFormatException numberFormatException) {
					LOG.info("Error parsing progress to float: "
							+ numberFormatException.getMessage());
				}
			}
		}

		Object health = trace.get("health");
		if (health != null) {
			if (health instanceof String) {
				try {
					float finalHealth = Float.valueOf(health.toString());
					trace.put("health", finalHealth);
				} catch (NumberFormatException numberFormatException) {
					LOG.info("Error parsing health to float: "
							+ numberFormatException.getMessage());
				}
			}
		}

		Object time = trace.get("time");
		if (time != null) {
			if (time instanceof String) {
				try {
					float finalTime = Float.valueOf(time.toString());
					trace.put("time", finalTime);
				} catch (NumberFormatException numberFormatException) {
					LOG.info("Error parsing time to float: "
							+ numberFormatException.getMessage());
				}
			}
		}

		Object success = trace.get(TopologyBuilder.TridentTraceKeys.SUCCESS);

		if (success != null) {
			if (success instanceof String) {
				boolean finalSuccess;
				if (success.toString().equalsIgnoreCase("true")) {
					finalSuccess = true;
				} else {
					finalSuccess = false;
				}
				trace.put(TopologyBuilder.TridentTraceKeys.SUCCESS,
						finalSuccess);
			}
		}

		Object event = trace.get(TopologyBuilder.TridentTraceKeys.EVENT);
		if (event != null) {
			trace.put("event_hashCode", event.hashCode());
		}

		Object type = trace.get(TopologyBuilder.TridentTraceKeys.TYPE);
		if (type != null) {
			trace.put("type_hashCode", type.hashCode());
		}

		Object target = trace.get(TopologyBuilder.TridentTraceKeys.TARGET);
		if (target != null) {
			trace.put("target_hashCode", target.hashCode());
		}

		Map result = new HashMap<>(inputTrace);
		result.put(TopologyBuilder.OUT_KEY, trace);

		return result;
	}

	@Override
	public void prepare(Map conf, TridentOperationContext context) {

	}

	@Override
	public void cleanup() {

	}
}
