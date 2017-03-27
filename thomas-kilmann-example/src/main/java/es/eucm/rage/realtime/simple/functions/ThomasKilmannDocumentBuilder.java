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
package es.eucm.rage.realtime.simple.functions;

import es.eucm.rage.realtime.simple.topologies.ThomasKilmannTopologyBuilder;
import es.eucm.rage.realtime.topologies.TopologyBuilder;
import es.eucm.rage.realtime.utils.Document;
import org.apache.storm.trident.operation.Function;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.tuple.TridentTuple;

import java.util.*;
import java.util.function.Consumer;
import java.util.logging.Logger;

/**
 * Storm Trident function for building a {@link Document} object with the
 * information required to display Kibana bisualization.
 */
public class ThomasKilmannDocumentBuilder implements Function {
	private static final Logger LOG = Logger
			.getLogger(ThomasKilmannDocumentBuilder.class.getName());

	private final String defaultTraceKey;

	/**
	 * Builds a {@link Document} from a TridentTouple. The trace is sanitized
	 * before being persisted. The {@link Document} is designed to be persisted
	 * in ElasticSearch.
	 * 
	 * @param defaultTraceKey
	 */
	public ThomasKilmannDocumentBuilder(String defaultTraceKey) {
		this.defaultTraceKey = defaultTraceKey;
	}

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {

		Map trace = (Map) tuple.getValueByField(defaultTraceKey);

		Map resultTracee = buildTrace(trace);

		Object biasExtObject = ((Map) resultTracee
				.get(TopologyBuilder.EXTENSIONS_KEY))
				.get(ThomasKilmannTopologyBuilder.BIASES_KEY);

		if (!(biasExtObject instanceof Map)) {
			LOG.info(ThomasKilmannTopologyBuilder.BIASES_KEY
					+ " extension is not a Map, found: " + biasExtObject);
			return;
		}

		Map<String, Boolean> biases = (Map<String, Boolean>) biasExtObject;

		for (Map.Entry<String, Boolean> entry : biases.entrySet()) {
			Map newTrace = new HashMap<>(resultTracee);
			newTrace.put(ThomasKilmannTopologyBuilder.BIAS_TYPE_KEY,
					entry.getKey());
			if (entry.getValue()) {
				newTrace.put(ThomasKilmannTopologyBuilder.BIAS_VALUE_TRUE_KEY,
						1);
			} else {
				newTrace.put(ThomasKilmannTopologyBuilder.BIAS_VALUE_FALSE_KEY,
						1);
			}

			Document<Map> doc = new Document(newTrace, null,
					ThomasKilmannTopologyBuilder.THOMAS_KILMANN_KEY,
					ThomasKilmannTopologyBuilder.THOMAS_KILMAN_INDEX_PREFIX);

			ArrayList<Object> object = new ArrayList<Object>(1);
			object.add(doc);

			collector.emit(object);
			return;
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
	 * <p>
	 * Also per each Extension found of type "bias" it creates a new trace with
	 * only that extension and the following fields:
	 * 
	 * @param inputTrace
	 * @return
	 */
	private Map buildTrace(Map inputTrace) {
		Map trace = new HashMap(inputTrace);
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

		Object gameplayId = trace
				.get(TopologyBuilder.TridentTraceKeys.GAMEPLAY_ID);
		if (gameplayId != null) {
			trace.put("gameplayId_hashCode", gameplayId.hashCode());
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

		return trace;
	}

	@Override
	public void prepare(Map conf, TridentOperationContext context) {

	}

	@Override
	public void cleanup() {

	}
}
