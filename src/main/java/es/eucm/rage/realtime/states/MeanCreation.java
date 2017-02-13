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
package es.eucm.rage.realtime.states;

import es.eucm.rage.realtime.utils.DBUtils;
import es.eucm.rage.realtime.utils.Document;
import org.apache.storm.trident.operation.Function;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.tuple.TridentTuple;

import java.util.ArrayList;
import java.util.Date;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Storm Trident function for building a {@link Document} object with the
 * information required to display Kibana bisualization.
 */
public class MeanCreation implements Function {

	private static final Logger LOG = Logger.getLogger(MeanCreation.class
			.getName());

	private final String tracesIndex;
	private final String defaultTraceKey;

	/**
	 * Builds a {@link Document} from a TridentTouple. The trace is sanitized
	 * before being persisted. The {@link Document} is designed to be persisted
	 * in ElasticSearch.
	 *
	 * @param sessionId
	 * @param defaultTraceKey
	 */
	public MeanCreation(String sessionId, String defaultTraceKey) {
		this.tracesIndex = DBUtils.getTracesIndex(sessionId);
		this.defaultTraceKey = defaultTraceKey;
	}

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {

		Map trace = (Map) tuple.getValueByField(defaultTraceKey);

		Map resultTraces = buildTrace(trace);

		Document<Map> doc = new Document(tracesIndex,
				ESGameplayState.RAGE_TRACES_DOCUMENT_TYPE, resultTraces, null);

		ArrayList<Object> object = new ArrayList<Object>(1);
		object.add(doc);

		collector.emit(object);
	}

	/**
	 * Sanitizes some fields dds basic trace values useful for the Kibana
	 * visualizations:
	 * 
	 * -> "stored": timestamp, -> sanitizes "score" to be a float field that can
	 * be used in the Y-axis of Kibana visualizations -> sanitizes "progress" to
	 * be a float field that can be used in the Y-axis of Kibana visualizations
	 * -> sanitizes "health" to be a float field that can be used in the Y-axis
	 * of Kibana visualizations -> sanitizes "success" to be a boolean field ->
	 * adds hash codes for "gameplayId", "event", "type" and "target" in case
	 * they are needed to be used in the Y-axis of Kibana visualizations
	 * 
	 * @param trace
	 * @return
	 */
	private Map buildTrace(Map trace) {
		trace.put(ESGameplayState.STORED_KEY, new Date());

		Object score = trace.get("score");
		if (score != null) {
			if (score instanceof String) {
				try {
					float finalScore = Float.valueOf(score.toString());
					trace.put("score", finalScore);
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

		Object success = trace.get("success");

		if (success != null) {
			if (success instanceof String) {
				boolean finalSuccess;
				if (success.toString().equalsIgnoreCase("true")) {
					finalSuccess = true;
				} else {
					finalSuccess = false;
				}
				trace.put("success", finalSuccess);
			}
		}

		Object gameplayId = trace.get("gameplayId");
		if (gameplayId != null) {
			trace.put("gameplayId_hashCode", gameplayId.hashCode());
		}

		Object event = trace.get("event");
		if (event != null) {
			trace.put("event_hashCode", event.hashCode());
		}

		Object type = trace.get("type");
		if (type != null) {
			trace.put("type_hashCode", type.hashCode());
		}

		Object target = trace.get("target");
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
