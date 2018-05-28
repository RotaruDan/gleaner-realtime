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

import com.esotericsoftware.minlog.Log;
import com.google.gson.Gson;
import es.eucm.rage.realtime.topologies.TopologyBuilder;
import es.eucm.rage.realtime.utils.Document;
import org.apache.storm.trident.TridentTopology;
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
 * Storm Trident function for building a {@link Map} object with the information
 * required to display Kibana bisualization.
 */
public class TraceToParentBuilder implements Function {
	private static final Logger LOG = Logger
			.getLogger(TraceToParentBuilder.class.getName());
	private static final boolean LOGGING = false;

	private final String defaultTraceKey;
	private final String analyticsKey;
	private Gson gson;

	/**
	 * Builds a {@link Map} from a TridentTouple. The trace is designed to be
	 * sent to kafka.
	 * 
	 * @param defaultTraceKey
	 */
	public TraceToParentBuilder(String defaultTraceKey, String analyticsKey) {
		this.defaultTraceKey = defaultTraceKey;
		this.analyticsKey = analyticsKey;
	}

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		try {
			Map inTrace = (Map) tuple.getValueByField(defaultTraceKey);

			Map trace = new HashMap(inTrace);
			Map analytics = (Map) tuple.getValueByField(analyticsKey);

			Object parentIdObj = analytics
					.get(TopologyBuilder.ANALYTICS_PARENT_ID_KEY);
			if (parentIdObj == null) {
				if (LOGGING) {
					Log.info("Analytics has no parent, reached top level GLP Root");
				}
				return;
			}

			Object origId = trace.get(TopologyBuilder.ORIGINAL_ID);
			if (origId == null) {
				origId = trace.get(TopologyBuilder.ACTIVITY_ID_KEY);
				trace.put(TopologyBuilder.ORIGINAL_ID, origId);
			}

			Object traceOriginalName = trace
					.get(TopologyBuilder.TRACE_ANALYTICS_ORIGINAL_NAME);
			if (traceOriginalName == null) {
				Object originalNameObject = analytics
						.get(TopologyBuilder.ANALYTICS_NAME);
				if (originalNameObject == null) {
					originalNameObject = origId;
				}
				trace.put(TopologyBuilder.TRACE_ANALYTICS_ORIGINAL_NAME,
						originalNameObject);
			}

			String parentId = parentIdObj.toString();
			trace.put(TopologyBuilder.CHILD_ACTIVITY_ID_KEY,
					trace.get(TopologyBuilder.ACTIVITY_ID_KEY));
			trace.put(TopologyBuilder.ACTIVITY_ID_KEY, parentId);

			ArrayList<Object> object = new ArrayList<Object>(1);
			object.add(gson.toJson(trace, Map.class));

			collector.emit(object);
		} catch (Exception ex) {
			if (LOGGING) {
				LOG.info("Error unexpected exception, discarding, "
						+ ex.getMessage() + ", " + ex.getCause());
				ex.printStackTrace();
			}
		}
	}

	@Override
	public void prepare(Map conf, TridentOperationContext context) {
		gson = new Gson();
	}

	@Override
	public void cleanup() {

	}
}
