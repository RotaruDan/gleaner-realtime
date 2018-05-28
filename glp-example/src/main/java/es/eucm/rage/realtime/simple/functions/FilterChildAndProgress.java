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
package es.eucm.rage.realtime.simple.functions;

import com.rits.cloning.Cloner;
import es.eucm.rage.realtime.simple.topologies.GLPTopologyBuilder;
import es.eucm.rage.realtime.topologies.TopologyBuilder;
import org.apache.storm.trident.operation.Filter;
import org.apache.storm.trident.operation.Function;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.tuple.TridentTuple;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

public class FilterChildAndProgress implements Function {
	private static final Logger LOGGER = Logger
			.getLogger(FilterChildAndProgress.class.getName());
	public static final boolean LOG = true;

	private String analyticsKey, traceKey;

	private Cloner cloner;

	/**
	 * Filters a Trace TridentTuple depending if it is achild of the current
	 * analytics
	 */
	public FilterChildAndProgress(String analyticsKey, String traceKey) {
		this.analyticsKey = analyticsKey;
		this.traceKey = traceKey;
	}

	@Override
	public void execute(TridentTuple objects, TridentCollector collector) {
		try {
			Object traceObject = objects.getValueByField(traceKey);

			if (!(traceObject instanceof Map)) {
				if (LOG) {
					LOGGER.info(traceKey + " field of tuple " + objects
							+ " is not a map, found: " + traceObject);
				}
				return;
			}

			Map trace = (Map) traceObject;
			Object childIdObject = trace
					.get(TopologyBuilder.CHILD_ACTIVITY_ID_KEY);

			if (childIdObject == null) {
				if (LOG) {
					LOGGER.info(TopologyBuilder.CHILD_ACTIVITY_ID_KEY
							+ " field of tuple " + objects + " is null");
				}
				return;
			}

			String childId = childIdObject.toString();

			Object analyticsObject = objects.getValueByField(analyticsKey);

			if (!(analyticsObject instanceof Map)) {
				if (LOG) {
					LOGGER.info(analyticsKey + " field of tuple " + objects
							+ " is not a map, found: " + analyticsObject);
				}
				return;
			}

			Map analytics = (Map) analyticsObject;
			Object childrenObject = analytics.get(GLPTopologyBuilder.CHILDREN);

			if (!(childrenObject instanceof List)) {
				if (LOG) {
					LOGGER.info(GLPTopologyBuilder.CHILDREN
							+ " field of tuple " + objects
							+ " is not an Array, found: " + childrenObject);
				}
				return;
			}

			List children = (List) childrenObject;

			for (int i = 0; i < children.size(); ++i) {
				Object childObj = children.get(i);
				if (childObj != null) {
					if (childObj.toString().equals(childId)) {

						// It's a child of the current parent

						// Compute progress
						Map progressedTrace = cloner.deepClone(trace);
						Map outValues = (Map) progressedTrace
								.get(TopologyBuilder.OUT_KEY);

						outValues.put(TopologyBuilder.TridentTraceKeys.EVENT,
								TopologyBuilder.TraceEventTypes.PROGRESSED);

						Map extensions = new HashMap(1);
						outValues.put(TopologyBuilder.EXTENSIONS_KEY,
								extensions);

						float value = 0;
						Object fullCompletedObj = analytics
								.get(GLPTopologyBuilder.ANALYTICS_FULL_COMPLETED);
						if (fullCompletedObj instanceof Map) {

							Map fullCompleted = (Map) fullCompletedObj;
							Object completedChildrenObject = fullCompleted
									.get(outValues
											.get(TopologyBuilder.TridentTraceKeys.NAME));
							if (completedChildrenObject instanceof List) {
								List completedChildten = (List) completedChildrenObject;
								int num = completedChildten.size();
								if (!completedChildten.contains(trace
										.get(TopologyBuilder.ORIGINAL_ID))) {
									num++;
								}
								value = (float) num / (float) children.size();
							} else {
								value = 1f / (float) children.size();
							}
						} else if (fullCompletedObj == null) {
							value = 1f / (float) children.size();
						}

						extensions.put(
								TopologyBuilder.TridentTraceKeys.PROGRESS,
								value);

						Object newTarget = analytics
								.get(TopologyBuilder.ANALYTICS_NAME);
						if (newTarget == null) {
							newTarget = trace
									.get(TopologyBuilder.ACTIVITY_ID_KEY);
						}
						outValues.put(TopologyBuilder.TridentTraceKeys.TARGET,
								newTarget);

						outValues
								.remove(TopologyBuilder.TridentTraceKeys.SUCCESS);
						collector.emit(Arrays.asList(progressedTrace));

						// Compute completion for current parent

						if (value == 1) {

							Map completedTrace = cloner.deepClone(trace);
							Map outCompValues = (Map) completedTrace
									.get(TopologyBuilder.OUT_KEY);

							outCompValues.put(
									TopologyBuilder.TridentTraceKeys.EVENT,
									TopologyBuilder.TraceEventTypes.COMPLETED);
							outCompValues.put(
									TopologyBuilder.TridentTraceKeys.TARGET,
									newTarget);
							outCompValues.put(TopologyBuilder.EXTENSIONS_KEY,
									new HashMap());
							// TODO check with real score
							outCompValues.put(
									TopologyBuilder.TridentTraceKeys.SUCCESS,
									true);

							collector.emit(Arrays.asList(completedTrace));
						}
					}
				}
			}

			return;
		} catch (Exception ex) {
			if (LOG) {
				LOGGER.info("Error unexpected exception, discarding "
						+ ex.toString());
				ex.printStackTrace();
			}
			return;
		}
	}

	@Override
	public void prepare(Map conf,
			TridentOperationContext tridentOperationContext) {
		this.cloner = new Cloner();
	}

	@Override
	public void cleanup() {

	}
}
