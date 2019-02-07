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
import org.apache.commons.lang.math.NumberUtils;
import org.apache.storm.trident.operation.Function;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.tuple.TridentTuple;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

public class CheckOperationAndValidateChildren implements Function {
	private static final Logger LOGGER = Logger
			.getLogger(CheckOperationAndValidateChildren.class.getName());
	public static final boolean LOG = false;

	private String traceKey, analyticsKey, weightsAnalyticsKey;

	/**
	 * Checks if the current node has operations, AND the children of this node
	 * are correctly referenced in the "weights" array AND extracts all the
	 * values possible from the CURRENT TRACE setting the "value" field in the
	 * "weights[i].children[j]"
	 * 
	 * @param traceKey
	 *            to get the current trace object for the analysis values
	 * @param analyticsKey
	 *            to get the current Analytics metadata object and obtain the
	 *            {@link TopologyBuilder#CHILDREN} and other values.
	 * @param weightsAnalyticsKey
	 *            to get the current Analytics metadata object and obtain the
	 *            {@link TopologyBuilder#OPERATION_CHILD_ID_KEY} and other
	 *            values for the analysis.
	 */
	public CheckOperationAndValidateChildren(String traceKey,
			String analyticsKey, String weightsAnalyticsKey) {
		this.traceKey = traceKey;
		this.analyticsKey = analyticsKey;
		this.weightsAnalyticsKey = weightsAnalyticsKey;
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

			Object analyticsObject = objects.getValueByField(analyticsKey);

			if (!(analyticsObject instanceof Map)) {
				if (LOG) {
					LOGGER.info(analyticsKey + " field of tuple " + objects
							+ " is not a map, found: " + analyticsObject);
				}
				return;
			}

			Map traceMap = (Map) traceObject;

			Object outObj = traceMap.get(TopologyBuilder.OUT_KEY);
			if (!(outObj instanceof Map)) {
				if (LOG) {
					LOGGER.info(TopologyBuilder.OUT_KEY + " field of tuple "
							+ traceMap + " is not a map, found: " + outObj);
				}
				return;
			}
			Map<String, Object> out = (Map) outObj;

			Map analyticsMap = (Map) analyticsObject;

			List children = (List) analyticsMap.get(TopologyBuilder.CHILDREN);

			Object weightsAnalyticsKeyObject = objects
					.getValueByField(weightsAnalyticsKey);

			if (!(weightsAnalyticsKeyObject instanceof Map)) {
				if (LOG) {
					LOGGER.info(weightsAnalyticsKey + " field of tuple "
							+ objects + " is not a map, found: "
							+ weightsAnalyticsKeyObject);
				}
				return;
			}

			Map weightsAnalyticsKeyMap = (Map) weightsAnalyticsKeyObject;

			Object weightsObject = weightsAnalyticsKeyMap
					.get(TopologyBuilder.WEIGHTS);

			if (!(weightsObject instanceof List)) {
				if (LOG) {
					LOGGER.info(TopologyBuilder.WEIGHTS + " field of tuple "
							+ weightsAnalyticsKeyMap
							+ " is not a List, found: " + weightsObject);
				}
				return;
			}

			List weights = (List) weightsObject;

			if (weights.size() == 0) {
				if (LOG) {
					LOGGER.info("Weights field of Map "
							+ weightsAnalyticsKeyMap
							+ " is empty -> DISCARDING ALL operationS!");
				}
				return;
			}

			Object childActId = traceMap
					.get(TopologyBuilder.CHILD_ACTIVITY_ID_KEY);
			if (childActId == null) {
				childActId = traceMap.get(TopologyBuilder.ORIGINAL_ID);
			}

			List discardedOperations = new ArrayList();
			for (int i = 0; i < weights.size(); ++i) {
				Object currentWeightObject = weights.get(i);
				if (!(currentWeightObject instanceof Map)) {
					if (LOG) {
						LOGGER.info(TopologyBuilder.WEIGHTS + " field of list "
								+ weights + " is not a Map, found: "
								+ currentWeightObject
								+ " -> DISCARDING operation!");
					}
					discardedOperations.add(currentWeightObject);
					continue;
				}

				Map currentOperation = (Map) currentWeightObject;

				// Check "name" is set correctly
				Object nameObject = currentOperation
						.get(TopologyBuilder.OPERATION_NAME_KEY);
				if (nameObject == null || nameObject.toString().isEmpty()) {
					if (LOG) {
						LOGGER.info(TopologyBuilder.OPERATION_NAME_KEY
								+ " field of Map " + currentOperation
								+ " is not a String, found: " + nameObject
								+ " -> DISCARDING operation!");
					}
					discardedOperations.add(currentWeightObject);
					continue;
				}

				// Check "op" is set correctly
				Object opObject = currentOperation
						.get(TopologyBuilder.OPERATION_KEY);
				if (opObject == null || opObject.toString().isEmpty()) {
					if (LOG) {
						LOGGER.info(TopologyBuilder.OPERATION_KEY
								+ " field of Map " + currentOperation
								+ " is not a String, found: " + opObject
								+ " -> DISCARDING operation!");
					}
					discardedOperations.add(currentWeightObject);
					continue;
				}

				// Check "children" is set correctly
				Object childrenObject = currentOperation
						.get(TopologyBuilder.OPERATION_CHILDREN_KEY);
				if (!(childrenObject instanceof List)) {
					if (LOG) {
						LOGGER.info(TopologyBuilder.OPERATION_CHILDREN_KEY
								+ " field of Map " + currentOperation
								+ " is not a List, found: " + childrenObject
								+ " -> DISCARDING operation!");
					}
					discardedOperations.add(currentWeightObject);
					continue;
				}

				List childrenList = (List) childrenObject;

				if (childrenList.size() == 0) {
					if (LOG) {
						LOGGER.info("Children field of Map " + currentOperation
								+ " is empty -> DISCARDING operation!");
					}
					discardedOperations.add(currentWeightObject);
					continue;
				}

				boolean opAndChildrenContinue = false;
				for (int j = 0; j < childrenList.size(); ++j) {
					Object currentChildObject = childrenList.get(j);

					if (!(currentChildObject instanceof Map)) {
						if (LOG) {
							LOGGER.info(TopologyBuilder.WEIGHTS
									+ " field of list " + childrenList
									+ " is not a Map, found: "
									+ currentChildObject
									+ " -> DISCARDING operation!");
						}
						discardedOperations.add(currentWeightObject);
						break;
					}

					Map currentChild = (Map) currentChildObject;

					// Check if child.name is a string
					Object childName = currentChild
							.get(TopologyBuilder.OPERATION_NAME_KEY);
					if (childName == null || childName.toString().isEmpty()) {
						if (LOG) {
							LOGGER.info(TopologyBuilder.OPERATION_NAME_KEY
									+ " field of Map " + currentChild
									+ " is not a String, found: " + childName
									+ " -> DISCARDING operation!");
						}
						discardedOperations.add(currentWeightObject);
						break;
					}

					String varName = childName.toString();

					// Check if child.id is a string
					Object childId = currentChild
							.get(TopologyBuilder.OPERATION_CHILD_ID_KEY);
					if (childId == null || childId.toString().isEmpty()) {
						if (LOG) {
							LOGGER.info(TopologyBuilder.OPERATION_CHILD_ID_KEY
									+ " field of Map " + currentChild
									+ " is not a String, found: " + childId
									+ " -> DISCARDING operation!");
						}
						discardedOperations.add(currentWeightObject);
						break;
					}

					// Check if child.multiplier exists
					Object childMultiplier = currentChild
							.get(TopologyBuilder.OPERATION_CHILD_MULTIPLIER_KEY);
					if (childMultiplier == null
							|| childMultiplier.toString().isEmpty()) {
						if (LOG) {
							LOGGER.info(TopologyBuilder.OPERATION_CHILD_MULTIPLIER_KEY
									+ " field of Map "
									+ childMultiplier
									+ " is not a String, found: "
									+ childMultiplier
									+ " -> DISCARDING operation!");
						}
						discardedOperations.add(currentWeightObject);
						break;
					}

					// Check if current child.id belongs to CHILDREN array of
					// current ANALYTICS meta-data (is a direct child)

					if (children.indexOf(childId) == -1) {
						if (LOG) {
							LOGGER.info("Child " + childId
									+ " is not part of children array, found: "
									+ children + " -> DISCARDING operation!");
						}
						discardedOperations.add(currentWeightObject);
						break;
					}

					// Check if current trace has in out {} (root) or out.ext {}
					// (extensions object) the "child.name" field
					// and update the child with "value" extracted from trace

					boolean valueFound = false;
					if (childId.equals(childActId)) {
						valueFound = traceHasValue(out, currentChild, varName);
					}
					if (valueFound) {
						opAndChildrenContinue = true;
					}
				}
				if (!opAndChildrenContinue) {
					if (LOG) {
						LOGGER.info("opAndChildrenContinue "
								+ opAndChildrenContinue
								+ " because no values have been found for current trace "
								+ traceMap + " -> DISCARDING operation!");
					}
					discardedOperations.add(currentWeightObject);
				}
			}

			weights.removeAll(discardedOperations);

			if (!weights.isEmpty()) {
				ArrayList<Object> object = new ArrayList<Object>(1);
				object.add(weights);

				collector.emit(object);
			}
		} catch (Exception ex) {
			LOGGER.info("Error unexpected exception, discarding"
					+ ex.toString());
			ex.printStackTrace();
		}
	}

	private boolean traceHasValue(Map<String, Object> out, Map child,
			String name) {

		boolean found = false;
		for (Map.Entry<String, Object> entry : out.entrySet()) {
			// System.out.println(entry.getKey() + "/" + entry.getValue());
			if (entry.getKey().equals(TopologyBuilder.EXTENSIONS_KEY)) {
				if (entry.getValue() instanceof Map) {
					Map<String, Object> ext = (Map) entry.getValue();
					for (Map.Entry<String, Object> extEntry : ext.entrySet()) {
						if (extEntry.getKey().equals(name)) {
							if (extEntry.getValue() != null
									&& NumberUtils.isNumber(extEntry.getValue()
											.toString())) {
								child.put(
										TopologyBuilder.OPERATION_CHILDREN_VALUE_KEY,
										extEntry.getValue());
								child.put(
										TopologyBuilder.OPERATION_CHILDREN_NEEDSUPDATE_KEY,
										true);
								found = true;
							}
						}
					}
				}
			} else if (entry.getKey().equals(name)) {
				if (entry.getValue() != null
						&& NumberUtils.isNumber(entry.getValue().toString())) {
					child.put(TopologyBuilder.OPERATION_CHILDREN_VALUE_KEY,
							entry.getValue());
					child.put(
							TopologyBuilder.OPERATION_CHILDREN_NEEDSUPDATE_KEY,
							true);
					found = true;
				}
			}
		}

		return found;
	}

	@Override
	public void prepare(Map map, TridentOperationContext tridentOperationContext) {

	}

	@Override
	public void cleanup() {

	}
}
