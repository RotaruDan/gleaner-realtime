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
package es.eucm.rage.realtime.simple.topologies;

import es.eucm.rage.realtime.states.elasticsearch.EsState;
import es.eucm.rage.realtime.utils.ESUtils;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.state.StateUpdater;
import org.apache.storm.trident.tuple.TridentTuple;

import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

public class PerformOperationUpdater implements StateUpdater<EsState> {
	private static final Logger LOGGER = Logger
			.getLogger(PerformOperationUpdater.class.getName());
	private static final boolean LOG = false;

	// Performs the "weights_full" object operation and UPDATES the result
	// into
	// agg_activityId_username AND
	// Updates the values that have been received (attribute
	// weights[i].children[j].needsUpdate == true), to the last values
	// Requires ACTIVITY_ID + WEIGHTS_full + TRACE_HEY

	@Override
	public void updateState(EsState state, List<TridentTuple> tuples,
			TridentCollector collector) {
		try {
			for (TridentTuple tuple : tuples) {

				List weights = (List) tuple
						.getValueByField(TopologyBuilder.WEIGHTS + "_full");

				String glpId = tuple
						.getStringByField(TopologyBuilder.GLP_ID_KEY);
				String resultsRootGlpId = ESUtils.getRootGLPId(glpId);
				String activityId = tuple
						.getStringByField(TopologyBuilder.ACTIVITY_ID_KEY);
				String name = tuple
						.getStringByField(es.eucm.rage.realtime.topologies.TopologyBuilder.TridentTraceKeys.NAME);

				for (Object weightObj : weights) {

					if (!(weightObj instanceof Map)) {
						if (LOG) {
							LOGGER.info("weight has not been found for current weights "
									+ weights
									+ " -> DISCARDING! or is not instance of Map "
									+ weightObj);
						}
						continue;
					}

					Map<String, Object> weight = (Map) weightObj;

					Object nameObj = weight
							.get(TopologyBuilder.OPERATION_NAME_KEY);
					if (nameObj == null) {
						if (LOG) {
							LOGGER.info("nameObj has not been found for current weight "
									+ weight + " -> DISCARDING!");
						}
						continue;
					}

					String varName = nameObj.toString();

					Object opObj = weight.get(TopologyBuilder.OPERATION_KEY);
					if (opObj == null) {
						if (LOG) {
							LOGGER.info("opObj has not been found for current weight "
									+ weight + " -> DISCARDING!");
						}
						continue;
					}

					// '+' or '*'
					String op = opObj.toString();

					Object childrenObj = weight
							.get(TopologyBuilder.OPERATION_CHILDREN_KEY);
					if (!(childrenObj instanceof List)) {
						if (LOG) {
							LOGGER.info("childrenObj has not been found for current weight "
									+ weight
									+ " -> DISCARDING! or is not instance of List "
									+ childrenObj);
						}
						continue;
					}

					float childResult = Float.NaN;
					List children = (List) childrenObj;
					for (Object childObj : children) {
						if (!(childObj instanceof Map)) {
							if (LOG) {
								LOGGER.info("childObj has not been found for current child "
										+ children
										+ " -> DISCARDING! or is not instance of Map "
										+ childObj);
							}
							continue;
						}

						Map<String, Object> child = (Map) childObj;

						Object multiplyerObj = child
								.get(TopologyBuilder.OPERATION_CHILD_MULTIPLIER_KEY);
						if (multiplyerObj == null) {
							if (LOG) {
								LOGGER.info("multiplyerObj has not been found for current child "
										+ child + " -> DISCARDING!");
							}
							continue;
						}

						float multiplyer;
						try {
							multiplyer = Float.parseFloat(multiplyerObj
									.toString());
						} catch (Exception ex) {
							if (LOG) {
								LOGGER.info("Error parsing multiplyerObj to float "
										+ multiplyerObj + " -> DISCARDING!");
							}
							continue;
						}

						Object valueObj = child
								.get(TopologyBuilder.OPERATION_CHILDREN_VALUE_KEY);

						if (valueObj == null) {
							if (LOG) {
								LOGGER.info("Error parsing valueObj, is null -> DISCARDING!");
							}
							continue;
						}

						float value;
						try {
							value = Float.parseFloat(valueObj.toString());
						} catch (Exception ex) {
							if (LOG) {
								LOGGER.info("Error parsing valueObj to float "
										+ valueObj + " -> DISCARDING!");
							}
							continue;
						}

						float multiplyedVal = value * multiplyer;
						if (Float.isNaN(childResult)) {
							childResult = multiplyedVal;
						} else if (op.equalsIgnoreCase("+")) {
							childResult += multiplyedVal;
						} else {
							childResult *= multiplyedVal;
						}

						Object needsUpdateObj = child
								.get(TopologyBuilder.OPERATION_CHILDREN_NEEDSUPDATE_KEY);

						if (needsUpdateObj != null) {

							boolean needsUpdate = false;
							try {
								needsUpdate = Boolean
										.parseBoolean(needsUpdateObj.toString());

							} catch (Exception booleanEx) {
								if (LOG) {
									LOGGER.info("Error parsing needsUpdateObj to boolean "
											+ needsUpdateObj
											+ " -> DISCARDING!");
								}
							}

							if (needsUpdate) {

								Object idObj = child
										.get(TopologyBuilder.OPERATION_CHILD_ID_KEY);
								if (idObj != null) {
									String id = idObj.toString();

									Object childVarNameObj = child
											.get(TopologyBuilder.OPERATION_NAME_KEY);
									if (childVarNameObj != null) {

										String childVarName = childVarNameObj
												.toString();

										state.setProperty(resultsRootGlpId,
												"agg_" + id + "_" + name,
												childVarName, value);
									} else {
										if (LOG) {
											LOGGER.info("childVarNameObj has not been found for current child "
													+ child + " -> DISCARDING!");
										}
									}
								} else {
									if (LOG) {
										LOGGER.info("idObj has not been found for current child "
												+ child + " -> DISCARDING!");
									}
								}
							}
						}
					}

					state.setProperty(resultsRootGlpId, "agg_" + activityId
							+ "_" + name, varName, childResult);
				}
			}
		} catch (Exception ex) {
			LOGGER.info("Error unexpected exception, discarding "
					+ ex.toString());
			ex.printStackTrace();
		}
	}

	@Override
	public void prepare(Map conf, TridentOperationContext context) {

	}

	@Override
	public void cleanup() {

	}
}
