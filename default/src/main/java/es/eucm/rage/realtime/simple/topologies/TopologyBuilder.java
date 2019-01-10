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

import es.eucm.rage.realtime.simple.filters.*;
import es.eucm.rage.realtime.functions.*;
import es.eucm.rage.realtime.functions.DocumentBuilder;
import es.eucm.rage.realtime.states.GameplayStateUpdater;
import es.eucm.rage.realtime.states.TraceStateUpdater;

import es.eucm.rage.realtime.states.elasticsearch.EsState;
import es.eucm.rage.realtime.utils.ESUtils;
import org.apache.storm.kafka.trident.OpaqueTridentKafkaSpout;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentState;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.trident.state.StateFactory;
import org.apache.storm.trident.state.StateUpdater;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

/**
 * RAGE Analytics implementation of
 * {@link es.eucm.rage.realtime.topologies.TopologyBuilder} performing the
 * real-time analysis. Check out:
 * https://github.com/e-ucm/rage-analytics/wiki/Understanding
 * -RAGE-Analytics-Traces-Flow
 */
public class TopologyBuilder implements
		es.eucm.rage.realtime.topologies.TopologyBuilder {

	private String o(String key) {
		return OUT_KEY + "." + key;
	}

	@Override
	public void build(TridentTopology tridentTopology,
			OpaqueTridentKafkaSpout spout, Stream tracesStream,
			StateFactory partitionPersistFactory,
			StateFactory persistentAggregateFactory, Map<String, Object> conf) {

		GameplayStateUpdater gameplayStateUpdater = new GameplayStateUpdater();

		/** ---> AbstractAnalysis definition <--- **/

		/*
		 * --> Analyzing for Kibana visualizations (traces index, 'sessionId')
		 * <--
		 */

		// 1 - For each TRACE_KEY (from Kibana) that we receive
		// 2 - Create an ElasticSearch "sanitized" document identified as
		// "document"
		// 3 - Finally persist the "document" to the SESSION_ID_KEY
		// ElasticSearch
		// index
		tracesStream
				// Filter only leafs
				.each(new Fields(TRACE_KEY), new DocumentBuilder(TRACE_KEY),
						new Fields(DOCUMENT_KEY))
				.peek(new LogConsumer("Directly to Kibana!"))
				.partitionPersist(partitionPersistFactory,
						new Fields(DOCUMENT_KEY), new TraceStateUpdater());

		// 1 - For each TRACE_KEY (from Kibana) that we receive
		// 2 - Create an ElasticSearch "sanitized" document identified as
		// "document"
		// 3 - Finally persist the "document" to the CLASS_ID
		// ElasticSearch
		// index
		tracesStream
				.each(new Fields(TRACE_KEY), new HasClassAttributes())
				.each(new Fields(TRACE_KEY),
						new DocumentBuilder(TRACE_KEY, CLASS_ID),
						new Fields(DOCUMENT_KEY))
				.peek(new LogConsumer("Directly to Kibana!"))
				.partitionPersist(partitionPersistFactory,
						new Fields(DOCUMENT_KEY), new TraceStateUpdater());

		/*
		 * --> Analyzing for the Alerts and Warnings system (results index,
		 * 'results-sessionId') <--
		 */

		// 1 - For each TRACE_KEY (from Kibana) that we receive
		// 2 - Extract the fields TridentTraceKeys.GAMEPLAY_ID and
		// TridentTraceKeys.EVENT so that we can play
		// with it below

		Stream gameplayIdStream = tracesStream
				.each(new Fields(TRACE_KEY),
						new TraceFieldExtractor(ACTIVITY_ID_KEY, GAMEPLAY_ID,
								o(TridentTraceKeys.NAME),
								o(TridentTraceKeys.EVENT)),
						new Fields(ACTIVITY_ID_KEY, GAMEPLAY_ID,
								TridentTraceKeys.NAME, TridentTraceKeys.EVENT))
				.each(new Fields(TRACE_KEY), new GlpIdFieldExtractor(),
						new Fields(GLP_ID_KEY)).peek(new LogConsumer("1"));

		// 3 - For each TRACE_KEY (from Kibana) that we receive
		// 4 - Extract the field 'timestamp' and add it to the document per
		// 'gameplayId' (player)
		gameplayIdStream
				.each(new Fields(TRACE_KEY),
						new TraceFieldExtractor(o(TridentTraceKeys.TIMESTAMP)),
						new Fields(TridentTraceKeys.TIMESTAMP))
				.each(new Fields(TridentTraceKeys.TIMESTAMP),
						new SimplePropertyCreator(TridentTraceKeys.TIMESTAMP,
								TridentTraceKeys.TIMESTAMP),
						new Fields(PROPERTY_KEY, VALUE_KEY))
				.peek(new LogConsumer("3"))
				.partitionPersist(
						partitionPersistFactory,
						new Fields(ACTIVITY_ID_KEY, TridentTraceKeys.NAME,
								GLP_ID_KEY, PROPERTY_KEY, VALUE_KEY),
						gameplayStateUpdater);

		// 5 - Add the name of the given player to the document ('gameplayId')
		gameplayIdStream
				.each(new Fields(TridentTraceKeys.NAME),
						new SimplePropertyCreator(TridentTraceKeys.NAME,
								TridentTraceKeys.NAME),
						new Fields(PROPERTY_KEY, VALUE_KEY))
				.peek(new LogConsumer("4"))
				.partitionPersist(
						partitionPersistFactory,
						new Fields(ACTIVITY_ID_KEY, TridentTraceKeys.NAME,
								GLP_ID_KEY, PROPERTY_KEY, VALUE_KEY),
						gameplayStateUpdater);

		// Alternatives (selected & unlocked) processing
		// 6 - For each TraceEventTypes.SELECTED or TraceEventTypes.UNLOCKED
		// trace (alternative type)
		// 7 - Count the amount of different "responses" have been made (Group
		// By TridentTraceKeys.RESPONSE)
		gameplayIdStream
				.each(new Fields(TridentTraceKeys.EVENT, TRACE_KEY),
						new FieldValuesOrFilter(TridentTraceKeys.EVENT,
								TraceEventTypes.SELECTED,
								TraceEventTypes.UNLOCKED))
				.each(new Fields(TRACE_KEY),
						new TraceFieldExtractor(o(TridentTraceKeys.TARGET),
								o(TridentTraceKeys.TYPE),
								o(TridentTraceKeys.RESPONSE)),
						new Fields(TridentTraceKeys.TARGET,
								TridentTraceKeys.TYPE,
								TridentTraceKeys.RESPONSE))
				.each(new Fields(TRACE_KEY, TridentTraceKeys.TYPE,
						TridentTraceKeys.EVENT, TridentTraceKeys.TARGET,
						TridentTraceKeys.RESPONSE),
						new PropertyCreator(TRACE_KEY, TridentTraceKeys.EVENT,
								TridentTraceKeys.TYPE, TridentTraceKeys.TARGET),
						new Fields(PROPERTY_KEY, VALUE_KEY))
				.each(new Fields(ACTIVITY_ID_KEY, TridentTraceKeys.NAME),
						new ActivityIdNameCreator(ACTIVITY_ID_KEY,
								TridentTraceKeys.NAME),
						new Fields(ACTIVITY_ID_KEY + "_"
								+ TridentTraceKeys.NAME))
				.peek(new LogConsumer("5"))
				.groupBy(
						new Fields(GLP_ID_KEY, ACTIVITY_ID_KEY + "_"
								+ TridentTraceKeys.NAME,
								TridentTraceKeys.EVENT, TridentTraceKeys.TYPE,
								TridentTraceKeys.TARGET,
								TridentTraceKeys.RESPONSE))
				.persistentAggregate(persistentAggregateFactory, new Count(),
						new Fields("count"));

		// Accessible (accessed & skipped)), GameObject (interacted & used) and
		// Completable (initialized) processing
		// 8 - For each TraceEventTypes.ACCESSED, TraceEventTypes.SKIPPED
		// (Accessible), TraceEventTypes.INITIALIZED
		// (Completable), "interacted" or TraceEventTypes.USED (GameObject)
		// trace
		// 9 - Count the amount of different "targets" have been made (Group By
		// TridentTraceKeys.TARGET)
		gameplayIdStream
				.each(new Fields(TridentTraceKeys.EVENT, TRACE_KEY),
						new FieldValuesOrFilter(TridentTraceKeys.EVENT,
								TraceEventTypes.ACCESSED,
								TraceEventTypes.SKIPPED,
								TraceEventTypes.INITIALIZED,
								TraceEventTypes.INTERACTED,
								TraceEventTypes.USED))
				.each(new Fields(TRACE_KEY),
						new TraceFieldExtractor(o(TridentTraceKeys.TARGET),
								o(TridentTraceKeys.TYPE)),
						new Fields(TridentTraceKeys.TARGET,
								TridentTraceKeys.TYPE))
				.peek(new LogConsumer("initialized 1"))
				.each(new Fields(TRACE_KEY, TridentTraceKeys.TYPE,
						TridentTraceKeys.EVENT, TridentTraceKeys.TARGET),
						new PropertyCreator(TRACE_KEY, TridentTraceKeys.EVENT,
								TridentTraceKeys.TYPE, TridentTraceKeys.TARGET),
						new Fields(PROPERTY_KEY, VALUE_KEY))
				.each(new Fields(ACTIVITY_ID_KEY, TridentTraceKeys.NAME),
						new ActivityIdNameCreator(ACTIVITY_ID_KEY,
								TridentTraceKeys.NAME),
						new Fields(ACTIVITY_ID_KEY + "_"
								+ TridentTraceKeys.NAME))
				.peek(new LogConsumer("initialized 2"))
				.groupBy(
						new Fields(GLP_ID_KEY, ACTIVITY_ID_KEY + "_"
								+ TridentTraceKeys.NAME,
								TridentTraceKeys.EVENT, TridentTraceKeys.TYPE,
								TridentTraceKeys.TARGET))
				.persistentAggregate(persistentAggregateFactory, new Count(),
						new Fields("count"));

		// Completable (Progressed) processing
		// 10 - For each TraceEventTypes.PROGRESSED (Completable) trace
		// 11 - Update the TridentTraceKeys.PROGRESS field to its latest value
		gameplayIdStream
				.each(new Fields(TridentTraceKeys.EVENT, TRACE_KEY),
						new FieldValueFilter(TridentTraceKeys.EVENT,
								TraceEventTypes.PROGRESSED))
				.each(new Fields(TRACE_KEY),
						new TraceFieldExtractor(o(TridentTraceKeys.TARGET),
								o(TridentTraceKeys.TYPE)),
						new Fields(TridentTraceKeys.TARGET,
								TridentTraceKeys.TYPE))
				.each(new Fields(TRACE_KEY),
						new TraceFieldExtractor(o(EXTENSIONS_KEY + "."
								+ TridentTraceKeys.PROGRESS)),
						new Fields(TridentTraceKeys.PROGRESS))
				.each(new Fields(TridentTraceKeys.PROGRESS,
						TridentTraceKeys.TYPE, TridentTraceKeys.EVENT,
						TridentTraceKeys.TARGET),
						new SuffixPropertyCreator(TridentTraceKeys.PROGRESS,
								TridentTraceKeys.PROGRESS,
								TridentTraceKeys.EVENT, TridentTraceKeys.TYPE,
								TridentTraceKeys.TARGET),
						new Fields(PROPERTY_KEY, VALUE_KEY))
				.peek(new LogConsumer("6"))
				.partitionPersist(
						partitionPersistFactory,
						new Fields(ACTIVITY_ID_KEY, TridentTraceKeys.NAME,
								GLP_ID_KEY, PROPERTY_KEY, VALUE_KEY),
						gameplayStateUpdater);

		// Completable (Completed) processing for field TridentTraceKeys.SUCCESS
		// 11 - For each TraceEventTypes.COMPLETED (Completable) trace
		// 12 - Update the TridentTraceKeys.SUCCESS field to its latest value
		gameplayIdStream
				.each(new Fields(TridentTraceKeys.EVENT, TRACE_KEY),
						new FieldValueFilter(TridentTraceKeys.EVENT,
								TraceEventTypes.COMPLETED))
				.each(new Fields(TRACE_KEY),
						new TraceFieldExtractor(o(TridentTraceKeys.TARGET),
								o(TridentTraceKeys.TYPE),
								o(TridentTraceKeys.SUCCESS)),
						new Fields(TridentTraceKeys.TARGET,
								TridentTraceKeys.TYPE, TridentTraceKeys.SUCCESS))
				.each(new Fields(TridentTraceKeys.SUCCESS,
						TridentTraceKeys.TYPE, TridentTraceKeys.EVENT,
						TridentTraceKeys.TARGET),
						new SuffixPropertyCreator(TridentTraceKeys.SUCCESS,
								TridentTraceKeys.SUCCESS,
								TridentTraceKeys.EVENT, TridentTraceKeys.TYPE,
								TridentTraceKeys.TARGET),
						new Fields(PROPERTY_KEY, VALUE_KEY))
				.partitionPersist(
						partitionPersistFactory,
						new Fields(ACTIVITY_ID_KEY, TridentTraceKeys.NAME,
								GLP_ID_KEY, PROPERTY_KEY, VALUE_KEY),
						gameplayStateUpdater);

		// Completable (Completed) processing for field TridentTraceKeys.SCORE
		// 13 - For each TraceEventTypes.COMPLETED (Completable) trace
		// 14 - Update the TridentTraceKeys.SCORE field to its latest value
		gameplayIdStream
				.each(new Fields(TridentTraceKeys.EVENT, TRACE_KEY),
						new FieldValueFilter(TridentTraceKeys.EVENT,
								TraceEventTypes.COMPLETED))
				.each(new Fields(TRACE_KEY),
						new TraceFieldExtractor(o(TridentTraceKeys.TARGET),
								o(TridentTraceKeys.TYPE),
								o(TridentTraceKeys.SCORE)),
						new Fields(TridentTraceKeys.TARGET,
								TridentTraceKeys.TYPE, TridentTraceKeys.SCORE))
				.each(new Fields(TridentTraceKeys.SCORE, TridentTraceKeys.TYPE,
						TridentTraceKeys.EVENT, TridentTraceKeys.TARGET),
						new SuffixPropertyCreator(TridentTraceKeys.SCORE,
								TridentTraceKeys.SCORE, TridentTraceKeys.EVENT,
								TridentTraceKeys.TYPE, TridentTraceKeys.TARGET),
						new Fields(PROPERTY_KEY, VALUE_KEY))
				.partitionPersist(
						partitionPersistFactory,
						new Fields(ACTIVITY_ID_KEY, TridentTraceKeys.NAME,
								GLP_ID_KEY, PROPERTY_KEY, VALUE_KEY),
						gameplayStateUpdater);

		/*
		 * --> Aggregation analysis <--
		 */

		TridentState staticState = tridentTopology
				.newStaticState(partitionPersistFactory);

		TridentState staticChildState = tridentTopology
				.newStaticState(partitionPersistFactory);

		tracesStream
				// 1 - Filter only leafs with glpId
				.each(new Fields(TRACE_KEY), new HasGLPId(TRACE_KEY))

				// 2 - With the "glpId" and "activityId" get the "analytics"
				// object
				.each(new Fields(TRACE_KEY),
						new TraceFieldExtractor(GLP_ID_KEY, ACTIVITY_ID_KEY,
								o(TridentTraceKeys.NAME)),
						new Fields(GLP_ID_KEY, ACTIVITY_ID_KEY,
								TridentTraceKeys.NAME))
				.peek(new LogConsumer("Agg analysis, only not bubbled filter!"))
				.stateQuery(
						staticChildState,
						new Fields(GLP_ID_KEY, ACTIVITY_ID_KEY),
						new GetFromElasticIndex(GLP_ID_KEY, null,
								ACTIVITY_ID_KEY), new Fields(ANALYTICS_KEY))
				.each(new Fields(TRACE_KEY, ANALYTICS_KEY),
						new IsDirectChildTrace(TRACE_KEY, ANALYTICS_KEY))
				// 3 - Get the weights_activityId object from analytics-rootId
				// index
				.stateQuery(
						staticState,
						new Fields(GLP_ID_KEY, ACTIVITY_ID_KEY),
						new GetFromElasticIndex("", GLP_ID_KEY, null,
								ACTIVITY_ID_KEY, WEIGHTS + "_"),
						new Fields(WEIGHTS + "_" + ANALYTICS_KEY))
				.each(new Fields(TRACE_KEY, ANALYTICS_KEY),
						new IsDirectChildTrace(TRACE_KEY, ANALYTICS_KEY))
				.each(new Fields(TRACE_KEY, ANALYTICS_KEY, WEIGHTS + "_"
						+ ANALYTICS_KEY),
						new HasOperationAndChildrenAreValidAndParseValues(
								TRACE_KEY, ANALYTICS_KEY, WEIGHTS + "_"
										+ ANALYTICS_KEY), new Fields(WEIGHTS))
				.stateQuery(
						staticChildState,
						new Fields(GLP_ID_KEY, TridentTraceKeys.NAME, WEIGHTS),
						new BuildWeightsValuesFromElasticIndex(GLP_ID_KEY,
								TridentTraceKeys.NAME, WEIGHTS),
						new Fields(WEIGHTS + "_full"))
				.peek(new LogConsumer("Weights full produced"))
				// Persist to ActivityID
				.partitionPersist(
						partitionPersistFactory,
						new Fields(GLP_ID_KEY, ACTIVITY_ID_KEY,
								TridentTraceKeys.NAME, WEIGHTS + "_full"),
						new PerformOperationUpdater());

		/*
		 * --> Additional/custom analysis needed can be added here or changing
		 * the code above <--
		 */

	}

	static class PerformOperationUpdater implements StateUpdater<EsState> {
		private static final Logger LOGGER = Logger
				.getLogger(PerformOperationUpdater.class.getName());
		private static final boolean LOG = true;

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

					List weights = (List) tuple.getValueByField(WEIGHTS
							+ "_full");

					String glpId = tuple.getStringByField(GLP_ID_KEY);
					String resultsRootGlpId = ESUtils.getRootGLPId(glpId);
					String activityId = tuple.getStringByField(ACTIVITY_ID_KEY);
					String name = tuple.getStringByField(TridentTraceKeys.NAME);

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

						Object nameObj = weight.get(OPERATION_NAME_KEY);
						if (nameObj == null) {
							if (LOG) {
								LOGGER.info("nameObj has not been found for current weight "
										+ weight + " -> DISCARDING!");
							}
							continue;
						}

						String varName = nameObj.toString();

						Object opObj = weight.get(OPERATION_KEY);
						if (opObj == null) {
							if (LOG) {
								LOGGER.info("opObj has not been found for current weight "
										+ weight + " -> DISCARDING!");
							}
							continue;
						}

						// '+' or '*'
						String op = opObj.toString();

						Object childrenObj = weight.get(OPERATION_CHILDREN_KEY);
						if (!(childrenObj instanceof List)) {
							if (LOG) {
								LOGGER.info("childrenObj has not been found for current weight "
										+ weight
										+ " -> DISCARDING! or is not instance of List "
										+ childrenObj);
							}
							continue;
						}

						float childResult = Float.NEGATIVE_INFINITY;
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
									.get(OPERATION_CHILD_MULTIPLIER_KEY);
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
									.get(OPERATION_CHILDREN_VALUE_KEY);

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

							if (op.equalsIgnoreCase("+")) {
								if (childResult == Float.NEGATIVE_INFINITY) {
									childResult = 0f;
								}
								childResult += multiplyedVal;
							} else {
								if (childResult == Float.NEGATIVE_INFINITY) {
									childResult = 1f;
								}
								childResult *= multiplyedVal;
							}

							Object needsUpdateObj = child
									.get(OPERATION_CHILDREN_NEEDSUPDATE_KEY);

							if (needsUpdateObj != null) {

								boolean needsUpdate = false;
								try {
									needsUpdate = Boolean
											.parseBoolean(needsUpdateObj
													.toString());

								} catch (Exception booleanEx) {
									if (LOG) {
										LOGGER.info("Error parsing needsUpdateObj to boolean "
												+ needsUpdateObj
												+ " -> DISCARDING!");
									}
								}

								if (needsUpdate) {

									Object idObj = child
											.get(OPERATION_CHILD_ID_KEY);
									if (idObj != null) {
										String id = idObj.toString();

										Object childVarNameObj = child
												.get(OPERATION_NAME_KEY);
										if (childVarNameObj != null) {

											String childVarName = childVarNameObj
													.toString();

											state.setProperty(resultsRootGlpId,
													"agg_" + id + "_" + name,
													childVarName, value);
										} else {
											if (LOG) {
												LOGGER.info("childVarNameObj has not been found for current child "
														+ child
														+ " -> DISCARDING!");
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

}
