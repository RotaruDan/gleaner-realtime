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

import es.eucm.rage.realtime.AbstractAnalysis;
import es.eucm.rage.realtime.simple.filters.*;
import es.eucm.rage.realtime.functions.*;
import es.eucm.rage.realtime.simple.functions.GetRootGlpFromIndex;
import es.eucm.rage.realtime.states.elasticsearch.EsState;
import org.apache.storm.kafka.trident.OpaqueTridentKafkaSpout;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentState;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.state.StateFactory;
import org.apache.storm.trident.state.StateUpdater;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;

import java.util.*;
import java.util.logging.Logger;

/**
 * RAGE Analytics implementation of
 * {@link es.eucm.rage.realtime.topologies.TopologyBuilder} performing the
 * real-time analysis. Check out:
 * https://github.com/e-ucm/rage-analytics/wiki/Understanding
 * -RAGE-Analytics-Traces-Flow
 * <p>
 * Furthermore adds the Thomas Kilmann analysis of bias implementation
 */
public class OverallTopologyBuilder implements
		es.eucm.rage.realtime.topologies.TopologyBuilder {

	public static final String OVERALL_INDEX = "beaconing-overall";

	private String o(String key) {
		return OUT_KEY + "." + key;
	}

	@Override
	public void build(TridentTopology tridentTopology,
			OpaqueTridentKafkaSpout spout, Stream tracesStream,
			StateFactory partitionPersistFactory,
			StateFactory persistentAggregateFactory, Map<String, Object> conf) {

		/** ---> AbstractAnalysis definition <--- **/
		/* DEFAULT TOPOLOGY ANALYSIS */

		/*
		 * --> Analyzing for Kibana visualizations (traces index, 'sessionId')
		 * <--
		 */

		/*
		 * --> Analyzing for the Alerts and Warnings system (results index,
		 * 'results-sessionId') <--
		 */
		TridentState staticState = tridentTopology
				.newStaticState(partitionPersistFactory);
		TridentState glpState = tridentTopology
				.newStaticState(partitionPersistFactory);

		// 1 - For each TRACE_KEY (from Kibana) that we receive
		// 2 - Extract the fields TridentTraceKeys.GAMEPLAY_ID and
		// TridentTraceKeys.EVENT so that we can play
		// with it below
		tracesStream
				// Filter only leafs
				.each(new Fields(TRACE_KEY), new IsLeafFilter(TRACE_KEY))
				.peek(new LogConsumer("Leaf passed"))
				.each(new Fields(TRACE_KEY),
						new TraceFieldExtractor(o(TridentTraceKeys.NAME),
								o(TridentTraceKeys.EVENT), GLP_ID_KEY,
								ACTIVITY_ID_KEY),
						new Fields(TridentTraceKeys.NAME,
								TridentTraceKeys.EVENT, GLP_ID_KEY,
								ACTIVITY_ID_KEY))
				.peek(new LogConsumer("log 1"))
				.each(new Fields(TridentTraceKeys.EVENT, TRACE_KEY),
						new FieldValueFilter(TridentTraceKeys.EVENT,
								TraceEventTypes.SELECTED))
				.peek(new LogConsumer("log 2"))
				// 2 - With the "glpId" and "activityId" get the "analytics"
				// object
				.stateQuery(
						staticState,
						new Fields(GLP_ID_KEY, ACTIVITY_ID_KEY),
						new GetFromElasticIndex(GLP_ID_KEY, null,
								ACTIVITY_ID_KEY), new Fields(ANALYTICS_KEY))
				.peek(new LogConsumer("Extracted Analytics"))
				// object
				.stateQuery(glpState, new Fields(GLP_ID_KEY),
						new GetRootGlpFromIndex(),
						new Fields(ROOT_ANALYTICS_KEY))
				.peek(new LogConsumer("Extracted ROOT_ANALYTICS"))
				// Persist to ActivityID
				.partitionPersist(
						partitionPersistFactory,
						new Fields(TridentTraceKeys.NAME, TRACE_KEY,
								ANALYTICS_KEY, ROOT_ANALYTICS_KEY),
						new ResponsesStateUpdater());

		tracesStream
				// Filter only leafs
				.each(new Fields(TRACE_KEY), new IsLeafFilter(TRACE_KEY))
				.peek(new LogConsumer("Leaf passed"))
				.each(new Fields(TRACE_KEY), new HasTimeFilter())
				.peek(new LogConsumer("Has Score"))
				.each(new Fields(TRACE_KEY),
						new TraceFieldExtractor(o(TridentTraceKeys.NAME),
								o(TridentTraceKeys.SCORE)),
						new Fields(TridentTraceKeys.NAME,
								TridentTraceKeys.SCORE))
				.peek(new LogConsumer("log 1"))
				// Persist to ActivityID
				.partitionPersist(
						partitionPersistFactory,
						new Fields(TridentTraceKeys.NAME,
								TridentTraceKeys.SCORE),
						new AverageScoreStateUpdater());
		// 1 - Extract "glpId" and "activityId" from trace
		AbstractAnalysis
				.enhanceTracesStream(
						tridentTopology.newStream("times" + Math.random()
								* 100000, spout))
				.each(new Fields(TRACE_KEY), new IsLeafFilter(TRACE_KEY))
				.peek(new LogConsumer("Completed Leaf passed"))
				.each(new Fields(TRACE_KEY), new HasTimeFilter())
				.peek(new LogConsumer("Completed HasTime passed"))
				.each(new Fields(TRACE_KEY),
						new TraceFieldExtractor(o(TridentTraceKeys.NAME),
								o(TridentTraceKeys.TARGET), o("ext."
										+ TridentTraceKeys.TIME),
								o(TridentTraceKeys.EVENT), GLP_ID_KEY,
								ACTIVITY_ID_KEY),
						new Fields(TridentTraceKeys.NAME,
								TridentTraceKeys.TARGET, TridentTraceKeys.TIME,
								TridentTraceKeys.EVENT, GLP_ID_KEY,
								ACTIVITY_ID_KEY))
				.peek(new LogConsumer("Checking completed"))
				.each(new Fields(TridentTraceKeys.EVENT, TRACE_KEY),
						new FieldValueFilter(TridentTraceKeys.EVENT,
								TraceEventTypes.COMPLETED))
				// Persist to ActivityID
				.partitionPersist(
						partitionPersistFactory,
						new Fields(TRACE_KEY, TridentTraceKeys.NAME,
								TridentTraceKeys.TARGET, TridentTraceKeys.TIME,
								ACTIVITY_ID_KEY), new CompletedStateUpdater());

		// Progressed ROOT GLP ID
		AbstractAnalysis
				.enhanceTracesStream(
						tridentTopology.newStream("progressed" + Math.random()
								* 100000, spout))
				.each(new Fields(TRACE_KEY), new IsRootCreatedInRoot(TRACE_KEY))
				.peek(new LogConsumer("Completed Leaf passed"))
				.each(new Fields(TRACE_KEY),
						new TraceFieldExtractor(o(TridentTraceKeys.NAME),
								o(TridentTraceKeys.TARGET), o("ext."
										+ TridentTraceKeys.PROGRESS),
								o(TridentTraceKeys.EVENT), GLP_ID_KEY,
								ACTIVITY_ID_KEY),
						new Fields(TridentTraceKeys.NAME,
								TridentTraceKeys.TARGET,
								TridentTraceKeys.PROGRESS,
								TridentTraceKeys.EVENT, GLP_ID_KEY,
								ACTIVITY_ID_KEY))
				.peek(new LogConsumer("Checking progressed"))
				.each(new Fields(TridentTraceKeys.EVENT, TRACE_KEY),
						new FieldValueFilter(TridentTraceKeys.EVENT,
								TraceEventTypes.PROGRESSED))
				.each(new Fields(TRACE_KEY), new HasProgressFilter())
				.peek(new LogConsumer("Completed HasProgressFilter passed"))
				// Persist to ActivityID
				.partitionPersist(
						partitionPersistFactory,
						new Fields(TRACE_KEY, TridentTraceKeys.NAME,
								TridentTraceKeys.TARGET,
								TridentTraceKeys.PROGRESS, ACTIVITY_ID_KEY),
						new ProgressedStateUpdater());
	}

	static class AverageScoreStateUpdater implements StateUpdater<EsState> {
		private static final Logger LOGGER = Logger
				.getLogger(ResponsesStateUpdater.class.getName());

		@Override
		public void updateState(EsState state, List<TridentTuple> tuples,
				TridentCollector collector) {
			try {
				for (TridentTuple tuple : tuples) {

					String name = tuple.getStringByField(TridentTraceKeys.NAME);
					Number score = (Number) tuple
							.getValueByField(TridentTraceKeys.SCORE);

					// Update the average score
					state.updateAverageScore(OVERALL_INDEX, name,
							score.floatValue());

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

	static class ResponsesStateUpdater implements StateUpdater<EsState> {
		private static final Logger LOGGER = Logger
				.getLogger(ResponsesStateUpdater.class.getName());

		@Override
		public void updateState(EsState state, List<TridentTuple> tuples,
				TridentCollector collector) {
			try {
				for (TridentTuple tuple : tuples) {

					String name = tuple.getStringByField(TridentTraceKeys.NAME);
					Map trace = (Map) tuple.getValueByField(TRACE_KEY);
					Map analytics = (Map) tuple.getValueByField(ANALYTICS_KEY);
					Map rootAnalytics = (Map) tuple
							.getValueByField(ROOT_ANALYTICS_KEY);

					// Update the completed array of the analytics
					state.updateAnswersArray(OVERALL_INDEX, name, trace,
							analytics, rootAnalytics);

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

	static class CompletedStateUpdater implements StateUpdater<EsState> {
		private static final Logger LOGGER = Logger
				.getLogger(CompletedStateUpdater.class.getName());

		@Override
		public void updateState(EsState state, List<TridentTuple> tuples,
				TridentCollector collector) {
			try {
				for (TridentTuple tuple : tuples) {

					// TRACE_KEY,
					// TridentTraceKeys.NAME,
					// TridentTraceKeys.TARGET,
					// TridentTraceKeys.TIME,
					// ACTIVITY_ID_KEY

					Map trace = (Map) tuple.getValueByField(TRACE_KEY);
					String name = tuple.getStringByField(TridentTraceKeys.NAME);
					String target = tuple
							.getStringByField(TridentTraceKeys.TARGET);
					Object timeObj = tuple
							.getValueByField(TridentTraceKeys.TIME);
					float time = 0f;
					if (timeObj != null) {
						try {
							time = Float.parseFloat(timeObj.toString());
						} catch (NumberFormatException nfexScore) {
							LOGGER.info("Time is not a FLOAT: " + timeObj + " "
									+ tuple);
						}
					}
					String activityId = tuple.getStringByField(ACTIVITY_ID_KEY);

					// Update the completed array of the analytics
					state.updateCompletedArray(OVERALL_INDEX, name, target,
							time, activityId);

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

	static class ProgressedStateUpdater implements StateUpdater<EsState> {
		private static final Logger LOGGER = Logger
				.getLogger(ProgressedStateUpdater.class.getName());

		@Override
		public void updateState(EsState state, List<TridentTuple> tuples,
				TridentCollector collector) {
			try {
				for (TridentTuple tuple : tuples) {

					// TRACE_KEY,
					// TridentTraceKeys.NAME,
					// TridentTraceKeys.TARGET,
					// TridentTraceKeys.TIME,
					// ACTIVITY_ID_KEY

					Map trace = (Map) tuple.getValueByField(TRACE_KEY);
					String name = tuple.getStringByField(TridentTraceKeys.NAME);
					String target = tuple
							.getStringByField(TridentTraceKeys.TARGET);
					Object progressObj = tuple
							.getValueByField(TridentTraceKeys.PROGRESS);
					float progress = 0f;
					if (progressObj != null) {
						try {
							progress = Float.parseFloat(progressObj.toString());
						} catch (NumberFormatException nfexScore) {
							LOGGER.info("Time is not a FLOAT: " + progressObj
									+ " " + tuple);
						}
					}
					String activityId = tuple.getStringByField(ACTIVITY_ID_KEY);

					// Update the completed array of the analytics
					state.updateProgressedArray(OVERALL_INDEX, name, target,
							progress, activityId);

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
