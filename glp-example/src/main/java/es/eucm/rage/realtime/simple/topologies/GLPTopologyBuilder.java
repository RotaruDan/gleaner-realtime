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

import clojure.lang.Numbers;
import es.eucm.rage.realtime.AbstractAnalysis;
import es.eucm.rage.realtime.simple.filters.FieldValueFilter;
import es.eucm.rage.realtime.simple.filters.IsLeafFilter;
import es.eucm.rage.realtime.functions.*;
import es.eucm.rage.realtime.simple.filters.*;
import es.eucm.rage.realtime.simple.functions.FilterChildAndProgress;
import es.eucm.rage.realtime.states.elasticsearch.EsState;
import es.eucm.rage.realtime.utils.ESUtils;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.storm.kafka.trident.OpaqueTridentKafkaSpout;
import org.apache.storm.kafka.trident.TridentKafkaStateFactory;
import org.apache.storm.kafka.trident.TridentKafkaUpdater;
import org.apache.storm.kafka.trident.mapper.TridentTupleToKafkaMapper;
import org.apache.storm.kafka.trident.selector.KafkaTopicSelector;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentState;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.CombinerAggregator;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.operation.builtin.Count;
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
public class GLPTopologyBuilder implements
		es.eucm.rage.realtime.topologies.TopologyBuilder {

	public static final String PARENT_TRACE = "parent";
	public static final String PARENT_CHILDREN_TRACE = "children";
	public static final String LIMITS = "limits";
	public static final String PARTIAL_THRESHOLDS = "partialThresholds";
	public static final String PARTIAL_THRESHOLD_SCORE = "score";
	public static final String LEARNING_OBJECTIVES = "learningObjectives";
	public static final String COMPETENCIES = "competencies";
	public static final String CONTRIBUTES = "contributes";
	private static final String GLP_STREAM_ID = "glp-"
			+ AbstractAnalysis.INPUT_SPOUT_TX_ID;
	private static final String PARENT_PROGRESSED_STREAM_ID = "parent-progressed-"
			+ AbstractAnalysis.INPUT_SPOUT_TX_ID;
	public static final String COMPLETED_KEY = "completed";
	public static final String ANALYTICS_FULL_COMPLETED = "fullCompleted";

	private String o(String key) {
		return OUT_KEY + "." + key;
	}

	@Override
	public void build(TridentTopology tridentTopology,
			OpaqueTridentKafkaSpout spout, Stream tracesStream,
			StateFactory partitionPersistFactory,
			StateFactory persistentAggregateFactory, Map<String, Object> conf) {

		/** ---> AbstractAnalysis definition <--- **/

		/*
		 * --> Additional/custom analysis needed can be added here or changing
		 * the code above <--
		 */
		/* GLP ANALYSIS */

		/** BUBBLE TRACES UPWARDS TO PARENT **/

		TridentState staticState = tridentTopology
				.newStaticState(partitionPersistFactory);
		// 1 - Start with a new stream to avoid conflicts with other analysis
		// such as Performance/Overall
		AbstractAnalysis
				.enhanceTracesStream(
						tridentTopology.newStream(GLP_STREAM_ID + Math.random()
								* 100000, spout))

				// Every trace that has a "glpId" will be part of the tree and
				// will need bubbling
				.each(new Fields(TRACE_KEY), new HasGLPId(TRACE_KEY))
				.each(new Fields(TRACE_KEY),
						new TraceFieldExtractor(GLP_ID_KEY, ACTIVITY_ID_KEY),
						new Fields(GLP_ID_KEY, ACTIVITY_ID_KEY))
				// 2 - With the "glpId" and "activityId" get the "analytics"
				// object
				.stateQuery(
						staticState,
						new Fields(GLP_ID_KEY, ACTIVITY_ID_KEY),
						new GetFromElasticIndex(GLP_ID_KEY, null,
								ACTIVITY_ID_KEY), new Fields(ANALYTICS_KEY))
				.peek(new LogConsumer("Extracted Analytics"))
				// 3 - With the "analytics" object create a new trace (called
				// PARENT_TRACE) whose "activityId" is the "analytics.parentId"
				// value
				.each(new Fields(TRACE_KEY, ANALYTICS_KEY),
						new TraceToParentBuilder(TRACE_KEY, ANALYTICS_KEY),
						new Fields(PARENT_TRACE))
				.peek(new LogConsumer("Built trace to Parent"))
				// 4 - Send the PARENT_TRACE data to Kafka
				.partitionPersist(toParentKafkaFactory(conf, "key"),
						new Fields(PARENT_TRACE), new TridentKafkaUpdater());

		/** COMPLETED LEAF ANALYSIS **/
		TridentState staticStateCompleted = tridentTopology
				.newStaticState(partitionPersistFactory);

		// 1 - Start with a new stream to avoid conflicts with other analysis
		// such as Performance/Overall
		AbstractAnalysis
				.enhanceTracesStream(
						tridentTopology.newStream(GLP_STREAM_ID + Math.random()
								* 100000, spout))
				.each(new Fields(TRACE_KEY),
						new TraceFieldExtractor(o(TridentTraceKeys.EVENT)),
						new Fields(TridentTraceKeys.EVENT))
				.peek(new LogConsumer("Extracted event"))
				// Filter only traces with event COMPLETED
				.each(new Fields(TridentTraceKeys.EVENT),
						new FieldValueFilter(TridentTraceKeys.EVENT,
								TraceEventTypes.COMPLETED))
				// Filter only traces with success TRUE
				.peek(new LogConsumer("Is a COMPLETED trace"))
				.each(new Fields(TRACE_KEY), new SuccessFilter(true))
				.peek(new LogConsumer("Success is TRUE"))
				// Filter only leafs
				.each(new Fields(TRACE_KEY), new IsLeafFilter(TRACE_KEY))
				.peek(new LogConsumer("Leaf passed"))
				// Extract GLP_ID, ActivityID and NAME
				.each(new Fields(TRACE_KEY),
						new TraceFieldExtractor(GLP_ID_KEY, ACTIVITY_ID_KEY,
								o(TridentTraceKeys.NAME)),
						new Fields(GLP_ID_KEY, ACTIVITY_ID_KEY,
								TridentTraceKeys.NAME))
				// Query Analytics associated
				.stateQuery(
						staticStateCompleted,
						new Fields(GLP_ID_KEY, ACTIVITY_ID_KEY),
						new GetFromElasticIndex(GLP_ID_KEY, null,
								ACTIVITY_ID_KEY), new Fields(ANALYTICS_KEY))
				.peek(new LogConsumer("Extracted Analytics"))
				// Filter by partial threshold limits
				.each(new Fields(TRACE_KEY, ANALYTICS_KEY),
						new PartialThresholdsFilter(ANALYTICS_KEY, TRACE_KEY))
				.peek(new LogConsumer("Partial threshold passed successfully"))
				// Extract Contributes Map
				.each(new Fields(ANALYTICS_KEY),
						new MapFieldExtractor(ANALYTICS_KEY, CONTRIBUTES),
						new Fields(CONTRIBUTES))
				.peek(new LogConsumer("Extracted CONTRIBUTES"))
				// Create with the Contributes Property & Value Fields
				.each(new Fields(CONTRIBUTES),
						new StringPropertyCreator(CONTRIBUTES, CONTRIBUTES),
						new Fields(PROPERTY_KEY, VALUE_KEY))
				.peek(new LogConsumer("Created Property"))
				.each(new Fields(ACTIVITY_ID_KEY, TridentTraceKeys.NAME),
						new ActivityIdNameCreator(ACTIVITY_ID_KEY,
								TridentTraceKeys.NAME),
						new Fields(ACTIVITY_ID_KEY + "_"
								+ TridentTraceKeys.NAME))
				.each(new Fields(TRACE_KEY), new RootGlpIdFieldExtractor(),
						new Fields(ROOT_ID_KEY))
				// Group by ACTIVITY ID
				.groupBy(
						new Fields(ROOT_ID_KEY, ACTIVITY_ID_KEY + "_"
								+ TridentTraceKeys.NAME, TridentTraceKeys.NAME,
								PROPERTY_KEY, VALUE_KEY))
				.persistentAggregate(persistentAggregateFactory, new Count(),
						new Fields("count"))
				.newValuesStream()
				.peek(new LogConsumer("NEW VALUES STREAM PEEK "))
				// Persist to ActivityID
				.partitionPersist(
						partitionPersistFactory,
						new Fields(ROOT_ID_KEY, ACTIVITY_ID_KEY + "_"
								+ TridentTraceKeys.NAME, TridentTraceKeys.NAME,
								PROPERTY_KEY, VALUE_KEY, "count"),
						new ContribStateUpdater(),
						new Fields(ROOT_ID_KEY, ACTIVITY_ID_KEY + "_"
								+ TridentTraceKeys.NAME, TridentTraceKeys.NAME,
								PROPERTY_KEY, VALUE_KEY, "count"))
				.newValuesStream()
				.each(new Fields(ROOT_ID_KEY, TridentTraceKeys.NAME),
						new ActivityIdNameCreator(ROOT_ID_KEY,
								TridentTraceKeys.NAME),
						new Fields(ROOT_ID_KEY + "_" + TridentTraceKeys.NAME))
				.groupBy(
						new Fields(ROOT_ID_KEY, ROOT_ID_KEY + "_"
								+ TridentTraceKeys.NAME, TridentTraceKeys.NAME,
								PROPERTY_KEY, VALUE_KEY, "count"))
				.persistentAggregate(persistentAggregateFactory, new Count(),
						new Fields("count_root"))
				.newValuesStream()
				.peek(new LogConsumer("NEW VALUES STREAM PEEK 2"))
				// Persist to ActivityID
				.partitionPersist(
						partitionPersistFactory,
						new Fields(ROOT_ID_KEY, ROOT_ID_KEY + "_"
								+ TridentTraceKeys.NAME, TridentTraceKeys.NAME,
								PROPERTY_KEY, VALUE_KEY, "count_root", "count"),
						new ContribStateUpdater("count_root", ROOT_ID_KEY + "_"
								+ TridentTraceKeys.NAME),
						new Fields(ROOT_ID_KEY, ROOT_ID_KEY + "_"
								+ TridentTraceKeys.NAME, TridentTraceKeys.NAME,
								PROPERTY_KEY, VALUE_KEY, "count_root"));

		/** PARENT PROGRESSED ANALYSIS **/
		TridentState staticParentProgressedState = tridentTopology
				.newStaticState(partitionPersistFactory);
		// 1 - Start with a new stream to avoid conflicts with other analysis
		// such as Performance/Overall
		AbstractAnalysis
				.enhanceTracesStream(
						tridentTopology.newStream(PARENT_PROGRESSED_STREAM_ID
								+ Math.random() * 100000, spout))
				// Filter all traces that are not bubbled
				.each(new Fields(TRACE_KEY), new IsBubbledTrace(TRACE_KEY))
				// Extract Event key
				.each(new Fields(TRACE_KEY),
						new TraceFieldExtractor(GLP_ID_KEY, ACTIVITY_ID_KEY,
								o(TridentTraceKeys.NAME),
								o(TridentTraceKeys.EVENT),
								CHILD_ACTIVITY_ID_KEY),
						new Fields(GLP_ID_KEY, ACTIVITY_ID_KEY,
								TridentTraceKeys.NAME, TridentTraceKeys.EVENT,
								CHILD_ACTIVITY_ID_KEY))
				.peek(new LogConsumer("Extracted event"))
				// Filter only traces with event COMPLETED
				.each(new Fields(TridentTraceKeys.EVENT),
						new FieldValueFilter(TridentTraceKeys.EVENT,
								TraceEventTypes.COMPLETED))
				.peek(new LogConsumer("Is a COMPLETED trace"))
				// Filter only Success == true traces
				/*
				 * .each(new Fields(TRACE_KEY), new SuccessFilter(true))
				 * .peek(new LogConsumer("Success is TRUE"))
				 */
				// completed (if applicable) to kafka
				// Extract Analytics
				.stateQuery(
						staticParentProgressedState,
						new Fields(GLP_ID_KEY, ACTIVITY_ID_KEY),
						new GetFromElasticIndex(GLP_ID_KEY, null,
								ACTIVITY_ID_KEY), new Fields(ANALYTICS_KEY))
				// Filter all traces that are not bubbled
				.each(new Fields(TRACE_KEY, ANALYTICS_KEY),
						new IsDirectChildTrace(TRACE_KEY, ANALYTICS_KEY))
				.peek(new LogConsumer("Extracted Analytics"))
				// Filter that it has not been completed before the PARENT
				// analytics (parent branch)
				/*
				 * Do analysis always, even if it's completed :) .each(new
				 * Fields(ANALYTICS_KEY, TridentTraceKeys.NAME), new
				 * IsCompletedAnalytics(ANALYTICS_KEY, TridentTraceKeys.NAME,
				 * false)) .peek(new
				 * LogConsumer("Analytics is not completed, proceeding", true))
				 */
				// Extract
				.each(new Fields(ANALYTICS_KEY, TRACE_KEY),
						new FilterChildAndProgress(ANALYTICS_KEY, TRACE_KEY),
						new Fields(TRACE_KEY + "result"))
				.peek(new LogConsumer("Progressed correctly"))
				// Persist to ActivityID
				.partitionPersist(
						partitionPersistFactory,
						new Fields(TRACE_KEY + "result", GLP_ID_KEY,
								ACTIVITY_ID_KEY, TridentTraceKeys.NAME,
								ANALYTICS_KEY, CHILD_ACTIVITY_ID_KEY),
						new ParentCompletedStateUpdater(),
						new Fields(TRACE_KEY + "toparent", ANALYTICS_KEY))
				.newValuesStream()
				.peek(new LogConsumer("New Values (PROGRESSED/COMPLETED)"))
				// With the "analytics" object create a new trace (called
				// PARENT_TRACE) whose "activityId" is the "analytics.parentId"
				// value
				.each(new Fields(TRACE_KEY + "toparent", ANALYTICS_KEY),
						new TraceToCurrentNodeBuilder(TRACE_KEY + "toparent",
								ANALYTICS_KEY), new Fields(PARENT_TRACE))
				.peek(new LogConsumer(
						"Built trace to parent (COMPLETED/PROGRESSED)"))
				// Send the PARENT_TRACE data to Kafka

				.partitionPersist(toParentKafkaFactory(conf, "key2"),
						new Fields(PARENT_TRACE), new TridentKafkaUpdater());

	}

	/**
	 * Creates a conexion with Kafka that will send traces to the queue when
	 * obtained from the stream.
	 * 
	 * @param conf
	 *            The configuration object to obtain the connection with kafka
	 * @param key
	 *            Unique value that can nbe used by the consumer, must be unique
	 *            per State Factory.
	 * @return the {@link TridentKafkaStateFactory} to pass it to the stream
	 */
	public static TridentKafkaStateFactory toParentKafkaFactory(
			final Map<String, Object> conf, final String key) {

		final String kafkaUrl = conf.get(AbstractAnalysis.KAFKA_URL_FLUX_PARAM)
				.toString();
		String bootstrapServers = kafkaUrl;
		final String topic = conf.get(AbstractAnalysis.TOPIC_NAME_FLUX_PARAM)
				.toString();
		// Set producer properties for the connection with Kafka
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		props.put(ProducerConfig.ACKS_CONFIG, "1");
		props.put(ProducerConfig.CLIENT_ID_CONFIG, "kafka-producer-glp-"
				+ AbstractAnalysis.INPUT_SPOUT_TX_ID + Math.random() * 100000);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
				StringSerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
				StringSerializer.class.getName());
		TridentKafkaStateFactory stateFactory = new TridentKafkaStateFactory()
				.withProducerProperties(props)
				.withKafkaTopicSelector(new KafkaTopicSelector() {
					@Override
					public String getTopic(TridentTuple tridentTuple) {
						// The name of the topic where to send the traces
						return topic;
					}
				})
				.withTridentTupleToKafkaMapper(new TridentTupleToKafkaMapper() {

					@Override
					public Object getKeyFromTuple(TridentTuple tridentTuple) {
						return key;
					}

					@Override
					public Object getMessageFromTuple(TridentTuple tridentTuple) {
						// The message that we send, the value of the tag
						// PARENT_TRACE
						return tridentTuple.getValueByField(PARENT_TRACE);
					}
				});
		return stateFactory;
	}

	static class ParentCompletedStateUpdater implements StateUpdater<EsState> {
		private static final Logger LOGGER = Logger
				.getLogger(ParentCompletedStateUpdater.class.getName());

		@Override
		public void updateState(EsState state, List<TridentTuple> tuples,
				TridentCollector collector) {
			try {
				for (TridentTuple tuple : tuples) {

					// receives TRACE_KEY, GLP_ID_KEY, ACTIVITY_ID_KEY,
					// GAMEPLAY_ID, ANALYTICS_KEY

					Map parentTraceOrig = (Map) tuple.getValueByField(TRACE_KEY
							+ "result");

					Map parentTrace = new HashMap(parentTraceOrig);
					String glpId = tuple.getStringByField(GLP_ID_KEY);
					String activityId = tuple.getStringByField(ACTIVITY_ID_KEY);
					String childId = tuple
							.getStringByField(CHILD_ACTIVITY_ID_KEY);
					String name = tuple.getStringByField(TridentTraceKeys.NAME);
					Object analytics = tuple.getValueByField(ANALYTICS_KEY);

					// Update the completed array of the analytics
					state.updateUniqueArray(glpId, activityId, COMPLETED_KEY,
							name, childId);

					// EMITS TRACE_KEY, ANALYTICS_KEY
					List<Object> ret = new ArrayList<>(2);
					ret.add(parentTrace);
					ret.add(analytics);
					collector.emit(ret);
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
