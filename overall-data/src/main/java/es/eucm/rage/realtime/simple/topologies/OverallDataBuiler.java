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
package es.eucm.rage.realtime.simple.topologies;

import es.eucm.rage.realtime.filters.ExtensionTypeFilter;
import es.eucm.rage.realtime.filters.FieldValueFilter;
import es.eucm.rage.realtime.filters.FieldValuesOrFilter;
import es.eucm.rage.realtime.functions.*;
import es.eucm.rage.realtime.states.GameplayStateUpdater;
import es.eucm.rage.realtime.states.TraceStateUpdater;
import es.eucm.rage.realtime.states.elasticsearch.EsState;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.trident.state.StateFactory;
import org.apache.storm.tuple.Fields;

/**
 * RAGE Analytics implementation of
 * {@link es.eucm.rage.realtime.topologies.TopologyBuilder} performing the
 * real-time analysis. Check out:
 * https://github.com/e-ucm/rage-analytics/wiki/Understanding
 * -RAGE-Analytics-Traces-Flow
 * <p>
 * Furthermore adds the Thomas Kilmann analysis of bias implementation
 */
public class OverallDataBuiler implements
		es.eucm.rage.realtime.topologies.TopologyBuilder {

	public static final String THOMAS_KILMANN_KEY = "thomasKilmann";
	public static final String BIASES_KEY = "biases";
	public static final String BIAS_TYPE_KEY = "bias_type";
	public static final String BIAS_VALUE_TRUE_KEY = "bias_value_true";
	public static final String BIAS_VALUE_FALSE_KEY = "bias_value_false";
	public static final String THOMAS_KILMAN_INDEX_PREFIX = OverallDataBuiler.THOMAS_KILMANN_KEY
			.toLowerCase();

	@Override
	public void build(TridentTopology tridentTopology, Stream tracesStream,
			StateFactory partitionPersistFactory,
			StateFactory persistentAggregateFactory) {

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

		// 1 - For each TRACE_KEY (from Kibana) that we receive
		// 2 - Extract the fields TridentTraceKeys.GAMEPLAY_ID and
		// TridentTraceKeys.EVENT so that we can play
		// with it below
		Stream gameplayIdStream = tracesStream
				.each(new Fields(TRACE_KEY),
						new TraceFieldExtractor(TridentTraceKeys.GAMEPLAY_ID,
								TridentTraceKeys.EVENT),
						new Fields(TridentTraceKeys.GAMEPLAY_ID,
								TridentTraceKeys.EVENT)).peek(
						new LogConsumer("1"));

		// 3 - For each TRACE_KEY (from Kibana) that we receive
		// 4 - Extract the field 'timestamp' and add it to the document per
		// 'gameplayId' (player)
		gameplayIdStream
				.each(new Fields(TRACE_KEY),
						new TraceFieldExtractor(TridentTraceKeys.TIMESTAMP),
						new Fields(TridentTraceKeys.TIMESTAMP))
				.each(new Fields(TridentTraceKeys.TIMESTAMP),
						new SimplePropertyCreator(TridentTraceKeys.TIMESTAMP,
								TridentTraceKeys.TIMESTAMP),
						new Fields(PROPERTY_KEY, VALUE_KEY))
				.peek(new LogConsumer("3"))
				.partitionPersist(
						partitionPersistFactory,
						new Fields(SESSION_ID_KEY,
								TridentTraceKeys.GAMEPLAY_ID, PROPERTY_KEY,
								VALUE_KEY), new GameplayStateUpdater());

		// 5 - Add the name of the given player to the document ('gameplayId')
		gameplayIdStream
				.each(new Fields(TRACE_KEY),
						new TraceFieldExtractor(TridentTraceKeys.NAME),
						new Fields(TridentTraceKeys.NAME))
				.each(new Fields(TridentTraceKeys.NAME),
						new SimplePropertyCreator(TridentTraceKeys.NAME,
								TridentTraceKeys.NAME),
						new Fields(PROPERTY_KEY, VALUE_KEY))
				.peek(new LogConsumer("4"))
				.partitionPersist(
						partitionPersistFactory,
						new Fields(SESSION_ID_KEY,
								TridentTraceKeys.GAMEPLAY_ID, PROPERTY_KEY,
								VALUE_KEY), new GameplayStateUpdater());

		// Alternatives (selected & unlocked) processing
		// 6 - For each TraceEventTypes.SELECTED or TraceEventTypes.UNLOCKED
		// trace (alternative type)
		// 7 - Count the amount of different "responses" have been made (Group
		// By TridentTraceKeys.RESPONSE)
		gameplayIdStream
				.each(new Fields(TridentTraceKeys.EVENT, TRACE_KEY),
						new FieldValueFilter(TridentTraceKeys.EVENT,
								TraceEventTypes.SELECTED))
				.each(new Fields(TRACE_KEY),
						new TraceFieldExtractor(TridentTraceKeys.SUCCESS),
						new Fields(TridentTraceKeys.SUCCESS))
				.each(new Fields(TRACE_KEY, TridentTraceKeys.EVENT,
						TridentTraceKeys.SUCCESS),
						new PropertyCreator(TRACE_KEY, TridentTraceKeys.EVENT,
								TridentTraceKeys.SUCCESS),
						new Fields(PROPERTY_KEY, VALUE_KEY))
				.peek(new LogConsumer("5"))
				.groupBy(
						new Fields(SESSION_ID_KEY,
								TridentTraceKeys.GAMEPLAY_ID,
								TridentTraceKeys.EVENT,
								TridentTraceKeys.SUCCESS))
				.persistentAggregate(persistentAggregateFactory, new Count(),
						new Fields("count"));
	}

}
