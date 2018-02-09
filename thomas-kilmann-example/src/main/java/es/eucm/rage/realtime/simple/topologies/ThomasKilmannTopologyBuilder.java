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

import es.eucm.rage.realtime.filters.ExtensionTypeFilter;
import es.eucm.rage.realtime.filters.FieldValueFilter;
import es.eucm.rage.realtime.filters.FieldValuesOrFilter;
import es.eucm.rage.realtime.functions.*;
import es.eucm.rage.realtime.simple.functions.ThomasKilmannDocumentBuilder;
import es.eucm.rage.realtime.states.GameplayStateUpdater;
import es.eucm.rage.realtime.states.TraceStateUpdater;
import es.eucm.rage.realtime.states.elasticsearch.EsState;
import es.eucm.rage.realtime.topologies.*;
import org.apache.storm.kafka.trident.OpaqueTridentKafkaSpout;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.trident.state.StateFactory;
import org.apache.storm.tuple.Fields;

import java.util.Map;

/**
 * RAGE Analytics implementation of
 * {@link es.eucm.rage.realtime.topologies.TopologyBuilder} performing the
 * real-time analysis. Check out:
 * https://github.com/e-ucm/rage-analytics/wiki/Understanding
 * -RAGE-Analytics-Traces-Flow
 * <p>
 * Furthermore adds the Thomas Kilmann analysis of bias implementation
 */
public class ThomasKilmannTopologyBuilder implements
		es.eucm.rage.realtime.topologies.TopologyBuilder {

	public static final String THOMAS_KILMANN_KEY = "thomasKilmann";
	public static final String BIASES_KEY = "biases";
	public static final String BIAS_TYPE_KEY = "bias_type";
	public static final String BIAS_VALUE_TRUE_KEY = "bias_value_true";
	public static final String BIAS_VALUE_FALSE_KEY = "bias_value_false";
	public static final String THOMAS_KILMAN_INDEX_PREFIX = ThomasKilmannTopologyBuilder.THOMAS_KILMANN_KEY
			.toLowerCase();

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
		 * --> Analyzing for the Alerts and Warnings system (results index,
		 * 'results-sessionId') <--
		 */

		// 1 - For each TRACE_KEY (from Kibana) that we receive
		// 2 - Extract the fields TridentTraceKeys.GAMEPLAY_ID and
		// TridentTraceKeys.EVENT so that we can play
		// with it below
		Stream gameplayIdStream = tracesStream.each(
				new Fields(TRACE_KEY),
				new TraceFieldExtractor(ACTIVITY_ID_KEY,
						o(TridentTraceKeys.EVENT)),
				new Fields(ACTIVITY_ID_KEY, TridentTraceKeys.EVENT)).peek(
				new LogConsumer("1"));

		/*
		 * --> Additional/custom analysis needed can be added here or changing
		 * the code above <--
		 */
		/* THOMAS KILMANN TOPOLOGY ANALYSIS */

		gameplayIdStream
				.each(new Fields(TridentTraceKeys.EVENT),
						new FieldValueFilter(TridentTraceKeys.EVENT,
								TraceEventTypes.SELECTED))
				// Filter only traces with ext.thomasKilmann
				.peek(new LogConsumer("ThomasKilmannTopologyBuilder 1"))
				.each(new Fields(TRACE_KEY),
						new ExtensionTypeFilter(THOMAS_KILMANN_KEY,
								String.class))
				.peek(new LogConsumer("ThomasKilmannTopologyBuilder 2"))
				.each(new Fields(TRACE_KEY),
						new ThomasKilmannDocumentBuilder(TRACE_KEY),
						new Fields(THOMAS_KILMANN_KEY))
				.peek(new LogConsumer("ThomasKilmannTopologyBuilder 3"))
				.partitionPersist(EsState.opaque(),
						new Fields(THOMAS_KILMANN_KEY), new TraceStateUpdater());

	}

}
