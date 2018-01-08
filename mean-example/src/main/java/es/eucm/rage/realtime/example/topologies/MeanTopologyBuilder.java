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
package es.eucm.rage.realtime.example.topologies;

import es.eucm.rage.realtime.example.AverageUpdater;
import es.eucm.rage.realtime.filters.FieldValueFilter;
import es.eucm.rage.realtime.functions.LogConsumer;
import es.eucm.rage.realtime.functions.ToDouble;
import es.eucm.rage.realtime.functions.TraceFieldExtractor;
import es.eucm.rage.realtime.states.GameplayStateUpdater;

import es.eucm.rage.realtime.topologies.TopologyBuilder;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentState;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.state.StateFactory;
import org.apache.storm.tuple.Fields;

/**
 * RAGE Analytics implementation of
 * {@link es.eucm.rage.realtime.topologies.TopologyBuilder} performing the
 * real-time analysis. Check out:
 * https://github.com/e-ucm/rage-analytics/wiki/Understanding
 * -RAGE-Analytics-Traces-Flow
 */
public class MeanTopologyBuilder implements
		es.eucm.rage.realtime.topologies.TopologyBuilder {

	private String o(String key) {
		return OUT_KEY + "." + key;
	}

	@Override
	public void build(TridentTopology tridentTopology, Stream tracesStream,
			StateFactory partitionPersistFactory,
			StateFactory persistentAggregateFactory) {

		/** ---> AbstractAnalysis definition <--- **/

		// 1 - For each TRACE_KEY that we receive
		// 2 - Extract the values of the fields TridentTraceKeys.GAMEPLAY_ID and
		// TridentTraceKeys.EVENT
		// 3 - Filter only the "completed" EVENT
		// 4 - Extract the values of the fields
		// TridentTraceKeys.TARGET,TridentTraceKeys.TYPE and
		// TridentTraceKeys.SCORE
		// 3 - Convert the SCORE from a String to a Double
		// 6 - Persist the score with the AverageUpdater which:
		// (1) queries the current state of the "mean" object
		// (2) updates the state of the object with the new value of the SCORE
		tracesStream
				.each(new Fields(TRACE_KEY),
						new TraceFieldExtractor(ACTIVITY_ID_KEY, GAMEPLAY_ID,
								o(TridentTraceKeys.EVENT)),
						new Fields(ACTIVITY_ID_KEY, GAMEPLAY_ID,
								TridentTraceKeys.EVENT))
				.peek(new LogConsumer("1"))
				.each(new Fields(TridentTraceKeys.EVENT, TRACE_KEY),
						new FieldValueFilter(TridentTraceKeys.EVENT,
								TraceEventTypes.COMPLETED))
				.each(new Fields(TRACE_KEY),
						new TraceFieldExtractor(o(TridentTraceKeys.TARGET),
								o(TridentTraceKeys.TYPE),
								o(TridentTraceKeys.SCORE)),
						new Fields(TridentTraceKeys.TARGET,
								TridentTraceKeys.TYPE, TridentTraceKeys.SCORE))
				.peek(new LogConsumer("2"))
				.each(new Fields(TridentTraceKeys.SCORE), new ToDouble(),
						new Fields(TridentTraceKeys.SCORE + "-double"))
				.peek(new LogConsumer("3"))
				.partitionPersist(
						partitionPersistFactory,
						new Fields(ACTIVITY_ID_KEY, TridentTraceKeys.TYPE,
								TridentTraceKeys.EVENT,
								TopologyBuilder.TridentTraceKeys.SCORE
										+ "-double"), new AverageUpdater(),
						new Fields("persistedAverage"));

	}

}
