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

import es.eucm.rage.realtime.simple.filters.FieldValueFilter;
import es.eucm.rage.realtime.simple.filters.HasScoreFilter;
import es.eucm.rage.realtime.simple.filters.IsLeafFilter;
import es.eucm.rage.realtime.functions.*;
import es.eucm.rage.realtime.states.elasticsearch.EsState;
import org.apache.storm.kafka.trident.OpaqueTridentKafkaSpout;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentTopology;
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
public class PerformanceTopologyBuilder implements
		es.eucm.rage.realtime.topologies.TopologyBuilder {

	public static final String PERFORMANCE_INDEX = "beaconing-performance";

	private String o(String key) {
		return OUT_KEY + "." + key;
	}

	@Override
	public void build(TridentTopology tridentTopology,
			OpaqueTridentKafkaSpout spout, Stream tracesStream,
			StateFactory partitionPersistFactory,
			StateFactory persistentAggregateFactory, Map<String, Object> conf) {

		/* PERFORMANCE TOPOLOGY ANALYSIS */

		/*
		 * --> Analyzing for the Performance Results <--
		 */

		// 1 - For each TRACE_KEY (from Kibana) that we receive
		// 2 - Ensure it has score
		// 3 - Extract the fields TridentTraceKeys.CLASS_ID, NAME, TIMESTAMP and
		// TridentTraceKeys.SCORE so that we can play
		// with it below
		// 4 - Persist per students, weeks, months
		tracesStream
				// Filter only leafs
				.each(new Fields(TRACE_KEY), new IsLeafFilter(TRACE_KEY))
				.peek(new LogConsumer("Leaf passed"))
				.each(new Fields(TRACE_KEY), new HasScoreFilter())
				.peek(new LogConsumer("Has Score"))
				.each(new Fields(TRACE_KEY),
						new TraceFieldExtractor(CLASS_ID,
								o(TridentTraceKeys.NAME),
								o(TridentTraceKeys.TIMESTAMP),
								o(TridentTraceKeys.SCORE)),
						new Fields(CLASS_ID, TridentTraceKeys.NAME,
								TridentTraceKeys.TIMESTAMP,
								TridentTraceKeys.SCORE))
				// Persist to ActivityID
				.partitionPersist(
						partitionPersistFactory,
						new Fields(CLASS_ID, TridentTraceKeys.NAME,
								TridentTraceKeys.TIMESTAMP,
								TridentTraceKeys.SCORE),
						new PerformanceStateUpdater());

	}

	static class PerformanceStateUpdater implements StateUpdater<EsState> {
		private static final Logger LOGGER = Logger
				.getLogger(PerformanceStateUpdater.class.getName());

		@Override
		public void updateState(EsState state, List<TridentTuple> tuples,
				TridentCollector collector) {
			try {
				for (TridentTuple tuple : tuples) {

					String classId = tuple.getStringByField(CLASS_ID);
					String timestamp = tuple
							.getStringByField(TridentTraceKeys.TIMESTAMP);
					String name = tuple.getStringByField(TridentTraceKeys.NAME);
					Object scoreObject = tuple
							.getValueByField(TridentTraceKeys.SCORE);

					Float score = Float.parseFloat(scoreObject.toString());

					// Update the completed array of the analytics
					state.updatePerformanceArray(PERFORMANCE_INDEX, classId,
							timestamp, name, score);

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
