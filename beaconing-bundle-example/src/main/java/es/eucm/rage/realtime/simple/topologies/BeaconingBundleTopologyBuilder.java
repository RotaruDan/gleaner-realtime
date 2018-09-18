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
public class BeaconingBundleTopologyBuilder implements
		es.eucm.rage.realtime.topologies.TopologyBuilder {

	private String o(String key) {
		return OUT_KEY + "." + key;
	}

	@Override
	public void build(TridentTopology tridentTopology,
			OpaqueTridentKafkaSpout spout, Stream tracesStream,
			StateFactory partitionPersistFactory,
			StateFactory persistentAggregateFactory, Map<String, Object> conf) {

		new GLPTopologyBuilder().build(tridentTopology, spout, tracesStream,
				partitionPersistFactory, persistentAggregateFactory, conf);
		new OverallTopologyBuilder().build(tridentTopology, spout,
				tracesStream, partitionPersistFactory,
				persistentAggregateFactory, conf);
		new PerformanceTopologyBuilder().build(tridentTopology, spout,
				tracesStream, partitionPersistFactory,
				persistentAggregateFactory, conf);
	}
}
