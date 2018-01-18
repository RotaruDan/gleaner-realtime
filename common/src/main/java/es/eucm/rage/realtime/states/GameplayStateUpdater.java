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
package es.eucm.rage.realtime.states;

import es.eucm.rage.realtime.topologies.TopologyBuilder;
import es.eucm.rage.realtime.states.elasticsearch.EsState;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.state.StateUpdater;
import org.apache.storm.trident.tuple.TridentTuple;

import java.util.List;
import java.util.Map;

/**
 * Sets the property extracting the {@link TopologyBuilder#PROPERTY_KEY} and the
 * {@link TopologyBuilder#VALUE_KEY} for a given
 * {@link TopologyBuilder#ACTIVITY_ID_KEY} and
 * {@link TopologyBuilder.TridentTraceKeys#GAMEPLAY_ID}.
 */
public class GameplayStateUpdater implements StateUpdater<EsState> {

	@Override
	public void updateState(EsState state, List<TridentTuple> tuples,
			TridentCollector collector) {
		try {
			for (TridentTuple tuple : tuples) {
				String activityId = tuple
						.getStringByField(TopologyBuilder.ACTIVITY_ID_KEY);
				String gameplayId = tuple
						.getStringByField(TopologyBuilder.GAMEPLAY_ID);
				String property = tuple
						.getStringByField(TopologyBuilder.PROPERTY_KEY);
				Object value = tuple.getValueByField(TopologyBuilder.VALUE_KEY);
				state.setProperty(activityId, gameplayId, property, value);
			}
		} catch (Exception ex) {
			System.out.println("Error unexpected exception, discarding"
					+ ex.toString());
		}
	}

	@Override
	public void prepare(Map conf, TridentOperationContext context) {

	}

	@Override
	public void cleanup() {

	}
}
