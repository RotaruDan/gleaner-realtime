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

import es.eucm.rage.realtime.functions.MapFieldExtractor;
import es.eucm.rage.realtime.topologies.TopologyBuilder;
import es.eucm.rage.realtime.states.elasticsearch.EsState;
import es.eucm.rage.realtime.utils.ESUtils;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.state.StateUpdater;
import org.apache.storm.trident.tuple.TridentTuple;

import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Sets the property extracting the {@link TopologyBuilder#PROPERTY_KEY} and the
 * {@link TopologyBuilder#VALUE_KEY} for a given
 * {@link TopologyBuilder#ACTIVITY_ID_KEY} and
 * {@link TopologyBuilder.TridentTraceKeys#GAMEPLAY_ID}. Also receives
 * {@link TopologyBuilder#GLP_ID_KEY} (for the index) and Also receives
 * {@link TopologyBuilder.TridentTraceKeys#NAME}.
 * 
 * Elasticsearch _id = {@link TopologyBuilder#ACTIVITY_ID_KEY} + "_" +
 * {@link TopologyBuilder.TridentTraceKeys#NAME}.
 */
public class GameplayStateUpdater implements StateUpdater<EsState> {
	private static final Logger LOGGER = Logger
			.getLogger(GameplayStateUpdater.class.getName());

	@Override
	public void updateState(EsState state, List<TridentTuple> tuples,
			TridentCollector collector) {
		try {
			for (TridentTuple tuple : tuples) {
				String activityId = tuple
						.getStringByField(TopologyBuilder.ACTIVITY_ID_KEY);
				String glpId = tuple
						.getStringByField(TopologyBuilder.ROOT_ID_KEY);

				if (glpId == null) {
					glpId = activityId;
				} else {
					glpId = ESUtils.getRootGLPId(glpId);
				}

				String name = tuple
						.getStringByField(TopologyBuilder.TridentTraceKeys.NAME);
				String property = tuple
						.getStringByField(TopologyBuilder.PROPERTY_KEY);
				Object value = tuple.getValueByField(TopologyBuilder.VALUE_KEY);
				state.setProperty(glpId, activityId + "_" + name, property,
						value);
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
