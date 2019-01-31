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
import com.rits.cloning.Cloner;
import es.eucm.rage.realtime.states.elasticsearch.EsState;
import es.eucm.rage.realtime.topologies.TopologyBuilder;
import es.eucm.rage.realtime.utils.ESUtils;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.state.StateUpdater;
import org.apache.storm.trident.tuple.TridentTuple;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Updater after the {@link ContribSumAggregation}. Updates to ElasticSearch the
 * Contribution values when a completed/success trace is received.
 */
public class ContribStateUpdater implements StateUpdater<EsState> {
	private static final Logger LOGGER = Logger
			.getLogger(ContribStateUpdater.class.getName());
	private final String countKey, docId;

	public ContribStateUpdater() {
		this(null, GLPTopologyBuilder.ACTIVITY_ID_KEY + "_"
				+ TopologyBuilder.TridentTraceKeys.NAME);
	}

	public ContribStateUpdater(String countKey, String docId) {
		this.countKey = countKey;
		this.docId = docId;
	}

	@Override
	public void updateState(EsState state, List<TridentTuple> tuples,
			TridentCollector collector) {
		try {
			for (TridentTuple tuple : tuples) {

				Map<String, Object> result = (Map) tuple
						.getValueByField(TopologyBuilder.VALUE_KEY);

				String rootGlpId = tuple
						.getStringByField(GLPTopologyBuilder.ROOT_ID_KEY);
				String docId = tuple.getStringByField(this.docId);
				String name = tuple
						.getStringByField(TopologyBuilder.TridentTraceKeys.NAME);
				String property = tuple
						.getStringByField(GLPTopologyBuilder.PROPERTY_KEY);
				Map<String, Object> competencies = (Map) result
						.get(GLPTopologyBuilder.COMPETENCIES);
				Map<String, Object> learningObjectives = (Map) result
						.get(GLPTopologyBuilder.LEARNING_OBJECTIVES);
				long count = tuple.getLongByField("count");
				long countAdd = 1;
				if (countKey != null) {
					countAdd = tuple.getLongByField(countKey);
				}
				Map resultCompetencies = new HashMap();

				for (Map.Entry<String, Object> stringObjectEntry : competencies
						.entrySet()) {
					resultCompetencies.put(stringObjectEntry.getKey(),
							(double) stringObjectEntry.getValue() * count
									* countAdd);
				}

				if (!resultCompetencies.isEmpty()) {
					state.setProperty(rootGlpId, docId,
							GLPTopologyBuilder.COMPETENCIES, resultCompetencies);
				}

				Map resultLearningObjectives = new HashMap();

				for (Map.Entry<String, Object> stringObjectEntry : learningObjectives
						.entrySet()) {
					resultLearningObjectives.put(stringObjectEntry.getKey(),
							(double) stringObjectEntry.getValue() * count
									* countAdd);
				}

				if (!resultLearningObjectives.isEmpty()) {
					state.setProperty(rootGlpId, docId,
							GLPTopologyBuilder.LEARNING_OBJECTIVES,
							resultLearningObjectives);
				}

				Map value = new HashMap();
				value.put(GLPTopologyBuilder.COMPETENCIES, competencies);
				value.put(GLPTopologyBuilder.LEARNING_OBJECTIVES,
						learningObjectives);
				List<Object> ret = new ArrayList<>(5);
				ret.add(rootGlpId);
				ret.add(docId);
				ret.add(name);
				ret.add(property);
				ret.add(value);
				ret.add(count);
				// GLP_ID_KEY, ACTIVITY_ID_KEY _ NAME,
				// PROPERTY_KEY,VALUE_KEY, count
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