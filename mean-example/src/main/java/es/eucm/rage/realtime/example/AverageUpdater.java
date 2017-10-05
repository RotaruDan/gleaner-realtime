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
package es.eucm.rage.realtime.example;

import es.eucm.rage.realtime.topologies.TopologyBuilder;
import es.eucm.rage.realtime.states.elasticsearch.EsState;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.state.StateUpdater;
import org.apache.storm.trident.tuple.TridentTuple;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AverageUpdater implements StateUpdater<EsState> {

	public static final String SUM_KEY = "sum";
	public static final String COUNT_KEY = "count";
	public static final String AVG_KEY = "avg";

	@Override
	public void updateState(EsState state, List<TridentTuple> tuples,
			TridentCollector collector) {
		for (TridentTuple tuple : tuples) {

			// Get stored value of the average computation state
			String index = tuple.get(0).toString();
			String type = tuple.get(1).toString();
			String id = tuple.get(2).toString();
			Map mean = (Map) state.getFromIndex(index, type, id);
			if (mean == null) {
				mean = new HashMap<>(3);
			}

			// Compute the new mean value
			double count;
			double sum;
			double score = tuple
					.getDoubleByField(TopologyBuilder.TridentTraceKeys.SCORE
							+ "-double");
			Object countObject = mean.get(COUNT_KEY);
			if (countObject == null) {
				countObject = 0d;
			}
			count = (double) countObject + 1d;
			mean.put(COUNT_KEY, count);

			Object sumObject = mean.get(SUM_KEY);
			if (sumObject == null) {
				sumObject = 0d;
			}
			sum = (double) sumObject + score;
			mean.put(SUM_KEY, sum);

			mean.put(AVG_KEY, sum / count);

			// Store the new value of the average computation state
			state.setOnIndex(index, type, id, mean);
		}
	}

	@Override
	public void prepare(Map conf, TridentOperationContext context) {

	}

	@Override
	public void cleanup() {

	}
}
