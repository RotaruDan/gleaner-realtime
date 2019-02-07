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
import es.eucm.rage.realtime.topologies.TopologyBuilder;
import es.eucm.rage.realtime.utils.ESUtils;
import org.apache.storm.trident.operation.CombinerAggregator;
import org.apache.storm.trident.tuple.TridentTuple;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Completed ANALYSIS - GLP.
 * 
 * Requires TopologyBuilder.VALUE_KEY TopologyBuilder.PROPERTY_KEY
 * TopologyBuilder.ACTIVITY_ID_KEY TopologyBuilder.GLP_ID_KEY
 * TopologyBuilder.TridentTraceKeys.NAME
 */
public class ContribSumAggregation implements
		CombinerAggregator<Map<String, Object>> {
	private static final Logger LOGGER = Logger
			.getLogger(ContribSumAggregation.class.getName());

	@Override
	public Map<String, Object> init(TridentTuple tuple) {
		try {
			Map contrib = (Map) tuple
					.getValueByField(TopologyBuilder.VALUE_KEY);
			Map result = new HashMap(contrib);
			Object actId;
			try {
				actId = tuple.getValueByField(TopologyBuilder.ACTIVITY_ID_KEY);
			} catch (NullPointerException exp) {
				actId = ESUtils.getRootGLPId(tuple.getValueByField(
						TopologyBuilder.GLP_ID_KEY).toString());
			}

			result.put(TopologyBuilder.ACTIVITY_ID_KEY, actId);
			result.put(TopologyBuilder.TridentTraceKeys.NAME, tuple
					.getValueByField(TopologyBuilder.TridentTraceKeys.NAME));
			result.put(TopologyBuilder.GLP_ID_KEY,
					tuple.getValueByField(TopologyBuilder.GLP_ID_KEY));
			result.put(TopologyBuilder.PROPERTY_KEY,
					tuple.getValueByField(TopologyBuilder.PROPERTY_KEY));
			return result;
		} catch (Exception ex) {
			LOGGER.info("Unexpected exception initializing");
			ex.printStackTrace();
			return new HashMap();
		}
	}

	@Override
	public Map<String, Object> combine(Map<String, Object> val1,
			Map<String, Object> val2) {

		Map<String, Object> res = new HashMap(val2);

		try {
			Map<String, Object> resCompetencies = (Map) res
					.get(GLPTopologyBuilder.COMPETENCIES);
			Map<String, Object> val1Competencies = (Map) val1
					.get(GLPTopologyBuilder.COMPETENCIES);
			if (val1Competencies != null) {
				for (Map.Entry<String, Object> stringObjectEntry : resCompetencies
						.entrySet()) {
					Object oldVal = val1Competencies.get(stringObjectEntry
							.getKey());
					if (oldVal != null) {
						try {
							resCompetencies.put(stringObjectEntry.getKey(),
									Numbers.add(stringObjectEntry.getValue(),
											oldVal));
						} catch (Exception nfex) {
							LOGGER.info("Number format exception parsing competencies, "
									+ "stringObjectEntry " + stringObjectEntry);
							nfex.printStackTrace();
						}
					}
				}
			}

			Map<String, Object> resLos = (Map) res
					.get(GLPTopologyBuilder.LEARNING_OBJECTIVES);
			Map<String, Object> val1Los = (Map) val1
					.get(GLPTopologyBuilder.LEARNING_OBJECTIVES);
			if (val1Los != null) {
				for (Map.Entry<String, Object> stringObjectEntry : resLos
						.entrySet()) {
					Object val1Object = val1Los.get(stringObjectEntry.getKey());
					if (val1Object != null) {
						try {
							resLos.put(stringObjectEntry.getKey(), Numbers.add(
									stringObjectEntry.getValue(), val1Object));
						} catch (Exception nfex) {
							LOGGER.info("Number format exception parsing learning objectives, "
									+ "stringObjectEntry " + stringObjectEntry);
							nfex.printStackTrace();
						}
					}
				}
			}
		} catch (Exception ex) {
			LOGGER.info("Unexpected exception combining");
			ex.printStackTrace();
		}
		return res;
	}

	@Override
	public Map<String, Object> zero() {
		return new HashMap();
	}

}