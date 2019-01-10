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
package es.eucm.rage.realtime.functions;

import com.rits.cloning.Cloner;
import es.eucm.rage.realtime.states.elasticsearch.EsState;
import es.eucm.rage.realtime.topologies.TopologyBuilder;
import es.eucm.rage.realtime.utils.ESUtils;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.state.BaseQueryFunction;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class BuildWeightsValuesFromElasticIndex extends
		BaseQueryFunction<EsState, List> {

	private static final Logger LOGGER = LoggerFactory
			.getLogger(BuildWeightsValuesFromElasticIndex.class);
	private static final boolean LOG = true;

	private String glpIdKey, weightsKey, nameKey;
	private Cloner cloner;

	@Override
	public void prepare(Map conf, TridentOperationContext context) {
		super.prepare(conf, context);
		cloner = new Cloner();
	}

	/**
	 * Iterates over the "weights" objects's children and retrieves all the last
	 * values from the "results-rootId" index (objectId:
	 * "agg_childActivityId_username") IF the "value" field is not set already.
	 * 
	 * If values are not available, the value is set to "0".
	 */
	public BuildWeightsValuesFromElasticIndex(String glpIdKey, String nameKey,
			String weightsKey) {
		this.glpIdKey = glpIdKey;
		this.weightsKey = weightsKey;
		this.nameKey = nameKey;
	}

	@Override
	public List<List> batchRetrieve(EsState indexState,
			List<TridentTuple> inputs) {
		List<List> res = new LinkedList<>();
		try {
			for (TridentTuple input : inputs) {

				Object glpIdObj = input.getValueByField(glpIdKey);

				if (glpIdObj == null) {
					if (LOG) {
						LOGGER.info("glpId has not been found for current trace "
								+ input + " -> DISCARDING!");
					}
					continue;
				}

				String glpId = glpIdObj.toString();

				Object nameObj = input.getValueByField(nameKey);

				if (nameObj == null) {
					if (LOG) {
						LOGGER.info("nameObj has not been found for current trace "
								+ input + " -> DISCARDING!");
					}
					continue;
				}

				String name = nameObj.toString();

				Object weightsObj = input.getValueByField(weightsKey);

				if (!(weightsObj instanceof List)) {
					if (LOG) {
						LOGGER.info("weightsObj has not been found for current trace "
								+ input
								+ " -> DISCARDING! or is not instance of List "
								+ weightsObj);
					}
					continue;
				}

				List weights = (List) weightsObj;

				for (Object weightObj : weights) {
					if (!(weightObj instanceof Map)) {
						if (LOG) {
							LOGGER.info("weight has not been found for current weights "
									+ weights
									+ " -> DISCARDING! or is not instance of Map "
									+ weightObj);
						}
						continue;
					}

					Map<String, Object> weight = (Map) weightObj;

					Object childrenObj = weight
							.get(TopologyBuilder.OPERATION_CHILDREN_KEY);
					if (!(childrenObj instanceof List)) {
						if (LOG) {
							LOGGER.info("childrenObj has not been found for current weight "
									+ weight
									+ " -> DISCARDING! or is not instance of List "
									+ childrenObj);
						}
						continue;
					}

					List children = (List) childrenObj;

					for (Object childObj : children) {
						if (!(childObj instanceof Map)) {
							if (LOG) {
								LOGGER.info("childObj has not been found for current child "
										+ children
										+ " -> DISCARDING! or is not instance of Map "
										+ childObj);
							}
							continue;
						}

						Map<String, Object> child = (Map) childObj;

						Object valueObj = child
								.get(TopologyBuilder.OPERATION_CHILDREN_VALUE_KEY);

						if (valueObj != null) {
							// Value was present in current trace, and was set
							// in
							// HasOperationAndChildrenAreValidAndParseValues
							// class
							continue;
						}

						Object varNameObj = child
								.get(TopologyBuilder.OPERATION_NAME_KEY);
						if (varNameObj == null) {
							if (LOG) {
								LOGGER.info("varNameObj has not been found for current child "
										+ child + " -> DISCARDING!");
							}
							continue;
						}

						String varName = varNameObj.toString();

						// we have to retrieve the value of the current children
						// from the operation

						Object idObj = child
								.get(TopologyBuilder.OPERATION_CHILD_ID_KEY);
						if (idObj == null) {
							if (LOG) {
								LOGGER.info("idObj has not been found for current child "
										+ child + " -> DISCARDING!");
							}
							continue;
						}

						String id = idObj.toString();

						String resultsIndex = ESUtils.getResultsIndex(ESUtils
								.getRootGLPId(glpId));
						String type = "_all";
						String docId = "agg_" + id + "_" + name;

						Object result = indexState.getFromIndex(resultsIndex,
								type, docId);
						if (!(result instanceof Map)) {
							// Set value to "0"
							child.put(
									TopologyBuilder.OPERATION_CHILDREN_VALUE_KEY,
									0f);
						} else {
							Map<String, Object> resultMap = (Map) result;

							Object varValueObj = resultMap.get(varName);
							if (varValueObj == null) {
								child.put(
										TopologyBuilder.OPERATION_CHILDREN_VALUE_KEY,
										0f);
							} else {
								child.put(
										TopologyBuilder.OPERATION_CHILDREN_VALUE_KEY,
										varValueObj);
							}
						}
					}
				}
				res.add(cloner.deepClone(weights));
			}
		} catch (Exception ex) {
			LOGGER.error(
					"error while executing BuildWeightsValues from Elastic Index / GetFromElasticIndex to elasticsearch,",
					ex);
		}
		return res;
	}

	@Override
	public void execute(TridentTuple objects, List tl,
			TridentCollector tridentCollector) {
		tridentCollector.emit(new Values(tl));
	}
}