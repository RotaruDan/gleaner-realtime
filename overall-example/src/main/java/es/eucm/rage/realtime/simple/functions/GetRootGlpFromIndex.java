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
package es.eucm.rage.realtime.simple.functions;

import es.eucm.rage.realtime.states.elasticsearch.EsState;
import es.eucm.rage.realtime.topologies.TopologyBuilder;
import es.eucm.rage.realtime.utils.ESUtils;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.state.BaseQueryFunction;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class GetRootGlpFromIndex extends BaseQueryFunction<EsState, Map> {

	private static final Logger LOG = LoggerFactory
			.getLogger(GetRootGlpFromIndex.class);

	public GetRootGlpFromIndex() {
	}

	@Override
	public List<Map> batchRetrieve(EsState indexState, List<TridentTuple> inputs) {
		List<Map> res = new LinkedList<>();
		try {
			for (TridentTuple input : inputs) {
				String index = input
						.getStringByField(TopologyBuilder.GLP_ID_KEY);
				String type = "_all";
				String id = ESUtils.getRootGLPId(index);
				res.add(indexState.getFromIndex(index, type, id));
			}
		} catch (Exception ex) {
			LOG.error(
					"error while executing batchRetrieve GetRootGlpFromIndex to elasticsearch,",
					ex);
			ex.printStackTrace();
		}
		return res;
	}

	@Override
	public void execute(TridentTuple objects, Map tl,
			TridentCollector tridentCollector) {
		tridentCollector.emit(new Values(tl));
	}
}