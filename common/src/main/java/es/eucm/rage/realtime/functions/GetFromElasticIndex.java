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

import es.eucm.rage.realtime.states.elasticsearch.EsState;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.state.BaseQueryFunction;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class GetFromElasticIndex extends BaseQueryFunction<EsState, Map> {

	private static final Logger LOG = LoggerFactory
			.getLogger(GetFromElasticIndex.class);

	private String indexPrefix, indexKey, typeKey, idKey, prefixId;

	public GetFromElasticIndex(String indexKey, String typeKey, String idKey) {
		this("", indexKey, typeKey, idKey);
	}

	public GetFromElasticIndex(String indexPrefix, String indexKey,
			String typeKey, String idKey) {
		this(indexPrefix, indexKey, typeKey, idKey, "");
	}

	public GetFromElasticIndex(String indexPrefix, String indexKey,
			String typeKey, String idKey, String prefixId) {
		super();
		this.indexPrefix = indexPrefix;
		this.indexKey = indexKey;
		this.typeKey = typeKey;
		this.idKey = idKey;
		this.prefixId = prefixId;
	}

	@Override
	public List<Map> batchRetrieve(EsState indexState, List<TridentTuple> inputs) {
		List<Map> res = new LinkedList<>();
		try {
			for (TridentTuple input : inputs) {
				String index = "_all";
				if (indexKey != null) {
					index = indexPrefix + input.getStringByField(indexKey);
				}
				String type = "_all";
				if (typeKey != null) {
					type = input.getStringByField(typeKey);
				}
				String id = "_all";
				if (idKey != null) {
					id = prefixId + input.getStringByField(idKey);
				}
				res.add(indexState.getFromIndex(index, type, id));
			}
		} catch (Exception ex) {
			LOG.error(
					"error while executing batchRetrieve GetFromElasticIndex to elasticsearch,",
					ex);
		}
		return res;
	}

	@Override
	public void execute(TridentTuple objects, Map tl,
			TridentCollector tridentCollector) {
		tridentCollector.emit(new Values(tl));
	}
}