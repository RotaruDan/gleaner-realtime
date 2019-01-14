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

import es.eucm.rage.realtime.topologies.TopologyBuilder;
import es.eucm.rage.realtime.utils.ESUtils;
import org.apache.storm.trident.operation.Function;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.tuple.TridentTuple;

import java.util.ArrayList;
import java.util.Map;
import java.util.logging.Logger;

public class RootGlpIdFieldExtractor implements Function {
	private static final Logger LOGGER = Logger
			.getLogger(RootGlpIdFieldExtractor.class.getName());
	public static final boolean LOG = false;

	/**
	 * Extracts field "glpId" from a "trace" tuple.
	 * 
	 * Requires {@link TopologyBuilder#TRACE_KEY} to extract the key
	 * {@link TopologyBuilder#GLP_ID_KEY}.
	 */
	public RootGlpIdFieldExtractor() {

	}

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		try {

			Object traceObject = tuple
					.getValueByField(TopologyBuilder.TRACE_KEY);
			if (!(traceObject instanceof Map)) {
				if (LOG) {
					LOGGER.info(TopologyBuilder.TRACE_KEY + " field of tuple "
							+ tuple + " is not a map, found: " + traceObject);
				}
				return;
			}

			Map traceMap = (Map) traceObject;

			Object glpIdObject = traceMap.get(TopologyBuilder.GLP_ID_KEY);

			if (glpIdObject == null || glpIdObject.toString().isEmpty()) {
				glpIdObject = traceMap.get(TopologyBuilder.ORIGINAL_ID);
				if (glpIdObject == null || glpIdObject.toString().isEmpty()) {
					glpIdObject = traceMap.get(TopologyBuilder.ACTIVITY_ID_KEY);
				}
			} else {
				glpIdObject = ESUtils.getRootGLPId(glpIdObject.toString());
			}
			ArrayList<Object> object = new ArrayList<Object>(1);
			object.add(glpIdObject);
			collector.emit(object);
		} catch (Exception ex) {
			LOGGER.info("Error unexpected exception, discarding "
					+ ex.toString());
			LOGGER.info(tuple.toString());
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