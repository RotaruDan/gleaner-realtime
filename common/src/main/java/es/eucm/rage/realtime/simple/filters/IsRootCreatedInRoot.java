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
package es.eucm.rage.realtime.simple.filters;

import es.eucm.rage.realtime.topologies.TopologyBuilder;
import es.eucm.rage.realtime.utils.ESUtils;
import org.apache.storm.trident.operation.Filter;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.tuple.TridentTuple;

import java.util.Map;
import java.util.logging.Logger;

public class IsRootCreatedInRoot implements Filter {
	private static final Logger LOGGER = Logger
			.getLogger(IsRootCreatedInRoot.class.getName());
	public static final boolean LOG = true;

	private String traceKey;

	/**
	 * Filters a Trace TridentTuple to ses if it is a trace going to ROOT and
	 * has been created in the ROOT node as well.
	 * 
	 * 1) {@link TopologyBuilder#CHILD_ACTIVITY_ID_KEY} must not be null 2)
	 * {@link TopologyBuilder#ORIGINAL_ID} must not be null 3)
	 * {@link TopologyBuilder#ACTIVITY_ID_KEY} must not be null 4)
	 * {@link TopologyBuilder#GLP_ID_KEY} must not be null 5) ROOT_ID must equal
	 * {@link TopologyBuilder#ORIGINAL_ID} and
	 * {@link TopologyBuilder#CHILD_ACTIVITY_ID_KEY}
	 * 
	 */
	public IsRootCreatedInRoot(String traceKey) {
		this.traceKey = traceKey;
	}

	@Override
	public boolean isKeep(TridentTuple objects) {
		try {
			Object traceObject = objects.getValueByField(traceKey);

			if (!(traceObject instanceof Map)) {
				if (LOG) {
					LOGGER.info(traceKey + " field of tuple " + objects
							+ " is not a map, found: " + traceObject);
				}
				return false;
			}

			Map traceMap = (Map) traceObject;

			Object childActivityObject = traceMap
					.get(TopologyBuilder.CHILD_ACTIVITY_ID_KEY);

			if (childActivityObject == null) {
				// Is a leaf, it's not coming from a child
				return false;
			}

			Object originalObject = traceMap.get(TopologyBuilder.ORIGINAL_ID);

			if (originalObject == null) {
				// Is a leaf, it's not coming from a child
				return false;
			}

			Object actIdObject = traceMap.get(TopologyBuilder.ACTIVITY_ID_KEY);

			if (actIdObject == null) {
				// Is a leaf, it's not coming from a child
				return false;
			}

			Object glpObj = traceMap.get(TopologyBuilder.GLP_ID_KEY);

			if (glpObj == null) {
				// Not self-generated progress
				return false;
			}

			String glpRoot = ESUtils.getRootGLPId(glpObj.toString());

			return glpRoot.equals(originalObject)
					&& childActivityObject.equals(originalObject);
		} catch (Exception ex) {
			LOGGER.info("Error unexpected exception, discarding"
					+ ex.toString());
			return false;
		}
	}

	@Override
	public void prepare(Map map, TridentOperationContext tridentOperationContext) {

	}

	@Override
	public void cleanup() {

	}
}
