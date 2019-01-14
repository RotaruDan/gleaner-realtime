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
import org.apache.storm.trident.operation.Filter;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.tuple.TridentTuple;

import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

public class IsDirectChildTrace implements Filter {
	private static final Logger LOGGER = Logger
			.getLogger(IsDirectChildTrace.class.getName());
	public static final boolean LOG = true;

	private String traceKey, analyticsKey;

	/**
	 * Filters trace to be coming directly from a children of current node.
	 * 
	 * DIRECT child means that {@link TopologyBuilder#ORIGINAL_ID} must not be
	 * null AND must be in the {@link TopologyBuilder#CHILDREN} array of the
	 * analytics metadata of the current node.
	 * 
	 */
	public IsDirectChildTrace(String traceKey, String analyticsKey) {
		this.traceKey = traceKey;
		this.analyticsKey = analyticsKey;
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

			Object analyticsObject = objects.getValueByField(analyticsKey);

			if (!(analyticsObject instanceof Map)) {
				if (LOG) {
					LOGGER.info(analyticsKey + " field of tuple " + objects
							+ " is not a map, found: " + analyticsObject);
				}
				return false;
			}

			Map traceMap = (Map) traceObject;
			Map analyticsMap = (Map) analyticsObject;

			String comesFrom = (String) traceMap
					.get(TopologyBuilder.ORIGINAL_ID);
			List children = (List) analyticsMap.get(TopologyBuilder.CHILDREN);

			if (children == null || !children.contains(comesFrom)) {
				return false;
			} else {
				return !traceMap.get(TopologyBuilder.CHILD_ACTIVITY_ID_KEY)
						.equals(traceMap.get(TopologyBuilder.ACTIVITY_ID_KEY));
			}
		} catch (Exception ex) {
			LOGGER.info("Error unexpected exception, discarding "
					+ ex.toString());
			ex.printStackTrace();
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
