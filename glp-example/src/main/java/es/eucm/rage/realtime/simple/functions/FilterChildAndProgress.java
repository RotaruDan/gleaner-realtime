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

import es.eucm.rage.realtime.simple.topologies.GLPTopologyBuilder;
import es.eucm.rage.realtime.topologies.TopologyBuilder;
import org.apache.storm.trident.operation.Filter;
import org.apache.storm.trident.operation.Function;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.tuple.TridentTuple;

import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

public class FilterChildAndProgress implements Function {
	private static final Logger LOGGER = Logger
			.getLogger(FilterChildAndProgress.class.getName());
	public static final boolean LOG = true;

	private String analyticsKey, traceKey;

	/**
	 * Filters a Trace TridentTuple depending if it is achild of the current analytics
	 *
	 */
	public FilterChildAndProgress(String analyticsKey, String traceKey) {
		this.analyticsKey = analyticsKey;
		this.traceKey = traceKey;
	}

	@Override
	public void execute(TridentTuple objects, TridentCollector collector) {
		try {
			Object traceObject = objects.getValueByField(traceKey);

			if (!(traceObject instanceof Map)) {
				if (LOG) {
					LOGGER.info(traceKey + " field of tuple " + objects
							+ " is not a map, found: " + traceObject);
				}
				return;
			}

			Object childIdObject = ((Map) traceObject).get(TopologyBuilder.CHILD_ACTIVITY_ID_KEY);

			if (childIdObject == null) {
				if (LOG) {
					LOGGER.info(TopologyBuilder.CHILD_ACTIVITY_ID_KEY + " field of tuple "
							+ objects + " is null");
				}
				return;
			}

			String childId = childIdObject.toString();

			Object analyticsObject = objects.getValueByField(analyticsKey);

			if (!(analyticsObject instanceof Map)) {
				if (LOG) {
					LOGGER.info(analyticsKey + " field of tuple " + objects
							+ " is not a map, found: " + analyticsObject);
				}
				return;
			}

			Object childrenObject = ((Map) analyticsObject)
					.get(GLPTopologyBuilder.CHILDREN);

			if (!(childrenObject instanceof List)) {
				if (LOG) {
					LOGGER.info(GLPTopologyBuilder.CHILDREN + " field of tuple "
							+ objects + " is not an Array, found: " + childrenObject);
				}
				return;
			}

			List children = (List) childrenObject;

			for(int i = 0; i < children.size(); ++i) {
				Object childObj = children.get(i);
				if(childObj != null) {
					if(childObj.toString().equals(childId)) {

						// TODO emit
						// TODO compute progress

					}
				}
			}

			return;
		} catch (Exception ex) {
			if (LOG) {
				LOGGER.info("Error unexpected exception, discarding "
						+ ex.toString());
			}
			return;
		}
	}

	@Override
	public void prepare(Map conf, TridentOperationContext tridentOperationContext) {

	}

	@Override
	public void cleanup() {

	}
}
