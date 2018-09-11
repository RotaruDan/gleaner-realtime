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

import java.util.Map;
import java.util.logging.Logger;

public class HasActivityIdFilter implements Filter {
	private static final Logger LOGGER = Logger
			.getLogger(HasActivityIdFilter.class.getName());
	public static final boolean LOG = false;

	/**
	 * Filters a Trace TridentTuple depending if it has the "activityId",
	 * "classId" and "activityName" fields
	 */
	public HasActivityIdFilter() {
	}

	@Override
	public boolean isKeep(TridentTuple objects) {
		try {
			Object traceObject = objects
					.getValueByField(TopologyBuilder.TRACE_KEY);

			if (!(traceObject instanceof Map)) {
				if (LOG) {
					LOGGER.info(TopologyBuilder.TRACE_KEY + " field of tuple "
							+ objects + " is not a map, found: " + traceObject);
				}
				return false;
			}

			Map trace = (Map) traceObject;

			Object activityIdObject = trace
					.get(TopologyBuilder.ACTIVITY_ID_KEY);

			if (activityIdObject == null) {
				if (LOG) {
					LOGGER.info(TopologyBuilder.ACTIVITY_ID_KEY + " is null");
				}
				return false;
			}

			if (!(activityIdObject instanceof String)) {
				if (LOG) {
					LOGGER.info(TopologyBuilder.ACTIVITY_ID_KEY
							+ " is a String, is: "
							+ activityIdObject.getClass());
				}
				return false;
			}

			Object activityNameObject = trace
					.get(TopologyBuilder.ACTIVITY_NAME_KEY);

			if (activityNameObject == null) {
				if (LOG) {
					LOGGER.info(TopologyBuilder.ACTIVITY_NAME_KEY + " is null");
				}
				return false;
			}

			if (!(activityNameObject instanceof String)) {
				if (LOG) {
					LOGGER.info(TopologyBuilder.ACTIVITY_NAME_KEY
							+ " is a String, is: "
							+ activityNameObject.getClass());
				}
				return false;
			}

			Object classIdObject = trace.get(TopologyBuilder.CLASS_ID);

			if (classIdObject == null) {
				if (LOG) {
					LOGGER.info(TopologyBuilder.CLASS_ID + " is null");
				}
				return false;
			}

			if (!(classIdObject instanceof String)) {
				if (LOG) {
					LOGGER.info(TopologyBuilder.CLASS_ID + " is a String, is: "
							+ classIdObject.getClass());
				}
				return false;
			}

			Object childActivityObject = trace
					.get(TopologyBuilder.CHILD_ACTIVITY_ID_KEY);

			if (childActivityObject != null) {
				// Is has been bubbled it will have a CHILD_ACTIVITY_ID_KEY
				// AND the CHILD_ACTIVITY_ID_KEY will be different
				// than the current ACTIVITY_ID_KEY
				if (LOG) {
					LOGGER.info(TopologyBuilder.CHILD_ACTIVITY_ID_KEY
							+ " is not null, trace has been bubbled, not overposting to the class, value is: "
							+ childActivityObject.getClass());
				}
				return false;
			}

			return true;
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
