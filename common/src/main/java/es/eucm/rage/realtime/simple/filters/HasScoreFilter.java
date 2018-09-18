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

public class HasScoreFilter implements Filter {
	private static final Logger LOGGER = Logger.getLogger(HasScoreFilter.class
			.getName());
	public static final boolean LOG = false;

	/**
	 * Filters a Trace TridentTuple depending if it has the "score" field
	 */
	public HasScoreFilter() {
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

			Object outObject = ((Map) traceObject).get(TopologyBuilder.OUT_KEY);

			if (!(outObject instanceof Map)) {
				if (LOG) {
					LOGGER.info(TopologyBuilder.OUT_KEY + " field of tuple "
							+ objects + " is not a map, found: " + traceObject);
				}
				return false;
			}

			Map trace = (Map) outObject;

			Object scoreObject = trace
					.get(TopologyBuilder.TridentTraceKeys.SCORE);

			if (scoreObject == null) {
				if (LOG) {
					LOGGER.info(TopologyBuilder.TridentTraceKeys.SCORE
							+ " is null");
				}
				return false;
			}

			try {
				Float.parseFloat(scoreObject.toString());
				return true;
			} catch (NumberFormatException nfe) {
				if (LOG) {
					LOGGER.info(TopologyBuilder.TridentTraceKeys.SCORE
							+ " field of trace " + traceObject
							+ " is not a number: " + scoreObject);
				}
			}
			return false;
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
