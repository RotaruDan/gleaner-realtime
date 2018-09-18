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

import es.eucm.rage.realtime.simple.topologies.GLPTopologyBuilder;
import org.apache.storm.trident.operation.Filter;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.tuple.TridentTuple;

import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

public class IsCompletedAnalytics implements Filter {
	private static final Logger LOGGER = Logger
			.getLogger(IsCompletedAnalytics.class.getName());
	public static final boolean LOG = true;
	private final boolean completed;

	private String analyticsKey;
	private String gameplayKey;

	/**
	 * Filters an Analytics if it's completed or not
	 * 
	 */
	public IsCompletedAnalytics(String analyticsKey, String gameplayKey,
			boolean completed) {
		this.analyticsKey = analyticsKey;
		this.gameplayKey = gameplayKey;
		this.completed = completed;
	}

	@Override
	public boolean isKeep(TridentTuple objects) {
		try {
			Object gameplayObject = objects.getValueByField(gameplayKey);
			if (gameplayObject == null) {
				if (LOG) {
					LOGGER.info(gameplayKey + " field of tuple " + objects
							+ " is not found");
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

			Map analyticsMap = (Map) analyticsObject;

			Object completedObject = analyticsMap
					.get(GLPTopologyBuilder.COMPLETED_KEY);

			if (completedObject instanceof List) {
				// Is completed if the completed value is true
				return ((List) completedObject).contains(gameplayObject) == completed;
			}

			return !completed;
		} catch (Exception ex) {
			LOGGER.info("Error unexpected exception, discarding"
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
