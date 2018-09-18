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
import es.eucm.rage.realtime.topologies.TopologyBuilder;
import org.apache.storm.trident.operation.Filter;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.tuple.TridentTuple;

import java.util.Map;
import java.util.logging.Logger;

public class PartialThresholdsFilter implements Filter {
	private static final Logger LOGGER = Logger
			.getLogger(PartialThresholdsFilter.class.getName());
	public static final boolean LOG = false;

	private String analyticsKey, traceKey;

	/**
	 * Filters a Trace TridentTuple depending if it has the correct partial
	 * thresholds
	 * 
	 */
	public PartialThresholdsFilter(String analyticsKey, String traceKey) {
		this.analyticsKey = analyticsKey;
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

			Float traceSscore;

			try {
				traceSscore = Float.parseFloat(scoreObject.toString());
			} catch (NumberFormatException nfex) {
				if (LOG) {
					LOGGER.info(TopologyBuilder.TridentTraceKeys.SCORE
							+ " field of trace " + traceObject
							+ " is not a FLOAT: " + scoreObject + " " + nfex);
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

			Object limitsObject = ((Map) analyticsObject)
					.get(GLPTopologyBuilder.LIMITS);

			if (!(limitsObject instanceof Map)) {
				if (LOG) {
					LOGGER.info(GLPTopologyBuilder.LIMITS + " field of tuple "
							+ objects + " is not a map, found: " + limitsObject);
				}
				return false;
			}

			Map limitsMap = (Map) limitsObject;

			Object partialThresholdsObject = limitsMap
					.get(GLPTopologyBuilder.PARTIAL_THRESHOLDS);

			if (!(partialThresholdsObject instanceof Map)) {
				if (LOG) {
					LOGGER.info(GLPTopologyBuilder.PARTIAL_THRESHOLDS
							+ " field of tuple " + objects
							+ " is not a map, found: "
							+ partialThresholdsObject);
				}
				return false;
			}

			Map partialThresholdsMap = (Map) partialThresholdsObject;

			Object scoreThresholdObject = partialThresholdsMap
					.get(GLPTopologyBuilder.PARTIAL_THRESHOLD_SCORE);

			if (scoreThresholdObject != null) {

				Float scoreThresholdFloat;
				try {
					scoreThresholdFloat = Float.parseFloat(scoreThresholdObject
							.toString());
					if (traceSscore > scoreThresholdFloat) {
						return true;
					}
				} catch (NumberFormatException nfexScore) {
					if (LOG) {
						LOGGER.info(GLPTopologyBuilder.PARTIAL_THRESHOLD_SCORE
								+ " field of trace " + traceObject
								+ " is not a FLOAT: " + scoreThresholdObject
								+ " " + nfexScore);
					}
				}
			}

			Object contributesThresholdObject = partialThresholdsMap
					.get(GLPTopologyBuilder.CONTRIBUTES);

			if (contributesThresholdObject != null) {

				Float contributesThresholdFloat;
				try {
					contributesThresholdFloat = Float
							.parseFloat(contributesThresholdObject.toString());
					if (traceSscore > contributesThresholdFloat) {
						return true;
					}
				} catch (NumberFormatException nfexScore) {
					if (LOG) {
						LOGGER.info(GLPTopologyBuilder.CONTRIBUTES
								+ " field of trace " + traceObject
								+ " is not a FLOAT: "
								+ contributesThresholdObject + " " + nfexScore);
					}
				}
			}

			Object learningObjectivesThresholdObject = partialThresholdsMap
					.get(GLPTopologyBuilder.LEARNING_OBJECTIVES);

			if (learningObjectivesThresholdObject != null) {
				Float learningObjectivesThresholdFloat;
				try {
					learningObjectivesThresholdFloat = Float
							.parseFloat(learningObjectivesThresholdObject
									.toString());
					if (traceSscore > learningObjectivesThresholdFloat) {
						return true;
					}
				} catch (NumberFormatException nfexScore) {
					if (LOG) {
						LOGGER.info(GLPTopologyBuilder.LEARNING_OBJECTIVES
								+ " field of trace " + traceObject
								+ " is not a FLOAT: "
								+ learningObjectivesThresholdObject + " "
								+ nfexScore);
					}
				}
			}

			if (LOG) {
				LOGGER.info(GLPTopologyBuilder.PARTIAL_THRESHOLDS
						+ " have not been satisfied, discarding trace.");
			}
			return false;
		} catch (Exception ex) {
			if (LOG) {
				LOGGER.info("Error unexpected exception, discarding "
						+ ex.toString());
			}
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
