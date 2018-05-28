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
package es.eucm.rage.realtime.filters;

import es.eucm.rage.realtime.topologies.TopologyBuilder;
import org.apache.storm.trident.operation.Filter;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.tuple.TridentTuple;

import java.util.Map;
import java.util.logging.Logger;

public class ExtensionTypeFilter implements Filter {
	private static final Logger LOGGER = Logger
			.getLogger(ExtensionTypeFilter.class.getName());
	public static final boolean LOG = false;

	private final Class clazz;
	private String extensionKey;

	/**
	 * Filters a Trace TridentTuple depending if it has an extension 'extKey' if
	 * type 'clazz'
	 */
	public ExtensionTypeFilter(String extKey, Class clazz) {
		this.extensionKey = extKey;
		this.clazz = clazz;
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

			Object extObject = trace.get(TopologyBuilder.EXTENSIONS_KEY);

			if (!(extObject instanceof Map)) {
				if (LOG) {
					LOGGER.info(TopologyBuilder.EXTENSIONS_KEY
							+ " field of trace " + trace
							+ " is not a map, found: " + extObject);
				}
				return false;
			}

			Map<String, Object> ext = (Map) extObject;

			Object extKeyObject = ext.get(extensionKey);

			if (extKeyObject == null) {

				for (String key : ext.keySet()) {
					if (key.toLowerCase().contains(extensionKey.toLowerCase())) {
						extKeyObject = ext.get(key);
						break;
					}
				}

				if (extKeyObject == null) {
					if (LOG) {
						LOGGER.info(extensionKey + " extension of extensions "
								+ ext + " is null");
					}
					return false;
				}

				ext.put(extensionKey, extKeyObject);
			}

			if (!(clazz.isAssignableFrom(extKeyObject.getClass()))) {
				if (LOG) {
					LOGGER.info(extensionKey + " extension of extensions "
							+ ext + " is not a " + clazz + ", found: "
							+ extKeyObject.getClass());
				}
				return false;
			}

		} catch (Exception ex) {
			LOGGER.info("Error unexpected exception, discarding"
					+ ex.toString());
			return false;
		}
		return true;
	}

	@Override
	public void prepare(Map map, TridentOperationContext tridentOperationContext) {

	}

	@Override
	public void cleanup() {

	}
}
