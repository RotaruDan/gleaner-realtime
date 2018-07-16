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

import java.util.Map;

import org.apache.storm.trident.operation.BaseFilter;
import org.apache.storm.trident.tuple.TridentTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import es.eucm.rage.realtime.topologies.TopologyBuilder;

public class ExtensionTypeFilter extends BaseFilter {
	
	/**
	 * @see java.io.Serializable
	 */
	private static final long serialVersionUID = 1843150241877981640L;

	private static final Logger LOGGER = LoggerFactory.getLogger(ExtensionTypeFilter.class);

	private final Class<?> clazz;
	
	private String extensionKey;

	/**
	 * Filters a Trace TridentTuple depending if it has an extension 'extKey' if
	 * type 'clazz'
	 */
	public ExtensionTypeFilter(String extKey, Class<?> clazz) {
		this.extensionKey = extKey;
		this.clazz = clazz;
	}

	// XXX Review, too much checks
	@Override
	public boolean isKeep(TridentTuple objects) {
		try {
			Object traceObject = objects
					.getValueByField(TopologyBuilder.TRACE_KEY);

			if (!(traceObject instanceof Map)) {
				if (LOGGER.isDebugEnabled()) {
					LOGGER.debug("{} field of tuple {} is not a map, found: {} => tuple filtered", TopologyBuilder.TRACE_KEY, objects, traceObject);
				}
				return false;
			}

			@SuppressWarnings("unchecked")
			Object outObject = ((Map<String, Object>) traceObject).get(TopologyBuilder.OUT_KEY);

			if (!(outObject instanceof Map)) {
				if (LOGGER.isDebugEnabled()) {
					LOGGER.debug("{} field of type {} is not a map, found: {} => tuple filtered",	TopologyBuilder.OUT_KEY, objects, traceObject);
				}
				return false;
			}

			@SuppressWarnings("unchecked")
			Map<String, Object> trace = (Map<String, Object>) outObject;

			Object extObject = trace.get(TopologyBuilder.EXTENSIONS_KEY);

			if (!(extObject instanceof Map)) {
				if (LOGGER.isDebugEnabled()) {
					LOGGER.debug("{} field of trace {} is not a map, found: {} => tuple filtered",	TopologyBuilder.EXTENSIONS_KEY, trace, extObject);
				}
				return false;
			}

			@SuppressWarnings("unchecked")
			Map<String, Object> ext = (Map<String, Object>) extObject;

			Object extKeyObject = ext.get(extensionKey);

			if (extKeyObject == null) {
				if (LOGGER.isDebugEnabled()) {
					LOGGER.debug("{} extension of extensions {} is null => tuple filtered",	extensionKey, ext);
				}
				return false;
			}

			if (!(clazz.isAssignableFrom(extKeyObject.getClass()))) {
				if (LOGGER.isDebugEnabled()) {
					LOGGER.debug("{} extension of extensions {} is not a {} => tuple filtered",	extensionKey, ext, clazz);
				}
				return false;
			}

		} catch (Exception ex) {
			LOGGER.error("Error comparing values => tuple filtered", ex);
			return false;
		}
		return true;
	}

}
