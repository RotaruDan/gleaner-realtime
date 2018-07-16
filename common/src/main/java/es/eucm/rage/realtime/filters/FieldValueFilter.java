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

import org.apache.storm.trident.operation.BaseFilter;
import org.apache.storm.trident.tuple.TridentTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FieldValueFilter extends BaseFilter {

	/**
	 * @see java.io.Serializable
	 */
	private static final long serialVersionUID = -5139354257713725085L;

	private static final Logger LOGGER = LoggerFactory.getLogger(FieldValueFilter.class);

	private String field;

	private Object value;

	/**
	 * Filters a TridentTuple depending on the value of a given field
	 * 
	 * @param field
	 *            field key to extract from the {@link TridentTuple}
	 * @param value
	 *            value that must be matched with the value from the field from
	 *            the {@link TridentTuple}
	 */
	public FieldValueFilter(String field, Object value) {
		this.field = field;
		this.value = value;
	}

	@Override
	public boolean isKeep(TridentTuple objects) {
		try {
			return value.equals(objects.getValueByField(field));
		} catch (Exception ex) {
			LOGGER.error("Error comparing values => tuple filtered", ex);
			return false;
		}
	}
}
