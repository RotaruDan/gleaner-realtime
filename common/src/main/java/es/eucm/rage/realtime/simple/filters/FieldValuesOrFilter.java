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

import org.apache.storm.trident.operation.Filter;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.tuple.TridentTuple;

import java.util.Map;
import java.util.logging.Logger;

public class FieldValuesOrFilter implements Filter {
	private static final Logger LOGGER = Logger
			.getLogger(FieldValuesOrFilter.class.getName());

	private String field;

	private Object[] values;

	/**
	 * Filters a TridentTuple depending on the value of multiple filters, if any
	 * of the given values the value from the provided field (OR).
	 * 
	 * @param field
	 *            field key to extract from the {@link TridentTuple}
	 * @param values
	 *            values that must be matched with the value from the field from
	 *            the {@link TridentTuple}
	 */
	public FieldValuesOrFilter(String field, Object... values) {
		this.field = field;
		this.values = values;
	}

	@Override
	public boolean isKeep(TridentTuple objects) {
		try {
			Object valueField = objects.getValueByField(field);
			for (int i = 0; i < values.length; ++i) {
				if (values[i].equals(valueField)) {
					return true;
				}
			}
		} catch (Exception ex) {
			LOGGER.info("Error unexpected exception, discarding "
					+ ex.toString());
			return false;
		}

		return false;
	}

	@Override
	public void prepare(Map map, TridentOperationContext tridentOperationContext) {

	}

	@Override
	public void cleanup() {

	}
}
