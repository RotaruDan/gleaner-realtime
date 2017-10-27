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
package es.eucm.rage.realtime.functions;

import org.apache.storm.trident.operation.Function;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.tuple.TridentTuple;

import java.util.Arrays;
import java.util.Map;

public class PropertyCreator implements Function {

	private String valueField;

	private String[] keysField;

	/**
	 * Creates a new {@link TridentTuple} depending on the value of the
	 * valueField and the keysField provided (concatenated)
	 * 
	 * @param valueField
	 *            Extracts the value of this field from the {@link TridentTuple}
	 *            . Emitted as the second parameter of the result
	 *            {@link TridentTuple}.
	 * @param keysField
	 *            builds the resulting keys by concatenating the resulting
	 *            String of this keys from the {@link TridentTuple} see
	 *            {@link PropertyCreator#toPropertyKey(TridentTuple)}. Emitted
	 *            as the first parameter of the result {@link TridentTuple}.
	 */
	public PropertyCreator(String valueField, String... keysField) {
		this.valueField = valueField;
		this.keysField = keysField;
	}

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		collector.emit(Arrays.asList(toPropertyKey(tuple),
				tuple.getValueByField(valueField)));
	}

	private String toPropertyKey(TridentTuple tuple) {
		String result = "";
		for (String key : keysField) {
			result += tuple.getStringByField(key) + ".";
		}
		return result.substring(0, result.length() - 1);
	}

	@Override
	public void prepare(Map conf, TridentOperationContext context) {
	}

	@Override
	public void cleanup() {
	}
}
