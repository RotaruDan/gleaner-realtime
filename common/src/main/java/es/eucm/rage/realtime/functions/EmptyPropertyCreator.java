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
import java.util.logging.Logger;

public class EmptyPropertyCreator implements Function {
	private static final Logger LOGGER = Logger
			.getLogger(EmptyPropertyCreator.class.getName());

	private String valueField;

	/**
	 * Creates a new {@link TridentTuple} depending on the value of the
	 * valueField and the keys field directly provided (directly concatenated).
	 * 
	 * @param valueField
	 *            Extracts the value of this field from the {@link TridentTuple}
	 *            . Emitted as the second parameter of the result
	 *            {@link TridentTuple}.
	 */
	public EmptyPropertyCreator(String valueField) {
		this.valueField = valueField;
	}

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		try {
			collector.emit(Arrays.asList(tuple.getValueByField(valueField)));
		} catch (Exception ex) {
			LOGGER.info("Error unexpected exception, discarding "
					+ ex.toString());
			ex.printStackTrace();
		}
	}

	@Override
	public void prepare(Map conf, TridentOperationContext context) {
	}

	@Override
	public void cleanup() {
	}
}
