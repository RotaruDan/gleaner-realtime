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

import org.apache.storm.trident.operation.Consumer;
import org.apache.storm.trident.tuple.TridentTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogConsumer implements Consumer {

	/**
	 * @see java.io.Serializable
	 */
	private static final long serialVersionUID = -262487619942683849L;

	private static final Logger LOGGER = LoggerFactory.getLogger(LogConsumer.class);

	private String prefix;
	
	public LogConsumer(String prefix) {
		this.prefix = prefix;
	}

	@Override
	public void accept(TridentTuple tridentTuple) {
		try {
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("{} - {}", prefix, tridentTuple);
			}
		} catch (Exception ex) {
			LOGGER.error("Error consuming data", ex);
		}
	}
}
