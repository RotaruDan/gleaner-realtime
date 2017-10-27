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
package es.eucm.rage.realtime.utils;

import java.io.Serializable;

/**
 * This class should be used to wrap data required to index a document.
 * 
 * @param <T>
 *            type of the underlying document
 */
public class Document<T> implements Serializable {

	private static final long serialVersionUID = 1L;

	/**
	 * The source document
	 */
	private T source;
	/**
	 * The document id
	 */
	private String id;

	/**
	 * The document type
	 */
	private String type;

	/**
	 * The document index prefix If its null no prefix will be added to the
	 * writing prefix, otherwise the indexPrefix will be build as follows:
	 * 
	 * "indexPrefix" + "-" + {@link ESUtils#getTracesIndex(String)} == >
	 * "indexPrefixValue-tracesIndexValue"
	 */
	private String indexPrefix;

	private String index;

	public Document(T source, String id, String type, String indexPrefix,
			String index) {
		this.source = source;
		this.id = id;
		this.type = type;
		this.indexPrefix = indexPrefix;
		this.index = index;
	}

	public T getSource() {
		return this.source;
	}

	public String getId() {
		return this.id;
	}

	public String getType() {
		return type;
	}

	public String getIndexPrefix() {
		return indexPrefix;
	}

	public String getIndex() {
		return index;
	}

	@Override
	public String toString() {
		String res = "[Source: " + source.toString() + ", Id: " + id + "]";
		return res;
	}
}
