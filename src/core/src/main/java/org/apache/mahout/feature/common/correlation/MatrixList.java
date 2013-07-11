/* Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.mahout.feature.common.correlation;

import java.util.ArrayList;
import java.util.Iterator;

public class MatrixList {

	protected String name;
	
	protected ArrayList<RowElement> elements;
	protected ArrayList<RowElement> uniqueRows;
	protected ArrayList<RowElement> uniqueCols;
	
	protected long occurrences;
	
	public MatrixList() {
		elements = new ArrayList<RowElement>();
		
		uniqueRows = new ArrayList<RowElement>();
		uniqueCols = new ArrayList<RowElement>();
		
		this.occurrences = 0;
	}
	
	public String getName() {
		return this.name;
	}
	
	public void setName(String name) {
		this.name = name;
	}
	
	public void store(int a, int b) {
		this.storeElement(a, b);
		this.storeUniqueRow(a);
		this.storeUniqueCol(b);
		
		this.occurrences = this.occurrences + 1;
	}
	
	private void storeElement(int row, int col) {
		boolean isNew = true;
		for (RowElement e: elements) {
			if (e.getRow() == row && e.getCol() == col) {
				isNew = false;
				e.increaseOccurrence();
				break;
			}
		}
		if (isNew) {
			elements.add(new RowElement(row, col));
		}
	}
	
	private void storeUniqueRow(int row) {
		boolean isNew = true;
		for (RowElement e: uniqueRows) {
			if (e.getRow() == row) {
				isNew = false;
				e.increaseOccurrence();
				break;
			}
		}
		if (isNew) {
			uniqueRows.add(new RowElement(row, -1));
		}
	}
	
	private void storeUniqueCol(int col) {
		boolean isNew = true;
		for (RowElement e: uniqueCols) {
			if (e.getCol() == col) {
				isNew = false;
				e.increaseOccurrence();
				break;
			}
		}
		if (isNew) {
			uniqueCols.add(new RowElement(-1, col));
		}
	}
	
	public Iterator elementsIterator() {
		return elements.iterator();
	}
	
	public long getOccurrences() {
		return this.occurrences;
	}
	
	public long getRowOccurrences(int row) {
		for (RowElement e: uniqueRows) {
			if (e.getRow() == row) {
				return e.getOccurrences();
			}
		}
		return -1;
	}
	
	public long getColOccurrences(int col) {
		for (RowElement e: uniqueCols) {
			if (e.getCol() == col) {
				return e.getOccurrences();
			}
		}
		return -1;
	}
}
