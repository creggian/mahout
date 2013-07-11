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

import org.apache.mahout.math.Vector;
import java.util.Iterator;
import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;
import java.lang.Math;

import org.apache.mahout.feature.common.correlation.MatrixList;
import org.apache.mahout.feature.common.correlation.RowElement;

public class MutualInformation {
	
	private int instanceNumber;
	
	public MutualInformation() {
	}
	
	public int getKey(Integer[] array, Integer value) {
		
		for (int i=0; i<array.length; i++) {
			if (array[i].intValue() == value.intValue()) {
				return i;
			}
		}
		
		return -1;
	}
	
	public ArrayList<Integer> getUniques(Vector v) {
		
		ArrayList<Integer> uniques = new ArrayList<Integer>();
		Iterator<Vector.Element> i = v.iterator();
		
		while (i.hasNext()) {
			Vector.Element element = i.next();
			Integer e = new Integer((int) element.get());
			
			if (!uniques.contains(e)) {
				uniques.add(e);
			}
		}
		
		return uniques;
	}
	
	public void resetMatrix(int[][] matrix, int xlength, int ylength) {
		for (int i=0; i<xlength; i++) {
			for (int j=0; j<ylength; j++) {
				matrix[i][j] = 0;
			}
		}
	}
	
	public void resetArray(int[] vector, int xlength) {
		for (int i=0; i<xlength; i++) {
			vector[i] = 0;
		}
	}
	
	/**
	 * return the total number of occurrences, that must be the
	 * (both) vector size.
	 */
	public int computeOccurrences(int[][] matrix, int[] x, int[] y, Vector v1, Vector v2,
																Integer[] v1elements, Integer[] v2elements) {
		
    int tot = 0;
    
		Iterator<Vector.Element> i1 = v1.iterator();
		Iterator<Vector.Element> i2 = v2.iterator();
    
    while (i1.hasNext() && i2.hasNext()) {
			
			Integer e1 = new Integer((int) i1.next().get());
			Integer e2 = new Integer((int) i2.next().get());
			
			int e1key = getKey(v1elements, e1);
			int e2key = getKey(v2elements, e2);
			
			x[e1key]++;
			y[e2key]++;
			matrix[e1key][e2key]++;
			tot++;
		}
		
		return tot;
	}
	
	public double computeResult(Vector v1, Vector v2) {
		
		// unique elements array
		ArrayList<Integer> v1uniques = getUniques(v1);
		ArrayList<Integer> v2uniques = getUniques(v2);
		
		Integer[] v1elements = new Integer[v1uniques.size()];
    v1elements = v1uniques.toArray(v1elements);
    
		Integer[] v2elements = new Integer[v2uniques.size()];
    v2elements = v2uniques.toArray(v2elements);
		
		// occurrences
		int[][] cooccurrences = new int[v1uniques.size()][v2uniques.size()];
		int[] v1occurrences = new int[v1uniques.size()];
		int[] v2occurrences = new int[v2uniques.size()];
		
		resetMatrix(cooccurrences, v1uniques.size(), v2uniques.size());
		resetArray(v1occurrences, v1uniques.size());
		resetArray(v2occurrences, v2uniques.size());
		
		int tot = computeOccurrences(cooccurrences, v1occurrences, v2occurrences, v1, v2, v1elements, v2elements);
		
		// mutual information
		double mi = 0.0;
		for (int i=0; i<v1uniques.size(); i++) {
			for (int j=0; j<v2uniques.size(); j++) {
				double pxy = (double) cooccurrences[i][j] / tot;
				double px = (double) v1occurrences[i] / tot;
				double py = (double) v2occurrences[j] / tot;
				
				if (pxy > 0.0) {
					mi = mi + (pxy * (Math.log( pxy/(px*py) ) / Math.log(2)));
					//mi = mi + (pxy * (Math.log( pxy/(px*py) )));
				}
			}
		}
		
		return mi;
	}
	
	public double computeResult(MatrixList matrix) {
		
		long tot = matrix.getOccurrences();
		Iterator<RowElement> iterator = matrix.elementsIterator();
		
		double mi = 0.0;
		while (iterator.hasNext()) {
			RowElement e = iterator.next();
			
			double pxy = (double) e.getOccurrences() / tot;
			double px = (double) matrix.getRowOccurrences(e.getRow()) / tot;
			double py = (double) matrix.getColOccurrences(e.getCol()) / tot;
			
			if (pxy > 0.0) {
				mi = mi + (pxy * (Math.log( pxy/(px*py) ) / Math.log(2)));
				//mi = mi + (pxy * (Math.log( pxy/(px*py) )));
			}
		}
		
		return mi;
	}
	
}
