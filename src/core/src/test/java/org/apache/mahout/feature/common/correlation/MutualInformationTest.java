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
import org.apache.mahout.math.DenseVector;
import java.util.Iterator;
import java.util.ArrayList;

import org.junit.Before;
import org.junit.Assert;
import org.junit.Test;

public class MutualInformationTest extends Assert {
	
	public static final double EPSILON = 0.000001;
	
	@Test
	public void testUniqueArrayList() {
		Vector v = new DenseVector(6);
    v.set(0, 1.0);
    v.set(1, 2.0);
    v.set(2, 3.0);
    v.set(3, 4.0);
    v.set(4, 3.0);
    v.set(5, 1.0);
    
		MutualInformation mi = new MutualInformation();
    ArrayList<Integer> uniques = mi.getUniques(v);
    
    assertEquals(4, uniques.size());
    for (int i=0; i<4; i++) {
			assertEquals(i+1, uniques.get(i).intValue());
		}
	}
	
	@Test
	public void testResetMatrix() {
		int[][] matrix = new int[2][2];
		for (int i=0; i<2; i++) {
			for (int j=0; j<2; j++) {
				matrix[i][j] = -1;
			}
		}
		
		MutualInformation mi = new MutualInformation();
		mi.resetMatrix(matrix, 2, 2);
		
		for (int i=0; i<2; i++) {
			for (int j=0; j<2; j++) {
				assertEquals(0, matrix[i][j]);
			}
		}
	}
	
	@Test
	public void testResetArray() {
		int[] array = new int[2];
		for (int i=0; i<2; i++) {
				array[i] = -1;
		}
		
		MutualInformation mi = new MutualInformation();
		mi.resetArray(array, 2);
		
		for (int i=0; i<2; i++) {
				assertEquals(0, array[i]);
		}
	}
	
	@Test
	public void testOccurrences() {
		Vector v1 = new DenseVector(10);
    v1.set(0, 2.0);
    v1.set(1, 2.0);
    v1.set(2, 0.0);
    v1.set(3, -2.0);
    v1.set(4, 0.0);
    v1.set(5, 0.0);
    v1.set(6, -2.0);
    v1.set(7, 2.0);
    v1.set(8, -2.0);
    v1.set(9, -2.0);
		
		Vector v2 = new DenseVector(10);
    v2.set(0, 2.0);
    v2.set(1, -2.0);
    v2.set(2, 0.0);
    v2.set(3, 0.0);
    v2.set(4, 0.0);
    v2.set(5, 0.0);
    v2.set(6, 0.0);
    v2.set(7, -2.0);
    v2.set(8, 0.0);
    v2.set(9, 2.0);
    
		MutualInformation mi = new MutualInformation();
    
    ArrayList<Integer> v1uniques = mi.getUniques(v1);
		ArrayList<Integer> v2uniques = mi.getUniques(v2);
		
		Integer[] v1elements = new Integer[v1uniques.size()];
    v1elements = v1uniques.toArray(v1elements);
    
		Integer[] v2elements = new Integer[v2uniques.size()];
    v2elements = v2uniques.toArray(v2elements);
		
		// occurrences
		int[][] cooccurrences = new int[v1uniques.size()][v2uniques.size()];
		int[] v1occurrences = new int[v1uniques.size()];
		int[] v2occurrences = new int[v2uniques.size()];
		
		mi.resetMatrix(cooccurrences, v1uniques.size(), v2uniques.size());
		mi.resetArray(v1occurrences, v1uniques.size());
		mi.resetArray(v2occurrences, v2uniques.size());
		
		int tot = mi.computeOccurrences(cooccurrences, v1occurrences, v2occurrences, v1, v2, v1elements, v2elements);
		assertEquals(10, tot);
		
		int[][] coExpected = {{1,2,0}, {0,0,3}, {1,0,3}};
		int[] xExpected = {3,3,4};
		int[] yExpected = {2,2,6};
		
		for (int i=0; i<3; i++) {
			for (int j=0; j<3; j++) {
				assertEquals(coExpected[i][j], cooccurrences[i][j]);
			}
		}
		
		for (int i=0; i<3; i++) {
			assertEquals(xExpected[i], v1occurrences[i]);
		}
		
		for (int i=0; i<3; i++) {
			assertEquals(yExpected[i], v2occurrences[i]);
		}
	}
	
	@Test
	public void testArrayListToArray() {
		Vector v = new DenseVector(6);
    v.set(0, 2.0);
    v.set(1, 2.0);
    v.set(2, 0.0);
    v.set(3, -2.0);
    v.set(4, -2.0);
    v.set(5, 0.0);
    
		MutualInformation mi = new MutualInformation();
    ArrayList<Integer> uniques = mi.getUniques(v);
    
    Integer[] array = new Integer[uniques.size()];
    array = uniques.toArray(array);
    
    assertEquals(2, array[0].intValue());
    assertEquals(0, array[1].intValue());
    assertEquals(-2, array[2].intValue());
	}
	
	@Test
  public void testMutualInformation1() {
		Vector v1 = new DenseVector(10);
    v1.set(0, 2.0);
    v1.set(1, 2.0);
    v1.set(2, 0.0);
    v1.set(3, -2.0);
    v1.set(4, 0.0);
    v1.set(5, 0.0);
    v1.set(6, -2.0);
    v1.set(7, 2.0);
    v1.set(8, -2.0);
    v1.set(9, -2.0);
		
		Vector v2 = new DenseVector(10);
    v2.set(0, 2.0);
    v2.set(1, -2.0);
    v2.set(2, 0.0);
    v2.set(3, 0.0);
    v2.set(4, 0.0);
    v2.set(5, 0.0);
    v2.set(6, 0.0);
    v2.set(7, -2.0);
    v2.set(8, 0.0);
    v2.set(9, 2.0);
		
		MutualInformation mi = new MutualInformation();
		double r = mi.computeResult(v1, v2);
		
		assertEquals(0.5343822, r, EPSILON);
	}
	
}
