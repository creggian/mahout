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

import org.junit.Before;
import org.junit.Assert;
import org.junit.Test;

public class PearsonCorrelationTest extends Assert {
	
	public static final double EPSILON = 0.000001;
	
	@Test
  public void testMaxCorrelation() {
		
		Vector v1 = new DenseVector(5);
    v1.set(0, 1.0);
    v1.set(1, 2.0);
    v1.set(2, 3.0);
    v1.set(3, 4.0);
    v1.set(4, 5.0);
		
		Vector v2 = new DenseVector(5);
    v2.set(0, 2.0);
    v2.set(1, 3.0);
    v2.set(2, 4.0);
    v2.set(3, 5.0);
    v2.set(4, 6.0);
		
		PearsonCorrelation corr = new PearsonCorrelation();
		double r = corr.computeResult(v1, v2);
		
		assertEquals(1, r, EPSILON);
	}
	
	@Test
  public void testCorrelation1() {
		
		Vector v1 = new DenseVector(5);
    v1.set(0, -3.0);
    v1.set(1, 1.0);
    v1.set(2, 13.0);
    v1.set(3, 5.0);
    v1.set(4, 7.0);
		
		Vector v2 = new DenseVector(5);
    v2.set(0, 15.0);
    v2.set(1, 2.0);
    v2.set(2, -3.0);
    v2.set(3, 4.0);
    v2.set(4, -5.0);
		
		PearsonCorrelation corr = new PearsonCorrelation();
		double r = corr.computeResult(v1, v2);
		
		assertEquals(-0.8253382, r, EPSILON);
	}
	
	@Test
  public void testMaxNegativeCorrelation() {
		
		Vector v1 = new DenseVector(5);
    v1.set(0, 1.0);
    v1.set(1, 2.0);
    v1.set(2, 3.0);
    v1.set(3, 4.0);
    v1.set(4, 5.0);
		
		Vector v2 = new DenseVector(5);
    v2.set(0, -2.0);
    v2.set(1, -3.0);
    v2.set(2, -4.0);
    v2.set(3, -5.0);
    v2.set(4, -6.0);
		
		PearsonCorrelation corr = new PearsonCorrelation();
		double r = corr.computeResult(v1, v2);
		
		assertEquals(-1, r, EPSILON);
	}
	
}
