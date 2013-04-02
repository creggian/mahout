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

public class PearsonCorrelation {
	
	private int instanceNumber;
	
	public PearsonCorrelation() {
	}
	
	public double computeResult(Vector v1, Vector v2) {
				
		instanceNumber = 0;
				
		// mean vector 1
		Iterator<Vector.Element> i1 = v1.iterateNonZero();
		double mean1 = 0.0;
		while (i1.hasNext()) {
			Vector.Element element = i1.next();
			mean1 = mean1 + element.get();
			instanceNumber++;
		}
		mean1 = mean1 / instanceNumber;
		
		// mean vector 2
		Iterator<Vector.Element> i2 = v2.iterateNonZero();
		double mean2 = 0.0;
		while (i2.hasNext()) {
			Vector.Element element = i2.next();
			mean2 = mean2 + element.get();
		}
		mean2 = mean2 / instanceNumber;
		
		// correlation
		Iterator<Vector.Element> i3 = v1.iterator();
		Iterator<Vector.Element> i4 = v2.iterator();
		
		double num = 0.0;
		double den1 = 0.0;
		double den2 = 0.0;
		while (i3.hasNext() && i4.hasNext()) {
			
			double x1 = i3.next().get();
			double x2 = i4.next().get();
			
			num = num + ((x1 - mean1)*(x2 - mean2));
			den1 = den1 + ((x1 - mean1)*(x1 - mean1));
			den2 = den2 + ((x2 - mean2)*(x2 - mean2));
			
		}
		
		double correlation = num / Math.sqrt(den1*den2);
		return correlation;
	}
	
}
