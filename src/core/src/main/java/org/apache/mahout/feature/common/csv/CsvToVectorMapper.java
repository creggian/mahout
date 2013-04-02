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
package org.apache.mahout.feature.common.csv;

import org.apache.mahout.feature.mrmr.common.commandline.DefaultOptionCreator;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

import com.google.common.collect.Lists;
import java.util.List;
import java.io.IOException;

public class CsvToVectorMapper extends Mapper<LongWritable, Text, Text, VectorWritable> {
		
	private int columnNumber;
	
	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		columnNumber = Integer.parseInt(conf.get(DefaultOptionCreator.COLUMN_NUMBER));
	}
			
	public void map(LongWritable key, Text line, Context context) throws IOException, InterruptedException {
		
		// TODO: not always this problem
		// first line (column name), don't care
		if (key.get() == 0) {
			return;
		}
		
		Vector input = new RandomAccessSparseVector(columnNumber);
		List<String> values = Lists.newArrayList(line.toString().split(","));
		
		int k = 0;
		double v = 0.0;
		for (String value : values) {
			
			try {
				v = Double.parseDouble(value);
			}
			catch (NumberFormatException e) {
				throw new IOException("CSV file contains non-numeric data");
			}
			
			input.setQuick(k, v);
			k++;
		}
		
		// Text type as key is required since "rowid" job takes as argument
		// SequenceFile<Text,VectorWritable>
		context.write(new Text(""+key.get()), new VectorWritable(input));
	}
	
}
