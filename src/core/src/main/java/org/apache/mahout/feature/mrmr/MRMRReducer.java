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
package org.apache.mahout.feature.mrmr;

import org.apache.mahout.feature.mrmr.common.commandline.DefaultOptionCreator;
import org.apache.mahout.feature.common.correlation.MatrixList;
import org.apache.mahout.feature.common.correlation.MutualInformation;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.filecache.DistributedCache;

import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.NamedVector;
import org.apache.mahout.math.VectorWritable;

import com.google.common.collect.Lists;

import java.lang.NumberFormatException;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MRMRReducer extends Reducer<IntWritable, Text, LongWritable, Text> {
		
	private int targetIndex;
	private int columnNumber;
	private int rowNumber;

	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		
		targetIndex = Integer.parseInt(conf.get(DefaultOptionCreator.TARGET_INDEX));
		columnNumber = Integer.parseInt(conf.get(DefaultOptionCreator.COLUMN_NUMBER));
		rowNumber = Integer.parseInt(conf.get(DefaultOptionCreator.ROW_NUMBER));
	}
	
	public void reduce(IntWritable index, Iterable<Text> items, Context context) throws IOException, InterruptedException {
		
		MatrixList target = new MatrixList();
		ArrayList<MatrixList> features = new ArrayList<MatrixList>();
		
		for (Text item: items) {
			String[] values = item.toString().split(",");
			
			int candidateValue = Integer.parseInt(values[0]);
			String type = values[2];
			
			if (type.equals("t")) {
				
				int targetValue = Integer.parseInt(values[1]);
				target.store(candidateValue, targetValue);
				
			} else if (type.equals("f")) {
				
				int featureValue = Integer.parseInt(values[1]);
				String featureName = values[3];
				
				boolean isNew = true;
				for (MatrixList matrix: features) {
					if (matrix.getName().equals(featureName)) {
						isNew = false;
						matrix.store(candidateValue, featureValue);
						break;
					}
				}
				if (isNew) {
					MatrixList matrix = new MatrixList();
					matrix.setName(featureName);
					matrix.store(candidateValue, featureValue);
					features.add(matrix);
				}
				
			}
		}
		
		MutualInformation mi = new MutualInformation();
		
		double sum_features = 0.0;
		for (MatrixList f: features) {
			sum_features = sum_features + mi.computeResult(f);
		}
		
		double sum_target = mi.computeResult(target);
		
		double coefficient = 1.0;
		if (features.size() > 1) coefficient = (1.0 / ((double) features.size()));
		double correlation = sum_target - (coefficient * sum_features);
		
		context.write(new LongWritable(0), new Text(index.get()+","+String.format("%.5f", correlation)));
	}
}
