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

public class MRMRReducer extends Reducer<IntWritable, VectorWritable, LongWritable, Text> {
		
	private int targetIndex;
	private int columnNumber;
	private int rowNumber;

	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		
		targetIndex = Integer.parseInt(conf.get(DefaultOptionCreator.TARGET_INDEX));
		columnNumber = Integer.parseInt(conf.get(DefaultOptionCreator.COLUMN_NUMBER));
		rowNumber = Integer.parseInt(conf.get(DefaultOptionCreator.ROW_NUMBER));
	}
	
	public void reduce(IntWritable index, Iterable<VectorWritable> vectors, Context context) throws IOException, InterruptedException {
		
		NamedVector target = null;
		NamedVector candidate = null;
		
		ArrayList<NamedVector> feature = new ArrayList<NamedVector>();
		
		for (VectorWritable vectorWritable: vectors) {
			Vector v = vectorWritable.get();
			NamedVector vector = ((NamedVector) v);
			
			if (vector.getName().equals("target")) {
				target = vector.clone();
			}
			else if (vector.getName().equals("candidate")) {
				candidate = vector.clone();
			}
			else if (vector.getName().equals("feature")) {
				feature.add(vector.clone());
			}
		}
		
		if (candidate == null) { // it's a feature inside S
			return;
		}
		
		MutualInformation mi = new MutualInformation();
		
		double sum_features = 0.0;
		for (NamedVector f: feature) {
			sum_features = sum_features + mi.computeResult(f, candidate);
		}
		
		double sum_target = mi.computeResult(target, candidate);
		
		double coefficient = 1.0;
		if (feature.size() > 1) coefficient = (1.0 / ((double) feature.size()));
		double correlation = sum_target - (coefficient * sum_features);
		
		context.write(new LongWritable(0), new Text(index.get()+","+String.format("%.5f", correlation)));
	}
}
