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
package org.apache.mahout.feature.mrmr.common.mapreduce;

import org.apache.mahout.feature.mrmr.common.commandline.DefaultOptionCreator;

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

public class MaxReducer extends Reducer<LongWritable, Text, LongWritable, Text> {
    
		private ArrayList<String> listSetS = new ArrayList<String>();
		private Path[] localFiles;
		
		private int columnNumber;

		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			
			columnNumber = Integer.parseInt(conf.get(DefaultOptionCreator.COLUMN_NUMBER));
			
			// Load selected features from the distributed cache
			try {
				localFiles = DistributedCache.getLocalCacheFiles(conf);
				if (localFiles != null && localFiles.length > 0) {
					BufferedReader readBuffer = new BufferedReader(new FileReader(localFiles[0].toString()));
					String str = null;
					while((str = readBuffer.readLine()) != null) {
						listSetS.add(str);
					}
				}
			}
			catch(Exception e) {
				e.printStackTrace();
			}
		}
    
		public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            
			String bestFeature = "";
			double maxValue = 0.0;
				
			int i = 0;
			for (Text val:values) {
				String[] valuedata = val.toString().split(",");
				
				double currentValue = Double.parseDouble(valuedata[1]);
				if ((i == 0) || (currentValue > maxValue)) {
					maxValue = currentValue;
					bestFeature = valuedata[0];
				}
				
				i++;
			}
			
			// Output of already knows best features
			i=0;
			String[] StringSetS = listSetS.toArray(new String[0]);
			for (i=0; i<StringSetS.length; i++) {
				String[] feature = StringSetS[i].split("\t");
				int rank = Integer.parseInt(feature[0]);
				context.write(new LongWritable(rank), new Text(feature[1]+"\t"+feature[2]));
			}
			
			context.write(new LongWritable(i+1), new Text(bestFeature+"\t"+maxValue));
		}
	}
