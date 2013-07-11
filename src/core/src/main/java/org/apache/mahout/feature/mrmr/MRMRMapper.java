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

public class MRMRMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
    
	private ArrayList<String> listSetS = new ArrayList<String>();
	private Path[] localFiles;
	
	private int targetIndex;
	private int columnNumber;
	
	private IntWritable keyOut = new IntWritable();
	private VectorWritable vectorOut = new VectorWritable();
	private Text textOut = new Text();
	
	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		
		targetIndex = Integer.parseInt(conf.get(DefaultOptionCreator.TARGET_INDEX));
		columnNumber = Integer.parseInt(conf.get(DefaultOptionCreator.COLUMN_NUMBER));
		
		// TODO: refactoring
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
			
	/**
	 * @param index			it's the column index of the original dataset
	 * @param vector		values of the column identified by index param
	 */
	public void map(LongWritable index, Text record, Context context) throws IOException, InterruptedException {
		
		if (index.get() == 0) {
			return;
		}
		
		ArrayList<String> Sindex = new ArrayList<String>();
		String[] StringSetS = listSetS.toArray(new String[0]);
		
		for (int i=0; i<StringSetS.length; i++) {
			String[] feature = StringSetS[i].split("\t");
			Sindex.add(feature[1]);	// feature[1] is the index
		}
		
		
		String[] values = record.toString().split(",");
		for (int i=0; i<columnNumber; i++) {
			// i is the index of the candidate feature
			if (Sindex.contains(""+i) || i == targetIndex) continue;
			
			keyOut.set(i);
			textOut.set(values[i]+","+values[targetIndex]+",t");
			context.write(keyOut, textOut);
			//System.out.println("-- "+keyOut.toString()+", "+textOut.toString());
			
			for (int j=0; j<columnNumber; j++) {
				// j is the index of the already selected feature
				if (!Sindex.contains(""+j)) continue;
				
				keyOut.set(i);
				textOut.set(values[i]+","+values[j]+",f,"+j);
				context.write(keyOut, textOut);
				//System.out.println("-- "+keyOut.toString()+", "+textOut.toString());
			}
		}
	}
	
}
