import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class IBM extends Configured implements Tool {

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, DoubleWritable> {

		private DoubleWritable col4 = new DoubleWritable();
		private Text combo = new Text(); /* Key should be text */

		public void configure(JobConf job) {
		}

		public void map(LongWritable key, Text value, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
		    String[] tokens = value.toString().split(",");

		    if (tokens[tokens.length-1].equals("false")) {
		    	combo.set((int) Double.parseDouble(tokens[29]) + "," + (int) Double.parseDouble(tokens[30]) + "," 
		    		+ (int) Double.parseDouble(tokens[31]) + "," + (int) Double.parseDouble(tokens[32]) + ",");
		    	col4.set(Double.parseDouble(tokens[3]));
				output.collect(combo, col4);
		    }
		}
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, DoubleWritable, Text, DoubleWritable> {
		public void reduce(Text key, Iterator<DoubleWritable> values, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
		    double sum = 0;
		    int count = 0;
		    double avg = 0;
		    while (values.hasNext()) {
		    	sum += values.next().get();
		    	count += 1;
			}
			avg = sum / count;
		    output.collect(key, new DoubleWritable(avg));
		}
    }

    public int run(String[] args) throws Exception {
	JobConf conf = new JobConf(getConf(), IBM.class);
	conf.setJobName("IBM");

	conf.setOutputKeyClass(Text.class);
	conf.setOutputValueClass(DoubleWritable.class);

	conf.setMapperClass(Map.class);
	conf.setCombinerClass(Reduce.class);
	conf.setReducerClass(Reduce.class);

	conf.setInputFormat(TextInputFormat.class);
	conf.setOutputFormat(TextOutputFormat.class);

	FileInputFormat.setInputPaths(conf, new Path(args[0]));
	FileOutputFormat.setOutputPath(conf, new Path(args[1]));

	JobClient.runJob(conf);
	return 0;
    }

    public static void main(String[] args) throws Exception {
	int res = ToolRunner.run(new Configuration(), new IBM(), args);
	System.exit(res);
    }
}