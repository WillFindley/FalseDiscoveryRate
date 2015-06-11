import java.util.Random;
import java.util.ArrayList;
import java.util.List;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.InterruptedException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.io.Writable; 
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configured;

import org.apache.commons.math3.distribution.BetaDistribution;

/**
 * Generates a mixed uniform beta distribution of p-values randomly distributed throughout HDFS for testing False Discovery Rate protocols
 *
 * @author Will Findley
 */ 
public class RandomDataGenerationDriver extends Configured implements Tool {

	public static void main(String[] args) throws Exception {

		if (args.length != 6) {
			System.out.println("\n" + 
					"This program generates a mixed uniform beta distribution of p-values " +  
					"randomly distributed throughout HDFS for testing False Discovery Rate protocols. \n" + 
					"Usage is: \n\n" +
					"hadoop jar [jarFile] RandomDataGenerationDriver [args0] [args1] [args2] [args3] [args4] [args5] \n\n" + 
					"args0 - number of mapper tasks \n" +
					"args1 - number of records produced by each mapper \n" +
					"args2 - pi0, the proportion of p-values that are uniformly distributed (false hypotheses) \n" +
					"args3 - alpha, the alpha for the beta distribution; less than one yields smaller values (strong true hypotheses) \n" +
					"args4 - beta, the beta for the beta distribution; greater than one yields larger values (weak true hypotheses) \n" +
					"args5 - slave directory in which to write p-value xml.\n"
					);
			return;
		}
		int res = ToolRunner.run(new Configuration(), new RandomDataGenerationDriver(), args);
		System.exit(res);
	}

	public int run(String[] args) throws Exception {

		Configuration conf = this.getConf();

		int numMapTasks = Integer.parseInt(args[0]);
		int numRecordsPerTask = Integer.parseInt(args[1]);
		Path outputDir = new Path(args[5]);

		conf.set("pi0", args[2]);	// proportion of false hypotheses
		conf.set("alpha", args[3]);	// effect of low p-values
		conf.set("beta", args[4]);	// efffect of hight p-values
		Job job = Job.getInstance(conf, "RandomDataGenerationDriver");
		job.setJarByClass(RandomDataGenerationDriver.class);

		// Because we're mapping out random data and not analyzing
		job.setNumReduceTasks(0);

		job.setInputFormatClass(RandomPValueInputFormat.class);

		RandomPValueInputFormat.setNumMapTasks(job, numMapTasks);
		RandomPValueInputFormat.setNumRecordPerTask(job, numRecordsPerTask);

		TextOutputFormat.setOutputPath(job, outputDir);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	// There's not much in here other than empty overrides because we're not reading in anything, just writing
	public static class FakeInputSplit extends InputSplit implements Writable {

		public void readFields(DataInput arg0) throws IOException {
		}

		public void write(DataOutput arg0) throws IOException {
		}

		public long getLength() throws IOException, InterruptedException {
			return 0;
		}

		public String[] getLocations() throws IOException, InterruptedException {
			return new String[0];
		}
	}

	public static class RandomPValueInputFormat extends InputFormat<Text,NullWritable> {

		public static final String NUM_MAP_TASKS = "random.generator.map.tasks";
		public static final String NUM_RECORDS_PER_TASK = "random.generator.num.records.per.map.task";

		public List<InputSplit> getSplits(JobContext job) throws IOException {

			int numSplits = job.getConfiguration().getInt(NUM_MAP_TASKS, -1);

			ArrayList<InputSplit> splits = new ArrayList<InputSplit>();
			for (int i = 0; i < numSplits; i++) {
				splits.add(new FakeInputSplit());
			}

			return splits;
		}

		public RecordReader<Text, NullWritable> createRecordReader(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {

			RandomPValueRecordReader rr = new RandomPValueRecordReader();
			rr.initialize(split, context);
			return rr;
		}

		public static void setNumMapTasks(Job job, int i) {
			job.getConfiguration().setInt(NUM_MAP_TASKS, i);
		}

		public static void setNumRecordPerTask(Job job, int i) {
			job.getConfiguration().setInt(NUM_RECORDS_PER_TASK, i);
		}
	}

	public static class RandomPValueRecordReader extends RecordReader<Text, NullWritable> {

		private int numRecordsToCreate = 0;
		private int createdRecords = 0;
		private Text key = new Text();
		private NullWritable value = NullWritable.get();
		private Random rndm = new Random();
		private double pi0 = 1.0; // proportion of false hypotheses
		private BetaDistribution TrueHypotheses = new BetaDistribution(1.0, 1.0);

		public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {

			this.numRecordsToCreate = context.getConfiguration().getInt(RandomPValueInputFormat.NUM_RECORDS_PER_TASK, -1);
			this.pi0 = Double.parseDouble(context.getConfiguration().get("pi0"));
			this.TrueHypotheses = new BetaDistribution(Double.parseDouble(context.getConfiguration().get("alpha")),
					Double.parseDouble(context.getConfiguration().get("beta")));
		}

		private double calculateP() {

			if (rndm.nextDouble() <= this.pi0) {
				return rndm.nextDouble();
			} else {
				return TrueHypotheses.inverseCumulativeProbability(rndm.nextDouble());
			}
		}

		public boolean nextKeyValue() throws IOException, InterruptedException {

			if (createdRecords < numRecordsToCreate) {
				int rowId = Math.abs(rndm.nextInt()) % 1000000000;
				double p = calculateP();

				String randomRecord = "<row Id=\"" + rowId + "\" p=\"" + p + "\" />";
				key.set(randomRecord); 
				createdRecords++; 
				return true;
			} else {
				return false;
			}
		}

		public Text getCurrentKey() throws IOException, InterruptedException {	
			return key; 
		}

		public NullWritable getCurrentValue() throws IOException, InterruptedException {
			return value; 
		}

		public float getProgress() throws IOException, InterruptedException { 
			return (float) createdRecords / (float) numRecordsToCreate;
		}

		public void close() throws IOException { 
		} 
	}
}	
