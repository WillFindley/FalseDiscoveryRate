import java.io.FileReader;
import java.io.BufferedReader;
import java.net.URI;
import java.text.SimpleDateFormat;
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

public class RandomDataGenerationDriver{

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		int numMapTasks = Integer.parseInt(args[0]);
		int numRecordsPerTask = Integer.parseInt(args[1]);
		Path wordList = new Path(args[2]);
		Path outputDir = new Path(args[3]);

		Job job = Job.getInstance(conf, "RandomDataGenerationDriver");
		job.setJarByClass(RandomDataGenerationDriver.class);

		// Because we're mapping out random data and not analyzing
		job.setNumReduceTasks(0);

		job.setInputFormatClass(RandomStackOverflowInputFormat.class);

		RandomStackOverflowInputFormat.setNumMapTasks(job, numMapTasks);
		RandomStackOverflowInputFormat.setNumRecordPerTask(job, numRecordsPerTask);
		RandomStackOverflowInputFormat.setRandomWordList(job, wordList);

		TextOutputFormat.setOutputPath(job, outputDir);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		System.exit(job.waitForCompletion(true) ? 0 : 2);
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

	public static class RandomStackOverflowInputFormat extends InputFormat<Text,NullWritable> {

		public static final String NUM_MAP_TASKS = "random.generator.map.tasks";
		public static final String NUM_RECORDS_PER_TASK = "random.generator.num.records.per.map.task";
		public static final String RANDOM_WORD_LIST = "random.generator.random.word.file";

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

			RandomStackOverflowRecordReader rr = new RandomStackOverflowRecordReader();
			rr.initialize(split, context);
			return rr;
		}

		public static void setNumMapTasks(Job job, int i) {
			job.getConfiguration().setInt(NUM_MAP_TASKS, i);
		}

		public static void setNumRecordPerTask(Job job, int i) {
			job.getConfiguration().setInt(NUM_RECORDS_PER_TASK, i);
		}

		public static void setRandomWordList(Job job, Path file) {
			job.addCacheFile(file.toUri());
		}
	}

	public static class RandomStackOverflowRecordReader extends RecordReader<Text, NullWritable> {

		private int numRecordsToCreate = 0;
		private int createdRecords = 0;
		private Text key = new Text();
		private NullWritable value = NullWritable.get();
		private Random rndm = new Random();
		private ArrayList<String> randomWords = new ArrayList<String>();
		private SimpleDateFormat frmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");

		public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {

			this.numRecordsToCreate = context.getConfiguration().getInt(RandomStackOverflowInputFormat.NUM_RECORDS_PER_TASK, -1);

			URI[] files = context.getCacheFiles();

			BufferedReader rdr = new BufferedReader(new FileReader(files[0].toString()));

			String line;
			while ((line = rdr.readLine()) != null) {
				randomWords.add(line);
			}
			rdr.close();
		}

		public boolean nextKeyValue() throws IOException, InterruptedException {

			if (createdRecords < numRecordsToCreate) {
				int score = Math.abs(rndm.nextInt()) % 15000;
				int rowId = Math.abs(rndm.nextInt()) % 1000000000;
				int postId = Math.abs(rndm.nextInt()) % 100000000; 
				int userId = Math.abs(rndm.nextInt()) % 1000000; 
				String creationDate = frmt.format(Math.abs(rndm.nextLong()));

				String text = getRandomText();

				String randomRecord = "<row Id=\"" + rowId + "\" PostId=\"" + postId + "\" Score=\"" + score + "\" Text=\""
					+ text + "\" CreationDate=\"" + creationDate + "\" UserId\"=" + userId + "\" />";
				key.set(randomRecord); 
				createdRecords++; 
				return true;
			} else {
				return false;
			}
		}

		private String getRandomText() {
			StringBuilder bldr = new StringBuilder();
			int numWords = Math.abs(rndm.nextInt()) % 30 + 1;

			for(int i = 0; i < numWords; i++) { 
				bldr.append(randomWords.get(Math.abs(rndm.nextInt()) % randomWords.size()) + " ");
			}
			return bldr.toString(); 
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
