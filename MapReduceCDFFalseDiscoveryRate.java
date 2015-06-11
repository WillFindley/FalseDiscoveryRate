
/**
 * Generates a 
 *
 * @author Will Findley
 */ 
public class MapReduceCDFFalseDiscoveryRate extends Configured implements Tool {

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

		Job job = Job.getInstance(conf, "MapReduceCDFFalseDiscoveryRate");
		job.setJarByClass(MapReduceCDFFalseDiscoveryRate.class);

		job.setMapperClass(FDRCalculationMapping.class);
		job.setCombinerClass(FDRModelAveragingReducer.class);
		job.setReducerClass(FDRModelAveragingReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static class FDRCalculationMapping extends Mapper<Object, Text, IntWritable, CountAverageTuple> {

		private IntWritable outHour = new IntWritable();
		private CountAverageTuple outCountAverage = new CountAverageTuple();

		// This helper function parses the stackoverflow into a Map for us.
		public static Double[] transformXmlToSortedPValues(String xml) {
			
			ArrayList<Double> pValues = new ArrayList<Double>();

			String startDelim = "p=\"";
			String stopDelim = "\" />";
			int startIndex = pValues.indexOf(startDelim) + startDelim.length();
			int stopIndex = 0;
			do {
				stopIndex = pValues.indexOf(stopDelim,startIndex);
				pValues.add(Double.parseDouble(pValues.substring(startIndex,stopIndex);
				startIndex = pValues.indexOf(startDelim,stopIndex) + startDelim.length();
			} while (startIndex != -1);
			
			return (Double[]) (pValues.toArray()); 
		}

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			Double[] pValues = transformXmlToSortedPValues(value.toString());
			
			
			context.write(outHour, outCountAverage);
		}
	}

	public static class FDRModelAveragingReducer extends Reducer<IntWritable, CountAverageTuple, IntWritable, CountAverageTuple> {
	}
}
