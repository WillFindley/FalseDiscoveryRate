
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

	public static class FDRCalculationMapping extends Mapper<Object, Text, Text, ArrayWritable> {

		// allContribute is only one text entry because everything will be averaged together in the reducer 
		private Text allContribute = new Text("BUM coefficients");
		private ArrayWritable coeffAns = new ArrayWritable();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			Double[] tmpPValues = transformXmlToPValues(value.toString());
			double[][] pValues = calculateEmpiricalCDF(tmpPValues);

			coeffAns.set(getOptCoeffs(pValues));

			// BUM = Beta-Uniform Distribution
			context.write(allContribute, coeffAns);
		}

		public static Double[] transformXmlToPValues(String xml) {

			ArrayList<Double> pValues = new ArrayList<Double>();

			String startDelim = "p=\"";
			String stopDelim = "\" />";
			int startIndex = pValues.indexOf(startDelim) + startDelim.length();
			int stopIndex = 0;
			do {
				stopIndex = pValues.indexOf(stopDelim,startIndex);
				pValues.add(Double.parseDouble(pValues.substring(startIndex,stopIndex)));
				startIndex = pValues.indexOf(startDelim,stopIndex) + startDelim.length();
			} while (startIndex != -1);

			return (double[]) (pValues.toArray()); 
		}

		private double[][] calculateEmpiricalCDF(Double[] tmpPValues) {

			double[][] pValues = new double[tmpPValues.length][2];
			for (int i = 0; i < tmpPValues.length; i++) {
				pValues[i][0] = tmpPValues[i].doubleValue();
			}

			// sort on p-values so that second column can be filled with empirical CDF values
			Arrays.sort(pValues, new Comparator<double[]>() {
				@Override
				public int compare(double[] entry1, double[] entry2) {
					return Double.compare(entry1[0],entry2[0]);
				}
			});

			// calculate emprical CDF values
			for (int i = 1; i <= pValues.length; i++) {
				pValues[i-1][1] = i / (double) pValues.length;
			}

			return pValues;
		}

		private double[] getOptCoeffs(double[][] pValues) {	

			Random rndm = new Random();

			// pi0 is 1 because should always conservatively start by overestimating the proportion of negatives	
			double[] coeffs = {1.0, rndm.nextDouble(), 1+rndm.nextInt(9)+rndm.nextDouble()};
			double avDelta = 1;
			double oldDelta = avDelta;
			double learningRate = 2;

			int sigDigits = 4; // number of significant digits in the model parameters;
			double tolerance = 1.0 / Math.pow(10,sigDigits);
			do {
				oldDelta = avDelta;
				// coeffs is implicitly returned because it is modified at the reference position
				avDelta = stochasticGradientDescent(pValues, coeffs, tolerance, learningRate);
				if (oldDelta < avDelta) learningRate *= 0.9;
			} while (avDelta >= tolerance);

			return coeffs;	
		}

		private double stochasticGradientDescent(double[][] pValues, double[] coeffs, double tolerance, double learningRate) {

			pValues = shuffleEmpiricalCDF(pValues);

			double avDelta = 0;
			double momentum = 0.5;
			double deltaPi0 = 0;
			double deltaAlpha = 0;
			double deltaBeta = 0;
			for (double[] data : pValues) {

				deltaPi0 = momentum * deltaPi0 + (1-momentum) * learningRate *
					(Math.pow(data[1] - calcModelCDFValue(data[0], coeffs[0] + (gradientStepSize/2),coeffs[1],coeffs[2]),2) - 
					 Math.pow(data[1] - calcModelCDFValue(data[0], coeffs[0] - (gradientStepSize/2),coeffs[1],coeffs[2]),2)) / gradientStepSize;
				deltaAlpha = momentum * deltaAlpha + (1-momentum) * learningRate *
					(Math.pow(data[1] - calcModelCDFValue(data[0], coeffs[0],coeffs[1] + (gradientStepSize/2),coeffs[2]),2) -
					 Math.pow(data[1] - calcModelCDFValue(data[0], coeffs[0],coeffs[1] - (gradientStepSize/2),coeffs[2]),2)) / gradientStepSize;
				deltaBeta = momentum * deltaBeta + (1-momentum) * learningRate *
					(Math.pow(data[1] - calcModelCDFValue(data[0], coeffs[0],coeffs[1],coeffs[2] + (gradientStepSize/2)),2) -
					 Math.pow(data[1] - calcModelCDFValue(data[0], coeffs[0],coeffs[1],coeffs[2] - (gradientStepSize/2)),2)) / gradientStepSize;
				avDelta = (avDelta + Math.sqrt(Math.pow(deltaPi0,2) + Math.pow(deltaAlpha,2) + Math.pow(deltaBeta,2)))/2;

				System.out.println("Current average error: " + avDelta);

				coeffs[0] = Math.min(1.0, Math.max(0.0, coeffs[0] - deltaPi0));
				coeffs[1] = Math.min(1.0-gradientStepSize, Math.max(gradientStepSize, coeffs[1] - deltaAlpha));
				coeffs[2] = Math.max(1.0+gradientStepSize, coeffs[2] - deltaBeta);

			}
			return avDelta;	
		}

		public double[][] shuffleEmpiricalCDF(double[][] pValues) {

			int ranSpot;
			double[] tmp;
			for (int i = 0; i < pValues.length; i++) {
				ranSpot = rndm.nextInt(pValues.length-i) + i;
				tmp = pValues[i].clone();
				pValues[i] = pValues[ranSpot].clone();
				pValues[ranSpot] = tmp.clone();
			}
			return pValues;
		}

		public double calcModelCDFValue(double p, double pi0, double alpha, double beta) {

			return pi0*p + (1-pi0)*new BetaDistribution(alpha, beta).cumulativeProbability(p);
		}

	}

	public static class FDRModelAveragingReducer extends Reducer<IntWritable, CountAverageTuple, IntWritable, CountAverageTuple> {
	}
}
