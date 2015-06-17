public class cdfFDRTest {  

	Random rndm = new Random();
	double[][] pValues = new double[1][2];	// first column is p-value, second is empirical CDF ordinate
	double pi0 = 1.0;
	BetaDistribution TrueHypotheses = new BetaDistribution(1.0, 1.0);

	public static void main(String[] args) {
		
		cdfFDRTest test = new cdfFDRTest(0.5, 0.5, 5.0, 300);
		System.out.println(Arrays.toString(test.run()));
	}

	public cdfFDRTest(double pi0, double alpha, double beta, int numPValues) {

		this.pi0 = pi0;
		this.TrueHypotheses = new BetaDistribution(alpha,beta);
		
		this.pValues = new double[numPValues][2];
		for (int i = 0; i < numPValues; i++) {
			this.pValues[i][0] = calculateP();
		}
	}

	private double calculateP() {

		if (this.rndm.nextDouble() <= this.pi0) {
			return this.rndm.nextDouble();
		} else {
			return this.TrueHypotheses.inverseCumulativeProbability(this.rndm.nextDouble());
		}
	}

	public double[] run() {
		
		calculateEmpiricalCDF();
		return getOptCoeffs();
	}

	private void calculateEmpiricalCDF() {
		
		// sort on p-values so that second column can be filled with empirical CDF values
		Arrays.sort(this.pValues, new Comparator<Double[]>() {
			@Override
			public int compare(final Double[] entry1, final Double[] entry2) {
				final Double p1 = entry1[0];
				final Double p2 = entry2[0];
				return p1.compareTo(p2);
			}
		});

		// calculate emprical CDF values
		for (int i = 1; i <= this.pValues.length; i++) {
			this.pValues[i-1][1] = i / (double) this.pValues.length;
		}
	}

	public double[] getOptCoeffs() {	
	
		double[] coeffs = {0.5, 0.5, 5};
		double maxDelta = 1;

		int sigDigits = 4; // number of significant digits in the model parameters;
		double tolerance = 1.0 / Math.pow(10,sigDigits);
		do {
			maxDelta = stochasticGradientDescent(coeffs, tolerance);	
		} while (maxDelta >= tolerance);

		return coeffs;	
	}

	public double stochasticGradientDescent(double[] coeffs, double gradientStepSize) {

		shuffleEmpiricalCDF();
		
		double maxDelta = 0;
		double learningRate = 0.5; 
		double momentum = 0.5;
		double deltaPi0 = 0;
		double deltaAlpha = 0;
		double deltaBeta = 0;
		for (double[] data : this.pValues) {

			deltaPi0 = momentum * deltaPi0 + (1-momentum) * learningRate *
				(Math.pow(data[1] - calcModelCDFValue(data[0], coeffs[0] + (gradientStepSize/2),coeffs[1],coeffs[2]),2) - 
				Math.pow(data[1] - calcModelCDFValue(data[0], coeffs[0] - (gradientStepSize/2),coeffs[1],coeffs[2]),2)) / gradientStepSize;
			deltaAlpha = momentum * deltaAlpha + (1-momentum) * learningRate *
				(Math.pow(data[1] - calcModelCDFValue(data[0], coeffs[0],coeffs[1] + (gradientStepSize/2),coeffs[2]),2) -
				Math.pow(data[1] - calcModelCDFValue(data[0], coeffs[0],coeffs[1] - (gradientStepSize/2),coeffs[2]),2)) / gradientStepSize;
			deltaBeta = momentum * deltaBeta + (1-momentum) * learningRate *
				(Math.pow(data[1] - calcModelCDFValue(data[0], coeffs[0],coeffs[1],coeffs[2] + (gradientStepSize/2)),2) -
				Math.pow(data[1] - calcModelCDFValue(data[0], coeffs[0],coeffs[1],coeffs[2] - (gradientStepSize/2)),2)) / gradientStepSize;
			maxDelta = Math.max(deltaPi0,Math.max(deltaAlpha,deltaBeta));

			System.out.println("pi0: " + deltaPi0 + "\t deltaAlpha: " + deltaAlpha + "\t deltaBeta: " + deltaBeta);
			System.out.println("maxDelta: " + maxDelta);

			coeffs[0] -= deltaPi0;
			coeffs[1] -= deltaAlpha;
			coeffs[2] -= deltaBeta;

			this.pi0 = coeffs[0];
			this.TrueHypotheses = new BetaDistribution(coeffs[1], coeffs[2]);
		}
		return maxDelta;	
	}

	public void shuffleEmpiricalCDF() {

		int ranSpot;
		double[] tmp;
		for (int i = 0; i < this.pValues.length; i++) {
			ranSpot = rndm.nextInt(this.pValues.length-i) + i;
			tmp = this.pValues[i].clone();
			this.pValues[i] = this.pValues[ranSpot].clone();
			this.pValues[ranSpot] = tmp.clone();
		}
	}

	public double calcModelCDFValue(double p, double pi0, double alpha, double beta) {

		return pi0*p + (1-pi0)*new BetaDistribution(alpha, beta).cumulativeProbability(p);
	}
}
