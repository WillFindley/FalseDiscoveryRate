
public class cdfFDRTest {  

	Random rndm = new Random();
	double[] pValues = {};
	double pi0 = 1.0;
	BetaDistribution TrueHypotheses = new BetaDistribution(1.0, 1.0);

	public static void main(String[] args) {
		
		cdfFDRTest test = new cdfFDRTest(0.5, 0.5, 5.0, 300);
		System.out.println(Arrays.toString(test.run()));
	}

	public cdfFDRTest(double pi0, double alpha, double beta, int numPValues) {

		this.pi0 = pi0;
		this.TrueHypotheses = new BetaDistribution(alpha,beta);
		
		this.pValues = new double[numPValues]
		for (int i = 0; i < numPValues; i++) {
			this.pValues[i] = calculateP();
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
		
		// sort pValues for empirical CDF calculation
		this.pValues = Arrays.sort(this.pValues);
		
		double[][][] grid = new double[21][21][21]; 
		
		return getOptCoeffs(grid);
	}

	public double[][][] getOptCoeffs(double[][][] grid) {	
	
		double[] coeffs = new double[3];

		int sigDigits = 4; // number of significant digits in the model parameters;
		double tolerance = 1.0 / Math.pow(10,sigDigits);
		do {
			
		} while (stepPi0 >= tolerance);

		return coeffs;	
	}
}
