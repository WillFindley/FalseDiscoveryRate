import java.io.DataInput;
import java.io.IOException;
import java.io.DataOutput;

import org.apache.hadoop.io.Writable;

public class Pi0AlphaBetaCountTuple implements Writable {

	private double pi0 = 0;
	private double alpha = 0;
	private double beta = 0;
	private long count = 0;

	public double getPi0() {

		return this.pi0;
	}

	public void setPi0(double pi0) {

		this.pi0 = pi0;
	}

	public double getAlpha() {

		return this.alpha;
	}

	public void setAlpha(double alpha) {

		this.alpha = alpha;
	}

	public double getBeta() {

		return this.beta;
	}

	public void setBeta(double beta) {

		this.beta = beta;
	}

	public long getCount() {

		return this.count;
	}

	public void setCount(long count) {

		this.count = count;
	}

	public void readFields(DataInput in) throws IOException {

		this.pi0 = in.readDouble();
		this.alpha = in.readDouble();
		this.beta = in.readDouble();
		this.count = in.readLong();
	}

	public void write(DataOutput out) throws IOException {

		out.writeDouble(this.pi0);
		out.writeDouble(this.alpha);
		out.writeDouble(this.beta);
		out.writeLong(this.count);
	}

	public String toString() {

		return "pi0: " + this.pi0 + "\t alpha: " + this.alpha + "\t beta: " + this.beta + "\t count: " + this.count;
	}
}

