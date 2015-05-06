package edu.gslis.hbase.trec;

import org.rosuda.REngine.Rserve.RConnection;
import org.rosuda.REngine.Rserve.RserveException;



public class RKernelDensity {
	
	private double[] ky;

	private RConnection c;

    public RKernelDensity() throws RserveException {
        c = new RConnection();
    }

	public RKernelDensity(String host, int port) throws RserveException {
	    c = new RConnection(host, port);
	}
	
	public void estimate(double[] data, double[] weights) {
		
		try {
			c.assign("x", data);
			c.assign("weights", weights);
			c.voidEval("weights = weights / sum(weights)");
			c.voidEval("kern = density(x, weights=weights, window=\"gaussian\", bw=\"SJ-dpi\", n=1024)");

			ky = c.eval("kern$y").asDoubles();
			

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

    public double mean() {
        double mu = 0.0;       
         try {
             mu = c.eval("mean(kern$y)").asDouble();
         } catch (Exception e) {
             e.printStackTrace();
         }
         return mu;
     }
	
	public double sd() {
       double sd = 0.0;       
        try {
            sd = c.eval("sd(kern$y)").asDouble();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return sd;
	}

	public double density(double x) {
		double f = 0.0;
		String cmd = "ind = which(abs(" + x + "-kern$x)==min(abs(" + x + "-kern$x)))";
		try {
			int ind = c.eval(cmd).asInteger();
			double ll = ky[ind-1];
			return ll;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return f;
	}

	public void close() {
		try {
			c.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
