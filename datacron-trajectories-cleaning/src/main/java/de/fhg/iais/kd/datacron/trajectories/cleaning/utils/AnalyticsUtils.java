package de.fhg.iais.kd.datacron.trajectories.cleaning.utils;

public final class AnalyticsUtils {
	
	private AnalyticsUtils(){}
	
	public static double greatCircle(double y1, double x1, double y2, double x2) {
		final double la1 = Math.PI / 180 * y1;
		final double lo1 = Math.PI / 180 * x1;
		final double la2 = Math.PI / 180 * y2;
		final double lo2 = Math.PI / 180 * x2;

		return 6372.795 * 2 * Math.asin(Math.sqrt(Math.pow(Math.sin((la2 - la1) / 2), 2)
				+ Math.cos(la1) * Math.cos(la2) * Math.pow(Math.sin((lo2 - lo1) / 2), 2)));
	}
	
}
