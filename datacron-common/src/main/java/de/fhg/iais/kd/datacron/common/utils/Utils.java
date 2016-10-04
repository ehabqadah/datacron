package de.fhg.iais.kd.datacron.common.utils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;

import org.apache.spark.api.java.JavaPairRDD;
import org.joda.time.DateTime;

import com.google.common.collect.Lists;

import scala.Tuple2;

public final class Utils {

	private Utils() {
	}

	public static double greatCircle(double y1, double x1, double y2, double x2) {
		final double la1 = Math.PI / 180 * y1;
		final double lo1 = Math.PI / 180 * x1;
		final double la2 = Math.PI / 180 * y2;
		final double lo2 = Math.PI / 180 * x2;

		return 6372.795 * 2 * Math.asin(Math.sqrt(Math.pow(Math.sin((la2 - la1) / 2), 2)
				+ Math.cos(la1) * Math.cos(la2) * Math.pow(Math.sin((lo2 - lo1) / 2), 2)));
	}

	public static long calculateTimeFromString(final boolean isPhysicalTime, final String dateString) {
		if (isPhysicalTime) {
			return DateTime.parse(dateString).getMillis();
		} else {
			return Long.valueOf(dateString);
		}
	}

	public static double calculateDistance(final boolean areGeoCoordinates, final double x1, final double y1,
			final double x2, final double y2) {
		double distance;

		if (areGeoCoordinates) {
			distance = de.fhg.iais.kd.datacron.common.utils.Utils.greatCircle(y1, x1, y2, x2);
		} else {
			distance = Math.sqrt(Math.pow(x2 - x1, 2) + Math.pow(y2 - y1, 2));
		}
		return distance;
	}

	/**
	 * Compute min of dates or coordinates of whole data
	 * 
	 * @param pairRDD:
	 *            (Table, dates or coordinates)
	 * @return pairRDD: (Table, Min date or Min coordinate)
	 */
	public static <T extends Comparable<T>> JavaPairRDD<String, T> computeMin(JavaPairRDD<String, T> pairRDD) {
		return pairRDD.reduceByKey((v1, v2) -> {
			return (v1.compareTo(v2) < 0 ? v1 : v2);
		});
	}

	/**
	 * Compute max of dates or coordinates of whole data
	 * 
	 * @param pairRDD:
	 *            (Table, dates or coordinates)
	 * @return pairRDD: (Table, Max date or Max coordinate)
	 */
	public static <T extends Comparable<T>> JavaPairRDD<String, T> computeMax(JavaPairRDD<String, T> pairRDD) {
		return pairRDD.reduceByKey((v1, v2) -> {
			return (v1.compareTo(v2) > 0 ? v1 : v2);
		});
	}

	/**
	 * Compute min of acceleration or speed or timediff or dates or coordinates
	 * of a trajectory
	 * 
	 * @param pairRDD:(TrajectoryID,
	 *            acceleration or speed or timediff or dates or coordinates)
	 * @return pairRDD: (TrajectoryID, Min acceleration or Min speed or Min
	 *         timediff or Min date or Min coordinate)
	 */
	public static JavaPairRDD<String, Double> computeMinDouble(JavaPairRDD<String, Double> pairRDD) {
		return pairRDD.reduceByKey((v1, v2) -> {
			return (v1 < v2 ? v1 : v2);
		});
	}

	/**
	 * Compute max of acceleration or speed or timediff or dates or coordinates
	 * of a trajectory
	 * 
	 * @param pairRDD:
	 *            (TrajectoryID, acceleration or speed or timediff or dates or
	 *            coordinates)
	 * @return pairRDD: (TrajectoryID, Max acceleration or Max speed or Max
	 *         timediff or Max date or Max coordinate)
	 */
	public static JavaPairRDD<String, Double> computeMaxDouble(JavaPairRDD<String, Double> pairRDD) {
		return pairRDD.reduceByKey((v1, v2) -> {
			return (v1 > v2 ? v1 : v2);
		});
	}

	/**
	 * Compute average of acceleration or speed or timediff of a trajectory
	 * 
	 * @param pairRDD:
	 *            (TrajectoryID, acceleration or speed or timediff)
	 * @return pairRDD: (TrajectoryID, Avg acceleration or Avg speed or Avg
	 *         timediff)
	 */
	public static JavaPairRDD<String, Double> computeAverage(JavaPairRDD<String, Double> pairRDD) {
		final Map<String, Object> nrTraj = pairRDD.countByKey();

		return pairRDD.reduceByKey((v1, v2) -> {
			return v1 + v2;
		}).mapToPair(tuple -> new Tuple2<>(tuple._1(), tuple._2() / (long) nrTraj.get(tuple._1())));
	}

	/**
	 * Compute median of acceleration or speed or timediff of a trajectory
	 * 
	 * @param pairRDD:
	 *            (TrajectoryID, acceleration or speed or timediff)
	 * @return pairRDD: (TrajectoryID, Median of acceleration or Median of speed
	 *         or Median of timediff)
	 */
	public static JavaPairRDD<String, Double> computeMedian(JavaPairRDD<String, Double> pairRDD) {
		return pairRDD.groupByKey().mapToPair(tuple -> {
			final String id = tuple._1();
			double median;

			median = computeMedian(tuple._2());

			return new Tuple2<>(id, median);
		});
	}

	public static double computeMedian(Iterable<Double> v) {
		double median;
		final ArrayList<Double> values = Lists.newArrayList(v);
		Collections.sort(values);

		if (values.size() % 2 == 0) {
			median = ((double) values.get(values.size() / 2) + (double) values.get(values.size() / 2 - 1)) / 2;
		} else {
			median = (double) values.get(values.size() / 2);
		}
		return median;
	}

	public static double computeQuantile(double p, Iterable<Double> v) {
		double median;
		final ArrayList<Double> values = Lists.newArrayList(v);
		Collections.sort(values);

		median = (double) values.get((int) (values.size() * p));

		return median;
	}

}
