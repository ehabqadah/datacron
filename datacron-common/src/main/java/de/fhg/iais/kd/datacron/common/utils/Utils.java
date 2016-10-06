package de.fhg.iais.kd.datacron.common.utils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.spark.api.java.JavaPairRDD;
import org.joda.time.DateTime;

import com.google.common.collect.Lists;

import scala.Tuple2;

/**
 * @author kthellmann
 *
 */
public final class Utils {

	private Utils() {
	}

	/**
	 * @param y1
	 * @param x1
	 * @param y2
	 * @param x2
	 * @return
	 */
	public static double greatCircle(double y1, double x1, double y2, double x2) {
		final double la1 = Math.PI / 180 * y1;
		final double lo1 = Math.PI / 180 * x1;
		final double la2 = Math.PI / 180 * y2;
		final double lo2 = Math.PI / 180 * x2;

		return 6372.795 * 2 * Math.asin(Math.sqrt(Math.pow(Math.sin((la2 - la1) / 2), 2)
				+ Math.cos(la1) * Math.cos(la2) * Math.pow(Math.sin((lo2 - lo1) / 2), 2)));
	}

	/**
	 * @param isPhysicalTime
	 * @param dateString
	 * @return
	 */
	public static long calculateTimeFromString(final boolean isPhysicalTime, final String dateString) {
		if (isPhysicalTime) {
			return DateTime.parse(dateString).getMillis();
		} else {
			return Long.valueOf(dateString);
		}
	}

	/**
	 * @param isPhysicalTime
	 * @param date1
	 * @return
	 */
	public static String calculateTimeAsString(final boolean isPhysicalTime, final long date1) {
		String date1Str;
		if (isPhysicalTime) {
			date1Str = String.valueOf(new DateTime(date1));
		} else {
			date1Str = String.valueOf(date1);
		}
		return date1Str;
	}

	/**
	 * @param areGeoCoordinates
	 * @param x1
	 * @param y1
	 * @param x2
	 * @param y2
	 * @return
	 */
	public static double calculateDistance(//
			final boolean areGeoCoordinates, //
			final double x1, //
			final double y1, //
			final double x2, //
			final double y2) {

		double distance = 0.0;

		if (areGeoCoordinates) {
			distance = de.fhg.iais.kd.datacron.common.utils.Utils.greatCircle(y1, x1, y2, x2);
		} else {
			distance = Math.sqrt(Math.pow(x2 - x1, 2) + Math.pow(y2 - y1, 2));
		}

		return distance;
	}

	/**
	 * @param isPhysicalTime
	 * @param date1
	 * @param date2
	 * @return
	 */
	public static double calculateDiffTime(boolean isPhysicalTime, long date1, long date2) {
		double diffTime = 0.0;

		if (isPhysicalTime) {
			diffTime = ((double) (date2 - date1)) / (1000 * 60 * 60 * 24);
		} else {
			diffTime = Math.toIntExact(date2 - date1);
		}

		return diffTime;
	}

	/**
	 * @param isPhysicalTime
	 * @param distance
	 * @param diffTime
	 * @return
	 */
	public static double calculateSpeed(boolean isPhysicalTime, double distance, double diffTime) {
		double speed = 0.0;

		if (isPhysicalTime) {
			speed = distance / (diffTime * 24);
		} else {
			speed = distance / diffTime;
		}

		return speed;
	}

	/**
	 * @param isPhysicalTime
	 * @param diffX
	 * @param diffY
	 * @return
	 */
	public static double calculateCourse(boolean isPhysicalTime, double diffX, double diffY) {
		double course = 180 * Math.atan2(diffX, diffY) / Math.PI;

		if (course < 0) {
			course += 360;
		}

		return course;
	}

	/**
	 * @param isPhysicalTime
	 * @param diffTime1
	 * @param speed1
	 * @param speed2
	 * @return
	 */
	public static double calculateAcceleration(//
			boolean isPhysicalTime, //
			double diffTime1, //
			double speed1, //
			double speed2) {

		double acceleration = 0.0;

		if (diffTime1 > 0) {
			if (isPhysicalTime) {
				acceleration = (speed2 - speed1) / (diffTime1 * 24 * 3600);
			} else {
				acceleration = (speed2 - speed1) / diffTime1;
			}
		}

		return acceleration;
	}
	
	
	/**
	 * @param course1
	 * @param course2
	 * @return
	 */
	public static double calculateTurn(double course1, double course2){
		return course1 - course2;
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

	/**
	 * @param v
	 * @return
	 */
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

	/**
	 * @param p
	 * @param v
	 * @return
	 */
	public static double computeQuantile(double p, Iterable<Double> v) {
		double median;
		final ArrayList<Double> values = Lists.newArrayList(v);
		Collections.sort(values);

		median = (double) values.get((int) (values.size() * p));

		return median;
	}

	/**
	 * @param map1
	 * @param map2
	 * @return
	 */
	public static Map<String, Double> subtractMap(Map<String, Double> map1, Map<String, Double> map2) {
		if (map1 == null || map2 == null) {
			return null;
		}

		Map<String, Double> differenceValuesMap = new HashMap<String, Double>();

		for (Entry<String, Double> entry : map1.entrySet()) {
			String key = entry.getKey();
			double value = entry.getValue();
			differenceValuesMap.put(key, value - map2.get(key));
		}

		return differenceValuesMap;
	}

	/**
	 * @param map
	 * @param divisor
	 * @return
	 */
	public static Map<String, Double> divideMapBy(Map<String, Double> map, double divisor) {
		if (map == null || divisor == 0) {
			return null;
		}

		Map<String, Double> dividedValuesMap = new HashMap<String, Double>();

		for (Entry<String, Double> entry : map.entrySet()) {
			String key = entry.getKey();
			double value = entry.getValue();
			dividedValuesMap.put(key, value / divisor);
		}

		return dividedValuesMap;
	}

}
