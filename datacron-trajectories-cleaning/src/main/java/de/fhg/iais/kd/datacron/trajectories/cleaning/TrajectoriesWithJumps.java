package de.fhg.iais.kd.datacron.trajectories.cleaning;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaRDD;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.name.Named;

import de.fhg.iais.kd.datacron.common.utils.Utils;
import de.fhg.iais.kd.datacron.trajectories.cleaning.data.provider.TrajectoriesProvider;
import de.fhg.iais.kd.datacron.trajectories.cleaning.data.provider.TrajectoriesWithJumpsProvider;
import de.fhg.iais.kd.datacron.trajectories.cleaning.data.provider.TrajectoriesWithJumpsProvider2;
import de.fhg.iais.kd.datacron.trajectories.cleaning.data.provider.TrajectoriesWithoutJumpsProvider;
import de.fhg.iais.kd.datacron.trajectories.cleaning.data.provider.TrajectoriesWithoutJumpsProvider2;
import de.fhg.iais.kd.datacron.trajectories.cleaning.table.beans.TBInputTrajectory;
import de.fhg.iais.kd.datacron.trajectories.cleaning.table.beans.TBOutlierTrajectory;
import de.fhg.iais.kd.datacron.trajectories.cleaning.table.beans.TBOutputTrajectory;

/**
 * Detect and correct trajectories with jumps:
 * 
 * I. Characteristics: Large distance in a short time interval btw. 2
 * consecutive points
 * 
 * II. Detection criteria: Distance btw. current and next larger than threshold.
 * 
 * Threshold = p quantile * distancethresholdfactor
 * 
 * 
 * III. Remove all outlier points
 * 
 * @author kthellmann
 */
@Singleton
public class TrajectoriesWithJumps implements Serializable {

	private static final long serialVersionUID = 5418827747835053055L;

	@Inject
	private transient TrajectoriesProvider trajectoriesInputProvider;
	@Inject
	private transient TrajectoriesWithJumpsProvider trajectoriesWithJumpsProvider;
	@Inject
	private transient TrajectoriesWithoutJumpsProvider trajectoriesWithoutJumpsProvider;
	@Inject
	private transient TrajectoriesWithJumpsProvider2 trajectoriesWithJumpsProvider2;
	@Inject
	private transient TrajectoriesWithoutJumpsProvider2 trajectoriesWithoutJumpsProvider2;

	@Inject
	@Named("spark.app.trajectories.jumps.speed.th.factor")
	private double jumpsThSpeedFactor;
	@Inject
	@Named("spark.app.trajectories.quantile.speed.p")
	private double pSpeed;
	@Inject
	@Named("spark.app.trajectories.jumps.distance.th.factor")
	private double jumpsThDistanceFactor;
	@Inject
	@Named("spark.app.trajectories.quantile.distance.p")
	private double pDistance;

	/**
	 * Detect and correct trajectories with jumps.
	 */
	public void clean() {
		this.detect();
	}

	/**
	 * Detect trajectories with jumps.
	 */
	private void detect() {
		// Load computed trajectories
		final JavaRDD<TBInputTrajectory> inputRDD = this.trajectoriesInputProvider.readInputTableRows();

		// Identify jumps by speed metric
		this.detectBySpeed(inputRDD);

		// Identify jumps by distance metric
		 this.detectByDistance(inputRDD);

		 this.trajectoriesInputProvider.close();
	}

	/**
	 * Identify jumps by distance metric
	 * 
	 * @param inputRDD
	 */
	private void detectByDistance(JavaRDD<TBInputTrajectory> inputRDD) {
		JavaRDD<TBOutlierTrajectory> trajectoriesWithJumpsDistanceMetricRDD = detectJumpsByDistanceMetric(inputRDD);

		trajectoriesWithJumpsDistanceMetricRDD.cache();

		// Update table of trajectories with jumps (distance metric)
		this.trajectoriesWithJumpsProvider2.updateTable(trajectoriesWithJumpsDistanceMetricRDD);

		// Fix corrupt trajectory (distance metric)
		JavaRDD<TBOutputTrajectory> trajectoriesWithoutJumpsDistanceMetricRDD = this.correct(trajectoriesWithJumpsDistanceMetricRDD);

		// Update table of trajectories without jumps (distance metric)
		this.trajectoriesWithoutJumpsProvider2.updateTable(trajectoriesWithoutJumpsDistanceMetricRDD);
	}
	
	/**
	 * Identify jumps by speed metric
	 * 
	 * @param inputRDD
	 */
	private void detectBySpeed(JavaRDD<TBInputTrajectory> inputRDD) {

		JavaRDD<TBOutlierTrajectory> trajectoriesWithJumpsSpeedMetricRDD = detectJumpsBySpeedMetric(inputRDD);

		trajectoriesWithJumpsSpeedMetricRDD.cache();

		// Update table of trajectories with jumps (speed metric)
		this.trajectoriesWithJumpsProvider.updateTable(trajectoriesWithJumpsSpeedMetricRDD);

		// Fix corrupt trajectory (speed metric)
		JavaRDD<TBOutputTrajectory> trajectoriesWithoutJumpsSpeedMetricRDD = this.correct(trajectoriesWithJumpsSpeedMetricRDD);

		// Update table of trajectories without jumps (speed metric)
		this.trajectoriesWithoutJumpsProvider.updateTable(trajectoriesWithoutJumpsSpeedMetricRDD);
	}

	/**
	 * @param inputRDD
	 * @return
	 */
	private JavaRDD<TBOutlierTrajectory> detectJumpsByDistanceMetric(JavaRDD<TBInputTrajectory> inputRDD) {
		final boolean isPhysicalTime = this.trajectoriesInputProvider.isTimeIsPhysical();
		final boolean areGeoCoordinates = this.trajectoriesInputProvider.isCoordinatesAreGeo();
		
		JavaRDD<Iterable<TBOutlierTrajectory>> markedTrajectoriesDistanceMetricRDD = inputRDD.//
				groupBy(point -> point.getId()).//
				map(v1 -> {
					Iterable<TBInputTrajectory> trajectoryPoints = v1._2();

					// Sort trajectory points chronological
					ImmutableList<TBInputTrajectory> chronologicalPoints = Ordering
							.from((TBInputTrajectory point1, TBInputTrajectory point2) -> {
								long time1 = Utils.calculateTimeFromString(isPhysicalTime, point1.getDate1());
								long time2 = Utils.calculateTimeFromString(isPhysicalTime, point2.getDate1());

								return Long.compare(time1, time2);

							}).immutableSortedCopy(trajectoryPoints);

					// Compute distance threshold
					List<Double> distance = new ArrayList<>();
					List<TBOutlierTrajectory> outlierTrajectories = new ArrayList<TBOutlierTrajectory>();

					for (TBInputTrajectory trajectory : chronologicalPoints) {
						distance.add(trajectory.getDistance());

						Map<String, Double> additional = new HashMap<String, Double>();
						additional.putAll(trajectory.getAbs_prop());
						additional.putAll(trajectory.getRel_prop());

						outlierTrajectories.add(new TBOutlierTrajectory(//
								trajectory.getId(), //
								trajectory.getId_c(), //
								trajectory.getDate1(), //
								trajectory.getX1(), //
								trajectory.getY1(), //
								trajectory.getX2(), //
								trajectory.getY2(), //
								trajectory.getSpeed(), //
								additional, //
								false));
					}

					double distanceThreshold = Utils.computeQuantile(this.pDistance, distance) * this.jumpsThDistanceFactor;

					// Compute distance btw current and next and compare with threshold
					TBOutlierTrajectory markedcurrent = null;
					
					for (int i = 0; i <= outlierTrajectories.size() - 2; i++) {
						TBOutlierTrajectory current = outlierTrajectories.get(i);
						TBOutlierTrajectory next = outlierTrajectories.get(i + 1);

						double x1 = current.getX1();
						double y1 = current.getY1();
						double x2 = next.getX1();
						double y2 = next.getY1();

						double distanceCurrentNext = Utils.calculateDistance(areGeoCoordinates, x1, y1, x2, y2);

						if (distanceCurrentNext > distanceThreshold) {
							markedcurrent = current;
							next.setOutlier(true);

							outliers: for (i = i + 1; i <= outlierTrajectories.size() - 2; i++) {
								next = outlierTrajectories.get(i + 1);

								double x1_ = markedcurrent.getX1();
								double y1_ = markedcurrent.getY1();
								double x2_ = next.getX1();
								double y2_ = next.getY1();

								double distanceMarkedCurrentNext = Utils.calculateDistance(areGeoCoordinates, x1_, y1_,
										x2_, y2_);

								if (distanceMarkedCurrentNext > distanceThreshold) {
									next.setOutlier(true);
								} else {
									markedcurrent = null;
									break outliers;
								}
							}
						}
					
					
					}

					return outlierTrajectories;
				});

		// Cache RDD
		markedTrajectoriesDistanceMetricRDD.cache();

		return markedTrajectoriesDistanceMetricRDD.filter(arg -> {
			return Iterables.any(arg, p -> p.isOutlier());
		}).flatMap(arg -> arg);

	}

	/**
	 * @param inputRDD
	 * @return
	 */
	private JavaRDD<TBOutlierTrajectory> detectJumpsBySpeedMetric(JavaRDD<TBInputTrajectory> inputRDD) {
		final boolean isPhysicalTime = this.trajectoriesInputProvider.isTimeIsPhysical();
		final boolean areGeoCoordinates = this.trajectoriesInputProvider.isCoordinatesAreGeo();

		JavaRDD<Iterable<TBOutlierTrajectory>> markedTrajectoriesSpeedMetricRDD = inputRDD.//
				groupBy(point -> point.getId()).//
				map(v1 -> {
					Iterable<TBInputTrajectory> trajectoryPoints = v1._2();

					// Sort trajectory points chronological
					ImmutableList<TBInputTrajectory> chronologicalPoints = Ordering
							.from((TBInputTrajectory point1, TBInputTrajectory point2) -> {
								long time1 = Utils.calculateTimeFromString(isPhysicalTime, point1.getDate1());
								long time2 = Utils.calculateTimeFromString(isPhysicalTime, point2.getDate1());

								return Long.compare(time1, time2);

							}).immutableSortedCopy(trajectoryPoints);

					// Compute speed threshold
					List<Double> speeds = new ArrayList<>();
					List<TBOutlierTrajectory> outlierTrajectories = new ArrayList<TBOutlierTrajectory>();

					for (TBInputTrajectory trajectory : chronologicalPoints) {
						speeds.add(trajectory.getSpeed());

						Map<String, Double> additional = new HashMap<String, Double>();
						additional.putAll(trajectory.getAbs_prop());
						additional.putAll(trajectory.getRel_prop());

						outlierTrajectories.add(new TBOutlierTrajectory(//
								trajectory.getId(), //
								trajectory.getId_c(), //
								trajectory.getDate1(), //
								trajectory.getX1(), //
								trajectory.getY1(), //
								trajectory.getX2(), //
								trajectory.getY2(), //
								trajectory.getSpeed(), //
								additional, //
								false));
					}

					double speedThreshold = Utils.computeQuantile(this.pSpeed, speeds) * this.jumpsThSpeedFactor;

					// Compute speed btw current and next and compare with threshold
					TBOutlierTrajectory markedcurrent = null;
					
					for (int i = 0; i <= outlierTrajectories.size() - 2; i++) {
						TBOutlierTrajectory current = outlierTrajectories.get(i);
						TBOutlierTrajectory next = outlierTrajectories.get(i + 1);
						double speedCurrentNext = current.getSpeed();

						if (speedCurrentNext > speedThreshold) {
							markedcurrent = current;
							next.setOutlier(true);

							outliers: for (i = i + 1; i <= outlierTrajectories.size() - 2; i++) {
								next = outlierTrajectories.get(i + 1);
								double speedMarkedCurrentNext = calculateSpeed(markedcurrent, next, isPhysicalTime,
										areGeoCoordinates);

								if (speedMarkedCurrentNext > speedThreshold) {
									next.setOutlier(true);
								} else {
									markedcurrent = null;
									break outliers;
								}
							}
						}
					
					}

					return outlierTrajectories;
				});

		// Cache RDD
		markedTrajectoriesSpeedMetricRDD.cache();

		return markedTrajectoriesSpeedMetricRDD.filter(arg -> {
			return Iterables.any(arg, p -> p.isOutlier());
		}).flatMap(arg -> arg);

	}

	/**
	 * @param t1
	 * @param t2
	 * @param isPhysicalTime
	 * @param coordinatesAreGeo
	 * @return speed
	 */
	private static double calculateSpeed(//
			TBOutlierTrajectory t1, //
			TBOutlierTrajectory t2, //
			boolean isPhysicalTime, //
			boolean coordinatesAreGeo) {
		
		final double distance = Utils.calculateDistance(coordinatesAreGeo, t1.getX1(), t1.getY1(), t2.getX1(), t2.getY1());
		final long date1 = Utils.calculateTimeFromString(isPhysicalTime, t1.getDate1());
		final long date2 = Utils.calculateTimeFromString(isPhysicalTime, t2.getDate1());

		final double diffTime = Utils.calculateDiffTime(isPhysicalTime, date1, date2);
		final double speed = Utils.calculateSpeed(isPhysicalTime, distance, diffTime);

		return speed;
	}

	/**
	 * Correct trajectories with jumps.
	 * 
	 * @return
	 */
	private JavaRDD<TBOutputTrajectory> correct(JavaRDD<TBOutlierTrajectory> trajectoriesWithJumpsRDD) {
		return trajectoriesWithJumpsRDD.filter(arg -> !arg.isOutlier()).map(arg -> {
			return new TBOutputTrajectory(arg.getId(), //
					arg.getId_c(), //
					arg.getDate1(), //
					String.valueOf(arg.getX1()), //
					String.valueOf(arg.getY1()), //
					arg.getAdditional());
		});
	}
}
