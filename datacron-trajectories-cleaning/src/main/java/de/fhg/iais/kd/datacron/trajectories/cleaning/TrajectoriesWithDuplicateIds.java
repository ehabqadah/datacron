package de.fhg.iais.kd.datacron.trajectories.cleaning;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.name.Named;

import de.fhg.iais.kd.datacron.common.utils.Utils;
import de.fhg.iais.kd.datacron.trajectories.cleaning.data.provider.StatisticsProvider;
import de.fhg.iais.kd.datacron.trajectories.cleaning.data.provider.TrajectoriesProvider;
import de.fhg.iais.kd.datacron.trajectories.cleaning.data.provider.TrajectoriesWithDuplicateIdsProvider;
import de.fhg.iais.kd.datacron.trajectories.cleaning.data.provider.TrajectoriesWithoutDuplicateIdsProvider;
import de.fhg.iais.kd.datacron.trajectories.cleaning.table.beans.TBInputTrajectory;
import de.fhg.iais.kd.datacron.trajectories.cleaning.table.beans.TBOutlierTrajectory;
import de.fhg.iais.kd.datacron.trajectories.cleaning.table.beans.TBOutputTrajectory;
import de.fhg.iais.kd.datacron.trajectories.cleaning.table.beans.TBStatistics;
import scala.Tuple2;

/**
 * Detect and correct trajectories with duplicate ids:
 * 
 * I. Characteristics: Non-unique ID. Time intervals overlapping.
 * 
 * II. Detection criteria: Exceptional high speed median
 * 
 * III. Correction approach: Determine distance threshold of tx:Compute min
 * spanning tree (edge=distance btw. points in chronological order ). Compute
 * median m* of distances (edges of spanning tree) (Idea: Median might help in
 * determining the minimal erroneous connection btw. 2 points). Determine
 * largest distance d* btw 2 points of tx.Compute threshold_tx for separating tx
 * into tx1 and tx2: (m* + d*)/2 (Idea: Average of min and max erronous
 * connection of tx). Perform splitting for each corrupt trajectory of type dID
 * by using threshold_tx.
 * 
 * @author kthellmann
 */
@Singleton
public class TrajectoriesWithDuplicateIds implements Serializable {

	private static final long serialVersionUID = 2103621255207851003L;

	@Inject
	private transient TrajectoriesProvider trajectoriesInputProvider;
	@Inject
	private transient StatisticsProvider statisticsInputProvider;
	@Inject
	private transient TrajectoriesWithDuplicateIdsProvider trajectoriesWithDuplicateIdsProvider;
	@Inject
	private transient TrajectoriesWithoutDuplicateIdsProvider trajectoriesWithoutDuplicateIdsProvider;

	@Inject
	@Named("spark.app.trajectories.dupid.th.factor")
	private double dupIdThresholdFactor;

	/**
	 * Detect and correct trajectories with duplicate id
	 */
	public void clean() {
		this.detect();
	}

	/**
	 * Detect trajectories with duplicate ids
	 */
	private void detect() {
		// Load computed trajectories
		final JavaRDD<TBInputTrajectory> inputRDD = trajectoriesInputProvider.readInputTableRows();

		// Load computed statistics about trajectories
		final JavaRDD<TBStatistics> statisticsRDD = statisticsInputProvider.readInputTableRows();

		// Create speedMedianRDD: (TrajectoryId, speed_m_ti)
		// Speed median speed_m_ti for each trajectory i€ {1..n}
		final JavaPairRDD<String, Double> speedMedianRDD = statisticsRDD.mapToPair(arg0 -> {
			return new Tuple2<>(arg0.getId(), arg0.getMedian_speed());
		});

		// Compute median m of all speed_m_ti
		final double speedMedian = Utils.computeMedian(speedMedianRDD.mapToPair(pair -> new Tuple2<>("ALL", pair._2())))
				.first()._2();

		// Set threshold th = c * m (c constant)
		final double speedThreshold = dupIdThresholdFactor * speedMedian;

		// Create speedAverageRDD:(TrajectoryId, speed_avg_ti)
		// Speed average speed_avg_ti for each trajectory i€ {1..n}
		final JavaPairRDD<String, Double> speedAverageRDD = statisticsRDD.mapToPair(arg0 -> {
			return new Tuple2<>(arg0.getId(), arg0.getAvg_speed());
		});

		// Compare all speed_avg_ti with m: if (speed_avg_ti > th) then possible
		// outlier
		final JavaPairRDD<String, Double> speedOutlierRDD = speedAverageRDD.filter(tuple -> {
			if (tuple._2() > speedThreshold) {
				return true;
			}

			return false;
		});

		final Set<String> speedOutlier = new HashSet<>(speedOutlierRDD.keys().collect());

		// Filter trajectories with duplicate ids
		final JavaRDD<TBOutlierTrajectory> trajectoriesWithDuplicateIDsRDD = inputRDD.filter(arg0 -> {
			if (speedOutlier.contains(arg0.getId())) {
				return true;
			}

			return false;
		}).map(arg0 -> {
			boolean outlier = false;

			if (arg0.getSpeed() > speedThreshold) {
				outlier = true;
			}

			Map<String, Double> additional = new HashMap<String, Double>();
			additional.putAll(arg0.getAbs_prop());
			additional.putAll(arg0.getRel_prop());

			return new TBOutlierTrajectory(//
					arg0.getId(), //
					arg0.getId_c(), //
					arg0.getDate1(), //
					arg0.getX1(), //
					arg0.getY1(), //
					arg0.getX2(), //
					arg0.getY2(), //
					arg0.getSpeed(), //
					additional, //
					outlier);
		});

		// Cache RDD
		trajectoriesWithDuplicateIDsRDD.cache();

		// Update table of trajectories with duplicate ids
		trajectoriesWithDuplicateIdsProvider.updateTable(trajectoriesWithDuplicateIDsRDD);

		// Fix corrupt trajectory
		this.correct(trajectoriesWithDuplicateIDsRDD);
		trajectoriesInputProvider.close();
	}

	/**
	 * Correct trajectories with duplicate ids
	 */
	private void correct(JavaRDD<TBOutlierTrajectory> trajectoriesWithDuplicateIDsRDD) {
		// I. Determine distance threshold of tx:
		final boolean isPhysicalTime = trajectoriesInputProvider.isTimeIsPhysical();
		final boolean areGeoCoordinates = trajectoriesInputProvider.isCoordinatesAreGeo();

		JavaRDD<TBOutputTrajectory> splitTrajectoriesRDD = trajectoriesWithDuplicateIDsRDD
				.groupBy(point -> point.getId()) //
				.flatMap(v1 -> {
					String idTx = v1._1();
					Iterable<TBOutlierTrajectory> trajectoryTxPoints = v1._2();

					// Sort trajectory points chronological
					ImmutableList<TBOutlierTrajectory> chronologicalPoints = sortChronologically(trajectoryTxPoints,
							isPhysicalTime);

					double maxSpeed = 0;
					int headTrajectory1 = -1;
					int headTrajectory2 = -1;

					for (int i = 0; i < chronologicalPoints.size() - 1; i++) {
						TBOutlierTrajectory currentPoint = chronologicalPoints.get(i);
						double speed = currentPoint.getSpeed();
						if (speed > maxSpeed) {
							maxSpeed = speed;
							headTrajectory1 = i;
							headTrajectory2 = i + 1;
						}
					}

					System.out.println("Speed: " + maxSpeed);
					System.out.println("Head 1: #" + headTrajectory1 + " = "
							+ chronologicalPoints.get(headTrajectory1).toString());
					System.out.println("Head 2: #" + headTrajectory2 + " = "
							+ chronologicalPoints.get(headTrajectory2).toString());

					// II. Perform splitting of tx into tx1 and tx2
					List<TBOutputTrajectory> t1 = new ArrayList<TBOutputTrajectory>();
					List<TBOutputTrajectory> t2 = new ArrayList<TBOutputTrajectory>();

					String idTx2 = idTx + "_" + ((int) (Math.random() * Integer.MAX_VALUE));

					TBOutlierTrajectory current = chronologicalPoints.get(headTrajectory1);
					t1.add(toOutputTrajectory(idTx, current));
					TBOutlierTrajectory previous = chronologicalPoints.get(headTrajectory2);
					t2.add(toOutputTrajectory(idTx2, previous));
					
					boolean currentIsInT1 = true;

					for (int i = headTrajectory1 - 1; i >= 0; i--) {
						TBOutlierTrajectory next = chronologicalPoints.get(i);

						double speedNextCurrent = next.getSpeed();
						double speedNextPrevious = calculateTBOutlierSpeed(isPhysicalTime, areGeoCoordinates, next,
								previous);

						if (speedNextCurrent <= speedNextPrevious) {
							// next belongs to trajectory of current
							if (currentIsInT1) {
								t1.add(toOutputTrajectory(idTx, next));
							} else {
								t2.add(toOutputTrajectory(idTx2, next));
							}
						} else {
							// next belongs to trajectory of previous
							if (currentIsInT1) {
								t2.add(toOutputTrajectory(idTx2, next));
								currentIsInT1=false;
							} else {
								t1.add(toOutputTrajectory(idTx, next));
								currentIsInT1=true;
							}

							previous = current;
						}

						current = next;
					}

					current = chronologicalPoints.get(headTrajectory2);
					previous = chronologicalPoints.get(headTrajectory1);
					currentIsInT1 = false;

					for (int i = headTrajectory2; i < chronologicalPoints.size() - 1; i++) {
						TBOutlierTrajectory next = chronologicalPoints.get(i);
						double speedCurrentNext = current.getSpeed();

						double speedPreviousNext = calculateTBOutlierSpeed(isPhysicalTime, areGeoCoordinates, previous,
								next);

						if (speedCurrentNext <= speedPreviousNext) {
							// next belongs to trajectory of current
							if (currentIsInT1) {
								t1.add(toOutputTrajectory(idTx, next));
							} else {
								t2.add(toOutputTrajectory(idTx2, next));
							}
						} else {
							// next belongs to trajectory of previous
							if (currentIsInT1) {
								t2.add(toOutputTrajectory(idTx2, next));
								currentIsInT1 = false;
							} else {
								t1.add(toOutputTrajectory(idTx, next));
								currentIsInT1 = true;
							}

							previous = current;
						}

						current = next;
					}

					return Iterables.concat(t1, t2);
				});

		splitTrajectoriesRDD.cache();

		trajectoriesWithoutDuplicateIdsProvider.updateTable(splitTrajectoriesRDD);

	}

	private double calculateTBOutlierSpeed(final boolean isPhysicalTime, final boolean areGeoCoordinates,
			TBOutlierTrajectory trajectory1, TBOutlierTrajectory trajectory2) {
		double x1 = trajectory1.getX1();
		double y1 = trajectory1.getY1();
		double x2 = trajectory2.getX1();
		double y2 = trajectory2.getY1();
		double distance = Utils.calculateDistance(areGeoCoordinates, x1, y1, x2, y2);

		long date1 = Utils.calculateTimeFromString(isPhysicalTime, trajectory1.getDate1());
		long date2 = Utils.calculateTimeFromString(isPhysicalTime, trajectory2.getDate1());
		double diffTime = Utils.calculateDiffTime(isPhysicalTime, date1, date2);

		return Utils.calculateSpeed(isPhysicalTime, distance, diffTime);
	}

	private TBOutputTrajectory toOutputTrajectory(String id, TBOutlierTrajectory tbOutlierTrajectory) {
		return new TBOutputTrajectory(//
				id, //
				tbOutlierTrajectory.getId_c(), //
				tbOutlierTrajectory.getDate1(), //
				String.valueOf(tbOutlierTrajectory.getX1()), //
				String.valueOf(tbOutlierTrajectory.getY1()), //
				tbOutlierTrajectory.getAdditional());
	}

	private static ImmutableList<TBOutlierTrajectory> sortChronologically(
			Iterable<TBOutlierTrajectory> trajectoryTxPoints, final boolean isPhysicalTime) {
		return Ordering.from((TBOutlierTrajectory point1, TBOutlierTrajectory point2) -> {
			long time1 = Utils.calculateTimeFromString(isPhysicalTime, point1.getDate1());
			long time2 = Utils.calculateTimeFromString(isPhysicalTime, point2.getDate1());

			return Long.compare(time1, time2);

		}).immutableSortedCopy(trajectoryTxPoints);
	}

}
