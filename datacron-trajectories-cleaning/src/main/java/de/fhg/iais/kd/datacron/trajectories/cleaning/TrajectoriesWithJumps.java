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
import de.fhg.iais.kd.datacron.trajectories.cleaning.data.provider.TrajectoriesWithoutJumpsProvider;
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
	@Named("spark.app.trajectories.jumps.th.factor")
	private double jumpsThresholdFactor;

	@Inject
	@Named("spark.app.trajectories.quantile.p")
	private double p;

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
		final JavaRDD<TBInputTrajectory> inputRDD = trajectoriesInputProvider.readInputTableRows();
		final boolean isPhysicalTime = trajectoriesInputProvider.isTimeIsPhysical();
		
		// Identify jumps
		JavaRDD<Iterable<TBOutlierTrajectory>> markedTrajectoriesRDD = inputRDD.//
				groupBy(point -> point.getId()).//
				map(v1 -> {
					Iterable<TBInputTrajectory> trajectoryPoints = v1._2();

					// Determine if time is physical and if coordinates are
					// geographical

					// Sort trajectory points chronological
					ImmutableList<TBInputTrajectory> chronologicalPoints = Ordering
							.from((TBInputTrajectory point1, TBInputTrajectory point2) -> {
								long time1 = Utils.calculateTimeFromString(isPhysicalTime, point1.getDate1());
								long time2 = Utils.calculateTimeFromString(isPhysicalTime, point2.getDate1());

								return Long.compare(time1, time2);

							}).immutableSortedCopy(trajectoryPoints);

					// Compute distance threshold
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

					double threshold = Utils.computeQuantile(p, speeds) * jumpsThresholdFactor;

					// Compute distances btw current and next and and compare with threshold
					for (int i = 0; i <= outlierTrajectories.size() - 3; i++) {
						TBOutlierTrajectory current = outlierTrajectories.get(i);
						if (!current.isOutlier()) {
							TBOutlierTrajectory next = outlierTrajectories.get(i + 1);
							double speedCurrentNext = current.getSpeed();
							if (speedCurrentNext > threshold) {
								next.setOutlier(true);
							}
						}
					}

					return outlierTrajectories;
				});

		// Cache RDD
		markedTrajectoriesRDD.cache();

		JavaRDD<TBOutlierTrajectory> trajectoriesWithJumpsRDD = markedTrajectoriesRDD.filter(//
				arg -> {
					return Iterables.any(arg, p -> p.isOutlier());
				}).flatMap(//
						arg -> arg//
		);

		trajectoriesWithJumpsRDD.cache();

		// Update table of trajectories with jumps
		trajectoriesWithJumpsProvider.updateTable(trajectoriesWithJumpsRDD);

		// Fix corrupt trajectory
		this.correct(trajectoriesWithJumpsRDD);
		
		trajectoriesInputProvider.close();
	}

	/**
	 * Correct trajectories with jumps.
	 */
	private void correct(JavaRDD<TBOutlierTrajectory> trajectoriesWithJumpsRDD) {
		JavaRDD<TBOutputTrajectory> trajectoriesWithoutJumpsRDD = trajectoriesWithJumpsRDD
				.filter(arg -> !arg.isOutlier()).map(arg -> {
					return new TBOutputTrajectory(arg.getId(), //
							arg.getId_c(), //
							arg.getDate1(), //
							String.valueOf(arg.getX1()), //
							String.valueOf(arg.getY1()), //
							arg.getAdditional());
				});

		trajectoriesWithoutJumpsProvider.updateTable(trajectoriesWithoutJumpsRDD);
	}
}
