package de.fhg.iais.kd.datacron.trajectories.computing;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.storage.StorageLevel;
import org.joda.time.DateTime;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Ordering;
import com.google.common.collect.PeekingIterator;
import com.google.inject.Inject;
import com.google.inject.Singleton;

import de.fhg.iais.kd.datacron.trajectories.computing.data.provider.TrajectoriesOutputProviderND;
import de.fhg.iais.kd.datacron.trajectories.computing.data.provider.TrajectoriesOutputProviderNDS;
import de.fhg.iais.kd.datacron.trajectories.computing.data.provider.ParametrizedInputProvider;
import de.fhg.iais.kd.datacron.trajectories.computing.table.beans.TBTrajectoriesOutput;
import de.fhg.iais.kd.datacron.trajectories.computing.table.beans.TBTrajectoriesParametrizedInput;
import de.fhg.iais.kd.datacron.trajectories.computing.utils.AnalyticsUtils;
import scala.Tuple2;

/**
 * @author kthellmann
 *
 */
@Singleton
public class Trajectories implements Serializable {

	private static final long serialVersionUID = 2103621255207851003L;

	@Inject
	private transient ParametrizedInputProvider parametrizeInputProvider;
	@Inject
	private transient TrajectoriesOutputProviderND outputProviderND;
	@Inject
	private transient TrajectoriesOutputProviderNDS outputProviderNDS;
	@Inject
	private transient Statistics statistics;

	public void compute() {
		// 1. Create input1RDD from inputData
		JavaRDD<TBTrajectoriesParametrizedInput> input1RDD = parametrizeInputProvider.createInputBean();

		// Cache input
		input1RDD.persist(StorageLevel.MEMORY_AND_DISK());

		// 2. Eliminate duplicates
		// 2.1 Group by id and date
		JavaPairRDD<Tuple2<String, Long>, Iterable<TBTrajectoriesParametrizedInput>> tuple1RDD = input1RDD
				.groupBy(input -> new Tuple2<>(input.getId(), input.getD()));

		// 2.2 Compute average of coordinates
		JavaRDD<TBTrajectoriesParametrizedInput> input2RDD = tuple1RDD.map(tuple -> {
			Iterable<TBTrajectoriesParametrizedInput> records = tuple._2();

			double sumX = 0.0;
			double sumY = 0.0;
			int recordsCounter = 0;

			for (TBTrajectoriesParametrizedInput record1 : records) {
				sumX += record1.getX();
				sumY += record1.getY();
				recordsCounter++;
			}

			final double averageX = sumX / recordsCounter;
			final double averageY = sumY / recordsCounter;

			TBTrajectoriesParametrizedInput record2 = records.iterator().next();

			record2.setX(averageX);
			record2.setY(averageY);

			return record2;

		});

		// 3. Compute trajectories
		// 3.1. Group records from input2RDD by id pair1RDD := (id, nonduplicates records) NDR
		JavaPairRDD<String, Iterable<TBTrajectoriesParametrizedInput>> pair1RDD = input2RDD.groupBy(input -> input.getId());

		// 3.2 Compute trajectory attributes: time-/coordinate differences, distance, course, speed, trace and acceleration
		JavaRDD<TBTrajectoriesOutput> result1RDD = computeTrajectories(pair1RDD);

		// Cache result
		result1RDD.persist(StorageLevel.MEMORY_AND_DISK());
		outputProviderND.updateTable(result1RDD);

		// 4. Remove stationary point and recompute trajectories
		JavaPairRDD<String, Iterable<TBTrajectoriesOutput>> pair2RDD = result1RDD.groupBy(result1 -> result1.getId());

		// Create pair3RDD := (id, stationarypoints)
		JavaPairRDD<String, Iterable<TBTrajectoriesOutput>> pair3RDD = pair2RDD.mapToPair(tuple -> {
			final Iterable<TBTrajectoriesOutput> trajectories = tuple._2();

			// Compute lists of trajectories sorted by id
			ImmutableList<TBTrajectoriesOutput> orderedRecordsById = Ordering.from(
					(TBTrajectoriesOutput firstTrajectory1, TBTrajectoriesOutput secondTrajectory1) -> firstTrajectory1
							.getId().compareTo(secondTrajectory1.getId()))
					.immutableSortedCopy(trajectories);

			// Create list with stationary points
			final Builder<TBTrajectoriesOutput> stationaryPointsBuilder = ImmutableList.builder();

			for (int t = 0; t + 2 < orderedRecordsById.size(); t++) {
				final TBTrajectoriesOutput firstTrajectory = orderedRecordsById.get(t);
				final TBTrajectoriesOutput secondTrajectory = orderedRecordsById.get(t + 1);
				final TBTrajectoriesOutput thirdTrajectory = orderedRecordsById.get(t + 2);

				if (firstTrajectory.getSpeed() < 1.0 && //
				secondTrajectory.getSpeed() < 1.0 && //
				thirdTrajectory.getSpeed() < 1.0) {
					stationaryPointsBuilder.add(secondTrajectory);
				}
			}

			ImmutableList<TBTrajectoriesOutput> stationaryPointsList = stationaryPointsBuilder.build();

			return new Tuple2<>(tuple._1(), stationaryPointsList);
		});

		// Compute join1RDD := pair1RDD.join(pair2RDD) by id -> (list non duplicates, list stationary points)
		JavaPairRDD<String, Tuple2<Iterable<TBTrajectoriesParametrizedInput>, Iterable<TBTrajectoriesOutput>>> join1RDD = pair1RDD.join(pair3RDD);

		// Create pair4RDD := (id, nonduplicates nonstationary records) //NDS
		final boolean timeIsPhysical = this.parametrizeInputProvider.isTimeIsPhysical();
		
		JavaPairRDD<String, Iterable<TBTrajectoriesParametrizedInput>> pair4RDD = join1RDD.mapToPair(tuple -> {
			final String id = tuple._1();
			final Iterable<TBTrajectoriesParametrizedInput> nonDuplicatesList = tuple._2()._1();
			final Iterable<TBTrajectoriesOutput> stationaryPointsList = tuple._2()._2();

			final Builder<TBTrajectoriesParametrizedInput> noStationaryPointsBuilder = ImmutableList.builder();

			for (TBTrajectoriesParametrizedInput nonduplicate : nonDuplicatesList) {
				final long date1 = nonduplicate.getD();

				boolean isStationaryPoint = Iterables.any(stationaryPointsList, stationarypoint -> {
					final String date2String = stationarypoint.getDate1();

					final long date2;
					if (timeIsPhysical) {
						date2 = DateTime.parse(date2String).getMillis();
					} else {
						date2 = Long.valueOf(date2String);
					}

					return date2 == date1;
				});

				if (!isStationaryPoint) {
					noStationaryPointsBuilder.add(nonduplicate);
				}
			}

			return new Tuple2<>(id, noStationaryPointsBuilder.build());
		});

		// Recompute trajectories
		JavaRDD<TBTrajectoriesOutput> result2RDD = this.computeTrajectories(pair4RDD);

		// Compute trajectories statistic
		statistics.generate(result2RDD);

		// Update result table
		outputProviderNDS.updateTable(result2RDD);

		// Close connection
		outputProviderND.close();

	}

	/**
	 * @param pair1RDD
	 * @return
	 */
	private JavaRDD<TBTrajectoriesOutput> computeTrajectories(
			JavaPairRDD<String, Iterable<TBTrajectoriesParametrizedInput>> pair1RDD) {
		final boolean isPhysicalTime = this.parametrizeInputProvider.isTimeIsPhysical();
		final boolean areGeoCoordinates = this.parametrizeInputProvider.isCoordinatesAreGeo();

		return pair1RDD.map(tuple -> {
			final Iterable<TBTrajectoriesParametrizedInput> records = tuple._2();

			// Compute lists of input records sorted by date
			ImmutableList<TBTrajectoriesParametrizedInput> orderedRecordsByDate = Ordering.from(
					(TBTrajectoriesParametrizedInput firstRecord, TBTrajectoriesParametrizedInput secondRecord) -> Long
							.compare(firstRecord.getD(), secondRecord.getD()))
					.immutableSortedCopy(records);

			// Compute list of counted input records tuples: (id, [<1,(current, next)>, <2, (current, next)>, ...])
			final Builder<Tuple2<Integer, Tuple2<TBTrajectoriesParametrizedInput, TBTrajectoriesParametrizedInput>>> pairRecordsBuilder = ImmutableList
					.builder();
			final PeekingIterator<TBTrajectoriesParametrizedInput> peekIterator1 = Iterators
					.peekingIterator(orderedRecordsByDate.iterator());

			int recordsCounter = 1;

			while (peekIterator1.hasNext()) {
				final TBTrajectoriesParametrizedInput currentRecord = peekIterator1.next();

				if (peekIterator1.hasNext()) {
					final TBTrajectoriesParametrizedInput nextRecord = peekIterator1.peek();
					pairRecordsBuilder.add(new Tuple2<>(recordsCounter++, new Tuple2<>(currentRecord, nextRecord)));
				}
			}

			ImmutableList<Tuple2<Integer, Tuple2<TBTrajectoriesParametrizedInput, TBTrajectoriesParametrizedInput>>> pairedRecordsByDate = pairRecordsBuilder
					.build();

			// Compute course, speed, distance and time-/coordinate differences
			// btw. current and next record
			final Builder<TBTrajectoriesOutput> trajectoriesBuilder1 = ImmutableList.builder();

			for (Tuple2<Integer, Tuple2<TBTrajectoriesParametrizedInput, TBTrajectoriesParametrizedInput>> recordPair : pairedRecordsByDate) {
				final int trajectoryCounter = recordPair._1();
				final TBTrajectoriesParametrizedInput record1 = recordPair._2()._1();
				final String id = record1.getId();
				final long date1 = record1.getD();
				final double x1 = record1.getX();
				final double y1 = record1.getY();

				final TBTrajectoriesParametrizedInput record2 = recordPair._2()._2();
				final long date2 = record2.getD();
				final double x2 = record2.getX();
				final double y2 = record2.getY();

				int diffTime = 0;

				if (isPhysicalTime) {
					diffTime = (int) TimeUnit.MILLISECONDS.toDays(date2 - date1);
				} else {
					diffTime = Math.toIntExact(date2 - date1);
				}

				final double diffX = x2 - x1;
				final double diffY = y2 - y1;

				double distance = 0.0;

				if (areGeoCoordinates) {
					distance = AnalyticsUtils.greatCircle(y1, x1, y2, x2);
				} else {
					distance = Math.sqrt(Math.pow(x2 - x1, 2) + Math.pow(y2 - y1, 2));
				}

				final Map<String, Double> absoluteParamsDifferencesMap = subtractMap(record2.getAbs_prop(),
						record1.getAbs_prop());

				double speed = 0.0;

				final Map<String, Double> relativeParamsValuesMap;
				
				if (diffTime > 0) {
					final Map<String, Double> relativeParamsDifferencesMap = subtractMap(record2.getRel_prop(), record1.getRel_prop());

					if (isPhysicalTime) {
						speed = distance / (diffTime * 24);
						relativeParamsValuesMap = divideMapBy(relativeParamsDifferencesMap, diffTime * 24 * 3600);
					} else {
						speed = distance / diffTime;
						relativeParamsValuesMap = divideMapBy(relativeParamsDifferencesMap, diffTime);
					}
				} else {
					relativeParamsValuesMap = null;
				}

			    double course = 180 * Math.atan2(diffX, diffY) / Math.PI;

				if (course < 0) {
					course += 360;
				}

				TBTrajectoriesOutput trajectory = new TBTrajectoriesOutput();
				trajectory.setId(id);
				trajectory.setId_c(trajectoryCounter);

				if (isPhysicalTime) {
					trajectory.setDate1(String.valueOf(new DateTime(date1)));
					trajectory.setDate2(String.valueOf(new DateTime(date2)));
				} else {
					trajectory.setDate1(String.valueOf(date1));
					trajectory.setDate2(String.valueOf(date2));
				}

				trajectory.setDifftime(diffTime);
				trajectory.setX1(x1);
				trajectory.setX2(x2);
				trajectory.setDiffx(diffX);
				trajectory.setY1(y1);
				trajectory.setY2(y2);
				trajectory.setDiffy(diffY);
				trajectory.setDistance(distance);
				trajectory.setSpeed(speed);
				trajectory.setCourse(course);
				trajectory.setAbs_prop(absoluteParamsDifferencesMap);
				trajectory.setRel_prop(relativeParamsValuesMap);
				trajectory.setGeocoordinates(areGeoCoordinates);
				trajectory.setPhysicaltime(isPhysicalTime);

				trajectoriesBuilder1.add(trajectory);
			}

			final ImmutableList<TBTrajectoriesOutput> trajectoriesList = trajectoriesBuilder1.build();

			// Compute acceleration and turn btw. current and next record pair
			final PeekingIterator<TBTrajectoriesOutput> peekIterator2 = Iterators.peekingIterator(trajectoriesList.iterator());
			final Builder<TBTrajectoriesOutput> trajectoriesBuilder2 = ImmutableList.builder();

			while (peekIterator2.hasNext()) {
				TBTrajectoriesOutput recordsPair1 = peekIterator2.next();
				final long diffTime1 = recordsPair1.getDifftime();
				final double speed1 = recordsPair1.getSpeed();
				final double course1 = recordsPair1.getCourse();

				if (peekIterator2.hasNext()) {
					final TBTrajectoriesOutput recordsPair2 = peekIterator2.peek();
					double acceleration = 0.0;

					if (diffTime1 > 0) {
						final double speed2 = recordsPair2.getSpeed();

						if (isPhysicalTime) {
							acceleration = (speed2 - speed1) / (diffTime1 * 24 * 3600);
						} else {
							acceleration = (speed2 - speed1) / diffTime1;
						}

						recordsPair1.setAcceleration(acceleration);
					}

					final double course2 = recordsPair2.getCourse();
					final double turn = course1 - course2;

					recordsPair1.setTurn(turn);
				}

				trajectoriesBuilder2.add(recordsPair1);
			}

			return trajectoriesBuilder2.build();

		}).flatMap(result -> result);
	}

	/**
	 * @param map1
	 * @param map2
	 * @return
	 */
	private Map<String, Double> subtractMap(Map<String, Double> map1, Map<String, Double> map2) {
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
	private Map<String, Double> divideMapBy(Map<String, Double> map, double divisor) {
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
