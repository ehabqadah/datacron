package de.fhg.iais.kd.datacron.trajectories.cleaning.data.provider;

import org.apache.spark.api.java.JavaSparkContext;

import com.google.inject.Inject;

import de.fhg.iais.cassandra.AbstractDataProvider;
import de.fhg.iais.cassandra.Cassandra;
import de.fhg.iais.kd.datacron.trajectories.cleaning.table.beans.TBOutputTrajectory;
import de.fhg.iais.kd.datacron.trajectories.cleaning.table.metadata.TBMTrajectoriesWithDuplicateIdsOutput;
import de.fhg.iais.kd.datacron.trajectories.cleaning.table.metadata.TBMTrajectoriesWithoutDuplicateIdsOutput;

/**
 * @author kthellmann
 */
public class TrajectoriesWithoutDuplicateIdsProvider extends AbstractDataProvider<TBOutputTrajectory> {

	private static final long serialVersionUID = 5624798897127022424L;

	@Inject
	public TrajectoriesWithoutDuplicateIdsProvider( //
			TBMTrajectoriesWithoutDuplicateIdsOutput tableMetadata, //
			JavaSparkContext sparkContext, //
			Cassandra cassandra) {

		super(tableMetadata, sparkContext, cassandra, TBOutputTrajectory.class);
	}

}
