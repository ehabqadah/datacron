package de.fhg.iais.kd.datacron.trajectories.cleaning.data.provider;

import org.apache.spark.api.java.JavaSparkContext;

import com.google.inject.Inject;

import de.fhg.iais.cassandra.AbstractDataProvider;
import de.fhg.iais.cassandra.Cassandra;
import de.fhg.iais.kd.datacron.trajectories.cleaning.table.beans.TBOutlierTrajectory;
import de.fhg.iais.kd.datacron.trajectories.cleaning.table.metadata.TBMTrajectoriesWithDuplicateIdsOutput;

/**
 * @author kthellmann
 */
public class TrajectoriesWithDuplicateIdsProvider extends AbstractDataProvider<TBOutlierTrajectory> {

	private static final long serialVersionUID = 5624798897127022424L;

	@Inject
	public TrajectoriesWithDuplicateIdsProvider( //
			TBMTrajectoriesWithDuplicateIdsOutput tableMetadata, //
			JavaSparkContext sparkContext, //
			Cassandra cassandra) {

		super(tableMetadata, sparkContext, cassandra, TBOutlierTrajectory.class);
	}

}
