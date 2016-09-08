package de.fhg.iais.kd.datacron.trajectories.computing.data.provider;

import org.apache.spark.api.java.JavaSparkContext;

import com.google.inject.Inject;

import de.fhg.iais.cassandra.AbstractDataProvider;
import de.fhg.iais.cassandra.Cassandra;
import de.fhg.iais.kd.datacron.trajectories.computing.table.beans.TBTrajectoriesStatisticsOutput;
import de.fhg.iais.kd.datacron.trajectories.computing.table.metadata.TBMTrajectoriesStatisticsOutput;

public class TrajectoriesStatisticsOutputProvider extends AbstractDataProvider<TBTrajectoriesStatisticsOutput> {

	private static final long serialVersionUID = 2540070645869505051L;

	@Inject
	public TrajectoriesStatisticsOutputProvider( //
			TBMTrajectoriesStatisticsOutput tableMetadata, //
			JavaSparkContext sparkContext, //
			Cassandra cassandra) {

		super(tableMetadata, sparkContext, cassandra, TBTrajectoriesStatisticsOutput.class);
	}

}
