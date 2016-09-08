package de.fhg.iais.kd.datacron.trajectories.cleaning.data.provider;

import org.apache.spark.api.java.JavaSparkContext;

import com.google.inject.Inject;

import de.fhg.iais.cassandra.AbstractDataProvider;
import de.fhg.iais.cassandra.Cassandra;
import de.fhg.iais.kd.datacron.trajectories.cleaning.table.beans.TBStatistics;
import de.fhg.iais.kd.datacron.trajectories.cleaning.table.metadata.TBMStatistics;

public class StatisticsProvider extends AbstractDataProvider<TBStatistics> {

	private static final long serialVersionUID = 3042437178960616961L;

	@Inject
	public StatisticsProvider( //
			TBMStatistics tableMetadata, //
			JavaSparkContext sparkContext, //
			Cassandra cassandra) {

		super(tableMetadata, sparkContext, cassandra, TBStatistics.class);
	}

}
