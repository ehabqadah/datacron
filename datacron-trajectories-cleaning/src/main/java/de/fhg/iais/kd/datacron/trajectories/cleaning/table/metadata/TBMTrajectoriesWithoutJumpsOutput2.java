package de.fhg.iais.kd.datacron.trajectories.cleaning.table.metadata;

import com.google.inject.Inject;
import com.google.inject.name.Named;

/**
 * @author kthellmann
 */
public class TBMTrajectoriesWithoutJumpsOutput2 extends TBMOutputTrajectory {
	
	private static final long serialVersionUID = 7898787411855061115L;

	@Inject
	@Named("spark.app.trajectories.without.jumps.dist.output")
	private String tableName;
	
	private static final String DESCRIPTION = "Trajectories without jumps output. (Distance based metric)";

	public String getTableName() {
		return tableName;
	}

	@Override
	public String getDescription() {
		return DESCRIPTION;
	}

}
