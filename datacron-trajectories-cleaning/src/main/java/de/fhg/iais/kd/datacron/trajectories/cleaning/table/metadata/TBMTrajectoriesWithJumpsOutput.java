package de.fhg.iais.kd.datacron.trajectories.cleaning.table.metadata;

import com.google.inject.Inject;
import com.google.inject.name.Named;

/**
 * @author kthellmann
 */
public class TBMTrajectoriesWithJumpsOutput extends TBMOutlierTrajectory {
	
	private static final long serialVersionUID = -1179949909933492278L;

	@Inject
	@Named("spark.app.trajectories.with.jumps.output")
	private String tableName;
	
	private static final String DESCRIPTION = "Trajectories with jumps output.";

	public String getTableName() {
		return tableName;
	}

	@Override
	public String getDescription() {
		return DESCRIPTION;
	}

}
