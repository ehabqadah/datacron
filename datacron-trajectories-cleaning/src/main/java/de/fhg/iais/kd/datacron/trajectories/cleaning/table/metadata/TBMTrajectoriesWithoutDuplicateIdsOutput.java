package de.fhg.iais.kd.datacron.trajectories.cleaning.table.metadata;

import com.google.inject.Inject;
import com.google.inject.name.Named;

/**
 * @author kthellmann
 */
public class TBMTrajectoriesWithoutDuplicateIdsOutput extends TBMOutputTrajectory {
	
	private static final long serialVersionUID = -8892051533364155874L;

	@Inject
	@Named("spark.app.trajectories.without.duplicate.ids.output")
	private String tableName;
	
	private static final String DESCRIPTION = "Trajectories without duplicate ids output.";

	public String getTableName() {
		return tableName;
	}

	@Override
	public String getDescription() {
		return DESCRIPTION;
	}

}
