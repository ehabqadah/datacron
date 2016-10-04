package de.fhg.iais.kd.datacron.trajectories.cleaning.impl;

import com.google.inject.Inject;

import de.fhg.iais.kd.datacron.trajectories.cleaning.TrajectoriesWithDuplicateIds;
import de.fhg.iais.kd.datacron.trajectories.cleaning.TrajectoriesWithJumps;

/**
 * @author kthellmann
 *
 */
public class SparkApp implements ISparkApp{

	private static final long serialVersionUID = -2456126582786815585L;

	private transient final TrajectoriesWithDuplicateIds trajectoriesWithDuplicateIds;
	
	private transient final TrajectoriesWithJumps trajectoriesWithJumps;
	
	@Inject
	public SparkApp(//
			TrajectoriesWithDuplicateIds trajectoriesWithDuplicateIds, //
			TrajectoriesWithJumps trajectoriesWithJumps) {
		
		this.trajectoriesWithDuplicateIds = trajectoriesWithDuplicateIds;
		this.trajectoriesWithJumps = trajectoriesWithJumps;		
	}

	@Override
	public void run() {
		
		// Clean corrupt trajectories
		//this.trajectoriesWithDuplicateIds.clean();
		this.trajectoriesWithJumps.clean();		
	}

}
