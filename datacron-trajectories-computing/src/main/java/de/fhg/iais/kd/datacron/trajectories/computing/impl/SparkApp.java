package de.fhg.iais.kd.datacron.trajectories.computing.impl;

import com.google.inject.Inject;

import de.fhg.iais.kd.datacron.trajectories.computing.Trajectories;

/**
 * @author kthellmann
 *
 */
public class SparkApp implements ISparkApp{

	private static final long serialVersionUID = -2456126582786815585L;

	private transient final Trajectories trajectories;
	
	@Inject
	public SparkApp(Trajectories trajectories ) {
		
		this.trajectories = trajectories;
		
	}

	@Override
	public void run() {
		
		// Compute trajectories
		this.trajectories.compute();
		
	}

}
