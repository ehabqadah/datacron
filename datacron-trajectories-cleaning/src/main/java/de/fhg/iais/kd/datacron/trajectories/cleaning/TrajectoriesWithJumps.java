package de.fhg.iais.kd.datacron.trajectories.cleaning;

import java.io.Serializable;

import org.apache.spark.api.java.JavaRDD;

import com.google.inject.Singleton;

import de.fhg.iais.kd.datacron.trajectories.cleaning.table.beans.TBTrajectories;

/**
 * @author kthellmann
 */
@Singleton
public class TrajectoriesWithJumps implements Serializable {

	private static final long serialVersionUID = 5418827747835053055L;

	/**
	 * Detect and correct corrupt trajectories:
	 * 
	 * I. Characteristics:
	 *   
	 * II. Detection criteria: 
	 *  > Large distance in a short time interval btw. 2 consecutive points
	 */
	public void clean() {		
		this.detect();
 	}
	
	/**
	 * Detect corrupt trajectories.
	 */
	private void detect(){		
	}
	
	/**
	 * Correct corrupt trajectories.
	 */
	private void correct(JavaRDD<TBTrajectories> trajectoriesWithDuplicateIDsRDD){		
	}	

}
