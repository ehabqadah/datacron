package de.fhg.iais.kd.datacron.trajectories.cleaning.clp;

import com.beust.jcommander.Parameter;

/**
 * @author kthellmann
 *
 */
public class CLParameter {
	
	@Parameter(names = "-c", //
			description = "Path to configuration file.", //
			required = true, //
			validateWith = CLParameterValidator.class)
	private String configurations;
	
	@Parameter(names = "-m", //
			description = "Path to input metadata.", //
			required = true, //
			validateWith = CLParameterValidator.class)
	private String metadata;
	

	public String getConfigurations() {
		return this.configurations;
	}
	
	public String getMetadata() {
		return this.metadata;
	}

}
