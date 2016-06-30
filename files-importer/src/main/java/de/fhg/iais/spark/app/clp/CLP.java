package de.fhg.iais.spark.app.clp;

import com.beust.jcommander.Parameter;

/**
 * 
 * @author kthellmann
 *
 */
public class CLP {
	@Parameter(names = "-c", description = "Path to configuration file.")
	private String config;

	@Parameter(names = "-i", description = "Path to input data.")
	private String input;

	public CLP() {
	}

	public String getConfig() {
		return config;
	}

	public String getInput() {
		return input;
	}

}
