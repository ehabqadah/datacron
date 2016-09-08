package de.fhg.iais.kd.datacron.files.importer.clp;

import java.util.ArrayList;
import java.util.List;

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
			description = "Path to input data mapping file.", //
			required = true, //
			validateWith = CLParameterValidator.class)
	private String mapping;
	
	@Parameter(names = "-o", //
			description = "Path to files with outliers (black lists).", //
			required = true, //
			validateWith = CLParameterValidator.class)
	private List<String> outlier = new ArrayList<String>();
	
	@Parameter(names = "-i", //
			description = "Path to input files.", //
			required = true, //
			validateWith = CLParameterValidator.class)
	private String input;
	

	public String getConfigurations() {
		return this.configurations;
	}
	
	public String getMapping() {
		return this.mapping;
	}

	public List<String> getOutlier() {
		return outlier;
	}
	
	public String getInput() {
		return input;
	}

}
