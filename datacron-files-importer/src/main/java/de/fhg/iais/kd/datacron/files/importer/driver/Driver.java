package de.fhg.iais.kd.datacron.files.importer.driver;

import java.io.Serializable;

import com.google.inject.Inject;

import de.fhg.iais.kd.datacron.files.importer.impl.SparkApp;


/**
 * @author kthellmann
 *
 */
public class Driver implements Serializable {

	private static final long serialVersionUID = 6019283703351410219L;

	private SparkApp analyticsApp;

	@Inject
	public Driver(SparkApp analyticsApp) {
		this.analyticsApp = analyticsApp;
	}

	public void run() {
		this.analyticsApp.run();
	}

}
