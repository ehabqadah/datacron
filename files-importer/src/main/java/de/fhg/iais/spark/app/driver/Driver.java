package de.fhg.iais.spark.app.driver;

import java.io.Serializable;

import com.google.inject.Inject;

import de.fhg.iais.spark.app.impl.ImportItemsApp;

/**
 * @author kthellmann
 *
 */
public class Driver implements Serializable {

	private static final long serialVersionUID = 6019283703351410219L;

	private ImportItemsApp sparkApp;

	@Inject
	public Driver(ImportItemsApp sparkApp) {
		this.sparkApp = sparkApp;
	}

	public void run() {		
		this.sparkApp.run();
	
	}	

}
