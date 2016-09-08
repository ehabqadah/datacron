package de.fhg.iais.kd.datacron.files.importer.impl;

import com.google.inject.Inject;

import de.fhg.iais.kd.datacron.files.importer.Importer;


/**
 * @author kthellmann
 *
 */
public class SparkApp implements ISparkApp{

	private static final long serialVersionUID = -2456126582786815585L;

	private transient final Importer importer;
	
	@Inject
	public SparkApp(Importer importer) {
		
		this.importer = importer;
		
	}

	public void run() {
		importer.importData();
	}

}
