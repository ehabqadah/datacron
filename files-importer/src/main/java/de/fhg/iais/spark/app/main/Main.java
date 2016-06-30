package de.fhg.iais.spark.app.main;

import com.google.inject.Guice;
import com.google.inject.Injector;

import de.fhg.iais.spark.app.driver.Driver;
import de.fhg.iais.spark.app.guice.Module;

/**
 * @author kthellmann
 *
 */
public class Main {

	public static void main(String... args) {		
		Module module = new Module(args);
		Injector injector = Guice.createInjector(module);
		Driver tableAppDriver = injector.getInstance(Driver.class);
		tableAppDriver.run();
	}
}