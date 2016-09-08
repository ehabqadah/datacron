package de.fhg.iais.kd.datacron.trajectories.cleaning.main;

import com.google.inject.Guice;
import com.google.inject.Injector;

import de.fhg.iais.kd.datacron.trajectories.cleaning.driver.Driver;
import de.fhg.iais.kd.datacron.trajectories.cleaning.guice.AnalyticsAppModule;

/**
 * @author kthellmann
 *
 */
public class Main {

	public static void main(String[] args) {
		try {
			
		AnalyticsAppModule module = new AnalyticsAppModule(args);
		Injector injector = Guice.createInjector(module);
		Driver tableAppDriver = injector.getInstance(Driver.class);
		tableAppDriver.run();
		
		} catch(Exception e) {			
			e.printStackTrace();
		}
	}

}
