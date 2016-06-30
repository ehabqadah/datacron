package de.fhg.iais.spark.app.guice;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Named;
import com.google.inject.name.Names;

import de.fhg.iais.spark.app.clp.CLP;
import de.fhg.iais.spark.app.impl.ISparkApp;
import de.fhg.iais.spark.app.impl.ImportItemsApp;


/**
 * @author kthellmann
 *
 */
public class Module extends AbstractModule  {

	private final String[] args;

	public Module(String[] args) {
		this.args = args;
	}

	@Override
	protected void configure() {
		String configurations = null;
		String input = null;

		CLP clp = new CLP();
		JCommander cmd = new JCommander(clp);

		try {
			cmd.parse(args);
			input = clp.getInput();
			configurations = clp.getConfig();

		} catch (ParameterException ex) {
			System.err.println(ex.getMessage());
			cmd.usage();
		}

		binder().bind(ISparkApp.class).to(ImportItemsApp.class);

		bindSparkConfigurations(configurations);

		
		bind(new TypeLiteral<String>() {
		}).annotatedWith(Names.named("spark.importer.data")).toInstance(input);
		
		
	}
	
	private void bindSparkConfigurations(String configurations) {
		Properties properties = new Properties();

		try {
			properties.load(new FileReader(configurations));
			Names.bindProperties(binder(), properties);
		} catch (FileNotFoundException e) {
			System.out.println("Configuration file not found.");
		} catch (IOException e) {
			System.out.println("Can't read configuration file.");
		}
	}
	
	@Provides
	@Singleton
	public SparkConf provideSparkConf(//
			@Named("spark.master") final String sparkMaster, //
			@Named("spark.executor.memory") final String sparkExecutorMemory, //
			@Named("spark.executor.cores") final String sparkExecutorCores, //
			@Named("spark.cores.max") final String sparkCoresMax, //
			@Named("spark.driver.port") final String sparkDriverPort, //
			@Named("spark.executor.port") final String sparkExecutorPort, //
			@Named("spark.broadcast.port") final String sparkBroadcastPort, //
			@Named("spark.fileserver.port") final String sparkFileserverPort, //
			@Named("spark.blockManager.port") final String sparkBlockManagerPort, //
			@Named("spark.replClassServer.port") final String sparkReplClassServerPort, //
			@Named("spark.cassandra.connection.host") final String sparkCassandraConnectionHost) {

		SparkConf sparkConfig = new SparkConf();

		sparkConfig.setAppName("File Importer");

		sparkConfig.setMaster(sparkMaster);

		sparkConfig.set("spark.cores.max", sparkCoresMax);
		sparkConfig.set("spark.executor.memory", sparkExecutorMemory);
		sparkConfig.set("spark.executor.cores", sparkExecutorCores);

		sparkConfig.set("spark.driver.port", sparkDriverPort);
		sparkConfig.set("spark.executor.port", sparkExecutorPort);
		sparkConfig.set("spark.broadcast.port", sparkBroadcastPort);
		sparkConfig.set("spark.fileserver.port", sparkFileserverPort);
		sparkConfig.set("spark.blockManager.port", sparkBlockManagerPort);
		sparkConfig.set("spark.replClassServer.port", sparkReplClassServerPort);

		sparkConfig.set("spark.cassandra.connection.host", sparkCassandraConnectionHost);

		return sparkConfig;
	}

	@Provides
	@Singleton
	public JavaSparkContext provideJavaSparkContext(SparkConf sparkConfig) {
		JavaSparkContext sparkContext = new JavaSparkContext(sparkConfig);

		return sparkContext;
	}

}
