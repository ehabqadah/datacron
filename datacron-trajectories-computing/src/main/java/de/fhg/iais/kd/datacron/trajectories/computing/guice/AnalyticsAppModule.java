package de.fhg.iais.kd.datacron.trajectories.computing.guice;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import com.google.common.collect.ImmutableList;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import com.google.inject.name.Names;

import de.fhg.iais.kd.datacron.trajectories.computing.clp.CLParameter;
import de.fhg.iais.kd.datacron.trajectories.computing.impl.ISparkApp;
import de.fhg.iais.kd.datacron.trajectories.computing.impl.SparkApp;

/**
 * @author kthellmann
 *
 */
public class AnalyticsAppModule extends AbstractModule {
	
	private final CLParameter clparameter;

	/**
	 * @param args
	 */
	public AnalyticsAppModule(String[] args) {
		this.clparameter = new CLParameter();
		final JCommander cmd = new JCommander(this.clparameter);

		try {
			cmd.parse(args);
		} catch (final ParameterException ex) {
			cmd.usage();
			throw new RuntimeException(ex);
		}
	}

	/* (non-Javadoc)
	 * @see com.google.inject.AbstractModule#configure()
	 */
	@Override
	protected void configure() {
		this.bindConfigurations(this.clparameter.getConfigurations());
		this.bindConfigurations(this.clparameter.getMetadata());
		
		this.binder().bind(ISparkApp.class).to(SparkApp.class);
	}

	/**
	 * @param configurations
	 */
	private void bindConfigurations(String configurations) {
		final Properties properties = new Properties();

		try {
			properties.load(new FileReader(configurations));
			Names.bindProperties(this.binder(), properties);					
		} catch (final FileNotFoundException e) {
			System.out.println("Configuration file not found.");
		} catch (final IOException e) {
			System.out.println("Can't read configuration file.");
		}
	}

	/**
	 * @param sparkMaster
	 * @param sparkExecutorMemory
	 * @param sparkExecutorCores
	 * @param sparkCoresMax
	 * @param sparkDriverPort
	 * @param sparkExecutorPort
	 * @param sparkBroadcastPort
	 * @param sparkFileserverPort
	 * @param sparkBlockManagerPort
	 * @param sparkReplClassServerPort
	 * @param sparkSerializer
	 * @param sparkCassandraConnectionHost
	 * @param sparkCassandraConsistencyLvl
	 * @return
	 */
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
			@Named("spark.serializer") final String sparkSerializer, //
			@Named("spark.cassandra.connection.host") final String sparkCassandraConnectionHost, //
			@Named("spark.cassandra.output.consistency.level") final String sparkCassandraConsistencyLvl) {

		final SparkConf sparkConfig = new SparkConf();

		sparkConfig.setAppName("Datacron Analytics App");

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
		sparkConfig.set("spark.cassandra.output.consistency.level", sparkCassandraConsistencyLvl);

		sparkConfig.set("spark.serializer", sparkSerializer);

		sparkConfig.set("spark.kryo.classesToRegister", ClassRegister.TABLE_BEANS_REGISTER);
		sparkConfig.set("spark.kryo.registrator", ImmutableRegistrator.class.getCanonicalName());

		return sparkConfig;

	}

	/**
	 * @param sparkConfig
	 * @return
	 */
	@Provides
	@Singleton
	public JavaSparkContext provideJavaSparkContext(SparkConf sparkConfig) {
		final JavaSparkContext sparkContext = new JavaSparkContext(sparkConfig);

		return sparkContext;
	}

	/**
	 * @param absoluteParamsString
	 * @return
	 */
	@Provides
	@Named("spark.app.trajectories.absoluteparamslist")
	public List<String> provideAbsoluteParamsList(@Named("spark.app.trajectories.absoluteparams") String absoluteParamsString) {
		if(absoluteParamsString.isEmpty()) {
			return Collections.emptyList();
		}
		
		String[] absoluteParamsArray = absoluteParamsString.split(",");
		return ImmutableList.copyOf(absoluteParamsArray);
	}

	/**
	 * @param relativeParamsString
	 * @return
	 */
	@Provides
	@Named("spark.app.trajectories.relativeparamslist")
	public List<String> provideRelativeParamsList(@Named("spark.app.trajectories.relativeparams") String relativeParamsString) {
		if(relativeParamsString.isEmpty()) {
			return Collections.emptyList();
		}
		
		String[] relativeParamsArray = relativeParamsString.split(",");
		return ImmutableList.copyOf(relativeParamsArray);
	}

}
