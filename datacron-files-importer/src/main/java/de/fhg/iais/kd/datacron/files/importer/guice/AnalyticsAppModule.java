package de.fhg.iais.kd.datacron.files.importer.guice;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import com.google.common.collect.ImmutableList;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Named;
import com.google.inject.name.Names;

import de.fhg.iais.kd.datacron.files.importer.clp.CLParameter;
import de.fhg.iais.kd.datacron.files.importer.impl.ISparkApp;
import de.fhg.iais.kd.datacron.files.importer.impl.SparkApp;

/**
 * @author kthellmann
 *
 */
public class AnalyticsAppModule extends AbstractModule {
	private final CLParameter clparameter;

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

	@Override
	protected void configure() {
		this.bindConfigurations(this.clparameter.getConfigurations());
		this.bindConfigurations(this.clparameter.getMapping());
		
		for(String outlier: this.clparameter.getOutlier()){
			this.bindConfigurations(outlier);
		}
		
		String input =this.clparameter.getInput();
		
		bind(new TypeLiteral<String>() {
		}).annotatedWith(Names.named("spark.app.input.files")).toInstance(input);
		
		this.binder().bind(ISparkApp.class).to(SparkApp.class);
	}

	private void bindConfigurations(String configurations) {
		final Properties properties = new Properties();

		try {
			properties.load(new FileReader(configurations));
			Names.bindProperties(this.binder(), properties);
		} catch (final FileNotFoundException e) {
			System.err.println("Configuration file not found.");
		} catch (final IOException e) {
			System.err.println("Can't read configuration file.");
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
			@Named("spark.replClassServer.port") final String sparkReplClassServerPort,//
			@Named("spark.serializer") final String sparkSerializer, //
			@Named("spark.cassandra.connection.host") final String sparkCassandraConnectionHost, //
			@Named("spark.cassandra.output.consistency.level") final String sparkCassandraConsistencyLvl//
			) {

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

	@Provides
	@Singleton
	public JavaSparkContext provideJavaSparkContext(SparkConf sparkConfig) {
		final JavaSparkContext sparkContext = new JavaSparkContext(sparkConfig);

		return sparkContext;
	}

	@Provides
	@Named("spark.app.trajectories.additionalparamslist")
	public List<String> provideAdditionalParamsList(@Named("spark.app.input.other") String list) {
		if (list.isEmpty()) {
			return Collections.emptyList();
		}
		return ImmutableList.copyOf(list.split(","));
	}
	
	@Provides
	@Named("spark.app.input.idList")
	public List<String> provideIdList(@Named("spark.app.input.id") String list) {
		if (list.isEmpty()) {
			return Collections.emptyList();
		}
		return ImmutableList.copyOf(list.split(","));
	}

	@Provides
	@Named("spark.app.trajectories.pointoutlierlist")
	public List<String> providePointOutlierList(@Named("spark.app.input.outliers.points") String list) {
		if (list.isEmpty()) {
			return Collections.emptyList();
		}
		return ImmutableList.copyOf(list.split(","));
	}
	
	@Provides
	@Named("spark.app.trajectories.dateoutlierlist")
	public List<String> provideDateOutlierList(@Named("spark.app.input.outliers.dates") String list) {
		if (list.isEmpty()) {
			return Collections.emptyList();
		}
		return ImmutableList.copyOf(list.split(","));
	}
	
	@Provides
	@Named("spark.app.trajectories.attributeoutlierlist")
	public Map<String, List<String>> provideAttributeOutlierMap(@Named("spark.app.input.outliers.attributes") String list) {		
		if (list.isEmpty()) {
			return Collections.emptyMap();
		}
		
		String[] attributes = list.split(";");	
		Map<String, List<String>> outlierAttributeMap = new HashMap<String, List<String>>();
		
		for (int i = 0; i < attributes.length; i++) {
			String[] values = attributes[i].split(":");
			String attributeName = values[0];
			ImmutableList<String> outlierList = ImmutableList.copyOf(values[1].split(","));
			outlierAttributeMap.put(attributeName, outlierList);
		}
		
		return outlierAttributeMap;
	}
	
}
