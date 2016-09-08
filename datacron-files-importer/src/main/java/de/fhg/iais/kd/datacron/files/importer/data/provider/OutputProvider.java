package de.fhg.iais.kd.datacron.files.importer.data.provider;

import org.apache.spark.api.java.JavaSparkContext;

import com.google.inject.Inject;

import de.fhg.iais.cassandra.AbstractDataProvider;
import de.fhg.iais.cassandra.Cassandra;
import de.fhg.iais.kd.datacron.files.importer.table.beans.TBOutput;
import de.fhg.iais.kd.datacron.files.importer.table.metadata.TBMOutput;

public class OutputProvider extends AbstractDataProvider<TBOutput> {

	private static final long serialVersionUID = -5556975391499283873L;

	@Inject
	public OutputProvider( //
			TBMOutput tableMetadata, //
			JavaSparkContext sparkContext, //
			Cassandra cassandra) {

		super(tableMetadata, sparkContext, cassandra, TBOutput.class);
	}
	
}
