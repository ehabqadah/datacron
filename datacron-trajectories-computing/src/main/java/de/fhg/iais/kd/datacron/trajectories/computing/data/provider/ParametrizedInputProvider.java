package de.fhg.iais.kd.datacron.trajectories.computing.data.provider;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.joda.time.DateTime;

import com.google.inject.Inject;
import com.google.inject.name.Named;

import de.fhg.iais.cassandra.AbstractDataProvider;
import de.fhg.iais.cassandra.Cassandra;
import de.fhg.iais.kd.datacron.trajectories.computing.table.beans.TBTrajectoriesInput;
import de.fhg.iais.kd.datacron.trajectories.computing.table.beans.TBTrajectoriesParametrizedInput;
import de.fhg.iais.kd.datacron.trajectories.computing.table.metadata.TBMTrajectoriesInput;

/**
 * @author kthellmann
 *
 */
public class ParametrizedInputProvider extends AbstractDataProvider<TBTrajectoriesInput> {

	private static final long serialVersionUID = 5347109640871313604L;

	@Inject
	@Named("spark.app.trajectories.inputdata")
	private String inputData;

	@Inject
	@Named("spark.app.trajectories.physicaltime")
	private boolean timeIsPhysical;

	@Inject
	@Named("spark.app.trajectories.geocoordinates")
	private boolean coordinatesAreGeo;

	@Inject
	@Named("spark.app.trajectories.absoluteparamslist")
	private List<String> absoluteParamsList;

	@Inject
	@Named("spark.app.trajectories.relativeparamslist")
	private List<String> relativeParamsList;

	@Inject
	public ParametrizedInputProvider( //
			TBMTrajectoriesInput tableMetadata, //
			JavaSparkContext sparkContext, //
			Cassandra cassandra) {

		super(tableMetadata, sparkContext, cassandra, TBTrajectoriesInput.class);
	}

	/**
	 * Create input RDD for trajectories computation
	 * 
	 * @return trajectories input RDD
	 */
	public JavaRDD<TBTrajectoriesParametrizedInput> createInputBean() {
	
		return this.readInputTableRows(inputData).map((TBTrajectoriesInput input) -> {

			TBTrajectoriesParametrizedInput parmetrizedInput = new TBTrajectoriesParametrizedInput();
			parmetrizedInput.setId(input.getId());
			
			if (timeIsPhysical) {
				DateTime dateTime = DateTime.parse(input.getD());
				parmetrizedInput.setD(dateTime.getMillis());
			} else {
				parmetrizedInput.setD(Long.valueOf(input.getD()));
			}

			parmetrizedInput.setX(Double.valueOf(input.getX()));
			parmetrizedInput.setY(Double.valueOf(input.getY()));

			Map<String, Double> absoluteParamsMap = new HashMap<String, Double>();

			Map<String, String> additionalData = input.getAdditional();
			
			for (String abs_param : absoluteParamsList) {
				absoluteParamsMap.put(abs_param, Double.valueOf(additionalData.get(abs_param)));
			}
			parmetrizedInput.setAbs_prop(absoluteParamsMap);

			Map<String, Double> relativeParamsMap = new HashMap<String, Double>();
			
			for (String rel_param : relativeParamsList) {
				relativeParamsMap.put(rel_param, Double.valueOf(additionalData.get(rel_param)));
			}
			parmetrizedInput.setRel_prop(relativeParamsMap);

			return parmetrizedInput;
		});
	}

	public boolean isTimeIsPhysical() {
		return timeIsPhysical;
	}

	public boolean isCoordinatesAreGeo() {
		return coordinatesAreGeo;
	}

}
