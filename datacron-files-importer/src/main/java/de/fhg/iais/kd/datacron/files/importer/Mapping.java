package de.fhg.iais.kd.datacron.files.importer;

import java.io.Serializable;
import java.util.List;

import com.google.inject.Inject;
import com.google.inject.name.Named;

public class Mapping implements Serializable {

	private static final long serialVersionUID = 3547577491214967483L;

	private final List<String> id;
	private final String date;
	private final String datepattern;
	private final String x;
	private final String y;
	private final List<String> other;
	private final boolean physicaltime;
	private final boolean geocoordinates;

	@Inject
	public Mapping(//
			@Named("spark.app.input.idList") List<String> id, //
			@Named("spark.app.input.date") String date, //
			@Named("spark.app.input.date.pattern") String datepattern, //
			@Named("spark.app.input.x") String x, //
			@Named("spark.app.input.y") String y, //
			@Named("spark.app.trajectories.additionalparamslist") List<String> other, //
			@Named("spark.app.input.physicaltime") boolean physicaltime, //
			@Named("spark.app.input.geocoordinates") boolean geocoordinates) {
		
		this.id = id;
		this.date = date;
		this.datepattern = datepattern;
		this.x = x;
		this.y = y;
		this.other = other;
		this.physicaltime = physicaltime;
		this.geocoordinates = geocoordinates;
	}

	public List<String> getId() {
		return id;
	}

	public String getDate() {
		return date;
	}
	
	public String getDatepattern() {
		return datepattern;
	}

	public String getX() {
		return x;
	}

	public String getY() {
		return y;
	}

	public List<String> getOther() {
		return other;
	}

	public boolean isPhysicaltime() {
		return physicaltime;
	}

	public boolean isGeocoordinates() {
		return geocoordinates;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("Mapping ");
		builder.append(", id=");
		builder.append(id);
		builder.append(", date=");
		builder.append(date);
		builder.append(", x=");
		builder.append(x);
		builder.append(", y=");
		builder.append(y);
		builder.append(", other=");
		builder.append(other);
		builder.append(", physicaltime=");
		builder.append(physicaltime);
		builder.append(", geocoordinates=");
		builder.append(geocoordinates);
		builder.append("]");
		return builder.toString();
	}

}
