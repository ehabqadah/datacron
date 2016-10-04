package de.fhg.iais.kd.datacron.trajectories.cleaning.table.beans;

import java.io.Serializable;
import java.util.Map;

/**
 * @author kthellmann
 *
 */
public class TBOutlierTrajectory implements Serializable {

	private static final long serialVersionUID = 8876557468617500355L;

	private String id;

	private long id_c;

	private String date1;

	private double x1;

	private double y1;

	private double x2;

	private double y2;

	private double speed;

	private Map<String, Double> additional;

	private boolean outlier;

	public TBOutlierTrajectory() {
	}

	public TBOutlierTrajectory(//
			String id, //
			long id_c, //
			String date1, //
			double x1, //
			double y1, //
			double x2, //
			double y2, //
			double speed, //
			Map<String, Double> additional, //
			boolean outlier) {

		this.id = id;
		this.id_c = id_c;
		this.date1 = date1;
		this.x1 = x1;
		this.y1 = y1;
		this.x2 = x2;
		this.y2 = y2;
		this.speed = speed;
		this.additional = additional;
		this.outlier = outlier;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public long getId_c() {
		return id_c;
	}

	public void setId_c(long id_c) {
		this.id_c = id_c;
	}

	public String getDate1() {
		return date1;
	}

	public void setDate1(String date1) {
		this.date1 = date1;
	}

	public double getX1() {
		return x1;
	}

	public void setX1(double x1) {
		this.x1 = x1;
	}

	public double getY1() {
		return y1;
	}

	public void setY1(double y1) {
		this.y1 = y1;
	}

	public double getX2() {
		return x2;
	}

	public void setX2(double x2) {
		this.x2 = x2;
	}

	public double getY2() {
		return y2;
	}

	public void setY2(double y2) {
		this.y2 = y2;
	}

	public double getSpeed() {
		return speed;
	}

	public void setSpeed(double speed) {
		this.speed = speed;
	}

	public Map<String, Double> getAdditional() {
		return additional;
	}

	public void setAdditional(Map<String, Double> additional) {
		this.additional = additional;
	}

	public boolean isOutlier() {
		return outlier;
	}

	public void setOutlier(boolean outlier) {
		this.outlier = outlier;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("TBOutlierTrajectory [id=");
		builder.append(id);
		builder.append(", id_c=");
		builder.append(id_c);
		builder.append(", date1=");
		builder.append(date1);
		builder.append(", x1=");
		builder.append(x1);
		builder.append(", y1=");
		builder.append(y1);
		builder.append(", x2=");
		builder.append(x2);
		builder.append(", y2=");
		builder.append(y2);
		builder.append(", additional=");
		builder.append(additional);
		builder.append(", outlier=");
		builder.append(outlier);
		builder.append("]");
		return builder.toString();
	}

}
