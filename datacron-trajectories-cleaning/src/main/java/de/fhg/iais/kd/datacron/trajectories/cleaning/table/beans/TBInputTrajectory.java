package de.fhg.iais.kd.datacron.trajectories.cleaning.table.beans;

import java.io.Serializable;
import java.util.Map;

/**
 * @author kthellmann
 *
 */
public class TBInputTrajectory implements Serializable {

	private static final long serialVersionUID = -5129749027141884796L;

	private String id;

	private int id_c;

	private String date1;

	private String date2;

	private int difftime;

	private double x1;

	private double x2;

	private double diffx;

	private double y1;

	private double y2;

	private double diffy;

	private double distance;

	private double speed;

	private double course;

	private double acceleration;

	private double turn;

	private Map<String, Double> abs_prop;

	private Map<String, Double> rel_prop;

	public TBInputTrajectory() {
	}

	public TBInputTrajectory(//
			String id, //
			int id_c, //
			String date1, //
			String date2, //
			int difftime, //
			double x1, //
			double x2, //
			double diffx, //
			double y1, //
			double y2, //
			double diffy, //
			double distance, //
			double speed, //
			double course, //
			double acceleration, //
			double turn, //
			Map<String, Double> abs_prop, //
			Map<String, Double> rel_prop) {

		super();

		this.id = id;
		this.id_c = id_c;
		this.date1 = date1;
		this.date2 = date2;
		this.difftime = difftime;
		this.x1 = x1;
		this.x2 = x2;
		this.diffx = diffx;
		this.y1 = y1;
		this.y2 = y2;
		this.diffy = diffy;
		this.distance = distance;
		this.speed = speed;
		this.course = course;
		this.acceleration = acceleration;
		this.turn = turn;
		this.abs_prop = abs_prop;
		this.rel_prop = rel_prop;
	}
	
	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public int getId_c() {
		return id_c;
	}

	public void setId_c(int id_c) {
		this.id_c = id_c;
	}

	public String getDate1() {
		return date1;
	}

	public void setDate1(String date1) {
		this.date1 = date1;
	}

	public String getDate2() {
		return date2;
	}

	public void setDate2(String date2) {
		this.date2 = date2;
	}

	public int getDifftime() {
		return difftime;
	}

	public void setDifftime(int difftime) {
		this.difftime = difftime;
	}

	public double getX1() {
		return x1;
	}

	public void setX1(double x1) {
		this.x1 = x1;
	}

	public double getX2() {
		return x2;
	}

	public void setX2(double x2) {
		this.x2 = x2;
	}

	public double getDiffx() {
		return diffx;
	}

	public void setDiffx(double diffx) {
		this.diffx = diffx;
	}

	public double getY1() {
		return y1;
	}

	public void setY1(double y1) {
		this.y1 = y1;
	}

	public double getY2() {
		return y2;
	}

	public void setY2(double y2) {
		this.y2 = y2;
	}

	public double getDiffy() {
		return diffy;
	}

	public void setDiffy(double diffy) {
		this.diffy = diffy;
	}

	public double getDistance() {
		return distance;
	}

	public void setDistance(double distance) {
		this.distance = distance;
	}

	public double getSpeed() {
		return speed;
	}

	public void setSpeed(double speed) {
		this.speed = speed;
	}

	public double getCourse() {
		return course;
	}

	public void setCourse(double course) {
		this.course = course;
	}

	public double getAcceleration() {
		return acceleration;
	}

	public void setAcceleration(double acceleration) {
		this.acceleration = acceleration;
	}

	public double getTurn() {
		return turn;
	}

	public void setTurn(double turn) {
		this.turn = turn;
	}

	public Map<String, Double> getAbs_prop() {
		return abs_prop;
	}

	public void setAbs_prop(Map<String, Double> abs_prop) {
		this.abs_prop = abs_prop;
	}

	public Map<String, Double> getRel_prop() {
		return rel_prop;
	}

	public void setRel_prop(Map<String, Double> rel_prop) {
		this.rel_prop = rel_prop;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("TBTrajectories [id=");
		builder.append(id);
		builder.append(", id_c=");
		builder.append(id_c);
		builder.append(", date1=");
		builder.append(date1);
		builder.append(", date2=");
		builder.append(date2);
		builder.append(", difftime=");
		builder.append(difftime);
		builder.append(", x1=");
		builder.append(x1);
		builder.append(", x2=");
		builder.append(x2);
		builder.append(", diffx=");
		builder.append(diffx);
		builder.append(", y1=");
		builder.append(y1);
		builder.append(", y2=");
		builder.append(y2);
		builder.append(", diffy=");
		builder.append(diffy);
		builder.append(", distance=");
		builder.append(distance);
		builder.append(", speed=");
		builder.append(speed);
		builder.append(", course=");
		builder.append(course);
		builder.append(", acceleration=");
		builder.append(acceleration);
		builder.append(", turn=");
		builder.append(turn);
		builder.append(", abs_prop=");
		builder.append(abs_prop);
		builder.append(", rel_prop=");
		builder.append(rel_prop);
		builder.append("]");
		return builder.toString();
	}

}
