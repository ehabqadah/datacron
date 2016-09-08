package de.fhg.iais.kd.datacron.trajectories.cleaning.table.beans;

import java.io.Serializable;

/**
 * @author kthellmann
 *
 */
public class TBStatistics implements Serializable {

	private static final long serialVersionUID = 8465481286956820078L;

	private String id;

	private int id_c;

	private int nr_points;

	private double min_speed;

	private double max_speed;

	private double avg_speed;

	private double median_speed;

	private double min_acceleration;

	private double max_acceleration;

	private double avg_acceleration;

	private double median_acceleration;

	private double min_difftime;

	private double max_difftime;

	private double avg_difftime;

	private double median_difftime;

	private double min_date;

	private double max_date;

	private double min_X;

	private double max_X;

	private double min_Y;

	private double max_Y;

	public TBStatistics() {
	}

	public TBStatistics(//
			String id,//
			int id_c, //
			int nr_points, //
			double min_speed, //
			double max_speed, //
			double avg_speed, //
			double median_speed, //
			double min_acceleration, //
			double max_acceleration, //
			double avg_acceleration, //
			double median_acceleration, //
			double min_difftime, //
			double max_difftime, //			
			double avg_difftime, //
			double median_difftime, //
			double min_date, //
			double max_date, //
			double min_X, //
			double max_X, //			
			double min_Y, //
			double max_Y) {
		
		super();
		
		this.id = id;
		this.id_c = id_c;
		this.nr_points = nr_points;
		this.min_speed = min_speed;
		this.max_speed = max_speed;
		this.avg_speed = avg_speed;
		this.median_speed = median_speed;
		this.min_acceleration = min_acceleration;
		this.max_acceleration = max_acceleration;
		this.avg_acceleration = avg_acceleration;
		this.median_acceleration = median_acceleration;
		this.min_difftime = min_difftime;
		this.max_difftime = max_difftime;
		this.avg_difftime = avg_difftime;
		this.median_difftime = median_difftime;
		this.min_date = min_date;
		this.max_date = max_date;
		this.min_X = min_X;
		this.max_X = max_X;
		this.min_Y = min_Y;
		this.max_Y = max_Y;
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

	public int getNr_points() {
		return nr_points;
	}

	public void setNr_points(int nr_points) {
		this.nr_points = nr_points;
	}

	public double getMin_speed() {
		return min_speed;
	}

	public void setMin_speed(double min_speed) {
		this.min_speed = min_speed;
	}

	public double getMax_speed() {
		return max_speed;
	}

	public void setMax_speed(double max_speed) {
		this.max_speed = max_speed;
	}

	public double getAvg_speed() {
		return avg_speed;
	}

	public void setAvg_speed(double avg_speed) {
		this.avg_speed = avg_speed;
	}

	public double getMedian_speed() {
		return median_speed;
	}

	public void setMedian_speed(double median_speed) {
		this.median_speed = median_speed;
	}

	public double getMin_acceleration() {
		return min_acceleration;
	}

	public void setMin_acceleration(double min_acceleration) {
		this.min_acceleration = min_acceleration;
	}

	public double getMax_acceleration() {
		return max_acceleration;
	}

	public void setMax_acceleration(double max_acceleration) {
		this.max_acceleration = max_acceleration;
	}

	public double getAvg_acceleration() {
		return avg_acceleration;
	}

	public void setAvg_acceleration(double avg_acceleration) {
		this.avg_acceleration = avg_acceleration;
	}

	public double getMedian_acceleration() {
		return median_acceleration;
	}

	public void setMedian_acceleration(double median_acceleration) {
		this.median_acceleration = median_acceleration;
	}

	public double getMin_difftime() {
		return min_difftime;
	}

	public void setMin_difftime(double min_difftime) {
		this.min_difftime = min_difftime;
	}

	public double getMax_difftime() {
		return max_difftime;
	}

	public void setMax_difftime(double max_difftime) {
		this.max_difftime = max_difftime;
	}

	public double getAvg_difftime() {
		return avg_difftime;
	}

	public void setAvg_difftime(double avg_difftime) {
		this.avg_difftime = avg_difftime;
	}

	public double getMedian_difftime() {
		return median_difftime;
	}

	public void setMedian_difftime(double median_difftime) {
		this.median_difftime = median_difftime;
	}

	public double getMin_date() {
		return min_date;
	}

	public void setMin_date(double min_date) {
		this.min_date = min_date;
	}

	public double getMax_date() {
		return max_date;
	}

	public void setMax_date(double max_date) {
		this.max_date = max_date;
	}

	public double getMin_X() {
		return min_X;
	}

	public void setMin_X(double min_X) {
		this.min_X = min_X;
	}

	public double getMax_X() {
		return max_X;
	}

	public void setMax_X(double max_X) {
		this.max_X = max_X;
	}

	public double getMin_Y() {
		return min_Y;
	}

	public void setMin_Y(double min_Y) {
		this.min_Y = min_Y;
	}

	public double getMax_Y() {
		return max_Y;
	}

	public void setMax_Y(double max_Y) {
		this.max_Y = max_Y;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("TBTrajectoriesStatisticsOutput [id=");
		builder.append(id);
		builder.append(", id_c=");
		builder.append(id_c);
		builder.append(", nr_points=");
		builder.append(nr_points);
		builder.append(", min_speed=");
		builder.append(min_speed);
		builder.append(", max_speed=");
		builder.append(max_speed);
		builder.append(", avg_speed=");
		builder.append(avg_speed);
		builder.append(", median_speed=");
		builder.append(median_speed);
		builder.append(", min_acceleration=");
		builder.append(min_acceleration);
		builder.append(", max_acceleration=");
		builder.append(max_acceleration);
		builder.append(", avg_acceleration=");
		builder.append(avg_acceleration);
		builder.append(", median_acceleration=");
		builder.append(median_acceleration);
		builder.append(", min_difftime=");
		builder.append(min_difftime);
		builder.append(", max_difftime=");
		builder.append(max_difftime);
		builder.append(", avg_difftime=");
		builder.append(avg_difftime);
		builder.append(", median_difftime=");
		builder.append(median_difftime);
		builder.append(", min_date=");
		builder.append(min_date);
		builder.append(", max_date=");
		builder.append(max_date);
		builder.append(", min_X=");
		builder.append(min_X);
		builder.append(", max_X=");
		builder.append(max_X);
		builder.append(", min_Y=");
		builder.append(min_Y);
		builder.append(", max_Y=");
		builder.append(max_Y);
		builder.append("]");
		return builder.toString();
	}

}
