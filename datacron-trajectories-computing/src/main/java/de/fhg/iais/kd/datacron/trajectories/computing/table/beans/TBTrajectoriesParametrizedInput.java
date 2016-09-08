package de.fhg.iais.kd.datacron.trajectories.computing.table.beans;

import java.io.Serializable;
import java.util.Map;

/**
 * @author kthellmann
 *
 */
public class TBTrajectoriesParametrizedInput implements Serializable {

	private static final long serialVersionUID = 8465481286956820078L;
	
	private String id;

	private String id_c;
	
	private long d;
	
	private double x;
	
	private double y;
	
	private Map<String, Double> abs_prop;
	
	private Map<String, Double> rel_prop;
	
	public TBTrajectoriesParametrizedInput(){		
	}
	
	public TBTrajectoriesParametrizedInput(//
			String id, //
			String id_c, //
			long d, //
			double x, //
			double y,//
			Map<String, Double> abs_prop, //
			Map<String, Double> rel_prop){
		
		this.id = id;
		this.id_c = id_c;
		this.d = d;
		this.x = x;
		this.y = y;
		this.abs_prop=abs_prop;
		this.rel_prop=rel_prop;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getId_c() {
		return id_c;
	}

	public void setId_c(String id_c) {
		this.id_c = id_c;
	}

	public long getD() {
		return d;
	}

	public void setD(long date) {
		this.d = date;
	}

	public double getX() {
		return x;
	}

	public void setX(double x) {
		this.x = x;
	}

	public double getY() {
		return y;
	}

	public void setY(double y) {
		this.y = y;
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
		builder.append("TBInput [id=");
		builder.append(id);
		builder.append(", id_c=");
		builder.append(id_c);
		builder.append(", d=");
		builder.append(d);
		builder.append(", x=");
		builder.append(x);
		builder.append(", y=");
		builder.append(y);
		builder.append(", abs_prop=");
		builder.append(abs_prop);
		builder.append(", rel_prop=");
		builder.append(rel_prop);
		builder.append("]");
		return builder.toString();
	}

}
