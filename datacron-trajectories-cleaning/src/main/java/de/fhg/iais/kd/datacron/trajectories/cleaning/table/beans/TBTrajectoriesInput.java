package de.fhg.iais.kd.datacron.trajectories.cleaning.table.beans;

import java.io.Serializable;
import java.util.Map;

/**
 * @author kthellmann
 *
 */
public class TBTrajectoriesInput implements Serializable {

	private static final long serialVersionUID = -5129749027141884796L;

	private String id;

	private String id_c;
	
	private String d;
	
	private String x;
	
	private String y;
	
	private Map<String, String> additional;
	
	public TBTrajectoriesInput(){		
	}
	
	public TBTrajectoriesInput(//
			String id, //
			String id_c, //
			String d, //
			String x, //
			String y,//
			Map<String, String> additional){
		
		this.id = id;
		this.id_c = id_c;
		this.d = d;
		this.x = x;
		this.y = y;
		this.additional=additional;
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

	public String getD() {
		return d;
	}

	public void setD(String date) {
		this.d = date;
	}

	public String getX() {
		return x;
	}

	public void setX(String x) {
		this.x = x;
	}

	public String getY() {
		return y;
	}

	public void setY(String y) {
		this.y = y;
	}

	public Map<String, String> getAdditional() {
		return additional;
	}

	public void setAdditional(Map<String, String> additional) {
		this.additional = additional;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("TBTrajectoriesParametrizedInput [id=");
		builder.append(id);
		builder.append(", id_c=");
		builder.append(id_c);
		builder.append(", d=");
		builder.append(d);
		builder.append(", x=");
		builder.append(x);
		builder.append(", y=");
		builder.append(y);
		builder.append(", additional=");
		builder.append(additional);
		builder.append("]");
		return builder.toString();
	}

}
