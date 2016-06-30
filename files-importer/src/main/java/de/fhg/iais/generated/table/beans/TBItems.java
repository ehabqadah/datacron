package de.fhg.iais.generated.table.beans;

import java.io.Serializable;
import java.nio.ByteBuffer;

import org.joda.time.DateTime;

public class TBItems implements Serializable {
	private static final long serialVersionUID = 7143759520722903519L;
	
	private String id;	
	private ByteBuffer content;
	private DateTime ingestdate;
	
	public TBItems() {
	}
	
	

	public TBItems(String id, ByteBuffer content, DateTime ingestdate) {
		super();
		this.id = id;
		this.content = content;
		this.ingestdate = ingestdate;
	}



	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public ByteBuffer getContent() {
		return content;
	}

	public void setContent(ByteBuffer content) {
		this.content = content;
	}

	public DateTime getIngestdate() {
		return ingestdate;
	}

	public void setIngestdate(DateTime ingestdate) {
		this.ingestdate = ingestdate;
	}

	
	
}
