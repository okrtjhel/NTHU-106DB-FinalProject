package org.vanilladb.core.storage.tx;

import org.vanilladb.core.storage.record.RecordId;

public class RecordField {
	public String tblName;
	public RecordId rid;
	public String fldName;
	
	public RecordField(String tblName, RecordId rid, String fldName) {
		this.tblName = tblName;
		this.rid = rid;
		this.fldName = fldName;
	}
	
	@Override
	public int hashCode() {
		return tblName.hashCode() + rid.hashCode() + fldName.hashCode();
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj == this)
			return true;
		if (obj == null || !(obj.getClass().equals(RecordField.class)))
			return false;
		RecordField rf = (RecordField) obj;
		return tblName.equals(rf.tblName) && rid.equals(rf.rid) && fldName.equals(rf.fldName);
	}
	
	public void print() {
		System.out.println("Table Name: " + this.tblName + " RecordId: " + this.rid + " Field Name: " + this.fldName);
	}
}
