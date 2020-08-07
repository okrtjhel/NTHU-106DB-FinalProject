package org.vanilladb.bench.server.param.micro;

import java.util.ArrayList;

import org.vanilladb.core.remote.storedprocedure.SpResultSet;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.Type;
import org.vanilladb.core.sql.VarcharConstant;
import org.vanilladb.core.sql.storedprocedure.SpResultRecord;
import org.vanilladb.core.sql.storedprocedure.StoredProcedureParamHelper;

public class TestbedLoaderParamHelper extends StoredProcedureParamHelper {

//	private static final String TABLES_DDL[] = {
//			"CREATE TABLE item ( i_id INT, i_im_id INT, i_name VARCHAR(24), "
//					+ "i_price DOUBLE, i_data VARCHAR(50) )"
//			};
//	private static final String INDEXES_DDL[] = {
//			"CREATE INDEX idx_item ON item (i_id)"};
	
	private int numOfItems = 0;

	public String[] getTableSchemas() {
		ArrayList<String> tbl_ddl = new ArrayList<String>();
		for(int i=0; i<10; ++i) {
			tbl_ddl.add("CREATE TABLE item"+i+" ( i_id INT, i_im_id INT, i_name VARCHAR(24), "
					+ "i_price DOUBLE, i_data VARCHAR(50) )");
		}
		return tbl_ddl.toArray(new String[0]);
	}

	public String[] getIndexSchemas() {
		ArrayList<String> idx_ddl = new ArrayList<String>();
		for(int i=0; i<10; ++i) {
			idx_ddl.add("CREATE INDEX idx_item"+i+" ON item"+i+" (i_id)");
		}
		return idx_ddl.toArray(new String[0]);
	}
	
	public int getNumberOfItems() {
		return numOfItems;
	}

	@Override
	public void prepareParameters(Object... pars) {
		numOfItems = (Integer) pars[0];
	}

	@Override
	public SpResultSet createResultSet() {
		// create schema
		Schema sch = new Schema();
		Type statusType = Type.VARCHAR(10);
		sch.addField("status", statusType);

		// create record
		SpResultRecord rec = new SpResultRecord();
		String status = isCommitted ? "committed" : "abort";
		rec.setVal("status", new VarcharConstant(status, statusType));

		// create result set
		return new SpResultSet(sch, rec);
	}

}
