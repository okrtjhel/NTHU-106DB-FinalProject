package org.vanilladb.bench.server.procedure.micro;

import org.vanilladb.bench.server.param.micro.MicroTxnProcParamHelper;
import org.vanilladb.bench.server.procedure.BasicStoredProcedure;
import org.vanilladb.core.query.algebra.Scan;
import java.util.Random;

public class MicroTxnProc extends BasicStoredProcedure<MicroTxnProcParamHelper> {

	public MicroTxnProc() {
		super(new MicroTxnProcParamHelper());
	}

	@Override
	protected void executeSql() {
		// SELECT
		Random ran = new Random();
		for (int idx = 0; idx < paramHelper.getReadCount(); idx++) {
			int iid = paramHelper.getReadItemId(idx);
			int r = ran.nextInt(10);
			Scan s = executeQuery("SELECT i_name, i_price FROM item"+r+" WHERE i_id = " + iid);
			s.beforeFirst();
			if (s.next()) {
				String name = (String) s.getVal("i_name").asJavaVal();
				double price = (Double) s.getVal("i_price").asJavaVal();

				paramHelper.setItemName(name, idx);
				paramHelper.setItemPrice(price, idx);
			} else
				throw new RuntimeException("Cloud not find item record with i_id = " + iid);

			s.close();
		}
		
		// UPDATE
		for (int idx = 0; idx < paramHelper.getWriteCount(); idx++) {
			int iid = paramHelper.getWriteItemId(idx);
			double newPrice = paramHelper.getNewItemPrice(idx);
			int r = ran.nextInt(10);
			executeUpdate("UPDATE item"+r+" SET i_price = " + newPrice + " WHERE i_id =" + iid);
			
		}
	}
}
