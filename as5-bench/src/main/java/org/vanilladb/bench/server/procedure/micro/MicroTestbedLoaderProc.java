package org.vanilladb.bench.server.procedure.micro;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.vanilladb.bench.benchmarks.micro.MicrobenchConstants;
import org.vanilladb.bench.server.param.micro.TestbedLoaderParamHelper;
import org.vanilladb.bench.server.procedure.BasicStoredProcedure;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.storage.tx.recovery.CheckpointTask;
import org.vanilladb.core.storage.tx.recovery.RecoveryMgr;

public class MicroTestbedLoaderProc extends BasicStoredProcedure<TestbedLoaderParamHelper> {
	private static Logger logger = Logger.getLogger(MicroTestbedLoaderProc.class.getName());

	public MicroTestbedLoaderProc() {
		super(new TestbedLoaderParamHelper());
	}

	@Override
	protected void executeSql() {
		if (logger.isLoggable(Level.INFO))
			logger.info("Start loading testbed...");

		// turn off logging set value to speed up loading process
		// TODO: remove this hack code in the future
		RecoveryMgr.enableLogging(false);
		
		dropOldData();
		createSchemas();

		// Generate item records
		generateItems(1, paramHelper.getNumberOfItems());

		if (logger.isLoggable(Level.INFO))
			logger.info("Loading completed. Flush all loading data to disks...");

		// TODO: remove this hack code in the future
		RecoveryMgr.enableLogging(true);

		// Create a checkpoint
		CheckpointTask cpt = new CheckpointTask();
		cpt.createCheckpoint();

		// Delete the log file and create a new one
		VanillaDb.logMgr().removeAndCreateNewLog();

		if (logger.isLoggable(Level.INFO))
			logger.info("Loading procedure finished.");

	}
	
	private void dropOldData() {
		// TODO: Implement this
		if (logger.isLoggable(Level.WARNING))
			logger.warning("Dropping is skipped.");
	}
	
	private void createSchemas() {
		if (logger.isLoggable(Level.FINE))
			logger.info("Create tables...");
		
		for (String cmd : paramHelper.getTableSchemas())
			executeUpdate(cmd);
		
		if (logger.isLoggable(Level.FINE))
			logger.info("Create indexes...");

		for (String cmd : paramHelper.getIndexSchemas())
			executeUpdate(cmd);
		
		if (logger.isLoggable(Level.FINE))
			logger.info("Finish creating schemas.");
	}

	private void generateItems(int startIId, int endIId) {
		if (logger.isLoggable(Level.FINE))
			logger.info("Start populating items from i_id=" + startIId + " to i_id=" + endIId);

		int iid, iimid;
		String iname, idata;
		double iprice;
		String sql;
		for (int i = startIId; i <= endIId; i++) {
			iid = i;

			// Deterministic value generation by item id
			iimid = iid % (MicrobenchConstants.MAX_IM - MicrobenchConstants.MIN_IM) + MicrobenchConstants.MIN_IM;
			iname = String.format("%0" + MicrobenchConstants.MIN_I_NAME + "d", iid);
			iprice = (iid % (int) (MicrobenchConstants.MAX_PRICE - MicrobenchConstants.MIN_PRICE)) + MicrobenchConstants.MIN_PRICE;
			idata = String.format("%0" + MicrobenchConstants.MIN_I_DATA + "d", iid);

			for(int j=0; j<10; ++j) {
				sql = "INSERT INTO item"+j+"(i_id, i_im_id, i_name, i_price, i_data) VALUES (" + iid + ", " + iimid + ", '"
						+ iname + "', " + iprice + ", '" + idata + "' )";
				executeUpdate(sql);
			}
		}

		if (logger.isLoggable(Level.FINE))
			logger.info("Populating items completed.");
	}
}
