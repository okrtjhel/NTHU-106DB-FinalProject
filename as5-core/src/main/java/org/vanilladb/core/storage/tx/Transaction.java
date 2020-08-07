/*******************************************************************************
 * Copyright 2017 vanilladb.org
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package org.vanilladb.core.storage.tx;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.storage.buffer.BufferMgr;
import org.vanilladb.core.storage.metadata.TableInfo;
import org.vanilladb.core.storage.record.RecordFile;
import org.vanilladb.core.storage.record.RecordId;
import org.vanilladb.core.storage.tx.concurrency.ConcurrencyMgr;
import org.vanilladb.core.storage.tx.recovery.RecoveryMgr;

/**
 * Provides transaction management for clients, ensuring that all transactions
 * are recoverable, and in general satisfy the ACID properties with specified
 * isolation level.
 */
public class Transaction {
	private static Logger logger = Logger.getLogger(Transaction.class.getName());

	private RecoveryMgr recoveryMgr;
	private ConcurrencyMgr concurMgr;
	private BufferMgr bufferMgr;
	private List<TransactionLifecycleListener> lifecycleListeners;
	private long txNum;
	private long startTn;
	private boolean readOnly;
	private HashMap<RecordField, Constant> writeSet;
	private HashSet<RecordField> readSet;
	private HashSet<RecordField> deleteSet;
	private boolean cCertified;
	private boolean rCertified;

	/**
	 * Creates a new transaction and associates it with a recovery manager, a
	 * concurrency manager, and a buffer manager. This constructor depends on
	 * the file, log, and buffer managers from {@link VanillaDb}, which are
	 * created during system initialization. Thus this constructor cannot be
	 * called until {@link VanillaDb#init(String)} is called first.
	 * 
	 * @param txMgr
	 *            the transaction manager
	 * @param concurMgr
	 *            the associated concurrency manager
	 * @param recoveryMgr
	 *            the associated recovery manager
	 * @param bufferMgr
	 *            the associated buffer manager
	 * @param readOnly
	 *            is read-only mode
	 * @param txNum
	 *            the number of the transaction
	 */
	public Transaction(TransactionMgr txMgr, TransactionLifecycleListener concurMgr,
			TransactionLifecycleListener recoveryMgr, TransactionLifecycleListener bufferMgr, boolean readOnly,
			long txNum, long startTn) {
		this.concurMgr = (ConcurrencyMgr) concurMgr;
		this.recoveryMgr = (RecoveryMgr) recoveryMgr;
		this.bufferMgr = (BufferMgr) bufferMgr;
		this.txNum = txNum;
		this.startTn = startTn;
		this.readOnly = readOnly;
		this.writeSet = new HashMap<RecordField, Constant>();
		this.readSet = new HashSet<RecordField>();
		this.deleteSet = new HashSet<RecordField>();
		this.cCertified = false;
		this.rCertified = false;

		lifecycleListeners = new LinkedList<TransactionLifecycleListener>();
		// XXX: A transaction manager must be added before a recovery manager to
		// prevent the following scenario:
		// <COMMIT 1>
		// <NQCKPT 1,2>
		//
		// Although, it may create another scenario like this:
		// <NQCKPT 2>
		// <COMMIT 1>
		// But the current algorithm can still recovery correctly during this
		// scenario.
		addLifecycleListener(txMgr);
		/*
		 * A recover manager must be added before a concurrency manager. For
		 * example, if the transaction need to roll back, it must hold all locks
		 * until the recovery procedure complete.
		 */
		addLifecycleListener(recoveryMgr);
		addLifecycleListener(concurMgr);
		addLifecycleListener(bufferMgr);
	}

	public void addLifecycleListener(TransactionLifecycleListener listener) {
		lifecycleListeners.add(listener);
	}

	/**
	 * Commits the current transaction. Flushes all modified blocks (and their
	 * log records), writes and flushes a commit record to the log, releases all
	 * locks, and unpins any pinned blocks.
	 */
	public void commit() {
		
		if(VanillaDb.txMgr().validate(this)) {
			upgradeWriteLock();
			commitCertify();
			commitWorkspace();
			for (TransactionLifecycleListener l : lifecycleListeners)
				l.onTxCommit(this);
	
			if (logger.isLoggable(Level.FINE))
				logger.fine("transaction " + txNum + " committed");
		
			VanillaDb.txMgr().finishedCommit(writeSet.keySet());
			VanillaDb.txMgr().endCommit(this);
		} else {
			VanillaDb.txMgr().endCommit(this);
			throw new ValidAbortException("abort tx." + this.txNum + " for no validation");
		}
		
	}

	/**
	 * Rolls back the current transaction. Undoes any modified values, flushes
	 * those blocks, writes and flushes a rollback record to the log, releases
	 * all locks, and unpins any pinned blocks.
	 */
	public void rollback() {
		rollbackCertify();
		
		for (TransactionLifecycleListener l : lifecycleListeners) {

			l.onTxRollback(this);
		}

		if (logger.isLoggable(Level.FINE))
			logger.fine("transaction " + txNum + " rolled back");
	}

	/**
	 * Finishes the current statement. Releases slocks obtained so far for
	 * repeatable read isolation level and does nothing in serializable
	 * isolation level. This method should be called after each SQL statement.
	 */
	public void endStatement() {
		for (TransactionLifecycleListener l : lifecycleListeners)
			l.onTxEndStatement(this);
	}

	public long getStartTn() {
		return this.startTn;
	}
	
	public HashSet<RecordField> getReadSet() {
		return new HashSet<RecordField>(this.readSet);
	}
	
	public HashSet<RecordField> getWriteSet() {
		return new HashSet<RecordField>(writeSet.keySet());
	}
	
	public long getTransactionNumber() {
		return this.txNum;
	}

	public boolean isReadOnly() {
		return this.readOnly;
	}

	public RecoveryMgr recoveryMgr() {
		return recoveryMgr;
	}

	public ConcurrencyMgr concurrencyMgr() {
		return concurMgr;
	}

	public BufferMgr bufferMgr() {
		return bufferMgr;
	}
	
	public void commitCertify() {
		this.cCertified = true;
	}
	
	public boolean commitCertified() {
		return this.cCertified;
	}
	
	public void rollbackCertify() {
		this.rCertified = true;
	}
	
	public boolean rollbackCertified() {
		return this.rCertified;
	}
	
	public void putVal(String tblName, RecordId rid, String fldName, Constant val) {
		writeSet.put(new RecordField(tblName, rid, fldName), val);
	}
	
	public Constant getVal(String tblName, RecordId rid, String fldName) {
		readSet.add(new RecordField(tblName, rid, fldName));
		return writeSet.get(new RecordField(tblName, rid, fldName));
	}
	
	public void delete(String tblName, RecordId rid) {
		deleteSet.add(new RecordField(tblName, rid, ""));
	}
	
	public boolean checkDeleteSet(String tblName, RecordId rid) {
		return deleteSet.contains(new RecordField(tblName, rid, ""));
	}
	
	private void upgradeWriteLock() {
		for (RecordField rf: writeSet.keySet())
			this.concurMgr.modifyRecord(rf.rid);
	}
	
	private void commitWorkspace() {
		for (Map.Entry<RecordField, Constant> entry: writeSet.entrySet()) {
			RecordField rfield = entry.getKey();
			Constant val = entry.getValue();
			//System.out.println(rfield.tblName + " before write");
			TableInfo ti = VanillaDb.catalogMgr().getTableInfo(rfield.tblName, this);
			RecordFile rfile = ti.open(this, true);
			rfile.moveToRecordId(rfield.rid);
			rfile.setVal(rfield.fldName, val);
			rfile.close();
		}

		for(RecordField rfield: deleteSet) {
			//System.out.println(rfield.tblName + " before delete");
			TableInfo ti = VanillaDb.catalogMgr().getTableInfo(rfield.tblName, this);
			//if(ti==null) System.out.println("ti is fucking null");
			RecordFile rfile = ti.open(this, true);
			rfile.delete(rfield.rid);
			rfile.close();
		}
	}
}
