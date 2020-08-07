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
package org.vanilladb.core.storage.tx.concurrency.tpl;

import org.vanilladb.core.storage.file.BlockId;
import org.vanilladb.core.storage.record.RecordId;
import org.vanilladb.core.storage.tx.Transaction;
/**
 * Read-committed two-phase-locking concurrency manager.
 */
public class ReadCommittedTplConcurrencyMgr extends TwoPhaseLockingConcurrencyMgr {

	public ReadCommittedTplConcurrencyMgr(long txNumber) {
		txNum = txNumber;
	}

	@Override
	public void onTxCommit(Transaction tx) {
		lockTbl.releaseAll(txNum, false);
	}

	@Override
	public void onTxRollback(Transaction tx) {
		lockTbl.releaseAll(txNum, false);
	}

	/**
	 * Releases all slocks obtained so far.
	 */
	@Override
	public void onTxEndStatement(Transaction tx) {
		lockTbl.releaseAll(txNum, true);
	}

	@Override
	public void modifyFile(String fileName) {
		lockTbl.xLock(fileName, txNum);
	}

	@Override
	public void readFile(String fileName) {
		lockTbl.isLock(fileName, txNum);
		// releases IS lock to allow phantoms
		lockTbl.release(fileName, txNum, TplLockTable.IS_LOCK);
	}

	@Override
	public void insertBlock(BlockId blk) {
		lockTbl.xLock(blk.fileName(), txNum);
		lockTbl.xLock(blk, txNum);
	}

	@Override
	public void modifyBlock(BlockId blk) {
		lockTbl.ixLock(blk.fileName(), txNum);
		lockTbl.xLock(blk, txNum);
	}

	@Override
	public void readBlock(BlockId blk) {
		lockTbl.isLock(blk.fileName(), txNum);
		// releases IS lock to allow phantoms
		lockTbl.release(blk.fileName(), txNum, TplLockTable.IS_LOCK);
		
		lockTbl.sLock(blk, txNum);
		// releases S lock to allow unrepeatable Read
		lockTbl.release(blk, txNum, TplLockTable.S_LOCK);
	}
	
	@Override
	public void modifyRecord(RecordId recId) {
		lockTbl.ixLock(recId.block().fileName(), txNum);
		lockTbl.ixLock(recId.block(), txNum);
		lockTbl.xLock(recId, txNum);
	}

	@Override
	public void readRecord(RecordId recId) {
		lockTbl.isLock(recId.block().fileName(), txNum);
		// releases IS lock to allow phantoms
		lockTbl.release(recId.block().fileName(), txNum, TplLockTable.IS_LOCK);
		
		lockTbl.isLock(recId.block(), txNum);
		// releases IS lock to allow phantoms
		lockTbl.release(recId.block(), txNum, TplLockTable.IS_LOCK);
		
		lockTbl.sLock(recId, txNum);
		// releases S lock to allow unrepeatable Read
		lockTbl.release(recId, txNum, TplLockTable.S_LOCK);
	}

	@Override
	public void modifyIndex(String dataFileName) {
		lockTbl.xLock(dataFileName, txNum);
	}

	@Override
	public void readIndex(String dataFileName) {
		lockTbl.isLock(dataFileName, txNum);
		// releases IS lock to allow phantoms
		lockTbl.release(dataFileName, txNum, TplLockTable.IS_LOCK);
	}
}
