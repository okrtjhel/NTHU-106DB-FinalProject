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
 * Serializable two-phase-locking concurrency manager.
 */
public class SerializableTplConcurrencyMgr extends TwoPhaseLockingConcurrencyMgr {

	public SerializableTplConcurrencyMgr(long txNumber) {
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

	@Override
	public void onTxEndStatement(Transaction tx) {
		// do nothing
	}

	@Override
	public void modifyFile(String fileName) {
		lockTbl.xLock(fileName, txNum);
	}

	@Override
	public void readFile(String fileName) {
		lockTbl.isLock(fileName, txNum);
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
		lockTbl.sLock(blk, txNum);
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
		lockTbl.isLock(recId.block(), txNum);
		lockTbl.sLock(recId, txNum);
	}

	@Override
	public void modifyIndex(String dataFileName) {
		lockTbl.ixLock(dataFileName, txNum);
	}

	@Override
	public void readIndex(String dataFileName) {
		lockTbl.isLock(dataFileName, txNum);
	}
}
