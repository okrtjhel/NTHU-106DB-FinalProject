package org.vanilladb.core.storage.tx.concurrency.tpl;

import java.util.ArrayList;
import java.util.List;

import org.vanilladb.core.storage.file.BlockId;
import org.vanilladb.core.storage.tx.concurrency.ConcurrencyMgr;

public abstract class TwoPhaseLockingConcurrencyMgr implements ConcurrencyMgr {
	
	// Singleton
	protected static TplLockTable lockTbl = new TplLockTable();
	
	protected long txNum;

	/*
	 * Methods for B-Tree index locking
	 */
	private List<BlockId> readIndexBlks = new ArrayList<BlockId>();
	private List<BlockId> writenIndexBlks = new ArrayList<BlockId>();

	/**
	 * Sets lock on the leaf block for update.
	 * 
	 * @param blk
	 *            the block id
	 */
	public void modifyLeafBlock(BlockId blk) {
		lockTbl.xLock(blk, txNum);
		writenIndexBlks.add(blk);
	}

	/**
	 * Sets lock on the leaf block for read.
	 * 
	 * @param blk
	 *            the block id
	 */
	public void readLeafBlock(BlockId blk) {
		lockTbl.sLock(blk, txNum);
		readIndexBlks.add(blk);
	}

	/**
	 * Sets exclusive lock on the directory block when crabbing down for
	 * modification.
	 * 
	 * @param blk
	 *            the block id
	 */
	public void crabDownDirBlockForModification(BlockId blk) {
		lockTbl.xLock(blk, txNum);
		writenIndexBlks.add(blk);
	}

	/**
	 * Sets shared lock on the directory block when crabbing down for read.
	 * 
	 * @param blk
	 *            the block id
	 */
	public void crabDownDirBlockForRead(BlockId blk) {
		lockTbl.sLock(blk, txNum);
		readIndexBlks.add(blk);
	}

	/**
	 * Releases exclusive locks on the directory block for crabbing back.
	 * 
	 * @param blk
	 *            the block id
	 */
	public void crabBackDirBlockForModification(BlockId blk) {
		lockTbl.release(blk, txNum, TplLockTable.X_LOCK);
	}

	/**
	 * Releases shared locks on the directory block for crabbing back.
	 * 
	 * @param blk
	 *            the block id
	 */
	public void crabBackDirBlockForRead(BlockId blk) {
		lockTbl.release(blk, txNum, TplLockTable.S_LOCK);
	}

	public void releaseIndexLocks() {
		for (BlockId blk : readIndexBlks)
			lockTbl.release(blk, txNum, TplLockTable.S_LOCK);
		for (BlockId blk : writenIndexBlks)
			lockTbl.release(blk, txNum, TplLockTable.X_LOCK);
		readIndexBlks.clear();
		writenIndexBlks.clear();
	}

	public void lockRecordFileHeader(BlockId blk) {
		lockTbl.xLock(blk, txNum);
	}

	public void releaseRecordFileHeader(BlockId blk) {
		lockTbl.release(blk, txNum, TplLockTable.X_LOCK);
	}
}
