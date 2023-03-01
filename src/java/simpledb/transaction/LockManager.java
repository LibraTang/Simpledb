package simpledb.transaction;

import simpledb.storage.PageId;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class LockManager {
    // Page级别的锁
    private Map<PageId, Lock> lockTable = new ConcurrentHashMap<>();

    public void acquireReadLock(PageId pid, TransactionId tid) {
        Lock lock = getLock(pid);
        lock.getReadLock(tid);
    }

    public void acquireWriteLock(PageId pid, TransactionId tid) {
        Lock lock = getLock(pid);
        lock.getWriteLock(tid);
    }

    private synchronized Lock getLock(PageId pid) {
        Lock lock = lockTable.get(pid);
        if (lock == null) {
            lock = new Lock();
            lockTable.put(pid, lock);
        }
        return lock;
    }

    public synchronized void releaseLock(PageId pid, TransactionId tid) {
        Lock lock = getLock(pid);
        lock.releaseLock(tid);
    }

    public boolean holdsLock(PageId pid, TransactionId tid) {
        Lock lock = getLock(pid);
        return lock.owners.contains(tid);
    }

    // 获取事务对应拿到的写锁
    public Set<PageId> getLockedWritePages(TransactionId tid) {
        return lockTable.entrySet().stream()
                .filter(entry -> entry.getValue().owners.contains(tid))
                .filter(entry -> entry.getValue().lockType == Lock.WRITE_TYPE)
                .map(entry -> entry.getKey())
                .collect(Collectors.toSet());
    }

    // 获取事务对应拿到的读锁
    public Set<PageId> getLockedReadPages(TransactionId tid) {
        return lockTable.entrySet().stream()
                .filter(entry -> entry.getValue().owners.contains(tid))
                .filter(entry -> entry.getValue().lockType == Lock.READ_TYPE)
                .map(entry -> entry.getKey())
                .collect(Collectors.toSet());
    }

    class Lock {
        public static final int READ_TYPE = 0;
        public static final int WRITE_TYPE = 1;

        // 当前锁的持有事务
        private volatile Set<TransactionId> owners = ConcurrentHashMap.newKeySet();
        // 锁类型: 0-读锁，1-写锁
        private volatile int lockType = READ_TYPE;

        // 获取读锁
        public void getReadLock(TransactionId tid) {
            if (owners.isEmpty()) {
                // 还没有事务持有该锁
                lockType = READ_TYPE;
            }
            if (lockType == READ_TYPE) {
                // 当前是读锁，可以直接获取，包括已经存在该事务的读锁的情况
                owners.add(tid);
                return;
            }
            if (canUpdateOrReenter(tid)) {
                return;
            }
            // 有事务持有写锁，阻塞
            while (!owners.isEmpty() && !canUpdateOrReenter(tid)) {
                try {
                    Thread.sleep(5);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            // 阻塞结束，获取读锁
            lockType = READ_TYPE;
            owners.add(tid);
        }

        // 获取写锁
        public void getWriteLock(TransactionId tid) {
            if (owners.isEmpty()) {
                // 还没有事务持有该锁，可以获取写锁
                lockType = WRITE_TYPE;
                owners.add(tid);
                return;
            }
            while (!owners.isEmpty() && !canUpdateOrReenter(tid)) {
                // 有其他事务持有锁，阻塞
                try {
                    Thread.sleep(5);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            // 阻塞结束，获取写锁
            lockType = WRITE_TYPE;
            owners.add(tid);
        }

        public void releaseLock(TransactionId tid) {
            owners.remove(tid);
        }

        // 当前锁的持有事务是同一个事务，可以升级成写锁，或重新获取读锁
        private boolean canUpdateOrReenter(TransactionId tid) {
            return owners.size() == 1 && owners.contains(tid);
        }
    }
}
