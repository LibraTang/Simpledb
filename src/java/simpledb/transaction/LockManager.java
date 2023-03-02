package simpledb.transaction;

import simpledb.storage.PageId;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class LockManager {
    // Page级别的锁
    private final Map<PageId, Lock> lockTable = new ConcurrentHashMap<>();
    // 死锁检测
    private final DeadLockDetector detector = new DeadLockDetector();

    public void acquireReadLock(PageId pid, TransactionId tid) throws TransactionAbortedException {
        Lock lock = getLock(pid);
        if (detector.waitFor(tid, pid)) {
            // 检测到死锁，abort
            throw new TransactionAbortedException();
        }
        lock.getReadLock(tid);
        // 已经授予锁
        detector.lockGrantedOrGiveUp(tid, pid);
    }

    public void acquireWriteLock(PageId pid, TransactionId tid) throws TransactionAbortedException {
        Lock lock = getLock(pid);
        if (detector.waitFor(tid, pid)) {
            // 检测到死锁，abort
            throw new TransactionAbortedException();
        }
        lock.getWriteLock(tid);
        detector.lockGrantedOrGiveUp(tid, pid);
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

    class DeadLockDetector {
        // 事务等待获取的page的锁
        private final Map<TransactionId, Set<PageId>> waitForMap = new ConcurrentHashMap<>();

        /**
         * 将事务和对应的锁加入map，并检查事务依赖
         * @param tid
         * @param pid
         * @return
         */
        public synchronized boolean waitFor(TransactionId tid, PageId pid) {
            // 加入等待map中
            waitForMap.compute(tid, (k, v) -> {
                if (v == null) {
                    v = new HashSet<>();
                }
                v.add(pid);
                return v;
            });
            Lock lock = lockTable.get(pid);
            // 检查持有该锁的事务的依赖关系
            if (hasLoop(lock.owners)) {
                // 如果会成环，当前事务放弃获取锁
                lockGrantedOrGiveUp(tid, pid);
                return true;
            }
            return false;
        }

        /**
         * 事务能够获取该锁或者放弃获取该锁，清除map中的等待记录
         * @param tid
         * @param pid
         */
        public synchronized void lockGrantedOrGiveUp(TransactionId tid, PageId pid) {
            Set<PageId> pageIds = waitForMap.get(tid);
            pageIds.remove(pid);
            if (pageIds.isEmpty()) {
                waitForMap.remove(tid);
            }
        }

        /**
         * 事务依赖是否存在环
         * @param waitingForOwners // 等待的锁的持有者
         * @return
         */
        private boolean hasLoop(Set<TransactionId> waitingForOwners) {
            // 记录已经检查过的事务
            Set<TransactionId> checked = new HashSet<>();
            // 准备检查的事务队列
            Queue<TransactionId> queue = new LinkedList<>(waitingForOwners);
            while (!queue.isEmpty()) {
                // 当前检查的事务
                TransactionId currentT = queue.poll();
                checked.add(currentT);
                // 该事务等待的锁
                Set<PageId> waitingPages = waitForMap.get(currentT);
                if (waitingPages == null) {
                    continue;
                }
                for (PageId pid : waitingPages) {
                    // 持有该锁的事务
                    Set<TransactionId> owners = lockTable.get(pid).owners;
                    for (TransactionId owner : owners) {
                        if (!waitForMap.containsKey(owner)) {
                            // 只有在等待锁的事务才需要检查
                            continue;
                        }
                        if (currentT.equals(owner)) {
                            // 自己占有的锁可以重入
                            continue;
                        }
                        if (checked.contains(owner)) {
                            // 重复访问，有环
                            return true;
                        } else {
                            // 加入检查队列中
                            queue.add(owner);
                        }
                    }
                }
            }
            return false;
        }
    }
}
