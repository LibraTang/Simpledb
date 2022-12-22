package simpledb.utils;

import java.util.HashMap;
import java.util.Map;

public class LruCache<K, V> {
    // 双向链表
    public class DLinkedNode {
        public DLinkedNode pre;
        public DLinkedNode next;
        public K key;
        public V value;

        public DLinkedNode() {
        }

        public DLinkedNode(K key, V value) {
            this.key = key;
            this.value = value;
        }
    }

    // 哈希表
    private final Map<K, DLinkedNode> cacheMap;
    // cache容量
    private final int capacity;
    // 头节点
    private final DLinkedNode head;
    // 尾节点
    private final DLinkedNode tail;

    public LruCache(int capacity) {
        this.cacheMap = new HashMap<>();
        this.capacity = capacity;
        this.head = new DLinkedNode();
        this.tail = new DLinkedNode();
        this.head.next = tail;
        this.tail.pre = head;
    }

    public int getCapacity() {
        return capacity;
    }

    public int getCacheSize() {
        return cacheMap.size();
    }

    public synchronized V get(K key) {
        if (cacheMap.containsKey(key)) {
            DLinkedNode node = cacheMap.get(key);
            moveToHead(node);
            return node.value;
        }
        return null;
    }

    public synchronized void put(K key, V value) {
        // 若当前节点节点已存在，则更新后移到头部
        if (cacheMap.containsKey(key)) {
            DLinkedNode node = cacheMap.get(key);
            node.value = value;
            moveToHead(node);
        } else {
            DLinkedNode node = new DLinkedNode(key, value);
            cacheMap.put(key, node);
            // 连接到头节点之后
            linkToHead(node);
        }
    }

    // 移动节点至头部
    public void moveToHead(DLinkedNode node) {
        // 解除原来的连接
        removeNode(node);
        // 连接到头节点之后
        linkToHead(node);
    }

    // 连接至头节点
    public void linkToHead(DLinkedNode node) {
        node.next = head.next;
        node.pre = head;
        head.next = node;
        head.next.pre = node;
    }

    // 移除一个节点
    public void removeNode(DLinkedNode node) {
        node.pre.next = node.next;
        node.next.pre = node.pre;
    }
}
