package com.lly.backend.IM;

import com.lly.backend.DM.DataManager;
import com.lly.backend.DM.dataItem.DataItem;
import com.lly.backend.TM.TransactionManagerImpl;
import com.lly.backend.common.MySubArray;
import com.lly.common.utils.Parser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class BPlusTree {
    DataManager dm;
    long bootUid; //根节点不是固定的，所以需要一个bootUid来记录根节点的uid
    DataItem bootDataItem;//根节点的DataItem
    Lock bootLock;

    /**
     * 创建一个新的B+树,根节点为一个空的叶子节点
     * @param dm
     * @return
     * @throws Exception
     */
    public static long create(DataManager dm) throws Exception {
        byte[] nilRootRaw = Node.newNilRootRaw();
        long rootUid = dm.insert(TransactionManagerImpl.SUPER_XID, nilRootRaw);
        return dm.insert(TransactionManagerImpl.SUPER_XID, Parser.long2Byte(rootUid));
    }

    /**
     * 从bootUid加载B+树
     * @param bootUid bootDataItem的uid，其中存储了rootuid
     * @param dm
     * @return
     * @throws Exception
     */
    public static BPlusTree load(long bootUid, DataManager dm) throws Exception {
        DataItem bootDataItem = dm.read(bootUid);
        assert bootDataItem != null;
        BPlusTree t = new BPlusTree();
        t.bootUid = bootUid;
        t.dm = dm;
        t.bootDataItem = bootDataItem;
        t.bootLock = new ReentrantLock();
        return t;
    }

    public List<Long> search(long key) throws Exception {
        return searchRange(key, key);

    }

    /**
     * 在B+树搜索指定键值范围内的叶子节点中的uid列表
     * @param leftKey
     * @param rightKey
     * @return 返回指定范围的键值对应的uid列表
     */
    public List<Long> searchRange(long leftKey, long rightKey) throws Exception {
        long rootUid = getRootUid();
        //从根节点开始搜索，找到leftKey节点的最左叶子
        long leafUid = searchLeaf(rootUid, leftKey);
        List<Long> uids = new ArrayList<>();
        while(true) {
            Node leaf = Node.loadNode(this, leafUid);
            Node.LeafSearchRangeRes res = leaf.leafSearchRange(leftKey, rightKey);
            leaf.release();
            uids.addAll(res.uids);
            if(res.broUid==0) {
                break;
            } else {
                //继续搜索兄弟节点
                leafUid = res.broUid;
            }
        }
        return uids;
    }


    /**
     *
     * @param nodeUid
     * @param key
     * @return
     * @throws Exception
     */
    private long searchLeaf(long nodeUid, long key) throws Exception {
        Node node = Node.loadNode(this, nodeUid);
        boolean isLeaf = node.isLeaf();
        node.release();

        if(isLeaf) {
            return node.uid;
        } else {
            //当前节点不是叶子节点，继续搜索下一个节点
            long nextUid = searchNext(nodeUid, key);
            return searchLeaf(nextUid, key);
        }

    }

    /**
     * 寻找当前key的下一个key
     * @param nodeUid 开始搜索的起始节点
     * @param key 当前的key
     * @return
     * @throws Exception
     */
    private long searchNext(long nodeUid, long key) throws Exception {
        while (true){
            Node node = Node.loadNode(this, nodeUid);
            //尝试在当前节点中查找到key的下一个节点
            Node.SearchNextRes searchNextRes = node.searchNext(key);
            node.release();
            if(searchNextRes.uid != 0) return searchNextRes.uid;
            //当前节点没有找到，在兄弟节点中继续查找
            nodeUid = searchNextRes.BroUid;
        }
    }

    private long getRootUid() {
        bootLock.lock();
        try {
            MySubArray sa = bootDataItem.data();
            return Parser.getLong(Arrays.copyOfRange(sa.raw, sa.start, sa.start+8));
        } finally {
            bootLock.unlock();
        }
    }

    /**
     *更新根节点的uid
     */
    private void updateRootUid(long left, long right, long rightKey) throws Exception {
        bootLock.lock();
        try{
            byte[] rootRaw = Node.newRootRaw(left, right, rightKey);
            long newRootUid = dm.insert(TransactionManagerImpl.SUPER_XID, rootRaw);
            bootDataItem.before();
            MySubArray diRaw = bootDataItem.data();
            System.arraycopy(Parser.long2Byte(newRootUid),0,diRaw.raw,diRaw.start,8);
            bootDataItem.after(TransactionManagerImpl.SUPER_XID);
        }finally {
            bootLock.unlock();
        }
    }

    /**
     * B+树插入键值对
     */
    public void insert(long key, long uid) throws Exception {
        long rootUid = getRootUid();
        insert(rootUid, key, uid);

    }

    private void insert(long rootUid, long key, long uid) {

    }


}
