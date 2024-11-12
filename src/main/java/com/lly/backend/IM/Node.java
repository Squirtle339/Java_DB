package com.lly.backend.IM;

import com.lly.backend.DM.dataItem.DataItem;
import com.lly.backend.common.MySubArray;
import com.lly.common.utils.Parser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Node结构如下：
 * [LeafFlag][KeyNumber][BroUid]
 * [Son0][Key0][Son1][Key1]...[SonN][KeyN]
 */
public class Node {

    static final int IS_LEAF_OFFSET = 0;//0
    static final int NO_KEYS_OFFSET = IS_LEAF_OFFSET+1;//[1:2]
    static final int Bro_OFFSET = NO_KEYS_OFFSET+2;//[3:10]
    static final int NODE_HEADER_SIZE = Bro_OFFSET+8;//大小为11Byte


    static final int BALANCE_NUMBER = 32;

    //(2*8)代表一组[Son0][Key0]，(BALANCE_NUMBER*2+2)的2主要是在b+树插入删除溢出时临时使用
    static final int NODE_SIZE = NODE_HEADER_SIZE + (2*8)*(BALANCE_NUMBER*2+2);

    BPlusTree tree;
    DataItem dataItem;
    MySubArray raw;
    long uid;

    /**
     * 新建一个根节点，两个子节点为 left 和 right, 初始键值为 key
     * @param left 左子树的 UID
     * @param right 右子树的 UID
     * @param key
     * @return
     */
    static byte[] newRootRaw(long left,long right,long key){
        MySubArray raw = new MySubArray(new byte[NODE_SIZE], 0, NODE_SIZE);
        setRawIsLeaf(raw, false);
        setRawNoKeys(raw, 2);
        setRawBro(raw, 0);
        setRawKthSon(raw, left, 0);
        setRawKthKey(raw, key, 0);
        setRawKthSon(raw, right, 1);
        setRawKthKey(raw, 0, 1);
        return raw.raw;
    }

    /**
     * 生成一个空的根节点
     * @return
     */
    static byte[] newNilRootRaw()  {
        MySubArray raw = new MySubArray(new byte[NODE_SIZE], 0, NODE_SIZE);

        setRawIsLeaf(raw, true);
        setRawNoKeys(raw, 0);
        setRawBro(raw, 0);

        return raw.raw;
    }

    /**
     * 加载一个节点
     * @param bTree 所属的 B+ 树，主要是为了使用 B+ 树的 DataManager
     * @param uid 节点的 UID
     * @return
     * @throws Exception
     */
    static Node loadNode(BPlusTree bTree, long uid) throws Exception {
        DataItem di = bTree.dm.read(uid);
        assert di != null;
        Node n = new Node();
        n.tree = bTree;
        n.dataItem = di;
        n.raw = di.data();
        n.uid = uid;
        return n;
    }

    /**
     * 释放节点的缓存引用
     */
    public void release() {
        dataItem.release();
    }

    public boolean isLeaf() {
        dataItem.rLock();
        try {
            return getRawIfLeaf(raw);
        } finally {
            dataItem.rUnLock();
        }
    }

    class LeafSearchRangeRes {
        List<Long> uids;
        long broUid;
    }
    /**
     * 辅助b+树搜索叶子节点指定范围的uid
     * @param leftKey
     * @param rightKey
     * @return 如果在当前节点叶子节点中找到了指定范围的所有uid，则直接返回uid列表，否则附带兄弟节点的uid，供上层继续搜索
     */
    public LeafSearchRangeRes leafSearchRange(long leftKey, long rightKey) {
        dataItem.rLock();
        try{
            int kth = 0;
            int noKeys = getRawNoKeys(raw);
            //移动到leftkey的位置
            while(kth < noKeys) {
                long ik = getRawKthKey(raw, kth);
                if(ik >= leftKey) {
                    break;
                }
                kth ++;
            }
            List<Long> uids = new ArrayList<>();
            while(kth < noKeys) {
                long ik = getRawKthKey(raw, kth);
                if(ik <= rightKey) {
                    uids.add(getRawKthSon(raw, kth));
                    kth ++;
                } else {
                    break;
                }
            }

            long broUid = 0;
            if(kth==noKeys) {
                broUid = getRawBro(raw);
            }
            LeafSearchRangeRes res = new LeafSearchRangeRes();
            res.uids = uids;
            res.broUid = broUid;
            return res;
        }finally {
            dataItem.rUnLock();
        }

    }


    class SearchNextRes {
        long uid;
        long BroUid;
    }

    /**
     * 寻找当前节点中 key 的下一个key, 如果找不到, 则返回兄弟节点的 UID，供上层继续搜索
     * @param key
     * @return
     */
    public SearchNextRes searchNext(long key) {
        dataItem.rLock();
        try {
            SearchNextRes res = new SearchNextRes();
            int noKeys = getRawNoKeys(raw);
            //从前往后遍历，找到第一个大于 key 的 key作为下一个节点
            for(int i = 0; i < noKeys; i ++) {
                long ik = getRawKthKey(raw, i);
                //key越大，在当前节点的key列表的位置越靠后
                if(key < ik) {
                    res.uid = getRawKthSon(raw, i);
                    res.BroUid = 0;
                    return res;
                }
            }
            //如果当前key是最大的，则返回她的兄弟节点
            res.uid = 0;
            res.BroUid = getRawBro(raw);
            return res;
        } finally {
            dataItem.rUnLock();
        }

    }





    /**
     * 设置字节数组是否为叶子节点
     * @param raw
     * @param isLeaf
     */
    static void setRawIsLeaf(MySubArray raw, boolean isLeaf) {
        if(isLeaf) {
            raw.raw[raw.start + IS_LEAF_OFFSET] = (byte)1;
        } else {
            raw.raw[raw.start + IS_LEAF_OFFSET] = (byte)0;
        }
    }
    static boolean getRawIfLeaf(MySubArray raw) {
        return raw.raw[raw.start + IS_LEAF_OFFSET] == (byte)1;
    }

    /**
     * 设置该节点中 key 的个数
     * @param raw
     * @param noKeys
     */
    static void setRawNoKeys(MySubArray raw, int noKeys) {
        System.arraycopy(Parser.short2Byte((short)noKeys), 0, raw.raw, raw.start+NO_KEYS_OFFSET, 2);
    }
    static int getRawNoKeys(MySubArray raw) {
        return (int)Parser.getShort(Arrays.copyOfRange(raw.raw, raw.start+NO_KEYS_OFFSET, raw.start+NO_KEYS_OFFSET+2));
    }


    /**
     * 设置该节点的下一兄弟节点存储在 DM 中的 UID（可以理解为指针）
     * @param raw
     * @param Bro
     */
    static void setRawBro(MySubArray raw, long Bro) {
        System.arraycopy(Parser.long2Byte(Bro), 0, raw.raw, raw.start+Bro_OFFSET, 8);
    }
    static long getRawBro(MySubArray raw) {
        return Parser.getLong(Arrays.copyOfRange(raw.raw, raw.start+Bro_OFFSET, raw.start+Bro_OFFSET+8));
    }

    /**
     * 设置第 k 个儿子节点的 UID
     * @param raw
     * @param uid
     * @param kth
     */
    static void setRawKthSon(MySubArray raw, long uid, int kth) {
        int offset = raw.start+NODE_HEADER_SIZE+kth*(8*2);
        System.arraycopy(Parser.long2Byte(uid), 0, raw.raw, offset, 8);
    }
    static long getRawKthSon(MySubArray raw, int kth) {
        int offset = raw.start+NODE_HEADER_SIZE+kth*(8*2);
        return Parser.getLong(Arrays.copyOfRange(raw.raw, offset, offset+8));
    }

    /**
     * 设置第 k 个 key
     * @param raw
     * @param key
     * @param kth
     */
    static void setRawKthKey(MySubArray raw, long key, int kth) {
        int offset = raw.start+NODE_HEADER_SIZE+kth*(8*2)+8;
        System.arraycopy(Parser.long2Byte(key), 0, raw.raw, offset, 8);
    }
    static long getRawKthKey(MySubArray raw, int kth) {
        int offset = raw.start+NODE_HEADER_SIZE+kth*(8*2)+8;
        return Parser.getLong(Arrays.copyOfRange(raw.raw, offset, offset+8));
    }

    /**
     * 把 from 从第 k个开始的数据组拷贝到 to 中，to从头开始
     * @param from
     * @param to
     * @param kth
     */
    static void copyRawFromKth(MySubArray from, MySubArray to, int kth) {
        int offset = from.start+NODE_HEADER_SIZE+kth*(8*2);
        System.arraycopy(from.raw, offset, to.raw, to.start+NODE_HEADER_SIZE, from.end-offset);
    }

    /**
     * 把第 k 个数据组后的数据组向后移动 2*8 个字节，为在第k位插入新数据腾出空间
     * @param raw
     * @param kth
     */
    static void shiftRawKth(MySubArray raw, int kth) {
        int begin = raw.start+NODE_HEADER_SIZE+(kth+1)*(8*2);
        int end = raw.start+NODE_SIZE-1;
        for(int i = end; i >= begin; i --) {
            raw.raw[i] = raw.raw[i-(8*2)];
        }
    }



}
