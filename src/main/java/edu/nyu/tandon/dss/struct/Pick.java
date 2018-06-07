package edu.nyu.tandon.dss.struct;

/**
 * @author michal.siedlaczek@nyu.edu
 */
public class Pick implements Comparable<Pick> {
    private int shard;
    private int bucket;

    public Pick(int shard, int bucket) {
        this.shard = shard;
        this.bucket = bucket;
    }

    @Override
    public int compareTo(Pick pick) {
        if (shard < pick.shard) return -1;
        if (shard > pick.shard) return 1;
        return Integer.compare(bucket, pick.bucket);
    }

    public Integer getShard() {
        return shard;
    }

    public Integer getBucket() {
        return bucket;
    }
}
