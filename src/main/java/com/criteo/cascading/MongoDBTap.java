package com.criteo.cascading;

import java.io.IOException;

import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;

import cascading.flow.FlowProcess;
import cascading.tap.SourceTap;
import cascading.tap.hadoop.io.RecordReaderIterator;
import cascading.tuple.TupleEntryIterator;
import cascading.tuple.TupleEntrySchemeIterator;

import com.mongodb.hadoop.io.BSONWritable;
import com.mongodb.hadoop.util.MongoConfigUtil;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class MongoDBTap extends SourceTap<JobConf, RecordReader<BSONWritable, BSONWritable>> {

    private final static MongoDBScheme scheme = new MongoDBScheme();
    private String mongoUri;

    public MongoDBTap(String mongoUri) {
        super(scheme);
        this.mongoUri = mongoUri;
    }

    @Override
    public String getIdentifier() {
        return mongoUri;
    }

    @Override
    public TupleEntryIterator openForRead(FlowProcess<JobConf> flowProcess, RecordReader<BSONWritable, BSONWritable> input) throws IOException {
        // String identifier = (String) flowProcess.getProperty( "cascading.source.path" );

        // this is only called cluster task side when Hadoop is providing a RecordReader instance it owns
        // during processing of an InputSplit
        if (input != null)
            return new TupleEntrySchemeIterator(flowProcess, getScheme(), new RecordReaderIterator(input));

        throw new RuntimeException("Very serious issue, I suggest you pidgin @mbrugidou");
    }

    @Override
    public boolean resourceExists(JobConf conf) throws IOException {
        // TODO should verify collection exists
        return true;
    }

    @Override
    public long getModifiedTime(JobConf conf) throws IOException {
        // TODO Auto-generated method stub
        long unix = System.currentTimeMillis();
        // unix = Long.MAX_VALUE;
        return unix;
    }

    @Override
    public void sourceConfInit(FlowProcess<JobConf> process, JobConf conf)
    {
        FileInputFormat.setInputPaths(conf, "/tmp/");
        MongoConfigUtil.setInputURI(conf, this.mongoUri);

        // very unsafe way to read : read from shard directly instead of reading from mongoS.
        // This can cause issues (which ones ?) if rebalancing during dump
        MongoConfigUtil.setReadSplitsFromShards(conf, true);

        super.sourceConfInit(process, conf);

    }

}
