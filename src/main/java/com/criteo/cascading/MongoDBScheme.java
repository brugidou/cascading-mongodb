package com.criteo.cascading;

import java.io.IOException;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;
import cascading.flow.FlowProcess;
import cascading.scheme.Scheme;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.hadoop.io.BSONWritable;
import com.mongodb.hadoop.mapred.MongoInputFormat;
import com.mongodb.hadoop.util.MongoConfigUtil;

@SuppressWarnings("rawtypes")
public class MongoDBScheme extends // <Config, Input, Output, SourceContext, SinkContext>
        Scheme<JobConf, RecordReader<BSONWritable, BSONWritable>, OutputCollector, BSONWritable[], BSONWritable[]> {

    public MongoDBScheme() {
        super(new Fields("bson"));
    }

    @Override
    public void sourceConfInit(FlowProcess<JobConf> process, Tap<JobConf, RecordReader<BSONWritable, BSONWritable>, OutputCollector> tap,
            JobConf conf) { // FlowProcess<Config> flowProcess, Tap<Config, Input, Output> tap, Config conf
        final DBObject fieldsBson = new BasicDBObject();

        MongoConfigUtil.setFields(conf, fieldsBson);

        final DBObject q = new BasicDBObject();
        if (conf.getBoolean("testing.limit", false)) {

            int lowerBound = conf.getInt("testing.limit.lowerbound", 0);
            int upperBound = conf.getInt("testing.limit.upperBound", 999999);
            q.put("partnerId", new BasicDBObject("$lt", upperBound).put("$gt", lowerBound)); // useful for testing locally
            System.out.println("Setting a query to limit fetched results to " + q);
        }
        MongoConfigUtil.setQuery(conf, q);
        final DBObject fields = new BasicDBObject();
        fields.put("_id", 0); // remove the impossible to jsonify _id field
        MongoConfigUtil.setFields(conf, fields);

        conf.setInputFormat(MongoInputFormat.class);
    }

    @Override
    public void sinkConfInit(FlowProcess<JobConf> process, Tap<JobConf, RecordReader<BSONWritable, BSONWritable>, OutputCollector> tap,
            JobConf conf) {
        // should never be called because this is a source scheme :-)
        throw new NotImplementedException();
    }

    @Override
    public void sourcePrepare(FlowProcess<JobConf> flowProcess, SourceCall<BSONWritable[], RecordReader<BSONWritable, BSONWritable>> sourceCall)
    {
        sourceCall.setContext(new BSONWritable[2]);

        sourceCall.getContext()[0] = sourceCall.getInput().createKey();
        sourceCall.getContext()[1] = sourceCall.getInput().createValue();
    }

    @Override
    public boolean source(FlowProcess<JobConf> flowProcess, SourceCall<BSONWritable[], RecordReader<BSONWritable, BSONWritable>> sourceCall) throws IOException {
        BSONWritable[] context = sourceCall.getContext();
        if (!sourceCall.getInput().next(context[0], context[1]))
            return false;

        Tuple tuple = sourceCall.getIncomingEntry().getTuple();
        tuple.set(0, context[1]);
        return true;

    }

    @Override
    public void sink(FlowProcess<JobConf> flowProcess, SinkCall<BSONWritable[], OutputCollector> sinkCall) throws IOException {
        // should never be called because this is a source scheme :-)
        throw new NotImplementedException();

    }

}
