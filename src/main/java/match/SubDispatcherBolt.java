package match;

import hermes.dataobj.Subscription;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by hult on 4/2/17.
 */
public class SubDispatcherBolt extends AbstractDispatcherBolt {

    public SubDispatcherBolt(int attrSize, int nSeg, int nMatcher) {
        super(attrSize, nSeg, nMatcher);
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        //System.out.println(this.toString() + " RECV: " + bytes.length + " bytes");
        Subscription sub = Subscription.fromBytes(input.getBinary(0));
        if (processed.contains(sub.id)) {
            return;
        }
        sub.timestamp = System.currentTimeMillis();
        processed.add(sub.id);
        List<Integer> vectors = dispatcher.vectorOfSubscription(sub.filter);

        List<Integer> ids = vectors.stream().map(v -> v / (N_SPACE / N_MATCHER)).distinct().collect(Collectors.toList());
        //System.out.println("map sub " + sub.id + " to " + ids.size());
        for (int id : ids) {
            int taskID = this.taskIDs.get(id);
            collector.emitDirect(taskID, Constant.SUB_STREAM, new Values(sub));
        }
        /*
        for (int v: vectors) {
            int id = v / N_SPACE_PER_BOLT;
            id2vec.putIfAbsent(id, new HashSet<>());
            id2vec.get(id).add(id % N_SPACE_PER_BOLT / N_SPACE_PER_MATCHER);
        }
        for (int id : id2vec.keySet()) {
            int taskID = this.taskIDs.get(id);
            collector.emitDirect(taskID, Constant.SUB_STREAM, new Values(sub, id2vec.get(id)));
        }
        id2vec.clear();
        */
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(Constant.SUB_STREAM, true, new Fields(Constant.SUB_FIELD));
        //declarer.declareStream(Constant.SUB_STREAM, true, new Fields(Constant.SUB_FIELD, Constant.MATCHER_ID));
    }
}
