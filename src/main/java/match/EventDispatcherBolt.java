package match;

import hermes.dataobj.Event;
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
public class EventDispatcherBolt extends AbstractDispatcherBolt {

    public EventDispatcherBolt(int attrSize, int nSeg, int nMatcher) {
        super(attrSize, nSeg, nMatcher);
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        Event event = Event.fromBytes(input.getBinary(0));
        event.timestamp = System.currentTimeMillis();
        System.out.println(event);
        List<Integer> vectors = this.dispatcher.vectorOfEvent(event.values);
        List<Integer> ids = vectors.stream().map(v -> v / N_SPACE_PER_BOLT).distinct().collect(Collectors.toList());
        for (int id : ids) {
            int taskID = this.taskIDs.get(id);
            collector.emitDirect(taskID, Constant.EVENT_STREAM, new Values(event));
        }
        /*
        for (int v: vectors) {
            int id = v / N_SPACE_PER_BOLT;
            id2vec.putIfAbsent(id, new HashSet<>());
            id2vec.get(id).add(v % N_SPACE_PER_BOLT / N_SPACE_PER_MATCHER);
        }
        for (int id: id2vec.keySet()) {
            int taskID = this.taskIDs.get(id);
            collector.emitDirect(taskID, Constant.EVENT_STREAM, new Values(event, id2vec.get(id)));
        }
        id2vec.clear();
        */
    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(Constant.EVENT_STREAM, true, new Fields(Constant.EVENT_FIELD));
        //declarer.declareStream(Constant.EVENT_STREAM, true, new Fields(Constant.EVENT_FIELD, Constant.MATCHER_ID));
    }
}
