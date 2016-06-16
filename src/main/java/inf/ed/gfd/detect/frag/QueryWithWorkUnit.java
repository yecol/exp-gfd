package inf.ed.gfd.detect.frag;

import inf.ed.gfd.detect.dist.ViolationResult;
import inf.ed.gfd.structure.Ball;
import inf.ed.gfd.structure.GfdMsg;
import inf.ed.gfd.structure.Partition;
import inf.ed.gfd.structure.WorkUnit;
import inf.ed.grape.interfaces.LocalComputeTask;
import inf.ed.grape.interfaces.Message;
import inf.ed.graph.structure.Graph;
import inf.ed.graph.structure.OrthogonalEdge;
import inf.ed.graph.structure.adaptor.VertexOString;
import inf.ed.isomorphism.LocalViolationEnumInspector;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntSet;

import java.util.List;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class QueryWithWorkUnit extends LocalComputeTask {

	// private Int2ObjectMap<Ball> mapBorderVertex2Ball;
	private Set<WorkUnit> workunits;
	private Int2ObjectMap<IntSet> prefetchRequest;

	static Logger log = LogManager.getLogger(QueryWithWorkUnit.class);

	public void setMapBorderVertex2Ball(Int2ObjectMap<Ball> map) {
		// this.mapBorderVertex2Ball = map;
	}

	public void setWorkUnits(Set<WorkUnit> workunits) {
		this.workunits = workunits;
	}

	public void setPrefetchQuest(Int2ObjectMap<IntSet> requests) {
		this.prefetchRequest = requests;
	}

	@Override
	public void compute(Partition partition) {
		log.debug("begin local compute current super step = " + this.getSuperstep());
		if (this.getSuperstep() == 0) {
			// send the prefetch requesets
			for (int destinationPID : prefetchRequest.keySet()) {
				GfdMsg content = new GfdMsg();
				content.requestedBorderNodes = prefetchRequest.get(destinationPID);
				Message<GfdMsg> m = new Message<GfdMsg>(partition.getPartitionID(), destinationPID,
						content);
				this.generatedMessages.add(m);
			}
		}
	}

	@Override
	public void incrementalCompute(Partition partition, List<Message<?>> incomingMessages) {

		log.info("now incremental compute, got incomming message size = ");

		if (getSuperstep() == 1) {

			log.info("superstep = 1, got prefetch request and send graph data as response.");
			receivePrefetchRequestAndTransferData(partition, incomingMessages);

		} else if (getSuperstep() == 2) {

			log.info("super step = 2, got prefetched request data and parse them into graph.");
			receiveTransferedGraphData(partition, incomingMessages);
			log.info("all the edges and vertex are added into partition.");
		}
	}

	private void receivePrefetchRequestAndTransferData(Partition partition,
			List<Message<?>> incomingMessages) {

		Int2ObjectMap<GfdMsg> newMessageContents = new Int2ObjectOpenHashMap<GfdMsg>();

		if (incomingMessages != null) {

			for (Message<?> recvMsg : incomingMessages) {
				log.debug(recvMsg.toString());

				if (!newMessageContents.containsKey(recvMsg.getSourcePartitionID())) {
					newMessageContents.put(recvMsg.getSourcePartitionID(), new GfdMsg());
				}
				GfdMsg newMsgContent = newMessageContents.get(recvMsg.getSourcePartitionID());
				GfdMsg recvContent = (GfdMsg) recvMsg.getContent();
				for (int borderNode : recvContent.requestedBorderNodes) {
					Ball borderNodeWithBall = partition.getGraph().getBall(borderNode, 1);
					if (borderNodeWithBall != null) {
						newMsgContent.transferingGraphData.add(borderNodeWithBall);
						log.debug(borderNodeWithBall.getInfo());
					}
				}
			}

			for (int targetPartitionID : newMessageContents.keySet()) {
				Message<GfdMsg> nMsg = new Message<GfdMsg>(partition.getPartitionID(),
						targetPartitionID, newMessageContents.get(targetPartitionID));
				this.generatedMessages.add(nMsg);
			}
		}
	}

	private void receiveTransferedGraphData(Partition partition, List<Message<?>> incomingMessages) {

		if (incomingMessages != null) {
			for (Message<?> recvMsg : incomingMessages) {

				log.debug(recvMsg.toString());

				GfdMsg recvContent = (GfdMsg) recvMsg.getContent();
				System.out.println("before add balls, " + partition.getPartitionInfo());
				for (Ball ball : recvContent.transferingGraphData) {
					System.out.println(ball.getInfo());
					partition.addTransferedGraph(ball);
				}
				System.out.println("after add balls, " + partition.getPartitionInfo());
			}
		}

		LocalViolationEnumInspector detector = new LocalViolationEnumInspector(workunits,
				partition.getGraph());
		List<Int2IntMap> partialViolations = detector.findIsomorphic();

		ViolationResult vr = (ViolationResult) this.generatedResult;
		vr.addViolations(partialViolations);
		log.debug("locally find " + partialViolations.size() + " violations");

	}

	@Override
	public void prepareResult(Partition partition) {
		// TODO Auto-generated method stub
	}
}
