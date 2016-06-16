package inf.ed.gfd.detect.repl;

import inf.ed.gfd.detect.dist.ViolationResult;
import inf.ed.gfd.structure.GFD2;
import inf.ed.gfd.structure.Partition;
import inf.ed.grape.interfaces.LocalComputeTask;
import inf.ed.grape.interfaces.Message;
import inf.ed.graph.structure.Graph;
import inf.ed.graph.structure.OrthogonalEdge;
import inf.ed.graph.structure.adaptor.TypedEdge;
import inf.ed.graph.structure.adaptor.VertexOString;
import inf.ed.graph.structure.adaptor.VertexString;
import inf.ed.isomorphism.VF2IsomorphismEnumInspector;
import it.unimi.dsi.fastutil.ints.Int2IntMap;

import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class QueryWithGFD extends LocalComputeTask {

	private List<GFD2> queries;

	static Logger log = LogManager.getLogger(QueryWithGFD.class);

	public void setQuery(List<GFD2> queries) {
		this.queries = queries;
	}

	@Override
	public void compute(Partition partition) {

		log.debug("local compute, 1 round.");
		for (GFD2 gfd : queries) {
			if (gfd.isConnected()) {
				processConnectedGFD(gfd, partition.getGraph());
			} else {
				processDisconnectedGFD(gfd, partition.getGraph());
			}
		}
	}

	private void processConnectedGFD(GFD2 gfd, Graph<VertexOString, OrthogonalEdge> KB) {

		assert gfd.isConnected() == true;
		long start = System.currentTimeMillis();

		log.info("STAT- begin process " + gfd.getID());
		VF2IsomorphismEnumInspector<VertexString, TypedEdge, VertexOString, OrthogonalEdge> inspector = new VF2IsomorphismEnumInspector<VertexString, TypedEdge, VertexOString, OrthogonalEdge>(
				gfd.getPattern(), 0, KB, gfd.getCandidates().get(0));

		inspector.setThreadNumLimit(inf.ed.gfd.util.KV.ISOMORPHISM_THREAD_LIMIT);
		List<Int2IntMap> results = inspector.findIsomorphic();

		log.info("STAT- isomorphism using " + (System.currentTimeMillis() - start) * 1.0 / 1000
				+ "s, for " + gfd.getCandidates().get(0).size() + " candidates.");

		start = System.currentTimeMillis();
		log.info("STAT- enumerate " + results.size() + " matches.");
		// int vio = gfd.verify(results, KB);
		List<Int2IntMap> violations = gfd.findViolations(results, KB);

		log.info("STAT- find " + violations.size() + " violations using "
				+ (System.currentTimeMillis() - start) * 1.0 / 1000 + "s.");

		// this.generatedMessages.add(violations);
		ViolationResult vr = (ViolationResult) this.generatedResult;
		vr.addViolations(violations);
	}

	private void processDisconnectedGFD(GFD2 gfd, Graph<VertexOString, OrthogonalEdge> KB) {

		assert gfd.isConnected() == false;
		long start = System.currentTimeMillis();

		log.info("STAT- begin process " + gfd.getID());
		VF2IsomorphismEnumInspector<VertexString, TypedEdge, VertexOString, OrthogonalEdge> inspector1 = new VF2IsomorphismEnumInspector<VertexString, TypedEdge, VertexOString, OrthogonalEdge>(
				gfd.getPatterns().get(0), 0, KB, gfd.getCandidates().get(0));

		inspector1.setThreadNumLimit(inf.ed.gfd.util.KV.ISOMORPHISM_THREAD_LIMIT);
		List<Int2IntMap> results1 = inspector1.findIsomorphic();

		log.info("STAT- isomorphism for component 1 using " + (System.currentTimeMillis() - start)
				* 1.0 / 1000 + "s, for " + gfd.getCandidates().get(0).size() + " candidates.");
		log.info("STAT- enumerate results1 " + results1.size() + " matches.");

		start = System.currentTimeMillis();

		VF2IsomorphismEnumInspector<VertexString, TypedEdge, VertexOString, OrthogonalEdge> inspector2 = new VF2IsomorphismEnumInspector<VertexString, TypedEdge, VertexOString, OrthogonalEdge>(
				gfd.getPatterns().get(1), 10, KB, gfd.getCandidates().get(10));
		inspector2.setThreadNumLimit(inf.ed.gfd.util.KV.ISOMORPHISM_THREAD_LIMIT);
		List<Int2IntMap> results2 = inspector2.findIsomorphic();

		log.info("STAT- isomorphism for component 2 using " + (System.currentTimeMillis() - start)
				* 1.0 / 1000 + "s, for " + gfd.getCandidates().get(0).size() + " candidates.");
		log.info("STAT- enumerate results2 " + results2.size() + " matches.");

		start = System.currentTimeMillis();
		int vio = gfd.verify2MatchList(results1, results2, KB);

		log.info("STAT- find " + vio + " violations using " + (System.currentTimeMillis() - start)
				* 1.0 / 1000 + "s.");
	}

	@Override
	public void incrementalCompute(Partition partition, List<Message<?>> incomingMessages) {
		throw new IllegalArgumentException(
				"Orthogonal graph doesn't support this method with certain para(s).");
	}

	@Override
	public void prepareResult(Partition partition) {
		// TODO Auto-generated method stub
	}
}
