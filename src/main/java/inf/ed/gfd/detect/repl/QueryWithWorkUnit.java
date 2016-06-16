package inf.ed.gfd.detect.repl;

import inf.ed.gfd.detect.dist.ViolationResult;
import inf.ed.gfd.structure.GFD2;
import inf.ed.gfd.structure.Partition;
import inf.ed.gfd.util.KV;
import inf.ed.grape.interfaces.LocalComputeTask;
import inf.ed.grape.interfaces.Message;
import inf.ed.graph.structure.Graph;
import inf.ed.graph.structure.OrthogonalEdge;
import inf.ed.graph.structure.adaptor.TypedEdge;
import inf.ed.graph.structure.adaptor.VertexOString;
import inf.ed.graph.structure.adaptor.VertexString;
import inf.ed.isomorphism.VF2IsomorphismEnumInspector;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class QueryWithWorkUnit extends LocalComputeTask {

	private List<GFD2> queries;
	private Set<ReplicatedGWorkUnit> workload;
	private Map<String, List<ReplicatedGWorkUnit>> assembledWorkload;

	static Logger log = LogManager.getLogger(QueryWithWorkUnit.class);

	public void setQuery(List<GFD2> queries) {
		this.queries = queries;
	}

	public void setWorkUnit(Set<ReplicatedGWorkUnit> workload) {
		this.workload = workload;
	}

	@Override
	public void compute(Partition partition) {
		log.debug("begin local compute, first assemble workunit by category.");
		assembleWorkUnit();
		log.debug("local violation detection. assemble and process.");
		for (List<ReplicatedGWorkUnit> workUnitList : assembledWorkload.values()) {
			ReplicatedGWorkUnit wu = workUnitList.get(0);
			if (wu.isConnected()) {
				processConnectedWorkload(workUnitList, partition.getGraph());
			} else {
				processDisconnectedWorkload(workUnitList, partition.getGraph());
			}
		}
	}

	private void assembleWorkUnit() {
		assembledWorkload = new HashMap<String, List<ReplicatedGWorkUnit>>();
		for (ReplicatedGWorkUnit wu : workload) {
			if (!assembledWorkload.containsKey(wu.getCategory())) {
				assembledWorkload.put(wu.getCategory(), new LinkedList<ReplicatedGWorkUnit>());
			}
			assembledWorkload.get(wu.getCategory()).add(wu);
		}
	}

	private GFD2 findGFDByID(String gfdID) {
		for (GFD2 gfd : queries) {
			if (gfd.getID().equals(gfdID)) {
				return gfd;
			}
		}
		throw new IllegalArgumentException("No gfd found via ID.");
	}

	private void processDisconnectedWorkload(List<ReplicatedGWorkUnit> workload,
			Graph<VertexOString, OrthogonalEdge> KB) {

		if (workload.size() == 0) {
			return;
		}

		Int2ObjectMap<List<Int2IntMap>> foundMatches1 = new Int2ObjectOpenHashMap<List<Int2IntMap>>();
		Int2ObjectMap<List<Int2IntMap>> foundMatches2 = new Int2ObjectOpenHashMap<List<Int2IntMap>>();

		ReplicatedGWorkUnit wu = workload.get(0);

		GFD2 gfd = findGFDByID(wu.getGFDID());

		IntSet candidates1 = new IntOpenHashSet();
		IntSet candidates2 = new IntOpenHashSet();

		for (int i = 0; i < workload.size(); i++) {
			candidates1.add(workload.get(i).getCandidate1());
			candidates2.add(workload.get(i).getCandidate2());
		}

		log.debug("STAT wu from " + gfd.getID() + ", wu-candidate1Size = " + candidates1.size());
		log.debug("STAT wu from " + gfd.getID() + ", wu-candidate2Size = " + candidates2.size());

		long start = System.currentTimeMillis();

		VF2IsomorphismEnumInspector<VertexString, TypedEdge, VertexOString, OrthogonalEdge> inspector1 = new VF2IsomorphismEnumInspector<VertexString, TypedEdge, VertexOString, OrthogonalEdge>(
				gfd.getPatterns().get(0), wu.getPivot1(), KB, candidates1);

		inspector1.setThreadNumLimit(inf.ed.gfd.util.KV.ISOMORPHISM_THREAD_LIMIT);
		List<Int2IntMap> results1 = inspector1.findIsomorphic();
		for (Int2IntMap match : results1) {
			if (!foundMatches1.containsKey(match.get(wu.getPivot1()))) {
				foundMatches1.put(match.get(wu.getPivot1()), new LinkedList<Int2IntMap>());
			}
			foundMatches1.get(match.get(wu.getPivot1())).add(match);
		}

		log.info("STAT- " + KV.DATASET + " - " + gfd.getID()
				+ " isomorphism for component 1 using " + (System.currentTimeMillis() - start)
				* 1.0 / 1000 + "s, for " + gfd.getCandidates().get(0).size() + " candidates.");
		log.info("STAT- enumerate results1 " + results1.size() + " matches.");

		start = System.currentTimeMillis();

		VF2IsomorphismEnumInspector<VertexString, TypedEdge, VertexOString, OrthogonalEdge> inspector2 = new VF2IsomorphismEnumInspector<VertexString, TypedEdge, VertexOString, OrthogonalEdge>(
				gfd.getPatterns().get(1), wu.getPivot2(), KB, candidates2);

		inspector2.setThreadNumLimit(inf.ed.gfd.util.KV.ISOMORPHISM_THREAD_LIMIT);
		List<Int2IntMap> results2 = inspector2.findIsomorphic();
		for (Int2IntMap match : results2) {
			if (!foundMatches2.containsKey(match.get(wu.getPivot2()))) {
				foundMatches2.put(match.get(wu.getPivot2()), new LinkedList<Int2IntMap>());
			}
			foundMatches2.get(match.get(wu.getPivot2())).add(match);
		}

		log.info("STAT- " + KV.DATASET + " - " + gfd.getID()
				+ " isomorphism for component 2 using " + (System.currentTimeMillis() - start)
				* 1.0 / 1000 + "s, for " + gfd.getCandidates().get(1).size() + " candidates.");
		log.info("STAT- enumerate results2 " + results2.size() + " matches.");

		ViolationResult vr = (ViolationResult) this.generatedResult;
		for (ReplicatedGWorkUnit w : workload) {
			if (foundMatches1.containsKey(w.getPivot1())
					&& foundMatches2.containsKey(w.getPivot2())) {
				List<Int2IntMap> violations = gfd.findViolationsIn2MatchList(
						foundMatches1.get(w.getPivot1()), foundMatches2.get(w.getPivot2()), KB);
				vr.addViolations(violations);
			}
		}
	}

	private void processConnectedWorkload(List<ReplicatedGWorkUnit> workload,
			Graph<VertexOString, OrthogonalEdge> KB) {

		if (workload.size() == 0) {
			return;
		}

		ReplicatedGWorkUnit wu = workload.get(0);

		GFD2 gfd = findGFDByID(wu.getGFDID());

		IntSet candidates = new IntOpenHashSet();

		for (int i = 0; i < workload.size(); i++) {
			candidates.add(workload.get(i).getCandidate1());
		}

		log.debug("STAT- wu from " + gfd.getID() + ", wu-candidateSize = " + candidates.size());

		long start = System.currentTimeMillis();

		VF2IsomorphismEnumInspector<VertexString, TypedEdge, VertexOString, OrthogonalEdge> inspector = new VF2IsomorphismEnumInspector<VertexString, TypedEdge, VertexOString, OrthogonalEdge>(
				gfd.getPattern(), wu.getPivot1(), KB, candidates);

		inspector.setThreadNumLimit(inf.ed.gfd.util.KV.ISOMORPHISM_THREAD_LIMIT);
		List<Int2IntMap> results = inspector.findIsomorphic();

		log.info("STAT- " + KV.DATASET + " - " + gfd.getID() + "isomorphism using "
				+ (System.currentTimeMillis() - start) * 1.0 / 1000 + "s, for "
				+ gfd.getCandidates().get(0).size() + " candidates.");

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
