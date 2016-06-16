package inf.ed.gfd.structure;

import inf.ed.gfd.util.KV;
import inf.ed.graph.structure.Graph;
import inf.ed.graph.structure.OrthogonalEdge;
import inf.ed.graph.structure.SimpleGraph;
import inf.ed.graph.structure.adaptor.TypedEdge;
import inf.ed.graph.structure.adaptor.VertexOString;
import inf.ed.graph.structure.adaptor.VertexString;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class GFD2 implements Serializable {

	private static final long serialVersionUID = 1L;

	/**
	 * also works as WorkUnit.
	 * */

	static Logger log = LogManager.getLogger(GFD2.class);

	private String ID;
	private int SCCNumber = 1;
	private ArrayList<Graph<VertexString, TypedEdge>> patterns;
	private Int2ObjectMap<IntSet> candidates;
	private Condition condition;
	private int radius = 0;
	public IntSet labelSet;

	private Int2IntMap candidate2et;

	public String getID() {
		return ID;
	}

	public void setID(String origin) {
		this.ID = origin;
	}

	public int getRadius() {
		if (radius == 0) {
			radius = patterns.get(0).getRadius(0);
		}
		return radius;
	}

	public GFD2() {

		this.patterns = new ArrayList<Graph<VertexString, TypedEdge>>();
		this.candidates = new Int2ObjectOpenHashMap<IntSet>();
		this.candidate2et = new Int2IntOpenHashMap();

		this.patterns.add(new SimpleGraph<VertexString, TypedEdge>(VertexString.class,
				TypedEdge.class));
		this.patterns.add(new SimpleGraph<VertexString, TypedEdge>(VertexString.class,
				TypedEdge.class));

		labelSet = new IntOpenHashSet();

		this.condition = new Condition();
	}

	public boolean isConnected() {
		return this.SCCNumber == 1;
	}

	public Graph<VertexString, TypedEdge> getPattern() {
		assert this.SCCNumber == 1;
		return this.patterns.get(0);
	}

	public ArrayList<Graph<VertexString, TypedEdge>> getPatterns() {
		assert this.SCCNumber == 2;
		return this.patterns;
	}

	public Int2ObjectMap<IntSet> getCandidates() {
		return this.candidates;
	}

	public Int2IntMap getCandidate2ET() {
		return this.candidate2et;
	}

	public int verify(List<Int2IntMap> matches, Graph<VertexOString, OrthogonalEdge> KB) {
		int violationCount = 0;
		for (Int2IntMap match : matches) {
			if (!this.condition.verify(match, KB)) {
				// log.debug("find a violation: " + match.toString());
				violationCount++;
			}
		}
		return violationCount;
	}

	public List<Int2IntMap> findViolations(List<Int2IntMap> matches,
			Graph<VertexOString, OrthogonalEdge> KB) {
		List<Int2IntMap> violations = new LinkedList<Int2IntMap>();
		for (Int2IntMap match : matches) {
			if (!this.condition.verify(match, KB)) {
				violations.add(match);
			}
		}
		return violations;
	}

	public boolean isViolation(Int2IntMap match, Graph<VertexOString, OrthogonalEdge> KB) {
		return !this.condition.verify(match, KB);
	}

	public int verify2MatchList(List<Int2IntMap> matches1, List<Int2IntMap> matches2,
			Graph<VertexOString, OrthogonalEdge> KB) {
		int violationCount = 0;
		for (Int2IntMap match1 : matches1) {
			for (Int2IntMap match2 : matches2) {
				Int2IntMap match = new Int2IntOpenHashMap();
				match.putAll(match1);
				match.putAll(match2);
				if (!this.condition.verify(match, KB)) {
					// log.debug("find a violation: " + match.toString());
					violationCount++;
				}
			}
		}
		return violationCount;
	}

	public List<Int2IntMap> findViolationsIn2MatchList(List<Int2IntMap> matches1,
			List<Int2IntMap> matches2, Graph<VertexOString, OrthogonalEdge> KB) {
		List<Int2IntMap> violations = new LinkedList<Int2IntMap>();
		for (Int2IntMap match1 : matches1) {
			for (Int2IntMap match2 : matches2) {
				Int2IntMap match = new Int2IntOpenHashMap();
				match.putAll(match1);
				match.putAll(match2);
				if (!this.condition.verify(match, KB)) {
					violations.add(match);
				}
			}
		}
		return violations;
	}

	public boolean verify2Candidate(int cand1, int cand2, Graph<VertexOString, OrthogonalEdge> KB) {
		// cand1 attribute should equals cand2 attribute.
		return KB.getVertex(cand1).getAttr().equals(KB.getVertex(cand2).getAttr());
	}

	public int findCandidates(Graph<VertexOString, OrthogonalEdge> KB) {
		int ret = 0;
		for (int vertexID : KB.allVertices().keySet()) {
			if (this.candidates.get(0).contains(vertexID)) {
				ret++;
			}
		}
		return ret;
	}


	// load GFD from file
	public void readFromFile(String filename) {

		log.info("GFDFile: " + filename);
		File file = new File(filename);
		LineIterator it;
		String flag = "";
		int candKey = 0;

		try {
			it = FileUtils.lineIterator(file, "UTF-8");
			while (it.hasNext()) {
				String line = it.nextLine();
				if (line.startsWith("#") || line.startsWith("*")) {
					continue;
				} else if (line.startsWith(KV.PTN_INPUT_FLAG_CAND)) {
					flag = KV.PTN_INPUT_FLAG_CAND;
					candKey = Integer.parseInt(line.split("\t")[1].trim());
					candidates.put(candKey, new IntOpenHashSet());
					continue;
				} else if (line.startsWith("%")) {
					// start with % is a input flag
					flag = line.trim();
					continue;
				}
				if (flag.equals(KV.PTN_INPUT_FLAG_CCNUM)) {
					this.SCCNumber = Integer.parseInt(line.trim().split("\t")[0]);
				}
				if (flag.equals(KV.PTN_INPUT_FLAG_PTN1)) {
					parseLineToPattern(0, line);
				}
				if (flag.equals(KV.PTN_INPUT_FLAG_PTN2)) {
					parseLineToPattern(1, line);
				}
				if (flag.equals(KV.PTN_INPUT_FLAG_CONX)) {
					condition.addCondition(KV.XCONDITION, line);
				}
				if (flag.equals(KV.PTN_INPUT_FLAG_CONY)) {
					condition.addCondition(KV.YCONDITION, line);
				}
				if (flag.equals(KV.PTN_INPUT_FLAG_CAND)) {
//					if (candidates.get(candKey).size() < KV.CANDIDATES_LIMIT) {
						String[] ele = line.trim().split("\t");
						int candidate = Integer.parseInt(ele[0]);
						candidates.get(candKey).add(candidate);
						if (ele.length == 2) {
							int estimated = Integer.parseInt(ele[1]);
							candidate2et.put(candidate, estimated);
						}
//					}
				}
			}
			LineIterator.closeQuietly(it);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void parseLineToPattern(int patternIndex, String line) {
		if (line.startsWith("v")) {
			// add vertex
			VertexString v = new VertexString(line);
			this.patterns.get(patternIndex).addVertex(v);
		}

		else if (line.startsWith("e")) {
			// add edge
			String[] elements = line.split("\t");
			VertexString source = patterns.get(patternIndex).getVertex(
					Integer.parseInt(elements[1].trim()));
			VertexString target = patterns.get(patternIndex).getVertex(
					Integer.parseInt(elements[3].trim()));
			TypedEdge e;
			if (!patterns.get(patternIndex).contains(source, target)) {
				e = patterns.get(patternIndex).addEdge(source, target);
			} else {
				e = patterns.get(patternIndex).getEdge(source, target);
			}
			int label = Integer.parseInt(elements[2].trim());
			e.setAttr(label);
			labelSet.add(label);
		}
	}
}
