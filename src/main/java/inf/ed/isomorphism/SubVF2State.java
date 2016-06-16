package inf.ed.isomorphism;

import inf.ed.graph.structure.Edge;
import inf.ed.graph.structure.Graph;
import inf.ed.graph.structure.Vertex;
import inf.ed.graph.structure.adaptor.Pair;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * A State implementation for testing isomorphism using the VF2 algorithm's
 * logic.
 * 
 * TODO: Do not require Q has continuous index. but check from smaller index to
 * large one.
 * 
 * TODO: Support parameter to guide it "find a match then stop" or enumerate all
 * matches.
 * 
 */
@SuppressWarnings("rawtypes")
public class SubVF2State<V1 extends Vertex, E1 extends Edge, V2 extends Vertex, E2 extends Edge>
		implements State {

	static Logger log = LogManager.getLogger(SubVF2State.class);

	/**
	 * The query graph
	 */
	private Graph<V1, E1> g1;

	/**
	 * The base graph (usually large)
	 */
	private Graph<V2, E2> g2;

	/**
	 * The number of nodes currently being matched between g1 and g2
	 */
	int coreLen;

	/**
	 * The number of nodes that were matched prior to this current pair being
	 * added, which is used in backtracking.
	 */
	int origCoreLen;

	// State information
	int t1bothLen, t2bothLen, t1inLen, t1outLen, t2inLen, t2outLen; // Core
																	// nodes are
																	// also
																	// counted
																	// by
																	// these...

	Int2IntMap core1;
	Int2IntMap core2;
	Int2IntMap in1;
	Int2IntMap in2;
	Int2IntMap out1;
	Int2IntMap out2;

	/**
	 * The seen node in g2 and its key.
	 */
	IntList sn2;
	IntSet sn2k;

	int v1 = 0;

	/**
	 * The node in g1 that was most recently added.
	 */
	int addedNode1;
	/**
	 * The seen node in g2 that was most recently added.
	 */
	IntList addedSeenNode2;

	/**
	 * The number of nodes in {@code g1}
	 */
	private final int n1;

	/**
	 * largest vertexID in g1.
	 */
	private final int l1;

	/**
	 * Whether the algorithm needs to check for edge number constraints on the
	 * graph's edges. This is stored as a global to avoid recomputing it each
	 * time {@code areEdgesCompatible} is called (a hot spot), when it is
	 * already known at state construction time.
	 */
	private final boolean checkMultiplexEdges;

	/**
	 * Creates a new {@code VF2State} with an empty mapping between the two
	 * graphs.
	 */
	public SubVF2State(Graph<V1, E1> g1, int v1, Graph<V2, E2> g2, int v2) {
		this.g1 = g1;
		this.g2 = g2;

		this.checkMultiplexEdges = false;

		n1 = g1.vertexSize();
		l1 = getLargestVertexID(g1);

		coreLen = 0;
		origCoreLen = 0;
		t1bothLen = 0;
		t1inLen = 0;
		t1outLen = 0;
		t2bothLen = 0;
		t2inLen = 0;
		t2outLen = 0;

		addedNode1 = NULL_NODE;

		core1 = new Int2IntOpenHashMap();
		core2 = new Int2IntOpenHashMap();
		in1 = new Int2IntOpenHashMap();
		in2 = new Int2IntOpenHashMap();
		out1 = new Int2IntOpenHashMap();
		out2 = new Int2IntOpenHashMap();

		sn2 = new IntArrayList();
		addedSeenNode2 = new IntArrayList();
		sn2k = new IntOpenHashSet();

		sn2.add(v2);
		sn2k.add(v2);

		this.v1 = v1;
	}

	protected SubVF2State(SubVF2State copy) {
		checkMultiplexEdges = copy.checkMultiplexEdges;
		g1 = copy.g1;
		g2 = copy.g2;
		coreLen = copy.coreLen;
		origCoreLen = copy.origCoreLen;
		t1bothLen = copy.t1bothLen;
		t2bothLen = copy.t2bothLen;
		t1inLen = copy.t1inLen;
		t2inLen = copy.t2inLen;
		t1outLen = copy.t1outLen;
		t2outLen = copy.t2outLen;
		n1 = copy.n1;
		l1 = copy.l1;

		sn2 = copy.sn2;
		sn2k = copy.sn2k;

		addedNode1 = NULL_NODE;
		addedSeenNode2 = new IntArrayList();

		// NOTE: we don't need to copy these arrays because their state restored
		// via the backTrack() function after processing on the cloned state
		// finishes
		core1 = copy.core1;
		core2 = copy.core2;
		in1 = copy.in1;
		in2 = copy.in2;
		out1 = copy.out1;
		out2 = copy.out2;
		v1 = copy.v1;
	}

	/**
	 * {@inheritDoc}
	 */
	public Pair<Integer> nextPair(int prevN1, int prevN2) {

		// log.debug(sn2.toString());
		//
		// log.debug("next pair,prevN1=" + prevN1 + ",prevN2=" + prevN2 +
		// " ,current sn2.size = "
		// + sn2.size());

		if (prevN1 == NULL_NODE)
			prevN1 = v1;

		if (prevN2 == NULL_NODE)
			prevN2 = 0;

		else {
			prevN2 = sn2.indexOf(prevN2);
			prevN2++;
			while (prevN2 < sn2.size()
					&& (core2.containsKey(sn2.get(prevN2)) || !out2.containsKey(sn2.get(prevN2)))
					&& (core2.containsKey(sn2.get(prevN2)) || !in2.containsKey(sn2.get(prevN2)))) {
				prevN2++;
			}
		}

		if (t1bothLen > coreLen && t2bothLen > coreLen) {
			while (prevN1 < l1
					&& (core1.containsKey(prevN1) || !out1.containsKey(prevN1) || !in1
							.containsKey(prevN1))) {
				prevN1++;
				prevN2 = 0;
			}
			while (prevN2 < sn2.size()
					&& (core2.containsKey(sn2.get(prevN2)) || !out2.containsKey(sn2.get(prevN2)) || !in2
							.containsKey(sn2.get(prevN2)))) {
				prevN2++;
			}
		} else if (t1outLen > coreLen && t2outLen > coreLen) {
			while (prevN1 < l1 && (core1.containsKey(prevN1) || !out1.containsKey(prevN1))) {
				prevN1++;
				prevN2 = 0;
			}
			while (prevN2 < sn2.size()
					&& (core2.containsKey(sn2.get(prevN2)) || !out2.containsKey(sn2.get(prevN2)))) {
				prevN2++;
			}

		} else if (t1inLen > coreLen && t2inLen > coreLen) {
			while (prevN1 < l1 && (core1.containsKey(prevN1) || !in1.containsKey(prevN1))) {
				prevN1++;
				prevN2 = 0;
			}
			while (prevN2 < sn2.size()
					&& (core2.containsKey(sn2.get(prevN2)) || !in2.containsKey(sn2.get(prevN2)))) {
				prevN2++;
			}
		} else {
			// log.error("should not come into this for s1.");
			while (prevN1 < l1 && core1.containsKey(prevN1)) {
				prevN1++;
				prevN2 = 0;
			}
			while (prevN2 < sn2.size() && core2.containsKey(sn2.get(prevN2))) {
				prevN2++;
			}
		}

		if (prevN1 < l1 && prevN2 < sn2.size()) {
			//
			// log.debug("prevN1=" + prevN1 + " prevN2=" + prevN2 + ", realID,"
			// + prevN1 + ":"
			// + sn2.get(prevN2));
			return new Pair<Integer>(prevN1, sn2.get(prevN2));
		} else
			return null;

	}

	protected boolean areCompatibleEdges(int v1, int v2, int v3, int v4) {
		// If either g1 or g2 is multigraph, then we need to check the number
		// of edges
		// if (checkMultiplexEdges) {
		// Set<? extends Edge> e1 = g1.getEdges(v1, v2);
		// Set<? extends Edge> e2 = g2.getEdges(v3, v4);
		// return e1.size() == e2.size();
		// }
		E1 e1 = g1.getEdge(v1, v2);
		E2 e2 = g2.getEdge(v3, v4);
		return e2.match(e1);
	}

	protected boolean areCompatableVertices(int v1, int v2) {
		return g1.getVertex(v1).match(g2.getVertex(v2));
	}

	/**
	 * {@inheritDoc}
	 */
	public boolean isFeasiblePair(int node1, int node2) {

		assert node1 < l1;
		assert !core1.containsKey(node1);

		if (!areCompatableVertices(node1, node2)) {
			return false;
		}

		int termout1 = 0, termout2 = 0, termin1 = 0, termin2 = 0, new1 = 0, new2 = 0;

		// Check the 'out' edges of node1
		for (int other1 : g1.getChildren(node1)) {
			if (core1.containsKey(other1)) {
				int other2 = core1.get(other1);
				// If there's node edge to the other node, or if there is some
				// edge incompatibility, then the mapping is not feasible
				if (!g2.contains(node2, other2)
						|| !areCompatibleEdges(node1, other1, node2, other2)) {
					return false;
				}
			} else {
				if (in1.containsKey(other1))
					termin1++;
				if (out1.containsKey(other1))
					termout1++;
				if ((!in1.containsKey(other1)) && (!out1.containsKey(other1)))
					new1++;
			}
		}

		// Check the 'in' edges of node1
		for (int other1 : g1.getParents(node1)) {
			if (core1.containsKey(other1)) {
				int other2 = core1.get(other1);
				// If there's node edge to the other node, or if there is some
				// edge incompatibility, then the mapping is not feasible
				if (!g2.contains(other2, node2)
						|| !areCompatibleEdges(other1, node1, other2, node2)) {
					return false;
				}
			} else {
				if (in1.containsKey(other1))
					termin1++;
				if (out1.containsKey(other1))
					termout1++;
				if ((!in1.containsKey(other1)) && (!out1.containsKey(other1)))
					new1++;
			}
		}

		// Check the 'out' edges of node2
		for (int other2 : g2.getChildren(node2)) {
			// don't need to check this, since not require deduced subgraph.
			// if (core2.containsKey(other2)) {
			// int other1 = core2.get(other2);
			// if (!g1.contains(node1, other1))
			// return false;
			// } else {
			if (in2.containsKey(other2))
				termin2++;
			if (out2.containsKey(other2))
				termout2++;
			if ((!in2.containsKey(other2)) && (!out2.containsKey(other2)))
				new2++;
			// }
		}

		// Check the 'in' edges of node2
		for (int other2 : g2.getParents(node2)) {
			// don't need to check this, since not require deduced subgraph.
			// if (core2.containsKey(other2)) {
			// int other1 = core2.get(other2);
			// if (!g1.contains(other1, node1))
			// return false;
			// }
			//
			// else {
			if (in2.containsKey(other2))
				termin2++;
			if (out2.containsKey(other2))
				termout2++;
			if ((!in2.containsKey(other2)) && (!out2.containsKey(other2)))
				new2++;
			// }
		}

		return termin1 <= termin2 && termout1 <= termout2 && new1 <= new2;
	}

	/**
	 * {@inheritDoc}
	 */
	public void addPair(int node1, int node2) {

		assert node1 < l1;
		assert coreLen < n1;

		coreLen++;
		addedNode1 = node1;

		if (!in1.containsKey(node1)) {
			in1.put(node1, coreLen);
			t1inLen++;
			if (out1.containsKey(node1))
				t1bothLen++;
		}
		if (!out1.containsKey(node1)) {
			out1.put(node1, coreLen);
			t1outLen++;
			if (in1.containsKey(node1))
				t1bothLen++;
		}

		if (!in2.containsKey(node2)) {
			in2.put(node2, coreLen);
			t2inLen++;
			if (out2.containsKey(node2))
				t2bothLen++;
		}
		if (!out2.containsKey(node2)) {
			out2.put(node2, coreLen);
			t2outLen++;
			if (in2.containsKey(node2))
				t2bothLen++;
		}

		core1.put(node1, node2);
		core2.put(node2, node1);

		for (int other : g1.getParents(node1)) {
			if (!in1.containsKey(other)) {
				in1.put(other, coreLen);
				t1inLen++;
				if (out1.containsKey(other))
					t1bothLen++;
			}
		}

		for (int other : g1.getChildren(node1)) {
			if (!out1.containsKey(other)) {
				out1.put(other, coreLen);
				t1outLen++;
				if (in1.containsKey(other))
					t1bothLen++;
			}
		}

		for (int other : g2.getParents(node2)) {
			if (!in2.containsKey(other)) {
				in2.put(other, coreLen);
				t2inLen++;
				if (out2.containsKey(other))
					t2bothLen++;
			}
			if (!sn2k.contains(other)) {
				addedSeenNode2.add(other);
				sn2k.add(other);
			}
		}

		for (int other : g2.getChildren(node2)) {
			if (!out2.containsKey(other)) {
				out2.put(other, coreLen);
				t2outLen++;
				if (in2.containsKey(other))
					t2bothLen++;
			}
			if (!sn2k.contains(other)) {
				addedSeenNode2.add(other);
				sn2k.add(other);
			}
		}

		sn2.addAll(addedSeenNode2);

	}

	/**
	 * {@inheritDoc}
	 */
	public boolean isGoal() {
		return coreLen == n1;
	}

	/**
	 * {@inheritDoc}
	 */
	public boolean isDead() {
		return t1bothLen > t2bothLen || t1outLen > t2outLen || t1inLen > t2inLen;
	}

	/**
	 * {@inheritDoc}
	 */
	public SubVF2State copy() {
		return new SubVF2State(this);
	}

	/**
	 * {@inheritDoc}
	 */
	public void backTrack() {

		assert addedNode1 != NULL_NODE;

		// log.info("backtrack:" + addedNode1 + ":" + core1.get(addedNode1));
		if (origCoreLen < coreLen) {

			int node2;

			if (in1.get(addedNode1) == coreLen)
				in1.remove(addedNode1);

			for (int other : g1.getParents(addedNode1)) {
				if (in1.get(other) == coreLen)
					in1.remove(other);
			}

			if (out1.get(addedNode1) == coreLen)
				out1.remove(addedNode1);

			for (int other : g1.getChildren(addedNode1)) {
				if (out1.get(other) == coreLen)
					out1.remove(other);
			}

			node2 = core1.get(addedNode1);

			if (in2.get(node2) == coreLen)
				in2.remove(node2);

			for (int other : g2.getParents(node2)) {
				if (in2.get(other) == coreLen)
					in2.remove(other);
			}

			if (out2.get(node2) == coreLen)
				out2.remove(node2);

			for (int other : g2.getChildren(node2)) {
				if (out2.get(other) == coreLen)
					out2.remove(other);
			}

			core1.remove(addedNode1);
			core2.remove(node2);

			sn2.removeAll(addedSeenNode2);
			sn2k.removeAll(addedSeenNode2);

			coreLen = origCoreLen;
			addedNode1 = NULL_NODE;
			addedSeenNode2.clear();
		}
	}

	@Override
	public void nextN1() {
		throw new IllegalArgumentException("VF2 State not support iterate N1 alone.");
	}

	@Override
	public Int2IntMap getMatch() {
		return this.core1;
	}

	private int getLargestVertexID(Graph<V1, E1> g) {
		int max = 0;
		for (int vertexID : g.allVertices().keySet()) {
			max = max > vertexID ? max : vertexID;
		}
		return ++max;
	}

	@Override
	public int curN1() {
		throw new IllegalArgumentException("VF2 State not support iterate N1 alone.");
	}
}
