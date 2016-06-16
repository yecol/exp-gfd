package inf.ed.gfd.structure;

import inf.ed.gfd.util.KV;
import inf.ed.gfd.util.Params;
import inf.ed.graph.structure.Graph;
import inf.ed.graph.structure.OrthogonalEdge;
import inf.ed.graph.structure.adaptor.VertexOString;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;

import java.io.Serializable;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class WorkUnit implements Serializable, Comparable<WorkUnit> {
	/**
		 * 
		 */
	private static final long serialVersionUID = 1L;

	public int ID;
	public boolean isConnected;
	public String gfdID;
	public GFD2 gfd;
	public Ball ball1;
	public Ball ball2;

	// Pivot node
	public int pivot1;
	public int pivot2;

	// candidate for pivot in this work unit
	public int candidate1;
	public int candidate2;

	// estimated evaluation cost
	private int et = 0;

	// estimated communication cost
	private int ec = 0;

	// generated in partition;
	public int originParititon;
	public Int2IntMap partition2CC;
	public IntSet borderNodesInGw1;
	public IntSet borderNodesInGw2;
	public Set<CrossingEdge> transferCrossingEdge;
	public Int2ObjectMap<IntSet> prefetchRequest;

	static Logger log = LogManager.getLogger(WorkUnit.class);

	public void estimateCC(int partitionCount, Int2IntMap allBorderNodeBallSize) {
		partition2CC = new Int2IntOpenHashMap();
		// cost measures as nodes + edges
		// local-cost : compute in current partition, only need to fetch border
		// nodes;
		// other-cost : compute in other partition, fetch borders and the whole
		// Gw graph;
		int localcost = 0, othercost = 0;

		for (int borderNode : borderNodesInGw1) {
			localcost += allBorderNodeBallSize.get(borderNode);
		}
		othercost = localcost + ball1.vertices.size() + ball1.edges.size();

		for (int partitionID = 0; partitionID < partitionCount; partitionID++) {
			if (partitionID == this.originParititon) {
				partition2CC.put(partitionID, localcost);
			}
			partition2CC.put(partitionID, othercost);
		}
	}

	private int determineBorderNodeInWhichPartition(int vertexID,
			Int2ObjectMap<IntSet> allBorderVertices) {
		for (int partitionID : allBorderVertices.keySet()) {
			if (allBorderVertices.get(partitionID).contains(vertexID)) {
				return partitionID;
			}
		}
		return -1;
	}

	public void buildPrefetchRequest(Int2ObjectMap<String> allVertices,
			Int2ObjectMap<Set<CrossingEdge>> mapBorderNodesAsSource,
			Int2ObjectMap<Set<CrossingEdge>> mapBorderNodesAsTarget,
			Int2ObjectMap<IntSet> allBorderVertices) {

		// refined: only border node with r>=2 need to transfer ball from other
		// fragment.
		prefetchRequest = new Int2ObjectOpenHashMap<IntSet>();
		transferCrossingEdge = new ObjectOpenHashSet<CrossingEdge>();

		// refined: limit crossing edge as incoming edge.
		for (int borderNode : this.borderNodesInGw1) {
			if (mapBorderNodesAsSource.containsKey(borderNode)) {
				for (CrossingEdge e : mapBorderNodesAsSource.get(borderNode)) {
					e.sourceLabel = allVertices.get(e.source);
					e.targetLabel = allVertices.get(e.target);
					transferCrossingEdge.add(e);

					if (this.gfd.getRadius() >= 2) {
						int partitionOfOtherNode = determineBorderNodeInWhichPartition(e.target,
								allBorderVertices);
						if (!prefetchRequest.containsKey(partitionOfOtherNode)) {
							prefetchRequest.put(partitionOfOtherNode, new IntOpenHashSet());
						}
						prefetchRequest.get(partitionOfOtherNode).add(e.target);
					}
				}
			}

			if (mapBorderNodesAsTarget.containsKey(borderNode)) {
				int nodeAsTarget = 0;
				for (CrossingEdge e : mapBorderNodesAsTarget.get(borderNode)) {
					nodeAsTarget++;
					if (nodeAsTarget > KV.NODE_AS_TARGET_EDGE_LIMIT) {
//						log.info("nodeID:" + borderNode + " exceeds edge limit = "
//								+ KV.NODE_AS_TARGET_EDGE_LIMIT);
						break;
					}
					e.sourceLabel = allVertices.get(e.source);
					e.targetLabel = allVertices.get(e.target);
					transferCrossingEdge.add(e);

					if (this.gfd.getRadius() >= 2) {
						int partitionOfOtherNode = determineBorderNodeInWhichPartition(e.source,
								allBorderVertices);
						if (!prefetchRequest.containsKey(partitionOfOtherNode)) {
							prefetchRequest.put(partitionOfOtherNode, new IntOpenHashSet());
						}
						prefetchRequest.get(partitionOfOtherNode).add(e.source);
					}
				}
			}
		}

		// log.debug("Gw_border = " + this.borderNodesInGw1 +
		// ", trans crossing="
		// + transferCrossingEdge.size());
	}

	public void computeAndSetBorderNodes(IntSet borderNodes) {
		// for connected
		this.borderNodesInGw1 = new IntOpenHashSet();
		for (int node : ball1.vertices.keySet()) {
			if (borderNodes.contains(node)) {
				this.borderNodesInGw1.add(node);
			}
		}
		// TODO disconnected
	}

	public WorkUnit(int ID, String gfdID, int candidate1, int generateInPartition) {
		this.ID = ID;
		this.gfdID = gfdID;
		this.candidate1 = candidate1;
		this.originParititon = generateInPartition;
	}

	public WorkUnit(int ID, int et) {
		// for bPar test
		this.ID = ID;
		this.et = et;
	}

	public boolean isConnected() {
		return this.isConnected;
	}

	public WorkUnit(String gfdID, int pivot, int candidate, int et) {
		// for distributed setting 1, connected one.
		this.isConnected = true;
		this.gfdID = gfdID;
		this.pivot1 = pivot;
		this.candidate1 = candidate;
		this.et = et;
	}

	public WorkUnit(String gfdID, int pivot1, int candidate1, int pivot2, int candidate2, int et1,
			int et2) {
		// for distributed setting 1, disconnected one.
		this.isConnected = false;
		this.gfdID = gfdID;
		this.pivot1 = pivot1;
		this.pivot2 = pivot2;
		this.candidate1 = candidate1;
		this.candidate2 = candidate2;
		this.et = et1 + et2;
	}

	@Override
	public String toString() {
		return "WU [ID=" + ID + ", c=" + isConnected + ", gfdID=" + gfdID + ", p1=" + pivot1
				+ ", p2=" + pivot2 + ", cand1=" + candidate1 + ", cand2=" + candidate2 + ", et="
				+ et + ", ec=" + ec + "]";
	}

	@Override
	public int compareTo(WorkUnit o) {
		return this.et - o.et;
	}
}
