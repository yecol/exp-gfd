package inf.ed.gfd.detect.repl;

import inf.ed.gfd.structure.GFD2;
import inf.ed.graph.structure.Graph;
import inf.ed.graph.structure.OrthogonalEdge;
import inf.ed.graph.structure.adaptor.VertexOString;
import inf.ed.isomorphism.Simulation;

import java.io.Serializable;
import java.util.List;

public class ReplicatedGWorkUnit implements Serializable, Comparable<ReplicatedGWorkUnit> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private int ID;
	private boolean isConnected;
	private String gfdID;
	private transient Graph<VertexOString, OrthogonalEdge> gw1;
	private transient Graph<VertexOString, OrthogonalEdge> gw2;

	// Pivot node
	private int pivot;

	private int pivot2;

	// candidate for pivot in this work unit
	private int candidate;

	private int candidate2;

	// estimated evaluation cost
	private int et = 0;

	public String getCategory() {
		return gfdID + "-" + pivot;
	}

	public String getGFDID() {
		return this.gfdID;
	}

	private GFD2 findGFDByID(List<GFD2> gfds, String pGfdID) {
		for (GFD2 gfd : gfds) {
			if (gfd.getID().equals(pGfdID)) {
				return gfd;
			}
		}
		return null;
	}

	public void estimateET(Graph<VertexOString, OrthogonalEdge> graph, List<GFD2> gfds) {

		GFD2 gfd = findGFDByID(gfds, gfdID);
		gw1 = graph.getSubgraph(VertexOString.class, OrthogonalEdge.class,
				graph.getVertex(candidate), gfd.getRadius(), gfd.labelSet);

		et += gw1.edgeSize() + gw1.vertexSize();

		// if (Simulation.match(gw1, gfd.getPattern())) {
		// et += gw1.edgeSize() + gw1.vertexSize();
		// }

		if (!this.isConnected()) {
			gw2 = graph.getSubgraph(VertexOString.class, OrthogonalEdge.class,
					graph.getVertex(candidate2), gfd.getRadius(), gfd.labelSet);

			// if (Simulation.match(gw1, gfd.getPattern())) {
			et += gw2.edgeSize() + gw2.vertexSize();
			// }
		}
	}

	public int getET() {
		return this.et;
	}

	public void setET(int et) {
		this.et = et;
	}

	public void setID(int ID) {
		this.ID = ID;
	}

	public int getID() {
		return this.ID;
	}

	public int getPivot1() {
		return this.pivot;
	}

	public int getPivot2() {
		return this.pivot2;
	}

	public int getCandidate1() {
		return this.candidate;
	}

	public int getCandidate2() {
		return this.candidate2;
	}

	public ReplicatedGWorkUnit(int ID, int et) {
		// for bPar test
		this.ID = ID;
		this.et = et;
	}

	public boolean isConnected() {
		return this.isConnected;
	}

	public ReplicatedGWorkUnit(String gfdID, int pivot, int candidate) {
		// for distributed setting 1, connected one.
		this.isConnected = true;
		this.gfdID = gfdID;
		this.pivot = pivot;
		this.candidate = candidate;
	}

	public ReplicatedGWorkUnit(String gfdID, int pivot1, int candidate1, int pivot2, int candidate2) {
		// for distributed setting 1, disconnected one.
		this.isConnected = false;
		this.gfdID = gfdID;
		this.pivot = pivot1;
		this.pivot2 = pivot2;
		this.candidate = candidate1;
		this.candidate2 = candidate2;
	}

	@Override
	public String toString() {
		return "WU [ID=" + ID + ", c=" + isConnected + ", gfdID=" + gfdID + ", p1=" + pivot
				+ ", p2=" + pivot2 + ", cand1=" + candidate + ", cand2=" + candidate2 + ", et="
				+ et + "]";
	}

	@Override
	public int compareTo(ReplicatedGWorkUnit o) {
		return this.et - o.getET();
	}

}
