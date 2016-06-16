package inf.ed.graph.structure;

import java.io.Serializable;

public abstract class OrthogonalVertex implements Vertex, Serializable {

	private static final long serialVersionUID = 1L;
	protected int id; // node ID
	protected OrthogonalEdge firstin; // first in node
	protected OrthogonalEdge firstout; // first out node

	public OrthogonalEdge GetFirstIn() {
		return this.firstin;
	}

	public OrthogonalEdge GetFirstOut() {
		return this.firstout;
	}

	public void SetID(int ID) {
		this.id = ID;
	}

	public void SetFirstIn(OrthogonalEdge firstin) {
		this.firstin = firstin;
	}

	public void SetFirstOut(OrthogonalEdge firstout) {
		this.firstout = firstout;
	}

	public int getID() {
		return this.id;
	}

	public abstract int hashCode();

	public abstract boolean equals(Object other);

	public abstract OrthogonalVertex copyWithoutEdge();
}