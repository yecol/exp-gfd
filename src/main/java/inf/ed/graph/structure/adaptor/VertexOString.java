package inf.ed.graph.structure.adaptor;

import inf.ed.graph.structure.OrthogonalEdge;
import inf.ed.graph.structure.OrthogonalVertex;

import java.io.Serializable;

/**
 * Orthogonal Vertex with String label.
 * 
 * @author yecol
 *
 */
public class VertexOString extends OrthogonalVertex implements Serializable {

	private static final long serialVersionUID = 1L;
	String attr;

	public VertexOString(int id) {
		this.id = id;
	}

	public VertexOString(VertexOString copy, boolean copyEdge) {
		this.id = copy.id;
		this.attr = copy.attr;
		if (copyEdge) {
			this.firstin = copy.firstin;
			this.firstout = copy.firstout;
		}
	}

	public VertexOString(String line) {
		int p = line.indexOf("\t");
		this.id = Integer.parseInt(line.substring(0, p).trim());
		this.attr = line.substring(p).trim();
	}

	public VertexOString(int id, String attr) {
		this.id = id;
		this.attr = attr;
	}

	public VertexOString(int id, String attr, OrthogonalEdge firstin, OrthogonalEdge firstout) {
		this.id = id;
		this.attr = attr;
		this.firstin = firstin;
		this.firstout = firstout;
	}

	public String getAttr() {
		return this.attr;
	}

	public void setAttr(String attr) {
		this.attr = attr;
	}

	@Override
	public int hashCode() {
		int result = String.valueOf(this.getID()).hashCode();
		result = 29 * result + attr.hashCode();
		return result;
	}

	@Override
	public boolean equals(Object other) {
		final VertexOString v = (VertexOString) other;
		return v.getID() == this.getID();
	}

	public boolean match(Object other) {
		final VertexOString o = (VertexOString) other;
		return o.attr.equals(this.attr);
	}

	@Override
	public String toString() {
		return "VOStr [ID=" + this.id + ", attr=" + this.attr + "]";
	}

	@Override
	public OrthogonalVertex copyWithoutEdge() {
		return new VertexOString(this, false);
	}
}
