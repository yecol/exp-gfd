package inf.ed.graph.structure;

import inf.ed.gfd.util.KV;
import inf.ed.graph.structure.adaptor.TypedEdge;
import it.unimi.dsi.fastutil.ints.IntSet;

import java.io.Serializable;

public class OrthogonalEdge implements Edge, Serializable {

	/**
	 * With the help of head link and tail link, it is easy to find an arc
	 * headed/tailed on a certain vertex.
	 */

	private static final long serialVersionUID = 1L;
	private OrthogonalVertex fromNode;
	private OrthogonalVertex toNode;
	private OrthogonalEdge hlink;// head link
	private OrthogonalEdge tlink;// tail link

	private int[] attrs = { -1, -1, -1 };
	public int attrCount = 0;

	public OrthogonalEdge() {
	}

	public OrthogonalEdge(OrthogonalVertex fromNode, OrthogonalVertex toNode, OrthogonalEdge hlink,
			OrthogonalEdge tlink) {
		this.fromNode = fromNode;
		this.toNode = toNode;
		this.hlink = hlink;
		this.tlink = tlink;
	}

	public OrthogonalEdge GetHLink() {
		return this.hlink;
	}

	public OrthogonalEdge GetTLink() {
		return this.tlink;
	}

	public void SetFromNode(OrthogonalVertex fnode) {
		this.fromNode = fnode;
	}

	public void SetToNode(OrthogonalVertex tnode) {
		this.toNode = tnode;
	}

	public void SetHLink(OrthogonalEdge hlink) {
		this.hlink = hlink;
	}

	public void SetTLink(OrthogonalEdge tlink) {
		this.tlink = tlink;
	}

	public boolean equals(Object other) {
		if (other == null)
			return false;
		if (!(other instanceof OrthogonalEdge))
			return false;

		final OrthogonalEdge e = (OrthogonalEdge) other;
		if (!this.fromNode.equals(e.from()) || !this.toNode.equals(e.to())) {
			return false;
		}
		return true;
	}

	public int hashcode() {
		int result = String.valueOf(this.fromNode).hashCode()
				+ String.valueOf(this.toNode).hashCode();
		return result;
	}

	public OrthogonalVertex from() {
		return this.fromNode;
	}

	public OrthogonalVertex to() {
		return this.toNode;
	}

	public void setAttr(int attr) {
		// TODO
		if (this.attrCount < KV.ATTR_LIMIT) {
			this.attrs[attrCount] = attr;
			attrCount++;
		}
	}

	public int[] getAttr() {
		return this.attrs;
	}

	public void copyAttr(OrthogonalEdge e) {
		for (int i = 0; i < e.attrCount; i++) {
			this.attrs[i] = e.getAttr()[i];
			attrCount++;
		}
	}

	public String getAttrString() {
		String s = "[";
		for (int i = 0; i < attrCount; i++) {
			s += attrs[i] + ",";
		}
		return s + "]";
	}

	public boolean match(Object o) {
		if (o instanceof OrthogonalEdge) {
			System.err.println("error");
		} else if (o instanceof TypedEdge) {
			TypedEdge e = (TypedEdge) o;
			for (int i = 0; i < attrCount; i++) {
				if (this.attrs[i] == e.getAttr()[0]) {
					return true;
				}
			}
			return false;
		}
		System.out.println("type error.");
		return false;
	}

	public boolean hasPossibleMatch(IntSet labelset) {
		if (labelset == null) {
			return true;
		}
		for (int i = 0; i < attrCount; i++) {
			if (labelset.contains(this.attrs[i])) {
				return true;
			}
		}
		return false;
	}

	public String toString() {
		return "oEdge [f=" + fromNode.getID() + ", t=" + toNode.getID() + ", attr=" + this.attrs
				+ " ]";
	}

}