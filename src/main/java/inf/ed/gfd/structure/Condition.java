package inf.ed.gfd.structure;

import inf.ed.gfd.util.KV;
import inf.ed.graph.structure.Graph;
import inf.ed.graph.structure.OrthogonalEdge;
import inf.ed.graph.structure.adaptor.VertexOString;
import it.unimi.dsi.fastutil.ints.Int2IntMap;

import java.io.Serializable;
import java.util.HashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Condition implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	static Logger log = LogManager.getLogger(Condition.class);

	private HashMap<Integer, String> XEqualsLiteral;
	private HashMap<Integer, Integer> XEqualsVariable;

	private HashMap<Integer, String> YEqualsLiteral;
	private HashMap<Integer, Integer> YEqualsVariable;
	private HashMap<Integer, Integer> YHasPropertyEdgeType;
	private HashMap<Integer, Integer> YHasProperty2Node;

	public Condition() {

		XEqualsLiteral = new HashMap<Integer, String>();
		XEqualsVariable = new HashMap<Integer, Integer>();

		YEqualsLiteral = new HashMap<Integer, String>();
		YEqualsVariable = new HashMap<Integer, Integer>();
		YHasPropertyEdgeType = new HashMap<Integer, Integer>();
		YHasProperty2Node = new HashMap<Integer, Integer>();
	}

	public void addCondition(int tag, String line) {

		if (tag == KV.XCONDITION) {
			this.addConditon2X(line);
		} else if (tag == KV.YCONDITION) {
			this.addCondition2Y(line);
		}
	}

	private void addConditon2X(String line) {

		String[] elements = line.split("\t");
		int nodeID = Integer.parseInt(elements[0].trim());
		if (elements[1].trim().equals(KV.CONDITION_TYPE_EQUAL_LET)) {
			this.XEqualsLiteral.put(nodeID, elements[2].trim());
		} else if (elements[1].trim().equals(KV.CONDITION_TYPE_EQUAL_VAR)) {
			int otherID = Integer.parseInt(elements[2].trim());
			this.XEqualsVariable.put(nodeID, otherID);
		}
	}

	private void addCondition2Y(String line) {
		String[] elements = line.split("\t");
		int nodeID = Integer.parseInt(elements[0].trim());
		if (elements[1].trim().equals(KV.CONDITION_TYPE_EQUAL_LET)) {
			this.YEqualsLiteral.put(nodeID, elements[2].trim());
		} else if (elements[1].trim().equals(KV.CONDITION_TYPE_EQUAL_VAR)) {
			int otherID = Integer.parseInt(elements[2].trim());
			this.YEqualsVariable.put(nodeID, otherID);
		} else if (elements[1].trim().equals(KV.CONDITION_TYPE_HAS_PROP)) {
			int edgeType = Integer.parseInt(elements[2].trim());
			int otherID = Integer.parseInt(elements[3].trim());
			this.YHasPropertyEdgeType.put(nodeID, edgeType);
			this.YHasProperty2Node.put(nodeID, otherID);
		}
	}

	public boolean verify(Int2IntMap match, Graph<VertexOString, OrthogonalEdge> KB) {

		// log.debug("begin verify: " + match.toString());

		// check for X
		for (int u : XEqualsLiteral.keySet()) {
			// for literal equation, if not equals then it valid.
			int vertexID = match.get(u);
			// log.debug(vertexID + ":" + KB.getVertex(vertexID).getAttr() +
			// ", expt = "
			// + XEqualsLiteral.get(u));
			if (!KB.getVertex(vertexID).getAttr().equals(XEqualsLiteral.get(u))) {
				return true;
			}
		}

		for (int u : XEqualsVariable.keySet()) {
			// for variable equation, if not equals then it valid
			int vertexID1 = match.get(u);
			int vertexID2 = match.get(XEqualsVariable.get(u));
			// log.debug(vertexID1 + ":" + KB.getVertex(vertexID1).getAttr() +
			// "|" + +vertexID2 + ":"
			// + KB.getVertex(vertexID2).getAttr());
			if (!KB.getVertex(vertexID1).getAttr().equals(KB.getVertex(vertexID2).getAttr())) {
				return true;
			}
		}

		// log.debug("passed X match");

		// check for Y. it valid only satisfy the condition
		for (int u : YEqualsLiteral.keySet()) {
			int vertexID = match.get(u);
			if (!KB.getVertex(vertexID).getAttr().equals(YEqualsLiteral.get(u))) {
				// log.debug("vio: " + vertexID + ":" +
				// KB.getVertex(vertexID).getAttr()
				// + ", but expt" + YEqualsLiteral.get(u));
				return false;
			}
		}

		for (int u : YEqualsVariable.keySet()) {
			int vertexID1 = match.get(u);
			int vertexID2 = match.get(YEqualsVariable.get(u));
			if (!KB.getVertex(vertexID1).getAttr().equals(KB.getVertex(vertexID2).getAttr())) {
				// log.debug("vio: " + vertexID1 + ":" +
				// KB.getVertex(vertexID1).getAttr() + "|"
				// + +vertexID2 + ":" + KB.getVertex(vertexID2).getAttr());
				return false;
			}
		}

		for (int u : YHasProperty2Node.keySet()) {
			// TODO minor bug, should refine this.
			int vertexID1 = match.get(u);
			int vertexID2 = match.get(YHasProperty2Node.get(u));
			if (!KB.contains(vertexID1, vertexID2)) {
				// log.debug("vio: no connection from: " +
				// KB.getVertex(vertexID1).getAttr()
				// + ", to: " + KB.getVertex(vertexID2).getAttr());
				return false;
			} else {
				OrthogonalEdge e = KB.getEdge(vertexID1, vertexID2);
				boolean found = false;
				for (int type : e.getAttr()) {
					if (type == YHasPropertyEdgeType.get(u)) {
						found = true;
						break;
					}
				}
				if (found == false) {
					return false;
				}
			}
		}

		return true;
	}
}
