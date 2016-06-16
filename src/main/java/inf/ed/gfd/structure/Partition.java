package inf.ed.gfd.structure;

import inf.ed.graph.structure.Graph;
import inf.ed.graph.structure.OrthogonalEdge;
import inf.ed.graph.structure.OrthogonalGraph;
import inf.ed.graph.structure.adaptor.VertexOString;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Data structure of partition, including a graph fragment and vertices with
 * crossing edges.
 * 
 * @author yecol
 *
 */

public class Partition implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	static Logger log = LogManager.getLogger(Partition.class);

	private int partitionID;

	Graph<VertexOString, OrthogonalEdge> graph;

	public IntSet borderVertices;

	public Partition(int partitionID) {
		this.partitionID = partitionID;
	}

	public boolean loadPartitionDataFromEVFile(String filePath) {
		this.graph = new OrthogonalGraph<VertexOString>(VertexOString.class);
		this.graph.loadGraphFromVEFile(filePath, true);
		return true;
	}

	public int getPartitionID() {
		return partitionID;
	}

	public Graph<VertexOString, OrthogonalEdge> getGraph() {
		return this.graph;
	}

	public void addCrossingEdges(Set<CrossingEdge> crossingEdges) {
		for (CrossingEdge e : crossingEdges) {
			if (!this.graph.contains(e.source)) {
				VertexOString vo = new VertexOString(e.source, e.sourceLabel);
				this.graph.addVertex(vo);
			}
			if (!this.graph.contains(e.target)) {
				VertexOString vo = new VertexOString(e.target, e.targetLabel);
				this.graph.addVertex(vo);
			}
			if (!this.graph.contains(e.source, e.target)) {
				this.graph.addEdge(this.graph.getVertex(e.source), this.graph.getVertex(e.target));
			}
			this.graph.getEdge(e.source, e.target).setAttr(e.edgetype);
		}
	}

	public void addTransferedGraph(Ball ball) {

		if (ball == null) {
			System.out.println("the ball is null");
			return;
		}

		// ball.print();

		for (int vertexID : ball.vertices.keySet()) {
			if (!this.graph.contains(vertexID)) {
				VertexOString vertex = new VertexOString(vertexID, ball.vertices.get(vertexID));
				this.graph.addVertex(vertex);
			}
		}

		for (TransEdge e : ball.edges) {
			if (!this.graph.contains(e.from, e.to)) {
				VertexOString from = this.graph.getVertex(e.from);
				VertexOString to = this.graph.getVertex(e.to);
				this.graph.addEdge(from, to);
			}
			this.graph.getEdge(e.from, e.to).setAttr(e.type);
		}
	}

	public void loadBorderVerticesFromFile(String filePath) {
		this.borderVertices = new IntOpenHashSet();
		int linecount = 0;
		try {
			File borderVertexFile = new File(filePath + ".p2bv");
			LineIterator it = FileUtils.lineIterator(borderVertexFile, "UTF-8");
			try {
				while (it.hasNext()) {
					linecount++;
					String[] elements = it.nextLine().split("\t");
					if (elements != null) {
						if (Integer.parseInt(elements[0]) == this.partitionID) {
							borderVertices.add(Integer.parseInt(elements[1]));
						}
					}
				}
			} finally {
				LineIterator.closeQuietly(it);
			}
			log.info("border vertex loaded, " + linecount + " lines scanned, "
					+ this.borderVertices.size() + " border nodes added.");
		} catch (IOException e) {
			log.error("load border file failed.");
			e.printStackTrace();
		}
		return;
	}

	public String getPartitionInfo() {
		String ret = "pID = " + this.partitionID + " | vertices = " + this.graph.vertexSize()
				+ " | edges = " + this.graph.edgeSize();
		if (borderVertices != null) {
			ret += " | border.size = " + borderVertices.size();
		}
		return ret;
	}
}
