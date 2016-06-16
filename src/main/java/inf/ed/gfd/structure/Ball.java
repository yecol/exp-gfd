package inf.ed.gfd.structure;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;

import java.io.Serializable;
import java.util.Set;

public class Ball implements Serializable {

	private static final long serialVersionUID = 1L;
	public Int2ObjectMap<String> vertices;
	public Set<TransEdge> edges;
	public int center = -1;
	public int radius = -1;

	public Ball(int center, int radius) {
		this.center = center;
		this.radius = radius;
		vertices = new Int2ObjectOpenHashMap<String>();
		edges = new ObjectOpenHashSet<TransEdge>();
	}

	public void print() {
		System.out.println("vsize = " + vertices.size() + ", esize = " + edges.size());
		System.out.println(vertices.toString());
		System.out.println(edges.toString());
	}

	@Override
	public boolean equals(Object obj) {
		Ball other = (Ball) obj;
		return this.center == other.center && this.radius == other.radius;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + prime;
		return result;
	}

	public String getInfo() {
		return "ball-centered in " + center + ", r = " + radius + ", vsize = " + vertices.size()
				+ ", esize = " + edges.size();
	}

}
