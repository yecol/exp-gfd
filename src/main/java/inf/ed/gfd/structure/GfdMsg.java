package inf.ed.gfd.structure;

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;

import java.io.Serializable;
import java.util.Set;

public class GfdMsg implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	public IntSet requestedBorderNodes = new IntOpenHashSet();
	public Set<Ball> transferingGraphData = new ObjectOpenHashSet<Ball>();

	public String toString() {
		String ret = "";
		ret += "requesting Node.size =" + requestedBorderNodes.size();
		ret += " transfering Graph size = " + transferingGraphData.size();
		return ret;
	}
}
