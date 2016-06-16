package inf.ed.isomorphism;

import inf.ed.graph.structure.adaptor.Pair;
import it.unimi.dsi.fastutil.ints.Int2IntMap;

public interface State {

	/**
	 * The node marker to be used when indicating that that no node is being
	 * matched.
	 */
	public static final int NULL_NODE = -1;
	public static final int NEW_N1 = -10001;

	/**
	 * Returns the next candidate for isomorphic matching given these prior two
	 * vertices that were matched. If {@code prevN1} and {@code prevN1} are
	 * {@code NULL_NODE}, this should return the initial candidate.
	 */
	Pair<Integer> nextPair(int prevN1, int prevN2);

	/**
	 * Adds the two vertices to this {@code State}'s vertex mapping.
	 */
	void addPair(int n1, int n2);

	/**
	 * Returns {@code true} if mapping {@code node1} to {@code node2} would
	 * preserve the isomorphism between the graphs to the extend that their
	 * vertices have been mapped thus far.
	 */
	boolean isFeasiblePair(int node1, int node2);

	/**
	 * Returns {@code true} if all the vertices have been mapped. Equivalently,
	 * returns {@code true} if the graphs are isomorphic.
	 */
	boolean isGoal();

	/**
	 * Returns {@code true} if the current state of mapping cannot proceed
	 * because some invalid mapping has occurred and no further pairs would
	 * result in an isomorphic match.
	 */
	boolean isDead();

	/**
	 * Makes a shallow copy of the content of this state.
	 */
	State copy();

	/**
	 * Undoes the mapping added in the prior call to {@code addPair}.
	 */
	void backTrack();

	void nextN1();

	int curN1();

	Int2IntMap getMatch();
}
