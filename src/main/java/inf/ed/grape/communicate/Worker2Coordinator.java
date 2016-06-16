package inf.ed.grape.communicate;

import inf.ed.gfd.structure.WorkUnit;
import inf.ed.grape.interfaces.Result;
import it.unimi.dsi.fastutil.ints.Int2IntMap;

import java.io.Serializable;
import java.rmi.RemoteException;
import java.util.Map;
import java.util.Set;

/**
 * Defines an interface to register remote Worker with the coordinator
 * 
 * @author Yecol
 */

public interface Worker2Coordinator extends java.rmi.Remote, Serializable {

	public Worker2Coordinator register(Worker worker, String workerID, int numWorkerThreads)
			throws RemoteException;

	/* for synchronised model only */
	public void localComputeCompleted(String workerID, Set<String> activeWorkerIDs)
			throws RemoteException;

	public void sendPartialResult(String workerID, Map<Integer, Result> mapPartitionID2Result,
			double communicationData) throws RemoteException;

	public void sendPartialWorkunitsAndBorderInfo(String workerID, Set<WorkUnit> partialWorkunits,
			Int2IntMap bordernodeBallSize, double findCandidateTime) throws RemoteException;

	public void shutdown() throws RemoteException;

}
