package inf.ed.grape.communicate;

import java.rmi.Remote;
import java.rmi.RemoteException;

/**
 * Defines the interface through which the application programmer communicates
 * with the Master.
 * 
 * @author yecol
 */

public interface Client2Coordinator extends Remote {

	/**
	 * Submits the graph problem to be computed.
	 * 
	 * @param graphFileName
	 *            the graph file name
	 * @param vertexClassName
	 *            the application specific vertex class name
	 */
	public void process() throws RemoteException;

	public void preProcess() throws RemoteException;

	public void postProcess() throws RemoteException;
}
