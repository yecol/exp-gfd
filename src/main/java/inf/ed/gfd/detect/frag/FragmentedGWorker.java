package inf.ed.gfd.detect.frag;

import inf.ed.gfd.detect.repl.ReplicatedGWorkUnit;
import inf.ed.gfd.structure.Ball;
import inf.ed.gfd.structure.CrossingEdge;
import inf.ed.gfd.structure.GFD2;
import inf.ed.gfd.structure.Partition;
import inf.ed.gfd.structure.WorkUnit;
import inf.ed.gfd.util.Dev;
import inf.ed.gfd.util.KV;
import inf.ed.gfd.util.Params;
import inf.ed.gfd.util.Stat;
import inf.ed.grape.communicate.Worker;
import inf.ed.grape.communicate.Worker2Coordinator;
import inf.ed.grape.communicate.Worker2WorkerProxy;
import inf.ed.grape.interfaces.LocalComputeTask;
import inf.ed.grape.interfaces.Message;
import inf.ed.grape.interfaces.Result;
import inf.ed.graph.structure.Graph;
import inf.ed.graph.structure.OrthogonalEdge;
import inf.ed.graph.structure.adaptor.VertexOString;
import inf.ed.graph.structure.adaptor.VertexString;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.carrotsearch.sizeof.RamUsageEstimator;

/**
 * Represents the computation node.
 * 
 * @author Yecol
 */

public class FragmentedGWorker extends UnicastRemoteObject implements Worker {

	private static final long serialVersionUID = 1L;

	/** The number of threads. */
	private int numThreads;

	/** The total partitions assigned. */
	private int totalPartitionsAssigned;

	/** The queue of partitions in the current super step. */
	private BlockingQueue<LocalComputeTask> currentLocalComputeTaskQueue;

	/** The queue of partitions in the next super step. */
	private BlockingQueue<LocalComputeTask> nextLocalComputeTasksQueue;

	/** hosting partitions */
	private Map<Integer, Partition> partitions;

	/** Host name of the node with time stamp information. */
	private String workerID;

	/** Coordinator Proxy object to interact with Master. */
	private Worker2Coordinator coordinatorProxy;

	/** VertexID 2 PartitionID Map */
	// private Map<Integer, Integer> mapVertexIdToPartitionId;

	/** PartitionID to WorkerID Map. */
	private Map<Integer, String> mapPartitionIdToWorkerId;

	/** Worker2WorkerProxy Object. */
	private Worker2WorkerProxy worker2WorkerProxy;

	/** Worker to Outgoing Messages Map. */
	private Map<String, List<Message<?>>> outgoingMessages;

	/** PartitionID to Outgoing Results Map. */
	private Map<Integer, Result> partialResults;

	/** partitionId to Previous Incoming messages - Used in current Super Step. */
	private Map<Integer, List<Message<?>>> previousIncomingMessages;

	/** partitionId to Current Incoming messages - used in next Super Step. */
	private Map<Integer, List<Message<?>>> currentIncomingMessages;

	/**
	 * boolean variable indicating whether the partitions can be worked upon by
	 * the workers in each super step.
	 **/
	private boolean flagLocalCompute = false;
	/**
	 * boolean variable to determine if a Worker can send messages to other
	 * Workers and to Master. It is set to true when a Worker is sending
	 * messages to other Workers.
	 */
	private boolean stopSendingMessage;

	private boolean flagLastStep = false;

	/** The super step counter. */
	private long superstep = 0;

	/** These are for GFD project **/
	private int holdingPartitionID = 0;
	private Int2ObjectMap<Ball> mapBorderVertex2Ball;
	private Int2IntMap mapBorderVertex2BallSize;

	static Logger log = LogManager.getLogger(FragmentedGWorker.class);

	/**
	 * Instantiates a new worker.
	 * 
	 * @throws RemoteException
	 *             the remote exception
	 */
	public FragmentedGWorker() throws RemoteException {
		InetAddress address = null;
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("MMdd.HHmmss.SSS");
		String timestamp = simpleDateFormat.format(new Date());
		String hostName = new String();
		try {
			address = InetAddress.getLocalHost();
			hostName = address.getHostName();
		} catch (UnknownHostException e) {
			hostName = "UnKnownHost";
			log.error(e);
		}

		this.workerID = "sync_" + hostName + "_" + timestamp;
		this.partitions = new HashMap<Integer, Partition>();
		this.currentLocalComputeTaskQueue = new LinkedBlockingDeque<LocalComputeTask>();
		this.nextLocalComputeTasksQueue = new LinkedBlockingQueue<LocalComputeTask>();
		this.currentIncomingMessages = new HashMap<Integer, List<Message<?>>>();
		this.partialResults = new HashMap<Integer, Result>();
		this.previousIncomingMessages = new HashMap<Integer, List<Message<?>>>();
		this.outgoingMessages = new HashMap<String, List<Message<?>>>();
		this.numThreads = 1;
		this.stopSendingMessage = false;

		this.mapBorderVertex2Ball = new Int2ObjectOpenHashMap<Ball>();
		this.mapBorderVertex2BallSize = new Int2IntOpenHashMap();

		for (int i = 0; i < numThreads; i++) {
			log.debug("Starting syncThread " + (i + 1));
			WorkerThread workerThread = new WorkerThread();
			workerThread.setName("Worker");
			workerThread.start();
		}

	}

	/**
	 * Adds the partition to be assigned to the worker.
	 * 
	 * @param partition
	 *            the partition to be assigned
	 * @throws RemoteException
	 *             the remote exception
	 */
	// @Override
	public void addPartition(Partition partition) throws RemoteException {
		throw new IllegalArgumentException("No partition in distributed setting 1.");
	}

	@Override
	public void addPartitionID(int partitionID) throws RemoteException {
		String filename = KV.GRAPH_FILE_PATH + ".p" + partitionID;
		Partition partition = new Partition(partitionID);
		partition.loadPartitionDataFromEVFile(filename);
		partition.loadBorderVerticesFromFile(KV.GRAPH_FILE_PATH);
		this.partitions.put(partitionID, partition);
		totalPartitionsAssigned = 1;
		this.holdingPartitionID = partitionID;
		log.info("loaded the fragmented graph: " + filename + ", part " + partitionID);
		log.debug(Dev.currentRuntimeState());
	}

	/**
	 * Gets the num threads.
	 * 
	 * @return the num threads
	 */
	@Override
	public int getNumThreads() {
		return numThreads;
	}

	/**
	 * Gets the worker id.
	 * 
	 * @return the worker id
	 */
	@Override
	public String getWorkerID() {
		return workerID;
	}

	/**
	 * The Class SyncWorkerThread.
	 */
	private class WorkerThread extends Thread {

		@Override
		public void run() {
			while (true) {
				try {
					Thread.sleep(100);
				} catch (InterruptedException e1) {
					e1.printStackTrace();
				}
				while (flagLocalCompute || flagLastStep) {
					log.debug(this + "superstep loop start for superstep " + superstep
							+ ", laststep = " + flagLastStep);
					try {

						LocalComputeTask localComputeTask = currentLocalComputeTaskQueue.take();

						Partition workingPartition = partitions.get(localComputeTask
								.getPartitionID());

						if (flagLastStep) {
							log.debug("last step");
							partialResults.put(localComputeTask.getPartitionID(),
									localComputeTask.getPartialResult());
						}

						else {

							if (superstep == 0) {

								/** begin step. initial compute */
								localComputeTask.compute(workingPartition);
								updateOutgoingMessages(localComputeTask.getMessages());
							}

							else {

								/** not begin step. incremental compute */
								List<Message<?>> messageForWorkingPartition = previousIncomingMessages
										.get(localComputeTask.getPartitionID());

								// if (messageForWorkingPartition != null) {

								localComputeTask.incrementalCompute(workingPartition,
										messageForWorkingPartition);

								updateOutgoingMessages(localComputeTask.getMessages());
								// }
							}

							localComputeTask.prepareForNextCompute();
						}

						nextLocalComputeTasksQueue.add(localComputeTask);
						checkAndSendMessage();

					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}
		}

	}

	/**
	 * Check and send message. Notice: this is a critical code area, which
	 * should put outside of the thread code.
	 * 
	 * @throws RemoteException
	 */

	private synchronized void checkAndSendMessage() {

		log.debug("synchronized checkAndSendMessage!");

		log.debug("nextQueueSize:" + nextLocalComputeTasksQueue.size() + " == partitionAssigned:"
				+ totalPartitionsAssigned);
		if ((!stopSendingMessage) && (nextLocalComputeTasksQueue.size() == totalPartitionsAssigned)) {
			log.debug("sendMessage!");

			stopSendingMessage = true;

			if (flagLastStep) {

				flagLastStep = false;

				log.debug("send partital result to coordinator for assemble, communicationData = "
						+ Stat.getInstance().communicationData);
				try {
					coordinatorProxy.sendPartialResult(workerID, partialResults,
							Stat.getInstance().communicationData);
				} catch (RemoteException e) {
					e.printStackTrace();
				}

				log.info("Done!");
			}

			else {

				log.debug(" Worker: Superstep " + superstep + " completed begin send infomation .");

				flagLocalCompute = false;

				for (Entry<String, List<Message<?>>> entry : outgoingMessages.entrySet()) {
					try {
						log.info("+++++try to send message to " + entry.getKey() + ", message = "
								+ entry.getValue());
						worker2WorkerProxy.sendMessage(entry.getKey(), entry.getValue());
					} catch (RemoteException e) {
						System.out.println("Can't send message to Worker " + entry.getKey()
								+ " which is down");
						e.printStackTrace();
					}
				}

				// This worker will be active only if it has some messages
				// queued up in the next superstep.
				// activeWorkerSet will have all the workers who will be
				// active
				// in the next superstep.
				Set<String> activeWorkerSet = new HashSet<String>();
				activeWorkerSet.addAll(outgoingMessages.keySet());
				if (currentIncomingMessages.size() > 0) {
					activeWorkerSet.add(workerID);
				}
				// Send a message to the Master saying that this superstep
				// has
				// been completed.
				try {
					coordinatorProxy.localComputeCompleted(workerID, activeWorkerSet);
				} catch (RemoteException e) {
					e.printStackTrace();
				}
			}

		}
	}

	/**
	 * Halts the run for this application and prints the output in a file.
	 * 
	 * @throws RemoteException
	 *             the remote exception
	 */
	@Override
	public void halt() throws RemoteException {
		System.out.println("Worker Machine " + workerID + " halts");
		this.restoreInitialState();
	}

	/**
	 * Restore the worker to the initial state
	 */
	private void restoreInitialState() {
		// this.partitionQueue.clear();
		this.currentIncomingMessages.clear();
		this.outgoingMessages.clear();
		this.mapPartitionIdToWorkerId.clear();
		// this.currentPartitionQueue.clear();
		this.previousIncomingMessages.clear();
		this.stopSendingMessage = false;
		this.flagLocalCompute = false;
		this.totalPartitionsAssigned = 0;
	}

	/**
	 * Updates the outgoing messages for every super step.
	 * 
	 * @param messagesFromCompute
	 *            Represents the map of destination vertex and its associated
	 *            message to be send
	 */
	private void updateOutgoingMessages(List<Message<?>> messagesFromCompute) {
		log.debug("updateOutgoingMessages.size = " + messagesFromCompute.size());

		String workerID = null;
		int partitionID = -1;
		List<Message<?>> workerMessages = null;

		for (Message<?> message : messagesFromCompute) {

			partitionID = message.getDestinationPartitionID();
			workerID = mapPartitionIdToWorkerId.get(partitionID);

			if (workerID.equals(this.workerID)) {

				/** for gfd, discard these message */
				// updateIncomingMessages(partitionID, message);
			} else {
				if (outgoingMessages.containsKey(workerID)) {
					outgoingMessages.get(workerID).add(message);
				} else {
					workerMessages = new ArrayList<Message<?>>();
					workerMessages.add(message);
					outgoingMessages.put(workerID, workerMessages);
				}
			}
		}
	}

	/**
	 * Sets the worker partition info.
	 * 
	 * @param totalPartitionsAssigned
	 *            the total partitions assigned
	 * @param mapVertexIdToPartitionId
	 *            the map vertex id to partition id
	 * @param mapPartitionIdToWorkerId
	 *            the map partition id id to worker id
	 * @param mapWorkerIdToWorker
	 *            the map worker id to worker
	 * @throws RemoteException
	 */
	@Override
	public void setWorkerPartitionInfo(int totalPartitionsAssigned,
			Map<Integer, Integer> mapVertexIdToPartitionId,
			Map<Integer, String> mapPartitionIdToWorkerId, Map<String, Worker> mapWorkerIdToWorker)
			throws RemoteException {
		this.totalPartitionsAssigned = totalPartitionsAssigned;
		this.mapPartitionIdToWorkerId = mapPartitionIdToWorkerId;
		this.worker2WorkerProxy = new Worker2WorkerProxy(mapWorkerIdToWorker);
	}

	/**
	 * The main method.
	 * 
	 * @param args
	 *            the arguments
	 * @throws Exception
	 *             the exception
	 */
	public static void main(String[] args) throws Exception {
		if (System.getSecurityManager() == null) {
			System.setSecurityManager(new SecurityManager());
		}

		if (args.length < 4) {
			System.out.println("paras: config-file, n, opt");
			System.exit(0);
		}

		try {
			String coordinatorMachineName = args[0];
			Params.CONFIG_FILENAME = args[1].trim();
			Params.N_PROCESSORS = Integer.parseInt(args[2].trim());
			Params.RUN_MODE = Integer.parseInt(args[3].trim());
			log.debug("Connected to Sc: " + coordinatorMachineName);
			log.debug("PARAM_CONFIG_FILE = " + Params.CONFIG_FILENAME);
			log.debug("PARAM_N = " + Params.N_PROCESSORS);
			log.debug("PARAM_RUN_MODE = " + Params.RUN_MODE);
			log.debug("Processing FRAGMENTED graph = " + KV.DATASET);

			String masterURL = "//" + coordinatorMachineName + "/" + KV.COORDINATOR_SERVICE_NAME;
			Worker2Coordinator worker2Coordinator = (Worker2Coordinator) Naming.lookup(masterURL);
			Worker worker = new FragmentedGWorker();
			Worker2Coordinator coordinatorProxy = worker2Coordinator.register(worker,
					worker.getWorkerID(), worker.getNumThreads());

			worker.setCoordinatorProxy(coordinatorProxy);
			log.info("Worker is bound and ready for computations ");

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * Sets the master proxy.
	 * 
	 * @param masterProxy
	 *            the new master proxy
	 */
	@Override
	public void setCoordinatorProxy(Worker2Coordinator coordinatorProxy) {
		this.coordinatorProxy = coordinatorProxy;
	}

	/**
	 * Receive message.
	 * 
	 * @param incomingMessages
	 *            the incoming messages
	 * @throws RemoteException
	 *             the remote exception
	 */
	@Override
	public void receiveMessage(List<Message<?>> incomingMessages) throws RemoteException {

		Stat.getInstance().communicationData += RamUsageEstimator.sizeOf(incomingMessages);

		/** partitionID to message list */
		List<Message<?>> partitionMessages = null;

		int partitionID = -1;

		for (Message<?> message : incomingMessages) {
			partitionID = message.getDestinationPartitionID();
			if (currentIncomingMessages.containsKey(partitionID)) {
				currentIncomingMessages.get(partitionID).add(message);
			} else {
				partitionMessages = new ArrayList<Message<?>>();
				partitionMessages.add(message);
				currentIncomingMessages.put(partitionID, partitionMessages);
			}
		}
	}

	/**
	 * Receives the messages sent by all the vertices in the same node and
	 * updates the current incoming message queue.
	 * 
	 * @param destinationVertex
	 *            Represents the destination vertex to which the message has to
	 *            be sent
	 * @param incomingMessage
	 *            Represents the incoming message for the destination vertex
	 */
	public void updateIncomingMessages(int partitionID, Message<?> incomingMessage) {
		List<Message<?>> partitionMessages = null;
		if (currentIncomingMessages.containsKey(partitionID)) {
			currentIncomingMessages.get(partitionID).add(incomingMessage);
		} else {
			partitionMessages = new ArrayList<Message<?>>();
			partitionMessages.add(incomingMessage);
			currentIncomingMessages.put(partitionID, partitionMessages);
		}
	}

	/** shutdown the worker */
	@Override
	public void shutdown() throws RemoteException {
		java.util.Date date = new java.util.Date();
		log.info("Worker" + workerID + " goes down now at :" + new Timestamp(date.getTime()));
		System.exit(0);
	}

	@Override
	public void nextLocalCompute(long superstep) throws RemoteException {

		/**
		 * Next local compute. No generated new local compute tasks. Transit
		 * compute task and status from the last step.
		 * */

		this.superstep = superstep;

		// Put all elements in current incoming queue to previous incoming queue
		// and clear the current incoming queue.
		this.previousIncomingMessages.clear();
		this.previousIncomingMessages.putAll(this.currentIncomingMessages);
		this.currentIncomingMessages.clear();

		this.stopSendingMessage = false;
		this.flagLocalCompute = true;

		this.outgoingMessages.clear();

		// Put all local compute tasks in current task queue.
		// clear the completed partitions.
		// Note: To avoid concurrency issues, it is very important that
		// completed partitions is cleared before the Worker threads start to
		// operate on the partition queue in the next super step
		BlockingQueue<LocalComputeTask> temp = new LinkedBlockingDeque<LocalComputeTask>(
				nextLocalComputeTasksQueue);
		this.nextLocalComputeTasksQueue.clear();
		this.currentLocalComputeTaskQueue.addAll(temp);

	}

	@Override
	public void processPartialResult() throws RemoteException {
		BlockingQueue<LocalComputeTask> temp = new LinkedBlockingDeque<LocalComputeTask>(
				nextLocalComputeTasksQueue);
		this.nextLocalComputeTasksQueue.clear();
		this.currentLocalComputeTaskQueue.addAll(temp);

		this.flagLastStep = true;
		this.stopSendingMessage = false;

	}

	@Override
	public void vote2halt() throws RemoteException {
		throw new IllegalArgumentException("this mothod doesn't support in synchronised model.");
	}

	@Override
	public void voteAgain() throws RemoteException {
		throw new IllegalArgumentException("this mothod doesn't support in synchronised model.");
	}

	@Override
	public boolean isHalt() {
		throw new IllegalArgumentException("this mothod doesn't support in synchronised model.");
	}

	@Override
	public boolean loadWholeGraph(int partitionID) throws RemoteException {
		throw new IllegalArgumentException("this mothod doesn't support in synchronised model.");
	}

	@Override
	public void setQueryWithGFD2(List<GFD2> queries) throws RemoteException {
		// Get all the queries from queries, generate partial work units
		// locally.

		log.debug("get ALL gfd queries from coordinator, total size = " + queries.size());
		long beginComputeCandidate = System.currentTimeMillis();
		for (GFD2 gfd : queries) {
			log.debug("get gfd from coordinator: " + gfd.getID());
			gfd.findCandidates(this.partitions.get(holdingPartitionID).getGraph());
		}
		double findCandidateTime = (System.currentTimeMillis() - beginComputeCandidate) * 1.0 / 1000;

		// generate partial work units.
		Set<WorkUnit> partitialWorkUnits = generatePartialWorkUnits(queries,
				this.partitions.get(this.holdingPartitionID));

		// build information of border nodes.
		buildBorderNodeAndNeighbour(queries, this.partitions.get(this.holdingPartitionID));
		System.out.println("current partial wus = " + partitialWorkUnits.size());
		System.err.println("neighbour size = " + mapBorderVertex2Ball.size());

		try {
			log.debug("begin send border nodes with its neighbour balls info to coordinator.");
			coordinatorProxy.sendPartialWorkunitsAndBorderInfo(workerID, partitialWorkUnits,
					mapBorderVertex2BallSize, findCandidateTime);
		} catch (RemoteException e) {
			e.printStackTrace();
		}

	}

	public void buildBorderNodeAndNeighbour(List<GFD2> queries, Partition p) {
		// FIXME here we assume the radiu <=2;
		// TODO filter border nodes with queries;
		log.fatal("begin build. total borderVertices = " + p.borderVertices.size());

		Set<String> allAppearedLabels = new ObjectOpenHashSet<String>();
		for (GFD2 gfd : queries) {
			for (VertexString v : gfd.getPattern().allVertices().values()) {
				allAppearedLabels.add(v.getAttr());
			}
		}
		log.fatal("got all labels");

		final int radiusLimit = 1;
		for (int borderVertex : p.borderVertices) {
			if (allAppearedLabels.contains(p.getGraph().getVertex(borderVertex).getAttr())) {
				Ball ball = p.getGraph().getBall(borderVertex, radiusLimit);
				mapBorderVertex2Ball.put(borderVertex, ball);
				mapBorderVertex2BallSize
						.put(borderVertex, ball.vertices.size() + ball.edges.size());
			}
		}

		System.out.println("total border nodes.size = " + mapBorderVertex2Ball.size());
	}

	private Set<WorkUnit> generatePartialWorkUnits(List<GFD2> queries, Partition p) {
		Set<WorkUnit> workunitSet = new HashSet<WorkUnit>();
		int partialWorkUnitUniqueID = 0;
		for (GFD2 gfd : queries) {
			if (gfd.isConnected()) {
				for (int candidate : gfd.getCandidates().get(0)) {
					if (p.getGraph().contains(candidate)) {

						// candidate in this partition.
						WorkUnit pwu = new WorkUnit(partialWorkUnitUniqueID++, gfd.getID(),
								candidate, p.getPartitionID());
						// get subgraph
						pwu.gfd = gfd;
						pwu.ball1 = p.getGraph().getBall(candidate, gfd.getRadius());
						pwu.computeAndSetBorderNodes(p.borderVertices);
						workunitSet.add(pwu);
					} else {
						// not have candidates.
					}
				}
			}
		}

		return workunitSet;
	}

	@Override
	public void setQueryWithWorkUnit(Set<ReplicatedGWorkUnit> workload, List<GFD2> gfds)
			throws RemoteException {
		throw new IllegalArgumentException("this mothod doesn't support in synchronised model.");
	}

	@Override
	public void setWorkUnitsAndPrefetchRequest(Set<WorkUnit> workload,
			Int2ObjectMap<IntSet> prefetchRequests, Set<CrossingEdge> crossingEdges)
			throws RemoteException {

		int prefetchRequestSize = 0;
		for (int key : prefetchRequests.keySet()) {
			prefetchRequestSize += prefetchRequests.get(key).size();
		}

		System.out.println(RamUsageEstimator.sizeOf(crossingEdges));

		Stat.getInstance().communicationData += RamUsageEstimator.sizeOf(crossingEdges);
		log.info("Get " + workload.size() + " work units from coordinator, crossing edgeSize = "
				+ crossingEdges.size() + ", requestSize = " + prefetchRequestSize + ", commData="
				+ Stat.getInstance().communicationData);

		for (Entry<Integer, Partition> entry : this.partitions.entrySet()) {
			// add crossing edges to this partition;
			log.debug("before add crossing edges, " + entry.getValue().getPartitionInfo());
			entry.getValue().addCrossingEdges(crossingEdges);
			log.debug("after add crossing edges, " + entry.getValue().getPartitionInfo());
			for (WorkUnit wu : workload) {
				entry.getValue().addTransferedGraph(wu.ball1);
			}
			log.debug("after add balls with " + entry.getValue().getPartitionInfo());

			try {
				QueryWithWorkUnit localComputeTask = new QueryWithWorkUnit();
				localComputeTask.init(entry.getKey());
				localComputeTask.setWorkUnits(workload);
				localComputeTask.setPrefetchQuest(prefetchRequests);
				localComputeTask.setMapBorderVertex2Ball(mapBorderVertex2Ball);
				this.nextLocalComputeTasksQueue.add(localComputeTask);

			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}
