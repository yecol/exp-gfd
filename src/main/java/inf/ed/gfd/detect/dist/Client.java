package inf.ed.gfd.detect.dist;

import inf.ed.gfd.util.KV;
import inf.ed.grape.communicate.Client2Coordinator;

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Client {

	static Logger log = LogManager.getLogger(Client.class);

	public static void main(String[] args) throws RemoteException, NotBoundException,
			MalformedURLException, ClassNotFoundException, InstantiationException,
			IllegalAccessException {
		String coordinatorMachineName = args[0];
		String coordinatorURL = "//" + coordinatorMachineName + "/" + KV.COORDINATOR_SERVICE_NAME;
		Client2Coordinator client2Coordinator = (Client2Coordinator) Naming.lookup(coordinatorURL);
		runApplication(client2Coordinator);
	}

	/**
	 * Run application.
	 * 
	 * @throws ClassNotFoundException
	 * @throws IllegalAccessException
	 * @throws InstantiationException
	 */
	private static void runApplication(Client2Coordinator client2Coordinator) {

		try {
			client2Coordinator.preProcess();
		} catch (Exception e) {
			log.error("message:" + e.getMessage());
			log.error("caused by" + e.getCause());
			e.printStackTrace();
		}
		// client2Coordinator.process();
	}
}
