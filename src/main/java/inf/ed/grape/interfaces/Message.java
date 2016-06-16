package inf.ed.grape.interfaces;

import java.io.Serializable;

public class Message<T> implements Serializable {

	private static final long serialVersionUID = -1L;

	private int sourcePartitionID;

	private int destinationPartitionID;

	private T content;

	public Message(int fromPartitionID, int destination, T content) {
		super();
		this.sourcePartitionID = fromPartitionID;
		this.destinationPartitionID = destination;
		this.content = content;
	}

	public int getSourcePartitionID() {
		return this.sourcePartitionID;
	}

	public int getDestinationPartitionID() {
		return this.destinationPartitionID;
	}

	public T getContent() {
		return this.content;
	}

	@Override
	public String toString() {
		return "Message [" + content + "] ," + sourcePartitionID + "-> " + destinationPartitionID;
	}
}
