package com.soteradefense.bsp;

import java.io.IOException;

import org.apache.giraph.GiraphRunner;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.util.ToolRunner;

/**
 * Vertex class used to read in email edges lists
 * <p/>
 * INPUT FORMAT -
 * <src>\t<dest:date1,date2,date3>|<dest:date1,date2,date3,date4>... each srckey
 * is unique in the input. The row_id is an increasing sequential integer. it is
 * used to allow each vertex to pass messages to the next vertex
 * <p/>
 * OUTPUT FORMAT - tab separated list of <key>,<key>,<value> pairs
 */
public class KeyDataVertex extends Vertex<Text, VIntWritable, Text, Text> {

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new GiraphRunner(), args));
	}

	/**
	 * The actual algorithm. See class documentation for description.
	 */
	@Override
	public void compute(Iterable<Text> messages) throws IOException {
		try {

			// Check to see if we received any messages from nodes notifying
			// that they have only a single edge
			for (Text incomingMessage : messages) {
				Text vertex = new Text(incomingMessage.toString().split(":")[0]);
				int value = Integer.parseInt(incomingMessage.toString().split(":")[1]);
				setValue(new VIntWritable(getValue().get() + 1 + value));

				// Remove the vertex and its corresponding edge
				removeVertexRequest(vertex);
				removeEdges(vertex);
				// System.err.println("Removing: " + vertex.toString());
			}

			// Broadcast the edges if we only have a single edge
			sendEdges();
		} catch (Exception e) {
			System.err.println(e.toString());
		}
	}

	private void sendEdges() {
		// System.err.println("Superstep: " + String.valueOf(getSuperstep()) +
		// " - EDGES:" + getId().toString() + ':' +
		// String.valueOf(getNumEdges()));
		if (getNumEdges() == 1 && getValue().get() != -1) {
			for (Edge<Text, Text> edge : getEdges()) {
				sendMessage(edge.getTargetVertexId(), new Text(getId().toString() + ":" + getValue().toString()));
			}
			setValue(new VIntWritable(-1));
			// This node will never vote to halt, but will simply be deleted.
		} else if (getValue().get() != -1) {
			// If we aren't being imminently deleted
			voteToHalt();
		}
	}

}
