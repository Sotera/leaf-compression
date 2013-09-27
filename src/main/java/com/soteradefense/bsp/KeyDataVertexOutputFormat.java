package com.soteradefense.bsp;

import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;


// INPUT FORMAT
public class KeyDataVertexOutputFormat extends TextVertexOutputFormat<Text, VIntWritable, Text> {

    /**
     * Reads the input and converts each line to a vertex.
     * Creates a link from each vertex to the previous one for communication during the computation.
     */
    private class KeyDataVertexWriter extends TextVertexWriter {

        @Override
        public void writeVertex(Vertex<Text, VIntWritable, Text, ?> vertex)
                throws IOException, InterruptedException {
            if (!vertex.getValue().toString().isEmpty()) {
                getRecordWriter().write(new Text(vertex.getId().toString()), new Text(vertex.getValue().toString()));
            }

        }
    }

    @Override
    public TextVertexWriter createVertexWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        return new KeyDataVertexWriter();
    }

}

