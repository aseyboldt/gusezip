package gusezip;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;


/**
 * Manage gUSE workflow zip files.
 * 
 * gUSE zip files describe a workflow. They consist of a workflow.xml
 * file, that contains a description of the edges of the workflow
 * graph, and for each node they contain a directory with exactly
 * one file -- a script -- that will be executed on a compute node.
 * 
 *  TODO
 *  
 * 
 * @author Adrian Seyboldt
 *
 */
public class GUSEZip {
	private URI zipuri;
	private Map<String, GUSENode> nodes;
	private String basename;
	private byte[] workflow;
	
	public GUSEZip(URI zipfile) throws IOException{
		zipuri = zipfile;
		nodes = new HashMap<>();
		readFromStream(getZipStream(zipuri));
	}
	
	private ZipInputStream getZipStream(URI zipuri) throws IOException {
		//return new ZipInputStream(getContext().getResource(zipuri));
		return new ZipInputStream(new FileInputStream(new File(zipuri)));
	}
	
	private void readFromStream(ZipInputStream stream) throws IOException {
		ZipEntry zentry;
		while ((zentry = stream.getNextEntry()) != null) {
			String name = zentry.getName();
			if (name.equals("workflow.xml")) {
				ByteArrayOutputStream out = new ByteArrayOutputStream();
				int b = stream.read();
				while (b != -1) {
				    out.write(b);
				    b = stream.read();
				}
				workflow = out.toByteArray();
			}
			
			String[] parts = name.split("/");
			if (parts.length < 3) {
				continue;
			} 
			
			if (parts.length > 3 || zentry.isDirectory()) {
				throw new RuntimeException("Unexpected third directory level");
			} 
		
			if (this.basename == null) {
				this.basename = parts[0];
			}
			assert this.basename.equals(parts[0]);
			
			if (!this.nodes.containsKey(parts[1])) {
				this.nodes.put(parts[1], new GUSENode(parts[1]));
			}
			
			this.nodes.get(parts[1]).addFile(parts[2], stream);
		
		}
		
		if (workflow == null) {
			throw new RuntimeException("Could not find workflow.xml");
		}
	}
	
	/**
	 * 
	 * @return The gUSE-ready zip file
	 * @throws IOException
	 */
	public InputStream asZip() throws IOException {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		ZipOutputStream zipstream = new ZipOutputStream(out);
		
		ZipEntry zentry = new ZipEntry(this.basename + "/");
		zipstream.putNextEntry(zentry);
		
		for (GUSENode node: nodes.values()) {
			node.writeInto(basename, zipstream);
		}
		
		zentry = new ZipEntry("workflow.xml");
		zipstream.putNextEntry(zentry);
		zipstream.write(workflow);
		
		zipstream.finish();
		return new ByteArrayInputStream(out.toByteArray());
	}
	
	public void addFile(String nodeName, String fileName, InputStream file)
			throws IOException {
		nodes.get(nodeName).addFile(fileName, file);
	}
	
	public InputStream getFile(String nodeName, String fileName) {
		return nodes.get(nodeName).getFile(fileName);
	}
	
	public Set<String> getJobNames() {
		return nodes.keySet();
	}
	
	public static void main(String[] args) throws IOException, URISyntaxException {
		GUSEZip wf = new GUSEZip(new URI("file:///home/adr/workspace/gusezip/workflow.zip"));
		FileOutputStream out = new FileOutputStream("/home/adr/workspace/gusezip/workflow_out.zip");
		InputStream zipstream = wf.asZip();
		
		int b;
		while ((b = zipstream.read()) != -1) { 
			out.write(b);
		}
		out.close();
	}

	private class GUSENode {
		String name;
		Map<String, byte[]> files;
		
		public GUSENode(String name) {
			this.name = name;
			files = new HashMap<>();
		}
		
		public void addFile(String name, InputStream file) throws IOException {
			ByteArrayOutputStream out = new ByteArrayOutputStream();

			int b = file.read();
			while (b != -1) {
			    out.write(b);
			    b = file.read();
			}
			files.put(name, out.toByteArray());
		}
		
		public InputStream getFile(String name) {
			return new ByteArrayInputStream(files.get(name));
		}
		
		public void writeInto(String basename, ZipOutputStream stream) throws IOException {
			ZipEntry dir = new ZipEntry(basename + "/" + name + "/");
			stream.putNextEntry(dir);
			for (String fname: files.keySet()) {
				ZipEntry zfile = new ZipEntry(basename + "/" + name + "/" + fname);
				stream.putNextEntry(zfile);
				stream.write(files.get(fname));
			}
		}
	}
}