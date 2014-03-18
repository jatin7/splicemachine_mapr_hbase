import groovy.util.NodeBuilder
import groovy.util.XmlNodePrinter
import groovy.util.XmlParser
/**
 * @author Patrick Wong
 * I tried not to use too many Groovy-isms, except for the manipulation of Groovy Nodes.
 *
 * Running an XmlParser seems to be an expensive operation, unfortunately. The node clone and XML output is fast though.
 */
class GenerateUserFacingPoms {
    static void main(String[] args) {
        if (args.length != 5) {
            System.err.println """
Need exactly five args: the HBase version, the full mapr-hbase version, the short MapR version, whether this is release or snapshot, and m7 or 'none'

Example param 1
 - 0.94.13-mapr-1401
 - 0.94.17-mapr-1403

Example param 2
 - 1.0.3-mapr-3.1.0
 - 2.3.0-mapr-4.0.0-beta-SNAPSHOT

Example param 3:
 - 3.1.0
 - 4.0.0

Example param 4:
 - release
 - snapshot

Example param 5:
 - m7
 - none
"""
            System.exit(1)
        }
        String privateHbaseVersion = args[0]
        String fullMaprVersion = args[1]
        String shortMaprVersion = args[2]
        String snapshotSuffix = args[3]
        String m7String = args[4]
        println "Full HBase version was read as: " + privateHbaseVersion
        println "Full mapr-hbase version was read as: " + fullMaprVersion
        println "Short MapR version was read as: " + shortMaprVersion
        println "Snapshot or release parameter was read as: " + snapshotSuffix
        println "M7 or none was read as: " + m7String

        File originalPomFile = new File("pom.xml")
        XmlParser xmlIn = new XmlParser()
        xmlIn.setTrimWhitespace(true)
        Node pomTree = xmlIn.parse(originalPomFile)

        if (snapshotSuffix.equalsIgnoreCase("snapshot") || snapshotSuffix.equalsIgnoreCase("-SNAPSHOT")) {
            snapshotSuffix = "-SNAPSHOT"
        } else if (snapshotSuffix.equalsIgnoreCase("release")) {
            snapshotSuffix = ""
        } else {
            System.err.println "invalid snapshot or release parameter: " + snapshotSuffix
            System.err.println "it must be either snapshot or release"
            System.exit(1)
        }
        if (m7String.equalsIgnoreCase("m7")) {
            Node withM7Tree = makeM7Pom(pomTree, privateHbaseVersion, fullMaprVersion, shortMaprVersion, snapshotSuffix)
			writePom(withM7Tree, shortMaprVersion)
        } else {
            Node nonM7Tree = makeNonM7Pom(pomTree, privateHbaseVersion, fullMaprVersion, shortMaprVersion, snapshotSuffix)
			writePom(nonM7Tree, shortMaprVersion)
        }
    }

    private static void writePom(Node pomTree, String suffix) {
        StringWriter writer = new StringWriter()
        XmlNodePrinter xmlOut = new XmlNodePrinter(new PrintWriter(writer))
        xmlOut.setPreserveWhitespace(true)
        xmlOut.setExpandEmptyElements(false)
        xmlOut.print(pomTree)
        File outputPomFile = new File("pom-generated-" + suffix + ".xml")
        outputPomFile.write(writer.toString())
    }

    private static Node makeM7Pom(Node pomTree, String privateHbaseVersion, String fullMaprVersion, String shortMaprVersion, String snapshotSuffix) {
        Node newPomTree = pomTree.clone()
        newPomTree.version[0].setValue(privateHbaseVersion + "-m7-" + shortMaprVersion + snapshotSuffix)
        newPomTree.properties[0]."mapr.hadoop.version"[0].setValue(fullMaprVersion)
        newPomTree.dependencies[0].append(makeMaprHbaseDependency())
        return newPomTree
    }
	
	private static Node makeNonM7Pom(Node pomTree, String privateHbaseVersion, String fullMaprVersion, String shortMaprVersion, String snapshotSuffix) {
		Node newPomTree = pomTree.clone()
		newPomTree.version[0].setValue(privateHbaseVersion + "-" + shortMaprVersion + snapshotSuffix)
		newPomTree.properties[0]."mapr.hadoop.version"[0].setValue(fullMaprVersion)
		return newPomTree
	}

    private static Node makeMaprHbaseDependency() {
        NodeBuilder builder = new NodeBuilder()
        Node maprHbaseDependency = builder.dependency() {
            groupId "com.mapr.fs"
            artifactId "mapr-hbase"
            version "\${mapr.hadoop.version}"
            scope "runtime"
        }
        return maprHbaseDependency
    }
}