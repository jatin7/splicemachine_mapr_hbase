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
        if (args.length < 2) {
            System.err << "Need two args: the full HBase version, and the full mapr-hbase version"
            System.exit(1)
        } else if (args.length > 2) {
            System.err << "Too many args. Need exactly two args: the full HBase version, and the full mapr-hbase version"
            System.exit(1)
        }
        String privateHbaseVersion = args[0]
        String maprHbaseVersion = args[1]
        println "Full HBase version was read as: " + privateHbaseVersion
        println "Full mapr-hbase version was read as: " + maprHbaseVersion

        File originalPomFile = new File("pom.xml")
        XmlParser xmlIn = new XmlParser()
        xmlIn.setTrimWhitespace(true)
        Node pomTree = xmlIn.parse(originalPomFile)

        Node newPomTree = pomTree.clone()
        newPomTree.version[0].setValue(privateHbaseVersion)
        newPomTree.properties[0]."mapr.hadoop.version"[0].setValue(maprHbaseVersion)

        NodeBuilder builder = new NodeBuilder()
        Node maprHbaseDependency = builder.dependency() {
            groupId "com.mapr.fs"
            artifactId "mapr-hbase"
            version "\${mapr.hadoop.version}"
            scope "runtime"
        }
        newPomTree.dependencies[0].append(maprHbaseDependency)

        StringWriter writer = new StringWriter()
        XmlNodePrinter xmlOut = new XmlNodePrinter(new PrintWriter(writer))
        xmlOut.setPreserveWhitespace(true)
        xmlOut.setExpandEmptyElements(false)
        xmlOut.print(newPomTree)
        File outputPomFile = new File("pom-userfacing-" + maprHbaseVersion + "-generated.xml")
        outputPomFile.write(writer.toString())
    }
}