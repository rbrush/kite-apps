package org.kitesdk.apps.spi.oozie;

import junit.framework.Assert;
import org.w3c.dom.Document;

import javax.xml.namespace.NamespaceContext;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathFactory;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.Iterator;

/**
 * Helper to work with Oozie XML.
 */
public class XMLUtil {

  private static final NamespaceContext CONTEXT = new NamespaceContext() {

    public String getNamespaceURI(String prefix) {

      if ("wf".equals(prefix))
        return OozieScheduling.OOZIE_WORKFLOW_NS;

      if ("coord".equals(prefix))
        return OozieScheduling.OOZIE_COORD_NS;

      if ("bn".equals(prefix))
        return OozieScheduling.OOZIE_BUNDLE_NS;

      if ("sp".equals(prefix))
        return OozieScheduling.OOZIE_SPARK_NS;

      throw new IllegalArgumentException("Unknown prefix:" + prefix);
    }

    public Iterator getPrefixes(String val) {
      return null;
    }

    public String getPrefix(String uri) {

      if (OozieScheduling.OOZIE_WORKFLOW_NS.equals(uri))
        return "wf";

      if (OozieScheduling.OOZIE_COORD_NS.equals(uri))
        return "coord";

      if (OozieScheduling.OOZIE_BUNDLE_NS.equals(uri))
        return "bn";

      if (OozieScheduling.OOZIE_SPARK_NS.equals(uri))
        return "sp";

      throw new IllegalArgumentException("Unknown uri:" + uri);
    }
  };
  private static javax.xml.xpath.XPath XPath;


  public static Document toDom(ByteArrayOutputStream output) throws Exception {

    // TODO: validate as part of document builder.
    InputStream input = new ByteArrayInputStream(output.toByteArray());

    try {
      // Get Oozie schemas from the oozie-client JAR.
      StreamSource[] sources = new StreamSource[] {
          new StreamSource(Thread.currentThread().getContextClassLoader().getResourceAsStream("oozie-workflow-0.5.xsd")),
          new StreamSource(Thread.currentThread().getContextClassLoader().getResourceAsStream("oozie-coordinator-0.4.xsd")),
          new StreamSource(Thread.currentThread().getContextClassLoader().getResourceAsStream("oozie-bundle-0.2.xsd")),
          new StreamSource(Thread.currentThread().getContextClassLoader().getResourceAsStream("spark-action-0.1.xsd"))};

      SchemaFactory factory = SchemaFactory.newInstance("http://www.w3.org/2001/XMLSchema");
      Schema schema = factory.newSchema(sources);
      Validator validator = schema.newValidator();

      try {
        validator.validate(new StreamSource(input));
      } finally {

        input.close();
      }

    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }

    DocumentBuilderFactory docBuilderFactory
        = DocumentBuilderFactory.newInstance();
    //ignore all comments inside the xml file
    docBuilderFactory.setIgnoringComments(true);

    //allow includes in the xml file
    docBuilderFactory.setNamespaceAware(true);
    try {
      docBuilderFactory.setXIncludeAware(true);
    } catch (UnsupportedOperationException e) {

    }

    DocumentBuilder builder = docBuilderFactory.newDocumentBuilder();

    return builder.parse(new ByteArrayInputStream(output.toByteArray()));
  }

  public static XPath getXPath() {

    XPath xpath = XPathFactory.newInstance().newXPath();
    xpath.setNamespaceContext(CONTEXT);

    return xpath;
  }
}
