package org.kitesdk.apps.spi.oozie;

import junit.framework.Assert;
import org.w3c.dom.Document;
import org.xml.sax.ErrorHandler;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

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

    InputStream input = new ByteArrayInputStream(output.toByteArray());

    return toDom(input);
  }

  public static Document toDom(InputStream input) throws Exception {

    StreamSource[] sources = new StreamSource[] {
        new StreamSource(Thread.currentThread().getContextClassLoader().getResourceAsStream("oozie-workflow-0.5.xsd")),
        new StreamSource(Thread.currentThread().getContextClassLoader().getResourceAsStream("oozie-coordinator-0.4.xsd")),
        new StreamSource(Thread.currentThread().getContextClassLoader().getResourceAsStream("oozie-bundle-0.2.xsd")),
        new StreamSource(Thread.currentThread().getContextClassLoader().getResourceAsStream("spark-action-0.1.xsd"))};

    SchemaFactory schemaFactory = SchemaFactory.newInstance("http://www.w3.org/2001/XMLSchema");
    Schema schema = schemaFactory.newSchema(sources);

    DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();

    factory.setIgnoringComments(true);
    factory.setNamespaceAware(true);
    factory.setSchema(schema);

    factory.setXIncludeAware(true);

    DocumentBuilder builder = factory.newDocumentBuilder();

    // To prevent the DOM API from complaining...
    builder.setErrorHandler(new ErrorHandler() {
      @Override
      public void warning(SAXParseException exception) throws SAXException {

        throw exception;
      }

      @Override
      public void error(SAXParseException exception) throws SAXException {
        throw exception;
      }

      @Override
      public void fatalError(SAXParseException exception) throws SAXException {
        throw exception;
      }
    });

    return builder.parse(input);
  }

  public static XPath getXPath() {

    XPath xpath = XPathFactory.newInstance().newXPath();
    xpath.setNamespaceContext(CONTEXT);

    return xpath;
  }
}
