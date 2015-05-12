package org.kitesdk.apps.spi.oozie;

import junit.framework.Assert;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.cli.OozieCLIException;

import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;
import java.io.FileReader;
import java.io.InputStream;
import java.util.ArrayList;

/**
 * Created by rb4106 on 4/24/15.
 */
public class OozieTestUtils {

  /**
   * Validates the file written at the given path is a valid Oozie bundle, coordinator
   * or workflow.
   */
  public static void validateSchema(FileSystem fs, Path path) {

    try {
      ArrayList ex = new ArrayList();

      // Get Oozie schemas from the oozie-client JAR.
      StreamSource[] sources = new StreamSource[] {
          new StreamSource(Thread.currentThread().getContextClassLoader().getResourceAsStream("oozie-workflow-0.5.xsd")),
          new StreamSource(Thread.currentThread().getContextClassLoader().getResourceAsStream("oozie-coordinator-0.4.xsd")),
          new StreamSource(Thread.currentThread().getContextClassLoader().getResourceAsStream("oozie-bundle-0.2.xsd"))};

      SchemaFactory factory = SchemaFactory.newInstance("http://www.w3.org/2001/XMLSchema");
      Schema schema = factory.newSchema(sources);
      Validator validator = schema.newValidator();

      InputStream input = fs.open(path);
      try {
        validator.validate(new StreamSource(input));
      } finally {

        input.close();
      }

    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
  }

}
