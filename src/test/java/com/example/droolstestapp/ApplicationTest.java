/**
 * Put your copyright and license info here.
 */
package com.example.droolstestapp;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.Callable;

import javax.validation.ConstraintViolationException;

import org.junit.Assert;
import org.junit.Test;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.LocalMode;
import com.datatorrent.stram.StramLocalCluster;

/**
 * Test the DAG declaration in local mode.
 */
public class ApplicationTest {

  @Test
  public void testApplication() throws IOException, Exception {
    try {
      LocalMode lma = LocalMode.newInstance();
      Configuration conf = new Configuration(false);
      conf.addResource(this.getClass().getResourceAsStream("/META-INF/properties-local.xml"));

      final String resultFolderPath = conf.get("dt.application.DroolsTestApplication.operator.finalOperator.prop.filePath");
      final String resultFileName = conf
        .get("dt.application.DroolsTestApplication.operator.finalOperator.prop.outputFileName");

      lma.prepareDAG(new Application(), conf);
      LocalMode.Controller lc = lma.getController();
      ((StramLocalCluster)lc).setExitCondition(new Callable<Boolean>()
      {
        @Override
        public Boolean call() throws Exception
        {
          if (new File(resultFolderPath).exists()) {
            Collection<File> files = FileUtils.listFiles(new File(resultFolderPath), new WildcardFileFilter(resultFileName
              + "*"), null);
            return files.size() >= 1;
          }
          return false;
        }
      });
      lc.run(30 * 1000); // runs for 30 seconds and quits


      Collection<File> files = FileUtils.listFiles(new File(resultFolderPath),
        new WildcardFileFilter(resultFileName + "*"), null);
      File resultFile = files.iterator().next();
      String fileData = FileUtils.readFileToString(resultFile);
      if(fileData.contains("all is not well")) {
        resultFile.delete();
        Assert.fail();
      }
      if(!fileData.contains("all is well factsValidated")) {
        resultFile.delete();
        Assert.fail();
      }
      if(!fileData.contains("all is well factRulesMapValidated")) {
        resultFile.delete();
        Assert.fail();
      }
      resultFile.delete();
    } catch (ConstraintViolationException e) {
      Assert.fail("constraint violations: " + e.getConstraintViolations());
    }
  }

}
