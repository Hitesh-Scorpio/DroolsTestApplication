/**
 * Put your copyright and license info here.
 */
package com.example.droolstestapp;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.drools.operator.DroolsStatelessOperator;

@ApplicationAnnotation(name="DroolsTestApplication")
public class Application implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    ProductDataGenerator dataGenerator = dag.addOperator("dataGenerator", ProductDataGenerator.class);
    DroolsStatelessOperator droolsOperator = dag.addOperator("droolsOperator", DroolsStatelessOperator.class);
    DroolsValidator droolsValidator = dag.addOperator("droolsValidator", DroolsValidator.class);
    FileOutputOperator finalOperator = dag.addOperator("finalOperator", FileOutputOperator.class);

    dag.addStream("data to drools", dataGenerator.out,droolsOperator.factsInput);
    dag.addStream("drools to validate", droolsOperator.factsOutput, droolsValidator.factsInput);
    dag.addStream("drools factRule map", droolsOperator.factAndFiredRulesOutput,droolsValidator.factRuleInput);
    dag.addStream("validator to file", droolsValidator.output,finalOperator.input);
  }
}
