package com.example.droolstestapp;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.common.util.BaseOperator;

public class DroolsValidator extends BaseOperator
{
  @Override
  public void endWindow()
  {
    if (factsValidated && factRuleMapValidated) {
      output.emit(finalOuput.toString());
    }
  }

  public DroolsValidator()
  {

  }
  private List<Object> incomingFacts = new ArrayList<>();
  private int totalTuples = 10;
  boolean factsValidated = false;
  boolean factRuleMapValidated = false;
  StringBuilder finalOuput = new StringBuilder();
  Map<Object, List<String>> factRules = new HashMap<>();
  public transient DefaultInputPort<Object> factsInput = new DefaultInputPort<Object>()
  {
    @Override
    public void process(Object fact)
    {
      incomingFacts.add(fact);
      if (incomingFacts.size() == totalTuples +1 ) {
        validateAndOutputResult();
        factsValidated = true;
      }
    }
  };
  @InputPortFieldAnnotation(optional = true)
  public transient DefaultInputPort<ConcurrentMap<Object,List<String>>> factRuleInput = new DefaultInputPort<ConcurrentMap<Object, java.util.List<String>>>()
  {
    @Override
    public void process(ConcurrentMap<Object, List<String>> objectListMap)
    {
      for (Map.Entry<Object,List<String>> entry : objectListMap.entrySet()) {
        factRules.put(entry.getKey(),entry.getValue());
      }
      validateFactRuleMap();

    }
  };
  public transient DefaultOutputPort<String> output = new DefaultOutputPort<>();

  public void validateFactRuleMap()
  {

    if(factRules.size()==11) {
      //check map
      boolean allIsWell = true;
      int platinumCount = 0;
      for (Map.Entry entry : factRules.entrySet())
      {
        Product product = (Product) entry.getKey();
        List<String> ruleList = (List<String>) entry.getValue();
        if(product.getType().equalsIgnoreCase("gold")  ) {
          if(product.getDiscount() != 22 ) {
            allIsWell = false;
            finalOuput.append("all is not well discount mismatch for gold");
          }
          if (ruleList.size() !=1 ) {
            allIsWell = false;
            finalOuput.append("all is not well rule List mismatch for gold");
          }

        }
        else if(product.getType().equalsIgnoreCase("diamond") ) {
          if(product.getDiscount() != 15 ) {
            allIsWell = false;
            finalOuput.append("all is not well discount mismatch for diamond");
          }
          if (product.getProductId() == 2 && ruleList.size() !=2 ) {
            allIsWell = false;
            finalOuput.append("all is not well rule List mismatch for diamond");
          } else if (product.getProductId() != 2 && ruleList.size() !=1 ) {
            allIsWell = false;
            finalOuput.append("all is not well rule List mismatch for diamond");
          }
        }
        else if(product.getType().equalsIgnoreCase("platinum")) {
          platinumCount++;
        }
      }
      if (platinumCount != 1) {
        finalOuput.append("all is not well platinum not found");
      } else if (allIsWell){
        finalOuput.append("all is well factRulesMapValidated");
      }
      factRuleMapValidated = true;
    }
    else if(factRules.size()>11) {
      finalOuput.append("all is not well factRulesMapValidated");
      factRuleMapValidated = true;
    }

  }

  public void validateAndOutputResult()
  {
    boolean allIsWell = true;
    int platinumCount = 0;
    for (Object fact : incomingFacts) {
      Product product = (Product) fact;
      if(product.getType().equalsIgnoreCase("gold") && product.getDiscount() != 22) {
        allIsWell = false;
        finalOuput.append("all is not well discount mismatch for gold");
      }
      else if(product.getType().equalsIgnoreCase("diamond") && product.getDiscount() != 15) {
        allIsWell = false;
        finalOuput.append("all is not well discount mismatch for diamond");
      }
      else if(product.getType().equalsIgnoreCase("platinum")) {
        platinumCount++;
      }
    }
    if (platinumCount != 1) {
      finalOuput.append("all is not well platinum not found");
    } else if (allIsWell){
      finalOuput.append("all is well factsValidated");
    }

  }

}
