package com.example.droolstestapp;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;

public class ProductDataGenerator implements InputOperator
{
  public DefaultOutputPort<Object> out = new DefaultOutputPort<>();
  private long totalTuplesEmitted = 0;
  private long tuplesCount = 10;

  @Override
  public void emitTuples()
  {
    if (totalTuplesEmitted < tuplesCount) {
      out.emit(getFact());
      totalTuplesEmitted++;
    }
  }
  private Object getFact()
  {
    Product fact = new Product(totalTuplesEmitted, totalTuplesEmitted % 2 == 1 ? "gold" : "diamond",0);
    return fact;
  }

  @Override
  public void beginWindow(long l)
  {

  }

  @Override
  public void endWindow()
  {

  }

  @Override
  public void setup(Context.OperatorContext context)
  {

  }

  @Override
  public void teardown()
  {

  }
}
