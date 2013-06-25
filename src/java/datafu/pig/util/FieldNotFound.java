package datafu.pig.util;

import org.apache.pig.backend.executionengine.ExecException;

public class FieldNotFound extends ExecException
{
  private static final long serialVersionUID = 1L;
  
  public FieldNotFound() {
    super();
  }
  
  public FieldNotFound(String message) {
    super(message);
  }

}
