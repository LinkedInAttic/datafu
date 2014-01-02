package datafu.hourglass.jobs;

public class MaxInputDataExceededException extends Throwable
{
  public MaxInputDataExceededException()
  {    
  }
  
  public MaxInputDataExceededException(String message)
  {
    super(message);
  }
}
