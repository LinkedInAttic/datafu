package datafu.pig.util;

import java.util.Properties;

import org.apache.pig.EvalFunc;
import org.apache.pig.impl.util.UDFContext;

/**
 * An abstract class which enables UDFs to store instance properties
 * on the front end which will be available on the back end.
 * For example, properties may be set in the call to outputSchema(),
 * which will be available when exec() is called.
 * 
 * @param <T>
 */
public abstract class ContextualEvalFunc<T> extends EvalFunc<T>
{
  private String instanceName;
  
  @Override
  public void setUDFContextSignature(String signature) {
    setInstanceName(signature);
  }
  
  /**
   * Helper method to return the context properties for this class
   * 
   * @return context properties
   */
  protected Properties getContextProperties() {
    UDFContext context = UDFContext.getUDFContext();
    Properties properties = context.getUDFProperties(this.getClass());
    return properties;
  }
  
  /**
   * Helper method to return the context properties for this instance of this class
   * 
   * @return instances properties
   */
  protected Properties getInstanceProperties() {
    Properties contextProperties = getContextProperties();
    if (!contextProperties.containsKey(getInstanceName())) {
      contextProperties.put(getInstanceName(), new Properties());
    }
    return (Properties)contextProperties.get(getInstanceName());
  }
  
  /**
   * 
   * @return the name of this instance corresponding to the UDF Context Signature
   * @see #setUDFContextSignature(String)
   */
  protected String getInstanceName() {
    if (instanceName == null) {
      throw new RuntimeException("Instance name is null.  This should not happen unless UDFContextSignature was not set.");
    }
    return instanceName;
  }
  
  private void setInstanceName(String instanceName) {
    this.instanceName = instanceName;
  }
}
