/*
 * Copyright 2010 LinkedIn Corp. and contributors
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
 
package datafu.pig.util;

import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Type;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;


/**
  Uses reflection to makes writing simple wrapper Pig UDFs easier.

  For example, writing a simple string trimming UDF might look like
  this:
  <pre>
  {@code
  public class TRIM extends EvalFunc<String> 
  {
    public String exec(Tuple input) throws IOException 
    {
      if (input.size() != 1)
        throw new IllegalArgumentException("requires a parameter");
  
      try {
        Object o = input.get(0);
        if (!(o instanceof String))
          throw new IllegalArgumentException("expected a string");
  
        String str = (String)o;
        return (str == null) ? null : str.trim();
      } 
      catch (Exception e) {
        throw WrappedIOException.wrap("error...", e);
      }
    }
  }
  }
  </pre>
  There is a lot of boilerplate to check the number of arguments and
  the parameter types in the tuple.

  Instead, with this class, you can derive from SimpleEvalFunc and
  create a <code>call()</code> method (not exec!), just specifying the
  arguments as a regular function. The class handles all the argument
  checking and exception wrapping for you. So your code would be:
  <pre>
  {@code
  public class TRIM2 extends SimpleEvalFunc<String> 
  {
    public String call(String s)
    {
      return (s != null) ? s.trim() : null;
    }
  }
  }
  </pre>

  An example of this UDF in action with Pig:
  <pre>
  {@code
  grunt> a = load 'test' as (x:chararray, y:chararray); dump a;
    (1 , 2)
    
  grunt> b = foreach a generate TRIM2(x); dump b;
    (1)
    
  grunt> c = foreach a generate TRIM2((int)x); dump c;
    datafu.pig.util.TRIM2(java.lang.String): argument type 
    mismatch [#1]; expected java.lang.String, got java.lang.Integer
    
  grunt> d = foreach a generate TRIM2(x, y); dump d;
    datafu.pig.util.TRIM2(java.lang.String): got 2 arguments, expected 1.
  }
  </pre>

*/

public abstract class SimpleEvalFunc<T> extends EvalFunc<T>
{
  // TODO Add support for other UDF types (e.g., FilterFunc)
  // TODO Algebraic EvalFuncs 
  
  Method m = null;

  public SimpleEvalFunc()
  {
    for (Method method : this.getClass().getMethods()) {
      if (method.getName() == "call")
        m = method;
    }
    if (m == null)
      throw new IllegalArgumentException(String.format("%s: couldn't find call() method in UDF.", getClass().getName()));
  }

  // Pig can't get the return type via reflection (as getReturnType normally tries to do), so give it a hand 
  @Override
  public Type getReturnType() 
  {
    return m.getReturnType();
  }

  private String _method_signature() 
  {
    StringBuilder sb = new StringBuilder(getClass().getName());
    Class<?> pvec[] = m.getParameterTypes();

    sb.append("(");
    for (int i=0; i < pvec.length; i++) {
      if (i > 0)
        sb.append(", ");
      sb.append(String.format("%s", pvec[i].getName()));
    }
    sb.append(")");

    return sb.toString();
  }
 
  @Override
  @SuppressWarnings("unchecked")
  public T exec(Tuple input) throws IOException
  {
    @SuppressWarnings("rawtypes")
    Class pvec[] = m.getParameterTypes();

    if (input == null || input.size() == 0)
      return null;
    
    // check right number of arguments
    if (input.size() != pvec.length) 
      throw new IOException(String.format("%s: got %d arguments, expected %d.", _method_signature(), input.size(), pvec.length));

    // pull and check argument types
    Object[] args = new Object[input.size()];
    for (int i=0; i < pvec.length; i++) {
      Object o = input.get(i);
      try {
        o = pvec[i].cast(o);
      }
      catch (ClassCastException e) {
        throw new IOException(String.format("%s: argument type mismatch [#%d]; expected %s, got %s", _method_signature(), i+1,
              pvec[i].getName(), o.getClass().getName()));
      }
      args[i] = o;
    }

    try {
      return (T) m.invoke(this, args);
    }
    catch (Exception e) {
        throw new IOException(String.format("%s: caught exception processing input.", _method_signature()), e);
    }
  }

  /**
   * Override outputSchema so we can verify the input schema at pig compile time, instead of runtime
   * @param inputSchema input schema
   * @return call to super.outputSchema in case schema was defined elsewhere
   */
  @Override
  public Schema outputSchema(Schema inputSchema)
  {
    if (inputSchema == null) {
      throw new IllegalArgumentException(String.format("%s: null schema passed to %s", _method_signature(), getClass().getName()));
    }

    // check correct number of arguments
    @SuppressWarnings("rawtypes")
    Class parameterTypes[] = m.getParameterTypes();
    if (inputSchema.size() != parameterTypes.length) {
      throw new IllegalArgumentException(String.format("%s: got %d arguments, expected %d.",
                                                       _method_signature(),
                                                       inputSchema.size(),
                                                       parameterTypes.length));
    }

    // check type for each argument
    for (int i=0; i < parameterTypes.length; i++) {
      try {
        byte inputType = inputSchema.getField(i).type;
        byte parameterType = DataType.findType(parameterTypes[i]);
        if (inputType != parameterType) {
          throw new IllegalArgumentException(String.format("%s: argument type mismatch [#%d]; expected %s, got %s",
                                                           _method_signature(),
                                                           i+1,
                                                           DataType.findTypeName(parameterType),
                                                           DataType.findTypeName(inputType)));
        }
      }
      catch (FrontendException fe) {
        throw new IllegalArgumentException(String.format("%s: Problem with input schema: ", _method_signature(), inputSchema), fe);
      }
    }

    // delegate to super to determine the actual outputSchema (if specified)
    return super.outputSchema(inputSchema);
  }
}

