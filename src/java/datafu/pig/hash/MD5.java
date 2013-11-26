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
 
package datafu.pig.hash;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.commons.codec.binary.Base64;

import datafu.pig.util.SimpleEvalFunc;

/**
 * Computes the MD5 value of a string and outputs it in hex (by default).
 * A method can be provided to the constructor, which may be either 'hex' or 'base64'.
 */
public class MD5 extends SimpleEvalFunc<String>
{
  private final MessageDigest md5er;
  private final boolean isBase64;
  
  public MD5()
  {
    this("hex");
  }
  
  public MD5(String method)
  {
    if ("hex".equals(method))
    {
      isBase64 = false;
    }
    else if ("base64".equals(method))
    {
      isBase64 = true;
    }
    else
    {
      throw new IllegalArgumentException("Expected either hex or base64");
    }
    
    try {
      md5er = MessageDigest.getInstance("md5");
    }
    catch (NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
    }
  }
  
  public String call(String val)
  {
    if (isBase64)
    {
      return new String(Base64.encodeBase64(md5er.digest(val.getBytes())));
    }
    else
    {
      return new BigInteger(1, md5er.digest(val.getBytes())).toString(16);
    }
  }
}
