/*
 * Copyright 2010 LinkedIn, Inc
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

import datafu.pig.util.SimpleEvalFunc;

/**
 * Computes the MD5 value of a string and outputs it in hex. 
 */
public class MD5 extends SimpleEvalFunc<String>
{
    private final MessageDigest md5er;

    public MD5()
    {
        try {
            md5er = MessageDigest.getInstance("md5");
        }
        catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    public String call(String val)
    {
        return new BigInteger(1, md5er.digest(val.getBytes())).toString(16);
    }
}
