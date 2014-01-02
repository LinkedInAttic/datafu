/*
 * Copyright 2013 LinkedIn Corp. and contributors
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

public class SHA extends SimpleEvalFunc<String> {
	private final MessageDigest sha;

	public SHA(){
		this("256");
	}
	
	public SHA(String algorithm){
		try {
			sha = MessageDigest.getInstance("SHA-"+algorithm);
		} catch (NoSuchAlgorithmException e) {
			throw new RuntimeException(e);
		}
	}
	
	public String call(String value){
		return new BigInteger(1, sha.digest(value.getBytes())).toString(16);
	}
}
