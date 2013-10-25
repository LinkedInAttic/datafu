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
