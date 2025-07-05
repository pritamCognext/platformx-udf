/*
 * Copyright (C) 2021, Cognext Analytics All rights reserved. 
 *
 * File Name	: EncryptionUDF.java
 * 
 * Created By	: Mukund
 * 
 * Created Date	: 03/09/2021
 */
package com.platformx.framework.udf;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.SecretKeySpec;
import javax.xml.bind.DatatypeConverter;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.bouncycastle.crypto.CryptoException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

	
@Component
public class EncryptionUDF implements Serializable {

	private static final String ALGORITHM = "AES";
	private static final String TRANSFORMATION = "AES";
	private static SecretKeySpec secretKeySpec;
	@Value("${framework.encryption.secretkey}")
    private String  secretKey;
	
	private static final long serialVersionUID = 1L;

	public EncryptionUDF(SQLContext sqlContext) {
		
		sqlContext.udf().register("encryptCol",
			    (String input) -> StringUtils.isNotBlank(input) ? encrypt(secretKey, input).toString() :input, DataTypes.StringType);
		
		sqlContext.udf().register("decryptCol",
			    (String input) -> StringUtils.isNotBlank(input) ? decrypt(secretKey, input).toString() : input, DataTypes.StringType);
		
	}
	
	public static void setKey(String myKey) {
		byte[] decodedKey = DatatypeConverter.parseHexBinary(myKey);	
		secretKeySpec = new SecretKeySpec(Arrays.copyOf(decodedKey, 16), ALGORITHM);		
	}
		
	public String encrypt(String key, String inputStringToEncrypt) throws CryptoException {
		return doCryptoencrypt(Cipher.ENCRYPT_MODE, key, inputStringToEncrypt);
	}
	
	public String decrypt(String key, String inputStringToDecrypt) throws CryptoException {
		return doCryptodecrypt(Cipher.DECRYPT_MODE, key, inputStringToDecrypt);
	}
	
	private static String doCryptoencrypt(int cipherMode, String key, String strToEncrypt) throws CryptoException{
		try {			
				setKey(key);
				Cipher cipher = Cipher.getInstance(TRANSFORMATION);
				cipher.init(Cipher.ENCRYPT_MODE, secretKeySpec);
				return DatatypeConverter.printHexBinary(cipher.doFinal(strToEncrypt.getBytes("UTF-8")));
			} catch (NoSuchPaddingException | NoSuchAlgorithmException | InvalidKeyException | BadPaddingException
				| IllegalBlockSizeException | UnsupportedEncodingException ex) {
			throw new CryptoException("Error encrypting/decrypting file : " +  ex.toString());
		}
	}

	private static String doCryptodecrypt(int cipherMode, String key, String strToDecrypt) throws CryptoException {
		String decryptedString = null;
		try {			
			setKey(key);
			Cipher cipher = Cipher.getInstance(TRANSFORMATION);
			cipher.init(cipherMode, secretKeySpec);
			decryptedString = new String(cipher.doFinal(DatatypeConverter.parseHexBinary(strToDecrypt)));
			return decryptedString;            
		} catch (NoSuchPaddingException | NoSuchAlgorithmException | InvalidKeyException | BadPaddingException
				| IllegalBlockSizeException ex) {
			throw new CryptoException("Error encrypting/decrypting " + ex);
		}
	}	
	
}