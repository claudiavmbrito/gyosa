package io.projectglow.vcf

import javax.crypto.Cipher
import javax.crypto.spec.{GCMParameterSpec, SecretKeySpec}
import org.apache.spark.sql.{DataFrame, SparkSession}
import java.util.Base64
import java.security.SecureRandom

class VCFAESEncryption(key: String) {

  private val ALGORITHM = "AES"
  private val TRANSFORMATION = "AES/GCM/PKCS5Padding"
  private val AES_KEY_SIZE = 128
  private val GCM_NONCE_LENGTH = 12  // standard nonce length for GCM mode
  private val GCM_TAG_LENGTH = 16 * 8 // in bits

  private val secureRandom = new SecureRandom()

  private def getCipher(mode: Int, iv: Array[Byte] = Array.empty[Byte]): Cipher = {
    val cipher = Cipher.getInstance(TRANSFORMATION)
    val secretKey = new SecretKeySpec(key.getBytes, ALGORITHM)
    
    if (iv.isEmpty) {
      cipher.init(mode, secretKey)
    } else {
      cipher.init(mode, secretKey, new GCMParameterSpec(GCM_TAG_LENGTH, iv))
    }
    cipher
  }

  def encryptVCF(df: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    df.map { row =>
      val line = row.mkString("\t")
      val nonce = new Array[Byte](GCM_NONCE_LENGTH)
      secureRandom.nextBytes(nonce)
      val cipher = getCipher(Cipher.ENCRYPT_MODE, nonce)
      val encryptedBytes = cipher.doFinal(line.getBytes)
      Base64.getEncoder.encodeToString(nonce ++ encryptedBytes)
    }.toDF("encrypted")
  }

  def decryptVCF(df: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    df.map { row =>
      val encryptedDataWithNonce = Base64.getDecoder.decode(row.getAs[String]("encrypted"))
      val (nonce, encryptedData) = encryptedDataWithNonce.splitAt(GCM_NONCE_LENGTH)
      val decryptedBytes = getCipher(Cipher.DECRYPT_MODE, nonce).doFinal(encryptedData)
      new String(decryptedBytes)
    }.toDF("decrypted")
  }
}

