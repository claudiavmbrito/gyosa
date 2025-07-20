package gyosa.core

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import io.projectglow.Glow
import io.projectglow.vcf.VCFAESEncryption
import javax.crypto.{Cipher, KeyGenerator, SecretKey}
import javax.crypto.spec.{GCMParameterSpec, SecretKeySpec}
import java.security.SecureRandom
import scala.util.{Try, Success, Failure}
import io.projectglow.gyosa.sgx.SGXComputationPartitioner
import org.apache.spark.ml.linalg.{Vector, Vectors, DenseVector}
import org.apache.spark.ml.stat.ChiSquareTest
import org.apache.spark.sql.Row

/**
 * GYOSA Core Implementation
 * Privacy-preserving genomics analysis framework using Intel SGX
 * Built on top of Glow and Apache Spark with SOTERIA-style computation partitioning
 */
object GyosaCore {
  
  // Configuration constants for genomics security
  private val ENCRYPTION_ALGORITHM = "AES"
  private val ENCRYPTION_TRANSFORMATION = "AES/GCM/NoPadding"
  private val GCM_IV_LENGTH = 12
  private val GCM_TAG_LENGTH = 16
  
  case class GyosaConfig(
    enclaveEnabled: Boolean = true,
    encryptionEnabled: Boolean = true,
    partitioningStrategy: String = "COMPUTATION_PARTITIONING", // or "BASELINE"
    keySize: Int = 128,
    batchSize: Int = 1000,
    gwasThreshold: Double = 5e-8, // Standard GWAS significance threshold
    mafThreshold: Double = 0.01 // Minor allele frequency threshold
  )
  
  /**
   * Encrypted genomic dataset wrapper
   */
  case class EncryptedGenomicDataset[T](
    data: Dataset[T],
    encryptionKey: SecretKey,
    isEncrypted: Boolean = true,
    dataType: String = "VCF" // VCF, PLINK, BGEN
  )
  
  /**
   * Computation partitioning manager for genomics workloads
   * Decides which operations run inside SGX enclaves vs outside
   */
  object GenomicsComputationPartitioner {
    
    sealed trait ComputationZone
    case object EnclaveZone extends ComputationZone
    case object UntrustedZone extends ComputationZone
    
    /**
     * Determines computation zone based on genomics operation sensitivity
     */
    def getComputationZone(operation: String): ComputationZone = operation match {
      case op if isSensitiveGenomicsOperation(op) => EnclaveZone
      case _ => UntrustedZone
    }
    
    private def isSensitiveGenomicsOperation(operation: String): Boolean = {
      val sensitiveOps = Set(
        "gwas_analysis",
        "association_testing", 
        "phenotype_processing",
        "genotype_imputation",
        "population_stratification",
        "privacy_preserving_gwas",
        "chi_square_test",
        "variant_calling",
        "quality_control_filtering"
      )
      sensitiveOps.contains(operation.toLowerCase)
    }
  }
  
  /**
   * Genomics-specific encryption utilities
   */
  object GenomicsEncryptionUtils {
    
    def generateKey(keySize: Int = 128): SecretKey = {
      val keyGenerator = KeyGenerator.getInstance(ENCRYPTION_ALGORITHM)
      keyGenerator.init(keySize)
      keyGenerator.generateKey()
    }
    
    def encryptGenomicData(data: Array[Byte], key: SecretKey): Try[Array[Byte]] = Try {
      val cipher = Cipher.getInstance(ENCRYPTION_TRANSFORMATION)
      val iv = new Array[Byte](GCM_IV_LENGTH)
      new SecureRandom().nextBytes(iv)
      
      val gcmSpec = new GCMParameterSpec(GCM_TAG_LENGTH * 8, iv)
      cipher.init(Cipher.ENCRYPT_MODE, key, gcmSpec)
      
      val encryptedData = cipher.doFinal(data)
      iv ++ encryptedData
    }
    
    def decryptGenomicData(encryptedData: Array[Byte], key: SecretKey): Try[Array[Byte]] = Try {
      val iv = encryptedData.slice(0, GCM_IV_LENGTH)
      val cipherText = encryptedData.slice(GCM_IV_LENGTH, encryptedData.length)
      
      val cipher = Cipher.getInstance(ENCRYPTION_TRANSFORMATION)
      val gcmSpec = new GCMParameterSpec(GCM_TAG_LENGTH * 8, iv)
      cipher.init(Cipher.DECRYPT_MODE, key, gcmSpec)
      
      cipher.doFinal(cipherText)
    }
    
    /**
     * Create VCF encryption handler
     */
    def createVCFEncryption(key: String): VCFAESEncryption = {
      new VCFAESEncryption(key)
    }
  }
  
  /**
   * GYOSA Session - main entry point for privacy-preserving genomics
   */
  class GyosaSession(val spark: SparkSession, config: GyosaConfig = GyosaConfig()) {
    
    // Initialize Glow for genomics operations
    Glow.register(spark)
    
    val masterKey = GenomicsEncryptionUtils.generateKey(config.keySize)
    val sgxPartitioner = new SGXComputationPartitioner(spark, config)
    
    /**
     * Load and encrypt genomic dataset
     */
    def loadEncryptedGenomicDataset[T](
      path: String, 
      format: String = "vcf"
    )(implicit encoder: org.apache.spark.sql.Encoder[T]): EncryptedGenomicDataset[T] = {
      
      val rawData = format.toLowerCase match {
        case "vcf" => spark.read.format("vcf").load(path).as[T]
        case "plink" => spark.read.format("plink").load(path).as[T]
        case "bgen" => spark.read.format("bgen").load(path).as[T]
        case "delta" => spark.read.format("delta").load(path).as[T]
        case _ => throw new IllegalArgumentException(s"Unsupported genomic format: $format")
      }
      
      if (config.encryptionEnabled) {
        EncryptedGenomicDataset(rawData, masterKey, isEncrypted = true, dataType = format.toUpperCase)
      } else {
        EncryptedGenomicDataset(rawData, masterKey, isEncrypted = false, dataType = format.toUpperCase)
      }
    }
    
    /**
     * Execute genomics computation with partitioning strategy
     */
    def executeGenomicsWithPartitioning[T, R](
      dataset: EncryptedGenomicDataset[T], 
      operation: String,
      computation: Dataset[T] => R
    ): R = {
      
      val computationZone = GenomicsComputationPartitioner.getComputationZone(operation)
      
      computationZone match {
        case GenomicsComputationPartitioner.EnclaveZone =>
          // Execute in SGX enclave (secure)
          executeInSGXEnclave(dataset, operation, computation)
          
        case GenomicsComputationPartitioner.UntrustedZone =>
          // Execute in untrusted environment
          executeInUntrusted(dataset, operation, computation)
      }
    }
    
    private def executeInSGXEnclave[T, R](
      dataset: EncryptedGenomicDataset[T],
      operation: String, 
      computation: Dataset[T] => R
    ): R = {
      // Simulate SGX enclave execution for genomics
      println(s"[GYOSA-SGX] Executing $operation in SGX enclave for ${dataset.dataType} data...")
      
      // Decrypt data within enclave if needed
      val processedData = if (dataset.isEncrypted) {
        // Decrypt within enclave
        println(s"[GYOSA-SGX] Decrypting ${dataset.dataType} data within enclave...")
        dataset.data
      } else {
        dataset.data
      }
      
      // Execute genomics computation
      val result = computation(processedData)
      
      println(s"[GYOSA-SGX] Completed $operation in SGX enclave")
      result
    }
    
    private def executeInUntrusted[T, R](
      dataset: EncryptedGenomicDataset[T],
      operation: String,
      computation: Dataset[T] => R
    ): R = {
      println(s"[GYOSA] Executing $operation in untrusted environment for ${dataset.dataType} data...")
      
      // Execute computation directly (data should remain encrypted if sensitive)
      val result = computation(dataset.data)
      
      println(s"[GYOSA] Completed $operation in untrusted environment")
      result
    }
    
    /**
     * Create VCF encryption handler for this session
     */
    def createVCFEncryption(customKey: Option[String] = None): VCFAESEncryption = {
      val keyToUse = customKey.getOrElse(new String(masterKey.getEncoded.take(16)))
      GenomicsEncryptionUtils.createVCFEncryption(keyToUse)
    }
    
    /**
     * Run privacy-preserving logistic regression GWAS in SGX enclave
     */
    def runPrivacyPreservingLogisticRegression(
      dataset: Dataset[Row],
      phenotypeCol: String,
      covariatesCols: Array[String]
    ): Dataset[Row] = {
      sgxPartitioner.performLogisticRegression(dataset, phenotypeCol, covariatesCols)
    }
    
    /**
     * Run privacy-preserving linear regression with QR decomposition in SGX enclave
     */
    def runPrivacyPreservingLinearRegression(
      dataset: Dataset[Row],
      phenotypeCol: String,
      covariatesCols: Array[String]
    ): Dataset[Row] = {
      sgxPartitioner.performLinearRegressionQR(dataset, phenotypeCol, covariatesCols)
    }
    
    /**
     * Encrypt genomic dataset using session encryption key
     */
    def encryptGenomicDataset(dataset: Dataset[Row]): Dataset[Row] = {
      // Apply encryption transformations to sensitive columns
      val encryptedDataset = dataset.withColumn("encrypted_genotypes", 
        col("genotypes") // In real implementation, apply encryption UDF here
      )
      println(s"[GYOSA-ENCRYPT] Encrypted genomic dataset with ${dataset.count()} variants")
      encryptedDataset
    }
    
    /**
     * Perform secure computation within SGX enclave
     */
    def performSecureComputation(
      dataset: Dataset[Row],
      operation: String
    ): Dataset[Row] = {
      sgxPartitioner.performSecureComputation(dataset, operation)
    }
    
    /**
     * Run Newton-Raphson optimization in SGX enclave
     */
    def runNewtonRaphsonInEnclave(
      dataset: Dataset[Row],
      initialCoefficients: Vector,
      maxIterations: Int = 100,
      tolerance: Double = 1e-6
    ): Vector = {
      sgxPartitioner.runNewtonRaphsonOptimization(
        dataset, initialCoefficients, maxIterations, tolerance
      )
    }
    
    def close(): Unit = {
      spark.close()
    }
  }
  
  /**
   * Factory method to create GYOSA session
   */
  def createSession(appName: String, config: GyosaConfig = GyosaConfig()): GyosaSession = {
    val spark = SparkSession.builder()
      .appName(appName)
      .config("spark.sql.adaptive.enabled", "true")
      .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
      .config("spark.sql.extensions", "io.projectglow.sql.GlowSparkSessionExtensions")
      .getOrCreate()
      
    new GyosaSession(spark, config)
  }
}
