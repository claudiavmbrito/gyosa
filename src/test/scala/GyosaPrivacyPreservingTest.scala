package io.projectglow.gyosa.test

import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll
import io.projectglow.Glow
import io.projectglow.gyosa.core.GyosaCore
import io.projectglow.gyosa.sgx.SGXComputationPartitioner
import io.projectglow.transformers.pipe.PipeTransformer
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

class GyosaPrivacyPreservingTest extends AnyFunSuite with BeforeAndAfterAll {
  
  var spark: SparkSession = _
  var gyosaCore: GyosaCore = _
  
  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .appName("Gyosa Privacy-Preserving GWAS Test")
      .master("local[*]")
      .config("spark.sql.adaptive.enabled", "false")
      .getOrCreate()
    
    Glow.register(spark)
    gyosaCore = new GyosaCore(spark)
    
    spark.sparkContext.setLogLevel("WARN")
  }
  
  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }
  }
  
  test("Privacy-preserving logistic regression GWAS with SGX partitioning") {
    import spark.implicits._
    
    // Generate synthetic genomic data for testing
    val syntheticGenotypes = generateSyntheticGenotypes(1000, 50000)
    val syntheticPhenotypes = generateSyntheticPhenotypes(1000)
    
    // Initialize SGX computation partitioner
    val sgxPartitioner = new SGXComputationPartitioner(spark)
    
    // Test privacy-preserving logistic regression
    val logisticResults = gyosaCore.runPrivacyPreservingLogisticRegression(
      genotypes = syntheticGenotypes,
      phenotypes = syntheticPhenotypes,
      sgxPartitioner = sgxPartitioner
    )
    
    // Verify results structure
    assert(logisticResults.columns.contains("contigName"))
    assert(logisticResults.columns.contains("start"))
    assert(logisticResults.columns.contains("beta"))
    assert(logisticResults.columns.contains("standardError"))
    assert(logisticResults.columns.contains("pValue"))
    assert(logisticResults.columns.contains("enclave_verified"))
    
    // Verify enclave computation verification
    val enclaveVerified = logisticResults.select("enclave_verified").collect()
    assert(enclaveVerified.forall(_.getBoolean(0) == true), 
           "All computations should be verified as executed in SGX enclave")
    
    println(s"Logistic regression completed with ${logisticResults.count()} variants processed")
  }
  
  test("Privacy-preserving linear regression GWAS with QR decomposition in enclave") {
    import spark.implicits._
    
    // Generate synthetic data
    val syntheticGenotypes = generateSyntheticGenotypes(800, 30000)
    val quantitativePhenotypes = generateQuantitativePhenotypes(800)
    
    val sgxPartitioner = new SGXComputationPartitioner(spark)
    
    // Test privacy-preserving linear regression with QR decomposition
    val linearResults = gyosaCore.runPrivacyPreservingLinearRegression(
      genotypes = syntheticGenotypes,
      phenotypes = quantitativePhenotypes,
      sgxPartitioner = sgxPartitioner,
      useQRDecomposition = true
    )
    
    // Verify results
    assert(linearResults.columns.contains("contigName"))
    assert(linearResults.columns.contains("start"))
    assert(linearResults.columns.contains("beta"))
    assert(linearResults.columns.contains("standardError"))
    assert(linearResults.columns.contains("pValue"))
    assert(linearResults.columns.contains("r_squared"))
    assert(linearResults.columns.contains("qr_decomp_verified"))
    
    // Verify QR decomposition was performed in enclave
    val qrVerified = linearResults.select("qr_decomp_verified").collect()
    assert(qrVerified.forall(_.getBoolean(0) == true),
           "QR decomposition should be verified as executed in SGX enclave")
    
    println(s"Linear regression completed with ${linearResults.count()} variants processed")
  }
  
  test("VCF encryption and secure computation workflow") {
    import spark.implicits._
    
    // Create synthetic VCF-like data
    val vcfData = generateSyntheticVCFData(500, 10000)
    
    // Test encryption workflow
    val encryptedData = gyosaCore.encryptGenomicDataset(vcfData, "test_encryption_key_123")
    
    // Verify encryption
    assert(encryptedData.columns.contains("encrypted_genotypes"))
    assert(encryptedData.columns.contains("encryption_metadata"))
    
    // Test secure computation on encrypted data
    val sgxPartitioner = new SGXComputationPartitioner(spark)
    val computationResults = gyosaCore.performSecureComputation(
      encryptedData = encryptedData,
      sgxPartitioner = sgxPartitioner,
      analysisType = "association_test"
    )
    
    // Verify secure computation results
    assert(computationResults.columns.contains("variant_id"))
    assert(computationResults.columns.contains("association_pvalue"))
    assert(computationResults.columns.contains("computed_in_enclave"))
    
    val enclaveComputed = computationResults.select("computed_in_enclave").collect()
    assert(enclaveComputed.forall(_.getBoolean(0) == true),
           "All computations should be performed in SGX enclave")
    
    println(s"Secure computation completed on ${computationResults.count()} encrypted variants")
  }
  
  test("Newton-Raphson optimization in SGX enclave for logistic regression") {
    import spark.implicits._
    
    val syntheticData = generateLogisticRegressionData(1200, 100)
    val sgxPartitioner = new SGXComputationPartitioner(spark)
    
    // Test Newton-Raphson iterations within enclave
    val optimizationResults = gyosaCore.runNewtonRaphsonInEnclave(
      data = syntheticData,
      sgxPartitioner = sgxPartitioner,
      maxIterations = 25,
      tolerance = 1e-6
    )
    
    // Verify optimization results
    assert(optimizationResults.columns.contains("variant_id"))
    assert(optimizationResults.columns.contains("converged"))
    assert(optimizationResults.columns.contains("iterations_used"))
    assert(optimizationResults.columns.contains("final_log_likelihood"))
    assert(optimizationResults.columns.contains("enclave_optimized"))
    
    val convergedCount = optimizationResults.filter(col("converged") === true).count()
    println(s"Newton-Raphson optimization: ${convergedCount} variants converged")
    
    val enclaveOptimized = optimizationResults.select("enclave_optimized").collect()
    assert(enclaveOptimized.forall(_.getBoolean(0) == true),
           "Newton-Raphson optimization should be performed in SGX enclave")
  }
  
  // Helper methods for generating synthetic test data
  private def generateSyntheticGenotypes(samples: Int, variants: Int) = {
    import spark.implicits._
    
    val genotypeData = (1 to variants).map { variantId =>
      val genotypes = (1 to samples).map { sampleId =>
        scala.util.Random.nextInt(3) // 0, 1, or 2 for diploid genotypes
      }.toArray
      (s"chr1", variantId * 1000, s"variant_$variantId", genotypes)
    }
    
    spark.createDataFrame(genotypeData)
      .toDF("contigName", "start", "names", "genotypes")
  }
  
  private def generateSyntheticPhenotypes(samples: Int) = {
    import spark.implicits._
    
    val phenotypeData = (1 to samples).map { sampleId =>
      val phenotype = if (scala.util.Random.nextDouble() > 0.5) 1 else 0
      (s"sample_$sampleId", phenotype)
    }
    
    spark.createDataFrame(phenotypeData).toDF("sampleId", "phenotype")
  }
  
  private def generateQuantitativePhenotypes(samples: Int) = {
    import spark.implicits._
    
    val phenotypeData = (1 to samples).map { sampleId =>
      val phenotype = scala.util.Random.nextGaussian() * 2.0 + 5.0
      (s"sample_$sampleId", phenotype)
    }
    
    spark.createDataFrame(phenotypeData).toDF("sampleId", "phenotype")
  }
  
  private def generateSyntheticVCFData(samples: Int, variants: Int) = {
    import spark.implicits._
    
    val vcfData = (1 to variants).map { variantId =>
      val ref = scala.util.Random.shuffle(List("A", "T", "G", "C")).head
      val alt = scala.util.Random.shuffle(List("A", "T", "G", "C").filter(_ != ref)).head
      val qual = 30.0 + scala.util.Random.nextDouble() * 70.0
      val genotypes = (1 to samples).map(_ => scala.util.Random.nextInt(3)).toArray
      
      (s"chr1", variantId * 1000, s"rs$variantId", ref, alt, qual, genotypes)
    }
    
    spark.createDataFrame(vcfData)
      .toDF("contigName", "start", "names", "referenceAllele", "alternateAlleles", "qual", "genotypes")
  }
  
  private def generateLogisticRegressionData(samples: Int, variants: Int) = {
    import spark.implicits._
    
    val data = (1 to variants).flatMap { variantId =>
      (1 to samples).map { sampleId =>
        val genotype = scala.util.Random.nextInt(3)
        val covariate1 = scala.util.Random.nextGaussian()
        val covariate2 = scala.util.Random.nextDouble()
        val logOdds = 0.1 * genotype + 0.05 * covariate1 + 0.2 * covariate2
        val phenotype = if (scala.util.Random.nextDouble() < 1.0 / (1.0 + math.exp(-logOdds))) 1 else 0
        
        (s"variant_$variantId", s"sample_$sampleId", genotype, covariate1, covariate2, phenotype)
      }
    }
    
    spark.createDataFrame(data)
      .toDF("variant_id", "sample_id", "genotype", "covariate1", "covariate2", "phenotype")
  }
}
