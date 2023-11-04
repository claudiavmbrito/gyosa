import org.apache.spark.sql.SparkSession
import io.projectglow.Glow
import io.projectglow.sql.GlowBaseTest
import io.projectglow.vcf.VCFAESEncryption

val spark = SparkSession.builder()
  .appName("VCF Encryption Application")
  .getOrCreate()

Glow.register(spark)

val encryptor = new VCFAESEncryption("16_character_key!")

val vcfDF = spark.read.format("vcf").load("/path/to/vcf_file")
val encryptedVCF = encryptor.encryptVCF(vcfDF)
val decryptedVCF = encryptor.decryptVCF(encryptedVCF)
