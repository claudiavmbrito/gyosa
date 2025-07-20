# GYOSA: Privacy-Preserving Genomic Analysis Framework

<h1 align="center">
<img src="https://img.shields.io/badge/status-research%20prototype-green.svg" />
<a href="https://opensource.org/licenses/BSD-3-Clause">
<img src="https://img.shields.io/badge/license-BSD--3-blue.svg" />
</a>
</h1>

**GYOSA** is a privacy-preserving genomic analysis framework that enables secure genome-wide association studies (GWAS) in untrusted cloud environments. Built on top of [Soteria](https://github.com/claudiavmbrito/Soteria), [GLOW](https://github.com/projectglow/glow), and [Apache Spark](https://github.com/apache/spark), GYOSA leverages Intel SGX through [Gramine](https://github.com/gramineproject/gramine) to provide hardware-based privacy guarantees.

## Key Features

- **Hardware-based Privacy**: Uses Intel SGX enclaves to protect sensitive genomic computations
- **Computation Partitioning**: Separates sensitive operations (inside enclaves) from non-sensitive operations (outside enclaves)
- **Distributed Processing**: Built on Apache Spark for scalable genomic analysis
- **Encryption Support**: Provides AES encryption for VCF files and genomic data


## Citation

If you use GYOSA in your research, please cite our paper:

```bibtex
@article{brito2025exploiting,
  title={Exploiting Trusted Execution Environments and Distributed Computation for Genomic Association Tests},
  author={Brito, Cl{\'a}udia V and Ferreira, Pedro G and Paulo, Jo{\~a}o T},
  journal={IEEE Journal of Biomedical and Health Informatics},
  year={2025},
  publisher={IEEE}
}
```

**Note 1**: This is an academic proof-of-concept prototype and has not received careful code review. This implementation is NOT ready for production use.

**Note 2**: This repository contains research prototypes for privacy-preserving genomics analysis.

___
## Getting Started

To offer an easy-to-use guide, we provide scripts to install all the components. 

#### Dependencies

Gyosa was built and tested with Intel's SGX SDK `2.6`, SGX Driver `1.8`, and Gramine `1.0` (previously named Graphene-SGX).
By relying on Glow, Gyosa depends on Apache Spark `3.2.1`, which upgrades from the previously built Soteria (Apache Spark `2.3.4`). 
The current implementation works for Ubuntu 18.04. 

### Set up Gyosa

```
git clone https://github.com/claudiavmbrito/Gyosa.git
cd Gyosa
```

### Glow

After doing ```git clone``` access the [glow](https://github.com/claudiavmbrito/Gyosa/gyosa/glow) folder and compile the framework with the changes.

You can follow the steps on Glow to compile this version of the framework.

Be aware that you should do:

```
sbt core/compile
sbt core/assembly
```

After compiling, you will find in ```target/scalaXXX``` the respective jar.


#### Dependencies

Glow and Spark are based on Scala, it needs ```scala-2.12.8```.
Launch the ```sbt``` command line and add the following dependencies for the encryption module.

```
> libraryDependencies += "org.apache.spark" %% "spark-core" % "3.2.1"

> libraryDependencies += "javax.crypto" % "jce" % "1.8_211"
```


#### Intel SGX

To install SGX SDK and its Drivers, please see `install_sgx.sh` and run:

```
bash ./install_sgx.sh
```

Follow the tutorials of SGX to see if it installed correctly.

#### Gramine 

- To use the previous and base code of Gramine used to develop Gyosa, please refer to https://github.com/gramineproject/gramine/tree/v1.0.
- To use the updated version of Gramine, follow [Gramine](https://github.com/gramineproject/gramine) documentation and install the tool. 
- The manifest files may need to be carefully changed to work with the new versions of Gramine.

1. In all the manifest files, replace the placeholders **(/path/to/...)** with actual paths.
2. Make sure you've included all necessary dependencies, environment variables, and trusted files for running your computation.

There are two manifest files, one with all the libraries (glow), and the second one that follows the Soteria approach with the partitioned scheme (gyosa).

You should create the manifest file now by running:

```
gramine-sgx-sign \
    -output glow.manifest.sgx \
    -key /path/to/your/enclave-key.pem \
    -libpal /path/to/gramine/libpal.so \
    -manifest glow.manifest.template
```

You can now run:

```
gramine-sgx /path/to/spark-submit [spark-submit arguments]
```


#### Encryption tests

In the [encryption_tests](https://github.com/claudiavmbrito/Gyosa/glow/encryption_tests) folder, you may find an example of running the VCFAESencryption module to encrypt your VCFs. Also replace the placeholders **(/path/to/...)** with actual paths.

Then run:
````
spark-submit --class app_glow --jars **/path/to/glow.jar** encryption_tests.jar
``````

#### Data Folder

##### Synthetic Data

The [data](https://github.com/claudiavmbrito/Gyosa/data) folder has the scripts to generate the synthetic data used for the $X^2$ tests. 

You can run the following commands to create 80000 files with 1000000 SNPs

```
python3 gen_large_vcf.py -p /path/to/data/folder -snp 1000000 -numfiles 80000 
```

This will generate 80k files with $1*10^6$ SNPs each in the specific folder. 

For the Genome in a Bottle data, please see the [link](https://www.nist.gov/programs-projects/genome-bottle) to access the consortium and the full data. 
___
## Contact

Please reach out at `claudia.v.brito@inesctec.pt` with any questions.
