<h1 align="center">
  <!-- logo as of current commit -->
 <img src=".media/main_page/logo_blue3_gyosa.png" height="200"/>

  <img src="https://img.shields.io/badge/status-research%20prototype-green.svg" />
  <a href="https://opensource.org/licenses/BSD-3-Clause">
    <img src="https://img.shields.io/badge/license-BSD--3-blue.svg" />
  </a>
</h1>

**GYOSA** is a privacy-preserving machine learning solution developed on top of [Soteria](https://github.com/claudiavmbrito/Soteria), [GLOW](https://github.com/projectglow/glow) and [Apache Spark](https://github.com/apache/spark) by resorting to [Gramine](https://github.com/gramineproject/gramine).

Built upon the concept of computation partitioning, **GYOSA** allows running GWASes inside the enclaves while running non-sensitive code outside. 
The main goal of **GYOSA** is to provide the community with a privacy-preserving and distributed solution to run genomic analysis in untrusted third-party infrastructures. 

**Note 1**: This is an academic proof-of-concept prototype and has not received careful code review. This implementation is NOT ready for production use.

**Note 2**: This repository is being updated. 

___
## Getting Started

The code for **GYOSA** will be fully published here **soon** along with information on all its dependencies.

To offer an easy-to-use guide, we provide scripts to install all the components. 

#### Dependencies

Gyosa was built and tested with Intel's SGX SDK `2.6`, SGX Driver `1.8`, and Gramine `1.0` (previously named Graphene-SGX).
By relying on Glow, Gyosa depends on Apache Spark `3.2.1`, which upgrades from the previously built Soteria (Apache Spark `2.3.4`). 

### Set up Gyosa

```
git clone https://github.com/claudiavmbrito/Gyosa.git
cd Gyosa
```

#### Intel SGX

To install SGX SDK and its Drivers, please see `install_sgx.sh` and run:

```
bash ./install_sgx.sh
```


#### Gramine 

- To use the previous and base code of Gramine used to develop Gyosa, please refer to https://github.com/gramineproject/gramine/tree/v1.0.
- To use the updated version of Gramine, follow [Gramine](https://github.com/gramineproject/gramine) documentation. 
- The manifest files need to be carefully changed to work with the new versions of Gramine. 


### Glow

You need to compile Glow once after doing  ```git clone```.


### Data

#### Synthetic Data

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
