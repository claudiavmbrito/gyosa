# Gyosa

**GYOSA** is a privacy-preserving machine learning solution developed on top of [Soteria](https://github.com/claudiavmbrito/Soteria), [GLOW](https://github.com/projectglow/glow) and [Apache Spark](https://github.com/apache/spark) by resorting to [Gramine](https://github.com/gramineproject/gramine).

Built upon the concept of computation partitioning, **GYOSA** allows running GWASes inside the enclaves while running non-sensitive code outside. 
The main goal of **GYOSA** is to provide the community with a privacy-preserving and distributed solution to run genomic analysis in untrusted third-party infrastructures. 

**Note 1**: This is an academic proof-of-concept prototype and has not received careful code review. This implementation is NOT ready for production use.

**Note 2**: This repository will be updated shortly. 

___
## Getting Started

The code for **GYOSA** will be fully published here **soon** along with information on all its dependencies.

To offer an easy-to-use guide, we provide scripts to install all the components. 

### Dependencies

Gyosa was built and tested with Intel's SGX SDK `2.6`, SGX Driver `1.8`, and Gramine `1.0` (previously named Graphene-SGX).
By relying on Glow, Gyosa depends on Apache Spark `3.2.1`, which upgrades from the previously built Soteria (Apache Spark `2.3.4`). 

### Intel SGX

To install SGX SDK and its Driver, please see `install_sgx.sh` and run:

```
bash ./install_sgx.sh
```

### Gramine 

- To use the previous and base code of Gramine used to develop Gyosa, please refer to https://github.com/gramineproject/gramine/tree/v1.0.
- To use the updated version of Gramine, follow [Gramine](https://github.com/gramineproject/gramine) documentation. 
- The manifest files need to be carefully changed to work with the new versions of Gramine. 

___
## Contact

Please contact us at `claudia.v.brito@inesctec.pt` with any questions.


