import random

# Define Path as argument
Path = sys.argv[1]


# Define the number of SNPs
num_snps = 5.51e6

# Open a VCF file for writing
with open("synthetic.vcf", "w") as outfile:
    # Write the VCF header
    outfile.write("##fileformat=VCFv4.2\n")
    outfile.write("#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\n")

    # Loop over the number of SNPs
    for i in range(int(num_snps)):
        # Randomly choose a chromosome
        chrom = str(random.randint(1, 22))

        # Randomly choose a position
        pos = str(random.randint(1, 3e9))

        # Write the SNP information to the VCF
        outfile.write(f"{chrom}\t{pos}\t.\tA\tT\t.\tPASS\t.\n")

