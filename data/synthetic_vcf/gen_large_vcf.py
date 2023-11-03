import random

# Define the number of SNPs (as argument and flag)
num_snps = sys.argv[1]

#num_snps = 1.0e6

# Define the number of VCF files (as argument)

num_files = sys.argv[2]

# Loop over the number of VCF files
for file_num in range(num_files):
    # Open a VCF file for writing
    with open(f"synthetic_{file_num}.vcf", "w") as outfile:
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