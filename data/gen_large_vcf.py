import random
import os


#num_snps = 1.0e6
#-p path -snp number of snps -f number of files
parser.add_argument("-p", "--path", help="Path to the VCF file")
parser.add_argument("-snp", "--numsnps", help="Number of Snps", type=int)
parser.add_argument("-f", "--numfiles", help="Number of Files", type=int)

args = parser.parse_args()

# Define Path as argument
path = args.path

# Define the number of SNPs
num_snps = args.snp

# Define the number of VCF files (as argument)
num_files = args.numfiles

# Loop over the number of VCF files
for file_num in range(num_files):
    # Open a VCF file for writing
    file_path = os.path.join(path, f"synthetic_{file_num}.vcf")
    with open(file_path, "w") as outfile:
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