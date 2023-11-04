import random

#-db DATABSE -u USERNAME -p PASSWORD -size 20
parser.add_argument("-p", "--path", help="Path to the VCF file")
parser.add_argument("-snp", "--numsnps", help="Number of Snps", type=int)

args = parser.parse_args()

# Define the number of SNPs
num_snps = args.snp

path= args.path

filepath=os.path.join(path, f"synthetic.vcf")

# Open a VCF file for writing
with open(filepath, "w") as outfile:
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

