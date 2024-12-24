import random
import string

# Function to generate a random k-mer string
def generate_kmer(k=3):
    return ''.join(random.choice(string.ascii_lowercase) for _ in range(k))

# Function to generate a sentence with k-mers
def generate_sentence(num_kmers, k=3):
    return ''.join(generate_kmer(k) for _ in range(num_kmers))

# Generate a file with multiple lines of k-mer sequences
def create_kmer_file(filename, num_sentences=100, num_kmers_per_sentence=10, k=3):
    with open(filename, "w") as file:
        for _ in range(num_sentences):
            sentence = generate_sentence(num_kmers_per_sentence, k)
            file.write(sentence + "\n")

# Main function
if __name__ == "__main__":
    create_kmer_file("sentences.txt")
    print("File 'sentences.txt' has been generated with random k-mer sequences.")
