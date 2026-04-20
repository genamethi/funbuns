import sys
from sage.all import Integer

def print_all_mersenne_factors(limit=40):
    # Added a 'Value' column with a width of 15 characters
    print(f"{'M_n':<5} | {'Value':<15} | {'Unique Prime Factors'}")
    print("-" * 80)
    
    for n in range(1, limit + 1):
        # Progress indicator
        sys.stdout.write(f"\r[Factoring M_{n}...]".ljust(80))
        sys.stdout.flush()
        
        M = Integer(2**n - 1)
        
        # Clear the progress line
        sys.stdout.write("\r" + " " * 80 + "\r")
        sys.stdout.flush()
        
        if M == 1:
            print(f"M_{n:<3} | {str(M):<15} | None")
            continue
            
        # Factor the number using the default PARI library
        factorization = M.factor()
        
        # Extract unique primes and join them into a string
        unique_primes = [str(prime_tuple[0]) for prime_tuple in factorization]
        primes_str = ", ".join(unique_primes)
        
        # Print the row with the Mersenne number's calculated value included
        print(f"M_{n:<3} | {str(M):<15} | {primes_str}")

if __name__ == "__main__":
    print_all_mersenne_factors(40)
