from sage.all import *

def check_mersenne_multi_edges(m_max=40, n_max=40):
    """
    Uses SageMath's native factorization to find Diophantine multi-edges.
    By Mersenne constraint, q must be a prime factor of M_k for k < m_max.
    """
    print(f"Extracting valid q candidates from Mersenne numbers up to M_{m_max}...")
    
    q_candidates = set()
    
    # 1. The Mersenne Sieve: Extract only mathematically legal 'q' bases
    for k in range(1, m_max + 1):
        Mk = 2**k - 1
        if Mk > 1:
            # SageMath factor() returns a list of tuples: (prime, multiplicity)
            for prime_factor, multiplicity in factor(Mk):
                q_candidates.add(prime_factor)
                
    print(f"Mathematical lock applied. Reduced search space to {len(q_candidates)} valid primes.")
    
    # 2. Sweep only the legal candidates
    collisions = []
    
    for q in sorted(q_candidates):
        seen_p = {}
        
        for m in range(1, m_max + 1):
            for n in range(1, n_max + 1):
                p_val = 2**m + q**n
                
                if p_val in seen_p:
                    m_prime, n_prime = seen_p[p_val]
                    collisions.append({
                        'p': p_val,
                        'q': q,
                        'm1': m,
                        'n1': n,
                        'm2': m_prime,
                        'n2': n_prime
                    })
                else:
                    seen_p[p_val] = (m, n)
                    
    return collisions

# Execute the check
results = check_mersenne_multi_edges(m_max=40, n_max=40)

print("\n--- Multi-Edge Results ---")
if not results:
    print("Zero multi-edges found.")
else:
    for res in results:
        print(f"Collision at p = {res['p']} with q = {res['q']}")
        print(f"  Path 1: m = {res['m1']}, n = {res['n1']}")
        print(f"  Path 2: m = {res['m2']}, n = {res['n2']}")
        print("-" * 30)
