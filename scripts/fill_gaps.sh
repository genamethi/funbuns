#!/usr/bin/env bash
# Fill 11 gaps (2,320,000 missing primes) from lost imap_unordered batches.
# Each range verified via nth_prime/prime_pi round-trip (2026-04-08, corrected 2026-04-09).
set -euo pipefail

echo "Filling 11 gaps: 2,320,000 missing primes"
echo ""

# Gap 1: indices [339,140,002 .. 339,340,001] — 200,000 primes
echo "Gap 1/10: 200,000 primes starting at p=7,348,029,781"
pixi run funbuns -i 7348029781 -n 200000

# Gap 2: indices [394,340,002 .. 394,540,001] — 200,000 primes
echo "Gap 2/10: 200,000 primes starting at p=8,606,495,411"
pixi run funbuns -i 8606495411 -n 200000

# Gap 3: indices [403,540,002 .. 403,740,001] — 200,000 primes
echo "Gap 3/10: 200,000 primes starting at p=8,817,098,887"
pixi run funbuns -i 8817098887 -n 200000

# Gap 4: indices [450,040,002 .. 450,240,001] — 200,000 primes
echo "Gap 4/10: 200,000 primes starting at p=9,884,608,897"
pixi run funbuns -i 9884608897 -n 200000

# Gap 5: indices [615,140,002 .. 615,340,001] — 200,000 primes
echo "Gap 5/10: 200,000 primes starting at p=13,712,757,959"
pixi run funbuns -i 13712757959 -n 200000

# Gap 6: indices [624,340,002 .. 624,540,001] — 200,000 primes
echo "Gap 6/10: 200,000 primes starting at p=13,927,577,789"
pixi run funbuns -i 13927577789 -n 200000

# Gap 7: indices [799,140,002 .. 800,140,001] — 1,000,000 primes
echo "Gap 7/10: 1,000,000 primes starting at p=18,033,938,093"
pixi run funbuns -i 18033938093 -n 1000000

# Gap 8: indices [899,940,002 .. 900,640,001] — 700,000 primes
echo "Gap 8/10: 700,000 primes starting at p=20,420,788,667"
pixi run funbuns -i 20420788667 -n 700000

# Gap 9: indices [927,940,002 .. 928,140,001] — 200,000 primes
echo "Gap 9/10: 200,000 primes starting at p=21,085,916,317"
pixi run funbuns -i 21085916317 -n 200000

# Gap 10a: indices [1,297,800,002 .. 1,297,910,001] — 110,000 primes
echo "Gap 10a/11: 110,000 primes starting at p=29,946,764,539"
pixi run funbuns -i 29946764539 -n 110000

# Gap 10b: indices [1,298,020,002 .. 1,298,130,001] — 110,000 primes
echo "Gap 10b/11: 110,000 primes starting at p=29,952,065,849"
pixi run funbuns -i 29952065849 -n 110000

echo ""
echo "All gaps regenerated. Run files in data/runs/."
echo "Do NOT run bmgr-integrate — merge manually into existing blocks."
