from pyhive import hive
import time

conn = hive.Connection(host='localhost', port=31140, username='hive', database='funbuns', auth='NONE')
cur = conn.cursor()

#for pred, label in [
#    ("p < 1000000", "p<1M"),
#    ("p BETWEEN 3 AND 250000000000", "p in [1M, 250B]"),
#]:
#    print(f"-- {label}: COUNT --", flush=True); t0=time.time()
#    cur.execute(f"SELECT COUNT(*) FROM funbuns.primes WHERE {pred}")
#    print(f"  {cur.fetchone()[0]} in {time.time()-t0:.2f}s")

print("-- decompositions join: k histogram --", flush=True); t0=time.time()
cur.execute("SELECT k, COUNT(*) FROM funbuns.primes WHERE p > 2 GROUP BY k ORDER BY k")
for row in cur.fetchall(): print(f"  k={row[0]}: {row[1]}")
print(f"  {time.time()-t0:.2f}s")

