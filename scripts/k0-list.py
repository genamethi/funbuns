from pyhive import hive
import time

conn = hive.Connection(host='localhost', port=31140, username='hive', database='funbuns', auth='NONE')
cur = conn.cursor()


print("Listing first 50 k = 0 primes", flush=True);
cur.execute("SELECT p FROM funbuns.primes WHERE k = 0 AND p > 500000 AND p < 1000000 ORDER BY p LIMIT 100000")
for row in cur.fetchall(): print(f"  {row[0]} ")

