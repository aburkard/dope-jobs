"""Check logo dimensions and only keep square ones."""
import io
import requests
from PIL import Image
from dotenv import load_dotenv
load_dotenv()
from db import get_connection

conn = get_connection()
with conn.cursor() as cur:
    cur.execute("SELECT ats, board_token, company_name, logo_url FROM pipeline_companies WHERE logo_url IS NOT NULL")
    rows = cur.fetchall()

print(f"Checking {len(rows)} logos...\n")

bad = []
for ats, token, name, url in rows:
    try:
        r = requests.get(url, timeout=10)
        if not r.ok:
            print(f"  {name:25s} FETCH FAILED ({r.status_code})")
            bad.append((ats, token, name, "fetch_failed"))
            continue
        img = Image.open(io.BytesIO(r.content))
        w, h = img.size
        ratio = max(w, h) / min(w, h) if min(w, h) > 0 else 999
        is_square = ratio < 1.3  # allow slight variation
        status = "OK" if is_square else "NOT SQUARE"
        print(f"  {name:25s} {w:4d}x{h:<4d} ratio={ratio:.2f} {status}")
        if not is_square:
            bad.append((ats, token, name, f"{w}x{h}"))
    except Exception as e:
        print(f"  {name:25s} ERROR: {str(e)[:50]}")
        bad.append((ats, token, name, "error"))

if bad:
    print(f"\n=== Removing {len(bad)} non-square logos ===")
    for ats, token, name, reason in bad:
        print(f"  {name} ({reason})")
        with conn.cursor() as cur:
            cur.execute("UPDATE pipeline_companies SET logo_url = NULL WHERE ats = %s AND board_token = %s", (ats, token))
    conn.commit()

# Show final count
with conn.cursor() as cur:
    cur.execute("SELECT COUNT(*) FROM pipeline_companies WHERE logo_url IS NOT NULL")
    count = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM pipeline_companies")
    total = cur.fetchone()[0]
print(f"\n{count}/{total} companies have square logos")
conn.close()
