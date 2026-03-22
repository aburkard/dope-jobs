"""Fix company data — manual overrides for known issues."""
from dotenv import load_dotenv
load_dotenv()
from db import get_connection

FIXES = {
    ("greenhouse", "grammarly"): {"company_name": "Superhuman", "domain": "superhuman.com"},
    ("greenhouse", "scaleai"): {"domain": "scale.com"},
    ("greenhouse", "chime"): {"company_name": "Chime", "domain": "chime.com"},
    ("greenhouse", "coinbase"): {"company_name": "Coinbase", "domain": "coinbase.com"},
    ("greenhouse", "reddit"): {"domain": "reddit.com"},
    ("greenhouse", "duolingo"): {"domain": "duolingo.com"},
    ("greenhouse", "epicgames"): {"domain": "epicgames.com"},
    ("greenhouse", "twitch"): {"domain": "twitch.tv"},
    ("greenhouse", "andurilindustries"): {"domain": "anduril.com"},
    ("greenhouse", "gusto"): {"company_name": "Gusto", "domain": "gusto.com"},
    ("greenhouse", "watershed"): {"company_name": "Watershed", "domain": "watershed.com"},
    ("greenhouse", "brex"): {"domain": "brex.com"},
    ("greenhouse", "cloudflare"): {"domain": "cloudflare.com"},
    ("greenhouse", "lattice"): {"domain": "lattice.com"},
    ("greenhouse", "samsara"): {"domain": "samsara.com"},
    ("greenhouse", "flexport"): {"domain": "flexport.com"},
    ("greenhouse", "relativity"): {"domain": "relativityspace.com"},
    ("greenhouse", "airbnb"): {"domain": "airbnb.com"},
    ("greenhouse", "roblox"): {"domain": "roblox.com"},
    ("greenhouse", "deepmind"): {"domain": "deepmind.google"},
    ("greenhouse", "vercel"): {"domain": "vercel.com"},
    ("greenhouse", "dropbox"): {"domain": "dropbox.com"},
    ("lever", "spotify"): {"company_name": "Spotify", "domain": "spotify.com"},
}

conn = get_connection()
for (ats, token), updates in FIXES.items():
    sets = ", ".join(f"{k} = %s" for k in updates)
    vals = list(updates.values()) + [ats, token]
    with conn.cursor() as cur:
        cur.execute(f"UPDATE pipeline_companies SET {sets} WHERE ats = %s AND board_token = %s", vals)
conn.commit()
print(f"Applied {len(FIXES)} fixes")
conn.close()
