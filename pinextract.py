import re
import pandas as pd

PIN_RE = re.compile(r'(?<!\d)([1-9]\d{5})(?!\d)')

def extract_pin(addr: str) -> str:
    if not isinstance(addr, str) or not addr.strip():
        return ""
    parts = [p.strip() for p in addr.split(",") if p.strip()]
    # 1) Try penultimate segment
    if len(parts) >= 2:
        m = PIN_RE.search(parts[-2])
        if m:
            return m.group(1)
    # 2) Fallback: anywhere in the string
    m = PIN_RE.search(addr)
    return m.group(1) if m else ""

# Simple checks
assert extract_pin("..., Karnataka 560017, India") == "560017"
assert extract_pin("Kodihalli, Bengaluru, Karnataka 560017, India") == "560017"

df = pd.read_csv("test.csv")
df["pin"] = df["Tagged Address"].apply(extract_pin)
df.to_csv("test_with_pin.csv", index=False)
