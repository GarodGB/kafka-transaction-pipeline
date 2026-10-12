from datetime import datetime, timezone
import re
import json
import os

DATE_FORMATS = [
    "%Y-%m-%d %H:%M:%S",  
    "%d/%m/%Y %H:%M",      
    "%m-%d-%Y %H:%M",      
    "%d-%b-%y %H:%M",     
    "%Y-%m-%d",            
    "%d/%m/%Y",           
    "%m-%d-%Y",            
    "%d-%b-%y",            
]

def clean_date(value):
   
    if value is None:
        return None
    text = str(value).strip()

    if text == "" or text.lower() == "nan":         
        return None

    if text.isdigit() and len(text) == 13:           
        return datetime.fromtimestamp(int(text) / 1000, tz=timezone.utc).replace(tzinfo=None)

    for fmt in DATE_FORMATS:                        
        try:
            result = datetime.strptime(text, fmt)
        except ValueError:
            continue                                 
        if result.year < 2000:                      
            return None
        return result

    return None 

def clean_amount(value):
    
    if value is None:
        return None
    text = str(value).strip()
    if text == "" or text.lower() == "nan":
        return None

    negative = text.startswith("(") and text.endswith(")")  
    text = re.sub(r"[^0-9.,-]", "", text)                    

    if "," in text and "." in text:                        
        if text.rfind(",") > text.rfind("."):                
            text = text.replace(".", "").replace(",", ".")
        else:                                              
            text = text.replace(",", "")
    elif "," in text:                                        
        text = text.replace(",", ".")

    try:
        number = float(text)
    except ValueError:
        return None

    return -abs(number) if negative else number   


KNOWN_PREFIXES = ["SQ", "IZ", "TST", "WPY", "SP", "PP", "PAYPAL", "GOOGLE"]

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
MAP_PATH = os.path.join(BASE_DIR, "config", "merchant_map.json")
MERCHANT_MAP = json.load(open(MAP_PATH)) if os.path.exists(MAP_PATH) else {}

def strip_junk(s):
  
    for _ in range(2):
        s = re.sub(r"\s*\*\w+$", "", s)
        s = re.sub(r"\s+\d{4,}$", "", s)
        s = re.sub(r"\s*#\d+$", "", s)
        s = re.sub(r"\s+BR(\s*\d+)?$", "", s)
        s = re.sub(r"\s*/\w+$", "", s)
    return s.strip()

def clean_merchant(value, use_map=True):
    if value is None or str(value).strip().lower() in ("", "nan"):
        return None, None
    name = re.sub(r"\s+", " ", str(value).upper().strip())

    payment_via = "DIRECT"
    if "*" in name:
        before, after = [p.strip() for p in name.split("*", 1)]
        if before in KNOWN_PREFIXES:
            payment_via = "PAYPAL" if before == "PP" else before
            name = after
        else:
            name = before

    name = strip_junk(name)
    if use_map:
        name = MERCHANT_MAP.get(name, name)
    return name, payment_via
    
    
    
    
ONLINE_WORDS = ["INTERNET", "ECOM", "E-COMMERCE"]

CITY_MAP = {
    
    "BEIRUT LB": "BEIRUT", "BEYROUTH": "BEIRUT", "BEYRUT": "BEIRUT",
  
    "TARABLUS": "TRIPOLI", "TRABLOUS": "TRIPOLI",
    "SAYDA": "SAIDA", "SIDON": "SAIDA",
    "JUNIYAH": "JOUNIEH", "JOUNIE": "JOUNIEH",
    "JBAIL": "JBEIL", "BYBLOS": "JBEIL",
    "ZAHLEH": "ZAHLE",
    "ASHRAFIYA": "ASHRAFIEH", "ACHRAFIEH": "ASHRAFIEH",
    "AL HAMRA": "HAMRA",
  
    "DUBAYY": "DUBAI",
    "AL QAHIRAH": "CAIRO",
    "AL KUWAYT": "KUWAIT CITY",
    "ISTANBOUL": "ISTANBUL",
}

def clean_city(value):
    
    if value is None or str(value).strip().lower() in ("", "nan"):
        return "UNKNOWN", False
    city = re.sub(r"\s+", " ", str(value).upper().strip())
    if city in ONLINE_WORDS:
        return "ONLINE", True
    city = CITY_MAP.get(city, city)
    return city, False


CC_PATH = os.path.join(BASE_DIR, "config", "city_country.json")
CITY_COUNTRY = json.load(open(CC_PATH)) if os.path.exists(CC_PATH) else {}

def clean_country(value, city):
    
    if value is None or str(value).strip().lower() in ("", "nan"):
        return "UNKNOWN", False
    country = str(value).upper().strip()
    expected = CITY_COUNTRY.get(city)
    if expected is None:
        return country, False
    return country, country != expected



def clean_fx(value):
   
    if value is None or str(value).strip().lower() in ("", "nan"):
        return None
    try:
        rate = float(str(value).strip())
    except ValueError:
        return None
    return rate if rate > 0 else None

def check_amounts(txn_amount, fx_rate, billing_amount):
    
    if txn_amount is None or fx_rate is None or billing_amount is None:
        return False
    expected = txn_amount * fx_rate
    difference = abs(expected - billing_amount)
    allowed = 0.01 * abs(billing_amount) + 0.01
    return difference > allowed


def clean_terminal(value):
    
    if value is None or str(value).strip().lower() in ("", "nan"):
        return None, False
    terminal = str(value).strip().upper()
    if terminal == "00000000":
        return None, False
    return terminal, True


FAKE_AUTH_CODES = ["000000", "ZZ0011", "9X4K7P", "A1B2C3"]

def clean_auth(value):
    
    if value is None or str(value).strip().lower() in ("", "nan"):
        return None, True
    code = str(value).strip().upper()
    if code in FAKE_AUTH_CODES:
        return None, True
    return code, False

def clean_row(row):
      
    txn_amount = clean_amount(row.get("TXN_AMOUNT"))
    billing_amount = clean_amount(row.get("BILLING_AMOUNT"))
    fx_rate = clean_fx(row.get("FX_RATE"))
    merchant, payment_via = clean_merchant(row.get("MERCHANT_NAME"))
    city, is_online = clean_city(row.get("MERCHANT_CITY"))
    country, country_mismatch = clean_country(row.get("MERCHANT_COUNTRY"), city)
    terminal_id, has_terminal = clean_terminal(row.get("TERMINAL_ID"))
    auth_code, auth_suspicious = clean_auth(row.get("AUTH_CODE"))

    return {
        
        "txn_id":           row.get("TXN_ID"),
        "account_id":       row.get("ACCOUNT_ID"),
        "txn_type":         row.get("TXN_TYPE"),
        "txn_ccy":          row.get("TXN_CCY"),
        "billing_currency": row.get("BILLING_CURRENCY"),
        "mcc_code":         row.get("MCC_CODE"),
        "processing_code":  row.get("PROCESSING_CODE"),
        "processing_type":  row.get("PROCESSING_TYPE"),
        "matches_status":   row.get("MATCHES_STATUS"),
        
        "txn_date_time":    clean_date(row.get("TXN_DATE_TIME")),
        "settle_date":      clean_date(row.get("SETTLE_DATE")),
        "txn_amount":       txn_amount,
        "billing_amount":   billing_amount,
        "fx_rate":          fx_rate,
        "merchant_name":    merchant,
        "merchant_city":    city,
        "merchant_country": country,
        "terminal_id":      terminal_id,
        "auth_code":        auth_code,
       
        "payment_via":      payment_via,
        "is_online":        is_online,
        "country_mismatch": country_mismatch,
        "amount_mismatch":  check_amounts(txn_amount, fx_rate, billing_amount),
        "has_terminal":     has_terminal,
        "auth_suspicious":  auth_suspicious,
    }


if __name__ == "__main__":
    tests = ["2022-03-10 00:51:36", "09/07/2022 17:49", "04-22-2022 22:52",
             "23-Apr-22 10:15", "1650000000000", "0000-00-00", "nan", ""]
    for t in tests:
        print(f"{t!r:25} → {clean_date(t)}")
    import pandas as pd
    raw = pd.read_excel("data/synthetic_dirty_transactions_v4.xlsx", dtype=str)

    for col in ["TXN_DATE_TIME", "SETTLE_DATE"]:
        cleaned = raw[col].apply(clean_date)
        print(f"{col}: {cleaned.isna().sum()} could not be read")
        print(raw.loc[cleaned.isna(), col].value_counts(dropna=False).head(), "\n")
        
        
    for t in ["-104.39", "$1,250.00", "172,22", "5.727.580,00", "(808.41)", "abc", "nan"]:
        print(f"{t!r:18} → {clean_amount(t)}")

    amounts = raw["TXN_AMOUNT"].apply(clean_amount)
    print("TXN_AMOUNT could not be read:", amounts.isna().sum())  
    
    
      
    print("\nMap loaded:", len(MERCHANT_MAP), "rules\n")
    sample = raw["MERCHANT_NAME"].sample(15, random_state=7)
    for v in sample:
        name, via = clean_merchant(v)
        print(f"{v!r:35} → {name:25} | {via}")
        
    
    cities = raw["MERCHANT_CITY"].apply(lambda v: clean_city(v)[0])
    print("\nCITY before:", raw["MERCHANT_CITY"].nunique(), " after:", cities.nunique())
    print(cities.value_counts().head(10))
    
    
    
    
    
    for country, city in [("LB","BEIRUT"), ("US","BEIRUT"), ("US","ONLINE"), ("gb","LONDON"), (None,"PARIS")]:
        print(f"{country!r:6} + {city:8} → {clean_country(country, city)}")
    res = raw.apply(lambda r: clean_country(r["MERCHANT_COUNTRY"], clean_city(r["MERCHANT_CITY"])[0]), axis=1)
    print("COUNTRY mismatches:", res.str[1].sum(), "of", len(raw))
    
    
    
    
    for v in ["1", "1.0793709386", "3.51216e-05", "abc", "0", "nan"]:
        print(f"{v!r:16} → {clean_fx(v)}")
    for t, f, b in [(-81.51, 1.069, -87.13), (-154.77, 1, -1.55), (14.99, 1, -14.99), (-154.26, 1, -174.13)]:
        print(t, f, b, "→", check_amounts(t, f, b))

    bill = raw["BILLING_AMOUNT"].apply(clean_amount)
    fx = raw["FX_RATE"].apply(clean_fx)
    print("BILLING unreadable:", bill.isna().sum(), " FX unreadable:", fx.isna().sum())
    mm = [check_amounts(t, f, b) for t, f, b in zip(amounts, fx, bill)]
    print("AMOUNT mismatches:", sum(mm), "of", len(raw))
    
    
    
    for v in ["PDYR05FW", "00000000", "nan"]:
        print(f"{v!r:12} → {clean_terminal(v)}")
    t = raw["TERMINAL_ID"].apply(clean_terminal)
    print("No terminal:", (~t.str[1]).sum())
    
    
    for v in ["RZF979", "000000", "A1B2C3", "nan"]:
        print(f"{v!r:12} → {clean_auth(v)}")
    a = raw["AUTH_CODE"].apply(clean_auth)
    print("Suspicious auth:", a.str[1].sum())
    
    
    
    
    print("\n===== CLEAN_ROW =====")
    one = raw.iloc[0].to_dict()
    for k, v in clean_row(one).items():
        print(f"  {k:18} {v}")

    clean = pd.DataFrame([clean_row(r) for r in raw.to_dict("records")])
    print("\nRows:", len(clean), " Columns:", len(clean.columns))
    print(clean[["is_online", "country_mismatch", "amount_mismatch", "has_terminal", "auth_suspicious"]].sum())