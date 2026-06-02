"""Static subsidiary / DBA-to-parent-ticker mapping.

Provides a fast, offline lookup for common subsidiary names, trade names,
and "doing business as" variants that appear on WARN Act filings but do not
match their publicly traded parent company name.

Usage::

    from services.entity_resolution.subsidiary_map import lookup_subsidiary

    ticker = lookup_subsidiary("Amazon.com Services LLC - Springfield IL")
    # -> "AMZN"
"""

import re
from typing import Optional

# ------------------------------------------------------------------
# Mapping table
#
# Keys are UPPERCASE normalized fragments.  The lookup function strips
# common suffixes, location tails, and store numbers before checking
# each key as a prefix / substring of the input.
# ------------------------------------------------------------------

_SUBSIDIARY_TO_TICKER: dict[str, str] = {
    # ---- Technology ----
    "AMAZON": "AMZN",
    "AMAZON.COM": "AMZN",
    "AMAZON WEB SERVICES": "AMZN",
    "AWS": "AMZN",
    "WHOLE FOODS": "AMZN",
    "TWITCH": "AMZN",
    "RING LLC": "AMZN",
    "ZAPPOS": "AMZN",
    "GOOGLE": "GOOGL",
    "ALPHABET": "GOOGL",
    "YOUTUBE": "GOOGL",
    "WAYMO": "GOOGL",
    "DEEPMIND": "GOOGL",
    "VERILY": "GOOGL",
    "META PLATFORMS": "META",
    "FACEBOOK": "META",
    "INSTAGRAM": "META",
    "WHATSAPP": "META",
    "OCULUS": "META",
    "APPLE": "AAPL",
    "MICROSOFT": "MSFT",
    "LINKEDIN": "MSFT",
    "GITHUB": "MSFT",
    "NUANCE": "MSFT",
    "ACTIVISION BLIZZARD": "MSFT",
    "ACTIVISION": "MSFT",
    "BLIZZARD": "MSFT",
    "XBOX": "MSFT",
    "ORACLE": "ORCL",
    "SALESFORCE": "CRM",
    "SLACK": "CRM",
    "TABLEAU": "CRM",
    "MULESOFT": "CRM",
    "INTEL": "INTC",
    "CISCO": "CSCO",
    "CISCO SYSTEMS": "CSCO",
    "IBM": "IBM",
    "RED HAT": "IBM",
    "ADOBE": "ADBE",
    "FIGMA": "ADBE",
    "NVIDIA": "NVDA",
    "MELLANOX": "NVDA",
    "AMD": "AMD",
    "ADVANCED MICRO DEVICES": "AMD",
    "XILINX": "AMD",
    "QUALCOMM": "QCOM",
    "BROADCOM": "AVGO",
    "VMWARE": "AVGO",
    "TEXAS INSTRUMENTS": "TXN",
    "PAYPAL": "PYPL",
    "VENMO": "PYPL",
    "UBER": "UBER",
    "UBER EATS": "UBER",
    "POSTMATES": "UBER",
    "LYFT": "LYFT",
    "SNAP": "SNAP",
    "SNAPCHAT": "SNAP",
    "TWITTER": "X",
    "X CORP": "X",
    "DELL": "DELL",
    "DELL TECHNOLOGIES": "DELL",
    "HEWLETT PACKARD ENTERPRISE": "HPE",
    "HPE": "HPE",
    "HP INC": "HPQ",
    "HEWLETT-PACKARD": "HPQ",

    # ---- Retail ----
    "WALMART": "WMT",
    "WAL-MART": "WMT",
    "SAMS CLUB": "WMT",
    "SAM'S CLUB": "WMT",
    "TARGET": "TGT",
    "HOME DEPOT": "HD",
    "LOWES": "LOW",
    "LOWE'S": "LOW",
    "COSTCO": "COST",
    "COSTCO WHOLESALE": "COST",
    "BEST BUY": "BBY",
    "DOLLAR GENERAL": "DG",
    "DOLLAR TREE": "DLTR",
    "FAMILY DOLLAR": "DLTR",
    "ROSS STORES": "ROST",
    "TJ MAXX": "TJX",
    "TJX": "TJX",
    "MARSHALLS": "TJX",
    "HOMEGOODS": "TJX",
    "NORDSTROM": "JWN",
    "MACYS": "M",
    "MACY'S": "M",
    "KOHLS": "KSS",
    "KOHL'S": "KSS",
    "GAP": "GAP",
    "OLD NAVY": "GAP",
    "BANANA REPUBLIC": "GAP",
    "ATHLETA": "GAP",
    "BED BATH & BEYOND": "BBBY",
    "KROGER": "KR",
    "ALBERTSONS": "ACI",
    "SAFEWAY": "ACI",
    "PUBLIX": "PUSH",
    "TRADER JOES": "ALDI",

    # ---- Healthcare / Pharmacy ----
    "WALGREENS": "WBA",
    "WALGREENS BOOTS ALLIANCE": "WBA",
    "WALGREEN": "WBA",
    "DUANE READE": "WBA",
    "CVS HEALTH": "CVS",
    "CVS PHARMACY": "CVS",
    "CVS CAREMARK": "CVS",
    "AETNA": "CVS",
    "UNITEDHEALTH": "UNH",
    "UNITED HEALTH": "UNH",
    "UNITEDHEALTHCARE": "UNH",
    "OPTUM": "UNH",
    "CIGNA": "CI",
    "EVERNORTH": "CI",
    "EXPRESS SCRIPTS": "CI",
    "HUMANA": "HUM",
    "ANTHEM": "ELV",
    "ELEVANCE HEALTH": "ELV",
    "JOHNSON & JOHNSON": "JNJ",
    "JANSSEN": "JNJ",
    "PFIZER": "PFE",
    "ABBVIE": "ABBV",
    "ALLERGAN": "ABBV",
    "MERCK": "MRK",
    "ELI LILLY": "LLY",
    "LILLY": "LLY",
    "ABBOTT": "ABT",
    "ABBOTT LABORATORIES": "ABT",
    "MEDTRONIC": "MDT",
    "BAXTER": "BAX",
    "STRYKER": "SYK",
    "BOSTON SCIENTIFIC": "BSX",
    "BECTON DICKINSON": "BDX",
    "HCA HEALTHCARE": "HCA",

    # ---- Logistics / Shipping ----
    "FEDEX": "FDX",
    "FEDEX GROUND": "FDX",
    "FEDEX FREIGHT": "FDX",
    "FEDEX EXPRESS": "FDX",
    "FEDEX OFFICE": "FDX",
    "UPS": "UPS",
    "UNITED PARCEL SERVICE": "UPS",
    "UPS SUPPLY CHAIN": "UPS",
    "UPS FREIGHT": "UPS",
    "DHL": "DPSGY",
    "XPO LOGISTICS": "XPO",
    "XPO": "XPO",

    # ---- Aerospace / Defense ----
    "BOEING": "BA",
    "SPIRIT AEROSYSTEMS": "SPR",
    "LOCKHEED MARTIN": "LMT",
    "LOCKHEED": "LMT",
    "RAYTHEON": "RTX",
    "RTX": "RTX",
    "NORTHROP GRUMMAN": "NOC",
    "GENERAL DYNAMICS": "GD",
    "L3HARRIS": "LHX",
    "TEXTRON": "TXT",
    "BELL HELICOPTER": "TXT",

    # ---- Automotive ----
    "GENERAL MOTORS": "GM",
    "GM": "GM",
    "CHEVROLET": "GM",
    "CADILLAC": "GM",
    "BUICK": "GM",
    "GMC": "GM",
    "FORD MOTOR": "F",
    "FORD": "F",
    "LINCOLN MOTOR": "F",
    "TESLA": "TSLA",
    "STELLANTIS": "STLA",
    "CHRYSLER": "STLA",
    "DODGE": "STLA",
    "JEEP": "STLA",
    "RAM TRUCKS": "STLA",
    "RIVIAN": "RIVN",

    # ---- Financial ----
    "JPMORGAN": "JPM",
    "JPMORGAN CHASE": "JPM",
    "JP MORGAN": "JPM",
    "CHASE BANK": "JPM",
    "CHASE": "JPM",
    "BANK OF AMERICA": "BAC",
    "MERRILL LYNCH": "BAC",
    "MERRILL": "BAC",
    "WELLS FARGO": "WFC",
    "CITIGROUP": "C",
    "CITIBANK": "C",
    "CITI": "C",
    "GOLDMAN SACHS": "GS",
    "MORGAN STANLEY": "MS",
    "CHARLES SCHWAB": "SCHW",
    "SCHWAB": "SCHW",
    "TD AMERITRADE": "SCHW",
    "FIDELITY": "FNF",
    "AMERICAN EXPRESS": "AXP",
    "AMEX": "AXP",
    "CAPITAL ONE": "COF",
    "DISCOVER FINANCIAL": "DFS",
    "US BANCORP": "USB",
    "US BANK": "USB",
    "PNC FINANCIAL": "PNC",
    "PNC BANK": "PNC",
    "TRUIST": "TFC",
    "BB&T": "TFC",
    "SUNTRUST": "TFC",
    "STATE STREET": "STT",
    "BNY MELLON": "BK",
    "BANK OF NEW YORK": "BK",
    "NORTHERN TRUST": "NTRS",
    "ALLY FINANCIAL": "ALLY",

    # ---- Telecom / Media ----
    "AT&T": "T",
    "ATT": "T",
    "AT AND T": "T",
    "DIRECTV": "T",
    "VERIZON": "VZ",
    "VERIZON WIRELESS": "VZ",
    "TRACFONE": "VZ",
    "T-MOBILE": "TMUS",
    "TMOBILE": "TMUS",
    "SPRINT": "TMUS",
    "METRO PCS": "TMUS",
    "COMCAST": "CMCSA",
    "XFINITY": "CMCSA",
    "NBCUNIVERSAL": "CMCSA",
    "NBC": "CMCSA",
    "UNIVERSAL PICTURES": "CMCSA",
    "DISNEY": "DIS",
    "WALT DISNEY": "DIS",
    "ABC": "DIS",
    "ESPN": "DIS",
    "PIXAR": "DIS",
    "MARVEL": "DIS",
    "LUCASFILM": "DIS",
    "21ST CENTURY FOX": "FOX",
    "FOX CORPORATION": "FOX",
    "FOX NEWS": "FOX",
    "WARNER BROS DISCOVERY": "WBD",
    "WARNER BROS": "WBD",
    "WARNER MEDIA": "WBD",
    "HBO": "WBD",
    "CNN": "WBD",
    "PARAMOUNT": "PARA",
    "CBS": "PARA",
    "VIACOM": "PARA",
    "NETFLIX": "NFLX",
    "CHARTER COMMUNICATIONS": "CHTR",
    "SPECTRUM": "CHTR",
    "COX COMMUNICATIONS": "COX",

    # ---- Food / Beverage ----
    "COCA-COLA": "KO",
    "COCA COLA": "KO",
    "COKE": "KO",
    "PEPSICO": "PEP",
    "PEPSI": "PEP",
    "FRITO LAY": "PEP",
    "FRITO-LAY": "PEP",
    "QUAKER OATS": "PEP",
    "GATORADE": "PEP",
    "MCDONALDS": "MCD",
    "MCDONALD'S": "MCD",
    "STARBUCKS": "SBUX",
    "CHIPOTLE": "CMG",
    "YUM BRANDS": "YUM",
    "TACO BELL": "YUM",
    "KFC": "YUM",
    "PIZZA HUT": "YUM",
    "DARDEN RESTAURANTS": "DRI",
    "OLIVE GARDEN": "DRI",
    "LONGHORN STEAKHOUSE": "DRI",
    "TYSON FOODS": "TSN",
    "TYSON": "TSN",
    "GENERAL MILLS": "GIS",
    "KELLOGG": "K",
    "KELLANOVA": "K",
    "MONDELEZ": "MDLZ",
    "KRAFT HEINZ": "KHC",
    "CONAGRA": "CAG",
    "HORMEL": "HRL",
    "HERSHEY": "HSY",
    "CAMPBELL SOUP": "CPB",
    "SMUCKER": "SJM",
    "JM SMUCKER": "SJM",
    "ARCHER DANIELS MIDLAND": "ADM",
    "ADM": "ADM",
    "SYSCO": "SYY",

    # ---- Consumer / Industrial ----
    "PROCTER & GAMBLE": "PG",
    "PROCTER AND GAMBLE": "PG",
    "P&G": "PG",
    "COLGATE-PALMOLIVE": "CL",
    "COLGATE PALMOLIVE": "CL",
    "COLGATE": "CL",
    "KIMBERLY-CLARK": "KMB",
    "KIMBERLY CLARK": "KMB",
    "3M": "MMM",
    "HONEYWELL": "HON",
    "GENERAL ELECTRIC": "GE",
    "GE AEROSPACE": "GE",
    "GE VERNOVA": "GEV",
    "GE HEALTHCARE": "GEHC",
    "CATERPILLAR": "CAT",
    "JOHN DEERE": "DE",
    "DEERE": "DE",
    "EMERSON ELECTRIC": "EMR",
    "EMERSON": "EMR",
    "PARKER HANNIFIN": "PH",
    "ILLINOIS TOOL WORKS": "ITW",
    "ITW": "ITW",
    "EATON": "ETN",
    "CUMMINS": "CMI",
    "PACCAR": "PCAR",
    "CARRIER GLOBAL": "CARR",
    "CARRIER": "CARR",
    "OTIS ELEVATOR": "OTIS",
    "OTIS": "OTIS",
    "NIKE": "NKE",
    "CONVERSE": "NKE",

    # ---- Energy ----
    "EXXONMOBIL": "XOM",
    "EXXON MOBIL": "XOM",
    "EXXON": "XOM",
    "MOBIL": "XOM",
    "CHEVRON": "CVX",
    "CONOCOPHILLIPS": "COP",
    "CONOCO": "COP",
    "PHILLIPS 66": "PSX",
    "MARATHON PETROLEUM": "MPC",
    "MARATHON OIL": "MRO",
    "VALERO": "VLO",
    "SCHLUMBERGER": "SLB",
    "SLB": "SLB",
    "HALLIBURTON": "HAL",
    "BAKER HUGHES": "BKR",
    "DEVON ENERGY": "DVN",
    "PIONEER NATURAL": "PXD",
    "EOG RESOURCES": "EOG",
    "OCCIDENTAL PETROLEUM": "OXY",
    "OCCIDENTAL": "OXY",

    # ---- Misc / Other ----
    "VISA": "V",
    "MASTERCARD": "MA",
    "BERKSHIRE HATHAWAY": "BRK.B",
    "GEICO": "BRK.B",
    "DURACELL": "BRK.B",
    "DAIRY QUEEN": "BRK.B",
    "BLACKROCK": "BLK",
    "VANGUARD": "VTI",
    "LOCKHEED": "LMT",
    "WASTE MANAGEMENT": "WM",
    "REPUBLIC SERVICES": "RSG",
    "CINTAS": "CTAS",
    "AUTOMATIC DATA PROCESSING": "ADP",
    "ADP": "ADP",
    "PAYCHEX": "PAYX",
    "SHERWIN-WILLIAMS": "SHW",
    "SHERWIN WILLIAMS": "SHW",
    "PPG": "PPG",
    "DOW": "DOW",
    "DUPONT": "DD",
    "INTERNATIONAL PAPER": "IP",
    "WEYERHAEUSER": "WY",
}

# Patterns to strip before matching
_LOCATION_TAIL = re.compile(r"\s*[-–—]\s*[A-Z][A-Za-z\s,]+(?:AL|AK|AZ|AR|CA|CO|CT|DE|FL|GA|HI|ID|IL|IN|IA|KS|KY|LA|ME|MD|MA|MI|MN|MS|MO|MT|NE|NV|NH|NJ|NM|NY|NC|ND|OH|OK|OR|PA|RI|SC|SD|TN|TX|UT|VT|VA|WA|WV|WI|WY)\s*$")
_STORE_NUMBER = re.compile(r"\s*(?:#|STORE\s*#?|PLANT\s*#?|FACILITY\s*#?|UNIT\s*#?|LOCATION\s*#?|SITE\s*#?)\s*\d+", re.IGNORECASE)
_DBA = re.compile(r"\s*(?:D/?B/?A|DOING BUSINESS AS)\s+", re.IGNORECASE)
_SUFFIX = re.compile(
    r"\s*\b(?:INC\.?|CORP\.?|CORPORATION|CO\.?|LLC|LTD\.?|LP|L\.P\.|PLC|"
    r"HOLDINGS?|HOLDCO|GROUP|ENTERPRISES?|INTERNATIONAL|INTL\.?|COMPANY|"
    r"SERVICES?|SOLUTIONS?|TECHNOLOG(?:Y|IES)|INDUSTR(?:Y|IES)|N\.?A\.?)\s*",
    re.IGNORECASE,
)
_PUNCT = re.compile(r"[,.'\"!@#$%^&*()\[\]{}]")
_WHITESPACE = re.compile(r"\s+")


def _normalize(name: str) -> str:
    """Normalize a company name for subsidiary matching."""
    name = name.upper().strip()
    # Strip location suffix ("- Springfield, IL")
    name = _LOCATION_TAIL.sub("", name)
    # Strip store / plant / facility numbers
    name = _STORE_NUMBER.sub("", name)
    # Handle DBA: keep only the part after "DBA"
    parts = _DBA.split(name, maxsplit=1)
    if len(parts) == 2:
        # Use the DBA name for matching
        name = parts[1]
    # Strip punctuation
    name = _PUNCT.sub("", name)
    # Strip corporate suffixes
    name = _SUFFIX.sub(" ", name)
    name = _WHITESPACE.sub(" ", name).strip()
    return name


def lookup_subsidiary(name: str) -> Optional[str]:
    """Look up a subsidiary / DBA name and return the parent ticker.

    Performs normalized exact match first, then checks whether any known
    subsidiary key is a prefix of the normalized input.

    Returns the ticker string (e.g. ``"AMZN"``) or ``None``.
    """
    if not name or not name.strip():
        return None

    norm = _normalize(name)
    if not norm:
        return None

    # 1. Exact match
    if norm in _SUBSIDIARY_TO_TICKER:
        return _SUBSIDIARY_TO_TICKER[norm]

    # 2. Prefix / substring match — try longest keys first for specificity
    for key in sorted(_SUBSIDIARY_TO_TICKER, key=len, reverse=True):
        if len(key) < 3:
            continue  # skip very short keys to avoid false positives
        if norm.startswith(key) or key in norm:
            return _SUBSIDIARY_TO_TICKER[key]

    return None
