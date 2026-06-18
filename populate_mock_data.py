"""
RAZALI — Real Data Populator
Updates Masters, Services, and Service_Duration sheets with actual salon data.
Does NOT touch the Bookings sheet.
Run: python populate_mock_data.py
"""
import json, os
import gspread
from google.oauth2.service_account import Credentials

CREDS_FILE = os.path.join(os.path.dirname(__file__), "creds.json")

def main():
    with open(CREDS_FILE) as f:
        creds_json = json.load(f)
    scopes = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_info(creds_json, scopes=scopes)
    client = gspread.authorize(creds)
    db = client.open("RAZALI_DB")

    # ── Masters — Tue–Sun, 09:00–23:00 ────────────────────────────────────────
    masters_data = [
        ["id","name","specialty","experience","mon","tue","wed","thu","fri","sat","sun","start_time","end_time"],
        [1, "Aysel Həsənova",  "Nails", "5 il", "FALSE","TRUE","TRUE","TRUE","TRUE","TRUE","TRUE","09:00","23:00"],
        [2, "Nigar Əliyeva",   "Hair",  "8 il", "FALSE","TRUE","TRUE","TRUE","TRUE","TRUE","TRUE","09:00","23:00"],
        [3, "Günel Hüseynova", "Nails", "6 il", "FALSE","TRUE","TRUE","TRUE","TRUE","TRUE","TRUE","09:00","23:00"],
        [4, "Sevinc Babayeva", "Hair",  "4 il", "FALSE","TRUE","TRUE","TRUE","TRUE","TRUE","TRUE","09:00","23:00"],
        [5, "Nərmin Quliyeva", "Nails", "2 il", "FALSE","TRUE","TRUE","TRUE","TRUE","TRUE","TRUE","09:00","23:00"],
    ]

    # ── Services — Nails (13) + Hair (10) only ─────────────────────────────────
    services_data = [
        ["id","category","name","price"],
        # Nails
        [1,  "Nails", "Klassik manikur",                15],
        [2,  "Nails", "Söküm/Shellak+manikur",          20],
        [3,  "Nails", "Manikur+Shellak",                 25],
        [4,  "Nails", "Manikur+Gellak",                  35],
        [5,  "Nails", "Qaynaq (70-90 AZN)",              70],
        [6,  "Nails", "Korreksiya (40-55 AZN)",          40],
        [7,  "Nails", "French/Dizayn/Vtirka",             5],
        [8,  "Nails", "Qaynağın sökülməsi",              10],
        [9,  "Nails", "Klassik SPA pedikur",             30],
        [10, "Nails", "SPA pedikur+Shellak",             40],
        [11, "Nails", "Manikur+pedikur (4 əl, +10 AZN)", 10],
        [12, "Nails", "Əl üçün parafin baxımı",          10],
        [13, "Nails", "Ayaq üçün parafin baxımı",        20],
        # Hair
        [14, "Hair",  "Saç kəsimi (30-40 AZN)",          30],
        [15, "Hair",  "Ukladka (15-20 AZN)",              15],
        [16, "Hair",  "Havalı ukladka (25-30 AZN)",       25],
        [17, "Hair",  "Saç buran ilə buruq (30-40 AZN)",  30],
        [18, "Hair",  "Dib boyası",                       60],
        [19, "Hair",  "Tonlaşdırma",                     100],
        [20, "Hair",  "Blondlaşdırma (150-250 AZN)",     150],
        [21, "Hair",  "Balyaj/Ombre (150-250 AZN)",      150],
        [22, "Hair",  "Keratin/Botox (80-200 AZN)",       80],
        [23, "Hair",  "Baxım",                            50],
    ]

    # ── Service_Duration ───────────────────────────────────────────────────────
    durations = []

    # Master 1 — Aysel (Nails)
    durations += [
        [1,1,45],[1,2,60],[1,3,60],[1,4,75],[1,5,120],
        [1,6,90],[1,7,20],[1,8,30],[1,9,60],[1,10,75],
        [1,11,90],[1,12,20],[1,13,30],
    ]
    # Master 2 — Nigar (Hair)
    durations += [
        [2,14,45],[2,15,30],[2,16,45],[2,17,45],[2,18,90],
        [2,19,120],[2,20,180],[2,21,180],[2,22,150],[2,23,60],
    ]
    # Master 3 — Günel (Nails)
    durations += [
        [3,1,45],[3,2,60],[3,3,60],[3,4,75],[3,5,120],
        [3,6,90],[3,7,20],[3,8,30],[3,9,60],[3,10,75],
        [3,11,90],[3,12,20],[3,13,30],
    ]
    # Master 4 — Sevinc (Hair)
    durations += [
        [4,14,45],[4,15,30],[4,16,45],[4,17,45],[4,18,90],
        [4,19,120],[4,20,180],[4,21,180],[4,22,150],[4,23,60],
    ]
    # Master 5 — Nərmin (Nails)
    durations += [
        [5,1,45],[5,2,60],[5,3,60],[5,4,75],[5,5,120],
        [5,6,90],[5,7,20],[5,8,30],[5,9,60],[5,10,75],
        [5,11,90],[5,12,20],[5,13,30],
    ]

    duration_data = [["master_id","service_id","duration_mins"]] + durations

    print("Updating Masters...")
    ws = db.worksheet("Masters")
    ws.clear()
    ws.update(masters_data, value_input_option="USER_ENTERED")

    print("Updating Services...")
    ws = db.worksheet("Services")
    ws.clear()
    ws.update(services_data, value_input_option="USER_ENTERED")

    print("Updating Service_Duration...")
    ws = db.worksheet("Service_Duration")
    ws.clear()
    ws.update(duration_data, value_input_option="USER_ENTERED")

    print("✅ Done!")
    print(f"   Masters:          {len(masters_data)-1}")
    print(f"   Services:         {len(services_data)-1}")
    print(f"   Duration entries: {len(durations)}")

if __name__ == "__main__":
    main()
