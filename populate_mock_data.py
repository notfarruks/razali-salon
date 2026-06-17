"""
RAZALI — Mock Data Populator
Clears and repopulates Masters, Services, Service_Duration sheets with demo data.
Does NOT touch the Bookings sheet.
Run: python populate_mock_data.py
"""
import json, os, sys
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

    # ── Masters ────────────────────────────────────────────────────────────────
    masters_data = [
        ["id","name","specialty","experience","mon","tue","wed","thu","fri","sat","sun","start_time","end_time"],
        [1,"Aysel Həsənova","Nails","5 il","TRUE","TRUE","TRUE","TRUE","TRUE","TRUE","FALSE","09:00","19:00"],
        [2,"Nigar Əliyeva","Hair","8 il","TRUE","TRUE","FALSE","TRUE","TRUE","TRUE","FALSE","10:00","20:00"],
        [3,"Leyla Mustafayeva","Make Up","3 il","FALSE","TRUE","TRUE","TRUE","TRUE","TRUE","FALSE","11:00","19:00"],
        [4,"Günel Hüseynova","Nails & Brows","6 il","TRUE","TRUE","TRUE","FALSE","TRUE","TRUE","FALSE","09:00","18:00"],
        [5,"Sevinc Babayeva","Hair & Make Up","4 il","TRUE","FALSE","TRUE","TRUE","TRUE","FALSE","TRUE","10:00","19:00"],
        [6,"Nərmin Quliyeva","Nails","2 il","TRUE","TRUE","TRUE","TRUE","FALSE","TRUE","FALSE","09:00","17:00"],
        [7,"Aytən Rəsulova","Brows","5 il","FALSE","TRUE","TRUE","TRUE","TRUE","TRUE","TRUE","10:00","18:00"],
    ]

    # ── Services ───────────────────────────────────────────────────────────────
    services_data = [
        ["id","category","name","price"],
        [1,"Nails","Gel manicure",35],
        [2,"Nails","Classic manicure",20],
        [3,"Nails","Gel pedicure",55],
        [4,"Nails","Classic pedicure",40],
        [5,"Nails","Nail design (per nail)",15],
        [6,"Hair","Haircut & blowout",35],
        [7,"Hair","Hair colouring",80],
        [8,"Hair","Highlights",120],
        [9,"Hair","Keratin treatment",100],
        [10,"Hair","Blowout",25],
        [11,"Make Up","Day makeup",60],
        [12,"Make Up","Evening makeup",80],
        [13,"Make Up","Bridal makeup",150],
        [14,"Brows","Brow threading",10],
        [15,"Brows","Brow lamination",45],
    ]

    # ── Service_Duration ───────────────────────────────────────────────────────
    # (master_id, service_id, duration_mins)
    durations = []
    # Master 1 — Aysel: Nails (1-5)
    durations += [[1,1,60],[1,2,45],[1,3,75],[1,4,60],[1,5,30]]
    # Master 2 — Nigar: Hair (6-10)
    durations += [[2,6,45],[2,7,120],[2,8,150],[2,9,90],[2,10,30]]
    # Master 3 — Leyla: Make Up (11-13)
    durations += [[3,11,60],[3,12,75],[3,13,120]]
    # Master 4 — Günel: Nails (1-5) + Brows (14-15)
    durations += [[4,1,60],[4,2,45],[4,3,75],[4,4,60],[4,5,30],[4,14,20],[4,15,60]]
    # Master 5 — Sevinc: Hair (6-10) + Make Up (11-12)
    durations += [[5,6,45],[5,7,120],[5,8,150],[5,10,30],[5,11,60],[5,12,75]]
    # Master 6 — Nərmin: Nails (1-5)
    durations += [[6,1,60],[6,2,45],[6,3,75],[6,4,60],[6,5,30]]
    # Master 7 — Aytən: Brows (14-15)
    durations += [[7,14,20],[7,15,60]]

    duration_data = [["master_id","service_id","duration_mins"]] + durations

    print("Clearing and writing Masters...")
    ws = db.worksheet("Masters")
    ws.clear()
    ws.update(masters_data, value_input_option="USER_ENTERED")

    print("Clearing and writing Services...")
    ws = db.worksheet("Services")
    ws.clear()
    ws.update(services_data, value_input_option="USER_ENTERED")

    print("Clearing and writing Service_Duration...")
    ws = db.worksheet("Service_Duration")
    ws.clear()
    ws.update(duration_data, value_input_option="USER_ENTERED")

    print("✅ Mock data populated successfully!")
    print(f"   Masters:          {len(masters_data)-1}")
    print(f"   Services:         {len(services_data)-1}")
    print(f"   Duration entries: {len(durations)}")

if __name__ == "__main__":
    main()
