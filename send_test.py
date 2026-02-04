import json
import requests

URL = "http://210.125.91.90:8001/api/threat/ingest"

def make_payload(include_ms=False, include_ppg23=False):
    # time 문자열을 "00:00:00" 또는 "00:00:00.040" 형식으로 생성
    samples = []
    for i in range(150):
        # 25Hz -> 40ms 간격, 총 0..5960ms
        ms = i * 40
        if include_ms:
            # HH:MM:SS.mmm 형태
            time_str = f"00:00:{ms//1000:02d}.{ms%1000:03d}"
        else:
            # 문서처럼 초까지만
            time_str = "00:00:00"

        s = {
            "time": time_str,
            "ax": 0.1 + (i % 5) * 0.01,
            "ay": -0.02 + (i % 3) * 0.01,
            "az": 9.8,
            "ppg_green": 52000 + i,
        }
        if include_ppg23:
            s["ppg_ir"] = 51000 + i
            s["ppg_red"] = 53000 + i

        samples.append(s)

    return {
        "device_id": "melpy-001",
        "sos_id": "SOS-TEST-001",
        "window_sec": 6,
        "hz": 25,
        "samples": samples,
    }

if __name__ == "__main__":
    payload = make_payload(include_ms=False, include_ppg23=False)
    r = requests.post(URL, json=payload, timeout=30)
    print("status:", r.status_code)
    print("response:", r.text)
