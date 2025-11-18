import requests
import random
import time
from datetime import datetime, timedelta


API_URL = "http://localhost:8000/loan_event"


def generate_loan_event():
    return {
        "id": random.randint(1, 100000),
        "member_id": random.randint(1, 50000),
        "loan_amnt": round(random.uniform(1000, 35000), 2),
        "funded_amnt": round(random.uniform(1000, 35000), 2),
        "funded_amnt_inv": round(random.uniform(1000, 35000), 2),
        "term": random.choice(["36 months", "60 months"]),
        "int_rate": round(random.uniform(5, 25), 2),
        "installment": round(random.uniform(50, 600), 2),
        "grade": random.choice(list("ABCDEFG")),
        "sub_grade": random.choice(["A1", "A2", "B1", "B2", "C1"]),
        "emp_title": random.choice(["Engineer", "Teacher", "Manager", "Analyst"]),
        "emp_length": random.choice(["< 1 year", "2 years", "5 years", "10+ years"]),
        "home_ownership": random.choice(["RENT", "OWN", "MORTGAGE"]),
        "annual_inc": round(random.uniform(20000, 150000), 2),
        "verification_status": random.choice(["Verified", "Not Verified", "Source Verified"]),
        "issue_d": "2024-01-01",
        "loan_status": random.choice(["Current", "Fully Paid", "Charged Off"]),
        "pymnt_plan": "n",
        "url": "http://example.com",
        "desc": "Loan for personal use",
        "purpose": random.choice(["credit_card", "debt_consolidation", "home_improvement", "car"]),
        "title": "Personal Loan",
        "zip_code": f"{random.randint(100, 999)}xx",
        "addr_state": random.choice(["CA", "NY", "TX", "FL", "IL"]),
        "dti": round(random.uniform(0, 40), 2),
        "delinq_2yrs": float(random.randint(0, 2)),
        "earliest_cr_line": "2005-01-01",
        "inq_last_6mths": float(random.randint(0, 3)),
        "mths_since_last_delinq": float(random.randint(10, 50)) if random.random() > 0.3 else None,
        "mths_since_last_record": None,
        "open_acc": float(random.randint(5, 20)),
        "pub_rec": float(random.randint(0, 1)),
        "revol_bal": round(random.uniform(0, 20000), 2),
        "revol_util": round(random.uniform(0, 100), 2),
        "total_acc": float(random.randint(10, 40)),
        "initial_list_status": random.choice(["f", "w"]),
        "out_prncp": round(random.uniform(0, 35000), 2),
        "out_prncp_inv": round(random.uniform(0, 35000), 2),
        "total_pymnt": round(random.uniform(0, 35000), 2),
        "total_pymnt_inv": round(random.uniform(0, 35000), 2),
        "total_rec_prncp": round(random.uniform(0, 35000), 2),
        "total_rec_int": round(random.uniform(0, 3500), 2),
        "total_rec_late_fee": round(random.uniform(0, 100), 2),
        "recoveries": round(random.uniform(0, 1000), 2),
        "collection_recovery_fee": round(random.uniform(0, 100), 2),
        "last_pymnt_d": "2024-01-01",
        "last_pymnt_amnt": round(random.uniform(0, 35000), 2),
        "next_pymnt_d": "2024-02-01",
        "last_credit_pull_d": "2024-01-01",
        "collections_12_mths_ex_med": 0.0,
        "mths_since_last_major_derog": None,
        "policy_code": 1.0,
        "application_type": random.choice(["Individual", "Joint App"]),
        "annual_inc_joint": None,
        "dti_joint": None,
        "verification_status_joint": None,
        "acc_now_delinq": 0.0,
        "tot_coll_amt": 0.0,
        "tot_cur_bal": 0.0,
        "open_acc_6m": 0.0,
        "open_il_6m": 0.0,
        "open_il_12m": 0.0,
        "open_il_24m": 0.0,
        "mths_since_rcnt_il": None,
        "total_bal_il": 0.0,
        "il_util": None,
        "open_rv_12m": 0.0,
        "open_rv_24m": 0.0,
        "max_bal_bc": 0.0,
        "all_util": None,
        "total_rev_hi_lim": 0.0,
        "inq_fi": 0.0,
        "total_cu_tl": 0.0,
        "inq_last_12m": 0.0
    }


def send_loan_event(event_data):
    """Gửi loan event đến API"""
    try:
        response = requests.post(API_URL, json=event_data, timeout=5)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error sending event: {e}")
        return None


def stream_events(num_events=100, interval=1.0):
    """
    Stream events liên tục
    
    Args:
        num_events: Số lượng events (-1 = infinite)
        interval: Thời gian chờ giữa các events (seconds)
    """
    print(f"   Starting to stream loan events to {API_URL}")
    print(f"   Sending {'infinite' if num_events == -1 else num_events} events with {interval}s interval")
    print("-" * 60)
    
    count = 0
    success_count = 0
    
    try:
        while num_events == -1 or count < num_events:
            count += 1
            
            # Generate event
            event = generate_loan_event()
            
            # Send event
            result = send_loan_event(event)
            
            if result:
                success_count += 1
                print(f"   [{count}] Event sent successfully | ID: {event['id']} | "
                      f"Amount: ${event['loan_amnt']:,.2f} | Grade: {event['grade']}")
            else:
                print(f"   [{count}] Failed to send event")
            
            # Wait before next event
            time.sleep(interval)
            
    except KeyboardInterrupt:
        print("\n   Streaming stopped by user")
    
    print("-" * 60)
    print(f"   Summary: {success_count}/{count} events sent successfully")


def send_single_event():
    """Gửi một event đơn lẻ (để test)"""
    event = generate_loan_event()
    print("   Sending loan event...")
    print(f"   ID: {event['id']}")
    print(f"   Amount: ${event['loan_amnt']:,.2f}")
    print(f"   Grade: {event['grade']}")
    
    result = send_loan_event(event)
    
    if result:
        print("   Success!")
        print(f"   Kafka Topic: {result['kafka_topic']}")
        print(f"   Kafka Partition: {result['kafka_partition']}")
        print(f"   Kafka Offset: {result['kafka_offset']}")
    else:
        print("   Failed to send event")


def check_api_health():
    """Kiểm tra health của API"""
    try:
        response = requests.get("http://localhost:8000/health", timeout=5)
        response.raise_for_status()
        health = response.json()
        print("   API is healthy")
        print(f"   Status: {health['status']}")
        print(f"   Kafka: {health['kafka']}")
        return True
    except Exception as e:
        print(f"   API health check failed: {e}")
        return False


if __name__ == "__main__":
    import sys
    
    # Check API health first
    print("   Checking API health...")
    if not check_api_health():
        print("   API is not responding. Make sure the API server is running.")
        sys.exit(1)
    
    print("\n" + "="*60)
    print("Select mode:")
    print("  1. Send single event (test)")
    print("  2. Stream events continuously")
    print("  3. Stream 10 events with 1s interval")
    print("  4. Stream 100 events with 0.5s interval")
    print("="*60)
    
    choice = input("Enter choice (1-4): ").strip()
    
    if choice == "1":
        send_single_event()
    elif choice == "2":
        stream_events(num_events=-1, interval=1.0)
    elif choice == "3":
        stream_events(num_events=10, interval=1.0)
    elif choice == "4":
        stream_events(num_events=100, interval=0.5)
    else:
        print("Invalid choice")