from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from typing import Optional
from kafka import KafkaProducer
import json
import uvicorn
import time
from kafka.errors import NoBrokersAvailable
from datetime import datetime

app = FastAPI(title="Loan Event API", version="1.0.0")

for _ in range(10):
    try:
        producer = KafkaProducer(
            bootstrap_servers="kafka:29092",
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        break
    except NoBrokersAvailable:
        time.sleep(3)
else:
    raise Exception("Kafka not available after retries")


# Pydantic model để validate input
class LoanEvent(BaseModel):
    id: int = Field(..., description="Loan ID")
    member_id: int = Field(..., description="Member ID")
    loan_amnt: float = Field(..., gt=0, description="Loan amount")
    funded_amnt: float = Field(..., gt=0)
    funded_amnt_inv: float = Field(..., gt=0)
    term: str = Field(..., description="Loan term (e.g., '36 months')")
    int_rate: float = Field(..., gt=0, lt=100, description="Interest rate")
    installment: float = Field(..., gt=0)
    grade: str = Field(..., pattern="^[A-G]$", description="Loan grade A-G")
    sub_grade: str
    emp_title: Optional[str] = None
    emp_length: Optional[str] = None
    home_ownership: str
    annual_inc: float = Field(..., gt=0)
    verification_status: str
    issue_d: str
    loan_status: str
    pymnt_plan: str
    url: Optional[str] = None
    desc: Optional[str] = None
    purpose: str
    title: Optional[str] = None
    zip_code: str
    addr_state: str
    dti: float
    delinq_2yrs: Optional[float] = 0.0
    earliest_cr_line: str
    inq_last_6mths: Optional[float] = 0.0
    mths_since_last_delinq: Optional[float] = None
    mths_since_last_record: Optional[float] = None
    open_acc: Optional[float] = 0.0
    pub_rec: Optional[float] = 0.0
    revol_bal: Optional[float] = 0.0
    revol_util: Optional[float] = 0.0
    total_acc: Optional[float] = 0.0
    initial_list_status: str
    out_prncp: float
    out_prncp_inv: float
    total_pymnt: float
    total_pymnt_inv: float
    total_rec_prncp: float
    total_rec_int: float
    total_rec_late_fee: float
    recoveries: float
    collection_recovery_fee: float
    last_pymnt_d: Optional[str] = None
    last_pymnt_amnt: float
    next_pymnt_d: Optional[str] = None
    last_credit_pull_d: Optional[str] = None
    collections_12_mths_ex_med: Optional[float] = 0.0
    mths_since_last_major_derog: Optional[float] = None
    policy_code: Optional[float] = 1.0
    application_type: str
    annual_inc_joint: Optional[float] = None
    dti_joint: Optional[float] = None
    verification_status_joint: Optional[str] = None
    acc_now_delinq: Optional[float] = 0.0
    tot_coll_amt: Optional[float] = 0.0
    tot_cur_bal: Optional[float] = 0.0
    open_acc_6m: Optional[float] = 0.0
    open_il_6m: Optional[float] = 0.0
    open_il_12m: Optional[float] = 0.0
    open_il_24m: Optional[float] = 0.0
    mths_since_rcnt_il: Optional[float] = None
    total_bal_il: Optional[float] = 0.0
    il_util: Optional[float] = None
    open_rv_12m: Optional[float] = 0.0
    open_rv_24m: Optional[float] = 0.0
    max_bal_bc: Optional[float] = 0.0
    all_util: Optional[float] = None
    total_rev_hi_lim: Optional[float] = 0.0
    inq_fi: Optional[float] = 0.0
    total_cu_tl: Optional[float] = 0.0
    inq_last_12m: Optional[float] = 0.0

    class Config:
        json_schema_extra = {
            "example": {
                "id": 1,
                "member_id": 12345,
                "loan_amnt": 10000.0,
                "funded_amnt": 10000.0,
                "funded_amnt_inv": 10000.0,
                "term": "36 months",
                "int_rate": 10.5,
                "installment": 322.5,
                "grade": "B",
                "sub_grade": "B2",
                "emp_title": "Engineer",
                "emp_length": "5 years",
                "home_ownership": "RENT",
                "annual_inc": 60000.0,
                "verification_status": "Verified",
                "issue_d": "2024-01-01",
                "loan_status": "Current",
                "pymnt_plan": "n",
                "purpose": "debt_consolidation",
                "zip_code": "940xx",
                "addr_state": "CA",
                "dti": 15.5,
                "delinq_2yrs": 0.0,
                "earliest_cr_line": "2010-01-01",
                "inq_last_6mths": 1.0,
                "mths_since_last_delinq": None,
                "mths_since_last_record": None,
                "open_acc": 10.0,
                "pub_rec": 0.0,
                "revol_bal": 5000.0,
                "revol_util": 50.0,
                "total_acc": 20.0,
                "initial_list_status": "f",
                "out_prncp": 8500.0,
                "out_prncp_inv": 8500.0,
                "total_pymnt": 1500.0,
                "total_pymnt_inv": 1500.0,
                "total_rec_prncp": 1500.0,
                "total_rec_int": 150.0,
                "total_rec_late_fee": 0.0,
                "recoveries": 0.0,
                "collection_recovery_fee": 0.0,
                "last_pymnt_d": "2024-02-01",
                "last_pymnt_amnt": 322.5,
                "next_pymnt_d": "2024-03-01",
                "last_credit_pull_d": "2024-01-01",
                "collections_12_mths_ex_med": 0.0,
                "mths_since_last_major_derog": None,
                "policy_code": 1.0,
                "application_type": "Individual",
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
        }


@app.get("/")
async def root():
    return {
        "message": "Loan Event Streaming API",
        "version": "1.0.0",
        "endpoints": {
            "POST /loan_event": "Submit a new loan event",
            "GET /health": "Health check"
        }
    }


@app.get("/health")
async def health_check():
    try:
        producer.bootstrap_connected()
        return {
            "status": "healthy",
            "kafka": "connected",
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Kafka connection failed: {str(e)}")


@app.post("/loan_event")
async def receive_loan_event(event: LoanEvent):
    """
    Endpoint để nhận loan event từ external system và push vào Kafka
    """
    try:
        # Convert Pydantic model to dict
        event_dict = event.dict()
        
        # Send to Kafka
        future = producer.send("loanEvents", event_dict)
        
        # Wait for confirmation (optional, có thể bỏ để tăng throughput)
        record_metadata = future.get(timeout=10)
        
        producer.flush()
        
        return {
            "status": "success",
            "message": "Loan event received and sent to Kafka",
            "event_id": event.id,
            "kafka_topic": record_metadata.topic,
            "kafka_partition": record_metadata.partition,
            "kafka_offset": record_metadata.offset,
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to process loan event: {str(e)}"
        )


@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup khi shutdown"""
    producer.close()
    print("Kafka producer closed")


if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )