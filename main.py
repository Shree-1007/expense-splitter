import os
import asyncpg
from fastapi import FastAPI, HTTPException
from contextlib import asynccontextmanager
from pydantic import BaseModel, field_validator
from decimal import Decimal, ROUND_HALF_UP
from typing import List, Dict
import uuid
from datetime import datetime
from urllib.parse import urlparse

app = FastAPI(title="Expense Splitter API")
db_pool = None

async def init_db():
    global db_pool
    # Try internal connection first using PG* variables
    db_user = os.getenv("PGUSER")
    db_password = os.getenv("PGPASSWORD")
    db_name = os.getenv("PGDATABASE")
    db_host = os.getenv("PGHOST")
    db_port = os.getenv("PGPORT")

    if not all([db_user, db_password, db_name, db_host, db_port]):
        missing = [k for k, v in [("PGUSER", db_user), ("PGPASSWORD", db_password),
                                  ("PGDATABASE", db_name), ("PGHOST", db_host),
                                  ("PGPORT", db_port)] if not v]
        raise ValueError(f"Missing environment variables for internal connection: {missing}")

    try:
        db_pool = await asyncpg.create_pool(
            user=db_user,
            password=db_password,
            database=db_name,
            host=db_host,
            port=int(db_port)
        )
        print("Connected to internal database successfully")
    except Exception as e:
        print(f"Internal connection failed: {str(e)}. Falling back to public connection.")
        # Fallback to public connection using DATABASE_PUBLIC_URL
        db_url = os.getenv("DATABASE_PUBLIC_URL")
        if not db_url:
            raise ValueError(f"Internal connection failed and DATABASE_PUBLIC_URL not set: {str(e)}")

        parsed_url = urlparse(db_url)
        db_user = parsed_url.username
        db_password = parsed_url.password
        db_name = parsed_url.path.lstrip('/')
        db_host = parsed_url.hostname
        db_port = parsed_url.port

        if not all([db_user, db_password, db_name, db_host, db_port]):
            raise ValueError(f"Invalid DATABASE_PUBLIC_URL: {db_url}")

        try:
            db_pool = await asyncpg.create_pool(
                user=db_user,
                password=db_password,
                database=db_name,
                host=db_host,
                port=db_port
            )
            print("Connected to public database successfully")
        except Exception as e2:
            raise ValueError(f"Failed to connect with both internal and public connections: {str(e2)}")

@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    async with db_pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS expenses (
                id TEXT PRIMARY KEY,
                amount DECIMAL(10,2),
                description TEXT NOT NULL,
                paid_by TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
    yield
    await db_pool.close()

app = FastAPI(title="Expense Splitter API", lifespan=lifespan)

class ExpenseCreate(BaseModel):
    amount: float
    description: str
    paid_by: str

    @field_validator("amount")
    def validate_amount(cls, v):
        if v <= 0:
            raise ValueError("Amount must be positive")
        return Decimal(str(v)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

    @field_validator("description")
    def validate_description(cls, v):
        if not v.strip():
            raise ValueError("Description cannot be empty")
        return v

    @field_validator("paid_by")
    def validate_paid_by(cls, v):
        if not v.strip():
            raise ValueError("Paid_by cannot be empty")
        return v

class ExpenseOut(BaseModel):
    id: str
    amount: float
    description: str
    paid_by: str
    created_at: datetime

@app.get("/test-db")
async def test_db():
    try:
        async with db_pool.acquire() as conn:
            await conn.execute("SELECT 1")
        return {"status": "Database connection successful"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database connection failed: {str(e)}")

@app.post("/expenses", response_model=ExpenseOut)
async def create_expense(expense: ExpenseCreate):
    expense_id = str(uuid.uuid4())
    async with db_pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO expenses (id, amount, description, paid_by) VALUES ($1, $2, $3, $4)",
            expense_id, float(expense.amount), expense.description, expense.paid_by
        )
    return {**expense.dict(), "id": expense_id, "created_at": datetime.utcnow()}

@app.get("/expenses", response_model=List[ExpenseOut])
async def get_expenses():
    async with db_pool.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM expenses ORDER BY created_at DESC")
    return [dict(row) for row in rows]

@app.put("/expenses/{expense_id}", response_model=ExpenseOut)
async def update_expense(expense_id: str, expense: ExpenseCreate):
    async with db_pool.acquire() as conn:
        result = await conn.fetchrow(
            "UPDATE expenses SET amount = $1, description = $2, paid_by = $3 WHERE id = $4 RETURNING *",
            float(expense.amount), expense.description, expense.paid_by, expense_id
        )
        if not result:
            raise HTTPException(status_code=404, detail="Expense not found")
    return dict(result)

@app.delete("/expenses/{expense_id}")
async def delete_expense(expense_id: str):
    async with db_pool.acquire() as conn:
        result = await conn.execute("DELETE FROM expenses WHERE id = $1", expense_id)
        if result == "DELETE 0":
            raise HTTPException(status_code=404, detail="Expense not found")
    return {"message": "Expense deleted"}

@app.get("/settlements")
async def get_settlements():
    async with db_pool.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM expenses")
    
    if not rows:
        return {"settlements": []}

    total_amount = sum(float(row["amount"]) for row in rows)
    num_people = len(set(row["paid_by"] for row in rows))
    if num_people == 0:
        return {"settlements": []}
    
    fair_share = total_amount / num_people
    balances = {}
    for row in rows:
        person = row["paid_by"]
        amount = float(row["amount"])
        balances[person] = balances.get(person, 0) + amount
    
    for person in balances:
        balances[person] = balances[person] - fair_share
    
    settlements = []
    debtors = [(person, balance) for person, balance in balances.items() if balance < 0]
    creditors = [(person, balance) for person, balance in balances.items() if balance > 0]
    
    i, j = 0, 0
    while i < len(debtors) and j < len(creditors):
        debtor, debt = debtors[i]
        creditor, credit = creditors[j]
        debt, credit = abs(debt), abs(credit)
        
        amount = min(debt, credit)
        if amount > 0:
            settlements.append({
                "from": debtor,
                "to": creditor,
                "amount": round(amount, 2)
            })
        
        debt -= amount
        credit -= amount
        
        if debt <= 0.01:
            i += 1
        if credit <= 0.01:
            j += 1
        
        debtors[i] = (debtor, -debt) if i < len(debtors) else None
        creditors[j] = (creditor, credit) if j < len(creditors) else None
    
    return {"settlements": settlements}

@app.get("/balances", response_model=Dict[str, float])
async def get_balances():
    async with db_pool.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM expenses")
    
    if not rows:
        return {}
    
    total_amount = sum(float(row["amount"]) for row in rows)
    num_people = len(set(row["paid_by"] for row in rows))
    if num_people == 0:
        return {}
    
    fair_share = total_amount / num_people
    balances = {}
    for row in rows:
        person = row["paid_by"]
        amount = float(row["amount"])
        balances[person] = balances.get(person, 0) + amount
    
    for person in balances:
        balances[person] = round(balances[person] - fair_share, 2)
    
    return balances

@app.get("/people", response_model=List[str])
async def get_people():
    async with db_pool.acquire() as conn:
        rows = await conn.fetch("SELECT DISTINCT paid_by FROM expenses")
    return [row["paid_by"] for row in rows]