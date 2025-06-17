from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel, validator
from typing import List, Dict, Optional
from decimal import Decimal, ROUND_HALF_UP
import asyncpg
from uuid import uuid4
import os
from datetime import datetime

app = FastAPI(title="Expense Splitter API")

# Database connection pool
db_pool = None

async def init_db():
    global db_pool
    db_pool = await asyncpg.create_pool(
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD", "password"),
        database=os.getenv("DB_NAME", "expense_splitter"),
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", 5432)
    )

# Models
class ExpenseCreate(BaseModel):
    amount: float
    description: str
    paid_by: str

    @validator("amount")
    def validate_amount(cls, v):
        if v <= 0:
            raise ValueError("Amount must be positive")
        return Decimal(str(v)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

    @validator("description")
    def validate_description(cls, v):
        if not v.strip():
            raise ValueError("Description cannot be empty")
        return v

    @validator("paid_by")
    def validate_paid_by(cls, v):
        if not v.strip():
            raise ValueError("Paid_by cannot be empty")
        return v

class ExpenseResponse(BaseModel):
    id: str
    amount: float
    description: str
    paid_by: str
    created_at: datetime

class BalanceResponse(BaseModel):
    person: str
    balance: float

class Settlement(BaseModel):
    from_person: str
    to_person: str
    amount: float

# Database initialization
@app.on_event("startup")
async def startup_event():
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

# Helper functions
async def get_people():
    async with db_pool.acquire() as conn:
        people = await conn.fetch("SELECT DISTINCT paid_by FROM expenses")
        return [p["paid_by"] for p in people]

async def calculate_balances():
    people = await get_people()
    if not people:
        return []
    
    balances = {person: Decimal("0.00") for person in people}
    async with db_pool.acquire() as conn:
        expenses = await conn.fetch("SELECT amount, paid_by FROM expenses")
    
    total_spent = sum(Decimal(str(e["amount"])) for e in expenses)
    if total_spent == 0:
        return [{"person": p, "balance": 0.00} for p in people]
    
    fair_share = total_spent / len(people)
    
    for expense in expenses:
        amount = Decimal(str(expense["amount"]))
        balances[expense["paid_by"]] += amount
    
    return [
        {"person": p, "balance": float((balances[p] - fair_share).quantize(Decimal("0.01")))}
        for p in people
    ]

async def calculate_settlements():
    balances = await calculate_balances()
    if not balances:
        return []
    
    # Create lists of debtors and creditors
    debtors = [(b["person"], Decimal(str(b["balance"]))) for b in balances if b["balance"] < 0]
    creditors = [(b["person"], Decimal(str(b["balance"]))) for b in balances if b["balance"] > 0]
    
    settlements = []
    i, j = 0, 0
    
    while i < len(debtors) and j < len(creditors):
        debtor, debt = debtors[i]
        creditor, credit = creditors[j]
        amount = min(-debt, credit)
        
        if amount > 0:
            settlements.append({
                "from_person": debtor,
                "to_person": creditor,
                "amount": float(amount.quantize(Decimal("0.01")))
            })
        
        debtors[i] = (debtor, debt + amount)
        creditors[j] = (creditor, credit - amount)
        
        if debtors[i][1] >= 0:
            i += 1
        if creditors[j][1] <= 0:
            j += 1
    
    return settlements

# API Endpoints
@app.get("/expenses", response_model=Dict[str, List[ExpenseResponse]])
async def get_expenses():
    async with db_pool.acquire() as conn:
        expenses = await conn.fetch("SELECT * FROM expenses")
    return {
        "success": True,
        "data": [dict(e) for e in expenses],
        "message": "Expenses retrieved successfully"
    }

@app.post("/expenses", response_model=Dict[str, ExpenseResponse])
async def add_expense(expense: ExpenseCreate):
    expense_id = str(uuid4())
    async with db_pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO expenses (id, amount, description, paid_by)
            VALUES ($1, $2, $3, $4)
            """,
            expense_id, expense.amount, expense.description, expense.paid_by
        )
        new_expense = await conn.fetchrow(
            "SELECT * FROM expenses WHERE id = $1", expense_id
        )
    return {
        "success": True,
        "data": dict(new_expense),
        "message": "Expense added successfully"
    }

@app.put("/expenses/{expense_id}", response_model=Dict[str, ExpenseResponse])
async def update_expense(expense_id: str, expense: ExpenseCreate):
    async with db_pool.acquire() as conn:
        result = await conn.fetchrow(
            """
            UPDATE expenses 
            SET amount = $1, description = $2, paid_by = $3
            WHERE id = $4
            RETURNING *
            """,
            expense.amount, expense.description, expense.paid_by, expense_id
        )
        if not result:
            raise HTTPException(status_code=404, detail="Expense not found")
    return {
        "success": True,
        "data": dict(result),
        "message": "Expense updated successfully"
    }

@app.delete("/expenses/{expense_id}", response_model=Dict[str, str])
async def delete_expense(expense_id: str):
    async with db_pool.acquire() as conn:
        result = await conn.execute(
            "DELETE FROM expenses WHERE id = $1", expense_id
        )
        if result == "DELETE 0":
            raise HTTPException(status_code=404, detail="Expense not found")
    return {
        "success": True,
        "data": "",
        "message": "Expense deleted successfully"
    }

@app.get("/settlements", response_model=Dict[str, List[Settlement]])
async def get_settlements():
    settlements = await calculate_settlements()
    return {
        "success": True,
        "data": settlements,
        "message": "Settlements calculated successfully"
    }

@app.get("/balances", response_model=Dict[str, List[BalanceResponse]])
async def get_balances():
    balances = await calculate_balances()
    return {
        "success": True,
        "data": balances,
        "message": "Balances retrieved successfully"
    }

@app.get("/people", response_model=Dict[str, List[str]])
async def get_people_list():
    people = await get_people()
    return {
        "success": True,
        "data": people,
        "message": "People retrieved successfully"
    }

# Error handling
@app.exception_handler(HTTPException)
async def http_exception_handler(request, exc):
    return {
        "success": False,
        "data": None,
        "message": str(exc.detail)
    }, exc.status_code