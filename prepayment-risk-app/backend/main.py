from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import create_engine, Column, Integer, String, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from pydantic import BaseModel
from typing import List

app = FastAPI()

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# SQLite setup
SQLALCHEMY_DATABASE_URL = "sqlite:///./prepayment_risk.db"
engine = create_engine(SQLALCHEMY_DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()


class PrepaymentData(Base):
    __tablename__ = "prepayment_data"

    id = Column(Integer, primary_key=True, index=True)
    month = Column(String, index=True)
    prepayment_rate = Column(Float)


Base.metadata.create_all(bind=engine)


class PrepaymentDataModel(BaseModel):
    month: str
    prepayment_rate: float


class PrepaymentDataResponse(PrepaymentDataModel):
    id: int


@app.get("/prepayment-data", response_model=List[PrepaymentDataResponse])
async def get_prepayment_data():
    db = SessionLocal()
    try:
        data = db.query(PrepaymentData).all()
        return [PrepaymentDataResponse(id=item.id, month=item.month, prepayment_rate=item.prepayment_rate) for item in
                data]
    finally:
        db.close()


@app.post("/prepayment-data", response_model=PrepaymentDataResponse)
async def add_prepayment_data(data: PrepaymentDataModel):
    db = SessionLocal()
    try:
        db_item = PrepaymentData(month=data.month, prepayment_rate=data.prepayment_rate)
        db.add(db_item)
        db.commit()
        db.refresh(db_item)
        return PrepaymentDataResponse(id=db_item.id, month=db_item.month, prepayment_rate=db_item.prepayment_rate)
    finally:
        db.close()


@app.delete("/prepayment-data/{item_id}")
async def delete_prepayment_data(item_id: int):
    db = SessionLocal()
    try:
        item = db.query(PrepaymentData).filter(PrepaymentData.id == item_id).first()
        if item is None:
            raise HTTPException(status_code=404, detail="Item not found")
        db.delete(item)
        db.commit()
        return {"status": "success", "message": f"Item {item_id} deleted"}
    finally:
        db.close()


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
