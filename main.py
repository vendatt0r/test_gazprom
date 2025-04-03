from fastapi import FastAPI, HTTPException, Depends
from pydantic import BaseModel
from sqlalchemy import create_engine, Column, Integer, Float, String, DateTime, ForeignKey
from sqlalchemy.orm import sessionmaker, declarative_base, Session, relationship
from datetime import datetime
import statistics

DATABASE_URL = "sqlite:///./test.db"
engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True, index=True)
    username = Column(String, unique=True, nullable=False)
    devices = relationship("Device", back_populates="owner")

class Device(Base):
    __tablename__ = "devices"
    id = Column(Integer, primary_key=True, index=True)
    device_id = Column(String, unique=True, nullable=False)
    owner_id = Column(Integer, ForeignKey("users.id"))
    owner = relationship("User", back_populates="devices")
    data = relationship("DeviceData", back_populates="device")

class DeviceData(Base):
    __tablename__ = "device_data"
    id = Column(Integer, primary_key=True, index=True)
    device_id = Column(String, ForeignKey("devices.device_id"), index=True)
    timestamp = Column(DateTime, default=datetime.utcnow)
    x = Column(Float, nullable=False)
    y = Column(Float, nullable=False)
    z = Column(Float, nullable=False)
    device = relationship("Device", back_populates="data")

Base.metadata.create_all(bind=engine)
app = FastAPI()

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

class UserCreate(BaseModel):
    username: str

class DeviceCreate(BaseModel):
    username: str
    device_id: str

class DeviceDataInput(BaseModel):
    device_id: str
    x: float
    y: float
    z: float

@app.post("/users/")
def create_user(user: UserCreate, db: Session = Depends(get_db)):
    if db.query(User).filter(User.username == user.username).first():
        raise HTTPException(status_code=400, detail="Username already exists")
    new_user = User(username=user.username)
    db.add(new_user)
    db.commit()
    db.refresh(new_user)
    return {"message": "User registered successfully", "user_id": new_user.id}

@app.post("/devices/")
def register_device(device: DeviceCreate, db: Session = Depends(get_db)):
    user = db.query(User).filter(User.username == device.username).first()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    if db.query(Device).filter(Device.device_id == device.device_id).first():
        raise HTTPException(status_code=400, detail="Device ID already registered")
    new_device = Device(device_id=device.device_id, owner=user)
    db.add(new_device)
    db.commit()
    db.refresh(new_device)
    return {"message": "Device registered successfully", "device_id": new_device.device_id}

@app.post("/data/")
def receive_data(data: DeviceDataInput, db: Session = Depends(get_db)):
    device = db.query(Device).filter(Device.device_id == data.device_id).first()
    if not device:
        raise HTTPException(status_code=404, detail="Device not registered")
    device_data = DeviceData(**data.dict())
    db.add(device_data)
    db.commit()
    db.refresh(device_data)
    return {"message": "Data stored successfully"}

@app.get("/stats/{device_id}")
def get_stats(device_id: str, start: datetime = None, end: datetime = None, db: Session = Depends(get_db)):
    query = db.query(DeviceData).filter(DeviceData.device_id == device_id)
    if start:
        query = query.filter(DeviceData.timestamp >= start)
    if end:
        query = query.filter(DeviceData.timestamp <= end)
    data = query.all()
    if not data:
        raise HTTPException(status_code=404, detail="No data found")
    def compute_stats(values):
        return {"min": min(values), "max": max(values), "count": len(values), "sum": sum(values), "median": statistics.median(values)}
    return {"x": compute_stats([d.x for d in data]), "y": compute_stats([d.y for d in data]), "z": compute_stats([d.z for d in data])}

@app.get("/user_stats/{username}")
def get_user_stats(username: str, db: Session = Depends(get_db)):
    user = db.query(User).filter(User.username == username).first()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    devices = db.query(Device).filter(Device.owner_id == user.id).all()
    if not devices:
        raise HTTPException(status_code=404, detail="No devices found for this user")
    all_data = db.query(DeviceData).filter(DeviceData.device_id.in_([d.device_id for d in devices])).all()
    if not all_data:
        raise HTTPException(status_code=404, detail="No data found for user's devices")
    def compute_stats(values):
        return {"min": min(values), "max": max(values), "count": len(values), "sum": sum(values), "median": statistics.median(values)}
    aggregated_stats = {
        "x": compute_stats([d.x for d in all_data]),
        "y": compute_stats([d.y for d in all_data]),
        "z": compute_stats([d.z for d in all_data])
    }
    per_device_stats = {}
    for device in devices:
        device_data = [d for d in all_data if d.device_id == device.device_id]
        per_device_stats[device.device_id] = {
            "x": compute_stats([d.x for d in device_data]),
            "y": compute_stats([d.y for d in device_data]),
            "z": compute_stats([d.z for d in device_data])
        }
    return {"aggregated": aggregated_stats, "per_device": per_device_stats}
