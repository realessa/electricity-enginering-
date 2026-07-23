from pydantic import BaseModel, Field
from datetime import datetime


class ElectricityReading(BaseModel):
    meter_id: str = Field(..., min_length=1)
    building: str = Field(..., min_length=1)
    consumption_kwh: float = Field(..., gt=0)
    timestamp: datetime
