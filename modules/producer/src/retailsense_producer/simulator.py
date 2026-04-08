"""
Sensor Event Simulator.
Generates realistic retail shopper events.
"""

import random
import uuid
from datetime import datetime, timezone
from typing import List

from pydantic import BaseModel, Field


class AgeRange(str):
    """Age range categories."""

    UNDER_18 = "under_18"
    AGE_18_25 = "18-25"
    AGE_26_35 = "26-35"
    AGE_36_50 = "36-50"
    OVER_50 = "over_50"


class Gender(str):
    """Gender categories."""

    MALE = "male"
    FEMALE = "female"
    UNKNOWN = "unknown"


class SensorEvent(BaseModel):
    """
    An anonymized shopper event from an edge sensor.

    IMPORTANT: This schema enforces privacy-by-design.
    NO PII fields (face_id, image_url, name, etc.) are allowed.
    """

    event_id: str = Field(..., description="Unique event identifier (UUID)")
    timestamp: int = Field(..., description="Unix timestamp in milliseconds")
    store_id: str = Field(..., description="Store identifier")
    zone_id: str = Field(..., description="Zone within store")
    sensor_id: str = Field(..., description="Sensor/device identifier")
    age_range: str = Field(..., description="Estimated age range")
    gender: str = Field(..., description="Estimated gender")
    dwell_time_seconds: float = Field(
        ..., ge=0, le=3600, description="Time spent in zone"
    )

    class Config:
        use_enum_values = True


class EventSimulator:
    """Generates realistic retail sensor events."""

    def __init__(self):
        # Store and zone configuration
        self.stores = ["ZRH_01", "BER_01", "MAD_01"]
        self.zones = {
            "ZRH_01": ["ZONE_A", "ZONE_B", "ZONE_C", "ZONE_D"],
            "BER_01": ["ZONE_X", "ZONE_Y", "ZONE_Z"],
            "MAD_01": ["ZONE_1", "ZONE_2", "ZONE_3", "ZONE_4", "ZONE_5"],
        }

        # Demographics distribution (realistic weights)
        self.age_ranges = [
            (AgeRange.UNDER_18, 0.10),
            (AgeRange.AGE_18_25, 0.20),
            (AgeRange.AGE_26_35, 0.25),
            (AgeRange.AGE_36_50, 0.30),
            (AgeRange.OVER_50, 0.15),
        ]

        self.genders = [
            (Gender.MALE, 0.48),
            (Gender.FEMALE, 0.48),
            (Gender.UNKNOWN, 0.04),
        ]

    def generate_event(self) -> SensorEvent:
        """Generate a single realistic sensor event."""
        # Select store and zone
        store_id = random.choice(self.stores)
        zone_id = random.choice(self.zones[store_id])

        # Generate realistic dwell time (exponential distribution, avg ~10 seconds)
        # Most visits are short, some are long
        dwell_time = max(0.5, random.expovariate(1 / 10))

        # Select demographics based on weighted distribution
        age_range = random.choices(
            [item[0] for item in self.age_ranges],
            weights=[item[1] for item in self.age_ranges],
        )[0]

        gender = random.choices(
            [item[0] for item in self.genders],
            weights=[item[1] for item in self.genders],
        )[0]

        # Generate sensor ID (simulates different devices)
        sensor_id = f"SENSOR_{random.randint(1000, 9999)}"

        return SensorEvent(
            event_id=str(uuid.uuid4()),
            timestamp=int(datetime.now(timezone.utc).timestamp() * 1000),
            store_id=store_id,
            zone_id=zone_id,
            sensor_id=sensor_id,
            age_range=age_range,
            gender=gender,
            dwell_time_seconds=round(dwell_time, 2),
        )

    def generate_batch(self, count: int) -> List[SensorEvent]:
        """Generate a batch of events."""
        return [self.generate_event() for _ in range(count)]
