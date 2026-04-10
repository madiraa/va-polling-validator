"""Data models for VA Polling Validator."""

from datetime import datetime
from enum import Enum
from typing import Optional
from pydantic import BaseModel, Field


class MatchStatus(str, Enum):
    """Status of polling place validation."""
    MATCH = "match"
    MISMATCH = "mismatch"
    NOT_FOUND = "not_found"
    ERROR = "error"
    PENDING = "pending"


class VoterRecord(BaseModel):
    """A voter record from the input CSV."""
    row_index: int
    precinct_code: Optional[str] = None
    reg_address_full: Optional[str] = None
    reg_address: str = ""
    reg_city: str = ""
    reg_state: str = "VA"
    reg_zip: str = ""
    polling_place_name: str
    polling_place_address: Optional[str] = None
    
    @property
    def full_address(self) -> str:
        """Construct full address for lookup."""
        if self.reg_address_full:
            return self.reg_address_full.strip()
        return f"{self.reg_address}, {self.reg_city}, {self.reg_state} {self.reg_zip}"


class ValidationResult(BaseModel):
    """Result of validating a single voter record."""
    row_index: int
    input_address: str
    input_polling_place: str
    input_polling_address: Optional[str] = None
    
    va_polling_place: Optional[str] = None
    va_polling_address: Optional[str] = None
    
    status: MatchStatus = MatchStatus.PENDING
    match_score: float = 0.0
    address_match_score: float = 0.0
    
    validation_timestamp: datetime = Field(default_factory=datetime.now)
    error_message: Optional[str] = None
    notes: Optional[str] = None
    
    @property
    def matches_va(self) -> int:
        """Return 1 for match, 0 for mismatch, -1 for not found/error."""
        if self.status == MatchStatus.MATCH:
            return 1
        elif self.status == MatchStatus.MISMATCH:
            return 0
        return -1


class ValidationProgress(BaseModel):
    """Track progress of validation job."""
    job_id: str
    total_records: int
    completed_records: int = 0
    matched: int = 0
    mismatched: int = 0
    not_found: int = 0
    errors: int = 0
    start_time: datetime = Field(default_factory=datetime.now)
    last_updated: datetime = Field(default_factory=datetime.now)
    
    @property
    def progress_pct(self) -> float:
        if self.total_records == 0:
            return 0.0
        return (self.completed_records / self.total_records) * 100
    
    @property
    def is_complete(self) -> bool:
        return self.completed_records >= self.total_records


class ValidatorConfig(BaseModel):
    """Configuration for the validator."""
    match_threshold: float = Field(default=85.0, description="Minimum fuzzy match score to consider a match")
    request_delay: float = Field(default=2.0, description="Delay between requests in seconds")
    max_retries: int = Field(default=3, description="Max retries for failed lookups")
    timeout: float = Field(default=30.0, description="Timeout for each lookup in seconds")
    headless: bool = Field(default=True, description="Run browser in headless mode")
    checkpoint_interval: int = Field(default=10, description="Save checkpoint every N records")
