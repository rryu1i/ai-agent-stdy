from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime

class Reviews(BaseModel):
    id: str
    order_id: str
    review_score: int
    review_comment_title: Optional[str]
    review_comment_message: Optional[str]
    review_creation_date: datetime
    review_answer_timestamp: Optional[datetime]

