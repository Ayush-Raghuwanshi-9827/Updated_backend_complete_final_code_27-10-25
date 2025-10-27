# app/models.py

import datetime
from sqlalchemy import Column, Integer, String, DateTime
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class User(Base):
    """
    User model that stores email, username, hashed password, mobile number,
    and the name of the dynamic database created for each user.
    """
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True)
    email = Column(String(255), unique=True, index=True, nullable=False)
    username = Column(String(255), unique=True, index=True, nullable=False)
    hashed_password = Column(String(255), nullable=False)
    dynamic_db = Column(String(255), nullable=False, default="")  # Initially blank
    mobile_number = Column(String(10))  # ✅ Added this field
    created_at = Column(DateTime, default=datetime.datetime.utcnow)


