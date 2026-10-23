"""
services/fake-banking-api/models.py
SQLAlchemy ORM models mirroring the banking_core_init.sql schema.
"""
from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import Boolean, Column, DateTime, ForeignKey, Numeric, String, Text
from sqlalchemy.orm import relationship

from database import Base


def _now():
    return datetime.now(timezone.utc)


class Customer(Base):
    __tablename__ = "customers"

    customer_id = Column(String, primary_key=True)
    full_name = Column(Text, nullable=False)
    national_id_hash = Column(Text, nullable=False)
    risk_segment = Column(String, nullable=False, default="STANDARD")
    home_country = Column(String, nullable=False, default="EG")
    phone_number = Column(String, nullable=False)
    phone_changed_at = Column(DateTime(timezone=True))
    created_at = Column(DateTime(timezone=True), nullable=False, default=_now)

    accounts = relationship("Account", back_populates="customer")
    cards = relationship("Card", back_populates="customer")
    devices = relationship("Device", back_populates="customer")


class Account(Base):
    __tablename__ = "accounts"

    account_id = Column(String, primary_key=True)
    customer_id = Column(String, ForeignKey("customers.customer_id"), nullable=False)
    account_type = Column(String, nullable=False, default="CURRENT")
    currency = Column(String, nullable=False, default="EGP")
    balance = Column(Numeric(15, 2), nullable=False, default=0)
    status = Column(String, nullable=False, default="ACTIVE")

    customer = relationship("Customer", back_populates="accounts")
    cards = relationship("Card", back_populates="account")


class Card(Base):
    __tablename__ = "cards"

    card_id = Column(String, primary_key=True)
    customer_id = Column(String, ForeignKey("customers.customer_id"), nullable=False)
    account_id = Column(String, ForeignKey("accounts.account_id"), nullable=False)
    masked_pan = Column(String, nullable=False)
    card_type = Column(String, nullable=False, default="DEBIT")
    status = Column(String, nullable=False, default="ACTIVE")
    issued_at = Column(DateTime(timezone=True), nullable=False, default=_now)

    customer = relationship("Customer", back_populates="cards")
    account = relationship("Account", back_populates="cards")
    transactions = relationship("Transaction", back_populates="card")


class Transaction(Base):
    __tablename__ = "transactions"

    transaction_id = Column(String, primary_key=True)
    card_id = Column(String, ForeignKey("cards.card_id"), nullable=False)
    customer_id = Column(String, ForeignKey("customers.customer_id"), nullable=False)
    merchant_id = Column(String, nullable=False)
    amount = Column(Numeric(12, 2), nullable=False)
    currency = Column(String, nullable=False, default="EGP")
    country = Column(String, nullable=False, default="EG")
    channel = Column(String, nullable=False, default="POS")
    transaction_time = Column(DateTime(timezone=True), nullable=False, default=_now)
    status = Column(String, nullable=False, default="APPROVED")
    fraud_label = Column(Boolean, nullable=False, default=False)

    card = relationship("Card", back_populates="transactions")


class Device(Base):
    __tablename__ = "devices"

    device_id = Column(String, primary_key=True)
    customer_id = Column(String, ForeignKey("customers.customer_id"), nullable=False)
    fingerprint_hash = Column(Text, nullable=False)
    trusted = Column(Boolean, nullable=False, default=False)
    first_seen_at = Column(DateTime(timezone=True), nullable=False, default=_now)
    last_seen_at = Column(DateTime(timezone=True), nullable=False, default=_now)
    country = Column(String, nullable=False, default="EG")

    customer = relationship("Customer", back_populates="devices")


class LoginEvent(Base):
    __tablename__ = "login_events"

    login_id = Column(String, primary_key=True)
    customer_id = Column(String, ForeignKey("customers.customer_id"), nullable=False)
    device_id = Column(String, ForeignKey("devices.device_id"))
    login_time = Column(DateTime(timezone=True), nullable=False, default=_now)
    country = Column(String, nullable=False, default="EG")
    success = Column(Boolean, nullable=False, default=True)
