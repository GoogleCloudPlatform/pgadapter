from sqlalchemy import MetaData, Table, Column, Integer, String, ForeignKey


metadata_obj = MetaData()

user_table = Table(
  "user_account",
  metadata_obj,
  Column("id", Integer, primary_key=True),
  Column("name", String(30)),
  Column("fullname", String),
)
address_table = Table(
  "address",
  metadata_obj,
  Column("id", Integer, primary_key=True),
  Column("user_id", ForeignKey("user_account.id"), nullable=False),
  Column("email_address", String, nullable=False),
)
