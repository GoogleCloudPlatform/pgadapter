from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.orm import registry
from sqlalchemy.orm import relationship


mapper_registry = registry()
Base = mapper_registry.generate_base()


class User(Base):
  __tablename__ = "user_account"

  id = Column(Integer, primary_key=True)
  name = Column(String(30))
  fullname = Column(String)

  addresses = relationship("Address", back_populates="user")

  def __repr__(self):
    return f"User(id={self.id!r}, name={self.name!r}, fullname={self.fullname!r})"


class Address(Base):
  __tablename__ = "address"

  id = Column(Integer, primary_key=True)
  email_address = Column(String, nullable=False)
  user_id = Column(Integer, ForeignKey("user_account.id"))

  user = relationship("User", back_populates="addresses")

  def __repr__(self):
    return f"Address(id={self.id!r}, email_address={self.email_address!r})"
