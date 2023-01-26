from __future__ import annotations

from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base, registry, relationship, sessionmaker
from sqlalchemy import Column, Integer, String, ForeignKey


sqlite_file = r"C:\Users\thuc-nhat-truong.huy\Desktop\DataOpsEngineerCaseStudy\personDB.db"
sqlite_url = f"sqlite:///{sqlite_file}"
engine = create_engine(sqlite_url, echo=False)


mapper_registry = registry()
Base = mapper_registry.generate_base()
SessionLocal = sessionmaker(bind=engine)

class Education(Base):
    __tablename__ = 'education'

    educational_num = Column(Integer, primary_key=True, nullable=False)
    name = Column(String(256), nullable=False)

    per = relationship("Person") # relationship with person table (1-*)

class Person(Base):
    __tablename__ = 'person'

    id = Column(Integer, primary_key=True, autoincrement=True, nullable=False)
    age = Column(Integer, nullable=False)
    workclass = Column(String(256), nullable=False)
    fnlwgt = Column(Integer, nullable=False)
    educational_number = Column(Integer, ForeignKey("education.educational_num"), nullable=False)
    occupation = Column(String(256), nullable=False)
    marital_status = Column(String(256), nullable=False)
    relation_ship = Column(String(256), nullable=False)
    race = Column(String(256), nullable=False)
    gender = Column(String(256), nullable=False)
    capital_gain = Column(Integer, nullable=False)
    capital_loss = Column(Integer, nullable=False)
    hours_per_week = Column(Integer, nullable=False)
    native_country = Column(Integer, nullable=False)
    income = Column(String(256), nullable=False)

    edu = relationship("Education", back_populates='per')


if __name__ == "__main__":

    Base.metadata.create_all(engine, checkfirst=True)