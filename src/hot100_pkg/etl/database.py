from sqlalchemy import Column, Integer, String, DateTime, ForeignKey
from sqlalchemy.orm import DeclarativeBase, relationship


class Base(DeclarativeBase):
    pass


class Charts(Base):
    __tablename__ = "charts"

    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False, unique=False)
    date = Column(DateTime, nullable=False, unique=True)

    entries = relationship("Entries", back_populates="charts")


class Entries(Base):
    __tablename__ = "entries"

    position = Column(Integer, primary_key=True, autoincrement=False)
    chart_id = Column(Integer, ForeignKey("charts.id"))
    title = Column(String, nullable=False, unique=True)
    artist = Column(String, nullable=False, unique=False)

    charts = relationship("Charts", back_populates="entries")
