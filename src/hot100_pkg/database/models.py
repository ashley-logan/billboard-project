import datetime
from sqlalchemy import ForeignKey
from sqlalchemy.orm import DeclarativeBase, relationship, Mapped, mapped_column
from sqlalchemy.ext.asyncio import AsyncAttrs


class Base(AsyncAttrs, DeclarativeBase):
    pass


class Charts(Base):
    __tablename__ = "charts"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    title: Mapped[str] = mapped_column(nullable=False, unique=False)
    date: Mapped[datetime.date] = mapped_column(nullable=False, unique=True)

    entries: Mapped["Entries"] = relationship(back_populates="charts")


class Entries(Base):
    __tablename__ = "entries"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    position: Mapped[int] = mapped_column(nullable=False, unique=False)
    chart_id: Mapped[int] = mapped_column(ForeignKey("charts.id"), unique=False)
    song_title: Mapped[str] = mapped_column(nullable=False, unique=False)
    artist: Mapped[str] = mapped_column(nullable=False, unique=False)

    charts: Mapped["Charts"] = relationship(back_populates="entries")
