"""Donor interfaces and concrete adapters for Layer 1."""

from app.v2.donors.base import (
    BenchmarkRegistryDonor,
    CandidateRegistryDonor,
    EtfSourceDonor,
    PortfolioStateDonor,
    ProviderSnapshotDonor,
)
from app.v2.donors.blueprint import SQLiteBlueprintDonor, SQLiteEtfDonor
from app.v2.donors.portfolio import SQLitePortfolioDonor
from app.v2.donors.providers import SQLiteProviderDonor

__all__ = [
    "BenchmarkRegistryDonor",
    "CandidateRegistryDonor",
    "EtfSourceDonor",
    "PortfolioStateDonor",
    "ProviderSnapshotDonor",
    "SQLiteBlueprintDonor",
    "SQLiteEtfDonor",
    "SQLitePortfolioDonor",
    "SQLiteProviderDonor",
]

