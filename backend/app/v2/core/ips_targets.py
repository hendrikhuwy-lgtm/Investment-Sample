from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable


@dataclass(frozen=True)
class SleeveTargetProfile:
    sleeve_key: str
    sleeve_name: str
    rank: int
    target_pct: float | None
    target_display: str | None
    min_pct: float
    max_pct: float
    sort_midpoint_pct: float
    is_nested: bool
    parent_sleeve_key: str | None
    counts_as_top_level_total: bool
    portfolio_sleeve_id: str

    @property
    def target_label(self) -> str:
        if self.target_display:
            return self.target_display
        if self.target_pct is None:
            return "Target pending"
        return f"{self.target_pct:.1f}%"

    @property
    def range_label(self) -> str:
        return f"{self.min_pct:.1f}% to {self.max_pct:.1f}%"

    @property
    def parent_sleeve_id(self) -> str | None:
        if not self.parent_sleeve_key:
            return None
        return f"sleeve_{self.parent_sleeve_key}"

    @property
    def parent_sleeve_name(self) -> str | None:
        if not self.parent_sleeve_key:
            return None
        parent = IPS_SLEEVE_PROFILE_BY_KEY.get(self.parent_sleeve_key)
        return parent.sleeve_name if parent else self.parent_sleeve_key.replace("_", " ").title()

    @property
    def drift_anchor_pct(self) -> float:
        if self.target_pct is not None:
            return float(self.target_pct)
        return float(self.sort_midpoint_pct)


IPS_SLEEVE_PROFILES: tuple[SleeveTargetProfile, ...] = (
    SleeveTargetProfile(
        sleeve_key="global_equity_core",
        sleeve_name="Global Equity Core",
        rank=1,
        target_pct=50.0,
        target_display=None,
        min_pct=45.0,
        max_pct=55.0,
        sort_midpoint_pct=50.0,
        is_nested=False,
        parent_sleeve_key=None,
        counts_as_top_level_total=True,
        portfolio_sleeve_id="global_equity",
    ),
    SleeveTargetProfile(
        sleeve_key="ig_bonds",
        sleeve_name="IG Bonds",
        rank=2,
        target_pct=20.0,
        target_display=None,
        min_pct=15.0,
        max_pct=25.0,
        sort_midpoint_pct=20.0,
        is_nested=False,
        parent_sleeve_key=None,
        counts_as_top_level_total=True,
        portfolio_sleeve_id="ig_bond",
    ),
    SleeveTargetProfile(
        sleeve_key="cash_bills",
        sleeve_name="Cash and Bills",
        rank=3,
        target_pct=10.0,
        target_display=None,
        min_pct=5.0,
        max_pct=15.0,
        sort_midpoint_pct=10.0,
        is_nested=False,
        parent_sleeve_key=None,
        counts_as_top_level_total=True,
        portfolio_sleeve_id="cash",
    ),
    SleeveTargetProfile(
        sleeve_key="real_assets",
        sleeve_name="Real Assets",
        rank=4,
        target_pct=10.0,
        target_display=None,
        min_pct=5.0,
        max_pct=15.0,
        sort_midpoint_pct=10.0,
        is_nested=False,
        parent_sleeve_key=None,
        counts_as_top_level_total=True,
        portfolio_sleeve_id="real_asset",
    ),
    SleeveTargetProfile(
        sleeve_key="alternatives",
        sleeve_name="Alternatives",
        rank=5,
        target_pct=7.0,
        target_display=None,
        min_pct=4.0,
        max_pct=10.0,
        sort_midpoint_pct=7.0,
        is_nested=False,
        parent_sleeve_key=None,
        counts_as_top_level_total=True,
        portfolio_sleeve_id="alt",
    ),
    SleeveTargetProfile(
        sleeve_key="convex",
        sleeve_name="Convex Protection",
        rank=6,
        target_pct=3.0,
        target_display=None,
        min_pct=2.0,
        max_pct=4.0,
        sort_midpoint_pct=3.0,
        is_nested=False,
        parent_sleeve_key=None,
        counts_as_top_level_total=True,
        portfolio_sleeve_id="convex",
    ),
    SleeveTargetProfile(
        sleeve_key="emerging_markets",
        sleeve_name="Emerging Markets",
        rank=7,
        target_pct=None,
        target_display="5.0% to 10.0%",
        min_pct=3.0,
        max_pct=12.0,
        sort_midpoint_pct=7.5,
        is_nested=True,
        parent_sleeve_key="global_equity_core",
        counts_as_top_level_total=False,
        portfolio_sleeve_id="emerging_markets",
    ),
    SleeveTargetProfile(
        sleeve_key="china_satellite",
        sleeve_name="China Satellite",
        rank=8,
        target_pct=None,
        target_display="2.0% to 4.0%",
        min_pct=0.0,
        max_pct=5.0,
        sort_midpoint_pct=3.0,
        is_nested=True,
        parent_sleeve_key="global_equity_core",
        counts_as_top_level_total=False,
        portfolio_sleeve_id="china_satellite",
    ),
    SleeveTargetProfile(
        sleeve_key="developed_ex_us_optional",
        sleeve_name="Developed ex US Optional Split",
        rank=9,
        target_pct=None,
        target_display="0.0% to 5.0%",
        min_pct=0.0,
        max_pct=10.0,
        sort_midpoint_pct=2.5,
        is_nested=True,
        parent_sleeve_key="global_equity_core",
        counts_as_top_level_total=False,
        portfolio_sleeve_id="developed_ex_us_optional",
    ),
)

IPS_SLEEVE_PROFILE_BY_KEY: dict[str, SleeveTargetProfile] = {
    profile.sleeve_key: profile for profile in IPS_SLEEVE_PROFILES
}
IPS_SLEEVE_PROFILE_BY_PORTFOLIO_ID: dict[str, SleeveTargetProfile] = {
    profile.portfolio_sleeve_id: profile for profile in IPS_SLEEVE_PROFILES
}


def get_ips_sleeve_profile(sleeve_id: str | None) -> SleeveTargetProfile | None:
    normalized = str(sleeve_id or "").strip()
    if not normalized:
        return None
    return IPS_SLEEVE_PROFILE_BY_KEY.get(normalized) or IPS_SLEEVE_PROFILE_BY_PORTFOLIO_ID.get(normalized)


def ordered_ips_sleeves(sleeve_ids: Iterable[str] | None = None) -> list[SleeveTargetProfile]:
    if sleeve_ids is None:
        return list(IPS_SLEEVE_PROFILES)
    seen: set[str] = {str(item or "").strip() for item in sleeve_ids if str(item or "").strip()}
    known = [profile for profile in IPS_SLEEVE_PROFILES if profile.sleeve_key in seen or profile.portfolio_sleeve_id in seen]
    unknown = sorted(seen - {profile.sleeve_key for profile in known} - {profile.portfolio_sleeve_id for profile in known})
    for raw in unknown:
        known.append(
            SleeveTargetProfile(
                sleeve_key=raw,
                sleeve_name=raw.replace("_", " ").title(),
                rank=999,
                target_pct=None,
                target_display=None,
                min_pct=0.0,
                max_pct=0.0,
                sort_midpoint_pct=0.0,
                is_nested=False,
                parent_sleeve_key=None,
                counts_as_top_level_total=True,
                portfolio_sleeve_id=raw,
            )
        )
    return known
