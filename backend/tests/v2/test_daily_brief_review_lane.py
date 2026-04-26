from __future__ import annotations


def test_review_lane_uses_visible_decision_status_first() -> None:
    from app.v2.surfaces.daily_brief.contract_builder import _review_lane

    assert _review_lane({"decision_status": "triggered", "next_action": "Monitor"}, "review") == "review_now"
    assert _review_lane({"decision_status": "review_now", "next_action": "Review now"}, "review") == "review_now"
    assert _review_lane({"decision_status": "near_trigger", "next_action": "Review now"}, "review") == "monitor"
    assert _review_lane({"decision_status": "do_not_act_yet", "next_action": "Review now"}, "review") == "do_not_act_yet"
