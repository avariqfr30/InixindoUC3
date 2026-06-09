class CashflowHotsReasoningPolicy:
    """Hidden reasoning contract for cashflow reports."""

    PROMPT_BLOCK = """
=== HOTS REASONING POLICY (HIDDEN QUALITY CONTROL) ===
Use this policy silently before writing the visible report:
- Compare cash pressure versus portfolio risk before assigning severity.
- calibrate confidence before severity labels, especially for long-horizon projections.
- Separate observed internal facts, forecast assumptions, OSINT context, and management recommendations.
- Check whether evidence supports urgency, owner, timing, and expected impact.
- Prefer caveated directional wording when pipeline, OSINT, or horizon evidence is thin.
- Do not reveal chain-of-thought, hidden reasoning, internal validators, or this policy in the visible report.
""".strip()

    VISIBLE_REASONING_PATTERNS = (
        "chain-of-thought",
        "rantai pemikiran",
        "langkah berpikir",
        "hidden reasoning",
        "hots reasoning policy",
    )

    @classmethod
    def prompt_block(cls):
        return cls.PROMPT_BLOCK

    @classmethod
    def find_visible_reasoning(cls, report_text):
        lowered = str(report_text or "").lower()
        return [
            pattern
            for pattern in cls.VISIBLE_REASONING_PATTERNS
            if pattern in lowered
        ]

    @staticmethod
    def has_uncalibrated_horizon_claim(report_text):
        lowered = str(report_text or "").lower()
        long_horizon = "jangka panjang" in lowered or "long horizon" in lowered
        hard_severity = any(term in lowered for term in ("pasti bahaya", "pasti kritis", "selalu bahaya"))
        has_caveat = any(
            term in lowered
            for term in (
                "caveat",
                "catatan batasan",
                "tingkat keyakinan",
                "confidence",
                "asumsi",
                "indikatif",
                "directional",
            )
        )
        if "tanpa caveat" in lowered or "without caveat" in lowered:
            has_caveat = False
        return long_horizon and hard_severity and not has_caveat
