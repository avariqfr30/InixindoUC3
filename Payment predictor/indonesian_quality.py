"""Deterministic, non-mutating Indonesian report checks."""
from __future__ import annotations

import re
import subprocess
from dataclasses import dataclass, field

from rapidfuzz import fuzz


@dataclass(frozen=True)
class IndonesianQualityResult:
    issues: tuple[str, ...] = ()
    warnings: tuple[str, ...] = ()
    metrics: dict[str, float | int] = field(default_factory=dict)

    @property
    def passes(self):
        return not self.issues


class IndonesianQualityGate:
    DEFAULT_ALLOWLIST = {
        "cashflow", "dashboard", "forecast", "invoice", "outstanding", "aging",
        "collection", "controller", "cash", "flow", "markdown", "osint",
        "account", "owner", "timeline", "baseline", "target", "mitigasi",
        "prioritas", "nominal", "likuiditas", "rekonsiliasi", "countercheck",
    }
    MARKER_RE = re.compile(r"\[\[(?:CHART|PIE|FLOW|DASHBOARD|GANTT):.*?\]\]", re.I)
    URL_RE = re.compile(r"https?://\S+|www\.\S+", re.I)
    WORD_RE = re.compile(r"\b[a-zA-ZÀ-ÿ][a-zA-ZÀ-ÿ'-]{2,}\b")

    def __init__(
        self,
        hunspell_binary="hunspell",
        dictionary="id_ID",
        duplicate_threshold=92.0,
        spell_timeout_seconds=3.0,
        domain_allowlist=None,
        runner=None,
    ):
        self.hunspell_binary = hunspell_binary
        self.dictionary = dictionary
        self.duplicate_threshold = float(duplicate_threshold)
        self.spell_timeout_seconds = float(spell_timeout_seconds)
        self.domain_allowlist = {
            str(item).lower() for item in (domain_allowlist or self.DEFAULT_ALLOWLIST)
        }
        self.runner = runner or self._run_hunspell

    @staticmethod
    def _normalized(value):
        return re.sub(r"\s+", " ", str(value or "")).strip().lower()

    def _paragraphs(self, text):
        paragraphs = []
        for block in re.split(r"\n\s*\n", str(text or "")):
            lines = [line.strip() for line in block.splitlines() if line.strip()]
            if not lines or any(line.startswith(("#", "|", "[[")) for line in lines):
                continue
            normalized = self._normalized(" ".join(lines))
            if len(self.WORD_RE.findall(normalized)) >= 8:
                paragraphs.append(normalized)
        return paragraphs

    def _eligible_words(self, text):
        source = self.MARKER_RE.sub(" ", str(text or ""))
        source = self.URL_RE.sub(" ", source)
        source = "\n".join(
            line for line in source.splitlines()
            if not line.lstrip().startswith(("#", "|"))
        )
        words = []
        for match in self.WORD_RE.finditer(source):
            raw = match.group(0)
            lowered = raw.lower()
            if raw.isupper() or lowered in self.domain_allowlist:
                continue
            words.append(lowered)
        return words

    def _run_hunspell(self, words):
        if not words:
            return ()
        completed = subprocess.run(
            [self.hunspell_binary, "-d", self.dictionary, "-l"],
            input="\n".join(words) + "\n",
            text=True,
            capture_output=True,
            timeout=self.spell_timeout_seconds,
            check=False,
        )
        if completed.returncode not in {0, 1}:
            raise RuntimeError(f"hunspell exited with {completed.returncode}")
        return tuple(line.strip().lower() for line in completed.stdout.splitlines() if line.strip())

    def evaluate(self, text):
        issues = set()
        warnings = set()
        metrics: dict[str, float | int] = {}

        paragraphs = self._paragraphs(text)
        duplicate_pairs = 0
        for index, left in enumerate(paragraphs):
            for right in paragraphs[index + 1:]:
                if fuzz.token_set_ratio(left, right) >= self.duplicate_threshold:
                    duplicate_pairs += 1
        if duplicate_pairs:
            issues.add("near_duplicate_paragraph")
        metrics["near_duplicate_pairs"] = duplicate_pairs

        starts = {}
        for sentence in re.split(r"(?<=[.!?])\s+", str(text or "")):
            words = self.WORD_RE.findall(sentence.lower())
            if len(words) < 8:
                continue
            opening = " ".join(words[:4])
            starts[opening] = starts.get(opening, 0) + 1
        repeated_openings = sum(1 for count in starts.values() if count >= 3)
        if repeated_openings:
            issues.add("near_duplicate_opening")
        metrics["repeated_openings"] = repeated_openings

        eligible_words = self._eligible_words(text)
        try:
            unknown = tuple(self.runner(eligible_words))
        except (FileNotFoundError, OSError, RuntimeError, subprocess.SubprocessError):
            unknown = ()
            warnings.add("spellcheck_unavailable")
        unknown_count = len(unknown)
        unknown_ratio = unknown_count / len(eligible_words) if eligible_words else 0.0
        metrics.update({
            "spellcheck_words": len(eligible_words),
            "unknown_words": unknown_count,
            "unknown_ratio": round(unknown_ratio, 4),
        })
        if unknown_count >= 8 and unknown_ratio > 0.12:
            issues.add("indonesian_spelling_density")

        return IndonesianQualityResult(
            issues=tuple(sorted(issues)),
            warnings=tuple(sorted(warnings)),
            metrics=metrics,
        )
