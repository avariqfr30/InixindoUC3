import concurrent.futures
import json
import logging
import re
from pathlib import Path
from urllib.parse import urlparse

import diskcache as dc
import requests
from ollama import Client
from pydantic import BaseModel, Field

from config import DATA_DIR, LLM_MODEL, OLLAMA_HOST, SERPER_API_KEY

logger = logging.getLogger(__name__)

osint_cache_dir = Path(DATA_DIR) / ".osint_cache" if DATA_DIR else Path("./.osint_cache")
osint_cache = dc.Cache(str(osint_cache_dir))

class InsightSchema(BaseModel):
    insight: str = Field(description="The extracted insight in Indonesian. 'NOT_FOUND' if missing.")

class Researcher:
    _SERPER_ENDPOINTS = {
        "search": "https://google.serper.dev/search",
        "news": "https://google.serper.dev/news",
    }

    _OSINT_TOPICS = [
        {
            "topic": "Siklus Anggaran Pemerintah",
            "query": "siklus pencairan APBN APBD termin pembayaran vendor",
        },
        {
            "topic": "Perilaku Pembayaran BUMN dan Korporasi",
            "query": "tren keterlambatan pembayaran invoice BUMN swasta",
        },
        {
            "topic": "Likuiditas dan Piutang Bisnis",
            "query": "risiko likuiditas perusahaan jasa karena piutang tertunda",
        },
        {
            "topic": "Regulasi Pengadaan dan Kontrak",
            "query": "regulasi terbaru pengadaan pemerintah termin pembayaran penyedia",
        },
    ]

    _DELAY_FACTOR_TOPICS = [
        {
            "factor": "Siklus anggaran pemerintah",
            "query": "siklus pencairan APBN APBD keterlambatan pembayaran vendor",
            "delay_days": (10, 30),
            "impact": "Potensi mundur tambahan ketika termin pembayaran bergantung pada pencairan anggaran.",
        },
        {
            "factor": "Approval korporasi dan BUMN",
            "query": "approval internal BUMN korporasi keterlambatan pembayaran invoice",
            "delay_days": (7, 21),
            "impact": "Potensi penambahan hari tunggu karena approval berlapis, BAST, atau verifikasi akhir.",
        },
        {
            "factor": "Likuiditas pelanggan",
            "query": "tekanan likuiditas perusahaan keterlambatan pembayaran invoice jasa",
            "delay_days": (14, 45),
            "impact": "Potensi penundaan tambahan ketika pelanggan sedang menjaga kas atau menahan pengeluaran.",
        },
        {
            "factor": "Regulasi pengadaan dan administrasi kontrak",
            "query": "regulasi pengadaan pemerintah administrasi kontrak termin pembayaran vendor",
            "delay_days": (5, 20),
            "impact": "Potensi penambahan waktu akibat revisi dokumen, termin, atau penyesuaian administrasi kontrak.",
        },
    ]

    _PROFILE_KEYWORD_GROUPS = {
        "government": ("pemerintah", "pemda", "kementerian", "dinas", "instansi", "apbn", "apbd", "pengadaan"),
        "bumn": ("bumn", "bumd", "persero", "holding negara"),
        "corporate": ("korporasi", "swasta", "enterprise", "perusahaan"),
        "training": ("pelatihan", "training", "sertifikasi", "academy", "bootcamp"),
        "consulting": ("konsultan", "consulting", "implementasi", "proyek", "jasa"),
        "payment_ops": ("invoice", "termin", "tagihan", "piutang", "pembayaran", "approval", "bast", "vendor"),
        "liquidity": ("cashflow", "arus kas", "likuiditas", "pencairan", "dana"),
    }

    _STRICT_PROFILE_TAGS = {"government", "bumn", "corporate", "training", "consulting"}
    _SOURCE_AUTHORITY_WEIGHTS = {
        "go.id": 4,
        "lkpp.go.id": 4,
        "kemenkeu.go.id": 4,
        "bi.go.id": 4,
        "ojk.go.id": 4,
        "bps.go.id": 3,
        "kontan.co.id": 2,
        "bisnis.com": 2,
        "katadata.co.id": 2,
        "cnbcindonesia.com": 2,
        "kompas.com": 1,
        "detik.com": 1,
    }
    _OSINT_LOW_VALUE_TERMS = {
        "crypto",
        "saham",
        "forex",
        "harga minyak",
        "sepak bola",
        "hiburan",
        "bitcoin",
    }

    @staticmethod
    def _is_serper_available():
        return bool(
            SERPER_API_KEY
            and SERPER_API_KEY.strip()
            and SERPER_API_KEY != "masukkan_api_key_serper_anda_disini"
        )

    @staticmethod
    def _normalize_osint_fragment(text, max_length=240):
        normalized = re.sub(r"\s+", " ", str(text or "")).strip()
        normalized = normalized.replace("…", " ")
        normalized = re.sub(r"\.{3,}", " ", normalized)
        normalized = re.sub(r"\s+([,.;:!?])", r"\1", normalized)
        normalized = normalized.strip(" \"'`-–—")
        if not normalized:
            return ""
        if len(normalized) <= max_length:
            return normalized

        candidate = normalized[: max_length + 1]
        sentence_breaks = [candidate.rfind(marker) for marker in (".", "!", "?", ";", ":")]
        best_sentence_break = max(sentence_breaks)
        if best_sentence_break >= int(max_length * 0.5):
            candidate = candidate[: best_sentence_break + 1]
        else:
            last_space = candidate.rfind(" ")
            candidate = candidate[: last_space if last_space > 0 else max_length]
        return candidate.strip(" ,;:-")

    @classmethod
    def _extract_profile_tags(cls, text):
        lowered = str(text or "").lower()
        tags = set()
        for tag, keywords in cls._PROFILE_KEYWORD_GROUPS.items():
            if any(keyword in lowered for keyword in keywords):
                tags.add(tag)
        return tags

    @staticmethod
    def _is_low_signal_fragment(entry):
        title = str((entry or {}).get("title") or "")
        snippet = str((entry or {}).get("snippet") or "")
        combined = f"{title} {snippet}"
        lowered = combined.lower()
        if any(term in lowered for term in Researcher._OSINT_LOW_VALUE_TERMS):
            return True
        if combined.count("...") >= 2 or combined.count("…") >= 2:
            return True
        if combined.count('"') % 2 == 1:
            return True
        cleaned = Researcher._normalize_osint_fragment(combined, max_length=120)
        return len(cleaned) < 35

    @classmethod
    def _score_company_comparable_entry(cls, entry, extra_context=""):
        context_tags = cls._extract_profile_tags(extra_context)
        profile_tags = context_tags & cls._STRICT_PROFILE_TAGS
        if not profile_tags:
            return 0

        entry_text = " ".join(
            [
                str(entry.get("title") or ""),
                str(entry.get("snippet") or ""),
                str(entry.get("domain") or ""),
            ]
        )
        entry_tags = cls._extract_profile_tags(entry_text)
        if not (entry_tags & profile_tags):
            return 0
        if not (entry_tags & {"payment_ops", "liquidity"}):
            return 0
        if cls._is_low_signal_fragment(entry):
            return 0

        domain = str(entry.get("domain") or "").lower()
        authority_score = 0
        for domain_fragment, score in cls._SOURCE_AUTHORITY_WEIGHTS.items():
            if domain_fragment in domain:
                authority_score = max(authority_score, score)

        profile_score = len(entry_tags & profile_tags) * 3
        cashflow_score = len(entry_tags & {"payment_ops", "liquidity"}) * 2
        source_score = min(authority_score, 4)
        return profile_score + cashflow_score + source_score

    @classmethod
    def _is_company_comparable_entry(cls, entry, extra_context=""):
        return cls._score_company_comparable_entry(entry, extra_context) >= 5

    @classmethod
    def _filter_company_comparable_entries(cls, entries, extra_context=""):
        scored_entries = []
        for entry in entries:
            score = cls._score_company_comparable_entry(entry, extra_context)
            if score >= 5:
                enriched_entry = dict(entry)
                enriched_entry["relevance_score"] = score
                scored_entries.append(enriched_entry)
        return sorted(
            scored_entries,
            key=lambda item: (item.get("relevance_score", 0), bool(item.get("date"))),
            reverse=True,
        )

    @classmethod
    def _build_entry_summary(cls, entry):
        raw_title = str(entry.get("title") or "")
        raw_snippet = str(entry.get("snippet") or "")
        source = entry.get("domain") or "-"
        date = f" ({entry['date']})" if entry.get("date") else ""

        use_title = raw_title and "..." not in raw_title and "…" not in raw_title
        headline = cls._normalize_osint_fragment(raw_title if use_title else raw_snippet, max_length=120)
        summary = cls._normalize_osint_fragment(raw_snippet or raw_title, max_length=220)

        lines = []
        if headline:
            lines.append(headline)
        if summary and summary != headline:
            lines.append(f"  Ringkasan: {summary}")
        if entry.get("relevance_score"):
            display_score = min(int(entry["relevance_score"]), 12)
            lines.append(f"  Relevansi: {display_score}/12 terhadap profil pembayaran perusahaan.")
        lines.append(f"  Sumber: {source}{date}")
        return "\n".join(lines)

    @classmethod
    def _build_contextual_query(cls, base_query, extra_context=""):
        tags = cls._extract_profile_tags(extra_context)
        query_terms = []
        if {"government", "bumn"} & tags:
            query_terms.extend(["pengadaan", "termin", "BAST", "pencairan"])
        if "training" in tags:
            query_terms.extend(["pelatihan", "sertifikasi", "vendor jasa"])
        if "consulting" in tags:
            query_terms.extend(["konsultan", "proyek jasa"])
        if "corporate" in tags:
            query_terms.extend(["korporasi", "approval invoice"])

        context_snippet = cls._normalize_osint_fragment(extra_context, max_length=120)
        parts = [base_query, "Indonesia", *query_terms]
        if context_snippet:
            parts.append(context_snippet)
        return " ".join(part for part in parts if part).strip()

    @staticmethod
    def fetch_full_markdown(url):
        """Fetches the clean markdown text of any URL using Jina Reader."""
        if not url: return ""
        try:
            jina_url = f"https://r.jina.ai/{url}"
            headers = {'User-Agent': 'Mozilla/5.0'}
            response = requests.get(jina_url, headers=headers, timeout=12)
            if response.status_code == 200:
                return response.text[:6000]
            return ""
        except Exception as e:
            logger.warning("Failed to fetch full markdown for %s: %s", url, e)
            return ""

    @classmethod
    def extract_insight_with_llm(cls, url, extraction_goal):
        """Universal Deep Scraper: Reads a URL and extracts a specific qualitative insight via Pydantic/LLM."""
        markdown_text = cls.fetch_full_markdown(url)
        if not markdown_text:
            return ""

        prompt = f"""
        You are an expert business researcher. Read the following source text.
        Your goal is to extract: {extraction_goal}

        SOURCE TEXT:
        {markdown_text}

        Respond ONLY with a valid JSON object using this schema. If the information is not present, use "NOT_FOUND".
        {{
            "insight": "<concise professional summary in Indonesian>"
        }}
        """
        try:
            client = Client(host=OLLAMA_HOST)
            res = client.chat(
                model=LLM_MODEL,
                messages=[{'role': 'user', 'content': prompt}],
                options={'temperature': 0.0}
            )
            raw_text = res['message']['content']
            match = re.search(r'\{.*\}', raw_text, re.DOTALL)
            parsed_dict = json.loads(match.group(0)) if match else json.loads(raw_text)

            # Pydantic validation
            data = InsightSchema.model_validate(parsed_dict)

            if "NOT_FOUND" in data.insight.upper() or not data.insight:
                return ""
            return data.insight
        except Exception as e:
            logger.warning("Insight extraction failed for %s: %s", url, e)
            return ""

    @classmethod
    def _execute_serper_query(cls, query, mode="search", num_results=6):
        if not cls._is_serper_available():
            return []

        endpoint = cls._SERPER_ENDPOINTS.get(mode, cls._SERPER_ENDPOINTS["search"])
        headers = {
            "X-API-KEY": SERPER_API_KEY,
            "Content-Type": "application/json",
        }
        payload = {
            "q": query,
            "num": num_results,
            "gl": "id",
            "hl": "id",
        }

        try:
            response = requests.post(endpoint, headers=headers, data=json.dumps(payload), timeout=10)
            response.raise_for_status()
            body = response.json()
        except Exception as exc:
            logger.warning("Serper %s request failed: %s", mode, exc)
            return []

        result_key = "organic" if mode == "search" else "news"
        rows = body.get(result_key, [])

        normalized = []
        for row in rows:
            title = (row.get("title") or "").strip()
            snippet = (row.get("snippet") or "").strip()
            link = (row.get("link") or "").strip()
            date = (row.get("date") or "").strip()

            if not title and not snippet:
                continue

            domain = urlparse(link).netloc.replace("www.", "") if link else "-"
            normalized.append(
                {
                    "title": title,
                    "snippet": snippet,
                    "link": link,
                    "domain": domain,
                    "date": date,
                }
            )

        return normalized

    @staticmethod
    def _deduplicate(items):
        deduplicated = []
        seen = set()

        for item in items:
            fingerprint = "|".join(
                [
                    item.get("domain", "").lower(),
                    item.get("title", "").lower(),
                    item.get("snippet", "")[:120].lower(),
                ]
            )
            if fingerprint in seen:
                continue
            seen.add(fingerprint)
            deduplicated.append(item)

        return deduplicated

    @staticmethod
    def _format_topic(topic, entries):
        lines = [f"[{topic}]"]

        if not entries:
            lines.append("- Tidak ada sinyal eksternal yang cukup sebanding dengan profil perusahaan saat ini.")
            return "\n".join(lines)

        for index, entry in enumerate(entries[:3], start=1):
            lines.append(f"{index}.")
            lines.append(Researcher._build_entry_summary(entry))

        return "\n".join(lines)

    @classmethod
    @osint_cache.memoize(expire=86400)
    def get_macro_finance_trends(cls, extra_context=""):
        if not cls._is_serper_available():
            return "Data OSINT eksternal tidak tersedia (SERPER_API_KEY belum dikonfigurasi)."

        context_snippet = (extra_context or "").strip()
        if not (cls._extract_profile_tags(context_snippet) & cls._STRICT_PROFILE_TAGS):
            return "OSINT tidak dipakai karena konteks perusahaan yang sebanding belum cukup jelas."
        search_jobs = []
        for topic_config in cls._OSINT_TOPICS:
            query = cls._build_contextual_query(topic_config["query"], context_snippet)
            search_jobs.append((topic_config["topic"], query, "search"))
            search_jobs.append((topic_config["topic"], query, "news"))

        topic_results = {topic["topic"]: [] for topic in cls._OSINT_TOPICS}

        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
            future_map = {
                executor.submit(cls._execute_serper_query, query, mode, 6): topic
                for topic, query, mode in search_jobs
            }
            for future, topic in future_map.items():
                try:
                    topic_results[topic].extend(future.result())
                except Exception as exc:
                    logger.warning("OSINT future failed for %s: %s", topic, exc)

        # --- DEEP SCRAPE THE #1 RESULT ---
        deep_insight_text = ""
        top_link = None
        for topic, entries in topic_results.items():
            comparable_entries = cls._filter_company_comparable_entries(entries, extra_context=context_snippet)
            if comparable_entries and comparable_entries[0].get("link"):
                top_link = comparable_entries[0]["link"]
                break

        if top_link:
            logger.info("Deep scraping OSINT for macro finance trends: %s", top_link)
            goal = "What are the latest macro trends regarding B2B payment behavior, budget cycles, or invoice collection challenges in Indonesia?"
            insight = cls.extract_insight_with_llm(top_link, goal)
            if insight:
                source_domain = urlparse(top_link).netloc.replace("www.", "")
                deep_insight_text = f"**Insight Mendalam (via {source_domain}):** {insight}\n\n"

        blocks = []
        for topic_config in cls._OSINT_TOPICS:
            topic_name = topic_config["topic"]
            unique_entries = cls._filter_company_comparable_entries(
                cls._deduplicate(topic_results.get(topic_name, [])),
                extra_context=context_snippet,
            )
            if unique_entries:
                blocks.append(cls._format_topic(topic_name, unique_entries))

        combined = "\n\n".join(blocks).strip()
        if not blocks:
            combined = "OSINT tidak dipakai karena tidak ada sinyal eksternal yang cukup sebanding dengan kondisi perusahaan."
        else:
            combined = (
                "Batas pakai OSINT: sinyal eksternal di bawah hanya dipakai sebagai pembanding untuk profil "
                "pembayaran, bukan sebagai fakta utama cashflow internal.\n\n"
                f"{combined}"
            )

        return deep_insight_text + combined

    @classmethod
    @osint_cache.memoize(expire=86400)
    def get_chapter_signal(cls, chapter_keywords, notes=""):
        if not cls._is_serper_available():
            return "Sinyal OSINT per bab tidak tersedia."

        query = (
            "payment behavior invoice collection risk Indonesia "
            f"{chapter_keywords or ''} {notes or ''}"
        ).strip()
        query = cls._build_contextual_query(query, f"{chapter_keywords or ''} {notes or ''}")

        results = cls._execute_serper_query(query, mode="search", num_results=5)
        unique_results = cls._filter_company_comparable_entries(
            cls._deduplicate(results),
            extra_context=f"{chapter_keywords or ''} {notes or ''}",
        )
        if not unique_results:
            return "OSINT bab ini tidak dipakai karena belum ada sinyal eksternal yang cukup sebanding."

        lines = []
        for index, entry in enumerate(unique_results[:3], start=1):
            lines.append(f"{index}.")
            lines.append(cls._build_entry_summary(entry))

        return "\n".join(lines)

    @classmethod
    @osint_cache.memoize(expire=86400)
    def get_payment_delay_risks(cls, extra_context=""):
        if not cls._is_serper_available():
            return []

        context_snippet = (extra_context or "").strip()
        if not (cls._extract_profile_tags(context_snippet) & cls._STRICT_PROFILE_TAGS):
            return []
        factors = []

        for topic in cls._DELAY_FACTOR_TOPICS:
            query = cls._build_contextual_query(topic["query"], context_snippet)

            search_results = cls._execute_serper_query(query, mode="search", num_results=4)
            news_results = cls._execute_serper_query(query, mode="news", num_results=4)
            combined = cls._filter_company_comparable_entries(
                cls._deduplicate(search_results + news_results),
                extra_context=context_snippet,
            )
            if not combined:
                continue

            sources = []
            snippets = []
            for item in combined[:2]:
                source = item.get("domain") or "-"
                if source not in sources:
                    sources.append(source)
                snippet = cls._normalize_osint_fragment(item.get("snippet") or item.get("title"), max_length=180)
                if snippet:
                    snippets.append(snippet)

            factors.append(
                {
                    "factor": topic["factor"],
                    "potential_delay_days": {
                        "min": topic["delay_days"][0],
                        "max": topic["delay_days"][1],
                    },
                    "impact": topic["impact"],
                    "summary": " ".join(snippets[:2]).strip(),
                    "source_domains": sources,
                }
            )

        return factors
