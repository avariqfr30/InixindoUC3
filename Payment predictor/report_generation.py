import concurrent.futures
import logging

from ollama import Client

from config import (
    DEFAULT_COLOR,
    LLM_MODEL,
    OLLAMA_HOST,
    REPORT_NUM_CTX,
    REPORT_NUM_PREDICT,
    REPORT_REPEAT_PENALTY,
    REPORT_TEMPERATURE,
    REPORT_TOP_P,
)
from osint_research import Researcher
from report_document import ReportDocumentAssembler
from report_finalization import ReportFinalizer
from report_prompting import ReportPromptBuilder
from report_quality import ReportQualityScorer
from report_structure import REPORT_STRUCTURE
from payment_deliberation import PaymentDeliberationBuilder

logger = logging.getLogger(__name__)


class ReportGenerator:
    SECTION_PASSES = REPORT_STRUCTURE.section_passes

    def __init__(self, kb_instance):
        self.ollama = Client(host=OLLAMA_HOST)
        self.kb = kb_instance
        self.io_pool = concurrent.futures.ThreadPoolExecutor(max_workers=4)
        self.structure = REPORT_STRUCTURE
        self.prompt_builder = ReportPromptBuilder(self.structure)
        self.quality_scorer = ReportQualityScorer(self.structure)
        self.finalizer = ReportFinalizer(self.structure)
        self.document_assembler = ReportDocumentAssembler(self.finalizer)

    @staticmethod
    def _normalize_analysis_payload(analysis_payload):
        return ReportFinalizer.normalize_analysis_payload(analysis_payload)

    @staticmethod
    def _build_dashboard_visual_markers(analysis_payload):
        return ReportFinalizer().build_dashboard_visual_markers(analysis_payload)

    @staticmethod
    def _build_operational_snapshot_block(analysis_payload):
        return ReportFinalizer().build_operational_snapshot_block(analysis_payload)

    @staticmethod
    def _build_user_instruction(notes, active_sections):
        return ReportPromptBuilder().build_user_instruction(notes, active_sections)

    @staticmethod
    def _build_section_scope(active_sections):
        return ReportPromptBuilder().build_section_scope(active_sections)

    def _build_report_prompt(self, report_context, notes, analysis_context, macro_osint, active_sections, include_visuals):
        return self.prompt_builder.build_report_prompt(
            report_context,
            notes,
            analysis_context,
            macro_osint,
            active_sections,
            include_visuals,
        )

    @staticmethod
    def _score_report_completeness(raw_text):
        return ReportQualityScorer().score(raw_text)

    @staticmethod
    def _is_acceptable_report(raw_text):
        return ReportQualityScorer().is_acceptable(raw_text)

    @staticmethod
    def _extract_visual_markers(visual_prompt):
        return ReportFinalizer.extract_visual_markers(visual_prompt)

    @staticmethod
    def _split_top_level_sections(raw_text):
        return ReportFinalizer.split_top_level_sections(raw_text)

    @staticmethod
    def _join_top_level_sections(sections):
        return ReportFinalizer.join_top_level_sections(sections)

    @staticmethod
    def _inject_subheading_block(section_body, subheading, content, before_subheading=None):
        return ReportFinalizer.inject_subheading_block(section_body, subheading, content, before_subheading)

    @staticmethod
    def _append_marker_block(section_body, marker):
        return ReportFinalizer.append_marker_block(section_body, marker)

    @classmethod
    def _inject_executive_headlines(cls, section_body, report_context):
        return ReportFinalizer().inject_executive_headlines(section_body, report_context)

    @staticmethod
    def _format_structured_context_block(raw_text):
        return ReportPromptBuilder.format_structured_context_block(raw_text)

    @classmethod
    def _sanitize_generated_report_text(cls, raw_text):
        return ReportFinalizer.sanitize_generated_report_text(raw_text)

    def _finalize_report_content(self, raw_text, report_context, macro_osint, analysis_payload=None):
        return self.finalizer.finalize(raw_text, report_context, macro_osint, analysis_payload=analysis_payload)

    def _build_fallback_report(self, report_context, notes, analysis_context, macro_osint, analysis_payload=None):
        return self.finalizer.build_fallback_report(
            report_context,
            notes,
            analysis_context,
            macro_osint,
            analysis_payload=analysis_payload,
            structured_context_block=self._format_structured_context_block(analysis_context),
        )

    def _run_generation_pass(self, report_context, notes, analysis_context, macro_osint, active_sections, include_visuals, label):
        prompt = self._build_report_prompt(
            report_context,
            notes,
            analysis_context,
            macro_osint,
            active_sections,
            include_visuals,
        )
        user_instruction = self._build_user_instruction(notes, active_sections)

        max_attempts = 2
        num_predict = REPORT_NUM_PREDICT

        for attempt in range(1, max_attempts + 1):
            response = self.ollama.chat(
                model=LLM_MODEL,
                messages=[
                    {"role": "system", "content": prompt},
                    {"role": "user", "content": user_instruction},
                ],
                options={
                    "num_ctx": REPORT_NUM_CTX,
                    "num_predict": num_predict,
                    "temperature": REPORT_TEMPERATURE,
                    "top_p": REPORT_TOP_P,
                    "repeat_penalty": REPORT_REPEAT_PENALTY,
                },
            )

            done_reason = response.get("done_reason")
            content = response["message"]["content"]

            logger.info(
                "Generation pass %s (attempt %s/%s) completed with done_reason=%s, eval_count=%s.",
                label,
                attempt,
                max_attempts,
                done_reason,
                response.get("eval_count"),
            )

            if done_reason == "length" and attempt < max_attempts:
                logger.warning(
                    "Generation pass %s was truncated due to length limits. Retrying with increased token budget.",
                    label
                )
                num_predict = int(num_predict * 1.5)
                continue

            return content

    def _polish_generated_content(self, content, report_context, editor=None):
        if editor is None:
            try:
                from writing_quality import ProtectedIndonesianEditor
                editor = ProtectedIndonesianEditor()
            except Exception:
                return content
        try:
            polished = editor.polish(content)
            if not polished or polished == content:
                return content
            score = self.quality_scorer.score(polished)
            final_qa = self.quality_scorer.final_qa(
                polished,
                rejected_claims=(report_context or {}).get("agent_rejected_claims"),
                deliberation_contract=(report_context or {}).get("document_contract"),
            )
            if score.get("passed") and final_qa.get("passes"):
                return polished
        except Exception:
            pass
        return content

    def run(self, notes="", analysis_context="", analysis_payload=None):
        logger.info("Starting cashflow intelligence report generation.")

        osint_context = "\n".join(part for part in (notes, analysis_context) if str(part or "").strip())
        global_osint_future = self.io_pool.submit(Researcher.get_macro_finance_trends, osint_context)
        report_context = self.kb.get_report_context(notes)

        try:
            macro_osint = global_osint_future.result(timeout=45)
        except Exception:
            macro_osint = "OSINT tidak dipakai karena konteks eksternal yang cukup sebanding tidak tersedia."
        report_context["osint_dossier"] = Researcher.build_osint_dossier(
            macro_osint,
            {
                "focus": notes,
                "financial_summary": report_context.get("financial_summary"),
                "notes": notes,
            },
        )
        deliberation_builder = PaymentDeliberationBuilder()
        document_contract = deliberation_builder.build(
            list(self.structure.section_sequence),
            report_context,
            analysis_payload,
            data_version=str(report_context.get("data_version") or ""),
        )
        report_context["document_contract"] = document_contract

        fallback_used = False
        generated_sections = []
        try:
            for section_pass in self.SECTION_PASSES:
                generated_sections.append(
                    self._run_generation_pass(
                        report_context,
                        notes,
                        analysis_context,
                        macro_osint,
                        section_pass["sections"],
                        section_pass["include_visuals"],
                        section_pass["label"],
                    ).strip()
                )

            generated_content = "\n\n".join(section for section in generated_sections if section).strip()
            generated_content = self._finalize_report_content(
                generated_content,
                report_context,
                macro_osint,
                analysis_payload=analysis_payload,
            )
            completeness_result = self.quality_scorer.score(generated_content)
            logger.info(
                "Report completeness score %.1f/100 before fallback.",
                completeness_result["score"],
            )
        except Exception as exc:
            logger.warning(
                "LLM report generation failed. Falling back to deterministic management draft: %s",
                exc,
            )
            generated_content = ""
            completeness_result = {"passed": False, "score": 0, "missing": ["llm_generation"]}

        if not completeness_result["passed"]:
            logger.warning("Generated report failed quality gate. Falling back to deterministic management draft.")
            fallback_used = True
            generated_content = self._build_fallback_report(
                report_context,
                notes,
                analysis_context,
                macro_osint,
                analysis_payload=analysis_payload,
            )
            generated_content = self._finalize_report_content(
                generated_content,
                report_context,
                macro_osint,
                analysis_payload=analysis_payload,
            )
            completeness_result = self.quality_scorer.score(generated_content)
            logger.info(
                "Report completeness score %.1f/100 after fallback.",
                completeness_result["score"],
            )

        generated_content = self._polish_generated_content(generated_content, report_context)
        completeness_result = self.quality_scorer.score(generated_content)
        final_qa = self.quality_scorer.final_qa(
            generated_content,
            rejected_claims=report_context.get("agent_rejected_claims"),
            deliberation_contract=document_contract,
        )
        if not final_qa["passes"]:
            logger.warning("Final report QA failed before DOCX render: %s", final_qa["findings"])
            if not fallback_used:
                fallback_used = True
                generated_content = self._build_fallback_report(
                    report_context,
                    notes,
                    analysis_context,
                    macro_osint,
                    analysis_payload=analysis_payload,
                )
                generated_content = self._finalize_report_content(
                    generated_content,
                    report_context,
                    macro_osint,
                    analysis_payload=analysis_payload,
                )
                completeness_result = self.quality_scorer.score(generated_content)
                final_qa = self.quality_scorer.final_qa(
                    generated_content,
                    rejected_claims=report_context.get("agent_rejected_claims"),
                    deliberation_contract=document_contract,
                )
            if not final_qa["passes"]:
                raise ValueError("Final report QA failed: " + "; ".join(final_qa["findings"]))

        document = self.document_assembler.assemble(
            generated_content,
            analysis_payload,
            DEFAULT_COLOR,
        )

        dashboard_screenshots_included = self._has_dashboard_screenshots(analysis_payload)
        run_metadata = {
            "fallback_used": fallback_used,
            "quality_gate_passed": completeness_result["passed"],
            "completeness_score": completeness_result["score"],
            "completeness_missing": completeness_result["missing"],
            "final_qa": final_qa,
            "document_deliberation": {
                "cache_key": document_contract.get("cache_key"),
                "accepted_claim_count": len(document_contract.get("claim_ledger") or []),
                "data_gap_count": len(document_contract.get("data_gap_register") or []),
                "appendix_sections": list((document_contract.get("appendix_manifest") or {}).keys()),
            },
            "osint_available": bool(
                macro_osint
                and "tidak tersedia" not in macro_osint.lower()
                and "tidak ada data osint" not in macro_osint.lower()
                and "osint tidak dipakai" not in macro_osint.lower()
                and "tidak ada sinyal eksternal yang cukup sebanding" not in macro_osint.lower()
            ),
            "visuals_included": any(marker in generated_content for marker in ("[[CHART:", "[[FLOW:", "[[DASHBOARD:")) or dashboard_screenshots_included,
            "dashboard_screenshots_included": dashboard_screenshots_included,
            "report_length": len(generated_content),
        }

        return document, "Inixindo_Cashflow_Intelligence_Report", run_metadata

    @staticmethod
    def _has_dashboard_screenshots(analysis_payload):
        return ReportDocumentAssembler().has_dashboard_screenshots(analysis_payload)

    @staticmethod
    def _embed_dashboard_screenshots(document, analysis_payload, theme_color):
        return ReportDocumentAssembler().embed_dashboard_screenshots(document, analysis_payload, theme_color)
