import concurrent.futures
import io
import json
import logging
import os
import re
import textwrap
from datetime import datetime
from urllib.parse import urlparse

import chromadb
import markdown
import matplotlib
import matplotlib.patches as patches
import matplotlib.pyplot as plt
import pandas as pd
import requests
from bs4 import BeautifulSoup, NavigableString, Tag
from chromadb.config import Settings
from chromadb.utils import embedding_functions
from docx import Document
from docx.enum.text import WD_ALIGN_PARAGRAPH, WD_LINE_SPACING
from docx.oxml import OxmlElement
from docx.oxml.ns import qn
from docx.shared import Cm, Inches, Pt, RGBColor
from ollama import Client
from sqlalchemy import create_engine

from config import (
    DEFAULT_COLOR,
    EMBED_MODEL,
    FINANCE_STRUCTURE,
    FINANCE_SYSTEM_PROMPT,
    LLM_MODEL,
    OLLAMA_HOST,
    PERSONAS,
    SERPER_API_KEY,
    WRITER_FIRM_NAME,
)

matplotlib.use("Agg")
logger = logging.getLogger(__name__)


class KnowledgeBase:
    def __init__(self, db_uri):
        os.makedirs("data", exist_ok=True)
        self.engine = create_engine(db_uri)
        self.chroma = chromadb.Client(Settings(anonymized_telemetry=False))
        self.embed_fn = embedding_functions.OllamaEmbeddingFunction(
            url=f"{OLLAMA_HOST}/api/embeddings",
            model_name=EMBED_MODEL,
        )
        self.collection = self.chroma.get_or_create_collection(
            name="finance_holistic_db",
            embedding_function=self.embed_fn,
        )
        self.df = None
        self.refresh_data()

    def refresh_data(self):
        try:
            self.df = pd.read_sql("SELECT * FROM invoices", self.engine)
        except Exception:
            csv_path = os.path.join("data", "db.csv")
            if not os.path.exists(csv_path):
                logger.error("Gagal memuat data: %s tidak ditemukan.", csv_path)
                return False

            raw_df = pd.read_csv(csv_path)
            raw_df.columns = [column.strip() for column in raw_df.columns]
            raw_df.to_sql("invoices", self.engine, index=False, if_exists="replace")
            self.df = raw_df

        existing_ids = self.collection.get().get("ids", [])
        if existing_ids:
            self.collection.delete(ids=existing_ids)

        ids = []
        documents = []
        metadatas = []

        for index, row in self.df.iterrows():
            text_representation = " | ".join(
                f"{column}: {value}" for column, value in row.items()
            )
            ids.append(str(index))
            documents.append(text_representation)
            metadatas.append(row.astype(str).to_dict())

        if ids:
            try:
                logger.info(
                    "Mengirim %s invoice ke Ollama embedding di %s.",
                    len(ids),
                    OLLAMA_HOST,
                )
                self.collection.add(
                    documents=documents,
                    metadatas=metadatas,
                    ids=ids,
                )
            except Exception as exc:
                logger.error("Gagal sinkronisasi embedding: %s", exc)
                return False

        return True

    def query(self, context_keywords=""):
        query_text = (
            "Historical invoice delays, payment behavior class A-E, "
            "systemic financial risk, collection bottlenecks. "
            f"{context_keywords or ''}"
        )
        max_results = 100
        if self.df is not None and not self.df.empty:
            max_results = min(120, len(self.df))
        collection_size = self.collection.count()
        if collection_size > 0:
            max_results = min(max_results, collection_size)

        try:
            result = self.collection.query(query_texts=[query_text], n_results=max_results)
            documents = result.get("documents", [])
            if documents and documents[0]:
                return "\n---\n".join(documents[0])
        except Exception as exc:
            logger.error("Query error: %s", exc)

        return "Tidak ada data finansial internal yang dapat dipakai."


class Researcher:
    _SERPER_ENDPOINTS = {
        "search": "https://google.serper.dev/search",
        "news": "https://google.serper.dev/news",
    }

    _OSINT_TOPICS = [
        {
            "topic": "Siklus Anggaran Pemerintah",
            "query": "siklus pencairan APBN APBD termin pembayaran vendor Indonesia",
        },
        {
            "topic": "Perilaku Pembayaran BUMN dan Korporasi",
            "query": "tren keterlambatan pembayaran invoice BUMN swasta Indonesia",
        },
        {
            "topic": "Likuiditas dan Piutang Bisnis",
            "query": "risiko likuiditas perusahaan jasa Indonesia karena piutang tertunda",
        },
        {
            "topic": "Regulasi Pengadaan dan Kontrak",
            "query": "regulasi terbaru pengadaan pemerintah termin pembayaran penyedia Indonesia",
        },
    ]

    _cache = {}

    @staticmethod
    def _is_serper_available():
        return bool(
            SERPER_API_KEY
            and SERPER_API_KEY.strip()
            and SERPER_API_KEY != "masukkan_api_key_serper_anda_disini"
        )

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
            lines.append("- Tidak ada sinyal eksternal yang relevan.")
            return "\n".join(lines)

        for index, entry in enumerate(entries[:4], start=1):
            title = entry.get("title") or "Tanpa judul"
            snippet = entry.get("snippet") or "Tidak ada ringkasan."
            source = entry.get("domain") or "-"
            date = f" ({entry['date']})" if entry.get("date") else ""

            lines.append(f"{index}. {title}{date}")
            lines.append(f"   Inti: {snippet}")
            lines.append(f"   Sumber: {source}")

        return "\n".join(lines)

    @classmethod
    def get_macro_finance_trends(cls, extra_context=""):
        if not cls._is_serper_available():
            return "Data OSINT eksternal tidak tersedia (SERPER_API_KEY belum dikonfigurasi)."

        context_snippet = (extra_context or "").strip()
        cache_key = context_snippet.lower()
        if cache_key in cls._cache:
            return cls._cache[cache_key]

        search_jobs = []
        for topic_config in cls._OSINT_TOPICS:
            query = topic_config["query"]
            if context_snippet:
                query = f"{query} {context_snippet[:180]}"

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

        blocks = []
        for topic_config in cls._OSINT_TOPICS:
            topic_name = topic_config["topic"]
            unique_entries = cls._deduplicate(topic_results.get(topic_name, []))
            blocks.append(cls._format_topic(topic_name, unique_entries))

        combined = "\n\n".join(blocks).strip()
        if not combined:
            combined = "Tidak ada data OSINT eksternal yang dapat dipakai."

        cls._cache[cache_key] = combined
        return combined

    @classmethod
    def get_chapter_signal(cls, chapter_keywords, notes=""):
        if not cls._is_serper_available():
            return "Sinyal OSINT per bab tidak tersedia."

        query = (
            "Indonesia payment behavior invoice collection risk "
            f"{chapter_keywords or ''} {notes or ''}"
        ).strip()

        results = cls._execute_serper_query(query, mode="search", num_results=5)
        unique_results = cls._deduplicate(results)
        if not unique_results:
            return "Tidak ada sinyal OSINT spesifik bab yang cukup relevan."

        lines = []
        for index, entry in enumerate(unique_results[:3], start=1):
            title = entry.get("title") or "Tanpa judul"
            snippet = entry.get("snippet") or "Tidak ada ringkasan."
            source = entry.get("domain") or "-"
            lines.append(f"{index}. {title}")
            lines.append(f"   Ringkasan: {snippet}")
            lines.append(f"   Sumber: {source}")

        return "\n".join(lines)


class StyleEngine:
    @staticmethod
    def _insert_field(paragraph, field_code, placeholder="1"):
        begin_run = paragraph.add_run()
        begin_field = OxmlElement("w:fldChar")
        begin_field.set(qn("w:fldCharType"), "begin")
        begin_run._r.append(begin_field)

        instruction_run = paragraph.add_run()
        instruction = OxmlElement("w:instrText")
        instruction.set(qn("xml:space"), "preserve")
        instruction.text = field_code
        instruction_run._r.append(instruction)

        separate_run = paragraph.add_run()
        separate_field = OxmlElement("w:fldChar")
        separate_field.set(qn("w:fldCharType"), "separate")
        separate_run._r.append(separate_field)

        paragraph.add_run(placeholder)

        end_run = paragraph.add_run()
        end_field = OxmlElement("w:fldChar")
        end_field.set(qn("w:fldCharType"), "end")
        end_run._r.append(end_field)

    @classmethod
    def insert_toc_field(cls, paragraph):
        cls._insert_field(
            paragraph,
            'TOC \\o "1-3" \\h \\z \\u',
            "Klik kanan lalu pilih Update Field untuk memuat daftar isi.",
        )

    @classmethod
    def apply_document_styles(cls, doc, theme_color):
        for section in doc.sections:
            section.top_margin = Cm(2.54)
            section.bottom_margin = Cm(2.54)
            section.left_margin = Cm(2.54)
            section.right_margin = Cm(2.54)

            header = section.header
            header_paragraph = (
                header.paragraphs[0] if header.paragraphs else header.add_paragraph()
            )
            header_paragraph.text = ""
            header_paragraph.alignment = WD_ALIGN_PARAGRAPH.LEFT
            header_paragraph.add_run("INIXINDO JOGJA | INTERNAL FINANCE REPORT")
            for run in header_paragraph.runs:
                run.font.name = "Calibri"
                run.font.size = Pt(8)
                run.font.color.rgb = RGBColor(120, 120, 120)

            footer = section.footer
            footer_paragraph = (
                footer.paragraphs[0] if footer.paragraphs else footer.add_paragraph()
            )
            footer_paragraph.text = ""
            footer_paragraph.alignment = WD_ALIGN_PARAGRAPH.RIGHT
            footer_paragraph.add_run("STRICTLY CONFIDENTIAL | Page ")
            cls._insert_field(footer_paragraph, "PAGE")
            footer_paragraph.add_run(" of ")
            cls._insert_field(footer_paragraph, "NUMPAGES")
            for run in footer_paragraph.runs:
                run.font.name = "Calibri"
                run.font.size = Pt(9)
                run.font.color.rgb = RGBColor(110, 110, 110)

        normal_style = doc.styles["Normal"]
        normal_style.font.name = "Calibri"
        normal_style.font.size = Pt(11)
        normal_style.font.color.rgb = RGBColor(33, 37, 41)
        normal_paragraph = normal_style.paragraph_format
        normal_paragraph.alignment = WD_ALIGN_PARAGRAPH.JUSTIFY
        normal_paragraph.line_spacing_rule = WD_LINE_SPACING.MULTIPLE
        normal_paragraph.line_spacing = 1.15
        normal_paragraph.space_after = Pt(8)

        heading_1 = doc.styles["Heading 1"]
        heading_1.font.name = "Calibri"
        heading_1.font.size = Pt(16)
        heading_1.font.bold = True
        heading_1.font.color.rgb = RGBColor(*theme_color)
        heading_1.paragraph_format.space_before = Pt(18)
        heading_1.paragraph_format.space_after = Pt(8)

        heading_2 = doc.styles["Heading 2"]
        heading_2.font.name = "Calibri"
        heading_2.font.size = Pt(13)
        heading_2.font.bold = True
        heading_2.font.color.rgb = RGBColor(0, 0, 0)
        heading_2.paragraph_format.space_before = Pt(14)
        heading_2.paragraph_format.space_after = Pt(4)

        heading_3 = doc.styles["Heading 3"]
        heading_3.font.name = "Calibri"
        heading_3.font.size = Pt(12)
        heading_3.font.bold = True
        heading_3.font.color.rgb = RGBColor(64, 64, 64)
        heading_3.paragraph_format.space_before = Pt(10)
        heading_3.paragraph_format.space_after = Pt(4)

        for style_name in [
            "List Bullet",
            "List Bullet 2",
            "List Bullet 3",
            "List Number",
            "List Number 2",
            "List Number 3",
        ]:
            try:
                list_style = doc.styles[style_name]
                list_style.font.name = "Calibri"
                list_style.font.size = Pt(11)
                list_style.paragraph_format.space_after = Pt(4)
            except KeyError:
                continue

        try:
            caption_style = doc.styles["Caption"]
            caption_style.font.name = "Calibri"
            caption_style.font.size = Pt(10)
            caption_style.font.italic = True
            caption_style.font.color.rgb = RGBColor(80, 80, 80)
        except KeyError:
            pass


class ChartEngine:
    @staticmethod
    def _theme_to_plt_color(theme_color):
        return tuple(component / 255 for component in theme_color)

    @staticmethod
    def create_bar_chart(data_str, theme_color):
        try:
            parts = [part.strip() for part in data_str.split("|")]
            if len(parts) >= 3:
                title = parts[0]
                y_label = parts[1]
                raw_data = "|".join(parts[2:])
            else:
                title = "Distribusi Historis Kelas Pembayaran"
                y_label = "Persentase"
                raw_data = data_str

            labels = []
            values = []
            for chunk in raw_data.split(";"):
                if "," not in chunk:
                    continue
                label, value = chunk.split(",", 1)
                numeric_value = re.sub(r"[^\d.]", "", value)
                if not numeric_value:
                    continue
                labels.append(label.strip())
                values.append(float(numeric_value))

            if not labels:
                return None

            fig, axis = plt.subplots(figsize=(7, 4.5))
            axis.bar(
                labels,
                values,
                color=ChartEngine._theme_to_plt_color(theme_color),
                alpha=0.9,
                width=0.5,
            )
            axis.set_title(title, fontsize=12, fontweight="bold", pad=20)
            axis.set_ylabel(y_label, fontsize=10)
            axis.spines["top"].set_visible(False)
            axis.spines["right"].set_visible(False)

            image_stream = io.BytesIO()
            plt.savefig(image_stream, format="png", bbox_inches="tight", dpi=150)
            plt.close(fig)
            image_stream.seek(0)
            return image_stream
        except Exception:
            return None

    @staticmethod
    def create_flowchart(data_str, theme_color):
        try:
            steps = [
                "\n".join(textwrap.wrap(step.strip(), width=18))
                for step in data_str.split("->")
                if step.strip()
            ]
            if len(steps) < 2:
                return None

            fig, axis = plt.subplots(figsize=(8, 3))
            axis.axis("off")
            x_positions = [index * 2.5 for index in range(len(steps))]

            for index in range(len(steps) - 1):
                axis.annotate(
                    "",
                    xy=(x_positions[index + 1] - 1.0, 0.5),
                    xytext=(x_positions[index] + 1.0, 0.5),
                    arrowprops={"arrowstyle": "-|>", "lw": 1.5},
                )

            for index, step in enumerate(steps):
                box = patches.FancyBboxPatch(
                    (x_positions[index] - 1.0, 0.1),
                    2.0,
                    0.8,
                    boxstyle="round,pad=0.1",
                    fc=ChartEngine._theme_to_plt_color(theme_color),
                    alpha=0.9,
                )
                axis.add_patch(box)
                axis.text(
                    x_positions[index],
                    0.5,
                    step,
                    ha="center",
                    va="center",
                    size=9,
                    color="white",
                    fontweight="bold",
                )

            axis.set_xlim(-1.2, (len(steps) - 1) * 2.5 + 1.2)
            axis.set_ylim(0, 1)

            image_stream = io.BytesIO()
            plt.savefig(
                image_stream,
                format="png",
                bbox_inches="tight",
                dpi=200,
                transparent=True,
            )
            plt.close(fig)
            image_stream.seek(0)
            return image_stream
        except Exception:
            return None


class DocumentBuilder:
    @staticmethod
    def _append_inline_text(paragraph, node, bold=False, italic=False, underline=False, monospace=False):
        if isinstance(node, NavigableString):
            text = str(node)
            if not text:
                return
            run = paragraph.add_run(text)
            run.bold = bold
            run.italic = italic
            run.underline = underline
            if monospace:
                run.font.name = "Consolas"
            return

        if not isinstance(node, Tag):
            return

        next_bold = bold or node.name in {"strong", "b"}
        next_italic = italic or node.name in {"em", "i"}
        next_underline = underline or node.name == "u"
        next_monospace = monospace or node.name == "code"

        if node.name == "br":
            paragraph.add_run("\n")
            return

        if node.name == "a":
            for child in node.children:
                DocumentBuilder._append_inline_text(
                    paragraph,
                    child,
                    bold=next_bold,
                    italic=next_italic,
                    underline=True,
                    monospace=next_monospace,
                )
            return

        for child in node.children:
            DocumentBuilder._append_inline_text(
                paragraph,
                child,
                bold=next_bold,
                italic=next_italic,
                underline=next_underline,
                monospace=next_monospace,
            )

    @staticmethod
    def _resolve_list_style(doc, ordered, level):
        ordered_styles = ["List Number", "List Number 2", "List Number 3"]
        bullet_styles = ["List Bullet", "List Bullet 2", "List Bullet 3"]
        style_names = ordered_styles if ordered else bullet_styles
        preferred_style = style_names[min(level, len(style_names) - 1)]

        try:
            doc.styles[preferred_style]
            return preferred_style
        except KeyError:
            return "List Number" if ordered else "List Bullet"

    @classmethod
    def _add_list(cls, doc, list_tag, level=0, ordered=False):
        style_name = cls._resolve_list_style(doc, ordered, level)

        for list_item in list_tag.find_all("li", recursive=False):
            inline_nodes = []
            nested_lists = []

            for child in list_item.children:
                if isinstance(child, Tag) and child.name in {"ul", "ol"}:
                    nested_lists.append(child)
                else:
                    inline_nodes.append(child)

            if inline_nodes:
                paragraph = doc.add_paragraph(style=style_name)
                paragraph.alignment = WD_ALIGN_PARAGRAPH.LEFT
                for node in inline_nodes:
                    cls._append_inline_text(paragraph, node)

            for nested_list in nested_lists:
                cls._add_list(
                    doc,
                    nested_list,
                    level=level + 1,
                    ordered=nested_list.name == "ol",
                )

    @staticmethod
    def _add_table(doc, table_tag):
        rows = table_tag.find_all("tr")
        if not rows:
            return

        header_cells = rows[0].find_all(["th", "td"])
        if not header_cells:
            return

        column_count = len(header_cells)
        table = doc.add_table(rows=1, cols=column_count)
        table.style = "Table Grid"

        for column_index, cell in enumerate(header_cells):
            paragraph = table.rows[0].cells[column_index].paragraphs[0]
            paragraph.text = cell.get_text(" ", strip=True)
            if paragraph.runs:
                paragraph.runs[0].bold = True

        for html_row in rows[1:]:
            html_cells = html_row.find_all(["th", "td"])
            table_cells = table.add_row().cells
            for column_index in range(column_count):
                value = ""
                if column_index < len(html_cells):
                    value = html_cells[column_index].get_text(" ", strip=True)
                table_cells[column_index].text = value

    @classmethod
    def parse_html_to_docx(cls, doc, html_content, theme_color):
        soup = BeautifulSoup(html_content, "html.parser")

        for element in soup.contents:
            if isinstance(element, NavigableString):
                if not str(element).strip():
                    continue
                paragraph = doc.add_paragraph(str(element).strip())
                paragraph.alignment = WD_ALIGN_PARAGRAPH.JUSTIFY
                continue

            if not isinstance(element, Tag):
                continue

            if element.name in {"h1", "h2", "h3", "h4"}:
                level = min(max(int(element.name[1]), 1), 3)
                heading = doc.add_heading(level=level)
                heading.alignment = WD_ALIGN_PARAGRAPH.LEFT
                cls._append_inline_text(heading, element)
                if level == 1:
                    for run in heading.runs:
                        run.font.color.rgb = RGBColor(*theme_color)
                continue

            if element.name == "p":
                paragraph = doc.add_paragraph()
                paragraph.alignment = WD_ALIGN_PARAGRAPH.JUSTIFY
                for child in element.children:
                    cls._append_inline_text(paragraph, child)
                continue

            if element.name in {"ul", "ol"}:
                cls._add_list(doc, element, level=0, ordered=element.name == "ol")
                continue

            if element.name == "table":
                cls._add_table(doc, element)

    @classmethod
    def _flush_markdown_block(cls, doc, lines, theme_color):
        if not lines:
            return

        markdown_text = "\n".join(lines).strip()
        if not markdown_text:
            return

        html_content = markdown.markdown(markdown_text, extensions=["tables"])
        cls.parse_html_to_docx(doc, html_content, theme_color)

    @staticmethod
    def _add_visual(doc, marker_type, marker_payload, theme_color):
        if marker_type == "CHART":
            image = ChartEngine.create_bar_chart(marker_payload, theme_color)
            width = Inches(5.8)
            caption = "Grafik distribusi historis kelas pembayaran"
        else:
            image = ChartEngine.create_flowchart(marker_payload, theme_color)
            width = Inches(6.3)
            caption = "Diagram alur rekomendasi mitigasi"

        if image is None:
            return

        image_paragraph = doc.add_paragraph()
        image_paragraph.alignment = WD_ALIGN_PARAGRAPH.CENTER
        image_paragraph.add_run().add_picture(image, width=width)

        try:
            caption_paragraph = doc.add_paragraph(caption, style="Caption")
        except KeyError:
            caption_paragraph = doc.add_paragraph(caption)
        caption_paragraph.alignment = WD_ALIGN_PARAGRAPH.CENTER

    @classmethod
    def process_content(cls, doc, raw_text, theme_color=DEFAULT_COLOR):
        markdown_buffer = []

        for raw_line in raw_text.splitlines():
            stripped_line = raw_line.strip()

            if stripped_line.startswith("[[CHART:") and stripped_line.endswith("]]"
            ):
                cls._flush_markdown_block(doc, markdown_buffer, theme_color)
                markdown_buffer = []
                payload = stripped_line.replace("[[CHART:", "", 1).rsplit("]]", 1)[0].strip()
                cls._add_visual(doc, "CHART", payload, theme_color)
                continue

            if stripped_line.startswith("[[FLOW:") and stripped_line.endswith("]]"
            ):
                cls._flush_markdown_block(doc, markdown_buffer, theme_color)
                markdown_buffer = []
                payload = stripped_line.replace("[[FLOW:", "", 1).rsplit("]]", 1)[0].strip()
                cls._add_visual(doc, "FLOW", payload, theme_color)
                continue

            markdown_buffer.append(raw_line.rstrip())

        cls._flush_markdown_block(doc, markdown_buffer, theme_color)

    @staticmethod
    def create_cover(doc, theme_color=DEFAULT_COLOR):
        StyleEngine.apply_document_styles(doc, theme_color)

        properties = doc.core_properties
        properties.title = "Inixindo Historical Revenue Prediction"
        properties.subject = "Internal Financial Intelligence Report"
        properties.author = WRITER_FIRM_NAME
        properties.category = "Finance"

        for _ in range(4):
            doc.add_paragraph()

        confidentiality = doc.add_paragraph("STRICTLY CONFIDENTIAL")
        confidentiality.alignment = WD_ALIGN_PARAGRAPH.CENTER
        confidentiality.runs[0].font.name = "Calibri"
        confidentiality.runs[0].font.size = Pt(10)
        confidentiality.runs[0].font.bold = True
        confidentiality.runs[0].font.color.rgb = RGBColor(120, 120, 120)

        doc.add_paragraph()

        title = doc.add_paragraph("HISTORICAL REVENUE PREDICTION REPORT")
        title.alignment = WD_ALIGN_PARAGRAPH.CENTER
        title.runs[0].font.name = "Calibri"
        title.runs[0].font.size = Pt(22)
        title.runs[0].font.bold = True

        organization = doc.add_paragraph("INIXINDO JOGJA")
        organization.alignment = WD_ALIGN_PARAGRAPH.CENTER
        organization.runs[0].font.name = "Calibri"
        organization.runs[0].font.size = Pt(34)
        organization.runs[0].font.bold = True
        organization.runs[0].font.color.rgb = RGBColor(*theme_color)

        doc.add_paragraph()

        metadata_table = doc.add_table(rows=4, cols=2)
        metadata_table.style = "Table Grid"
        metadata = [
            ("Cakupan Data", "Seluruh histori invoice dan catatan penagihan"),
            ("Tipe Laporan", "Analisis prediksi arus kas dan risiko likuiditas"),
            ("Tanggal Generasi", datetime.now().strftime("%d %B %Y")),
            ("Disusun Oleh", WRITER_FIRM_NAME),
        ]

        for row_index, (label, value) in enumerate(metadata):
            left_cell = metadata_table.rows[row_index].cells[0]
            right_cell = metadata_table.rows[row_index].cells[1]
            left_cell.text = label
            right_cell.text = value
            if left_cell.paragraphs[0].runs:
                left_cell.paragraphs[0].runs[0].bold = True

        doc.add_page_break()

    @staticmethod
    def add_table_of_contents(doc):
        doc.add_heading("Daftar Isi", level=1)
        toc_paragraph = doc.add_paragraph()
        StyleEngine.insert_toc_field(toc_paragraph)

        note = doc.add_paragraph(
            "Catatan: jika daftar isi belum muncul, klik kanan pada area daftar isi lalu pilih Update Field."
        )
        note.alignment = WD_ALIGN_PARAGRAPH.LEFT
        note.runs[0].italic = True
        note.runs[0].font.size = Pt(10)

        doc.add_page_break()


class ReportGenerator:
    def __init__(self, kb_instance):
        self.ollama = Client(host=OLLAMA_HOST)
        self.kb = kb_instance
        self.io_pool = concurrent.futures.ThreadPoolExecutor(max_workers=6)

    @staticmethod
    def _resolve_visual_instruction(chapter):
        visual_intent = chapter.get("visual_intent")
        if visual_intent == "bar_chart":
            return (
                "Mandatory Visual: [[CHART: Distribusi Historis Kelas Pembayaran | Persentase | "
                "Kelas A,30; Kelas B,25; Kelas C,20; Kelas D,15; Kelas E,10]]"
            )
        if visual_intent == "flowchart":
            return (
                "Action Plan Visual: [[FLOW: Analisis Pola Historis -> Pembaruan SOP Penagihan -> "
                "Pengurangan Risiko Gagal Bayar]]"
            )
        return "Do not force visuals."

    def _build_chapter_prompt(self, chapter, notes, macro_osint, chapter_osint):
        rag_data = self.kb.query(f"{chapter.get('keywords', '')} {notes or ''}")
        persona = PERSONAS.get("default", "Chief Financial Officer")

        subsection_headers = "\n".join(
            f"### {subtitle}" for subtitle in chapter.get("subsections", [])
        )

        external_context = (
            f"{macro_osint}\n\n"
            f"=== CHAPTER-SPECIFIC OSINT SIGNALS ===\n"
            f"{chapter_osint}"
        )

        return FINANCE_SYSTEM_PROMPT.format(
            persona=persona,
            industry_trends=external_context,
            rag_data=rag_data,
            visual_prompt=self._resolve_visual_instruction(chapter),
            chapter_title=chapter.get("title", "Bab"),
            sub_chapters=subsection_headers,
        )

    def run(self, notes=""):
        logger.info("Starting historical financial report generation.")

        global_osint_future = self.io_pool.submit(Researcher.get_macro_finance_trends, notes)
        chapter_osint_futures = {
            chapter["id"]: self.io_pool.submit(
                Researcher.get_chapter_signal,
                chapter.get("keywords", ""),
                notes,
            )
            for chapter in FINANCE_STRUCTURE
        }

        try:
            macro_osint = global_osint_future.result(timeout=25)
        except Exception:
            macro_osint = "Tidak ada tren finansial eksternal yang tersedia."

        document = Document()
        DocumentBuilder.create_cover(document, DEFAULT_COLOR)
        DocumentBuilder.add_table_of_contents(document)

        for index, chapter in enumerate(FINANCE_STRUCTURE):
            chapter_id = chapter["id"]

            try:
                chapter_osint = chapter_osint_futures[chapter_id].result(timeout=10)
            except Exception:
                chapter_osint = "Tidak ada sinyal OSINT spesifik bab yang tersedia."

            try:
                prompt = self._build_chapter_prompt(chapter, notes, macro_osint, chapter_osint)
            except Exception as exc:
                logger.error("Gagal menyiapkan konteks untuk %s: %s", chapter["title"], exc)
                continue

            try:
                response = self.ollama.chat(
                    model=LLM_MODEL,
                    messages=[
                        {"role": "system", "content": prompt},
                        {
                            "role": "user",
                            "content": (
                                f"Tulis konten untuk {chapter['title']}. "
                                "Gunakan semua subbab ### yang diwajibkan, "
                                "dan gunakan numbered list/bullet list untuk rekomendasi tindakan."
                            ),
                        },
                    ],
                    options={
                        "num_ctx": 65536,
                        "num_predict": 4096,
                        "temperature": 0.3,
                        "top_p": 0.85,
                        "repeat_penalty": 1.15,
                    },
                )
            except Exception as exc:
                logger.error("Gagal menghasilkan konten untuk %s: %s", chapter["title"], exc)
                continue

            document.add_heading(chapter["title"], level=1)
            DocumentBuilder.process_content(
                document,
                response["message"]["content"],
                DEFAULT_COLOR,
            )

            if index < len(FINANCE_STRUCTURE) - 1:
                document.add_page_break()

        return document, "Inixindo_Historical_Revenue_Prediction"
