import os
import io
import re
import json
import logging
import requests
import pandas as pd
import chromadb
from chromadb.config import Settings
import concurrent.futures
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.patches as patches
import textwrap
from datetime import datetime
from PIL import Image, ImageStat
from sqlalchemy import create_engine
import markdown
from bs4 import BeautifulSoup
from docx import Document
from docx.shared import Pt, RGBColor, Inches, Cm
from docx.enum.text import WD_ALIGN_PARAGRAPH, WD_LINE_SPACING
from docx.oxml.ns import qn
from docx.oxml import OxmlElement
from ollama import Client 
from chromadb.utils import embedding_functions

from config import (
    GOOGLE_API_KEY, GOOGLE_CX_ID, OLLAMA_HOST, LLM_MODEL, EMBED_MODEL, DB_URI,
    WRITER_FIRM_NAME, DEFAULT_COLOR, FINANCE_STRUCTURE, 
    PERSONAS, FINANCE_SYSTEM_PROMPT, DEMO_MODE
)

logger = logging.getLogger(__name__)

class KnowledgeBase:
    def __init__(self, db_uri):
        os.makedirs("data", exist_ok=True)
        self.engine = create_engine(db_uri)
        self.chroma = chromadb.Client(Settings(anonymized_telemetry=False))
        self.embed_fn = embedding_functions.OllamaEmbeddingFunction(
            url=f"{OLLAMA_HOST}/api/embeddings", model_name=EMBED_MODEL
        )
        self.collection = self.chroma.get_or_create_collection(
            name="finance_holistic_db", embedding_function=self.embed_fn
        )
        self.df = None
        self.refresh_data()

    def refresh_data(self):
        try: 
            self.df = pd.read_sql("SELECT * FROM invoices", self.engine)
        except Exception:
            csv_path = os.path.join("data", "db.csv")
            if os.path.exists(csv_path):
                raw_df = pd.read_csv(csv_path)
                raw_df.columns = [c.strip() for c in raw_df.columns]
                raw_df.to_sql("invoices", self.engine, index=False, if_exists='replace')
                self.df = raw_df
            else: 
                logger.error(f"Gagal memuat DB: File {csv_path} tidak ditemukan!")
                return False
            
        existing = self.collection.get()['ids']
        if existing: self.collection.delete(existing)
        
        ids, docs, metas = [], [], []
        for idx, row in self.df.iterrows():
            text_rep = " | ".join([f"{col}: {val}" for col, val in row.items()])
            ids.append(str(idx))
            docs.append(text_rep)
            metas.append(row.astype(str).to_dict())
            
        if ids:
            try:
                logger.info(f"Mengirim {len(ids)} data invoice ke Ollama ({OLLAMA_HOST}) untuk embedding...")
                self.collection.add(documents=docs, metadatas=metas, ids=ids)
            except Exception as e:
                logger.error(f"Gagal terhubung ke Ollama Embedding: {e}")
                return False
                
        return True

    def query(self, context_keywords=None):
        try:
            # Query is now completely unrestricted by timeframe
            query_str = f"Historical invoice delays, overall payment behavior, class A B C D E, systemic financial risks. {context_keywords or ''}"
            res = self.collection.query(
                query_texts=[query_str], 
                n_results=100 # Scoops up a massive chunk of overall history
            )
            if res['documents'] and len(res['documents'][0]) > 0: 
                return "\n---\n".join(res['documents'][0])
        except Exception as e: 
            logger.error(f"Query Error: {e}")
            return "Tidak ada data finansial internal."

class Researcher:
    @staticmethod
    def _execute_serper_search(query, num_results=3):
        """Mesin inti Serper.dev untuk mengeksekusi pencarian OSINT."""
        if not SERPER_API_KEY or SERPER_API_KEY == "masukkan_api_key_serper_anda_disini":
            return "Data OSINT eksternal tidak tersedia (API Key Serper belum diisi)."
        
        url = "https://google.serper.dev/search"
        payload = json.dumps({
            "q": query,
            "num": num_results,
            "gl": "id", # Geolocation: Indonesia (Memastikan hasil relevan secara lokal)
            "hl": "id"  # Language: Bahasa Indonesia
        })
        headers = {
            'X-API-KEY': SERPER_API_KEY,
            'Content-Type': 'application/json'
        }
        
        try:
            response = requests.request("POST", url, headers=headers, data=payload, timeout=8)
            res_json = response.json()
            
            # Mengekstrak hasil pencarian organik
            organic_results = res_json.get('organic', [])
            snippets = [item.get('snippet', '') for item in organic_results if 'snippet' in item]
            
            if not snippets:
                return "Pencarian berhasil, namun tidak ada snippet data yang ditemukan."
                
            return "\n".join(snippets)
        except Exception as e:
            logger.error(f"Serper API Error: {str(e)}")
            return "Gagal memuat tren OSINT via Serper.dev."
            
    @staticmethod
    def get_macro_finance_trends():
        query = "Siklus pencairan anggaran APBN pemerintah BUMN corporate payment behavior invoice trends Indonesia"
        return Researcher._execute_serper_search(query, num_results=3)

class StyleEngine:
    @staticmethod
    def apply_document_styles(doc, theme_color):
        for section in doc.sections:
            section.top_margin = Cm(2.54); section.bottom_margin = Cm(2.54)
            section.left_margin = Cm(2.54); section.right_margin = Cm(2.54)
            footer = section.footer
            footer_para = footer.paragraphs[0] if footer.paragraphs else footer.add_paragraph()
            footer_para.alignment = WD_ALIGN_PARAGRAPH.RIGHT
            run = footer_para.add_run("STRICTLY CONFIDENTIAL | Inixindo Jogja Financial Report")
            run.font.name = 'Calibri'; run.font.size = Pt(9); run.font.color.rgb = RGBColor(128, 128, 128)

        style_normal = doc.styles['Normal']
        style_normal.font.name = 'Calibri'; style_normal.font.size = Pt(11); style_normal.font.color.rgb = RGBColor(33, 37, 41)
        pf = style_normal.paragraph_format
        pf.alignment = WD_ALIGN_PARAGRAPH.JUSTIFY
        pf.line_spacing_rule = WD_LINE_SPACING.MULTIPLE; pf.line_spacing = 1.15; pf.space_after = Pt(10) 

        style_h1 = doc.styles['Heading 1']
        style_h1.font.name = 'Arial'; style_h1.font.size = Pt(16); style_h1.font.bold = True
        style_h1.font.color.rgb = RGBColor(*theme_color)
        style_h1.paragraph_format.space_before = Pt(24); style_h1.paragraph_format.space_after = Pt(12)

        style_h2 = doc.styles['Heading 2']
        style_h2.font.name = 'Arial'; style_h2.font.size = Pt(14); style_h2.font.bold = True
        style_h2.font.color.rgb = RGBColor(0, 0, 0)
        style_h2.paragraph_format.space_before = Pt(18); style_h2.paragraph_format.space_after = Pt(6)

        style_h3 = doc.styles['Heading 3']
        style_h3.font.name = 'Calibri'; style_h3.font.size = Pt(12); style_h3.font.bold = True
        style_h3.font.color.rgb = RGBColor(64, 64, 64)
        style_h3.paragraph_format.space_before = Pt(12); style_h3.paragraph_format.space_after = Pt(6)

class ChartEngine:
    @staticmethod
    def _get_plt_color(theme_color): return tuple(c/255 for c in theme_color)

    @staticmethod
    def create_bar_chart(data_str, theme_color):
        try:
            parts = data_str.split('|')
            title_str, ylabel_str, raw_data = parts[0].strip(), parts[1].strip(), parts[2].strip() if len(parts) == 3 else ("Distribusi Historis Kelas Pembayaran", "Persentase", data_str)
            labels, values = [], []
            for p in raw_data.split(';'):
                if ',' in p:
                    l, v = p.split(',', 1)
                    labels.append(l.strip())
                    values.append(float(re.sub(r'[^\d.]', '', v)))
            if not labels: return None
            fig, ax = plt.subplots(figsize=(7, 4.5))
            ax.bar(labels, values, color=ChartEngine._get_plt_color(theme_color), alpha=0.9, width=0.5)
            ax.set_title(title_str, fontsize=12, fontweight='bold', pad=20)
            ax.set_ylabel(ylabel_str, fontsize=10)
            ax.spines['top'].set_visible(False); ax.spines['right'].set_visible(False)
            img = io.BytesIO()
            plt.savefig(img, format='png', bbox_inches='tight', dpi=150)
            plt.close()
            img.seek(0)
            return img
        except Exception: return None

    @staticmethod
    def create_flowchart(data_str, theme_color):
        try:
            steps = ["\n".join(textwrap.wrap(s.strip(), width=18)) for s in data_str.split('->')]
            if len(steps) < 2: return None
            fig, ax = plt.subplots(figsize=(8, 3))
            ax.axis('off')
            x_pos = [i * 2.5 for i in range(len(steps))]
            for i in range(len(steps) - 1):
                ax.annotate("", xy=(x_pos[i+1]-1.0, 0.5), xytext=(x_pos[i]+1.0, 0.5), arrowprops=dict(arrowstyle="-|>", lw=1.5))
            for i, step in enumerate(steps):
                box = patches.FancyBboxPatch((x_pos[i]-1.0, 0.1), 2.0, 0.8, boxstyle="round,pad=0.1", fc=ChartEngine._get_plt_color(theme_color), alpha=0.9)
                ax.add_patch(box)
                ax.text(x_pos[i], 0.5, step, ha="center", va="center", size=9, color="white", fontweight='bold')
            ax.set_xlim(-1.2, (len(steps)-1)*2.5 + 1.2)
            ax.set_ylim(0, 1)
            img = io.BytesIO()
            plt.savefig(img, format='png', bbox_inches='tight', dpi=200, transparent=True)
            plt.close()
            img.seek(0)
            return img
        except Exception: return None

class DocumentBuilder:
    @staticmethod
    def parse_html_to_docx(doc, html_content, theme_color):
        soup = BeautifulSoup(html_content, 'html.parser')
        for element in soup.children:
            if element.name is None: continue
            if element.name in ['h1', 'h2', 'h3']:
                level = int(element.name[1])
                p = doc.add_heading(element.get_text().strip(), level=level)
                if level == 1: 
                    for run in p.runs: run.font.color.rgb = RGBColor(*theme_color)
            elif element.name == 'p':
                p = doc.add_paragraph()
                p.alignment = WD_ALIGN_PARAGRAPH.JUSTIFY 
                for child in element.children:
                    if child.name in ['strong', 'b']: p.add_run(child.get_text()).bold = True
                    elif child.name in ['em', 'i']: p.add_run(child.get_text()).italic = True
                    elif child.name is None: p.add_run(str(child))
            elif element.name in ['ul', 'ol']:
                style = 'List Bullet' if element.name == 'ul' else 'List Number'
                for li in element.find_all('li'):
                    p = doc.add_paragraph(li.get_text(), style=style)
                    p.alignment = WD_ALIGN_PARAGRAPH.JUSTIFY

    @staticmethod
    def process_content(doc, raw_text, theme_color=DEFAULT_COLOR):
        clean_lines = []
        for line in raw_text.split('\n'):
            line = line.strip()
            if line.startswith('[[CHART:') and line.endswith(']]'):
                img = ChartEngine.create_bar_chart(line.replace('[[CHART:', '').replace(']]', '').strip(), theme_color)
                if img: doc.add_paragraph().add_run().add_picture(img, width=Inches(5.5))
                continue
            if line.startswith('[[FLOW:') and line.endswith(']]'):
                img = ChartEngine.create_flowchart(line.replace('[[FLOW:', '').replace(']]', '').strip(), theme_color)
                if img: doc.add_paragraph().add_run().add_picture(img, width=Inches(6.5))
                continue
            clean_lines.append(line)
            
        html = markdown.markdown("\n".join(clean_lines), extensions=['tables'])
        DocumentBuilder.parse_html_to_docx(doc, html, theme_color)

    @staticmethod
    def create_cover(doc, theme_color=DEFAULT_COLOR):
        StyleEngine.apply_document_styles(doc, theme_color)
        for _ in range(5): doc.add_paragraph()
        
        conf = doc.add_paragraph("S T R I C T L Y   C O N F I D E N T I A L")
        conf.alignment = WD_ALIGN_PARAGRAPH.CENTER
        conf.runs[0].font.size = Pt(10)
        conf.runs[0].font.color.rgb = RGBColor(128, 128, 128)
        conf.runs[0].font.bold = True
        doc.add_paragraph() 
        
        t = doc.add_paragraph("ALL-TIME HISTORICAL REVENUE PREDICTOR")
        t.alignment = WD_ALIGN_PARAGRAPH.CENTER
        t.runs[0].font.name = 'Arial'
        t.runs[0].font.size = Pt(20)
        
        c = doc.add_paragraph("INIXINDO JOGJA")
        c.alignment = WD_ALIGN_PARAGRAPH.CENTER
        c.runs[0].font.name = 'Arial'
        c.runs[0].bold = True
        c.runs[0].font.size = Pt(36)
        c.runs[0].font.color.rgb = RGBColor(*theme_color)
        doc.add_paragraph() 
        
        p_name = doc.add_paragraph(f"Cakupan Data: Seluruh Histori Database (All-Time)")
        p_name.alignment = WD_ALIGN_PARAGRAPH.CENTER
        p_name.runs[0].font.size = Pt(14)
        
        current_date = datetime.now().strftime("%d %B %Y")
        p_date = doc.add_paragraph(f"Tanggal Generasi AI: {current_date}")
        p_date.alignment = WD_ALIGN_PARAGRAPH.CENTER
        p_date.runs[0].font.size = Pt(12)
        p_date.runs[0].font.color.rgb = RGBColor(128, 128, 128)
        
        for _ in range(8): doc.add_paragraph()
        
        s = doc.add_paragraph(f"Prepared for Executive Board by:\n{WRITER_FIRM_NAME}")
        s.alignment = WD_ALIGN_PARAGRAPH.CENTER
        s.runs[0].font.bold = True
        doc.add_page_break()

class ReportGenerator:
    def __init__(self, kb_instance):
        self.ollama = Client(host=OLLAMA_HOST)
        self.kb = kb_instance
        self.io_pool = concurrent.futures.ThreadPoolExecutor(max_workers=5)

    def _fetch_chapter_context(self, chap, notes, research_futures):
        try:
            try: industry_trends = research_futures['trends'].result(timeout=5)
            except Exception: industry_trends = "Tidak ada tren finansial eksternal."

            rag_data = self.kb.query(chap['keywords'] + " " + notes)
            persona = PERSONAS.get('default')
            
            subs = "\n".join([f"### {s}" for s in chap['subs']])
            
            visual_prompt = "Do not force visuals."
            if "visual_intent" in chap:
                if chap['visual_intent'] == "bar_chart": visual_prompt = "Mandatory Visual: [[CHART: Distribusi Historis Kelas Pembayaran | Persentase | Kelas A,30; Kelas B,25; Kelas C,20; Kelas D,15; Kelas E,10]]"
                elif chap['visual_intent'] == "flowchart": visual_prompt = "Action Plan Visual: [[FLOW: Analisis Pola Historis -> Pembaruan SOP Penagihan -> Pengurangan Risiko Gagal Bayar]]."

            prompt = FINANCE_SYSTEM_PROMPT.format(
                persona=persona, industry_trends=industry_trends,
                rag_data=rag_data, visual_prompt=visual_prompt,
                chapter_title=chap['title'], sub_chapters=subs
            )
            return {"prompt": prompt, "success": True}
        except Exception as e:
            return {"prompt": "", "success": False, "error": str(e)}

    def run(self, notes=""):
        logger.info(f"Starting All-Time Historical Financial Generation")
        
        research_futures = {
            'trends': self.io_pool.submit(Researcher.get_macro_finance_trends)
        }
        
        context_futures = {}
        for chap in FINANCE_STRUCTURE:
            context_futures[chap['id']] = self.io_pool.submit(
                self._fetch_chapter_context, chap, notes, research_futures
            )

        doc = Document()
        DocumentBuilder.create_cover(doc, DEFAULT_COLOR)
        
        for i, chap in enumerate(FINANCE_STRUCTURE):
            ctx = context_futures[chap['id']].result()
            if ctx['success']:
                try:
                    res = self.ollama.chat(
                        model=LLM_MODEL, 
                        messages=[{'role': 'system', 'content': ctx['prompt']}, {'role': 'user', 'content': f"Write content for {chap['title']}. Remember: Use '###' for EVERY sub-chapter header and focus on ALL-TIME historical patterns."}],
                        options={'num_ctx': 4096}  
                    )
                    doc.add_heading(chap['title'], level=1)
                    DocumentBuilder.process_content(doc, res['message']['content'], DEFAULT_COLOR)
                    if i < len(FINANCE_STRUCTURE) - 1: doc.add_page_break()
                except Exception as e: logger.error(f"Error {chap['title']}: {e}")

        return doc, "Inixindo_Historical_Revenue_Prediction"