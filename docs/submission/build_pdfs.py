"""Rebuild the report and three presentation slides with ReportLab 4.4."""
from pathlib import Path
import re
from xml.sax.saxutils import escape

import reportlab
from reportlab.lib import colors
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import ParagraphStyle
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.pdfgen import canvas
from reportlab.platypus import Paragraph, Table, TableStyle

HERE = Path(__file__).resolve().parent
INK, ACCENT, PALE, WHITE = "#103139", "#226957", "#E5F0EC", "#FFFFFF"


def paragraph(doc, text, x, top, width, size=10.5, font="Vera", color=INK):
    style = ParagraphStyle("text", fontName=font, fontSize=size, leading=size*1.36,
                           textColor=colors.HexColor(color))
    obj = Paragraph(text, style)
    _, height = obj.wrap(width, 2000)
    obj.drawOn(doc, x, top-height)
    return top-height


def markup(text):
    value = escape(text)
    value = re.sub(r"\[([^\]]+)\]\((https?://[^)]+)\)", r'<link href="\2" color="#216957"><u>\1</u></link>', value)
    value = re.sub(r"\*\*(.+?)\*\*", r"<b>\1</b>", value)
    value = re.sub(r"\*([^*]+)\*", r"<i>\1</i>", value)
    return re.sub(r"`([^`]+)`", r'<font name="Courier">\1</font>', value)


def draw_table(doc, block, top):
    rows = [line.strip("|").split("|") for line in block.splitlines() if not line.startswith("| ---")]
    widths = [135, 371] if len(rows[0]) == 2 else [185, 107, 107, 107]
    cells = []
    for i, row in enumerate(rows):
        style = ParagraphStyle("cell", fontName="VeraB" if i == 0 else "Vera", fontSize=9.2,
                               leading=13.2, textColor=colors.HexColor(WHITE if i == 0 else INK))
        cells.append([Paragraph(markup(cell.strip()), style) for cell in row])
    obj = Table(cells, colWidths=widths)
    obj.setStyle(TableStyle([("BACKGROUND",(0,0),(-1,0),colors.HexColor(INK)),
                             ("ROWBACKGROUNDS",(0,1),(-1,-1),[colors.HexColor(PALE),colors.white]),
                             ("VALIGN",(0,0),(-1,-1),"TOP"),
                             ("TOPPADDING",(0,0),(-1,-1),6),("BOTTOMPADDING",(0,0),(-1,-1),6)]))
    _, height = obj.wrap(506, 2000)
    obj.drawOn(doc, 53, top-height)
    return top-height


def report():
    pages = (HERE/"report.md").read_text().strip().split("\n\n---\n\n")
    doc = canvas.Canvas(str(HERE/"Cloud-Chaos-Twin-Report.pdf"), pagesize=letter, invariant=True)
    doc.setTitle("Cloud Chaos Twin | Experimental report")
    doc.setAuthor("Stony Brook University, Group 1")
    for number, page in enumerate(pages, 1):
        paragraph(doc, "STONY BROOK UNIVERSITY   /   BIG DATA SYSTEMS", 53, 757, 400, 8, "VeraB", ACCENT)
        paragraph(doc, "NOVEMBER 28, 2026", 460, 757, 110, 8, "VeraB", ACCENT)
        doc.setStrokeColor(colors.HexColor("#C4D4CF"))
        doc.line(53, 740, 559, 740)
        top = 718
        for block in page.strip().split("\n\n"):
            if block.startswith("# "):
                top = paragraph(doc, markup(block[2:]), 53, top, 506, 24, "VeraB")-16
            elif block.startswith("## "):
                top = paragraph(doc, markup(block[3:]), 53, top, 506, 14, "VeraB", ACCENT)-7
            elif block.startswith("|"):
                top = draw_table(doc, block, top)-12
            elif block.startswith("```"):
                for line in block.splitlines()[1:-1]:
                    top = paragraph(doc, escape(line), 53, top, 506, 8.3, "Courier")
                top -= 12
            else:
                top = paragraph(doc, markup(block), 53, top, 506, 9.7 if number==len(pages) else 10.4)-11
        if top < 46:
            raise ValueError(f"Page {number} overlaps its footer")
        doc.line(53, 44, 559, 44)
        paragraph(doc, "Cloud Chaos Twin   /   Group 1", 53, 36, 200, 8)
        paragraph(doc, f"{number} / {len(pages)}", 525, 36, 45, 8)
        doc.showPage()
    doc.save()


def slides():
    doc = canvas.Canvas(str(HERE/"Cloud-Chaos-Twin-Slides.pdf"), pagesize=(960,540), invariant=True)
    doc.setTitle("Cloud Chaos Twin | Saved fault and recovery")
    doc.setAuthor("Stony Brook University, Group 1")
    def page(dark, label):
        doc.setFillColor(colors.HexColor(INK if dark else "#F8F8F3"))
        doc.rect(0,0,960,540,fill=1,stroke=0)
        paragraph(doc, label, 48, 496, 865, 14, "VeraB", "#83D4BE" if dark else ACCENT)
    page(True, "AMS 560 / CSE 542     ·     STONY BROOK UNIVERSITY")
    paragraph(doc,"Cloud Chaos Twin",48,427,850,46,"VeraB",WHITE)
    paragraph(doc,"When checkout slows down, can we see where the fault spread and warn the next service in time?",48,326,850,29,"Vera",WHITE)
    paragraph(doc,"We trace shopping requests, measure what changes during a controlled fault, and compare warnings with an independent record of service health.",48,177,845,21,"Vera","#BEE7DD")
    paragraph(doc,"Maitreya Vaghulade  ·  Merul Shah  ·  Param Shah  ·  Komalika Acharya  ·  Mithil Bavishi",48,51,865,14,"Vera",WHITE)
    doc.showPage()
    page(False,"PROJECT DELIVERABLES     /     NOVEMBER 28, 2026")
    paragraph(doc,"What the team can demonstrate",48,442,875,36,"VeraB")
    for y, label, detail in [
        (365,"Telemetry &amp; service graph","Ten application services traced. All 15 required browse, cart and checkout relationships verified."),
        (278,"Fault &amp; recovery","Three catalog-delay trials recovered. The final trial’s median catalog RPC time rose from 0.274 to 106.930 ms, then fell to 0.282 ms."),
        (182,"Ingestion &amp; replay","Spark rebuilt 11 Parquet tables from recorded Kafka messages. Continuous ingestion delay still needs measurement."),
        (99,"Health &amp; forecasts","The two channels agreed on 1,548 of 1,600 service-windows. The other 52 were unscorable. Five warning methods missed the final test.")]:
        bottom = paragraph(doc,label,48,y,275,19,"VeraB",ACCENT)
        end = paragraph(doc,detail,340,y,570,18)
        if end < 18 or bottom < 18:
            raise ValueError("Slide row overlaps edge")
    doc.showPage()
    page(True,"BASELINES     /     OBSERVED RECONSTRUCTION AND ADVANCE WARNING")
    paragraph(doc,"Tracing recovered links. Forecasts missed onsets.",48,439,885,32,"VeraB",WHITE)
    paragraph(doc,"0 vs. 15 of 15",48,334,420,40,"VeraB","#83D4BE")
    paragraph(doc,"Expected links on three synthetic journeys: direct server-parent join versus walking span ancestry.",48,255,395,20,"Vera",WHITE)
    paragraph(doc,"0.0 F1",500,334,395,40,"VeraB","#83D4BE")
    paragraph(doc,"Five methods at 30 seconds on one held-out delay trial. Mean scorable coverage was 86.7%.",500,255,395,20,"Vera",WHITE)
    paragraph(doc,"The saved run connects a measured fault to its service graph and recovery. A new early-warning rule awaits fresh trials on the same cutoffs.",48,126,854,18,"Vera","#BEE7DD")
    paragraph(doc,"Sources   ·   CCT-10 validation receipt   ·   CCT-11 saved-run demo   ·   observability.fixture",48,42,875,13,"Vera",WHITE)
    doc.showPage()
    doc.save()


if __name__ == "__main__":
    for name, filename in [("Vera","Vera.ttf"),("VeraB","VeraBd.ttf"),("VeraI","VeraIt.ttf")]:
        pdfmetrics.registerFont(TTFont(name,str(Path(reportlab.__file__).parent/"fonts"/filename)))
    pdfmetrics.registerFontFamily("Vera",normal="Vera",bold="VeraB",italic="VeraI",boldItalic="VeraB")
    report()
    slides()

