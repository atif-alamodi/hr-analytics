# ===================================================
# منصة تحليلات الموارد البشرية الذكية v4.0
# رسال الود لتقنية المعلومات
# المرحلة 2: ميزانية التدريب + ROI + الاحتياجات التدريبية
# ===================================================

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import io, json, math
from datetime import datetime

st.set_page_config(page_title="تحليلات HR | رسال الود", page_icon="📊", layout="wide", initial_sidebar_state="expanded")

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Noto+Sans+Arabic:wght@300;400;500;600;700;800&display=swap');
*{font-family:'Noto Sans Arabic',sans-serif}
.main .block-container{padding-top:1rem;max-width:1400px}
[data-testid="stSidebar"]{background:linear-gradient(180deg,#0F4C5C 0%,#1A1A2E 100%)}
[data-testid="stSidebar"] *{color:white !important}
[data-testid="stMetric"]{background:white;border-radius:12px;padding:16px 20px;box-shadow:0 1px 3px rgba(0,0,0,.06);border:1px solid #E2E8F0}
[data-testid="stMetric"] label{font-size:13px !important;color:#64748B !important}
[data-testid="stMetric"] [data-testid="stMetricValue"]{font-size:22px !important;font-weight:700 !important}
h1{color:#0F4C5C !important;font-weight:800 !important}
.hdr{background:linear-gradient(135deg,#0F4C5C,#1A1A2E);padding:24px 32px;border-radius:16px;margin-bottom:24px;color:white}
.hdr h1{color:white !important;margin:0;font-size:26px}
.hdr p{color:rgba(255,255,255,.7);margin:4px 0 0;font-size:14px}
.ibox{background:#EFF6FF;border-radius:12px;padding:14px 18px;border-right:4px solid #3B82F6;margin-bottom:10px;font-size:14px;line-height:1.7}
.ibox.warn{background:#FFF7ED;border-right-color:#F97316}
.ibox.ok{background:#F0FDF4;border-right-color:#22C55E}
.ibox.bad{background:#FEF2F2;border-right-color:#EF4444}
.kpi-card{background:linear-gradient(135deg,#0F4C5C,#1B4D5C);color:white;border-radius:14px;padding:20px;text-align:center;margin-bottom:12px}
.kpi-card h3{font-size:28px;margin:8px 0 4px;font-weight:800}
.kpi-card p{font-size:12px;opacity:.7;margin:0}
#MainMenu,footer{visibility:hidden}
</style>
""", unsafe_allow_html=True)

CL = {'primary':'#0F4C5C','accent':'#E36414','success':'#2D6A4F','danger':'#9A031E','dept':px.colors.qualitative.Set2}

def hdr(title, sub=""):
    st.markdown(f'<div class="hdr"><h1>{title}</h1><p>{sub}</p></div>', unsafe_allow_html=True)

def ibox(text, t="info"):
    c = {"info":"ibox","warning":"ibox warn","success":"ibox ok","danger":"ibox bad"}
    icons = {"info":"💡","warning":"⚠️","success":"✅","danger":"🚨"}
    st.markdown(f'<div class="{c.get(t,"ibox")}">{icons.get(t,"💡")} {text}</div>', unsafe_allow_html=True)

def kpi(label, value):
    st.markdown(f'<div class="kpi-card"><p>{label}</p><h3>{value}</h3></div>', unsafe_allow_html=True)

def fmt(v): return f"{v:,.0f}"

def has(df, n): return df is not None and n in df.columns

def safe_mean(df, n): return df[n].mean() if has(df,n) and len(df)>0 else 0

# ===== TRAINING PROVIDERS DATABASE =====
PROVIDERS = {
    "السعودية": [
        {"name": "معهد الإدارة العامة (IPA)", "speciality": "الإدارة والقيادة", "type": "حكومي", "url": "ipa.edu.sa"},
        {"name": "غرفة جدة - بيت التدريب", "speciality": "المهارات المهنية", "type": "شبه حكومي", "url": "jcci.org.sa"},
        {"name": "KPMG Academy", "speciality": "المالية والمحاسبة", "type": "خاص", "url": "kpmg.com/sa"},
        {"name": "PwC Academy ME", "speciality": "التحول الرقمي والمالية", "type": "خاص", "url": "pwcacademy.me"},
        {"name": "Misk Academy", "speciality": "التقنية والابتكار", "type": "غير ربحي", "url": "misk.org.sa"},
        {"name": "بكه للتعليم", "speciality": "إدارة المشاريع PMP/Agile", "type": "خاص", "url": "bakkah.com"},
        {"name": "معهد تيك كورنر", "speciality": "البرمجة والبيانات", "type": "خاص", "url": "techcorner.sa"},
        {"name": "BIBF الشرق الأوسط", "speciality": "الخدمات المالية والفنتك", "type": "خاص", "url": "bibf.com"},
        {"name": "معهد الأمير مشعل للتدريب", "speciality": "الأمن السيبراني", "type": "خاص", "url": ""},
        {"name": "Udacity MENA", "speciality": "الذكاء الاصطناعي والبيانات", "type": "أونلاين", "url": "udacity.com"},
    ],
    "الخليج": [
        {"name": "BIBF البحرين", "speciality": "البنوك والمالية", "type": "حكومي", "url": "bibf.com"},
        {"name": "Informa Connect", "speciality": "القيادة والاستراتيجية", "type": "خاص", "url": "informaconnect.com"},
        {"name": "London Business School ME", "speciality": "MBA والقيادة التنفيذية", "type": "خاص", "url": "lbs.ac.uk"},
        {"name": "Dale Carnegie UAE", "speciality": "المهارات القيادية", "type": "خاص", "url": "dalecarnegie.com"},
        {"name": "QA International (Doha)", "speciality": "IT وتحول رقمي", "type": "خاص", "url": "qa.com"},
    ],
    "مصر": [
        {"name": "الجامعة الأمريكية بالقاهرة (AUC)", "speciality": "إدارة الأعمال والتسويق", "type": "أكاديمي", "url": "aucegypt.edu"},
        {"name": "IMI Egypt", "speciality": "الإدارة والقيادة", "type": "خاص", "url": "imi-eg.com"},
        {"name": "Sprints", "speciality": "البرمجة والتقنية", "type": "خاص", "url": "sprints.ai"},
        {"name": "Manara", "speciality": "التطوير المهني التقني", "type": "خاص", "url": "manara.tech"},
        {"name": "Digital Egypt Pioneers", "speciality": "التحول الرقمي", "type": "حكومي", "url": "mcit.gov.eg"},
    ],
    "أونلاين عالمي": [
        {"name": "Coursera for Business", "speciality": "متعدد", "type": "أونلاين", "url": "coursera.org"},
        {"name": "LinkedIn Learning", "speciality": "مهارات مهنية", "type": "أونلاين", "url": "linkedin.com/learning"},
        {"name": "Udemy Business", "speciality": "متعدد", "type": "أونلاين", "url": "udemy.com"},
        {"name": "HBS Online", "speciality": "القيادة والاستراتيجية", "type": "أونلاين", "url": "online.hbs.edu"},
        {"name": "Google Career Certificates", "speciality": "التقنية والبيانات", "type": "أونلاين", "url": "grow.google"},
    ]
}

# Training categories -> skills mapping
TRAINING_CATEGORIES = {
    "المبيعات والإيرادات": ["مبيعات استشارية","إدارة حسابات","تفاوض","CRM","خدمة عملاء","NPS/CSAT"],
    "التسويق والنمو": ["تسويق رقمي","Growth Hacking","SEO","تحليلات تسويقية","بناء علامة تجارية","شراكات"],
    "تطوير الأعمال": ["شراكات استراتيجية","توسع إقليمي","بناء عروض","ذكاء تنافسي","عروض المستثمرين"],
    "عمليات المنتجات": ["إدارة منتجات رقمية","Agile/Scrum","تصميم UX","Product-Led Growth","OKRs"],
    "البيانات والذكاء": ["Python/SQL","Power BI/Tableau","AI تنبؤي","هندسة بيانات","حوكمة بيانات"],
    "المالية": ["نمذجة مالية","IFRS","تحليل مالي","تقييم شركات"],
    "الموارد البشرية": ["استقطاب مواهب تقنية","إدارة أداء","OKRs","ثقافة مؤسسية","تفاعل موظفين"],
    "الحوكمة والمخاطر": ["إدارة مخاطر فنتك","SAMA/PCI-DSS","أمن سيبراني"],
    "الشؤون القانونية": ["عقود رقمية","حماية ملكية فكرية","تنظيمات تجارة إلكترونية"],
}

# Default budget template (based on Resal's actual structure)
DEFAULT_BUDGET = [
    {"dept":"المبيعات والإيرادات","budget":16000,"pct":22.9,"priority":"حرج","fit":"مباشر","cat":"محرك إيرادات"},
    {"dept":"التسويق والنمو","budget":13000,"pct":18.6,"priority":"حرج","fit":"مباشر","cat":"محرك إيرادات"},
    {"dept":"تطوير الأعمال","budget":11000,"pct":15.7,"priority":"عالي","fit":"مباشر","cat":"محرك إيرادات"},
    {"dept":"عمليات المنتجات","budget":9000,"pct":12.9,"priority":"عالي","fit":"مباشر","cat":"ممكّن نمو"},
    {"dept":"البيانات والذكاء","budget":7000,"pct":10.0,"priority":"عالي","fit":"مباشر","cat":"ممكّن نمو"},
    {"dept":"المالية","budget":5000,"pct":7.1,"priority":"متوسط","fit":"داعم","cat":"بنية تحتية"},
    {"dept":"الموارد البشرية","budget":4000,"pct":5.7,"priority":"متوسط","fit":"داعم","cat":"بنية تحتية"},
    {"dept":"الحوكمة والمخاطر","budget":3000,"pct":4.3,"priority":"متوسط","fit":"داعم","cat":"بنية تحتية"},
    {"dept":"الشؤون القانونية","budget":2000,"pct":2.9,"priority":"أساسي","fit":"داعم","cat":"بنية تحتية"},
]

DEFAULT_PROGRAMS = {
    "المبيعات والإيرادات": [
        {"program":"مهارات البيع الاستشاري المتقدم","budget":3600,"source":"خارجي","timing":"Q1-Q2","impact":"زيادة معدل التحويل وإغلاق الصفقات الكبرى"},
        {"program":"نجاح العملاء وتعظيم CLV","budget":3000,"source":"خارجي","timing":"Q1","impact":"تحسين الاحتفاظ بالعملاء وزيادة البيع"},
        {"program":"إدارة الحسابات الاستراتيجية","budget":2800,"source":"خارجي","timing":"Q2","impact":"تعظيم الإيرادات من الحسابات الرئيسية"},
        {"program":"تجربة العملاء ومقاييس NPS/CSAT","budget":2400,"source":"داخلي","timing":"Q1-Q3","impact":"تعزيز تجربة العملاء وزيادة الولاء"},
        {"program":"التفاوض ومعالجة الاعتراضات","budget":2200,"source":"خارجي","timing":"Q2-Q3","impact":"تحسين شروط العقود وزيادة هوامش الربح"},
        {"program":"أدوات CRM وأتمتة المبيعات","budget":2000,"source":"داخلي","timing":"Q1","impact":"زيادة كفاءة فريق المبيعات"},
    ],
    "التسويق والنمو": [
        {"program":"التسويق الرقمي المتقدم وإدارة الحملات","budget":3000,"source":"خارجي","timing":"Q1","impact":"زيادة ROAS العائد على الإنفاق الإعلاني"},
        {"program":"استراتيجيات Growth Hacking","budget":2800,"source":"خارجي","timing":"Q1-Q2","impact":"تسريع اكتساب المستخدمين بتكلفة أقل"},
        {"program":"SEO والتسويق بالمحتوى","budget":2000,"source":"أونلاين","timing":"Q2","impact":"زيادة الزيارات العضوية والتحويلات"},
        {"program":"تحليلات التسويق وقياس الأداء","budget":2000,"source":"أونلاين","timing":"Q1-Q2","impact":"قرارات تسويقية مبنية على البيانات"},
        {"program":"إدارة العلامة التجارية وتحديد المواقع","budget":1600,"source":"خارجي","timing":"Q3","impact":"تعزيز مكانة رسال في السوق"},
        {"program":"تسويق الشراكات والتحالفات","budget":1600,"source":"داخلي","timing":"Q2-Q3","impact":"توسيع شبكة الشركاء التجاريين"},
    ],
    "تطوير الأعمال": [
        {"program":"استراتيجية BD والشراكات","budget":3000,"source":"خارجي","timing":"Q1","impact":"بناء شراكات مع قطاعات رئيسية"},
        {"program":"التوسع في أسواق MENA","budget":2400,"source":"خارجي","timing":"Q1-Q2","impact":"دعم خطة التوسع الإقليمي"},
        {"program":"بناء العروض وتصميم الحلول","budget":2000,"source":"داخلي","timing":"Q2","impact":"تحسين معدل الفوز بالعروض"},
        {"program":"الاستخبارات السوقية والتنافسية","budget":2000,"source":"أونلاين","timing":"Q1-Q3","impact":"فهم أعمق للفرص والتهديدات"},
        {"program":"عروض المستثمرين والعرض التقديمي","budget":1600,"source":"خارجي","timing":"Q2","impact":"الاستعداد لجولات التمويل"},
    ],
    "عمليات المنتجات": [
        {"program":"إدارة المنتجات الرقمية المتقدمة","budget":2400,"source":"خارجي","timing":"Q1","impact":"تحسين دورة المنتج وسرعة الإطلاق"},
        {"program":"منهجيات Agile/Scrum المتقدمة","budget":2000,"source":"أونلاين","timing":"Q1-Q2","impact":"زيادة كفاءة فريق التطوير"},
        {"program":"تصميم UX مبني على البيانات","budget":2000,"source":"خارجي","timing":"Q2","impact":"تحسين معدل التحويل داخل التطبيق"},
        {"program":"إدارة النمو عبر المنتج (PLG)","budget":1600,"source":"أونلاين","timing":"Q2-Q3","impact":"النمو من خلال المنتج بدل المبيعات التقليدية"},
        {"program":"OKRs ومقاييس أداء المنتج","budget":1000,"source":"داخلي","timing":"Q1","impact":"مواءمة أهداف المنتج مع الأهداف المالية"},
    ],
    "البيانات والذكاء": [
        {"program":"تحليلات بيانات متقدمة Python/SQL","budget":2000,"source":"أونلاين","timing":"Q1-Q2","impact":"تحليلات متقدمة لدعم القرار"},
        {"program":"لوحات بيانات Power BI/Tableau","budget":1600,"source":"أونلاين","timing":"Q1","impact":"رؤى فورية للإدارة"},
        {"program":"BI تنبؤي وتطبيقات AI","budget":1600,"source":"خارجي","timing":"Q2-Q3","impact":"التنبؤ بسلوك العملاء والإيرادات"},
        {"program":"هندسة البيانات والمستودعات","budget":1000,"source":"أونلاين","timing":"Q2","impact":"بنية بيانات قوية وقابلة للتوسع"},
        {"program":"حوكمة البيانات والخصوصية","budget":800,"source":"داخلي","timing":"Q3","impact":"الامتثال للوائح حماية البيانات"},
    ],
    "المالية": [
        {"program":"النمذجة المالية وتقييم الشركات الناشئة","budget":2000,"source":"خارجي","timing":"Q1","impact":"دعم التقييم السوقي والتمويل"},
        {"program":"التقارير المالية IFRS","budget":1600,"source":"أونلاين","timing":"Q2","impact":"تقارير مالية بمعايير دولية"},
        {"program":"التحليل المالي وإدارة التدفقات","budget":1400,"source":"أونلاين","timing":"Q1-Q2","impact":"تحسين إدارة السيولة والربحية"},
    ],
    "الموارد البشرية": [
        {"program":"استقطاب المواهب التقنية والاحتفاظ","budget":1600,"source":"خارجي","timing":"Q1-Q2","impact":"جذب أفضل الكفاءات في سوق تنافسي"},
        {"program":"إدارة الأداء وOKRs الاستراتيجية","budget":1400,"source":"أونلاين","timing":"Q1","impact":"ربط أداء الموظفين بنمو الأعمال"},
        {"program":"بناء الثقافة وتفاعل الموظفين","budget":1000,"source":"داخلي","timing":"Q2-Q3","impact":"تعزيز الإنتاجية وتقليل الدوران"},
    ],
    "الحوكمة والمخاطر": [
        {"program":"إدارة مخاطر الفنتك","budget":1200,"source":"خارجي","timing":"Q2","impact":"إدارة المخاطر التنظيمية والتشغيلية"},
        {"program":"الامتثال التنظيمي SAMA/PCI-DSS","budget":1000,"source":"أونلاين","timing":"Q1","impact":"ضمان الامتثال للمتطلبات التنظيمية"},
        {"program":"أمن المعلومات والسيبراني","budget":800,"source":"أونلاين","timing":"Q1-Q3","impact":"حماية البيانات والأنظمة"},
    ],
    "الشؤون القانونية": [
        {"program":"العقود التجارية الرقمية وSLAs","budget":1000,"source":"خارجي","timing":"Q2","impact":"حماية مصالح الشركة في الشراكات"},
        {"program":"حماية الملكية الفكرية والعلامات","budget":600,"source":"أونلاين","timing":"Q3","impact":"حماية الأصول الفكرية"},
        {"program":"تنظيمات التجارة الإلكترونية والمدفوعات","budget":400,"source":"داخلي","timing":"Q1","impact":"الامتثال لأنظمة التجارة الإلكترونية"},
    ],
}

Q_SPLIT = {"Q1":0.35,"Q2":0.30,"Q3":0.20,"Q4":0.15}

# ===== ROI CALCULATION (Phillips 5-Level Model) =====
def calc_roi(total_budget, expected_revenue_increase_pct, current_revenue, retention_improvement_pct, avg_salary, headcount, productivity_gain_pct):
    """Phillips ROI Methodology"""
    # Level 1: Reaction (assumed 90% satisfaction)
    l1_satisfaction = 90

    # Level 2: Learning (assumed 85% pass rate)
    l2_learning = 85

    # Level 3: Application (75% apply within 30 days)
    l3_application = 75

    # Level 4: Business Impact
    revenue_gain = current_revenue * (expected_revenue_increase_pct / 100)
    retention_savings = (retention_improvement_pct / 100) * headcount * avg_salary * 0.5  # 50% of salary = replacement cost
    productivity_value = productivity_gain_pct / 100 * headcount * avg_salary * 0.1  # 10% of salary = productivity value

    total_benefits = revenue_gain + retention_savings + productivity_value

    # Level 5: ROI
    roi_pct = ((total_benefits - total_budget) / total_budget) * 100
    payback_months = (total_budget / max(total_benefits/12, 1))

    return {
        "satisfaction": l1_satisfaction,
        "learning": l2_learning,
        "application": l3_application,
        "revenue_gain": revenue_gain,
        "retention_savings": retention_savings,
        "productivity_value": productivity_value,
        "total_benefits": total_benefits,
        "roi_pct": roi_pct,
        "payback_months": payback_months,
        "bcr": total_benefits / max(total_budget, 1),
    }


# ===== SMART DATA LOADER (from v3) =====
COL_MAP = {
    'emp id':'رقم الموظف','employee id':'رقم الموظف','name (english)':'الاسم الإنجليزي',
    'name (arabic)':'الاسم','name':'الاسم','department':'القسم','dept':'القسم',
    'job title':'المسمى الوظيفي','position':'المسمى الوظيفي','join date':'تاريخ التعيين',
    'hiring date':'تاريخ التعيين','location':'الموقع','city':'الموقع',
    'tenure (yrs)':'سنوات الخدمة','tenure':'سنوات الخدمة',
    'basic salary':'الراتب الأساسي','salary':'الراتب الأساسي','الراتب الأساسي':'الراتب الأساسي',
    'nationality group':'الجنسية','nationality':'الجنسية','الجنسية':'الجنسية',
    'gender':'الجنس','الجنس':'الجنس','status':'الحالة','الحالة':'الحالة',
    'القسم':'القسم','الاسم':'الاسم','الموقع':'الموقع',
    'gross salary':'الراتب الإجمالي','net salary':'صافي الراتب',
    'housing allowance':'بدل السكن','transportation allowance':'بدل النقل',
    'grade':'الدرجة','level':'المستوى','age':'العمر','age group':'الفئة العمرية',
    'generation':'الجيل','employment type':'نوع التوظيف','division':'القطاع',
}

def smart_read(xl, sheet):
    df_raw = pd.read_excel(xl, sheet_name=sheet, header=None)
    best_row, best_score = 0, 0
    for i in range(min(5, len(df_raw))):
        score = sum(1 for v in df_raw.iloc[i] if isinstance(v, str) and len(str(v).strip())>1 and not str(v).startswith('Unnamed') and not str(v).startswith('Total'))
        if score > best_score: best_score, best_row = score, i
    df = pd.read_excel(xl, sheet_name=sheet, header=best_row)
    df = df[[c for c in df.columns if not str(c).startswith('Unnamed')]].dropna(how='all').reset_index(drop=True)
    return df

def norm_cols(df):
    new = {}
    for c in df.columns:
        k = str(c).strip().lower()
        new[c] = COL_MAP.get(k, c)
    df = df.rename(columns=new)
    if 'الاسم' not in df.columns and 'الاسم الإنجليزي' in df.columns:
        df['الاسم'] = df['الاسم الإنجليزي']
    return df

def parse_dashboard(xl):
    sections = {}
    try:
        df_raw = pd.read_excel(xl, sheet_name='Dashboard', header=None)
        cur, data, hdrs = None, [], []
        for _, row in df_raw.iterrows():
            vals = [v for v in row if pd.notna(v)]
            if not vals:
                if cur and data: sections[cur] = pd.DataFrame(data, columns=hdrs if hdrs else None)
                data, hdrs, cur = [], [], None
                continue
            text = str(vals[0]).strip()
            if len(vals)==1 and len(text)>3 and text.isupper() and text!='TOTAL':
                if cur and data: sections[cur] = pd.DataFrame(data, columns=hdrs if hdrs else None)
                cur, data, hdrs = text, [], []
            elif cur and not hdrs and any(isinstance(v,str) for v in vals):
                hdrs = [str(v).strip() for v in vals if pd.notna(v)]
            elif cur and hdrs:
                clean = [v for v in vals if pd.notna(v)]
                if clean and str(clean[0]).strip()!='TOTAL':
                    padded = (clean + [None]*len(hdrs))[:len(hdrs)]
                    data.append(padded)
        if cur and data: sections[cur] = pd.DataFrame(data, columns=hdrs if hdrs else None)
    except: pass
    return sections


# ===== MAIN =====
def main():
    with st.sidebar:
        st.markdown("<div style='text-align:center;padding:20px 0;'><div style='background:linear-gradient(135deg,#E36414,#E9C46A);width:60px;height:60px;border-radius:14px;display:flex;align-items:center;justify-content:center;margin:0 auto 12px;font-size:24px;font-weight:800;color:white;'>HR</div><h2 style='margin:0;font-size:17px;'>تحليلات الموارد البشرية</h2><p style='opacity:.6;font-size:11px;'>رسال الود لتقنية المعلومات</p></div>", unsafe_allow_html=True)
        st.markdown("---")
        section = st.radio("📌 القسم", ["📊 التحليلات العامة", "📚 التدريب والتطوير"], label_visibility="collapsed")
        st.markdown("---")

        if section == "📊 التحليلات العامة":
            page = st.radio("📌", ["🏠 نظرة عامة","📊 الأقسام والمواقع","🤖 المحلل الذكي","📋 بيانات الموظفين","📥 تصدير"], label_visibility="collapsed")
        else:
            page = st.radio("📌", ["📚 ميزانية التدريب","💹 عائد التدريب ROI","📋 الاحتياجات التدريبية","🏫 جهات التدريب","📥 تصدير التدريب"], label_visibility="collapsed")

        st.markdown("---")
        st.markdown("##### 📁 بيانات الموظفين")
        file = st.file_uploader("ارفع Excel", type=["xlsx","xls","csv"], label_visibility="collapsed", key="emp_file")
        if file: st.success("✅ تم التحميل")


    # ===== LOAD DATA =====
    emp = pd.DataFrame()
    all_sheets = {}
    dash_sections = {}

    if file:
        try:
            if file.name.endswith('.csv'):
                emp = norm_cols(pd.read_csv(file))
                all_sheets = {'البيانات': emp}
            else:
                xl = pd.ExcelFile(file)
                for s in xl.sheet_names:
                    try:
                        df_s = norm_cols(smart_read(xl, s))
                        all_sheets[s] = df_s
                        if len(emp)==0 and len(df_s)>5:
                            name_cols = [c for c in df_s.columns if any(x in str(c).lower() for x in ['name','اسم','emp','موظف'])]
                            if name_cols: emp = df_s
                    except: pass
                if len(emp)==0 and all_sheets: emp = list(all_sheets.values())[0]
                if 'Dashboard' in xl.sheet_names:
                    file.seek(0)
                    dash_sections = parse_dashboard(pd.ExcelFile(file))
        except: pass

    if '#' in emp.columns and len(emp)>0:
        emp = emp[pd.to_numeric(emp['#'], errors='coerce').notna()].reset_index(drop=True)

    n = len(emp)


    # =========================================
    #         📊 GENERAL ANALYTICS PAGES
    # =========================================
    if section == "📊 التحليلات العامة":

        if page == "🏠 نظرة عامة":
            hdr("📊 نظرة عامة", "ملخص شامل لبيانات القوى العاملة")
            if n == 0:
                st.info("📁 ارفع ملف بيانات الموظفين من القائمة الجانبية")
                return
            cols = st.columns(4)
            with cols[0]: st.metric("👥 الموظفين", n)
            with cols[1]: st.metric("🏢 الأقسام", emp['القسم'].nunique() if has(emp,'القسم') else '-')
            with cols[2]: st.metric("📍 المواقع", emp['الموقع'].nunique() if has(emp,'الموقع') else '-')
            with cols[3]: st.metric("📅 متوسط الخدمة", f"{safe_mean(emp,'سنوات الخدمة'):.1f}" if has(emp,'سنوات الخدمة') else '-')

            if has(emp,'القسم'):
                c1,c2 = st.columns(2)
                with c1:
                    dc = emp['القسم'].value_counts().reset_index(); dc.columns=['القسم','العدد']
                    fig = px.pie(dc, values='العدد', names='القسم', title='توزيع الموظفين', hole=.4, color_discrete_sequence=CL['dept'])
                    fig.update_layout(font=dict(family="Noto Sans Arabic"), height=400); st.plotly_chart(fig, use_container_width=True)
                with c2:
                    if has(emp,'الموقع'):
                        lc = emp['الموقع'].value_counts().reset_index(); lc.columns=['الموقع','العدد']
                        fig = px.pie(lc, values='العدد', names='الموقع', title='التوزيع الجغرافي', hole=.4)
                        fig.update_layout(font=dict(family="Noto Sans Arabic"), height=400); st.plotly_chart(fig, use_container_width=True)

        elif page == "📊 الأقسام والمواقع":
            hdr("📊 الأقسام والمواقع")
            if n==0: st.info("📁 ارفع ملف بيانات"); return
            if has(emp,'القسم'):
                dc = emp['القسم'].value_counts().reset_index(); dc.columns=['القسم','العدد']
                fig = px.bar(dc.sort_values('العدد'), x='العدد', y='القسم', orientation='h', color='العدد', color_continuous_scale='teal')
                fig.update_layout(font=dict(family="Noto Sans Arabic"), height=500); st.plotly_chart(fig, use_container_width=True)

        elif page == "🤖 المحلل الذكي":
            hdr("🤖 المحلل الذكي", "يبحث في كل الأوراق")
            if n==0: st.info("📁 ارفع ملف"); return
            q = st.text_input("💬 اسأل:", placeholder="مثال: كم موظف في جدة؟ ما نسبة السعودة؟")
            if st.button("🔍 تحليل", type="primary", use_container_width=True) and q:
                # Simplified answerer
                a = f"إجمالي الموظفين: {n}\n"
                if has(emp,'القسم'): a += f"الأقسام: {emp['القسم'].nunique()}\n"
                if has(emp,'الموقع'):
                    for l,c in emp['الموقع'].value_counts().items(): a += f"  - {l}: {c}\n"
                st.info(a)

        elif page == "📋 بيانات الموظفين":
            hdr("📋 بيانات الموظفين")
            if n==0: st.info("📁 ارفع ملف"); return
            if all_sheets:
                sn = st.selectbox("الورقة:", list(all_sheets.keys()))
                st.dataframe(all_sheets[sn], use_container_width=True, hide_index=True, height=600)

        elif page == "📥 تصدير":
            hdr("📥 تصدير البيانات")
            if n==0: st.info("📁 ارفع ملف"); return
            o = io.BytesIO()
            with pd.ExcelWriter(o, engine='xlsxwriter') as w:
                for nm, d in all_sheets.items():
                    d.to_excel(w, sheet_name=nm[:31], index=False)
            st.download_button("📥 تحميل", data=o.getvalue(), file_name=f"HR_{datetime.now().strftime('%Y%m%d')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", type="primary", use_container_width=True)


    # =========================================
    #        📚 TRAINING & DEVELOPMENT PAGES
    # =========================================
    elif section == "📚 التدريب والتطوير":

        # Initialize training state
        if 'budget_data' not in st.session_state:
            st.session_state.budget_data = DEFAULT_BUDGET.copy()
        if 'programs' not in st.session_state:
            st.session_state.programs = DEFAULT_PROGRAMS.copy()

        # ======= 📚 TRAINING BUDGET =======
        if page == "📚 ميزانية التدريب":
            hdr("📚 ميزانية التدريب", "خطة توزيع ميزانية التدريب السنوية")

            # Budget Controls
            st.markdown("### ⚙️ إعدادات الميزانية")
            c1, c2, c3 = st.columns(3)
            with c1:
                total_budget = st.number_input("💰 إجمالي الميزانية (ريال)", min_value=10000, max_value=5000000, value=70000, step=5000)
            with c2:
                fiscal_year = st.selectbox("📅 السنة المالية", [2025, 2026, 2027], index=1)
            with c3:
                company_name = st.text_input("🏢 اسم الشركة", value="رسال الود لتقنية المعلومات")

            # Auto-calculate budgets proportionally
            budget_df = pd.DataFrame(st.session_state.budget_data)
            budget_df['budget'] = (budget_df['pct'] / 100 * total_budget).astype(int)

            st.markdown("---")

            # KPI Cards
            st.markdown("### 📊 المؤشرات الرئيسية")
            k1, k2, k3, k4 = st.columns(4)
            with k1: kpi("إجمالي الميزانية", f"{total_budget:,} ريال")
            with k2: kpi("عدد الأقسام", str(len(budget_df)))
            with k3:
                revenue_pct = budget_df[budget_df['cat']=='محرك إيرادات']['budget'].sum() / total_budget * 100
                kpi("نسبة محركات الإيرادات", f"{revenue_pct:.0f}%")
            with k4:
                total_programs = sum(len(v) for v in st.session_state.programs.values())
                kpi("البرامج التدريبية", str(total_programs))

            st.markdown("---")

            # Department Allocation Table
            st.markdown("### 📋 توزيع الميزانية حسب القسم")
            edit_df = budget_df[['dept','budget','pct','priority','fit','cat']].copy()
            edit_df.columns = ['القسم','الميزانية (ريال)','النسبة %','الأولوية','التوافق الاستراتيجي','التصنيف']
            st.dataframe(edit_df, use_container_width=True, hide_index=True)

            # Charts
            c1, c2 = st.columns(2)
            with c1:
                fig = px.pie(budget_df, values='budget', names='dept', title='توزيع الميزانية حسب القسم', hole=.35,
                    color_discrete_sequence=px.colors.qualitative.Set2)
                fig.update_layout(font=dict(family="Noto Sans Arabic"), height=420)
                st.plotly_chart(fig, use_container_width=True)

            with c2:
                cat_df = budget_df.groupby('cat')['budget'].sum().reset_index()
                cat_df.columns = ['التصنيف','الميزانية']
                colors = {'محرك إيرادات':'#0F4C5C','ممكّن نمو':'#E36414','بنية تحتية':'#64748B'}
                fig = px.pie(cat_df, values='الميزانية', names='التصنيف', title='التوزيع الاستراتيجي',
                    color='التصنيف', color_discrete_map=colors, hole=.35)
                fig.update_layout(font=dict(family="Noto Sans Arabic"), height=420)
                st.plotly_chart(fig, use_container_width=True)

            # Quarterly Distribution
            st.markdown("### 📅 الخطة ربع السنوية")
            q_data = []
            for _, row in budget_df.iterrows():
                q_row = {"القسم": row['dept']}
                for q, pct in Q_SPLIT.items():
                    q_row[q] = int(row['budget'] * pct)
                q_row['الإجمالي'] = row['budget']
                q_data.append(q_row)

            q_df = pd.DataFrame(q_data)
            totals = {"القسم": "الإجمالي"}
            for c in ['Q1','Q2','Q3','Q4','الإجمالي']:
                totals[c] = q_df[c].sum()
            q_df = pd.concat([q_df, pd.DataFrame([totals])], ignore_index=True)
            st.dataframe(q_df, use_container_width=True, hide_index=True)

            # Quarterly chart
            q_totals = {q: int(total_budget * pct) for q, pct in Q_SPLIT.items()}
            fig = go.Figure()
            fig.add_trace(go.Bar(x=list(q_totals.keys()), y=list(q_totals.values()),
                marker_color=[CL['primary'], CL['accent'], CL['success'], '#64748B'],
                text=[f"{v:,}" for v in q_totals.values()], textposition='outside'))
            fig.update_layout(title=f'توزيع الميزانية ربع السنوي - {fiscal_year}', font=dict(family="Noto Sans Arabic"), height=350, yaxis_tickformat=',')
            st.plotly_chart(fig, use_container_width=True)

            # Detailed Programs
            st.markdown("### 📋 البرامج التدريبية التفصيلية")
            for dept, programs in st.session_state.programs.items():
                with st.expander(f"📌 {dept} ({sum(p['budget'] for p in programs):,} ريال)"):
                    prog_df = pd.DataFrame(programs)
                    prog_df.columns = ['البرنامج','الميزانية','المصدر','التوقيت','الأثر المتوقع']
                    st.dataframe(prog_df, use_container_width=True, hide_index=True)


        # ======= 💹 TRAINING ROI =======
        elif page == "💹 عائد التدريب ROI":
            hdr("💹 عائد التدريب ROI", "نموذج Phillips ذو 5 مستويات لحساب العائد على الاستثمار في التدريب")

            st.markdown("### ⚙️ بيانات الحساب")
            c1, c2 = st.columns(2)
            with c1:
                roi_budget = st.number_input("💰 ميزانية التدريب (ريال)", value=70000, step=5000)
                current_rev = st.number_input("📈 الإيرادات السنوية الحالية (ريال)", value=5000000, step=100000)
                rev_increase = st.slider("📊 الزيادة المتوقعة في الإيرادات %", 1, 50, 15)
            with c2:
                hc = st.number_input("👥 عدد الموظفين", value=83, step=1)
                avg_sal = st.number_input("💵 متوسط الراتب الشهري (ريال)", value=8000, step=500)
                retention_imp = st.slider("🔄 تحسن الاحتفاظ %", 1, 30, 10)
                prod_gain = st.slider("⚡ مكاسب الإنتاجية %", 1, 30, 10)

            if st.button("📊 حساب ROI", type="primary", use_container_width=True):
                roi = calc_roi(roi_budget, rev_increase, current_rev, retention_imp, avg_sal*12, hc, prod_gain)

                st.markdown("---")
                st.markdown("### 📊 نتائج تحليل ROI (نموذج Phillips)")

                # Level indicators
                st.markdown("#### المستويات الخمسة")
                l1, l2, l3, l4, l5 = st.columns(5)
                with l1: kpi("المستوى 1: رضا المتدربين", f"{roi['satisfaction']}%")
                with l2: kpi("المستوى 2: التعلم", f"{roi['learning']}%")
                with l3: kpi("المستوى 3: التطبيق", f"{roi['application']}%")
                with l4: kpi("المستوى 4: الأثر", f"{roi['total_benefits']:,.0f}")
                with l5: kpi("المستوى 5: ROI", f"{roi['roi_pct']:.0f}%")

                st.markdown("---")

                # Detailed breakdown
                st.markdown("#### 💰 تفصيل العوائد المالية")
                c1, c2 = st.columns(2)
                with c1:
                    benefits_data = pd.DataFrame([
                        {"المصدر": "زيادة الإيرادات", "القيمة (ريال)": roi['revenue_gain']},
                        {"المصدر": "وفورات الاحتفاظ", "القيمة (ريال)": roi['retention_savings']},
                        {"المصدر": "مكاسب الإنتاجية", "القيمة (ريال)": roi['productivity_value']},
                        {"المصدر": "إجمالي العوائد", "القيمة (ريال)": roi['total_benefits']},
                        {"المصدر": "تكلفة التدريب", "القيمة (ريال)": roi_budget},
                        {"المصدر": "صافي العائد", "القيمة (ريال)": roi['total_benefits'] - roi_budget},
                    ])
                    st.dataframe(benefits_data, use_container_width=True, hide_index=True)

                with c2:
                    fig = go.Figure()
                    fig.add_trace(go.Bar(name='العوائد', x=['زيادة الإيرادات','وفورات الاحتفاظ','الإنتاجية'],
                        y=[roi['revenue_gain'], roi['retention_savings'], roi['productivity_value']],
                        marker_color=[CL['primary'], CL['accent'], CL['success']]))
                    fig.add_hline(y=roi_budget, line_dash="dash", line_color="red", annotation_text=f"تكلفة التدريب: {roi_budget:,}")
                    fig.update_layout(title='العوائد مقابل التكلفة', font=dict(family="Noto Sans Arabic"), height=380, yaxis_tickformat=',')
                    st.plotly_chart(fig, use_container_width=True)

                # Summary metrics
                st.markdown("#### 📈 مؤشرات الأداء")
                m1, m2, m3 = st.columns(3)
                with m1: st.metric("📊 ROI", f"{roi['roi_pct']:.0f}%", help="(العوائد - التكلفة) / التكلفة × 100")
                with m2: st.metric("💰 BCR نسبة المنفعة للتكلفة", f"{roi['bcr']:.1f}x", help="كل 1 ريال مستثمر يعود بـ X ريال")
                with m3: st.metric("⏱️ فترة الاسترداد", f"{roi['payback_months']:.1f} شهر")

                # Insights
                st.markdown("#### 🤖 تحليل ذكي")
                if roi['roi_pct'] > 200:
                    ibox(f"عائد ممتاز! كل 1 ريال مستثمر في التدريب يعود بـ {roi['bcr']:.1f} ريال. الاستثمار يسترد في {roi['payback_months']:.0f} شهر فقط.", "success")
                elif roi['roi_pct'] > 100:
                    ibox(f"عائد جيد جداً ({roi['roi_pct']:.0f}%). التدريب يحقق أكثر من ضعف تكلفته.", "success")
                elif roi['roi_pct'] > 0:
                    ibox(f"عائد إيجابي ({roi['roi_pct']:.0f}%). التدريب يحقق ربحاً لكن يُنصح بزيادة التركيز على البرامج ذات الأثر المباشر.", "warning")
                else:
                    ibox(f"العائد سلبي ({roi['roi_pct']:.0f}%). يُنصح بمراجعة البرامج التدريبية والتركيز على ذات الأثر المباشر.", "danger")


        # ======= 📋 TRAINING NEEDS =======
        elif page == "📋 الاحتياجات التدريبية":
            hdr("📋 تحليل الاحتياجات التدريبية", "TNA مع مواءمة جهات التدريب")

            st.markdown("### 🎯 تحديد الاحتياجات")
            st.markdown("اختر الأقسام والمهارات المطلوبة:")

            selected_depts = st.multiselect("📌 الأقسام", list(TRAINING_CATEGORIES.keys()), default=list(TRAINING_CATEGORIES.keys())[:3])

            needs_results = []
            for dept in selected_depts:
                with st.expander(f"📌 {dept}", expanded=True):
                    skills = TRAINING_CATEGORIES[dept]
                    selected_skills = st.multiselect(f"المهارات المطلوبة - {dept}", skills, default=skills[:3], key=f"sk_{dept}")

                    for skill in selected_skills:
                        c1, c2, c3 = st.columns(3)
                        with c1:
                            level = st.select_slider(f"المستوى الحالي: {skill}", ["مبتدئ","أساسي","متوسط","متقدم","خبير"], value="أساسي", key=f"lv_{dept}_{skill}")
                        with c2:
                            target = st.select_slider(f"المستوى المستهدف: {skill}", ["مبتدئ","أساسي","متوسط","متقدم","خبير"], value="متقدم", key=f"tg_{dept}_{skill}")
                        with c3:
                            priority = st.selectbox(f"الأولوية: {skill}", ["حرج","عالي","متوسط","منخفض"], key=f"pr_{dept}_{skill}")

                        needs_results.append({
                            "القسم": dept, "المهارة": skill,
                            "المستوى الحالي": level, "المستوى المستهدف": target,
                            "الفجوة": ["مبتدئ","أساسي","متوسط","متقدم","خبير"].index(target) - ["مبتدئ","أساسي","متوسط","متقدم","خبير"].index(level),
                            "الأولوية": priority
                        })

            if needs_results:
                st.markdown("---")
                st.markdown("### 📊 ملخص تحليل الفجوات")
                needs_df = pd.DataFrame(needs_results)
                st.dataframe(needs_df, use_container_width=True, hide_index=True)

                # Gap visualization
                if len(needs_df) > 0:
                    fig = px.bar(needs_df, x='المهارة', y='الفجوة', color='الأولوية',
                        color_discrete_map={"حرج":"#EF4444","عالي":"#F97316","متوسط":"#3B82F6","منخفض":"#6B7280"},
                        title='خريطة الفجوات التدريبية')
                    fig.update_layout(font=dict(family="Noto Sans Arabic"), height=400)
                    st.plotly_chart(fig, use_container_width=True)

                # Provider matching
                st.markdown("### 🏫 جهات التدريب المقترحة")
                for dept in selected_depts:
                    dept_needs = [n for n in needs_results if n['القسم']==dept and n['الفجوة']>0]
                    if dept_needs:
                        st.markdown(f"**{dept}:**")
                        for market, providers in PROVIDERS.items():
                            matched = [p for p in providers if any(
                                any(sk in p['speciality'] or p['speciality'] in sk for sk in [n['المهارة'] for n in dept_needs])
                                for _ in [1]
                            ) or any(w in p['speciality'] for w in ['متعدد','إدارة','قيادة'])]
                            if matched:
                                for p in matched[:2]:
                                    ibox(f"**{p['name']}** ({market}) - {p['speciality']} | النوع: {p['type']}" + (f" | {p['url']}" if p['url'] else ""))


        # ======= 🏫 PROVIDERS =======
        elif page == "🏫 جهات التدريب":
            hdr("🏫 دليل جهات التدريب", "السوق السعودي والخليجي والمصري")

            market = st.selectbox("🌍 اختر السوق:", list(PROVIDERS.keys()))
            providers = PROVIDERS[market]

            for p in providers:
                c1, c2, c3 = st.columns([3,2,1])
                with c1: st.markdown(f"**{p['name']}**")
                with c2: st.markdown(f"📌 {p['speciality']}")
                with c3: st.markdown(f"🏷️ {p['type']}")
                if p['url']:
                    st.markdown(f"🔗 [{p['url']}](https://{p['url']})")
                st.markdown("---")

            # Statistics
            st.markdown("### 📊 إحصائيات")
            all_p = []
            for m, ps in PROVIDERS.items():
                for p in ps:
                    all_p.append({"السوق": m, "الجهة": p['name'], "النوع": p['type']})
            ap_df = pd.DataFrame(all_p)
            c1, c2 = st.columns(2)
            with c1:
                fig = px.bar(ap_df['السوق'].value_counts().reset_index(), x='السوق', y='count', title='عدد الجهات حسب السوق', color='السوق', color_discrete_sequence=CL['dept'])
                fig.update_layout(font=dict(family="Noto Sans Arabic"), height=350)
                st.plotly_chart(fig, use_container_width=True)
            with c2:
                fig = px.pie(ap_df, names='النوع', title='التوزيع حسب النوع', hole=.3, color_discrete_sequence=CL['dept'])
                fig.update_layout(font=dict(family="Noto Sans Arabic"), height=350)
                st.plotly_chart(fig, use_container_width=True)


        # ======= 📥 EXPORT TRAINING =======
        elif page == "📥 تصدير التدريب":
            hdr("📥 تصدير تقارير التدريب")

            export_format = st.selectbox("📄 صيغة التصدير:", ["Excel (.xlsx)", "CSV (.csv)", "HTML (.html)"])

            reports = {}
            if st.checkbox("📚 ملخص الميزانية", value=True):
                reports['ملخص الميزانية'] = pd.DataFrame(st.session_state.budget_data)
            if st.checkbox("📋 البرامج التفصيلية", value=True):
                all_progs = []
                for dept, progs in st.session_state.programs.items():
                    for p in progs:
                        all_progs.append({"القسم": dept, **p})
                if all_progs:
                    reports['البرامج التفصيلية'] = pd.DataFrame(all_progs)
            if st.checkbox("📅 الخطة ربع السنوية"):
                budget_df = pd.DataFrame(st.session_state.budget_data)
                budget_df['budget'] = (budget_df['pct'] / 100 * 70000).astype(int)
                q_data = []
                for _, row in budget_df.iterrows():
                    q_row = {"القسم": row['dept']}
                    for q, pct in Q_SPLIT.items(): q_row[q] = int(row['budget'] * pct)
                    q_row['الإجمالي'] = row['budget']
                    q_data.append(q_row)
                reports['ربع سنوي'] = pd.DataFrame(q_data)
            if st.checkbox("🏫 جهات التدريب"):
                all_p = []
                for m, ps in PROVIDERS.items():
                    for p in ps: all_p.append({"السوق":m,"الجهة":p['name'],"التخصص":p['speciality'],"النوع":p['type'],"الموقع":p.get('url','')})
                reports['جهات التدريب'] = pd.DataFrame(all_p)

            if reports:
                if "Excel" in export_format:
                    o = io.BytesIO()
                    with pd.ExcelWriter(o, engine='xlsxwriter') as w:
                        for nm, d in reports.items():
                            d.to_excel(w, sheet_name=nm[:31], index=False)
                            w.sheets[nm[:31]].right_to_left()
                    st.download_button("📥 تحميل Excel", data=o.getvalue(),
                        file_name=f"Training_Report_{datetime.now().strftime('%Y%m%d')}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        type="primary", use_container_width=True)
                elif "CSV" in export_format:
                    combined = pd.concat(reports.values(), ignore_index=True)
                    st.download_button("📥 تحميل CSV", data=combined.to_csv(index=False).encode('utf-8-sig'),
                        file_name=f"Training_Report_{datetime.now().strftime('%Y%m%d')}.csv",
                        mime="text/csv", type="primary", use_container_width=True)
                elif "HTML" in export_format:
                    html = "<html dir='rtl'><head><meta charset='utf-8'><style>body{font-family:sans-serif}table{border-collapse:collapse;width:100%;margin:20px 0}th,td{border:1px solid #ddd;padding:8px;text-align:right}th{background:#0F4C5C;color:white}</style></head><body>"
                    html += f"<h1>تقرير التدريب - {datetime.now().strftime('%Y-%m-%d')}</h1>"
                    for nm, d in reports.items():
                        html += f"<h2>{nm}</h2>{d.to_html(index=False)}"
                    html += "</body></html>"
                    st.download_button("📥 تحميل HTML", data=html.encode('utf-8'),
                        file_name=f"Training_Report_{datetime.now().strftime('%Y%m%d')}.html",
                        mime="text/html", type="primary", use_container_width=True)

            else:
                st.warning("اختر تقرير واحد على الأقل")


if __name__ == "__main__":
    main()
