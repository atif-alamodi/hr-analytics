# ===================================================
# منصة تحليلات الموارد البشرية الذكية v5.0
# رسال الود لتقنية المعلومات
# المجموعة أ: تحليل الرواتب + Headcount + حاسبة المستحقات + الأداء
# + المرحلة 2: ميزانية التدريب + ROI + الاحتياجات التدريبية
# ===================================================

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import io, math, json
import openpyxl
from datetime import datetime, date
from dateutil.relativedelta import relativedelta

st.set_page_config(page_title="تحليلات HR | رسال الود", page_icon="📊", layout="wide", initial_sidebar_state="expanded")

# ===== STYLES =====
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Noto+Sans+Arabic:wght@300;400;500;600;700;800&display=swap');
*{font-family:'Noto Sans Arabic',sans-serif}
.main .block-container{padding-top:.8rem;max-width:1400px}
[data-testid="stSidebar"]{background:linear-gradient(180deg,#0F4C5C 0%,#1A1A2E 100%)}
[data-testid="stSidebar"] *{color:white !important}
[data-testid="stMetric"]{background:white;border-radius:12px;padding:14px 18px;box-shadow:0 1px 3px rgba(0,0,0,.06);border:1px solid #E2E8F0}
[data-testid="stMetric"] label{font-size:12px !important;color:#64748B !important}
[data-testid="stMetric"] [data-testid="stMetricValue"]{font-size:20px !important;font-weight:700 !important}
h1{color:#0F4C5C !important;font-weight:800 !important}
.hdr{background:linear-gradient(135deg,#0F4C5C,#1A1A2E);padding:20px 28px;border-radius:14px;margin-bottom:20px;color:white}
.hdr h1{color:white !important;margin:0;font-size:24px}
.hdr p{color:rgba(255,255,255,.7);margin:4px 0 0;font-size:13px}
.ibox{background:#EFF6FF;border-radius:10px;padding:12px 16px;border-right:4px solid #3B82F6;margin-bottom:8px;font-size:13px;line-height:1.7}
.ibox.warn{background:#FFF7ED;border-right-color:#F97316}
.ibox.ok{background:#F0FDF4;border-right-color:#22C55E}
.ibox.bad{background:#FEF2F2;border-right-color:#EF4444}
.kpi{background:linear-gradient(135deg,#0F4C5C,#1B4D5C);color:white;border-radius:12px;padding:16px;text-align:center;margin-bottom:10px}
.kpi h3{font-size:24px;margin:6px 0 2px;font-weight:800}
.kpi p{font-size:11px;opacity:.7;margin:0}
#MainMenu,footer{visibility:hidden}
</style>
""", unsafe_allow_html=True)

CL = {'p':'#0F4C5C','a':'#E36414','s':'#2D6A4F','d':'#9A031E','dept':px.colors.qualitative.Set2,'sal':px.colors.qualitative.Pastel}

def hdr(t,s=""): st.markdown(f'<div class="hdr"><h1>{t}</h1><p>{s}</p></div>',unsafe_allow_html=True)
def ibox(t,tp="info"):
    c={"info":"ibox","warning":"ibox warn","success":"ibox ok","danger":"ibox bad"}
    ic={"info":"💡","warning":"⚠️","success":"✅","danger":"🚨"}
    st.markdown(f'<div class="{c.get(tp,"ibox")}">{ic.get(tp,"💡")} {t}</div>',unsafe_allow_html=True)
def kpi(l,v): st.markdown(f'<div class="kpi"><p>{l}</p><h3>{v}</h3></div>',unsafe_allow_html=True)
def fmt(v): return f"{v:,.0f}"
def has(df,n): return df is not None and n in df.columns and len(df)>0

# ===== DATA LOADER =====
COL_MAP = {
    'emp id':'رقم الموظف','employee id':'رقم الموظف','name (english)':'الاسم الإنجليزي',
    'name (arabic)':'الاسم','name':'الاسم','department':'القسم','division':'القطاع',
    'job title':'المسمى الوظيفي','position':'المسمى الوظيفي','join date':'تاريخ التعيين',
    'hiring date':'تاريخ التعيين','location':'الموقع','city':'الموقع',
    'tenure (yrs)':'سنوات الخدمة','basic salary':'الراتب الأساسي',
    'nationality group':'الجنسية','nationality':'الجنسية','gender':'الجنس',
    'gross salary':'الراتب الإجمالي','net salary':'صافي الراتب',
    'housing allowance':'بدل السكن','transportation allowance':'بدل النقل',
    'grade':'الدرجة','level':'المستوى','age':'العمر','age group':'الفئة العمرية',
    'generation':'الجيل','employment type':'نوع التوظيف',
    'salary month':'شهر الراتب','quarter':'الربع','salary year':'سنة الراتب',
    'gosi deduction':'خصم التأمينات','overtime hours':'ساعات إضافية',
    'overtime cost':'تكلفة الإضافي','special allowance':'بدل خاص',
    'mobile allowance':'بدل جوال','living cost  allowance':'بدل معيشة',
    'salary range':'شريحة الراتب','other deduction':'خصومات أخرى',
    'hourly rate (total salary)':'سعر الساعة الإجمالي',
    'gross salary with overtime':'الإجمالي مع الإضافي',
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
    return df.rename(columns=new)


# ===== END-OF-SERVICE CALCULATOR (Saudi Labor Law Art 84/85) =====
def calc_eos(monthly_salary, start_date, end_date, is_resignation=False):
    """حاسبة مستحقات نهاية الخدمة - نظام العمل السعودي"""
    delta = relativedelta(end_date, start_date)
    total_days = (end_date - start_date).days
    total_years = total_days / 365.25

    # Article 84: Base calculation
    if total_years <= 5:
        eos_84 = (monthly_salary / 2) * total_years
    else:
        first_5 = (monthly_salary / 2) * 5
        remaining = monthly_salary * (total_years - 5)
        eos_84 = first_5 + remaining

    # Article 85: Resignation adjustments
    if is_resignation:
        if total_years < 2:
            eos_final = 0
            eos_pct = 0
            note = "لا يستحق مكافأة (أقل من سنتين)"
        elif total_years < 5:
            eos_final = eos_84 / 3
            eos_pct = 33.3
            note = "ثلث المكافأة (من 2 إلى 5 سنوات)"
        elif total_years < 10:
            eos_final = eos_84 * 2 / 3
            eos_pct = 66.7
            note = "ثلثا المكافأة (من 5 إلى 10 سنوات)"
        else:
            eos_final = eos_84
            eos_pct = 100
            note = "المكافأة كاملة (أكثر من 10 سنوات)"
    else:
        eos_final = eos_84
        eos_pct = 100
        note = "المكافأة كاملة (إنهاء من صاحب العمل / انتهاء العقد)"

    # Vacation balance calculation (21 days for first 5 years, 30 after)
    daily_salary = monthly_salary / 30
    vac_days_per_year = 30 if total_years >= 5 else 21

    return {
        "years": delta.years, "months": delta.months, "days": delta.days,
        "total_years": round(total_years, 2),
        "total_days": total_days,
        "eos_art84": round(eos_84, 2),
        "eos_final": round(eos_final, 2),
        "eos_pct": eos_pct,
        "note": note,
        "daily_salary": round(daily_salary, 2),
        "vac_days_per_year": vac_days_per_year,
        "is_resignation": is_resignation,
    }


# ===== TRAINING DATA (from v4) =====
PROVIDERS = {
    "السعودية": [
        {"name":"معهد الإدارة العامة","spec":"الإدارة والقيادة","type":"حكومي","url":"ipa.edu.sa"},
        {"name":"غرفة جدة","spec":"المهارات المهنية","type":"شبه حكومي","url":"jcci.org.sa"},
        {"name":"KPMG Academy","spec":"المالية والمحاسبة","type":"خاص","url":"kpmg.com/sa"},
        {"name":"PwC Academy","spec":"التحول الرقمي","type":"خاص","url":"pwcacademy.me"},
        {"name":"Misk Academy","spec":"التقنية والابتكار","type":"غير ربحي","url":"misk.org.sa"},
        {"name":"بكه للتعليم","spec":"إدارة المشاريع","type":"خاص","url":"bakkah.com"},
        {"name":"BIBF","spec":"الخدمات المالية","type":"خاص","url":"bibf.com"},
        {"name":"Udacity MENA","spec":"AI والبيانات","type":"أونلاين","url":"udacity.com"},
    ],
    "الخليج": [
        {"name":"Informa Connect","spec":"القيادة","type":"خاص","url":"informaconnect.com"},
        {"name":"London Business School ME","spec":"MBA","type":"خاص","url":"lbs.ac.uk"},
        {"name":"Dale Carnegie UAE","spec":"المهارات القيادية","type":"خاص","url":"dalecarnegie.com"},
    ],
    "مصر": [
        {"name":"الجامعة الأمريكية بالقاهرة","spec":"إدارة الأعمال","type":"أكاديمي","url":"aucegypt.edu"},
        {"name":"Sprints","spec":"البرمجة والتقنية","type":"خاص","url":"sprints.ai"},
        {"name":"Digital Egypt Pioneers","spec":"التحول الرقمي","type":"حكومي","url":"mcit.gov.eg"},
    ],
    "أونلاين": [
        {"name":"Coursera for Business","spec":"متعدد","type":"أونلاين","url":"coursera.org"},
        {"name":"LinkedIn Learning","spec":"مهارات مهنية","type":"أونلاين","url":"linkedin.com/learning"},
        {"name":"Google Certificates","spec":"التقنية","type":"أونلاين","url":"grow.google"},
    ]
}

DEFAULT_BUDGET = [
    {"dept":"المبيعات","budget":16000,"pct":22.9,"priority":"حرج","cat":"محرك إيرادات"},
    {"dept":"التسويق","budget":13000,"pct":18.6,"priority":"حرج","cat":"محرك إيرادات"},
    {"dept":"تطوير الأعمال","budget":11000,"pct":15.7,"priority":"عالي","cat":"محرك إيرادات"},
    {"dept":"عمليات المنتجات","budget":9000,"pct":12.9,"priority":"عالي","cat":"ممكّن نمو"},
    {"dept":"البيانات والذكاء","budget":7000,"pct":10.0,"priority":"عالي","cat":"ممكّن نمو"},
    {"dept":"المالية","budget":5000,"pct":7.1,"priority":"متوسط","cat":"بنية تحتية"},
    {"dept":"الموارد البشرية","budget":4000,"pct":5.7,"priority":"متوسط","cat":"بنية تحتية"},
    {"dept":"الحوكمة","budget":3000,"pct":4.3,"priority":"متوسط","cat":"بنية تحتية"},
    {"dept":"القانونية","budget":2000,"pct":2.9,"priority":"أساسي","cat":"بنية تحتية"},
]

Q_SPLIT = {"Q1":0.35,"Q2":0.30,"Q3":0.20,"Q4":0.15}

def calc_roi(budget, rev_inc_pct, current_rev, ret_pct, avg_sal, hc, prod_pct):
    rev_gain = current_rev * rev_inc_pct / 100
    ret_save = ret_pct / 100 * hc * avg_sal * 0.5
    prod_val = prod_pct / 100 * hc * avg_sal * 0.1
    total = rev_gain + ret_save + prod_val
    return {"rev":rev_gain,"ret":ret_save,"prod":prod_val,"total":total,
            "roi":((total-budget)/max(budget,1))*100,"bcr":total/max(budget,1),
            "payback":budget/max(total/12,1)}


# ===== ACCESS CONTROL SYSTEM =====
import hashlib

def hash_pw(pw):
    return hashlib.sha256(pw.encode()).hexdigest()

# Default users (can be managed in-app)
DEFAULT_USERS = {
    "admin": {"password": hash_pw("admin123"), "role": "مدير", "name": "مدير النظام", "sections": "all"},
    "analyst": {"password": hash_pw("analyst123"), "role": "محلل", "name": "محلل البيانات",
        "sections": "📊 التحليلات العامة,💰 تحليل الرواتب,👥 Headcount,🔍 التحليل العام,📤 التقارير والتصدير"},
    "viewer": {"password": hash_pw("viewer123"), "role": "عارض", "name": "عارض",
        "sections": "📊 التحليلات العامة,📤 التقارير والتصدير"},
}

ROLE_DESCRIPTIONS = {
    "مدير": "وصول كامل لجميع الأقسام + إدارة المستخدمين",
    "محلل": "وصول للتحليلات والتقارير بدون إدارة المستخدمين",
    "عارض": "عرض التقارير فقط بدون تعديل",
}

ALL_SECTIONS = ["📊 التحليلات العامة","💰 تحليل الرواتب","👥 Headcount","⚖️ حاسبة المستحقات",
    "📚 التدريب والتطوير","🎯 التوظيف","🔍 التحليل العام","📝 الاستبيانات","🧠 اختبارات الشخصية","📤 التقارير والتصدير"]

def init_users():
    if 'users_db' not in st.session_state:
        st.session_state.users_db = DEFAULT_USERS.copy()

def login_page():
    st.markdown("<div style='text-align:center;padding:40px 0;'><div style='background:linear-gradient(135deg,#E36414,#E9C46A);width:80px;height:80px;border-radius:16px;display:flex;align-items:center;justify-content:center;margin:0 auto 16px;font-size:32px;font-weight:800;color:white;'>HR</div><h1 style='color:#0F4C5C;'>منصة تحليلات الموارد البشرية</h1><p style='color:#64748B;'>رسال الود لتقنية المعلومات</p></div>", unsafe_allow_html=True)
    col1, col2, col3 = st.columns([1,2,1])
    with col2:
        st.markdown("### 🔐 تسجيل الدخول")
        username = st.text_input("اسم المستخدم:", key="login_user")
        password = st.text_input("كلمة المرور:", type="password", key="login_pass")
        lc1, lc2 = st.columns(2)
        with lc1:
            if st.button("🔓 دخول", type="primary", use_container_width=True):
                init_users()
                users = st.session_state.users_db
                if username in users and users[username]["password"] == hash_pw(password):
                    st.session_state.logged_in = True
                    st.session_state.current_user = username
                    st.session_state.user_role = users[username]["role"]
                    st.session_state.user_name = users[username]["name"]
                    st.session_state.user_sections = users[username]["sections"]
                    st.rerun()
                else:
                    st.error("❌ اسم المستخدم أو كلمة المرور غير صحيحة")
        with lc2:
            if st.button("👤 دخول بدون حساب", use_container_width=True):
                st.session_state.logged_in = True
                st.session_state.current_user = "guest"
                st.session_state.user_role = "عارض"
                st.session_state.user_name = "زائر"
                st.session_state.user_sections = "all"
                st.rerun()

        st.markdown("---")
        with st.expander("📋 الحسابات الافتراضية"):
            st.markdown("| المستخدم | كلمة المرور | الدور |")
            st.markdown("|---|---|---|")
            st.markdown("| admin | admin123 | مدير |")
            st.markdown("| analyst | analyst123 | محلل |")
            st.markdown("| viewer | viewer123 | عارض |")

def check_section_access(section_name):
    if not st.session_state.get('logged_in'): return False
    user_sections = st.session_state.get('user_sections', 'all')
    if user_sections == "all": return True
    return section_name in user_sections

def user_management_page():
    hdr("👥 إدارة المستخدمين والصلاحيات", "إضافة وتعديل المستخدمين وصلاحياتهم")
    init_users()

    if st.session_state.get('user_role') != "مدير":
        st.warning("⚠️ هذه الصفحة متاحة للمدير فقط")
        return

    # Current users
    st.markdown("### 📋 المستخدمين الحاليين")
    users = st.session_state.users_db
    user_rows = []
    for uname, udata in users.items():
        user_rows.append({"المستخدم": uname, "الاسم": udata["name"], "الدور": udata["role"],
            "الأقسام": "جميع الأقسام" if udata["sections"]=="all" else udata["sections"]})
    st.dataframe(pd.DataFrame(user_rows), use_container_width=True, hide_index=True)

    # Add new user
    st.markdown("### ➕ إضافة مستخدم جديد")
    uc1, uc2 = st.columns(2)
    with uc1:
        new_user = st.text_input("اسم المستخدم:", key="nu_user")
        new_pass = st.text_input("كلمة المرور:", type="password", key="nu_pass")
        new_name = st.text_input("الاسم الكامل:", key="nu_name")
    with uc2:
        new_role = st.selectbox("الدور:", list(ROLE_DESCRIPTIONS.keys()), key="nu_role")
        st.info(f"📋 {ROLE_DESCRIPTIONS[new_role]}")
        if new_role == "مدير":
            new_sections = "all"
        else:
            new_sections_list = st.multiselect("الأقسام المتاحة:", ALL_SECTIONS, default=ALL_SECTIONS[:3], key="nu_sec")
            new_sections = ",".join(new_sections_list) if new_sections_list else "all"

    if st.button("➕ إضافة المستخدم", type="primary", key="nu_btn"):
        if new_user and new_pass and new_name:
            st.session_state.users_db[new_user] = {
                "password": hash_pw(new_pass), "role": new_role,
                "name": new_name, "sections": new_sections}
            st.success(f"✅ تم إضافة {new_name} بدور {new_role}")
            st.rerun()
        else:
            st.error("يرجى تعبئة جميع الحقول")

    # Delete user
    st.markdown("### 🗑️ حذف مستخدم")
    del_user = st.selectbox("اختر المستخدم:", [u for u in users.keys() if u != st.session_state.current_user], key="del_u")
    if st.button("🗑️ حذف", key="del_btn"):
        if del_user in st.session_state.users_db:
            del st.session_state.users_db[del_user]
            st.success(f"✅ تم حذف {del_user}")
            st.rerun()

# ===== SURVEY TEMPLATES =====
SURVEY_TEMPLATES = {
    "رضا الموظفين": {
        "description": "استبيان شامل لقياس مستوى رضا الموظفين عن بيئة العمل",
        "questions": [
            {"q": "أشعر بالرضا عن عملي بشكل عام", "cat": "الرضا العام"},
            {"q": "أحصل على تقدير كافٍ لإنجازاتي", "cat": "التقدير"},
            {"q": "لدي فرص كافية للتطور المهني", "cat": "التطور"},
            {"q": "العلاقة مع مديري المباشر جيدة", "cat": "الإدارة"},
            {"q": "بيئة العمل مريحة ومحفزة", "cat": "بيئة العمل"},
            {"q": "الراتب والمزايا عادلة مقارنة بالسوق", "cat": "التعويضات"},
            {"q": "أشعر بالانتماء للشركة", "cat": "الانتماء"},
            {"q": "التواصل الداخلي في الشركة فعّال", "cat": "التواصل"},
            {"q": "لدي توازن جيد بين العمل والحياة الشخصية", "cat": "التوازن"},
            {"q": "أوصي بالعمل في هذه الشركة للآخرين", "cat": "التوصية"},
        ]
    },
    "بيئة العمل": {
        "description": "تقييم بيئة العمل المادية والتنظيمية",
        "questions": [
            {"q": "المكتب والمرافق مجهزة بشكل جيد", "cat": "المرافق"},
            {"q": "الأدوات والتقنيات المتاحة كافية لأداء العمل", "cat": "الأدوات"},
            {"q": "إجراءات السلامة المهنية مطبقة", "cat": "السلامة"},
            {"q": "ساعات العمل مناسبة", "cat": "ساعات العمل"},
            {"q": "الإضاءة والتهوية مناسبة", "cat": "البيئة المادية"},
            {"q": "مساحة العمل كافية ومريحة", "cat": "المساحة"},
            {"q": "الضوضاء في بيئة العمل مقبولة", "cat": "البيئة المادية"},
            {"q": "خدمات الطعام والمشروبات متاحة", "cat": "الخدمات"},
        ]
    },
    "المشاركة والالتزام": {
        "description": "قياس مستوى مشاركة الموظفين والتزامهم التنظيمي",
        "questions": [
            {"q": "أبذل جهداً إضافياً عندما يتطلب العمل ذلك", "cat": "الالتزام"},
            {"q": "أشعر بالحماس تجاه عملي اليومي", "cat": "الحماس"},
            {"q": "أفهم أهداف الشركة وأساهم في تحقيقها", "cat": "التوافق"},
            {"q": "أشارك بفعالية في اجتماعات الفريق", "cat": "المشاركة"},
            {"q": "أقدم أفكاراً ومقترحات لتحسين العمل", "cat": "المبادرة"},
            {"q": "أشعر أن عملي له قيمة وتأثير", "cat": "القيمة"},
            {"q": "أتعاون بشكل جيد مع زملائي", "cat": "التعاون"},
            {"q": "أفتخر بالعمل في هذه الشركة", "cat": "الفخر"},
        ]
    }
}

# ===== BIG FIVE QUESTIONS =====
BIG5_QUESTIONS = [
    {"q": "أستمتع بالتفاعل مع مجموعات كبيرة من الناس", "trait": "الانبساطية", "d": 1},
    {"q": "أبادر ببدء المحادثات مع الغرباء", "trait": "الانبساطية", "d": 1},
    {"q": "أفضل العمل بمفردي على العمل الجماعي", "trait": "الانبساطية", "d": -1},
    {"q": "أشعر بالطاقة في الأماكن الاجتماعية", "trait": "الانبساطية", "d": 1},
    {"q": "أهتم بمشاعر الآخرين وأتعاطف معهم", "trait": "القبول", "d": 1},
    {"q": "أثق في نوايا الآخرين بسهولة", "trait": "القبول", "d": 1},
    {"q": "أسعى لمساعدة الآخرين حتى لو لم يطلبوا", "trait": "القبول", "d": 1},
    {"q": "أتجنب الصراعات والمواجهات", "trait": "القبول", "d": 1},
    {"q": "أنظم مهامي وأخطط مسبقاً بعناية", "trait": "الإتقان", "d": 1},
    {"q": "ألتزم بالمواعيد النهائية دائماً", "trait": "الإتقان", "d": 1},
    {"q": "أهتم بالتفاصيل الدقيقة في عملي", "trait": "الإتقان", "d": 1},
    {"q": "أتبع القواعد والإجراءات المحددة", "trait": "الإتقان", "d": 1},
    {"q": "أشعر بالقلق أو التوتر بسهولة", "trait": "العصابية", "d": 1},
    {"q": "تتقلب مشاعري بشكل كبير", "trait": "العصابية", "d": 1},
    {"q": "أجد صعوبة في التعامل مع الضغوط", "trait": "العصابية", "d": 1},
    {"q": "أميل للتفكير السلبي في المواقف الصعبة", "trait": "العصابية", "d": 1},
    {"q": "أحب تجربة أشياء جديدة وغير مألوفة", "trait": "الانفتاح", "d": 1},
    {"q": "أستمتع بالأفكار المجردة والفلسفية", "trait": "الانفتاح", "d": 1},
    {"q": "أقدّر الفن والجمال والإبداع", "trait": "الانفتاح", "d": 1},
    {"q": "أفضل الروتين والأساليب المجربة", "trait": "الانفتاح", "d": -1},
]

BIG5_TRAITS = {
    "الانبساطية": {"en": "Extraversion", "color": "#E36414", "desc": "مستوى الطاقة الاجتماعية والحماس"},
    "القبول": {"en": "Agreeableness", "color": "#2D6A4F", "desc": "التعاون والثقة والتعاطف مع الآخرين"},
    "الإتقان": {"en": "Conscientiousness", "color": "#0F4C5C", "desc": "التنظيم والانضباط والمسؤولية"},
    "العصابية": {"en": "Neuroticism", "color": "#9A031E", "desc": "الاستقرار العاطفي ومقاومة الضغوط"},
    "الانفتاح": {"en": "Openness", "color": "#7209B7", "desc": "حب الاستكشاف والإبداع والتجديد"},
}

# ===== DISC QUESTIONS =====
DISC_QUESTIONS = [
    {"q": "أحب اتخاذ القرارات بسرعة وحسم", "style": "D"},
    {"q": "أسعى لتحقيق النتائج بأي طريقة", "style": "D"},
    {"q": "أتحدى الوضع الراهن وأسعى للتغيير", "style": "D"},
    {"q": "أستمتع بالمنافسة والفوز", "style": "D"},
    {"q": "أحب قيادة الآخرين وتوجيههم", "style": "D"},
    {"q": "أستمتع بإقناع الآخرين بأفكاري", "style": "I"},
    {"q": "أحب العمل ضمن فريق والتعاون", "style": "I"},
    {"q": "أنا متفائل وأرى الجانب الإيجابي", "style": "I"},
    {"q": "أحب بيئة العمل المرحة والاجتماعية", "style": "I"},
    {"q": "أجيد التواصل والتحدث أمام الآخرين", "style": "I"},
    {"q": "أفضل الاستقرار والأمان في العمل", "style": "S"},
    {"q": "أصبر على المهام الروتينية والمتكررة", "style": "S"},
    {"q": "أدعم زملائي وأساعدهم دائماً", "style": "S"},
    {"q": "أفضل التغيير التدريجي على التغيير المفاجئ", "style": "S"},
    {"q": "أستمع أكثر مما أتحدث", "style": "S"},
    {"q": "أهتم بالدقة والجودة في كل شيء", "style": "C"},
    {"q": "أفضل اتباع القواعد والإجراءات المحددة", "style": "C"},
    {"q": "أحلل البيانات والمعلومات قبل اتخاذ القرار", "style": "C"},
    {"q": "أسعى للكمال في عملي", "style": "C"},
    {"q": "أفضل العمل المنظم والمهيكل", "style": "C"},
]

DISC_STYLES = {
    "D": {"name": "القيادة (Dominance)", "color": "#E74C3C", "desc": "حاسم، تنافسي، يركز على النتائج", "strengths": "اتخاذ القرارات، حل المشكلات، القيادة", "challenges": "الصبر، التعاطف، التفويض"},
    "I": {"name": "التأثير (Influence)", "color": "#F39C12", "desc": "متحمس، اجتماعي، ملهم", "strengths": "التواصل، التحفيز، بناء العلاقات", "challenges": "التنظيم، المتابعة، التركيز"},
    "S": {"name": "الثبات (Steadiness)", "color": "#27AE60", "desc": "صبور، داعم، مستقر", "strengths": "الاستماع، العمل الجماعي، الاستقرار", "challenges": "التكيف مع التغيير، المبادرة، الحسم"},
    "C": {"name": "الالتزام (Conscientiousness)", "color": "#2980B9", "desc": "دقيق، تحليلي، منظم", "strengths": "الجودة، التحليل، الدقة", "challenges": "المرونة، السرعة، التواصل العاطفي"},
}


# ===== MAIN APP =====
def main():
    # Auth check
    if 'logged_in' not in st.session_state:
        st.session_state.logged_in = False

    if not st.session_state.logged_in:
        login_page()
        return

    init_users()

    # Sidebar
    with st.sidebar:
        st.markdown(f"<div style='text-align:center;padding:16px 0;'><div style='background:linear-gradient(135deg,#E36414,#E9C46A);width:56px;height:56px;border-radius:12px;display:flex;align-items:center;justify-content:center;margin:0 auto 10px;font-size:22px;font-weight:800;color:white;'>HR</div><h2 style='margin:0;font-size:16px;'>تحليلات الموارد البشرية</h2><p style='opacity:.6;font-size:11px;'>رسال الود لتقنية المعلومات v5</p><p style='opacity:.8;font-size:11px;'>👤 {st.session_state.user_name} ({st.session_state.user_role})</p></div>", unsafe_allow_html=True)
        st.markdown("---")

        # Filter sections by access
        available_sections = [s for s in ALL_SECTIONS if check_section_access(s)]
        if st.session_state.user_role == "مدير":
            available_sections.append("👥 إدارة المستخدمين")


        section = st.radio("📂", available_sections, label_visibility="collapsed")
        st.markdown("---")

        if section == "📊 التحليلات العامة":
            page = st.radio("📌", ["🏠 نظرة عامة","📊 الأقسام","🤖 المحلل الذكي","📋 البيانات"], label_visibility="collapsed")
        elif section == "💰 تحليل الرواتب":
            page = st.radio("📌", ["💰 لوحة الرواتب","📈 تحليل شهري/ربعي","🏷️ تحليل حسب الفئات","📊 سلم الرواتب","📥 تصدير الرواتب"], label_visibility="collapsed")
        elif section == "👥 Headcount":
            page = st.radio("📌", ["👥 Headcount Planning","📊 تحليل الأداء"], label_visibility="collapsed")
        elif section == "⚖️ حاسبة المستحقات":
            page = "⚖️ حاسبة المستحقات"
        elif section == "🎯 التوظيف":
            page = st.radio("📌", ["📋 تخطيط التوظيف","📊 متابعة التوظيف","📥 تصدير التوظيف"], label_visibility="collapsed")
        elif section == "🔍 التحليل العام":
            page = st.radio("📌", ["📊 تحليل تلقائي","🤖 أسئلة ذكية"], label_visibility="collapsed")
        elif section == "📝 الاستبيانات":
            page = st.radio("📌", ["📋 قوالب جاهزة","🔨 بناء استبيان","📊 تحليل النتائج","📥 تصدير الاستبيانات"], label_visibility="collapsed")
        elif section == "🧠 اختبارات الشخصية":
            page = st.radio("📌", ["🧠 Big Five","💎 DISC","📊 تقارير الشخصية","📥 تصدير الاختبارات"], label_visibility="collapsed")
        elif section == "📤 التقارير والتصدير":
            page = st.radio("📌", ["📄 تقرير PDF","📝 تقرير Word","📊 تقرير شامل"], label_visibility="collapsed")
        elif section == "👥 إدارة المستخدمين":
            page = "👥 إدارة المستخدمين"
        else:
            page = st.radio("📌", ["📚 ميزانية التدريب","💹 ROI التدريب","📋 الاحتياجات التدريبية","🏫 جهات التدريب","📥 تصدير التدريب"], label_visibility="collapsed")

        # Logout button
        st.markdown("---")
        if st.button("🚪 تسجيل الخروج", use_container_width=True):
            for key in ['logged_in','current_user','user_role','user_name','user_sections']:
                st.session_state.pop(key, None)
            st.rerun()

        st.markdown("---")
        st.markdown("##### 📁 ملف البيانات")
        file = st.file_uploader("ارفع Excel", type=["xlsx","xls","csv"], label_visibility="collapsed", key="main_uploader")
        if file:
            # Store file bytes in session_state immediately
            st.session_state['uploaded_file_name'] = file.name
            st.session_state['uploaded_file_bytes'] = file.getvalue()
            st.success(f"✅ {file.name}")
        elif 'uploaded_file_name' in st.session_state:
            st.info(f"📂 {st.session_state['uploaded_file_name']}")
            if st.button("🗑️ إزالة الملف", use_container_width=True):
                for k in ['uploaded_file_name','uploaded_file_bytes','_parsed_cache_key','_parsed_emp','_parsed_sal','_parsed_sheets']:
                    st.session_state.pop(k, None)
                st.rerun()


    # ===== LOAD DATA =====
    emp = pd.DataFrame()
    sal_df = pd.DataFrame()
    all_sheets = {}

    # Use session_state data if file not currently in uploader
    file_bytes = None
    file_name = None
    if file:
        file_bytes = file.getvalue()
        file_name = file.name
    elif 'uploaded_file_bytes' in st.session_state:
        file_bytes = st.session_state['uploaded_file_bytes']
        file_name = st.session_state.get('uploaded_file_name', 'data.xlsx')

    if file_bytes:
        # Check if we already parsed this file (same name + size)
        cache_key = f"{file_name}_{len(file_bytes)}"
        if st.session_state.get('_parsed_cache_key') == cache_key and '_parsed_emp' in st.session_state:
            emp = st.session_state['_parsed_emp']
            sal_df = st.session_state.get('_parsed_sal', pd.DataFrame())
            all_sheets = st.session_state.get('_parsed_sheets', {})
        else:
            try:
                if file_name.endswith('.csv'):
                    emp = norm_cols(pd.read_csv(io.BytesIO(file_bytes)))
                else:
                    xl = pd.ExcelFile(io.BytesIO(file_bytes))
                    for s in xl.sheet_names:
                        try:
                            df_s = smart_read(xl, s)
                            if len(df_s) > 500 and any(c.lower() in ['salary month','gross salary','شهر الراتب'] for c in df_s.columns):
                                sal_df = norm_cols(df_s)
                            df_s = norm_cols(df_s)
                            all_sheets[s] = df_s
                            if len(emp)==0 and len(df_s)>5:
                                name_cols = [c for c in df_s.columns if any(x in str(c).lower() for x in ['name','اسم','emp','موظف'])]
                                if name_cols: emp = df_s
                        except: pass
                    if len(emp)==0 and all_sheets: emp = list(all_sheets.values())[0]

                    if 'Salary Scale' in xl.sheet_names:
                        try: all_sheets['Salary Scale'] = pd.read_excel(xl, 'Salary Scale', header=0)
                        except: pass
                    if 'Positions' in xl.sheet_names:
                        try: all_sheets['Positions'] = pd.read_excel(xl, 'Positions', header=0)
                        except: pass
            except: pass

            # Cache parsed results
            st.session_state['_parsed_cache_key'] = cache_key
            st.session_state['_parsed_emp'] = emp
            st.session_state['_parsed_sal'] = sal_df
            st.session_state['_parsed_sheets'] = all_sheets

    if '#' in emp.columns and len(emp)>0:
        emp = emp[pd.to_numeric(emp['#'], errors='coerce').notna()].reset_index(drop=True)

    n = len(emp)

    # If salary data found, also create a snapshot (latest month)
    sal_snapshot = pd.DataFrame()
    if len(sal_df) > 0:
        if has(sal_df, 'سنة الراتب'):
            latest_year = sal_df['سنة الراتب'].max()
            yr_data = sal_df[sal_df['سنة الراتب']==latest_year]
            if has(yr_data, 'شهر الراتب'):
                months_order = ['January','February','March','April','May','June','July','August','September','October','November','December']
                available = yr_data['شهر الراتب'].unique()
                for m in reversed(months_order):
                    if m in available:
                        sal_snapshot = yr_data[yr_data['شهر الراتب']==m]
                        break
        if len(sal_snapshot)==0:
            sal_snapshot = sal_df.drop_duplicates(subset=['الاسم'] if has(sal_df,'الاسم') else sal_df.columns[0], keep='last')


    # =========================================
    #            📊 GENERAL ANALYTICS
    # =========================================
    if section == "📊 التحليلات العامة":
        if page == "🏠 نظرة عامة":
            hdr("📊 نظرة عامة","ملخص شامل لبيانات القوى العاملة")
            if n==0 and len(sal_df)==0:
                st.info("📁 ارفع ملف بيانات الموظفين أو ملف الرواتب من القائمة الجانبية")
                return

            data = sal_snapshot if len(sal_snapshot)>0 else emp
            total = len(data)

            cols = st.columns(4)
            with cols[0]: st.metric("👥 الموظفين", total)
            with cols[1]:
                dept_col = 'القسم' if has(data,'القسم') else ('القطاع' if has(data,'القطاع') else None)
                st.metric("🏢 الأقسام/القطاعات", data[dept_col].nunique() if dept_col else '-')
            with cols[2]:
                if has(data,'الجنسية'):
                    sa = data[data['الجنسية'].isin(['Saudi','سعودي','سعودية'])]
                    st.metric("🇸🇦 السعودة", f"{round(len(sa)/max(total,1)*100,1)}%")
                elif has(data,'الموقع'):
                    sa = data[data['الموقع'].isin(['Jeddah','Riyadh','جدة','الرياض'])]
                    st.metric("📍 في السعودية", f"{round(len(sa)/max(total,1)*100,1)}%")
                else: st.metric("📋 الأوراق", len(all_sheets))
            with cols[3]:
                if has(data,'الراتب الإجمالي'): st.metric("💰 إجمالي الرواتب", f"{data['الراتب الإجمالي'].sum():,.0f}")
                elif has(data,'الراتب الأساسي'): st.metric("💰 متوسط الراتب", f"{data['الراتب الأساسي'].mean():,.0f}")
                elif has(data,'سنوات الخدمة'): st.metric("📅 متوسط الخدمة", f"{data['سنوات الخدمة'].mean():.1f}")
                else: st.metric("📋 أعمدة", len(data.columns))

            if dept_col:
                c1,c2 = st.columns(2)
                with c1:
                    dc = data[dept_col].value_counts().reset_index(); dc.columns=[dept_col,'العدد']
                    fig = px.pie(dc, values='العدد', names=dept_col, title=f'توزيع الموظفين حسب {dept_col}', hole=.4, color_discrete_sequence=CL['dept'])
                    fig.update_layout(font=dict(family="Noto Sans Arabic"),height=380); st.plotly_chart(fig,use_container_width=True)
                with c2:
                    if has(data,'الجنسية'):
                        nc = data['الجنسية'].value_counts().reset_index(); nc.columns=['الجنسية','العدد']
                        fig = px.pie(nc, values='العدد', names='الجنسية', title='توزيع الجنسيات', hole=.4, color_discrete_sequence=CL['sal'])
                        fig.update_layout(font=dict(family="Noto Sans Arabic"),height=380); st.plotly_chart(fig,use_container_width=True)
                    elif has(data,'الموقع'):
                        lc = data['الموقع'].value_counts().reset_index(); lc.columns=['الموقع','العدد']
                        fig = px.pie(lc, values='العدد', names='الموقع', title='التوزيع الجغرافي', hole=.4)
                        fig.update_layout(font=dict(family="Noto Sans Arabic"),height=380); st.plotly_chart(fig,use_container_width=True)

        elif page == "📊 الأقسام":
            hdr("📊 تحليل الأقسام")
            data = sal_snapshot if len(sal_snapshot)>0 else emp
            if len(data)==0: st.info("📁 ارفع ملف"); return
            dept_col = 'القسم' if has(data,'القسم') else ('القطاع' if has(data,'القطاع') else None)
            if dept_col:
                dc = data[dept_col].value_counts().reset_index(); dc.columns=[dept_col,'العدد']
                fig = px.bar(dc.sort_values('العدد'), x='العدد', y=dept_col, orientation='h', color='العدد', color_continuous_scale='teal', title=f'حجم كل {dept_col}')
                fig.update_layout(font=dict(family="Noto Sans Arabic"),height=500); st.plotly_chart(fig,use_container_width=True)

        elif page == "🤖 المحلل الذكي":
            hdr("🤖 المحلل الذكي","يبحث في كل الأوراق")
            data = sal_snapshot if len(sal_snapshot)>0 else emp
            if len(data)==0: st.info("📁 ارفع ملف"); return
            q = st.text_input("💬 اكتب سؤالك:", placeholder="ما نسبة السعودة؟ كم عدد الأقسام؟")
            if st.button("🔍 تحليل",type="primary",use_container_width=True) and q:
                ql = q.lower()
                a = ""
                total = len(data)
                if any(w in ql for w in ['سعود','جنسي','national','saudi']):
                    if has(data,'الجنسية'):
                        sa = data[data['الجنسية'].isin(['Saudi','سعودي','سعودية'])]
                        a = f"نسبة السعودة: {round(len(sa)/total*100,1)}% ({len(sa)} من {total})\n\n"
                        for nat,cnt in data['الجنسية'].value_counts().items():
                            a += f"  - {nat}: {cnt} ({round(cnt/total*100,1)}%)\n"
                    else: a = "لا يوجد عمود جنسية. أضف Nationality أو الجنسية للملف."
                elif any(w in ql for w in ['قسم','أقسام','department','division']):
                    dc = data['القسم'].value_counts() if has(data,'القسم') else (data['القطاع'].value_counts() if has(data,'القطاع') else None)
                    if dc is not None:
                        a = f"عدد الأقسام: {len(dc)}\n\n"
                        for d,c in dc.items(): a += f"  - {d}: {c} ({round(c/total*100,1)}%)\n"
                elif any(w in ql for w in ['راتب','رواتب','salary','تكلف']):
                    if has(data,'الراتب الإجمالي'):
                        a = f"إجمالي الرواتب الشهرية: {data['الراتب الإجمالي'].sum():,.0f} ريال\nمتوسط: {data['الراتب الإجمالي'].mean():,.0f}\nالأعلى: {data['الراتب الإجمالي'].max():,.0f}\nالأقل: {data['الراتب الإجمالي'].min():,.0f}"
                    elif has(data,'الراتب الأساسي'):
                        a = f"متوسط الراتب الأساسي: {data['الراتب الأساسي'].mean():,.0f} ريال"
                    else: a = "لا يوجد بيانات رواتب."
                else:
                    a = f"الموظفين: {total}\n"
                    for c in data.columns[:10]:
                        if data[c].dtype == 'object': a += f"{c}: {data[c].nunique()} قيمة فريدة\n"
                    a += f"\nالأعمدة: {', '.join(data.columns[:15])}"
                st.info(a if a else "جرب سؤال آخر")

        elif page == "📋 البيانات":
            hdr("📋 البيانات")
            if not all_sheets and n==0: st.info("📁 ارفع ملف"); return
            if all_sheets:
                sn = st.selectbox("الورقة:", list(all_sheets.keys()))
                st.dataframe(all_sheets[sn], use_container_width=True, hide_index=True, height=600)
            elif n>0:
                st.dataframe(emp, use_container_width=True, hide_index=True, height=600)


    # =========================================
    #           💰 SALARY ANALYSIS
    # =========================================
    elif section == "💰 تحليل الرواتب":

        if len(sal_df)==0 and n==0:
            hdr("💰 تحليل الرواتب")
            st.info("📁 ارفع ملف الرواتب (مثل Mother of Dashboards) من القائمة الجانبية")
            return

        data = sal_df if len(sal_df)>0 else emp
        snap = sal_snapshot if len(sal_snapshot)>0 else data

        if page == "💰 لوحة الرواتب":
            hdr("💰 لوحة تحليل الرواتب","تحليل شامل لتكاليف الرواتب والبدلات")

            total_emp = snap['الاسم'].nunique() if has(snap,'الاسم') else len(snap)
            k1,k2,k3,k4,k5 = st.columns(5)
            with k1: st.metric("👥 الموظفين", total_emp)
            with k2: st.metric("💰 إجمالي شهري", f"{snap['الراتب الإجمالي'].sum():,.0f}" if has(snap,'الراتب الإجمالي') else '-')
            with k3: st.metric("📊 المتوسط", f"{snap['الراتب الإجمالي'].mean():,.0f}" if has(snap,'الراتب الإجمالي') else '-')
            with k4: st.metric("📈 الأعلى", f"{snap['الراتب الإجمالي'].max():,.0f}" if has(snap,'الراتب الإجمالي') else '-')
            with k5: st.metric("📉 الأقل", f"{snap['الراتب الإجمالي'].min():,.0f}" if has(snap,'الراتب الإجمالي') else '-')

            st.markdown("---")

            # Salary components breakdown
            sal_components = ['الراتب الأساسي','بدل السكن','بدل النقل','بدل خاص','بدل معيشة','بدل جوال']
            available_components = [c for c in sal_components if has(snap,c)]

            if available_components:
                st.markdown("### 📊 تركيبة الراتب")
                comp_data = {c: snap[c].sum() for c in available_components}
                comp_df = pd.DataFrame(list(comp_data.items()), columns=['المكون','الإجمالي'])
                c1,c2 = st.columns(2)
                with c1:
                    fig = px.pie(comp_df, values='الإجمالي', names='المكون', title='توزيع مكونات الراتب', hole=.35, color_discrete_sequence=CL['sal'])
                    fig.update_layout(font=dict(family="Noto Sans Arabic"),height=380); st.plotly_chart(fig,use_container_width=True)
                with c2:
                    fig = px.bar(comp_df.sort_values('الإجمالي',ascending=True), x='الإجمالي', y='المكون', orientation='h', color='المكون', color_discrete_sequence=CL['dept'], title='مكونات الراتب بالقيمة')
                    fig.update_layout(font=dict(family="Noto Sans Arabic"),height=380,showlegend=False,xaxis_tickformat=','); st.plotly_chart(fig,use_container_width=True)

            # By Department/Division
            dept_col = 'القسم' if has(snap,'القسم') else ('القطاع' if has(snap,'القطاع') else None)
            if dept_col and has(snap,'الراتب الإجمالي'):
                st.markdown(f"### 🏢 الرواتب حسب {dept_col}")
                c1,c2 = st.columns(2)
                with c1:
                    ds = snap.groupby(dept_col)['الراتب الإجمالي'].sum().reset_index().sort_values('الراتب الإجمالي',ascending=True)
                    fig = px.bar(ds, x='الراتب الإجمالي', y=dept_col, orientation='h', title=f'إجمالي الرواتب حسب {dept_col}', color='الراتب الإجمالي', color_continuous_scale='teal')
                    fig.update_layout(font=dict(family="Noto Sans Arabic"),height=400,xaxis_tickformat=','); st.plotly_chart(fig,use_container_width=True)
                with c2:
                    ds2 = snap.groupby(dept_col)['الراتب الإجمالي'].mean().reset_index().sort_values('الراتب الإجمالي',ascending=True)
                    fig = px.bar(ds2, x='الراتب الإجمالي', y=dept_col, orientation='h', title=f'متوسط الراتب حسب {dept_col}', color='الراتب الإجمالي', color_continuous_scale='oranges')
                    fig.update_layout(font=dict(family="Noto Sans Arabic"),height=400,xaxis_tickformat=','); st.plotly_chart(fig,use_container_width=True)

            # By Nationality
            if has(snap,'الجنسية') and has(snap,'الراتب الإجمالي'):
                st.markdown("### 🌍 الرواتب حسب الجنسية")
                c1,c2 = st.columns(2)
                with c1:
                    ns = snap.groupby('الجنسية').agg({'الراتب الإجمالي':['mean','count']}).reset_index()
                    ns.columns = ['الجنسية','المتوسط','العدد']
                    fig = px.bar(ns, x='الجنسية', y='المتوسط', color='العدد', title='متوسط الراتب حسب الجنسية', text='العدد', color_continuous_scale='teal')
                    fig.update_layout(font=dict(family="Noto Sans Arabic"),height=380,yaxis_tickformat=','); st.plotly_chart(fig,use_container_width=True)
                with c2:
                    if has(snap,'الجنس'):
                        gs = snap.groupby('الجنس')['الراتب الإجمالي'].mean().reset_index()
                        fig = px.bar(gs, x='الجنس', y='الراتب الإجمالي', title='متوسط الراتب حسب الجنس', color='الجنس', color_discrete_sequence=[CL['p'],CL['a']])
                        fig.update_layout(font=dict(family="Noto Sans Arabic"),height=380,yaxis_tickformat=','); st.plotly_chart(fig,use_container_width=True)

            # Salary distribution
            if has(snap,'الراتب الإجمالي'):
                st.markdown("### 📊 توزيع الرواتب")
                c1,c2 = st.columns(2)
                with c1:
                    fig = px.histogram(snap, x='الراتب الإجمالي', nbins=20, title='توزيع الرواتب الإجمالية', color_discrete_sequence=[CL['p']])
                    fig.update_layout(font=dict(family="Noto Sans Arabic"),height=350); st.plotly_chart(fig,use_container_width=True)
                with c2:
                    if has(snap,'شريحة الراتب'):
                        sr = snap['شريحة الراتب'].value_counts().reset_index(); sr.columns=['الشريحة','العدد']
                        fig = px.bar(sr, x='الشريحة', y='العدد', title='توزيع شرائح الرواتب', color='الشريحة', color_discrete_sequence=CL['dept'])
                        fig.update_layout(font=dict(family="Noto Sans Arabic"),height=350); st.plotly_chart(fig,use_container_width=True)

        elif page == "📈 تحليل شهري/ربعي":
            hdr("📈 تحليل الرواتب الشهري والربعي")
            if len(sal_df)==0: st.info("📁 ارفع ملف رواتب شهري (مثل Mother of Dashboards)"); return

            if has(sal_df,'سنة الراتب'):
                year = st.selectbox("📅 السنة:", sorted(sal_df['سنة الراتب'].unique(), reverse=True))
                yr = sal_df[sal_df['سنة الراتب']==year]

                if has(yr,'شهر الراتب') and has(yr,'الراتب الإجمالي'):
                    months_order = ['January','February','March','April','May','June','July','August','September','October','November','December']
                    monthly = yr.groupby('شهر الراتب')['الراتب الإجمالي'].sum().reindex(months_order).dropna()
                    fig = go.Figure()
                    fig.add_trace(go.Bar(x=monthly.index, y=monthly.values, marker_color=CL['p'], text=[f"{v:,.0f}" for v in monthly.values], textposition='outside'))
                    fig.update_layout(title=f'إجمالي الرواتب الشهرية - {year}', font=dict(family="Noto Sans Arabic"), height=400, yaxis_tickformat=',')
                    st.plotly_chart(fig, use_container_width=True)

                if has(yr,'الربع') and has(yr,'الراتب الإجمالي'):
                    quarterly = yr.groupby('الربع')['الراتب الإجمالي'].sum()
                    c1,c2 = st.columns(2)
                    with c1:
                        fig = px.bar(quarterly.reset_index(), x='الربع', y='الراتب الإجمالي', title=f'الرواتب ربع السنوية - {year}', color='الربع', color_discrete_sequence=[CL['p'],CL['a'],CL['s'],'#64748B'])
                        fig.update_layout(font=dict(family="Noto Sans Arabic"),height=350,yaxis_tickformat=','); st.plotly_chart(fig,use_container_width=True)
                    with c2:
                        # Headcount trend by month
                        hc_monthly = yr.groupby('شهر الراتب')['الاسم'].nunique().reindex(months_order).dropna() if has(yr,'الاسم') else None
                        if hc_monthly is not None:
                            fig = go.Figure()
                            fig.add_trace(go.Scatter(x=hc_monthly.index, y=hc_monthly.values, mode='lines+markers', line=dict(color=CL['a'],width=3), fill='tozeroy', fillcolor='rgba(227,100,20,0.1)'))
                            fig.update_layout(title=f'عدد الموظفين شهرياً - {year}', font=dict(family="Noto Sans Arabic"),height=350)
                            st.plotly_chart(fig, use_container_width=True)

                # Overtime analysis
                if has(yr,'ساعات إضافية') and has(yr,'تكلفة الإضافي'):
                    st.markdown("### ⏰ تحليل الساعات الإضافية")
                    c1,c2 = st.columns(2)
                    with c1: st.metric("🕐 إجمالي الساعات", f"{yr['ساعات إضافية'].sum():,.0f}")
                    with c2: st.metric("💰 تكلفة الإضافي", f"{yr['تكلفة الإضافي'].sum():,.0f} ريال")

        elif page == "🏷️ تحليل حسب الفئات":
            hdr("🏷️ تحليل حسب الفئات","الجنس، الجيل، المستوى، نوع التوظيف")
            if len(sal_df)==0 and n==0: st.info("📁 ارفع ملف"); return
            data = sal_snapshot if len(sal_snapshot)>0 else emp

            tabs = st.tabs(["👫 الجنس","🎂 الأجيال","📊 المستويات","📋 نوع التوظيف"])

            with tabs[0]:
                if has(data,'الجنس'):
                    gc = data['الجنس'].value_counts().reset_index(); gc.columns=['الجنس','العدد']
                    c1,c2 = st.columns(2)
                    with c1:
                        fig = px.pie(gc, values='العدد', names='الجنس', title='التوزيع حسب الجنس', hole=.4, color_discrete_map={'Male':CL['p'],'Female':CL['a']})
                        fig.update_layout(font=dict(family="Noto Sans Arabic"),height=350); st.plotly_chart(fig,use_container_width=True)
                    with c2:
                        if has(data,'الراتب الإجمالي'):
                            gs = data.groupby('الجنس')['الراتب الإجمالي'].agg(['mean','median']).reset_index()
                            gs.columns = ['الجنس','المتوسط','الوسيط']
                            st.dataframe(gs, use_container_width=True, hide_index=True)
                else: st.info("لا يوجد عمود جنس")

            with tabs[1]:
                if has(data,'الجيل'):
                    gc2 = data['الجيل'].value_counts().reset_index(); gc2.columns=['الجيل','العدد']
                    fig = px.bar(gc2, x='الجيل', y='العدد', title='التوزيع حسب الجيل', color='الجيل', color_discrete_sequence=CL['dept'])
                    fig.update_layout(font=dict(family="Noto Sans Arabic"),height=350); st.plotly_chart(fig,use_container_width=True)
                elif has(data,'الفئة العمرية'):
                    ac = data['الفئة العمرية'].value_counts().reset_index(); ac.columns=['الفئة','العدد']
                    fig = px.bar(ac, x='الفئة', y='العدد', title='التوزيع حسب الفئة العمرية', color='الفئة', color_discrete_sequence=CL['dept'])
                    fig.update_layout(font=dict(family="Noto Sans Arabic"),height=350); st.plotly_chart(fig,use_container_width=True)
                else: st.info("لا يوجد بيانات أجيال")

            with tabs[2]:
                if has(data,'المستوى'):
                    lc = data['المستوى'].value_counts().reset_index(); lc.columns=['المستوى','العدد']
                    c1,c2 = st.columns(2)
                    with c1:
                        fig = px.pie(lc, values='العدد', names='المستوى', title='التوزيع حسب المستوى', hole=.35, color_discrete_sequence=CL['dept'])
                        fig.update_layout(font=dict(family="Noto Sans Arabic"),height=350); st.plotly_chart(fig,use_container_width=True)
                    with c2:
                        if has(data,'الراتب الإجمالي'):
                            ls = data.groupby('المستوى')['الراتب الإجمالي'].mean().reset_index().sort_values('الراتب الإجمالي',ascending=True)
                            fig = px.bar(ls, x='الراتب الإجمالي', y='المستوى', orientation='h', title='متوسط الراتب حسب المستوى', color='الراتب الإجمالي', color_continuous_scale='teal')
                            fig.update_layout(font=dict(family="Noto Sans Arabic"),height=350,xaxis_tickformat=','); st.plotly_chart(fig,use_container_width=True)
                else: st.info("لا يوجد عمود مستوى")

            with tabs[3]:
                if has(data,'نوع التوظيف'):
                    ec = data['نوع التوظيف'].value_counts().reset_index(); ec.columns=['النوع','العدد']
                    fig = px.pie(ec, values='العدد', names='النوع', title='أنواع التوظيف', hole=.35, color_discrete_sequence=CL['sal'])
                    fig.update_layout(font=dict(family="Noto Sans Arabic"),height=350); st.plotly_chart(fig,use_container_width=True)
                else: st.info("لا يوجد عمود نوع التوظيف")

        elif page == "📊 سلم الرواتب":
            hdr("📊 سلم الرواتب والدرجات")
            if 'Salary Scale' in all_sheets:
                ss = all_sheets['Salary Scale'].dropna(subset=['Grade'] if 'Grade' in all_sheets['Salary Scale'].columns else all_sheets['Salary Scale'].columns[:1])
                ss_norm = norm_cols(ss)
                st.dataframe(ss, use_container_width=True, hide_index=True)
                if 'Min Salary' in ss.columns and 'Max Salary' in ss.columns:
                    ss_clean = ss.dropna(subset=['Min Salary','Max Salary'])
                    fig = go.Figure()
                    fig.add_trace(go.Bar(name='الحد الأدنى', x=ss_clean['Grade'].astype(str), y=ss_clean['Min Salary'], marker_color=CL['s']))
                    fig.add_trace(go.Bar(name='المتوسط', x=ss_clean['Grade'].astype(str), y=ss_clean['Mid Salary'], marker_color=CL['a']))
                    fig.add_trace(go.Bar(name='الحد الأقصى', x=ss_clean['Grade'].astype(str), y=ss_clean['Max Salary'], marker_color=CL['d']))
                    fig.update_layout(title='سلم الرواتب حسب الدرجة', barmode='group', font=dict(family="Noto Sans Arabic"), height=420, yaxis_tickformat=',')
                    st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("لم يتم العثور على ورقة Salary Scale في الملف المرفوع")

        elif page == "📥 تصدير الرواتب":
            hdr("📥 تصدير تقارير الرواتب")
            data = sal_snapshot if len(sal_snapshot)>0 else emp
            if len(data)==0: st.info("📁 ارفع ملف"); return
            o = io.BytesIO()
            with pd.ExcelWriter(o, engine='xlsxwriter') as w:
                data.to_excel(w, sheet_name='البيانات', index=False)
                w.sheets['البيانات'].right_to_left()
            st.download_button("📥 تحميل Excel", data=o.getvalue(),
                file_name=f"Salary_Report_{datetime.now().strftime('%Y%m%d')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", type="primary", use_container_width=True)


    # =========================================
    #        👥 HEADCOUNT & PERFORMANCE
    # =========================================
    elif section == "👥 Headcount":

        if page == "👥 Headcount Planning":
            hdr("👥 Headcount Planning","تخطيط القوى العاملة")

            data = sal_snapshot if len(sal_snapshot)>0 else emp
            total = len(data)

            st.markdown("### 📊 الوضع الحالي")
            dept_col = 'القسم' if has(data,'القسم') else ('القطاع' if has(data,'القطاع') else None)

            if total > 0 and dept_col:
                hc = data[dept_col].value_counts().reset_index()
                hc.columns = [dept_col, 'الحالي']

                # Nationality breakdown per dept
                if has(data,'الجنسية'):
                    sa_per_dept = data[data['الجنسية'].isin(['Saudi','سعودي'])].groupby(dept_col).size().reset_index(name='سعودي')
                    non_sa = data[~data['الجنسية'].isin(['Saudi','سعودي'])].groupby(dept_col).size().reset_index(name='غير سعودي')
                    hc = hc.merge(sa_per_dept, on=dept_col, how='left').merge(non_sa, on=dept_col, how='left').fillna(0)
                    hc['سعودي'] = hc['سعودي'].astype(int)
                    hc['غير سعودي'] = hc['غير سعودي'].astype(int)
                    hc['نسبة السعودة'] = (hc['سعودي'] / hc['الحالي'] * 100).round(1)

                # Level breakdown
                if has(data,'المستوى'):
                    for lvl in data['المستوى'].unique():
                        hc[lvl] = data[data['المستوى']==lvl].groupby(dept_col).size().reindex(hc[dept_col]).fillna(0).astype(int).values

                st.dataframe(hc, use_container_width=True, hide_index=True)

                # Gender breakdown
                if has(data,'الجنس'):
                    st.markdown("### 👫 التوزيع حسب الجنس")
                    gd = pd.crosstab(data[dept_col], data['الجنس'])
                    st.dataframe(gd, use_container_width=True)

                # Headcount planning tool
                st.markdown("---")
                st.markdown("### 📋 تخطيط Headcount المستقبلي")
                growth_pct = st.slider("📈 نسبة النمو المستهدفة %", 0, 50, 15)

                plan = hc[[dept_col,'الحالي']].copy()
                plan['المستهدف'] = (plan['الحالي'] * (1 + growth_pct/100)).apply(math.ceil)
                plan['الفرق'] = plan['المستهدف'] - plan['الحالي']
                if has(data,'الراتب الإجمالي'):
                    avg_by_dept = data.groupby(dept_col)['الراتب الإجمالي'].mean()
                    plan['التكلفة الشهرية المتوقعة'] = plan.apply(lambda r: int(r['الفرق'] * avg_by_dept.get(r[dept_col], 0)), axis=1)
                    plan['التكلفة السنوية'] = plan['التكلفة الشهرية المتوقعة'] * 12

                st.dataframe(plan, use_container_width=True, hide_index=True)

                # Totals
                cols = st.columns(4)
                with cols[0]: st.metric("👥 الحالي", total)
                with cols[1]: st.metric("🎯 المستهدف", plan['المستهدف'].sum())
                with cols[2]: st.metric("📊 التعيينات المطلوبة", plan['الفرق'].sum())
                with cols[3]:
                    if 'التكلفة السنوية' in plan.columns:
                        st.metric("💰 التكلفة السنوية", f"{plan['التكلفة السنوية'].sum():,.0f}")
            else:
                st.info("📁 ارفع ملف بيانات الموظفين لبناء Headcount")
                st.markdown("### 📋 أو أدخل البيانات يدوياً")
                num_depts = st.number_input("عدد الأقسام", 1, 20, 5)
                manual_data = []
                for i in range(num_depts):
                    c1,c2,c3 = st.columns(3)
                    with c1: dept = st.text_input(f"اسم القسم {i+1}", f"قسم {i+1}", key=f"d_{i}")
                    with c2: current = st.number_input(f"العدد الحالي", 0, 500, 10, key=f"c_{i}")
                    with c3: target = st.number_input(f"المستهدف", 0, 500, 12, key=f"t_{i}")
                    manual_data.append({"القسم":dept, "الحالي":current, "المستهدف":target, "الفرق":target-current})
                if manual_data:
                    st.dataframe(pd.DataFrame(manual_data), use_container_width=True, hide_index=True)

        elif page == "📊 تحليل الأداء":
            hdr("📊 تحليل الأداء","تحليل إنتاجية وأداء الموظفين")

            data = sal_snapshot if len(sal_snapshot)>0 else emp

            if len(data)==0: st.info("📁 ارفع ملف"); return

            # Productivity metrics from salary data
            if has(data,'الراتب الإجمالي'):
                st.markdown("### 💰 مؤشرات التكلفة والإنتاجية")
                dept_col = 'القسم' if has(data,'القسم') else ('القطاع' if has(data,'القطاع') else None)

                c1,c2,c3 = st.columns(3)
                with c1: st.metric("💵 متوسط تكلفة الموظف/شهر", f"{data['الراتب الإجمالي'].mean():,.0f}")
                with c2: st.metric("📊 الانحراف المعياري", f"{data['الراتب الإجمالي'].std():,.0f}")
                with c3:
                    if has(data,'ساعات إضافية'):
                        st.metric("⏰ متوسط الإضافي", f"{data['ساعات إضافية'].mean():.1f} ساعة")

                if dept_col:
                    perf = data.groupby(dept_col).agg({
                        'الراتب الإجمالي': ['mean','sum','count'],
                    }).reset_index()
                    perf.columns = [dept_col, 'متوسط الراتب', 'إجمالي الرواتب', 'عدد الموظفين']
                    perf['نسبة التكلفة %'] = (perf['إجمالي الرواتب'] / perf['إجمالي الرواتب'].sum() * 100).round(1)
                    perf['نسبة العدد %'] = (perf['عدد الموظفين'] / perf['عدد الموظفين'].sum() * 100).round(1)
                    perf['كفاءة التكلفة'] = (perf['نسبة التكلفة %'] / perf['نسبة العدد %']).round(2)

                    st.markdown("### 📊 كفاءة التكلفة حسب القسم")
                    st.dataframe(perf.sort_values('كفاءة التكلفة', ascending=False), use_container_width=True, hide_index=True)

                    c1,c2 = st.columns(2)
                    with c1:
                        fig = px.scatter(perf, x='نسبة العدد %', y='نسبة التكلفة %', size='عدد الموظفين', color=dept_col,
                            title='العدد مقابل التكلفة (الحجم = عدد الموظفين)', color_discrete_sequence=CL['dept'])
                        fig.add_trace(go.Scatter(x=[0,50], y=[0,50], mode='lines', line=dict(dash='dash',color='gray'), name='خط التوازن'))
                        fig.update_layout(font=dict(family="Noto Sans Arabic"),height=400); st.plotly_chart(fig,use_container_width=True)
                    with c2:
                        fig = px.bar(perf.sort_values('كفاءة التكلفة'), x='كفاءة التكلفة', y=dept_col, orientation='h',
                            title='مؤشر كفاءة التكلفة (1 = متوازن)', color='كفاءة التكلفة', color_continuous_scale='RdYlGn_r')
                        fig.add_vline(x=1, line_dash="dash", line_color="gray")
                        fig.update_layout(font=dict(family="Noto Sans Arabic"),height=400); st.plotly_chart(fig,use_container_width=True)

                    ibox("مؤشر كفاءة التكلفة: إذا كان أكبر من 1 فالقسم يكلف أكثر من حجمه النسبي. إذا كان أقل من 1 فالقسم فعّال من حيث التكلفة.")

                # Overtime analysis as performance indicator
                if has(data,'ساعات إضافية') and dept_col:
                    st.markdown("### ⏰ تحليل الساعات الإضافية")
                    ot = data.groupby(dept_col)['ساعات إضافية'].agg(['mean','sum']).reset_index()
                    ot.columns = [dept_col, 'المتوسط','الإجمالي']
                    fig = px.bar(ot.sort_values('المتوسط',ascending=True), x='المتوسط', y=dept_col, orientation='h',
                        title='متوسط الساعات الإضافية حسب القسم', color='المتوسط', color_continuous_scale='oranges')
                    fig.update_layout(font=dict(family="Noto Sans Arabic"),height=400); st.plotly_chart(fig,use_container_width=True)
            else:
                st.warning("لا يوجد بيانات رواتب للتحليل. ارفع ملف رواتب أو أضف عمود Gross Salary.")


    # =========================================
    #         ⚖️ LABOR CALCULATOR (MOJ-MATCHING)
    # =========================================
    elif section == "⚖️ حاسبة المستحقات":
        hdr("⚖️ الحاسبة العمالية الشاملة","مطابقة لحاسبة وزارة العدل - نظام العمل السعودي")

        # ===== بيانات الموظف =====
        st.markdown("### 👤 بيانات الموظف")
        e1, e2, e3 = st.columns([2,1,1])
        with e1: emp_name = st.text_input("اسم الموظف:", key="empn")
        with e2: emp_id = st.text_input("رقم الموظف:", key="empid")
        with e3: worker_type = st.radio("الجنسية:", ["سعودي","غير سعودي"], horizontal=True, key="wt")

        st.markdown("### 💵 تفاصيل الأجر")
        s1, s2, s3, s4 = st.columns(4)
        with s1: basic_sal = st.number_input("الأجر الأساسي:", min_value=0.0, max_value=500000.0, value=5000.0, step=0.01, format="%.2f", key="bsal")
        with s2: housing = st.number_input("بدل السكن:", min_value=0.0, max_value=500000.0, value=1250.0, step=0.01, format="%.2f", key="hous")
        with s3: transport = st.number_input("بدل المواصلات:", min_value=0.0, max_value=100000.0, value=500.0, step=0.01, format="%.2f", key="trns")
        with s4: other_allow = st.number_input("بدلات أخرى:", min_value=0.0, max_value=500000.0, value=0.0, step=0.01, format="%.2f", key="otha")

        # GOSI calculation - Saudi only
        is_saudi = worker_type == "سعودي"
        if is_saudi:
            gosi_pct = st.slider("🏛️ نسبة خصم التأمينات الاجتماعية (%):", 0.0, 25.0, 9.75, 0.25, key="gosi_pct",
                help="النسبة الافتراضية: 9.75% (حصة الموظف)")
            gosi_base = basic_sal + housing
            gosi_deduction = gosi_base * (gosi_pct / 100)
            net_after_gosi = gosi_base - gosi_deduction
            total_sal = net_after_gosi + transport + other_allow

            g1,g2,g3,g4 = st.columns(4)
            with g1: kpi("وعاء التأمينات (أساسي+سكن)", f"{gosi_base:,.2f}")
            with g2: kpi(f"خصم التأمينات ({gosi_pct}%)", f"{gosi_deduction:,.2f}")
            with g3: kpi("بعد خصم التأمينات", f"{net_after_gosi:,.2f}")
            with g4: kpi("💰 الأجر النهائي (صافي)", f"{total_sal:,.2f}")

            ibox(f"طريقة الحساب: (الأساسي {basic_sal:,} + السكن {housing:,}) - التأمينات {gosi_pct}% = {net_after_gosi:,.2f} + المواصلات {transport:,} + أخرى {other_allow:,} = **{total_sal:,.2f} ريال**")
            gross_sal = basic_sal + housing + transport + other_allow
        else:
            gosi_pct = 0; gosi_deduction = 0
            gross_sal = basic_sal + housing + transport + other_allow
            total_sal = gross_sal
            g1,g2 = st.columns(2)
            with g1: kpi("إجمالي الأجر", f"{total_sal:,.2f}")
            with g2: kpi("💰 الأجر النهائي", f"{total_sal:,.2f}")
            ibox("غير سعودي: لا يوجد خصم تأمينات اجتماعية.", "success")

        daily_sal = gross_sal / 30
        results_summary = []

        # ========== 1. الأجور المتأخرة ==========
        st.markdown("---")
        st.markdown("### 💰 1. الأجور المتأخرة")
        dw_method = st.radio("طريقة الإدخال:", ["بإدخال التاريخ من إلى","بإدخال عدد الأشهر والأيام"], horizontal=True, key="dwm")
        if dw_method == "بإدخال التاريخ من إلى":
            d1, d2 = st.columns(2)
            with d1: dw_from = st.date_input("من:", value=date.today(), key="dwf")
            with d2: dw_to = st.date_input("إلى:", value=date.today(), key="dwt")
            dw_total_days = (dw_to - dw_from).days
        else:
            d1, d2 = st.columns(2)
            with d1: dw_months = st.number_input("عدد الأشهر:", 0, 120, 0, key="dwmo")
            with d2: dw_extra_days = st.number_input("عدد الأيام:", 0, 30, 0, key="dwdy")
            dw_total_days = dw_months * 30 + dw_extra_days
        delayed_amount = daily_sal * dw_total_days
        if dw_total_days > 0:
            st.success(f"الأجور المتأخرة: **{delayed_amount:,.2f} ريال** ({dw_total_days} يوم x {daily_sal:,.2f})")
            results_summary.append(("الأجور المتأخرة", delayed_amount))

        # ========== 2. مكافأة نهاية الخدمة ==========
        st.markdown("---")
        st.markdown("### 📊 2. مكافأة نهاية الخدمة")
        ec1, ec2 = st.columns(2)
        with ec1: eos_method = st.radio("طريقة الاحتساب:", ["حسب المادة (84)","حسب المادة (85) - استقالة"], key="eosm")
        with ec2: unpaid_leave = st.number_input("إجمالي أيام الإجازات بدون أجر:", 0, 9999, 0, key="unp")
        ec3, ec4 = st.columns(2)
        with ec3: eos_start = st.date_input("بداية العمل:", value=date(2018,1,1), key="eoss")
        with ec4: eos_end = st.date_input("نهاية العمل:", value=date.today(), key="eose")

        eos_service_days = max((eos_end - eos_start).days - unpaid_leave, 0)
        eos_years = eos_service_days / 365.25
        eos_delta = relativedelta(eos_end, eos_start)
        eos_monthly = gross_sal
        if eos_years <= 5:
            eos_84 = (eos_monthly / 2) * eos_years
        else:
            eos_84 = (eos_monthly / 2) * 5 + eos_monthly * (eos_years - 5)
        is_85 = "85" in eos_method
        if is_85:
            if eos_years < 2: eos_final=0; eos_pct=0; eos_note="لا يستحق (أقل من سنتين)"
            elif eos_years < 5: eos_final=eos_84/3; eos_pct=33.3; eos_note="ثلث المكافأة (2-5 سنوات)"
            elif eos_years < 10: eos_final=eos_84*2/3; eos_pct=66.7; eos_note="ثلثا المكافأة (5-10 سنوات)"
            else: eos_final=eos_84; eos_pct=100; eos_note="كاملة (10+ سنوات)"
        else:
            eos_final=eos_84; eos_pct=100; eos_note="المكافأة كاملة (المادة 84)"

        ek1,ek2,ek3,ek4 = st.columns(4)
        with ek1: kpi("مدة الخدمة", f"{eos_delta.years} سنة {eos_delta.months} شهر")
        with ek2: kpi("المكافأة كاملة (84)", f"{eos_84:,.2f}")
        with ek3: kpi(f"المستحق ({eos_pct}%)", f"{eos_final:,.2f}")
        with ek4: kpi("الأجر اليومي", f"{daily_sal:,.2f}")

        calc_rows = []
        if eos_years <= 5:
            calc_rows.append({"البند": f"{eos_years:.2f} سنة x نصف شهر", "المبلغ": f"{eos_84:,.2f}"})
        else:
            f5 = (eos_monthly/2)*5; r5 = eos_monthly*(eos_years-5)
            calc_rows.append({"البند": "أول 5 سنوات x نصف شهر", "المبلغ": f"{f5:,.2f}"})
            calc_rows.append({"البند": f"ما بعد 5 سنوات ({eos_years-5:.2f}) x شهر كامل", "المبلغ": f"{r5:,.2f}"})
        if is_85: calc_rows.append({"البند": f"المستحق (مادة 85): {eos_pct}%", "المبلغ": f"{eos_final:,.2f}"})
        st.dataframe(pd.DataFrame(calc_rows), use_container_width=True, hide_index=True)
        ibox(eos_note, "success" if eos_pct==100 else ("danger" if eos_pct==0 else "warning"))
        if unpaid_leave > 0: ibox(f"تم خصم {unpaid_leave} يوم إجازة بدون أجر.")
        ibox(f"المكافأة تُحسب على أساس الأجر الإجمالي قبل خصم التأمينات: {eos_monthly:,.2f} ريال")
        results_summary.append(("مكافأة نهاية الخدمة", eos_final))

        # ========== 3. أجر الإجازة ==========
        st.markdown("---")
        st.markdown("### 🏖️ 3. أجر الإجازة")
        vac_days_input = st.number_input("عدد أيام الإجازة المستحقة:", min_value=0.0, max_value=365.0, value=0.0, step=0.01, format="%.2f", key="vacd")
        vac_amount = daily_sal * vac_days_input
        if vac_days_input > 0:
            st.success(f"أجر الإجازة: **{vac_amount:,.2f} ريال** ({vac_days_input} يوم x {daily_sal:,.2f})")
            results_summary.append(("أجر الإجازة", vac_amount))

        # ========== 4. أجر العمل الإضافي ==========
        st.markdown("---")
        st.markdown("### ⏰ 4. أجر العمل الإضافي")
        ibox("المادة 107: أجر ساعة الإضافي = أجر الساعة + 50% (150%).")
        oc1, oc2, oc3 = st.columns(3)
        with oc1: ot_work_hours = st.selectbox("ساعات اليوم الفعلية:", list(range(2,13)), index=6, key="oth")
        with oc2: ot_days = st.number_input("عدد الأيام الإضافية:", 0, 365, 0, key="otd")
        with oc3: ot_hours = st.number_input("عدد الساعات الإضافية:", 0, 9999, 0, key="othr")
        ot_hourly = basic_sal / 30 / ot_work_hours
        ot_rate = ot_hourly * 1.5
        ot_total_hours = (ot_days * ot_work_hours) + ot_hours
        ot_amount = ot_total_hours * ot_rate
        if ot_total_hours > 0:
            st.success(f"ساعة الإضافي: {ot_rate:,.2f} | الساعات: {ot_total_hours} | **الإجمالي: {ot_amount:,.2f} ريال**")
            results_summary.append(("أجر العمل الإضافي", ot_amount))

        # ========== 5. التعويض عن الإنهاء غير المشروع ==========
        st.markdown("---")
        st.markdown("### 🚫 5. التعويض عن الإنهاء لغير سبب مشروع (المادة 77)")
        contract_type = st.radio("نوع العقد:", ["عقد محدد المدة","عقد غير محدد المدة"], key="ctype")
        if contract_type == "عقد محدد المدة":
            st.markdown("**المدة المتبقية من العقد:**")
            uc1, uc2 = st.columns(2)
            with uc1: ct_from = st.date_input("من:", value=date.today(), key="ctf")
            with uc2: ct_to = st.date_input("إلى:", value=date.today(), key="ctt")
            remaining_days = (ct_to - ct_from).days
            comp = daily_sal * remaining_days
            min_comp = gross_sal * 2
            unfair_amount = max(comp, min_comp)
            if remaining_days > 0:
                note77 = "(الحد الأدنى: شهرين)" if comp < min_comp else "(أجر المدة المتبقية)"
                st.success(f"المتبقي: {remaining_days} يوم | **التعويض: {unfair_amount:,.2f} ريال** {note77}")
                results_summary.append(("تعويض إنهاء غير مشروع", unfair_amount))
        else:
            uc1, uc2 = st.columns(2)
            with uc1: uct_start = st.date_input("بداية العمل:", value=date(2018,1,1), key="ucts")
            with uc2: uct_end = st.date_input("تاريخ الإنهاء:", value=date.today(), key="ucte")
            svc_yrs = (uct_end - uct_start).days / 365.25
            comp = (daily_sal * 15) * svc_yrs
            min_comp = gross_sal * 2
            unfair_amount = max(comp, min_comp)
            if svc_yrs > 0:
                note77 = "(الحد الأدنى: شهرين)" if comp < min_comp else "(15 يوم/سنة)"
                st.success(f"الخدمة: {svc_yrs:.2f} سنة | **التعويض: {unfair_amount:,.2f} ريال** {note77}")
                results_summary.append(("تعويض إنهاء غير مشروع", unfair_amount))

        # ========== 6. أيام الإجازة المستحقة ==========
        st.markdown("---")
        st.markdown("### 📅 6. أيام الإجازة المستحقة في فترة الخدمة")
        ibox("المادة 109: الحد الأدنى 21 يوم في أول 5 سنوات، 30 يوم بعدها.")
        vc1, vc2 = st.columns(2)
        with vc1: vd_first5 = st.number_input("أيام الإجازة في أول 5 سنوات:", min_value=21, max_value=60, value=21, key="vd5")
        with vc2: vd_after5 = st.number_input("أيام الإجازة بعد 5 سنوات:", min_value=30, max_value=60, value=30, key="vda5")
        vc3, vc4 = st.columns(2)
        with vc3: vd_from = st.date_input("من تاريخ:", value=date(2018,1,1), key="vdf")
        with vc4: vd_to = st.date_input("إلى تاريخ:", value=date.today(), key="vdt")
        vd_yrs = (vd_to - vd_from).days / 365.25
        vd_delta = relativedelta(vd_to, vd_from)
        vd_total = (vd_yrs * vd_first5) if vd_yrs <= 5 else (5 * vd_first5) + ((vd_yrs - 5) * vd_after5)
        if vd_yrs > 0:
            st.success(f"الخدمة: {vd_delta.years} سنة {vd_delta.months} شهر | **الإجازة المستحقة: {vd_total:.1f} يوم**")

        # ========== 7. حسم الغياب والتأخر ==========
        st.markdown("---")
        st.markdown("### 📉 7. حسم الغياب والتأخر")
        ac1, ac2 = st.columns(2)
        with ac1:
            abs_hours_day = st.selectbox("ساعات العمل اليومية:", list(range(2,13)), index=6, key="absh")
            abs_days = st.number_input("عدد أيام الغياب:", 0, 365, 0, key="absd")
        with ac2:
            abs_hours = st.number_input("عدد ساعات التأخر:", 0, 999, 0, key="abshr")
            abs_minutes = st.number_input("عدد دقائق التأخر:", 0, 59, 0, key="absmin")
        abs_hourly = daily_sal / abs_hours_day
        abs_minute_rate = abs_hourly / 60
        abs_day_ded = abs_days * daily_sal
        abs_hr_ded = abs_hours * abs_hourly
        abs_min_ded = abs_minutes * abs_minute_rate
        abs_total = abs_day_ded + abs_hr_ded + abs_min_ded
        if abs_total > 0:
            parts = []
            if abs_days > 0: parts.append(f"غياب {abs_days} يوم = {abs_day_ded:,.2f}")
            if abs_hours > 0: parts.append(f"تأخر {abs_hours} ساعة = {abs_hr_ded:,.2f}")
            if abs_minutes > 0: parts.append(f"تأخر {abs_minutes} دقيقة = {abs_min_ded:,.2f}")
            st.warning(f"{' | '.join(parts)} | **إجمالي الحسم: {abs_total:,.2f} ريال**")
            results_summary.append(("حسم الغياب والتأخر (يُخصم)", abs_total))

        # ========== 8. متوسط أجر آخر سنة ==========
        st.markdown("---")
        st.markdown("### 📊 8. متوسط الأجر لآخر سنة")
        ibox("يُستخدم عندما يكون الأجر متغيراً (عمولات، مكافآت).")
        months_ar = ["يناير","فبراير","مارس","أبريل","مايو","يونيو","يوليو","أغسطس","سبتمبر","أكتوبر","نوفمبر","ديسمبر"]
        month_sals = []
        for i in range(0, 12, 6):
            cols = st.columns(6)
            for j in range(6):
                if i+j < 12:
                    with cols[j]:
                        val = st.number_input(f"{months_ar[i+j]}:", 0, 500000, 0, 100, key=f"ms{i+j}")
                        month_sals.append(val)
        non_zero = [s for s in month_sals if s > 0]
        if non_zero:
            avg_12 = sum(month_sals) / 12
            avg_actual = sum(non_zero) / len(non_zero)
            st.success(f"الإجمالي: {sum(month_sals):,.0f} | المتوسط (12 شهر): **{avg_12:,.2f}** | المتوسط ({len(non_zero)} أشهر فعلية): {avg_actual:,.2f}")

        # ========================================
        #          الملخص النهائي + التصدير
        # ========================================
        st.markdown("---")
        st.markdown("### 🟰 ملخص المستحقات النهائية")
        if emp_name or emp_id:
            st.markdown(f"**الموظف:** {emp_name or '-'} | **الرقم:** {emp_id or '-'} | **الجنسية:** {worker_type}")

        if results_summary:
            grand_total = 0
            summary_rows = []
            for label, amount in results_summary:
                is_ded = "خصم" in label or "حسم" in label
                summary_rows.append({"البند": label, "المبلغ (ريال)": f"{amount:,.2f}", "النوع": "🔴 خصم" if is_ded else "🟢 استحقاق"})
                grand_total += (-amount if is_ded else amount)
            summary_rows.append({"البند": "🟰 صافي المستحقات النهائية", "المبلغ (ريال)": f"{grand_total:,.2f}", "النوع": "💰 الإجمالي"})
            st.dataframe(pd.DataFrame(summary_rows), use_container_width=True, hide_index=True)

            k1, k2 = st.columns(2)
            with k1: kpi("💰 صافي المستحقات النهائية", f"{grand_total:,.2f} ريال")
            with k2: kpi("📋 عدد البنود", f"{len(results_summary)}")

            # ===== EXPORT =====
            st.markdown("### 📥 تصدير التقرير")
            export_rows = [
                {"البند": "اسم الموظف", "القيمة": emp_name or "-"},
                {"البند": "رقم الموظف", "القيمة": emp_id or "-"},
                {"البند": "الجنسية", "القيمة": worker_type},
                {"البند": "الأجر الأساسي", "القيمة": f"{basic_sal:,.2f}"},
                {"البند": "بدل السكن", "القيمة": f"{housing:,.2f}"},
                {"البند": "بدل المواصلات", "القيمة": f"{transport:,.2f}"},
                {"البند": "بدلات أخرى", "القيمة": f"{other_allow:,.2f}"},
                {"البند": "إجمالي الأجر", "القيمة": f"{gross_sal:,.2f}"},
                {"البند": f"خصم التأمينات ({gosi_pct}%)", "القيمة": f"{gosi_deduction:,.2f}"},
                {"البند": "صافي الأجر", "القيمة": f"{total_sal:,.2f}"},
                {"البند": "---", "القيمة": "---"}]
            for label, amount in results_summary:
                is_ded = "خصم" in label or "حسم" in label
                export_rows.append({"البند": label, "القيمة": f"{'-' if is_ded else ''}{amount:,.2f}"})
            export_rows.append({"البند": "---", "القيمة": "---"})
            export_rows.append({"البند": "صافي المستحقات النهائية", "القيمة": f"{grand_total:,.2f}"})

            # ===== PROFESSIONAL EXCEL (matching MOJ template) =====
            ox = io.BytesIO()
            wb_exp = openpyxl.Workbook()
            ws_exp = wb_exp.active
            ws_exp.title = "المستحقات"
            ws_exp.sheet_view.rightToLeft = True

            from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
            from openpyxl.utils import get_column_letter

            # Colors
            dark_blue = PatternFill("solid", fgColor="1F4E79")
            med_blue = PatternFill("solid", fgColor="2E75B6")
            hdr_blue = PatternFill("solid", fgColor="4472C4")
            light1 = PatternFill("solid", fgColor="D6E4F0")
            light2 = PatternFill("solid", fgColor="EBF1F8")

            white_font = Font(bold=True, color="FFFFFF", size=16, name="Calibri")
            white_font12 = Font(bold=True, color="FFFFFF", size=12, name="Calibri")
            white_font13 = Font(bold=True, color="FFFFFF", size=13, name="Calibri")
            white_font11 = Font(bold=True, color="FFFFFF", size=11, name="Calibri")
            bold11 = Font(bold=True, size=11, name="Calibri")
            normal11 = Font(size=11, name="Calibri")
            blue_val = Font(size=11, color="0000FF", name="Calibri")
            red_val = Font(size=11, color="FF0000", name="Calibri")
            small10 = Font(size=10, name="Calibri")
            gray10 = Font(size=10, color="808080", name="Calibri")

            thin_border = Border(
                left=Side(style='thin', color='B0B0B0'),
                right=Side(style='thin', color='B0B0B0'),
                top=Side(style='thin', color='B0B0B0'),
                bottom=Side(style='thin', color='B0B0B0'))

            center = Alignment(horizontal='center', vertical='center', wrap_text=True)
            left = Alignment(horizontal='left', vertical='center', wrap_text=True)
            right_al = Alignment(horizontal='right', vertical='center', wrap_text=True)

            # Column widths
            ws_exp.column_dimensions['A'].width = 8
            ws_exp.column_dimensions['B'].width = 35
            ws_exp.column_dimensions['C'].width = 35
            ws_exp.column_dimensions['D'].width = 20
            ws_exp.column_dimensions['E'].width = 20
            ws_exp.column_dimensions['F'].width = 18

            r = 1  # current row

            def write_merged(row, col1, col2, value, font, fill, align_style=center):
                ws_exp.merge_cells(start_row=row, start_column=col1, end_row=row, end_column=col2)
                c = ws_exp.cell(row=row, column=col1, value=value)
                c.font = font; c.fill = fill; c.alignment = align_style; c.border = thin_border
                for cc in range(col1+1, col2+1):
                    ws_exp.cell(row=row, column=cc).fill = fill
                    ws_exp.cell(row=row, column=cc).border = thin_border

            def write_cell(row, col, value, font=normal11, fill=None, align_style=center):
                c = ws_exp.cell(row=row, column=col, value=value)
                c.font = font; c.alignment = align_style; c.border = thin_border
                if fill: c.fill = fill

            # === ROW 1: Title ===
            write_merged(r, 1, 6, "بيان تسوية مستحقات نهاية الخدمة", white_font, dark_blue)
            ws_exp.row_dimensions[r].height = 35
            r += 1

            # === ROW 2: Employee Info Header ===
            write_merged(r, 1, 6, "بيانات الموظف", white_font12, med_blue)
            r += 1

            # === ROW 3: Name & ID ===
            bg = light1
            write_cell(r, 1, "اسم الموظف", bold11, bg, left)
            ws_exp.merge_cells(start_row=r, start_column=2, end_row=r, end_column=3)
            write_cell(r, 2, emp_name or "-", normal11, bg, center)
            ws_exp.cell(r,3).fill=bg; ws_exp.cell(r,3).border=thin_border
            write_cell(r, 4, "رقم الموظف", bold11, bg, left)
            ws_exp.merge_cells(start_row=r, start_column=5, end_row=r, end_column=6)
            write_cell(r, 5, emp_id or "-", normal11, bg, center)
            ws_exp.cell(r,6).fill=bg; ws_exp.cell(r,6).border=thin_border
            r += 1

            # === ROW 4: Dates ===
            bg = light2
            eos_start_str = eos_start.strftime('%Y-%m-%d') if hasattr(eos_start, 'strftime') else str(eos_start)
            eos_end_str = eos_end.strftime('%Y-%m-%d') if hasattr(eos_end, 'strftime') else str(eos_end)
            write_cell(r, 1, "تاريخ الالتحاق", bold11, bg, left)
            ws_exp.merge_cells(start_row=r, start_column=2, end_row=r, end_column=3)
            write_cell(r, 2, eos_start_str, normal11, bg, center)
            ws_exp.cell(r,3).fill=bg; ws_exp.cell(r,3).border=thin_border
            write_cell(r, 4, "آخر يوم عمل", bold11, bg, left)
            ws_exp.merge_cells(start_row=r, start_column=5, end_row=r, end_column=6)
            write_cell(r, 5, eos_end_str, normal11, bg, center)
            ws_exp.cell(r,6).fill=bg; ws_exp.cell(r,6).border=thin_border
            r += 1

            # === ROW 5: Service duration ===
            bg = light1
            write_cell(r, 1, "مدة الخدمة (سنوات)", bold11, bg, left)
            ws_exp.merge_cells(start_row=r, start_column=2, end_row=r, end_column=3)
            write_cell(r, 2, round(eos_years, 2), blue_val, bg, center)
            ws_exp.cell(r,3).fill=bg; ws_exp.cell(r,3).border=thin_border
            write_cell(r, 4, "مدة الخدمة (أيام)", bold11, bg, left)
            ws_exp.merge_cells(start_row=r, start_column=5, end_row=r, end_column=6)
            write_cell(r, 5, eos_service_days, blue_val, bg, center)
            ws_exp.cell(r,6).fill=bg; ws_exp.cell(r,6).border=thin_border
            r += 1

            # === ROW 6: Leave balance & total salary ===
            bg = light2
            write_cell(r, 1, "رصيد الإجازات (أيام)", bold11, bg, left)
            ws_exp.merge_cells(start_row=r, start_column=2, end_row=r, end_column=3)
            write_cell(r, 2, vac_days_input, blue_val, bg, center)
            ws_exp.cell(r,3).fill=bg; ws_exp.cell(r,3).border=thin_border
            write_cell(r, 4, "إجمالي الراتب الشهري", bold11, bg, left)
            ws_exp.merge_cells(start_row=r, start_column=5, end_row=r, end_column=6)
            write_cell(r, 5, round(gross_sal, 2), blue_val, bg, center)
            ws_exp.cell(r,6).fill=bg; ws_exp.cell(r,6).border=thin_border
            r += 1

            # === ROW 7: Salary Details Header ===
            write_merged(r, 1, 6, "تفاصيل الراتب الشهري", white_font12, med_blue)
            r += 1

            # === ROW 8: Salary table header ===
            sal_headers = ["#", "البند", "", "المبلغ (ريال)", "النسبة", ""]
            for i, h in enumerate(sal_headers, 1):
                write_cell(r, i, h, white_font11, hdr_blue, center)
            ws_exp.merge_cells(start_row=r, start_column=2, end_row=r, end_column=3)
            ws_exp.merge_cells(start_row=r, start_column=5, end_row=r, end_column=6)
            r += 1

            # === ROWS 9-12: Salary items ===
            sal_items = [
                ("الأجر الأساسي", basic_sal),
                ("بدل السكن", housing),
                ("بدل المواصلات", transport),
                ("بدلات أخرى", other_allow),
            ]
            for idx, (item, amt) in enumerate(sal_items, 1):
                bg = light1 if idx % 2 == 1 else light2
                pct = (amt / gross_sal * 100) if gross_sal > 0 else 0
                write_cell(r, 1, idx, normal11, bg, center)
                ws_exp.merge_cells(start_row=r, start_column=2, end_row=r, end_column=3)
                write_cell(r, 2, item, normal11, bg, left)
                ws_exp.cell(r,3).fill=bg; ws_exp.cell(r,3).border=thin_border
                write_cell(r, 4, round(amt, 2), blue_val, bg, center)
                ws_exp.merge_cells(start_row=r, start_column=5, end_row=r, end_column=6)
                write_cell(r, 5, f"{pct:.1f}%", normal11, bg, center)
                ws_exp.cell(r,6).fill=bg; ws_exp.cell(r,6).border=thin_border
                r += 1

            # === ROW 13: Total Salary ===
            ws_exp.merge_cells(start_row=r, start_column=2, end_row=r, end_column=3)
            write_cell(r, 2, "إجمالي الراتب", white_font12, dark_blue, center)
            ws_exp.cell(r,3).fill=dark_blue; ws_exp.cell(r,3).border=thin_border
            write_cell(r, 4, round(gross_sal, 2), white_font12, dark_blue, center)
            ws_exp.merge_cells(start_row=r, start_column=5, end_row=r, end_column=6)
            write_cell(r, 5, "100%", white_font12, dark_blue, center)
            ws_exp.cell(r,6).fill=dark_blue; ws_exp.cell(r,6).border=thin_border
            write_cell(r, 1, "", normal11, dark_blue, center)
            r += 1

            # === ROW 14: Benefits Details Header ===
            write_merged(r, 1, 6, "تفاصيل المستحقات", white_font12, med_blue)
            r += 1

            # === ROW 15: Benefits table header ===
            ben_headers = ["#", "البند", "طريقة الحساب", "التفاصيل", "المبلغ (ريال)", "السند النظامي"]
            for i, h in enumerate(ben_headers, 1):
                write_cell(r, i, h, white_font11, hdr_blue, center)
            r += 1

            # === Benefits rows ===
            ben_idx = 0
            for label, amount in results_summary:
                ben_idx += 1
                bg = light1 if ben_idx % 2 == 1 else light2
                is_ded = "خصم" in label or "حسم" in label

                # Determine calculation method and legal basis
                if "نهاية الخدمة" in label:
                    calc_method = f"نصف الراتب × السنوات (أول 5 سنوات) + الراتب × بقية السنوات"
                    details = f"{eos_years:.2f} سنة | مادة {'85' if is_85 else '84'} ({eos_pct}%)"
                    legal = f"المادة {'85' if is_85 else '84'}"
                elif "إجازة" in label:
                    calc_method = f"الراتب ÷ 30 × عدد أيام الإجازة"
                    details = f"{vac_days_input} يوم × {daily_sal:,.2f} ريال/يوم"
                    legal = "المادة 109"
                elif "متأخرة" in label:
                    calc_method = "الراتب ÷ 30 × عدد أيام التأخر"
                    details = f"{dw_total_days} يوم"
                    legal = "المادة 88"
                elif "إضافي" in label:
                    calc_method = "ساعة الإضافي (150%) × عدد الساعات"
                    details = f"{ot_total_hours} ساعة × {ot_rate:,.2f}"
                    legal = "المادة 107"
                elif "إنهاء" in label or "تعويض" in label:
                    calc_method = "تعويض الإنهاء لغير سبب مشروع"
                    details = f"نوع العقد: {contract_type}"
                    legal = "المادة 77"
                elif "حسم" in label or "غياب" in label:
                    calc_method = "الأجر اليومي × أيام الغياب + ساعات/دقائق التأخر"
                    details = "خصم من المستحقات"
                    legal = "نظام العمل"
                else:
                    calc_method = "-"; details = "-"; legal = "-"

                write_cell(r, 1, ben_idx, normal11, bg, center)
                write_cell(r, 2, label, normal11, bg, left)
                write_cell(r, 3, calc_method, small10, bg, center)
                write_cell(r, 4, details, small10, bg, center)
                val_font = red_val if is_ded else blue_val
                write_cell(r, 5, round(-amount if is_ded else amount, 2), val_font, bg, center)
                write_cell(r, 6, legal, small10, bg, center)
                r += 1

            # === Total Benefits Row ===
            ws_exp.merge_cells(start_row=r, start_column=2, end_row=r, end_column=4)
            write_cell(r, 1, "", normal11, dark_blue, center)
            write_cell(r, 2, "إجمالي المستحقات النهائية", white_font13, dark_blue, center)
            ws_exp.cell(r,3).fill=dark_blue; ws_exp.cell(r,3).border=thin_border
            ws_exp.cell(r,4).fill=dark_blue; ws_exp.cell(r,4).border=thin_border
            write_cell(r, 5, round(grand_total, 2), white_font13, dark_blue, center)
            write_cell(r, 6, "ريال سعودي", white_font11, dark_blue, center)
            r += 1

            # === Legal Basis Section ===
            write_merged(r, 1, 6, "السند النظامي", white_font12, med_blue)
            r += 1

            legal_notes = [
                "● المادة 84: مكافأة نهاية الخدمة - نصف الراتب عن كل سنة من الخمس الأولى وراتب كامل عن كل سنة بعدها",
                "● المادة 85: إذا كان إنهاء العلاقة بسبب استقالة العامل، يستحق ثلث المكافأة (2-5 سنوات)، ثلثيها (5-10)، كاملة (10+)",
                "● المادة 77: تعويض الإنهاء غير المشروع - يستحق العامل تعويضاً إذا لم يتضمن العقد تعويضاً محدداً",
                "● المادة 109: يستحق العامل تعويضاً نقدياً عن رصيد إجازاته المتراكمة عند انتهاء العلاقة",
                "● المادة 88: يجب على صاحب العمل دفع أجر العامل وتصفية حقوقه خلال أسبوع من تاريخ انتهاء العلاقة",
            ]
            for i, note in enumerate(legal_notes):
                bg = light1 if i % 2 == 0 else light2
                write_merged(r, 1, 6, note, small10, bg, left)
                r += 1

            # === Settlement Date ===
            write_cell(r, 1, "تاريخ إعداد التسوية:", gray10, None, left)
            write_cell(r, 2, datetime.now().strftime('%Y-%m-%d'), gray10, None, center)

            wb_exp.save(ox)

            fname = f"مستحقات_{emp_name or 'موظف'}_{datetime.now().strftime('%Y%m%d')}"
            xc1, xc2 = st.columns(2)
            with xc1:
                st.download_button("📥 تحميل Excel", data=ox.getvalue(), file_name=f"{fname}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", type="primary", use_container_width=True)
            with xc2:
                csv_df = pd.DataFrame(export_rows)
                csv_data = csv_df.to_csv(index=False).encode('utf-8-sig')
                st.download_button("📥 تحميل CSV", data=csv_data, file_name=f"{fname}.csv", mime="text/csv", use_container_width=True)
        else:
            st.info("عبّئ البيانات أعلاه وستظهر المستحقات هنا تلقائياً")

        ibox("إصدار استرشادي تقريبي ولا يغني عن الاستشارة القانونية المتخصصة.", "warning")

    # =========================================
    #         📚 TRAINING & DEVELOPMENT
    # =========================================
    elif section == "📚 التدريب والتطوير":

        if 'budget_data' not in st.session_state:
            st.session_state.budget_data = DEFAULT_BUDGET.copy()

        if page == "📚 ميزانية التدريب":
            hdr("📚 ميزانية التدريب","خطة توزيع ميزانية التدريب السنوية")
            c1,c2 = st.columns(2)
            with c1: total_budget = st.number_input("💰 إجمالي الميزانية (ريال)", 10000, 5000000, 70000, 5000)
            with c2: fy = st.selectbox("📅 السنة", [2025,2026,2027], index=1)

            budget_df = pd.DataFrame(st.session_state.budget_data)
            budget_df['budget'] = (budget_df['pct']/100*total_budget).astype(int)

            k1,k2,k3 = st.columns(3)
            with k1: kpi("الميزانية", f"{total_budget:,}")
            with k2: kpi("الأقسام", str(len(budget_df)))
            with k3:
                rev = budget_df[budget_df['cat']=='محرك إيرادات']['budget'].sum()
                kpi("محركات الإيرادات", f"{round(rev/total_budget*100)}%")

            edit_df = budget_df[['dept','budget','pct','priority','cat']].copy()
            edit_df.columns = ['القسم','الميزانية','النسبة %','الأولوية','التصنيف']
            st.dataframe(edit_df, use_container_width=True, hide_index=True)

            c1,c2 = st.columns(2)
            with c1:
                fig = px.pie(budget_df, values='budget', names='dept', title='توزيع الميزانية', hole=.35, color_discrete_sequence=CL['dept'])
                fig.update_layout(font=dict(family="Noto Sans Arabic"),height=380); st.plotly_chart(fig,use_container_width=True)
            with c2:
                cat_df = budget_df.groupby('cat')['budget'].sum().reset_index()
                fig = px.pie(cat_df, values='budget', names='cat', title='التوزيع الاستراتيجي', hole=.35,
                    color_discrete_map={'محرك إيرادات':CL['p'],'ممكّن نمو':CL['a'],'بنية تحتية':'#64748B'})
                fig.update_layout(font=dict(family="Noto Sans Arabic"),height=380); st.plotly_chart(fig,use_container_width=True)

            st.markdown("### 📅 الخطة ربع السنوية")
            q_data = []
            for _, r in budget_df.iterrows():
                qr = {"القسم":r['dept']}
                for q,p in Q_SPLIT.items(): qr[q] = int(r['budget']*p)
                qr['الإجمالي'] = r['budget']
                q_data.append(qr)
            q_df = pd.DataFrame(q_data)
            totals = {"القسم":"الإجمالي"}
            for c in ['Q1','Q2','Q3','Q4','الإجمالي']: totals[c] = q_df[c].sum()
            q_df = pd.concat([q_df, pd.DataFrame([totals])], ignore_index=True)
            st.dataframe(q_df, use_container_width=True, hide_index=True)

        elif page == "💹 ROI التدريب":
            hdr("💹 عائد التدريب ROI","نموذج Phillips ذو 5 مستويات")
            c1,c2 = st.columns(2)
            with c1:
                rb = st.number_input("💰 ميزانية التدريب", value=70000, step=5000)
                cr = st.number_input("📈 الإيرادات السنوية", value=5000000, step=100000)
                ri = st.slider("📊 الزيادة المتوقعة %", 1, 50, 15)
            with c2:
                hc2 = st.number_input("👥 عدد الموظفين", value=83)
                as2 = st.number_input("💵 متوسط الراتب الشهري", value=8000, step=500)
                rt = st.slider("🔄 تحسن الاحتفاظ %", 1, 30, 10)
                pg = st.slider("⚡ الإنتاجية %", 1, 30, 10)

            if st.button("📊 حساب ROI", type="primary", use_container_width=True):
                roi = calc_roi(rb, ri, cr, rt, as2*12, hc2, pg)
                k1,k2,k3 = st.columns(3)
                with k1: kpi("ROI", f"{roi['roi']:.0f}%")
                with k2: kpi("BCR", f"{roi['bcr']:.1f}x")
                with k3: kpi("الاسترداد", f"{roi['payback']:.1f} شهر")

                fig = go.Figure()
                fig.add_trace(go.Bar(x=['الإيرادات','الاحتفاظ','الإنتاجية'], y=[roi['rev'],roi['ret'],roi['prod']], marker_color=[CL['p'],CL['a'],CL['s']]))
                fig.add_hline(y=rb, line_dash="dash", line_color="red", annotation_text=f"التكلفة: {rb:,}")
                fig.update_layout(title='العوائد مقابل التكلفة', font=dict(family="Noto Sans Arabic"), height=380, yaxis_tickformat=',')
                st.plotly_chart(fig, use_container_width=True)

        elif page == "📋 الاحتياجات التدريبية":
            hdr("📋 تحليل الاحتياجات التدريبية")
            cats = {"المبيعات":["بيع استشاري","CRM","تفاوض"],"التسويق":["تسويق رقمي","SEO","Growth Hacking"],
                    "التقنية":["Python/SQL","Power BI","AI"],"المالية":["IFRS","نمذجة مالية"],"الموارد البشرية":["استقطاب","أداء","OKRs"]}
            depts = st.multiselect("📌 الأقسام", list(cats.keys()), default=list(cats.keys())[:3])
            needs = []
            for d in depts:
                with st.expander(f"📌 {d}", expanded=True):
                    skills = st.multiselect(f"المهارات", cats[d], default=cats[d][:2], key=f"s_{d}")
                    for s in skills:
                        c1,c2 = st.columns(2)
                        with c1: lv = st.select_slider(f"الحالي: {s}", ["مبتدئ","أساسي","متوسط","متقدم","خبير"], value="أساسي", key=f"l_{d}_{s}")
                        with c2: tg = st.select_slider(f"المستهدف: {s}", ["مبتدئ","أساسي","متوسط","متقدم","خبير"], value="متقدم", key=f"t_{d}_{s}")
                        levels = ["مبتدئ","أساسي","متوسط","متقدم","خبير"]
                        needs.append({"القسم":d,"المهارة":s,"الحالي":lv,"المستهدف":tg,"الفجوة":levels.index(tg)-levels.index(lv)})
            if needs:
                ndf = pd.DataFrame(needs)
                st.dataframe(ndf, use_container_width=True, hide_index=True)
                fig = px.bar(ndf, x='المهارة', y='الفجوة', color='القسم', title='خريطة الفجوات', color_discrete_sequence=CL['dept'])
                fig.update_layout(font=dict(family="Noto Sans Arabic"),height=380); st.plotly_chart(fig,use_container_width=True)

        elif page == "🏫 جهات التدريب":
            hdr("🏫 دليل جهات التدريب")
            market = st.selectbox("🌍 السوق:", list(PROVIDERS.keys()))
            for p in PROVIDERS[market]:
                st.markdown(f"**{p['name']}** | {p['spec']} | {p['type']} | [{p['url']}](https://{p['url']})")
                st.markdown("---")

        elif page == "📥 تصدير التدريب":
            hdr("📥 تصدير تقارير التدريب")
            o = io.BytesIO()
            with pd.ExcelWriter(o, engine='xlsxwriter') as w:
                pd.DataFrame(st.session_state.budget_data).to_excel(w, sheet_name='الميزانية', index=False)
                all_p = []
                for m, ps in PROVIDERS.items():
                    for p in ps: all_p.append({"السوق":m,"الجهة":p['name'],"التخصص":p['spec'],"النوع":p['type']})
                pd.DataFrame(all_p).to_excel(w, sheet_name='جهات التدريب', index=False)
            st.download_button("📥 تحميل", data=o.getvalue(),
                file_name=f"Training_{datetime.now().strftime('%Y%m%d')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", type="primary", use_container_width=True)


    # =========================================
    #         🎯 RECRUITMENT MODULE
    # =========================================
    elif section == "🎯 التوظيف":

        # Initialize recruitment data
        if 'recruit_plans' not in st.session_state:
            st.session_state.recruit_plans = []
        if 'recruit_tracking' not in st.session_state:
            st.session_state.recruit_tracking = []

        if page == "📋 تخطيط التوظيف":
            hdr("📋 تخطيط ميزانية التوظيف", "تخطيط تكاليف التوظيف الجديد وتقدير الميزانية السنوية")

            st.markdown("### ➕ إضافة وظيفة جديدة للخطة")
            rc1, rc2, rc3 = st.columns(3)
            with rc1:
                rp_title = st.text_input("المسمى الوظيفي:", key="rpt")
                rp_dept = st.text_input("القسم:", key="rpd")
                rp_count = st.number_input("عدد المطلوب:", 1, 50, 1, key="rpc")
            with rc2:
                rp_salary = st.number_input("الراتب الشهري المتوقع:", 0.0, 200000.0, 5000.0, 100.0, format="%.2f", key="rps")
                rp_housing_pct = st.number_input("% بدل السكن:", 0.0, 100.0, 25.0, key="rph")
                rp_transport = st.number_input("بدل المواصلات:", 0.0, 10000.0, 500.0, format="%.2f", key="rptr")
            with rc3:
                rp_agency_fee = st.number_input("رسوم الاستقدام/التوظيف:", 0.0, 100000.0, 0.0, format="%.2f", key="rpaf")
                rp_visa = st.number_input("تكلفة التأشيرة/الإقامة:", 0.0, 50000.0, 0.0, format="%.2f", key="rpv")
                rp_training = st.number_input("تكلفة التدريب الأولي:", 0.0, 50000.0, 0.0, format="%.2f", key="rptrn")
                rp_nationality = st.selectbox("الجنسية:", ["سعودي","غير سعودي"], key="rpnat")

            rp_housing = rp_salary * (rp_housing_pct / 100)
            rp_monthly_total = rp_salary + rp_housing + rp_transport
            rp_gosi = (rp_salary + rp_housing) * 0.1175 if rp_nationality == "سعودي" else (rp_salary + rp_housing) * 0.02
            rp_annual_per = (rp_monthly_total + rp_gosi) * 12 + rp_agency_fee + rp_visa + rp_training
            rp_annual_total = rp_annual_per * rp_count

            st.info(f"💰 التكلفة الشهرية للفرد: **{rp_monthly_total + rp_gosi:,.2f}** | السنوية للفرد: **{rp_annual_per:,.2f}** | الإجمالي ({rp_count}): **{rp_annual_total:,.2f} ريال**")

            if st.button("➕ إضافة للخطة", type="primary", key="rpbtn"):
                st.session_state.recruit_plans.append({
                    "المسمى": rp_title, "القسم": rp_dept, "العدد": rp_count,
                    "الجنسية": rp_nationality, "الراتب": rp_salary,
                    "السكن": rp_housing, "المواصلات": rp_transport,
                    "التأمينات (صاحب العمل)": round(rp_gosi, 2),
                    "الشهري/فرد": round(rp_monthly_total + rp_gosi, 2),
                    "رسوم التوظيف": rp_agency_fee, "التأشيرة": rp_visa,
                    "التدريب": rp_training, "السنوي/فرد": round(rp_annual_per, 2),
                    "الإجمالي السنوي": round(rp_annual_total, 2)
                })
                st.success(f"✅ تمت إضافة {rp_title} ({rp_count})")
                st.rerun()

            # Display plan
            if st.session_state.recruit_plans:
                st.markdown("---")
                st.markdown("### 📊 خطة التوظيف الحالية")
                plan_df = pd.DataFrame(st.session_state.recruit_plans)
                st.dataframe(plan_df, use_container_width=True, hide_index=True)

                # Summary
                total_headcount = plan_df["العدد"].sum()
                total_annual = plan_df["الإجمالي السنوي"].sum()
                total_monthly = plan_df.apply(lambda r: r["الشهري/فرد"] * r["العدد"], axis=1).sum()
                total_onetime = plan_df.apply(lambda r: (r["رسوم التوظيف"] + r["التأشيرة"] + r["التدريب"]) * r["العدد"], axis=1).sum()

                k1,k2,k3,k4 = st.columns(4)
                with k1: kpi("👥 إجمالي المطلوب", f"{total_headcount}")
                with k2: kpi("💰 التكلفة الشهرية", f"{total_monthly:,.0f}")
                with k3: kpi("📅 التكلفة السنوية", f"{total_annual:,.0f}")
                with k4: kpi("🔑 تكاليف لمرة واحدة", f"{total_onetime:,.0f}")

                # Charts
                ch1, ch2 = st.columns(2)
                with ch1:
                    fig = px.pie(plan_df, names="القسم", values="الإجمالي السنوي", title="توزيع الميزانية حسب القسم")
                    fig.update_layout(font=dict(family="Noto Sans Arabic"), height=350)
                    st.plotly_chart(fig, use_container_width=True)
                with ch2:
                    fig = px.bar(plan_df, x="المسمى", y="الإجمالي السنوي", color="الجنسية", title="التكلفة حسب الوظيفة", text_auto=True)
                    fig.update_layout(font=dict(family="Noto Sans Arabic"), height=350)
                    st.plotly_chart(fig, use_container_width=True)

                if st.button("🗑️ مسح الخطة بالكامل", key="rpclr"):
                    st.session_state.recruit_plans = []
                    st.rerun()

        elif page == "📊 متابعة التوظيف":
            hdr("📊 متابعة عمليات التوظيف", "تتبع مراحل التوظيف والتكاليف والوقت المستغرق")

            STAGES = ["طلب توظيف","إعلان","فرز السير الذاتية","مقابلة أولية","مقابلة نهائية","عرض وظيفي","قبول","مباشرة"]

            st.markdown("### ➕ إضافة عملية توظيف")
            tc1, tc2, tc3 = st.columns(3)
            with tc1:
                tr_title = st.text_input("المسمى الوظيفي:", key="trt")
                tr_dept = st.text_input("القسم:", key="trd")
            with tc2:
                tr_stage = st.selectbox("المرحلة الحالية:", STAGES, key="trs")
                tr_candidates = st.number_input("عدد المرشحين:", 0, 500, 0, key="trc")
            with tc3:
                tr_start = st.date_input("تاريخ البدء:", value=date.today(), key="trst")
                tr_budget = st.number_input("الميزانية المخصصة:", 0.0, 500000.0, 0.0, format="%.2f", key="trb")
                tr_spent = st.number_input("المصروف حتى الآن:", 0.0, 500000.0, 0.0, format="%.2f", key="trsp")

            if st.button("➕ إضافة", type="primary", key="trbtn"):
                days_elapsed = (date.today() - tr_start).days
                st.session_state.recruit_tracking.append({
                    "المسمى": tr_title, "القسم": tr_dept, "المرحلة": tr_stage,
                    "المرشحين": tr_candidates, "تاريخ البدء": str(tr_start),
                    "الأيام": days_elapsed, "الميزانية": tr_budget,
                    "المصروف": tr_spent, "المتبقي": round(tr_budget - tr_spent, 2),
                    "التقدم %": round((STAGES.index(tr_stage) + 1) / len(STAGES) * 100)
                })
                st.success(f"✅ تمت إضافة {tr_title}")
                st.rerun()

            if st.session_state.recruit_tracking:
                st.markdown("---")
                st.markdown("### 📋 العمليات الجارية")
                track_df = pd.DataFrame(st.session_state.recruit_tracking)
                st.dataframe(track_df, use_container_width=True, hide_index=True)

                # KPIs
                avg_days = track_df["الأيام"].mean()
                total_budget = track_df["الميزانية"].sum()
                total_spent = track_df["المصروف"].sum()
                total_candidates = track_df["المرشحين"].sum()
                open_positions = len(track_df[track_df["المرحلة"] != "مباشرة"])

                k1,k2,k3,k4,k5 = st.columns(5)
                with k1: kpi("📋 العمليات", f"{len(track_df)}")
                with k2: kpi("⏱️ متوسط الأيام", f"{avg_days:.0f}")
                with k3: kpi("👥 المرشحين", f"{total_candidates}")
                with k4: kpi("💰 المصروف/الميزانية", f"{total_spent:,.0f}/{total_budget:,.0f}")
                with k5: kpi("📂 مفتوحة", f"{open_positions}")

                # Pipeline chart
                stage_counts = track_df["المرحلة"].value_counts().reindex(STAGES, fill_value=0)
                fig = go.Figure(go.Funnel(y=stage_counts.index, x=stage_counts.values, textinfo="value+percent initial"))
                fig.update_layout(title="مسار التوظيف (Funnel)", font=dict(family="Noto Sans Arabic"), height=400)
                st.plotly_chart(fig, use_container_width=True)

                if st.button("🗑️ مسح المتابعة", key="trclr"):
                    st.session_state.recruit_tracking = []
                    st.rerun()

        elif page == "📥 تصدير التوظيف":
            hdr("📥 تصدير بيانات التوظيف")
            ox = io.BytesIO()
            with pd.ExcelWriter(ox, engine='xlsxwriter') as w:
                if st.session_state.recruit_plans:
                    pd.DataFrame(st.session_state.recruit_plans).to_excel(w, sheet_name='خطة التوظيف', index=False)
                    ws = w.sheets['خطة التوظيف']; ws.right_to_left()
                if st.session_state.recruit_tracking:
                    pd.DataFrame(st.session_state.recruit_tracking).to_excel(w, sheet_name='متابعة التوظيف', index=False)
                    ws = w.sheets['متابعة التوظيف']; ws.right_to_left()
                if not st.session_state.recruit_plans and not st.session_state.recruit_tracking:
                    pd.DataFrame({"ملاحظة": ["لا توجد بيانات"]}).to_excel(w, sheet_name='فارغ', index=False)
            st.download_button("📥 تحميل Excel", data=ox.getvalue(),
                file_name=f"Recruitment_{datetime.now().strftime('%Y%m%d')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", type="primary", use_container_width=True)


    # =========================================
    #         🔍 GENERAL ANALYSIS MODULE
    # =========================================
    elif section == "🔍 التحليل العام":

        if page == "📊 تحليل تلقائي":
            hdr("📊 التحليل التلقائي للبيانات", "ارفع أي ملف Excel وسيتم تحليله تلقائياً")

            ga_file = st.file_uploader("📁 ارفع ملف Excel أو CSV:", type=["xlsx","xls","csv"], key="ga_uploader")
            ga_df = pd.DataFrame()

            if ga_file:
                try:
                    if ga_file.name.endswith('.csv'):
                        ga_df = pd.read_csv(ga_file)
                    else:
                        ga_xl = pd.ExcelFile(ga_file)
                        if len(ga_xl.sheet_names) > 1:
                            ga_sheet = st.selectbox("اختر الشيت:", ga_xl.sheet_names, key="ga_sh")
                        else:
                            ga_sheet = ga_xl.sheet_names[0]
                        ga_df = pd.read_excel(ga_xl, ga_sheet)
                except Exception as e:
                    st.error(f"خطأ في قراءة الملف: {e}")

            elif len(emp) > 0:
                ga_df = emp.copy()
                st.info("📂 يتم تحليل بيانات الموظفين المرفوعة في القائمة الجانبية")

            if len(ga_df) > 0:
                st.markdown("---")
                st.markdown("### 📊 نظرة عامة على البيانات")

                gi1, gi2, gi3, gi4 = st.columns(4)
                with gi1: kpi("📋 الصفوف", f"{len(ga_df):,}")
                with gi2: kpi("📊 الأعمدة", f"{len(ga_df.columns)}")
                with gi3: kpi("❌ القيم الفارغة", f"{ga_df.isnull().sum().sum():,}")
                with gi4: kpi("🔢 الأعمدة الرقمية", f"{len(ga_df.select_dtypes('number').columns)}")

                # Data types summary
                st.markdown("### 📋 هيكل البيانات")
                dtype_data = []
                for col in ga_df.columns:
                    dtype_data.append({
                        "العمود": col,
                        "النوع": str(ga_df[col].dtype),
                        "القيم الفريدة": ga_df[col].nunique(),
                        "الفارغة": ga_df[col].isnull().sum(),
                        "عينة": str(ga_df[col].dropna().iloc[0]) if len(ga_df[col].dropna()) > 0 else "-"
                    })
                st.dataframe(pd.DataFrame(dtype_data), use_container_width=True, hide_index=True)

                # Numeric columns analysis
                num_cols = ga_df.select_dtypes('number').columns.tolist()
                cat_cols = [c for c in ga_df.columns if ga_df[c].dtype == 'object' and ga_df[c].nunique() < 30 and ga_df[c].nunique() > 1]

                if num_cols:
                    st.markdown("### 📈 الإحصائيات الوصفية")
                    desc = ga_df[num_cols].describe().T
                    desc.columns = ["العدد","المتوسط","الانحراف","الأدنى","25%","الوسيط","75%","الأقصى"]
                    st.dataframe(desc.style.format("{:,.2f}"), use_container_width=True)

                    # Auto charts
                    st.markdown("### 📊 رسوم بيانية تلقائية")

                    # Histogram for numeric columns
                    sel_num = st.selectbox("اختر عمود رقمي:", num_cols, key="ga_num")
                    fig = px.histogram(ga_df, x=sel_num, nbins=30, title=f"توزيع: {sel_num}", color_discrete_sequence=[CL['p']])
                    fig.update_layout(font=dict(family="Noto Sans Arabic"), height=350)
                    st.plotly_chart(fig, use_container_width=True)

                    if len(num_cols) >= 2:
                        sc1, sc2 = st.columns(2)
                        with sc1: sel_x = st.selectbox("المحور X:", num_cols, index=0, key="ga_x")
                        with sc2: sel_y = st.selectbox("المحور Y:", num_cols, index=min(1, len(num_cols)-1), key="ga_y")
                        color_col = st.selectbox("التلوين حسب (اختياري):", ["بدون"] + cat_cols, key="ga_clr") if cat_cols else "بدون"
                        fig = px.scatter(ga_df, x=sel_x, y=sel_y, color=None if color_col=="بدون" else color_col,
                            title=f"{sel_y} vs {sel_x}", opacity=0.6)
                        fig.update_layout(font=dict(family="Noto Sans Arabic"), height=400)
                        st.plotly_chart(fig, use_container_width=True)

                if cat_cols:
                    st.markdown("### 📊 تحليل الأعمدة النصية")
                    sel_cat = st.selectbox("اختر عمود:", cat_cols, key="ga_cat")
                    vc = ga_df[sel_cat].value_counts().head(15)
                    fig = px.bar(x=vc.index, y=vc.values, title=f"توزيع: {sel_cat}", labels={"x": sel_cat, "y": "العدد"}, color_discrete_sequence=[CL['a']])
                    fig.update_layout(font=dict(family="Noto Sans Arabic"), height=350)
                    st.plotly_chart(fig, use_container_width=True)

                    # Cross analysis
                    if num_cols and cat_cols:
                        st.markdown("### 📊 تحليل متقاطع")
                        cx1, cx2 = st.columns(2)
                        with cx1: cross_cat = st.selectbox("التصنيف:", cat_cols, key="ga_cc")
                        with cx2: cross_num = st.selectbox("القيمة:", num_cols, key="ga_cn")
                        cross_agg = ga_df.groupby(cross_cat)[cross_num].agg(['mean','sum','count']).reset_index()
                        cross_agg.columns = [cross_cat, "المتوسط", "الإجمالي", "العدد"]
                        fig = px.bar(cross_agg, x=cross_cat, y="المتوسط", title=f"متوسط {cross_num} حسب {cross_cat}", text_auto=".1f", color_discrete_sequence=[CL['s']])
                        fig.update_layout(font=dict(family="Noto Sans Arabic"), height=350)
                        st.plotly_chart(fig, use_container_width=True)

                # Correlation heatmap
                if len(num_cols) >= 3:
                    st.markdown("### 🔥 خريطة الارتباط")
                    corr = ga_df[num_cols].corr()
                    fig = px.imshow(corr, text_auto=".2f", aspect="auto", title="مصفوفة الارتباط", color_continuous_scale="RdBu_r")
                    fig.update_layout(font=dict(family="Noto Sans Arabic"), height=500)
                    st.plotly_chart(fig, use_container_width=True)

                # Raw data
                with st.expander("📋 عرض البيانات الخام"):
                    st.dataframe(ga_df, use_container_width=True, height=400)

        elif page == "🤖 أسئلة ذكية":
            hdr("🤖 المحلل الذكي", "اطرح أسئلة عن بياناتك بالعربي أو الإنجليزي")

            if len(emp) > 0:
                st.success(f"📂 البيانات جاهزة: {len(emp):,} صف × {len(emp.columns)} عمود")

                q = st.text_input("💬 اسأل عن بياناتك:", placeholder="مثال: كم متوسط الرواتب حسب القسم؟ أو ما أعلى 5 رواتب؟", key="ga_q")

                if q:
                    num_cols = emp.select_dtypes('number').columns.tolist()
                    cat_cols = [c for c in emp.columns if emp[c].dtype=='object' and emp[c].nunique() < 50 and emp[c].nunique() > 1]
                    ql = q.lower()

                    try:
                        # Pattern matching for common questions
                        if any(w in ql for w in ['متوسط','average','mean']):
                            if num_cols:
                                matched_num = None
                                for nc in num_cols:
                                    if any(p in ql for p in [nc.lower(), nc.replace('_',' ').lower()]):
                                        matched_num = nc; break
                                if not matched_num: matched_num = num_cols[0]

                                matched_cat = None
                                for cc in cat_cols:
                                    if any(p in ql for p in [cc.lower(), cc.replace('_',' ').lower(), 'قسم','department','div']):
                                        matched_cat = cc; break

                                if matched_cat:
                                    result = emp.groupby(matched_cat)[matched_num].mean().sort_values(ascending=False)
                                    st.dataframe(result.reset_index().rename(columns={matched_num: f"متوسط {matched_num}"}), use_container_width=True, hide_index=True)
                                    fig = px.bar(x=result.index, y=result.values, title=f"متوسط {matched_num} حسب {matched_cat}", text_auto=".1f")
                                    fig.update_layout(font=dict(family="Noto Sans Arabic"), height=350)
                                    st.plotly_chart(fig, use_container_width=True)
                                else:
                                    for nc in num_cols:
                                        st.metric(nc, f"{emp[nc].mean():,.2f}")

                        elif any(w in ql for w in ['أعلى','top','highest','أكبر','max']):
                            n = 5
                            for w in ql.split():
                                try: n = int(w); break
                                except: pass
                            sort_col = num_cols[0] if num_cols else emp.columns[0]
                            for nc in num_cols:
                                if nc.lower() in ql: sort_col = nc; break
                            st.dataframe(emp.nlargest(n, sort_col), use_container_width=True, hide_index=True)

                        elif any(w in ql for w in ['أقل','bottom','lowest','أصغر','min']):
                            n = 5
                            for w in ql.split():
                                try: n = int(w); break
                                except: pass
                            sort_col = num_cols[0] if num_cols else emp.columns[0]
                            for nc in num_cols:
                                if nc.lower() in ql: sort_col = nc; break
                            st.dataframe(emp.nsmallest(n, sort_col), use_container_width=True, hide_index=True)

                        elif any(w in ql for w in ['عدد','count','كم','how many']):
                            matched_cat = None
                            for cc in cat_cols:
                                if any(p in ql for p in [cc.lower(), cc.replace('_',' ').lower()]):
                                    matched_cat = cc; break
                            if matched_cat:
                                vc = emp[matched_cat].value_counts()
                                st.dataframe(vc.reset_index().rename(columns={matched_cat: "الفئة", "count": "العدد"}), use_container_width=True, hide_index=True)
                                fig = px.pie(names=vc.index, values=vc.values, title=f"توزيع {matched_cat}")
                                fig.update_layout(font=dict(family="Noto Sans Arabic"), height=350)
                                st.plotly_chart(fig, use_container_width=True)
                            else:
                                st.metric("إجمالي الصفوف", f"{len(emp):,}")

                        elif any(w in ql for w in ['مجموع','total','sum','إجمالي']):
                            for nc in num_cols:
                                if nc.lower() in ql or any(p in ql for p in nc.lower().split('_')):
                                    st.metric(f"مجموع {nc}", f"{emp[nc].sum():,.2f}")
                                    break
                            else:
                                if num_cols:
                                    sums = {nc: emp[nc].sum() for nc in num_cols}
                                    st.dataframe(pd.DataFrame({"العمود": sums.keys(), "المجموع": [f"{v:,.2f}" for v in sums.values()]}), use_container_width=True, hide_index=True)

                        elif any(w in ql for w in ['توزيع','distribution','histogram']):
                            matched_col = num_cols[0] if num_cols else None
                            for nc in num_cols:
                                if nc.lower() in ql: matched_col = nc; break
                            if matched_col:
                                fig = px.histogram(emp, x=matched_col, nbins=25, title=f"توزيع {matched_col}")
                                fig.update_layout(font=dict(family="Noto Sans Arabic"), height=350)
                                st.plotly_chart(fig, use_container_width=True)

                        elif any(w in ql for w in ['أعمدة','columns','حقول','fields']):
                            st.write("📋 الأعمدة المتاحة:")
                            for i, c in enumerate(emp.columns, 1):
                                st.write(f"  {i}. **{c}** ({emp[c].dtype})")

                        else:
                            st.warning("💡 حاول صياغة السؤال بطريقة مختلفة. أمثلة:")
                            st.write("- ما متوسط الرواتب حسب القسم؟")
                            st.write("- أعلى 10 رواتب")
                            st.write("- عدد الموظفين حسب الجنسية")
                            st.write("- توزيع الأعمار")
                            st.write("- مجموع الرواتب")

                    except Exception as e:
                        st.error(f"خطأ: {e}")
                        st.info("حاول سؤال آخر أو تأكد من البيانات")
            else:
                ibox("ارفع ملف بيانات من القائمة الجانبية أولاً.", "warning")


    # =========================================
    #       📤 REPORTS & EXPORT MODULE
    # =========================================
    elif section == "📤 التقارير والتصدير":

        if page == "📄 تقرير PDF":
            hdr("📄 تصدير تقرير PDF احترافي", "تقرير شامل بشعار الشركة وتنسيق رسمي")

            if len(emp) > 0:
                st.markdown("### ⚙️ إعدادات التقرير")
                pc1, pc2 = st.columns(2)
                with pc1:
                    pdf_title = st.text_input("عنوان التقرير:", "تقرير الموارد البشرية", key="pdft")
                    pdf_company = st.text_input("اسم الشركة:", "رسال الود لتقنية المعلومات", key="pdfc")
                    pdf_prepared = st.text_input("إعداد:", emp_name if 'emp_name' in dir() else "", key="pdfp")
                with pc2:
                    pdf_period = st.text_input("الفترة:", datetime.now().strftime('%Y'), key="pdfpr")
                    pdf_sections = st.multiselect("الأقسام المطلوبة:",
                        ["ملخص تنفيذي","إحصائيات القوى العاملة","تحليل الرواتب","توزيع الأقسام","توزيع الجنسيات"],
                        default=["ملخص تنفيذي","إحصائيات القوى العاملة"],
                        key="pdfsec")

                if st.button("📄 إنشاء تقرير PDF", type="primary", key="pdfbtn"):
                    try:
                        from fpdf import FPDF

                        class ArabicPDF(FPDF):
                            def header(self):
                                self.set_fill_color(15, 76, 92)
                                self.rect(0, 0, 210, 25, 'F')
                                self.set_text_color(255, 255, 255)
                                self.set_font('Helvetica', 'B', 14)
                                self.cell(0, 25, pdf_company, align='C', ln=True)

                            def footer(self):
                                self.set_y(-15)
                                self.set_text_color(128, 128, 128)
                                self.set_font('Helvetica', '', 8)
                                self.cell(0, 10, f'Page {self.page_no()}/{{nb}} | {datetime.now().strftime("%Y-%m-%d")}', align='C')

                        pdf = ArabicPDF()
                        pdf.alias_nb_pages()
                        pdf.add_page()
                        pdf.set_auto_page_break(auto=True, margin=20)

                        # Title
                        pdf.ln(30)
                        pdf.set_text_color(15, 76, 92)
                        pdf.set_font('Helvetica', 'B', 20)
                        pdf.cell(0, 12, pdf_title, align='C', ln=True)
                        pdf.set_font('Helvetica', '', 11)
                        pdf.set_text_color(100, 100, 100)
                        pdf.cell(0, 8, f"Period: {pdf_period} | Prepared by: {pdf_prepared}", align='C', ln=True)
                        pdf.ln(10)

                        n = len(emp)
                        num_cols = emp.select_dtypes('number').columns.tolist()

                        if "ملخص تنفيذي" in pdf_sections:
                            pdf.set_fill_color(46, 117, 182)
                            pdf.set_text_color(255, 255, 255)
                            pdf.set_font('Helvetica', 'B', 13)
                            pdf.cell(0, 10, '  Executive Summary', fill=True, ln=True)
                            pdf.set_text_color(0, 0, 0)
                            pdf.set_font('Helvetica', '', 10)
                            pdf.ln(5)
                            pdf.cell(0, 7, f"Total Employees: {n}", ln=True)
                            sal_col = [c for c in num_cols if 'gross' in c.lower() or 'salary' in c.lower() or 'net' in c.lower()]
                            if sal_col:
                                sc = sal_col[0]
                                latest = emp.drop_duplicates(subset=[c for c in emp.columns if 'id' in c.lower() or 'name' in c.lower()][:1], keep='last') if any('id' in c.lower() for c in emp.columns) else emp
                                pdf.cell(0, 7, f"Avg Salary ({sc}): {latest[sc].mean():,.2f} SAR", ln=True)
                                pdf.cell(0, 7, f"Total Payroll: {latest[sc].sum():,.2f} SAR", ln=True)
                            pdf.ln(8)

                        if "إحصائيات القوى العاملة" in pdf_sections:
                            pdf.set_fill_color(46, 117, 182)
                            pdf.set_text_color(255, 255, 255)
                            pdf.set_font('Helvetica', 'B', 13)
                            pdf.cell(0, 10, '  Workforce Statistics', fill=True, ln=True)
                            pdf.set_text_color(0, 0, 0)
                            pdf.set_font('Helvetica', '', 10)
                            pdf.ln(5)

                            cat_cols = [c for c in emp.columns if emp[c].dtype=='object' and 1 < emp[c].nunique() < 30]
                            for cc in cat_cols[:5]:
                                pdf.set_font('Helvetica', 'B', 10)
                                pdf.cell(0, 7, f"{cc}:", ln=True)
                                pdf.set_font('Helvetica', '', 9)
                                for val, cnt in emp[cc].value_counts().head(8).items():
                                    pct = cnt/len(emp)*100
                                    pdf.cell(0, 6, f"    {val}: {cnt} ({pct:.1f}%)", ln=True)
                                pdf.ln(3)

                        if "تحليل الرواتب" in pdf_sections and num_cols:
                            pdf.add_page()
                            pdf.set_fill_color(46, 117, 182)
                            pdf.set_text_color(255, 255, 255)
                            pdf.set_font('Helvetica', 'B', 13)
                            pdf.cell(0, 10, '  Salary Analysis', fill=True, ln=True)
                            pdf.set_text_color(0, 0, 0)
                            pdf.set_font('Helvetica', '', 10)
                            pdf.ln(5)

                            # Stats table
                            pdf.set_font('Helvetica', 'B', 9)
                            pdf.set_fill_color(230, 240, 250)
                            headers = ['Column', 'Mean', 'Min', 'Max', 'Std']
                            widths = [50, 35, 35, 35, 35]
                            for i, h in enumerate(headers):
                                pdf.cell(widths[i], 8, h, border=1, fill=True, align='C')
                            pdf.ln()
                            pdf.set_font('Helvetica', '', 8)
                            for nc in num_cols[:10]:
                                vals = [nc[:25], f"{emp[nc].mean():,.1f}", f"{emp[nc].min():,.1f}", f"{emp[nc].max():,.1f}", f"{emp[nc].std():,.1f}"]
                                for i, v in enumerate(vals):
                                    pdf.cell(widths[i], 7, v, border=1, align='C')
                                pdf.ln()

                        pdf_bytes = pdf.output()
                        st.download_button("📥 تحميل PDF", data=bytes(pdf_bytes),
                            file_name=f"{pdf_title}_{datetime.now().strftime('%Y%m%d')}.pdf",
                            mime="application/pdf", type="primary", use_container_width=True)
                        st.success("✅ تم إنشاء التقرير بنجاح!")

                    except ImportError:
                        st.error("مكتبة fpdf2 غير مثبتة. أضف `fpdf2` في requirements.txt")
                    except Exception as e:
                        st.error(f"خطأ: {e}")
            else:
                ibox("ارفع ملف بيانات أولاً من القائمة الجانبية.", "warning")

        elif page == "📝 تقرير Word":
            hdr("📝 تصدير تقرير Word احترافي", "مستند Word منسق بشكل احترافي")

            if len(emp) > 0:
                wc1, wc2 = st.columns(2)
                with wc1:
                    word_title = st.text_input("عنوان التقرير:", "تقرير الموارد البشرية", key="wdt")
                    word_company = st.text_input("اسم الشركة:", "رسال الود لتقنية المعلومات", key="wdc")
                with wc2:
                    word_prepared = st.text_input("إعداد:", "", key="wdp")
                    word_period = st.text_input("الفترة:", datetime.now().strftime('%Y'), key="wdpr")

                if st.button("📝 إنشاء تقرير Word", type="primary", key="wdbtn"):
                    try:
                        from docx import Document
                        from docx.shared import Inches, Pt, Cm, RGBColor
                        from docx.enum.text import WD_ALIGN_PARAGRAPH
                        from docx.enum.table import WD_TABLE_ALIGNMENT

                        doc = Document()

                        # Style adjustments
                        style = doc.styles['Normal']
                        style.font.name = 'Calibri'
                        style.font.size = Pt(11)

                        # Title
                        p = doc.add_paragraph()
                        p.alignment = WD_ALIGN_PARAGRAPH.CENTER
                        run = p.add_run(word_company)
                        run.font.size = Pt(18)
                        run.font.bold = True
                        run.font.color.rgb = RGBColor(15, 76, 92)

                        p = doc.add_paragraph()
                        p.alignment = WD_ALIGN_PARAGRAPH.CENTER
                        run = p.add_run(word_title)
                        run.font.size = Pt(14)
                        run.font.color.rgb = RGBColor(46, 117, 182)

                        p = doc.add_paragraph()
                        p.alignment = WD_ALIGN_PARAGRAPH.CENTER
                        run = p.add_run(f"Period: {word_period} | Prepared by: {word_prepared} | Date: {datetime.now().strftime('%Y-%m-%d')}")
                        run.font.size = Pt(9)
                        run.font.color.rgb = RGBColor(128, 128, 128)

                        doc.add_paragraph("_" * 60)

                        # Summary
                        doc.add_heading('Executive Summary', level=1)
                        n = len(emp)
                        num_cols = emp.select_dtypes('number').columns.tolist()
                        doc.add_paragraph(f"Total Records: {n:,}")

                        # Category breakdown
                        cat_cols = [c for c in emp.columns if emp[c].dtype=='object' and 1 < emp[c].nunique() < 30]
                        for cc in cat_cols[:4]:
                            doc.add_heading(cc, level=2)
                            table = doc.add_table(rows=1, cols=3)
                            table.style = 'Light Grid Accent 1'
                            table.alignment = WD_TABLE_ALIGNMENT.CENTER
                            hdr_cells = table.rows[0].cells
                            hdr_cells[0].text = cc; hdr_cells[1].text = 'Count'; hdr_cells[2].text = '%'
                            for val, cnt in emp[cc].value_counts().head(8).items():
                                row_cells = table.add_row().cells
                                row_cells[0].text = str(val)
                                row_cells[1].text = str(cnt)
                                row_cells[2].text = f"{cnt/n*100:.1f}%"
                            doc.add_paragraph("")

                        # Numeric summary
                        if num_cols:
                            doc.add_heading('Numeric Summary', level=1)
                            table = doc.add_table(rows=1, cols=5)
                            table.style = 'Light Grid Accent 1'
                            table.alignment = WD_TABLE_ALIGNMENT.CENTER
                            for i, h in enumerate(['Column', 'Mean', 'Min', 'Max', 'Std']):
                                table.rows[0].cells[i].text = h
                            for nc in num_cols[:12]:
                                row_cells = table.add_row().cells
                                row_cells[0].text = nc[:30]
                                row_cells[1].text = f"{emp[nc].mean():,.2f}"
                                row_cells[2].text = f"{emp[nc].min():,.2f}"
                                row_cells[3].text = f"{emp[nc].max():,.2f}"
                                row_cells[4].text = f"{emp[nc].std():,.2f}"

                        # Footer
                        doc.add_paragraph("")
                        p = doc.add_paragraph()
                        run = p.add_run("This report is auto-generated by HR Analytics Platform - Risal Al-Wud IT")
                        run.font.size = Pt(8)
                        run.font.color.rgb = RGBColor(128, 128, 128)

                        doc_bytes = io.BytesIO()
                        doc.save(doc_bytes)

                        st.download_button("📥 تحميل Word", data=doc_bytes.getvalue(),
                            file_name=f"{word_title}_{datetime.now().strftime('%Y%m%d')}.docx",
                            mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                            type="primary", use_container_width=True)
                        st.success("✅ تم إنشاء التقرير بنجاح!")

                    except ImportError:
                        st.error("مكتبة python-docx غير مثبتة. أضف `python-docx` في requirements.txt")
                    except Exception as e:
                        st.error(f"خطأ: {e}")
            else:
                ibox("ارفع ملف بيانات أولاً من القائمة الجانبية.", "warning")

        elif page == "📊 تقرير شامل":
            hdr("📊 التقرير الشامل", "تصدير جميع التحليلات في ملف Excel واحد")

            if len(emp) > 0:
                st.markdown("### ⚙️ اختر محتوى التقرير")
                rpt_summary = st.checkbox("📊 ملخص تنفيذي", value=True, key="rpts")
                rpt_workforce = st.checkbox("👥 تحليل القوى العاملة", value=True, key="rptw")
                rpt_salary = st.checkbox("💰 تحليل الرواتب", value=True, key="rptsalx")
                rpt_recruit = st.checkbox("🎯 بيانات التوظيف", value=True, key="rptr2")
                rpt_training = st.checkbox("📚 بيانات التدريب", value=True, key="rptt")

                if st.button("📊 إنشاء التقرير الشامل", type="primary", key="rptbtn"):
                    ox = io.BytesIO()
                    with pd.ExcelWriter(ox, engine='xlsxwriter') as w:
                        num_cols = emp.select_dtypes('number').columns.tolist()
                        cat_cols = [c for c in emp.columns if emp[c].dtype=='object' and 1 < emp[c].nunique() < 30]

                        if rpt_summary:
                            summary_data = {"المؤشر": [], "القيمة": []}
                            summary_data["المؤشر"].append("إجمالي السجلات"); summary_data["القيمة"].append(len(emp))
                            summary_data["المؤشر"].append("الأعمدة"); summary_data["القيمة"].append(len(emp.columns))
                            for nc in num_cols[:5]:
                                summary_data["المؤشر"].append(f"متوسط {nc}"); summary_data["القيمة"].append(round(emp[nc].mean(), 2))
                                summary_data["المؤشر"].append(f"إجمالي {nc}"); summary_data["القيمة"].append(round(emp[nc].sum(), 2))
                            pd.DataFrame(summary_data).to_excel(w, sheet_name='ملخص تنفيذي', index=False)
                            w.sheets['ملخص تنفيذي'].right_to_left()

                        if rpt_workforce:
                            for cc in cat_cols[:6]:
                                safe_name = cc[:28]
                                vc = emp[cc].value_counts().reset_index()
                                vc.columns = [cc, "العدد"]
                                vc["النسبة %"] = (vc["العدد"] / len(emp) * 100).round(1)
                                vc.to_excel(w, sheet_name=safe_name, index=False)
                                try: w.sheets[safe_name].right_to_left()
                                except: pass

                        if rpt_salary and num_cols:
                            desc = emp[num_cols].describe().T.reset_index()
                            desc.columns = ["العمود","العدد","المتوسط","الانحراف","الأدنى","25%","الوسيط","75%","الأقصى"]
                            desc.to_excel(w, sheet_name='تحليل الرواتب', index=False)
                            w.sheets['تحليل الرواتب'].right_to_left()

                        if rpt_recruit:
                            if st.session_state.get('recruit_plans'):
                                pd.DataFrame(st.session_state.recruit_plans).to_excel(w, sheet_name='خطة التوظيف', index=False)
                                w.sheets['خطة التوظيف'].right_to_left()
                            if st.session_state.get('recruit_tracking'):
                                pd.DataFrame(st.session_state.recruit_tracking).to_excel(w, sheet_name='متابعة التوظيف', index=False)
                                w.sheets['متابعة التوظيف'].right_to_left()

                        if rpt_training and 'budget_data' in st.session_state:
                            pd.DataFrame(st.session_state.budget_data).to_excel(w, sheet_name='ميزانية التدريب', index=False)
                            w.sheets['ميزانية التدريب'].right_to_left()

                        # Raw data
                        emp.to_excel(w, sheet_name='البيانات الخام', index=False)

                    st.download_button("📥 تحميل التقرير الشامل", data=ox.getvalue(),
                        file_name=f"HR_Report_{datetime.now().strftime('%Y%m%d')}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        type="primary", use_container_width=True)
                    st.success("✅ تم إنشاء التقرير الشامل!")
            else:
                ibox("ارفع ملف بيانات أولاً من القائمة الجانبية.", "warning")


    # =========================================
    #         📝 SURVEYS MODULE
    # =========================================
    elif section == "📝 الاستبيانات":

        if 'surveys_data' not in st.session_state:
            st.session_state.surveys_data = {}
        if 'survey_responses' not in st.session_state:
            st.session_state.survey_responses = []
        if 'custom_surveys' not in st.session_state:
            st.session_state.custom_surveys = {}

        if page == "📋 قوالب جاهزة":
            hdr("📋 قوالب الاستبيانات الجاهزة", "اختر قالب جاهز واملأ الاستبيان")

            template = st.selectbox("📝 اختر القالب:", list(SURVEY_TEMPLATES.keys()), key="sv_tmpl")
            tmpl = SURVEY_TEMPLATES[template]
            ibox(tmpl["description"])

            st.markdown("### 👤 بيانات المشارك")
            sv1, sv2, sv3 = st.columns(3)
            with sv1: sv_name = st.text_input("الاسم:", key="sv_name")
            with sv2: sv_dept = st.text_input("القسم:", key="sv_dept")
            with sv3: sv_date = st.date_input("التاريخ:", value=date.today(), key="sv_date")

            st.markdown(f"### 📝 {template}")
            st.info("قيّم كل عبارة من 1 (غير موافق تماماً) إلى 5 (موافق تماماً)")

            answers = {}
            for i, q_item in enumerate(tmpl["questions"]):
                answers[i] = st.slider(f"{i+1}. {q_item['q']}", 1, 5, 3, key=f"sv_q{i}")

            if st.button("✅ إرسال الاستبيان", type="primary", key="sv_submit"):
                if sv_name:
                    response = {
                        "الاسم": sv_name, "القسم": sv_dept, "التاريخ": str(sv_date),
                        "القالب": template, "الإجابات": answers,
                        "المتوسط العام": round(sum(answers.values()) / len(answers), 2)
                    }
                    # Add category averages
                    cats = {}
                    for i, q_item in enumerate(tmpl["questions"]):
                        cat = q_item["cat"]
                        cats.setdefault(cat, []).append(answers[i])
                    response["التفاصيل"] = {c: round(sum(v)/len(v), 2) for c, v in cats.items()}
                    st.session_state.survey_responses.append(response)
                    st.success(f"✅ تم حفظ استبيان {sv_name} - المتوسط: {response['المتوسط العام']}/5")
                    st.rerun()
                else:
                    st.error("يرجى إدخال الاسم")

        elif page == "🔨 بناء استبيان":
            hdr("🔨 بناء استبيان مخصص", "أنشئ استبيانك الخاص")

            st.markdown("### ⚙️ إعدادات الاستبيان")
            cs_name = st.text_input("اسم الاستبيان:", key="cs_name")
            cs_desc = st.text_input("الوصف:", key="cs_desc")

            st.markdown("### ➕ إضافة أسئلة")
            if 'custom_q_list' not in st.session_state:
                st.session_state.custom_q_list = []

            cq1, cq2 = st.columns([3,1])
            with cq1: new_q = st.text_input("السؤال:", key="cs_newq")
            with cq2: new_cat = st.text_input("التصنيف:", key="cs_newcat")

            if st.button("➕ إضافة سؤال", key="cs_addq"):
                if new_q:
                    st.session_state.custom_q_list.append({"q": new_q, "cat": new_cat or "عام"})
                    st.rerun()

            if st.session_state.custom_q_list:
                st.markdown("### 📋 الأسئلة المضافة")
                for i, cq in enumerate(st.session_state.custom_q_list):
                    st.write(f"{i+1}. {cq['q']} [{cq['cat']}]")

                if st.button("💾 حفظ الاستبيان", type="primary", key="cs_save"):
                    if cs_name:
                        st.session_state.custom_surveys[cs_name] = {
                            "description": cs_desc, "questions": st.session_state.custom_q_list.copy()
                        }
                        st.session_state.custom_q_list = []
                        st.success(f"✅ تم حفظ الاستبيان: {cs_name}")
                        st.rerun()

                if st.button("🗑️ مسح الأسئلة", key="cs_clear"):
                    st.session_state.custom_q_list = []
                    st.rerun()

            # Show saved custom surveys
            if st.session_state.custom_surveys:
                st.markdown("---")
                st.markdown("### 📂 الاستبيانات المخصصة المحفوظة")
                for name, survey in st.session_state.custom_surveys.items():
                    with st.expander(f"📝 {name} ({len(survey['questions'])} سؤال)"):
                        st.write(survey['description'])
                        for i, q in enumerate(survey['questions']):
                            st.write(f"{i+1}. {q['q']} [{q['cat']}]")

        elif page == "📊 تحليل النتائج":
            hdr("📊 تحليل نتائج الاستبيانات")

            if st.session_state.survey_responses:
                responses = st.session_state.survey_responses
                st.success(f"📊 إجمالي الاستجابات: {len(responses)}")

                # Summary table
                summary_rows = []
                for r in responses:
                    row = {"الاسم": r["الاسم"], "القسم": r["القسم"], "القالب": r["القالب"], "المتوسط": r["المتوسط العام"]}
                    summary_rows.append(row)
                sdf = pd.DataFrame(summary_rows)
                st.dataframe(sdf, use_container_width=True, hide_index=True)

                # KPIs
                k1,k2,k3,k4 = st.columns(4)
                avg_all = sdf["المتوسط"].mean()
                with k1: kpi("📊 المتوسط العام", f"{avg_all:.2f}/5")
                with k2: kpi("✅ الاستجابات", f"{len(responses)}")
                with k3: kpi("📈 أعلى تقييم", f"{sdf['المتوسط'].max():.2f}")
                with k4: kpi("📉 أقل تقييم", f"{sdf['المتوسط'].min():.2f}")

                # Charts
                ch1, ch2 = st.columns(2)
                with ch1:
                    fig = px.bar(sdf, x="الاسم", y="المتوسط", color="القالب", title="التقييم حسب المشارك", text_auto=".2f")
                    fig.add_hline(y=avg_all, line_dash="dash", annotation_text=f"المتوسط: {avg_all:.2f}")
                    fig.update_layout(font=dict(family="Noto Sans Arabic"), height=350)
                    st.plotly_chart(fig, use_container_width=True)
                with ch2:
                    if sdf["القسم"].nunique() > 1:
                        dept_avg = sdf.groupby("القسم")["المتوسط"].mean().reset_index()
                        fig = px.bar(dept_avg, x="القسم", y="المتوسط", title="المتوسط حسب القسم", text_auto=".2f", color_discrete_sequence=[CL['s']])
                        fig.update_layout(font=dict(family="Noto Sans Arabic"), height=350)
                        st.plotly_chart(fig, use_container_width=True)

                # Category breakdown
                st.markdown("### 📊 التحليل حسب التصنيف")
                all_cats = {}
                for r in responses:
                    if "التفاصيل" in r:
                        for cat, val in r["التفاصيل"].items():
                            all_cats.setdefault(cat, []).append(val)
                if all_cats:
                    cat_avg = {c: sum(v)/len(v) for c, v in all_cats.items()}
                    cat_df = pd.DataFrame({"التصنيف": cat_avg.keys(), "المتوسط": cat_avg.values()}).sort_values("المتوسط")
                    fig = px.bar(cat_df, x="المتوسط", y="التصنيف", orientation='h', title="المتوسط حسب التصنيف", text_auto=".2f", color_discrete_sequence=[CL['a']])
                    fig.update_layout(font=dict(family="Noto Sans Arabic"), height=400)
                    st.plotly_chart(fig, use_container_width=True)

                if st.button("🗑️ مسح جميع الاستجابات", key="sv_clr"):
                    st.session_state.survey_responses = []
                    st.rerun()
            else:
                ibox("لا توجد استجابات بعد. اذهب لصفحة القوالب الجاهزة واملأ استبيان.", "warning")

        elif page == "📥 تصدير الاستبيانات":
            hdr("📥 تصدير بيانات الاستبيانات")
            if st.session_state.survey_responses:
                ox = io.BytesIO()
                with pd.ExcelWriter(ox, engine='xlsxwriter') as w:
                    rows = []
                    for r in st.session_state.survey_responses:
                        row = {"الاسم": r["الاسم"], "القسم": r["القسم"], "التاريخ": r["التاريخ"], "القالب": r["القالب"], "المتوسط": r["المتوسط العام"]}
                        if "التفاصيل" in r:
                            row.update(r["التفاصيل"])
                        rows.append(row)
                    pd.DataFrame(rows).to_excel(w, sheet_name='الاستجابات', index=False)
                    w.sheets['الاستجابات'].right_to_left()
                st.download_button("📥 تحميل", data=ox.getvalue(), file_name=f"Surveys_{datetime.now().strftime('%Y%m%d')}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", type="primary", use_container_width=True)
            else:
                ibox("لا توجد بيانات للتصدير.", "warning")


    # =========================================
    #       🧠 PERSONALITY TESTS MODULE
    # =========================================
    elif section == "🧠 اختبارات الشخصية":

        if 'personality_results' not in st.session_state:
            st.session_state.personality_results = []

        if page == "🧠 Big Five":
            hdr("🧠 اختبار العوامل الخمسة الكبرى", "Big Five Personality Test - OCEAN Model")

            st.markdown("### 👤 بيانات المشارك")
            b1, b2 = st.columns(2)
            with b1: bf_name = st.text_input("الاسم:", key="bf_name")
            with b2: bf_dept = st.text_input("القسم:", key="bf_dept")

            st.markdown("### 📝 قيّم كل عبارة (1 = غير موافق تماماً، 5 = موافق تماماً)")
            bf_answers = {}
            for i, q in enumerate(BIG5_QUESTIONS):
                trait = q["trait"]
                bf_answers[i] = st.slider(f"{i+1}. {q['q']} *({trait})*", 1, 5, 3, key=f"bf_{i}")

            if st.button("✅ حساب النتائج", type="primary", key="bf_calc"):
                if bf_name:
                    # Calculate scores per trait
                    trait_scores = {}
                    trait_counts = {}
                    for i, q in enumerate(BIG5_QUESTIONS):
                        t = q["trait"]
                        score = bf_answers[i] if q["d"] == 1 else (6 - bf_answers[i])
                        trait_scores[t] = trait_scores.get(t, 0) + score
                        trait_counts[t] = trait_counts.get(t, 0) + 1

                    percentages = {}
                    for t in trait_scores:
                        max_score = trait_counts[t] * 5
                        percentages[t] = round(trait_scores[t] / max_score * 100)

                    result = {
                        "type": "Big Five", "الاسم": bf_name, "القسم": bf_dept,
                        "التاريخ": str(date.today()), "scores": percentages
                    }
                    st.session_state.personality_results.append(result)

                    # Display results
                    st.markdown("---")
                    st.markdown(f"### 📊 نتائج {bf_name}")

                    cols = st.columns(5)
                    for i, (trait, pct) in enumerate(percentages.items()):
                        info = BIG5_TRAITS[trait]
                        with cols[i]:
                            kpi(f"{trait}", f"{pct}%")
                            st.caption(info["desc"])

                    # Radar chart
                    fig = go.Figure()
                    traits_list = list(percentages.keys())
                    values = list(percentages.values()) + [list(percentages.values())[0]]
                    fig.add_trace(go.Scatterpolar(r=values, theta=traits_list + [traits_list[0]], fill='toself', name=bf_name,
                        line_color=CL['p'], fillcolor='rgba(15,76,92,0.2)'))
                    fig.update_layout(polar=dict(radialaxis=dict(range=[0,100])), title=f"ملف الشخصية - {bf_name}",
                        font=dict(family="Noto Sans Arabic"), height=450, showlegend=False)
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.error("يرجى إدخال الاسم")

        elif page == "💎 DISC":
            hdr("💎 اختبار DISC", "تقييم أنماط السلوك المهني")

            st.markdown("### 👤 بيانات المشارك")
            d1, d2 = st.columns(2)
            with d1: disc_name = st.text_input("الاسم:", key="disc_name")
            with d2: disc_dept = st.text_input("القسم:", key="disc_dept")

            st.markdown("### 📝 قيّم كل عبارة (1 = لا تنطبق، 5 = تنطبق تماماً)")
            disc_answers = {}
            for i, q in enumerate(DISC_QUESTIONS):
                disc_answers[i] = st.slider(f"{i+1}. {q['q']}", 1, 5, 3, key=f"disc_{i}")

            if st.button("✅ حساب النتائج", type="primary", key="disc_calc"):
                if disc_name:
                    style_scores = {"D": 0, "I": 0, "S": 0, "C": 0}
                    style_counts = {"D": 0, "I": 0, "S": 0, "C": 0}
                    for i, q in enumerate(DISC_QUESTIONS):
                        s = q["style"]
                        style_scores[s] += disc_answers[i]
                        style_counts[s] += 1

                    percentages = {}
                    for s in style_scores:
                        max_s = style_counts[s] * 5
                        percentages[s] = round(style_scores[s] / max_s * 100)

                    dominant = max(percentages, key=percentages.get)
                    result = {
                        "type": "DISC", "الاسم": disc_name, "القسم": disc_dept,
                        "التاريخ": str(date.today()), "scores": percentages, "dominant": dominant
                    }
                    st.session_state.personality_results.append(result)

                    # Display results
                    st.markdown("---")
                    st.markdown(f"### 📊 نتائج {disc_name}")

                    cols = st.columns(4)
                    for i, (style, pct) in enumerate(percentages.items()):
                        info = DISC_STYLES[style]
                        with cols[i]:
                            kpi(f"{info['name']}", f"{pct}%")

                    # Dominant style details
                    dom_info = DISC_STYLES[dominant]
                    st.markdown(f"### 🏆 النمط السائد: {dom_info['name']}")
                    dc1, dc2, dc3 = st.columns(3)
                    with dc1: ibox(f"**الوصف:** {dom_info['desc']}")
                    with dc2: ibox(f"**نقاط القوة:** {dom_info['strengths']}", "success")
                    with dc3: ibox(f"**التحديات:** {dom_info['challenges']}", "warning")

                    # Bar chart
                    disc_df = pd.DataFrame({"النمط": [DISC_STYLES[s]["name"] for s in percentages],
                        "النسبة": list(percentages.values()),
                        "اللون": [DISC_STYLES[s]["color"] for s in percentages]})
                    fig = px.bar(disc_df, x="النمط", y="النسبة", title=f"ملف DISC - {disc_name}",
                        text_auto=True, color="النمط", color_discrete_map={DISC_STYLES[s]["name"]: DISC_STYLES[s]["color"] for s in DISC_STYLES})
                    fig.update_layout(font=dict(family="Noto Sans Arabic"), height=400, showlegend=False)
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.error("يرجى إدخال الاسم")

        elif page == "📊 تقارير الشخصية":
            hdr("📊 تقارير اختبارات الشخصية", "عرض ومقارنة نتائج جميع الاختبارات")

            results = st.session_state.personality_results
            if results:
                st.success(f"📊 إجمالي الاختبارات: {len(results)}")

                # Filter by type
                test_type = st.radio("نوع الاختبار:", ["الكل","Big Five","DISC"], horizontal=True, key="pt_filter")
                filtered = results if test_type == "الكل" else [r for r in results if r["type"] == test_type]

                if filtered:
                    # Summary table
                    rows = []
                    for r in filtered:
                        row = {"الاسم": r["الاسم"], "القسم": r["القسم"], "النوع": r["type"], "التاريخ": r["التاريخ"]}
                        for k, v in r["scores"].items():
                            row[k] = f"{v}%"
                        if "dominant" in r: row["النمط السائد"] = r["dominant"]
                        rows.append(row)
                    st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

                    # Compare Big Five results
                    bf_results = [r for r in filtered if r["type"] == "Big Five"]
                    if len(bf_results) >= 2:
                        st.markdown("### 📊 مقارنة نتائج Big Five")
                        fig = go.Figure()
                        for r in bf_results:
                            traits = list(r["scores"].keys())
                            vals = list(r["scores"].values()) + [list(r["scores"].values())[0]]
                            fig.add_trace(go.Scatterpolar(r=vals, theta=traits + [traits[0]], fill='toself', name=r["الاسم"]))
                        fig.update_layout(polar=dict(radialaxis=dict(range=[0,100])), title="مقارنة ملفات الشخصية",
                            font=dict(family="Noto Sans Arabic"), height=500)
                        st.plotly_chart(fig, use_container_width=True)

                    # Compare DISC results
                    disc_results = [r for r in filtered if r["type"] == "DISC"]
                    if len(disc_results) >= 2:
                        st.markdown("### 📊 مقارنة نتائج DISC")
                        comp_rows = []
                        for r in disc_results:
                            for style, pct in r["scores"].items():
                                comp_rows.append({"الاسم": r["الاسم"], "النمط": DISC_STYLES[style]["name"], "النسبة": pct})
                        comp_df = pd.DataFrame(comp_rows)
                        fig = px.bar(comp_df, x="الاسم", y="النسبة", color="النمط", barmode="group",
                            title="مقارنة DISC", color_discrete_map={DISC_STYLES[s]["name"]: DISC_STYLES[s]["color"] for s in DISC_STYLES})
                        fig.update_layout(font=dict(family="Noto Sans Arabic"), height=400)
                        st.plotly_chart(fig, use_container_width=True)

                    # Individual report
                    st.markdown("### 📄 تقرير فردي")
                    selected = st.selectbox("اختر الموظف:", [r["الاسم"] for r in filtered], key="pt_sel")
                    sel_result = next((r for r in filtered if r["الاسم"] == selected), None)
                    if sel_result:
                        st.markdown(f"**{sel_result['الاسم']}** | {sel_result['القسم']} | {sel_result['type']} | {sel_result['التاريخ']}")
                        if sel_result["type"] == "Big Five":
                            for trait, pct in sel_result["scores"].items():
                                info = BIG5_TRAITS[trait]
                                level = "مرتفع" if pct >= 70 else ("متوسط" if pct >= 40 else "منخفض")
                                st.progress(pct/100, text=f"{trait} ({info['en']}): {pct}% - {level}")
                                st.caption(f"  {info['desc']}")
                        elif sel_result["type"] == "DISC":
                            for style, pct in sel_result["scores"].items():
                                info = DISC_STYLES[style]
                                st.progress(pct/100, text=f"{info['name']}: {pct}%")

                if st.button("🗑️ مسح جميع النتائج", key="pt_clr"):
                    st.session_state.personality_results = []
                    st.rerun()
            else:
                ibox("لا توجد نتائج بعد. اذهب لاختبار Big Five أو DISC.", "warning")

        elif page == "📥 تصدير الاختبارات":
            hdr("📥 تصدير نتائج اختبارات الشخصية")
            if st.session_state.personality_results:
                ox = io.BytesIO()
                with pd.ExcelWriter(ox, engine='xlsxwriter') as w:
                    rows = []
                    for r in st.session_state.personality_results:
                        row = {"الاسم": r["الاسم"], "القسم": r["القسم"], "النوع": r["type"], "التاريخ": r["التاريخ"]}
                        for k, v in r["scores"].items(): row[k] = v
                        if "dominant" in r: row["النمط السائد"] = r["dominant"]
                        rows.append(row)
                    pd.DataFrame(rows).to_excel(w, sheet_name='نتائج الاختبارات', index=False)
                    w.sheets['نتائج الاختبارات'].right_to_left()
                st.download_button("📥 تحميل", data=ox.getvalue(),
                    file_name=f"Personality_{datetime.now().strftime('%Y%m%d')}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", type="primary", use_container_width=True)
            else:
                ibox("لا توجد بيانات للتصدير.", "warning")


    # =========================================
    #       👥 USER MANAGEMENT
    # =========================================
    elif section == "👥 إدارة المستخدمين":
        user_management_page()


if __name__ == "__main__":
    main()

