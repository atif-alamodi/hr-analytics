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


# ===== MAIN APP =====
def main():
    # Sidebar
    with st.sidebar:
        st.markdown("<div style='text-align:center;padding:16px 0;'><div style='background:linear-gradient(135deg,#E36414,#E9C46A);width:56px;height:56px;border-radius:12px;display:flex;align-items:center;justify-content:center;margin:0 auto 10px;font-size:22px;font-weight:800;color:white;'>HR</div><h2 style='margin:0;font-size:16px;'>تحليلات الموارد البشرية</h2><p style='opacity:.6;font-size:11px;'>رسال الود لتقنية المعلومات v5</p></div>", unsafe_allow_html=True)
        st.markdown("---")

        section = st.radio("📂", ["📊 التحليلات العامة","💰 تحليل الرواتب","👥 Headcount","⚖️ حاسبة المستحقات","📚 التدريب والتطوير"], label_visibility="collapsed")
        st.markdown("---")

        if section == "📊 التحليلات العامة":
            page = st.radio("📌", ["🏠 نظرة عامة","📊 الأقسام","🤖 المحلل الذكي","📋 البيانات"], label_visibility="collapsed")
        elif section == "💰 تحليل الرواتب":
            page = st.radio("📌", ["💰 لوحة الرواتب","📈 تحليل شهري/ربعي","🏷️ تحليل حسب الفئات","📊 سلم الرواتب","📥 تصدير الرواتب"], label_visibility="collapsed")
        elif section == "👥 Headcount":
            page = st.radio("📌", ["👥 Headcount Planning","📊 تحليل الأداء"], label_visibility="collapsed")
        elif section == "⚖️ حاسبة المستحقات":
            page = "⚖️ حاسبة المستحقات"
        else:
            page = st.radio("📌", ["📚 ميزانية التدريب","💹 ROI التدريب","📋 الاحتياجات التدريبية","🏫 جهات التدريب","📥 تصدير التدريب"], label_visibility="collapsed")

        st.markdown("---")
        st.markdown("##### 📁 ملف البيانات")
        file = st.file_uploader("ارفع Excel", type=["xlsx","xls","csv"], label_visibility="collapsed")
        if file: st.success("✅ تم التحميل")


    # ===== LOAD DATA =====
    emp = pd.DataFrame()
    sal_df = pd.DataFrame()
    all_sheets = {}

    if file:
        try:
            if file.name.endswith('.csv'):
                emp = norm_cols(pd.read_csv(file))
            else:
                xl = pd.ExcelFile(file)
                for s in xl.sheet_names:
                    try:
                        df_s = smart_read(xl, s)
                        # Check if it's a large salary dataset
                        if len(df_s) > 500 and any(c.lower() in ['salary month','gross salary','شهر الراتب'] for c in df_s.columns):
                            sal_df = norm_cols(df_s)
                        df_s = norm_cols(df_s)
                        all_sheets[s] = df_s
                        if len(emp)==0 and len(df_s)>5:
                            name_cols = [c for c in df_s.columns if any(x in str(c).lower() for x in ['name','اسم','emp','موظف'])]
                            if name_cols: emp = df_s
                    except: pass
                if len(emp)==0 and all_sheets: emp = list(all_sheets.values())[0]

                # Try loading specific sheets
                if 'Salary Scale' in xl.sheet_names:
                    try: all_sheets['Salary Scale'] = pd.read_excel(xl, 'Salary Scale', header=0)
                    except: pass
                if 'Positions' in xl.sheet_names:
                    try: all_sheets['Positions'] = pd.read_excel(xl, 'Positions', header=0)
                    except: pass
        except: pass

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

        ibox("ملاحظة: التاريخ الميلادي يستخدم للعقود الميلادية. إصدار استرشادي تقريبي.")

        # ===== SHARED EMPLOYEE DATA (always visible) =====
        st.markdown("---")
        st.markdown("### 👤 بيانات العامل الأساسية")

        w1, w2 = st.columns(2)
        with w1:
            worker_type = st.radio("العامل:", ["سعودي","غير سعودي"], horizontal=True, key="wt")
        with w2:
            cal_type = st.radio("نوع التاريخ:", ["ميلادي","هجري (تقريبي)"], horizontal=True, key="caltype")

        s1, s2, s3, s4 = st.columns(4)
        with s1:
            basic_sal = st.number_input("💵 الأجر الأساسي:", min_value=0, max_value=500000, value=5000, step=100, key="bsal")
        with s2:
            allowances = st.number_input("💰 البدلات:", min_value=0, max_value=500000, value=3000, step=100, key="alw")
        with s3:
            total_sal = basic_sal + allowances
            st.metric("📊 الإجمالي", f"{total_sal:,}")
        with s4:
            sal_after_gosi = st.number_input("💳 بعد حسم التأمينات:", min_value=0, max_value=500000, value=int(total_sal * 0.9025), step=100, key="sagosi")

        n1, n2 = st.columns(2)
        with n1:
            plaintiff = st.text_input("اسم المدعي:", key="plnt")
        with n2:
            defendant = st.text_input("اسم المدعى عليه:", key="dfnd")

        daily_sal = total_sal / 30
        hourly_basic = basic_sal / 30 / 8

        # ===== SECTION CHECKBOXES =====
        st.markdown("---")
        st.markdown("### 📋 اختر البنود المطلوب حسابها")

        chk_delayed = st.checkbox("💰 الأجور المتأخرة", key="chk1")
        chk_eos = st.checkbox("📊 مكافأة نهاية الخدمة", value=True, key="chk2")
        chk_vac = st.checkbox("🏖️ أجر الإجازة", key="chk3")
        chk_ot = st.checkbox("⏰ أجر العمل الإضافي", key="chk4")
        chk_unfair = st.checkbox("🚫 التعويض عن الإنهاء لغير سبب مشروع", key="chk5")
        chk_vac_days = st.checkbox("📅 معرفة عدد أيام الإجازة في فترة الخدمة", key="chk6")
        chk_absence = st.checkbox("📉 معرفة مبلغ الحسم بسبب الغياب والتأخر", key="chk7")
        chk_avg = st.checkbox("📊 معرفة متوسط الأجر لآخر سنة", key="chk8")

        # Collect all results
        results_summary = []

        # ======= 💰 الأجور المتأخرة =======
        if chk_delayed:
            st.markdown("---")
            st.markdown("### 💰 الأجور المتأخرة")

            dw_method = st.radio("طريقة الإدخال:", ["بإدخال التاريخ من إلى","بإدخال عدد الأشهر والأيام"], horizontal=True, key="dw_m")
            if dw_method == "بإدخال التاريخ من إلى":
                dc1, dc2 = st.columns(2)
                with dc1: dw_from = st.date_input("من التاريخ:", value=date(2024,1,1), key="dwf")
                with dc2: dw_to = st.date_input("إلى التاريخ:", value=date.today(), key="dwt")
                dw_total_days = (dw_to - dw_from).days
            else:
                dc1, dc2 = st.columns(2)
                with dc1: dw_months = st.number_input("عدد الأشهر:", 0, 120, 3, key="dwmo")
                with dc2: dw_extra_days = st.number_input("عدد الأيام:", 0, 30, 0, key="dwdy")
                dw_total_days = dw_months * 30 + dw_extra_days

            delayed_amount = daily_sal * dw_total_days
            st.info(f"📊 إجمالي الأجور المتأخرة: **{delayed_amount:,.2f} ريال** ({dw_total_days} يوم × {daily_sal:,.2f} ريال/يوم)")
            results_summary.append(("الأجور المتأخرة", delayed_amount))


        # ======= 📊 مكافأة نهاية الخدمة =======
        if chk_eos:
            st.markdown("---")
            st.markdown("### 📊 مكافأة نهاية الخدمة")

            st.markdown("""
            <div class="ibox ok">✅ <b>المادة 84:</b> نصف شهر عن كل سنة من الخمس الأولى + شهر كامل عن كل سنة بعدها.</div>
            <div class="ibox warn">⚠️ <b>المادة 85 (استقالة):</b> أقل من سنتين = 0 | 2-5 سنوات = ثلث | 5-10 = ثلثان | 10+ = كاملة.</div>
            """, unsafe_allow_html=True)

            ec1, ec2 = st.columns(2)
            with ec1:
                eos_method = st.radio("طريقة احتساب المكافأة:", ["حسب المادة (84)","حسب المادة (85)"], key="eosm")
            with ec2:
                unpaid_leave = st.number_input("إجمالي أيام الإجازات بدون أجر:", 0, 9999, 0, key="unp")

            ec3, ec4 = st.columns(2)
            with ec3: eos_start = st.date_input("بداية العمل:", value=date(2018,1,1), key="eoss")
            with ec4: eos_end = st.date_input("نهاية العمل:", value=date.today(), key="eose")

            eos_service_days = (eos_end - eos_start).days - unpaid_leave
            if eos_service_days < 0: eos_service_days = 0
            eos_years = eos_service_days / 365.25
            eos_delta = relativedelta(eos_end, eos_start)

            # Article 84 base
            if eos_years <= 5:
                eos_84 = (total_sal / 2) * eos_years
            else:
                eos_84 = (total_sal / 2) * 5 + total_sal * (eos_years - 5)

            # Article 85 adjustment
            is_85 = "85" in eos_method
            if is_85:
                if eos_years < 2:
                    eos_final = 0; eos_pct = 0; eos_note = "لا يستحق مكافأة (أقل من سنتين)"
                elif eos_years < 5:
                    eos_final = eos_84 / 3; eos_pct = 33.3; eos_note = "ثلث المكافأة (2 إلى 5 سنوات)"
                elif eos_years < 10:
                    eos_final = eos_84 * 2 / 3; eos_pct = 66.7; eos_note = "ثلثا المكافأة (5 إلى 10 سنوات)"
                else:
                    eos_final = eos_84; eos_pct = 100; eos_note = "المكافأة كاملة (10+ سنوات)"
            else:
                eos_final = eos_84; eos_pct = 100; eos_note = "المكافأة كاملة (المادة 84)"

            st.info(f"📊 مدة الخدمة: **{eos_delta.years} سنة {eos_delta.months} شهر {eos_delta.days} يوم** | الأيام الفعلية: {eos_service_days:,} يوم | السنوات: {eos_years:.2f}")

            # Detail breakdown
            calc_rows = []
            if eos_years <= 5:
                calc_rows.append({"البند": f"{eos_years:.2f} سنة × نصف شهر ({total_sal:,}/2)", "المبلغ": f"{eos_84:,.2f}"})
            else:
                first5 = (total_sal / 2) * 5
                rest = total_sal * (eos_years - 5)
                calc_rows.append({"البند": f"أول 5 سنوات × نصف شهر ({total_sal:,}/2 × 5)", "المبلغ": f"{first5:,.2f}"})
                calc_rows.append({"البند": f"ما بعد 5 سنوات × شهر كامل ({total_sal:,} × {eos_years-5:.2f})", "المبلغ": f"{rest:,.2f}"})
            calc_rows.append({"البند": "إجمالي المكافأة (مادة 84)", "المبلغ": f"{eos_84:,.2f}"})
            if is_85:
                calc_rows.append({"البند": f"المستحق (مادة 85): {eos_pct}%", "المبلغ": f"{eos_final:,.2f}"})
            st.dataframe(pd.DataFrame(calc_rows), use_container_width=True, hide_index=True)

            ibox(eos_note, "success" if eos_pct==100 else ("danger" if eos_pct==0 else "warning"))
            if unpaid_leave > 0:
                ibox(f"تم خصم {unpaid_leave} يوم إجازة بدون أجر من مدة الخدمة.")

            results_summary.append(("مكافأة نهاية الخدمة", eos_final))


        # ======= 🏖️ أجر الإجازة =======
        if chk_vac:
            st.markdown("---")
            st.markdown("### 🏖️ أجر الإجازة")

            vac_days_input = st.number_input("عدد أيام الإجازة المستحقة:", 0, 365, 21, key="vacd")
            vac_amount = daily_sal * vac_days_input

            st.info(f"📊 أجر الإجازة: **{vac_amount:,.2f} ريال** ({vac_days_input} يوم × {daily_sal:,.2f} ريال/يوم)")
            results_summary.append(("أجر الإجازة", vac_amount))


        # ======= ⏰ أجر العمل الإضافي =======
        if chk_ot:
            st.markdown("---")
            st.markdown("### ⏰ أجر العمل الإضافي")

            ibox("المادة 107: أجر ساعة الإضافي = أجر الساعة + 50% من الأجر الأساسي للساعة.")

            oc1, oc2, oc3 = st.columns(3)
            with oc1:
                ot_work_hours = st.selectbox("ساعات اليوم الفعلية:", list(range(2,13)), index=6, key="oth")
            with oc2:
                ot_days = st.number_input("عدد الأيام الإضافية:", 0, 365, 0, key="otd")
            with oc3:
                ot_hours = st.number_input("عدد الساعات الإضافية:", 0, 9999, 0, key="othr")

            ot_hourly = basic_sal / 30 / ot_work_hours
            ot_rate = ot_hourly * 1.5
            ot_total_hours = (ot_days * ot_work_hours) + ot_hours
            ot_amount = ot_total_hours * ot_rate

            st.info(f"📊 سعر الساعة: {ot_hourly:,.2f} | سعر ساعة الإضافي (150%): {ot_rate:,.2f} | إجمالي الساعات: {ot_total_hours} | **الإجمالي: {ot_amount:,.2f} ريال**")
            results_summary.append(("أجر العمل الإضافي", ot_amount))


        # ======= 🚫 التعويض عن الإنهاء غير المشروع =======
        if chk_unfair:
            st.markdown("---")
            st.markdown("### 🚫 التعويض عن الإنهاء لغير سبب مشروع")

            ibox("المادة 77: عقد محدد = أجر المدة المتبقية (حد أدنى شهرين). عقد غير محدد = 15 يوم عن كل سنة (حد أدنى شهرين).")

            contract_type = st.radio("نوع العقد:", ["عقد محدد المدة","عقد غير محدد المدة"], key="ctype")

            if contract_type == "عقد محدد المدة":
                st.markdown("**المدة المتبقية من العقد:**")
                uc1, uc2 = st.columns(2)
                with uc1: ct_from = st.date_input("من تاريخ:", value=date.today(), key="ctf")
                with uc2: ct_to = st.date_input("إلى تاريخ:", value=date(2026,12,31), key="ctt")

                remaining_days = (ct_to - ct_from).days
                comp = daily_sal * remaining_days
                min_comp = total_sal * 2
                unfair_amount = max(comp, min_comp)

                if comp < min_comp:
                    st.info(f"📊 المدة المتبقية: {remaining_days} يوم | أجر المدة: {comp:,.2f} | الحد الأدنى (شهرين): {min_comp:,.2f} | **التعويض: {unfair_amount:,.2f} ريال** (تم تطبيق الحد الأدنى)")
                else:
                    st.info(f"📊 المدة المتبقية: {remaining_days} يوم | **التعويض: {unfair_amount:,.2f} ريال** (أجر المدة المتبقية)")
            else:
                uc1, uc2 = st.columns(2)
                with uc1: uct_start = st.date_input("بداية العمل:", value=date(2018,1,1), key="ucts")
                with uc2: uct_end = st.date_input("تاريخ الإنهاء:", value=date.today(), key="ucte")

                service_yrs = (uct_end - uct_start).days / 365.25
                comp = (daily_sal * 15) * service_yrs
                min_comp = total_sal * 2
                unfair_amount = max(comp, min_comp)

                if comp < min_comp:
                    st.info(f"📊 مدة الخدمة: {service_yrs:.2f} سنة | 15 يوم/سنة: {comp:,.2f} | الحد الأدنى (شهرين): {min_comp:,.2f} | **التعويض: {unfair_amount:,.2f} ريال** (تم تطبيق الحد الأدنى)")
                else:
                    st.info(f"📊 مدة الخدمة: {service_yrs:.2f} سنة | **التعويض: {unfair_amount:,.2f} ريال** (15 يوم × {service_yrs:.2f} سنة)")

            results_summary.append(("تعويض الإنهاء غير المشروع", unfair_amount))


        # ======= 📅 أيام الإجازة المستحقة =======
        if chk_vac_days:
            st.markdown("---")
            st.markdown("### 📅 معرفة عدد أيام الإجازة في فترة الخدمة")

            ibox("المادة 109: الحد الأدنى 21 يوم في أول 5 سنوات، و 30 يوم بعد ذلك.")

            vc1, vc2 = st.columns(2)
            with vc1:
                vd_first5 = st.number_input("عدد أيام الإجازة في أول 5 سنوات:", min_value=21, max_value=60, value=21, key="vd5")
            with vc2:
                vd_after5 = st.number_input("عدد أيام الإجازة بعد 5 سنوات:", min_value=30, max_value=60, value=30, key="vda5")

            vc3, vc4 = st.columns(2)
            with vc3: vd_from = st.date_input("من تاريخ:", value=date(2018,1,1), key="vdf")
            with vc4: vd_to = st.date_input("إلى تاريخ:", value=date.today(), key="vdt")

            vd_total_yrs = (vd_to - vd_from).days / 365.25
            vd_delta = relativedelta(vd_to, vd_from)
            if vd_total_yrs <= 5:
                vd_total_days = vd_total_yrs * vd_first5
            else:
                vd_total_days = (5 * vd_first5) + ((vd_total_yrs - 5) * vd_after5)

            st.info(f"📊 مدة الخدمة: {vd_delta.years} سنة {vd_delta.months} شهر {vd_delta.days} يوم ({vd_total_yrs:.2f} سنة) | **إجمالي أيام الإجازة المستحقة: {vd_total_days:.1f} يوم**")

            if vd_total_yrs > 5:
                f5 = 5 * vd_first5
                af5 = (vd_total_yrs - 5) * vd_after5
                st.caption(f"أول 5 سنوات: {f5:.0f} يوم + ما بعدها: {af5:.1f} يوم = {vd_total_days:.1f} يوم")


        # ======= 📉 حسم الغياب والتأخر =======
        if chk_absence:
            st.markdown("---")
            st.markdown("### 📉 معرفة مبلغ الحسم بسبب الغياب والتأخر")

            ac1, ac2 = st.columns(2)
            with ac1:
                abs_hours_day = st.selectbox("ساعات العمل اليومية:", list(range(2,13)), index=6, key="absh")
                abs_days = st.number_input("عدد أيام الغياب:", 0, 365, 0, key="absd")
            with ac2:
                abs_hours = st.number_input("عدد ساعات التأخر:", 0, 999, 0, key="abshr")
                abs_minutes = st.number_input("عدد دقائق التأخر:", 0, 59, 0, key="absmin")

            abs_hourly = daily_sal / abs_hours_day
            abs_minute_rate = abs_hourly / 60

            abs_day_deduct = abs_days * daily_sal
            abs_hr_deduct = abs_hours * abs_hourly
            abs_min_deduct = abs_minutes * abs_minute_rate
            abs_total = abs_day_deduct + abs_hr_deduct + abs_min_deduct

            details = []
            if abs_days > 0: details.append(f"غياب {abs_days} يوم = {abs_day_deduct:,.2f}")
            if abs_hours > 0: details.append(f"تأخر {abs_hours} ساعة = {abs_hr_deduct:,.2f}")
            if abs_minutes > 0: details.append(f"تأخر {abs_minutes} دقيقة = {abs_min_deduct:,.2f}")

            st.info(f"📊 أجر الساعة: {abs_hourly:,.2f} | أجر الدقيقة: {abs_minute_rate:,.4f} | " + " | ".join(details) + f" | **إجمالي الحسم: {abs_total:,.2f} ريال**")
            results_summary.append(("حسم الغياب والتأخر (يُخصم)", abs_total))


        # ======= 📊 متوسط أجر آخر سنة =======
        if chk_avg:
            st.markdown("---")
            st.markdown("### 📊 معرفة متوسط الأجر لآخر سنة")

            ibox("يُستخدم لحساب المكافأة عندما يكون الأجر متغيراً (عمولات، مكافآت).")

            months_ar = ["يناير","فبراير","مارس","أبريل","مايو","يونيو","يوليو","أغسطس","سبتمبر","أكتوبر","نوفمبر","ديسمبر"]
            month_sals = []

            for i in range(0, 12, 4):
                cols = st.columns(4)
                for j in range(4):
                    if i+j < 12:
                        with cols[j]:
                            val = st.number_input(f"{months_ar[i+j]}:", 0, 500000, 0, step=100, key=f"ms{i+j}")
                            month_sals.append(val)

            non_zero = [s for s in month_sals if s > 0]
            if non_zero:
                avg_total = sum(month_sals)
                avg_12 = avg_total / 12
                avg_actual = sum(non_zero) / len(non_zero)
                st.info(f"📊 الإجمالي: {avg_total:,.0f} ريال | المتوسط (12 شهر): **{avg_12:,.2f} ريال** | المتوسط ({len(non_zero)} أشهر فعلية): {avg_actual:,.2f} ريال")


        # ======= 📊 ملخص إجمالي المستحقات =======
        if results_summary:
            st.markdown("---")
            st.markdown("### 📊 ملخص إجمالي المستحقات النهائية")

            grand_total = 0
            summary_rows = []
            for label, amount in results_summary:
                is_deduction = "خصم" in label or "حسم" in label
                summary_rows.append({"البند": label, "المبلغ (ريال)": f"{amount:,.2f}", "النوع": "خصم" if is_deduction else "استحقاق"})
                if is_deduction:
                    grand_total -= amount
                else:
                    grand_total += amount

            summary_rows.append({"البند": "🟰 صافي المستحقات النهائية", "المبلغ (ريال)": f"{grand_total:,.2f}", "النوع": "الإجمالي"})
            st.dataframe(pd.DataFrame(summary_rows), use_container_width=True, hide_index=True)

            k1, k2 = st.columns(2)
            with k1: kpi("💰 صافي المستحقات النهائية", f"{grand_total:,.2f} ريال")
            with k2:
                if plaintiff or defendant:
                    kpi("📋 الأطراف", f"{plaintiff or '-'} ضد {defendant or '-'}")

            ibox("هذه الحاسبة استرشادية تقريبية ولا تغني عن الاستشارة القانونية المتخصصة. للحالات المعقدة راجع المحكمة العمالية أو محامي مختص.", "warning")


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


if __name__ == "__main__":
    main()
