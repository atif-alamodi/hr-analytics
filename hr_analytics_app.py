# ===================================================
# منصة تحليلات الموارد البشرية الذكية v3.0
# Smart HR Analytics - يقرأ أي ملف Excel تلقائياً
# رسال الود لتقنية المعلومات
# ===================================================

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import io, re
from datetime import datetime

st.set_page_config(page_title="تحليلات HR | رسال الود", page_icon="📊", layout="wide", initial_sidebar_state="expanded")

st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Noto+Sans+Arabic:wght@300;400;500;600;700;800&display=swap');
    * { font-family: 'Noto Sans Arabic', sans-serif; }
    .main .block-container { padding-top: 1rem; max-width: 1400px; }
    [data-testid="stSidebar"] { background: linear-gradient(180deg, #0F4C5C 0%, #1A1A2E 100%); }
    [data-testid="stSidebar"] * { color: white !important; }
    [data-testid="stMetric"] { background: white; border-radius: 12px; padding: 16px 20px; box-shadow: 0 1px 3px rgba(0,0,0,0.06); border: 1px solid #E2E8F0; }
    [data-testid="stMetric"] label { font-size: 13px !important; color: #64748B !important; }
    [data-testid="stMetric"] [data-testid="stMetricValue"] { font-size: 24px !important; font-weight: 700 !important; }
    h1 { color: #0F4C5C !important; font-weight: 800 !important; }
    .insight-box { background: #EFF6FF; border-radius: 12px; padding: 16px 20px; border-right: 4px solid #3B82F6; margin-bottom: 12px; font-size: 14px; line-height: 1.8; }
    .insight-warning { background: #FFF7ED; border-right-color: #F97316; }
    .insight-success { background: #F0FDF4; border-right-color: #22C55E; }
    .insight-danger { background: #FEF2F2; border-right-color: #EF4444; }
    .app-header { background: linear-gradient(135deg, #0F4C5C, #1A1A2E); padding: 24px 32px; border-radius: 16px; margin-bottom: 24px; color: white; }
    .app-header h1 { color: white !important; margin: 0; font-size: 28px; }
    .app-header p { color: rgba(255,255,255,0.7); margin: 4px 0 0; font-size: 14px; }
    #MainMenu { visibility: hidden; }
    footer { visibility: hidden; }
</style>
""", unsafe_allow_html=True)

CL = {'primary':'#0F4C5C','accent':'#9A031E','success':'#2D6A4F','dept':px.colors.qualitative.Set2}

def insight_box(text, t="info"):
    cls = f"insight-box insight-{t}" if t != "info" else "insight-box"
    icons = {"info":"💡","warning":"⚠️","success":"✅","danger":"🚨"}
    st.markdown(f'<div class="{cls}">{icons.get(t,"💡")} {text}</div>', unsafe_allow_html=True)

# ===== SMART DATA LOADER =====
# Column name mapping: English -> Arabic standard names
COL_MAP = {
    'emp id': 'رقم الموظف', 'employee id': 'رقم الموظف', 'id': 'رقم الموظف',
    'name (english)': 'الاسم الإنجليزي', 'name (arabic)': 'الاسم', 'الاسم': 'الاسم',
    'name': 'الاسم', 'employee name': 'الاسم',
    'department': 'القسم', 'القسم': 'القسم', 'dept': 'القسم',
    'job title': 'المسمى الوظيفي', 'المسمى الوظيفي': 'المسمى الوظيفي', 'title': 'المسمى الوظيفي', 'position': 'المسمى الوظيفي',
    'join date': 'تاريخ التعيين', 'تاريخ التعيين': 'تاريخ التعيين', 'hire date': 'تاريخ التعيين', 'start date': 'تاريخ التعيين',
    'location': 'الموقع', 'الموقع': 'الموقع', 'city': 'الموقع', 'المدينة': 'الموقع',
    'tenure (yrs)': 'سنوات الخدمة', 'tenure': 'سنوات الخدمة', 'سنوات الخدمة': 'سنوات الخدمة',
    'salary': 'الراتب الأساسي', 'الراتب الأساسي': 'الراتب الأساسي', 'basic salary': 'الراتب الأساسي',
    'الجنسية': 'الجنسية', 'nationality': 'الجنسية',
    'الحالة': 'الحالة', 'status': 'الحالة',
    'تقييم الأداء %': 'تقييم الأداء %', 'performance': 'تقييم الأداء %', 'performance %': 'تقييم الأداء %',
    'الرضا الوظيفي %': 'الرضا الوظيفي %', 'satisfaction': 'الرضا الوظيفي %',
    'أيام الغياب': 'أيام الغياب', 'absence': 'أيام الغياب', 'absence days': 'أيام الغياب',
    'ساعات التدريب': 'ساعات التدريب', 'training hours': 'ساعات التدريب',
    'الدرجة': 'الدرجة', 'grade': 'الدرجة', 'level': 'الدرجة',
    'الجنس': 'الجنس', 'gender': 'الجنس',
    'العمر': 'العمر', 'age': 'العمر',
}

def smart_read_sheet(xl, sheet_name):
    """Read a sheet with auto-detection of header row"""
    df_raw = pd.read_excel(xl, sheet_name=sheet_name, header=None)
    # Try to find the header row (row with most non-null string values)
    best_row = 0
    best_score = 0
    for i in range(min(5, len(df_raw))):
        row = df_raw.iloc[i]
        score = sum(1 for v in row if isinstance(v, str) and len(str(v).strip()) > 1 and not str(v).startswith('Total') and not str(v).startswith('Unnamed'))
        if score > best_score:
            best_score = score
            best_row = i

    df = pd.read_excel(xl, sheet_name=sheet_name, header=best_row)
    # Drop unnamed and empty columns
    df = df[[c for c in df.columns if not str(c).startswith('Unnamed')]]
    # Drop rows that are all NaN
    df = df.dropna(how='all').reset_index(drop=True)
    # Remove header-like rows (e.g. row containing '#' as value)
    if len(df) > 0 and '#' in df.columns:
        df = df[df['#'].apply(lambda x: not isinstance(x, str) or x != '#')]
    return df

def normalize_columns(df):
    """Map column names to standard Arabic names"""
    new_cols = {}
    for c in df.columns:
        key = str(c).strip().lower()
        if key in COL_MAP:
            new_cols[c] = COL_MAP[key]
        else:
            new_cols[c] = c
    df = df.rename(columns=new_cols)
    # If we have Arabic name, use it as primary name column
    if 'الاسم' not in df.columns and 'الاسم الإنجليزي' in df.columns:
        df['الاسم'] = df['الاسم الإنجليزي']
    return df

def parse_dashboard(xl):
    """Extract structured data from Dashboard sheet"""
    sections = {}
    try:
        df_raw = pd.read_excel(xl, sheet_name='Dashboard', header=None)
        current_section = None
        section_data = []
        headers = []

        for _, row in df_raw.iterrows():
            vals = [v for v in row if pd.notna(v)]
            if len(vals) == 0:
                if current_section and section_data:
                    sections[current_section] = pd.DataFrame(section_data, columns=headers if headers else None)
                    section_data = []
                    headers = []
                    current_section = None
                continue

            text = str(vals[0]).strip() if vals else ""

            if len(vals) == 1 and len(text) > 3 and text.isupper() and text not in ['TOTAL']:
                if current_section and section_data:
                    sections[current_section] = pd.DataFrame(section_data, columns=headers if headers else None)
                    section_data = []
                current_section = text
                headers = []
            elif current_section and not headers and any(isinstance(v, str) for v in vals):
                headers = [str(v).strip() for v in vals if pd.notna(v)]
            elif current_section and headers:
                clean = [v for v in vals if pd.notna(v)]
                if clean and str(clean[0]).strip() != 'TOTAL':
                    if len(clean) == len(headers):
                        section_data.append(clean)
                    elif len(clean) > 0:
                        padded = clean + [None] * (len(headers) - len(clean))
                        section_data.append(padded[:len(headers)])

        if current_section and section_data:
            sections[current_section] = pd.DataFrame(section_data, columns=headers if headers else None)
    except:
        pass
    return sections

def has(df, name):
    return df is not None and name in df.columns

def safe_mean(df, name):
    return df[name].mean() if has(df, name) and len(df) > 0 else 0

def safe_sum(df, name):
    return df[name].sum() if has(df, name) else 0

def fmt(v): return f"{v:,.0f} ريال"


# ===== INSIGHTS GENERATOR =====
def gen_insights(emp, dashboard_sections, all_sheets):
    ins = []
    try:
        n = len(emp)
        ins.append({'t':'info','c':'عام','x':f'إجمالي الموظفين: {n} موظف'})

        # Department analysis
        if has(emp, 'القسم') and n > 0:
            dc = emp['القسم'].value_counts()
            ins.append({'t':'info','c':'الأقسام','x':f'عدد الأقسام: {len(dc)}. الأكبر: {dc.index[0]} ({dc.iloc[0]} موظف). الأصغر: {dc.index[-1]} ({dc.iloc[-1]} موظف)'})

        # Location analysis
        if has(emp, 'الموقع') and n > 0:
            lc = emp['الموقع'].value_counts()
            total_local = lc.get('Jeddah', 0) + lc.get('Riyadh', 0) + lc.get('جدة', 0) + lc.get('الرياض', 0)
            if total_local > 0:
                ins.append({'t':'info','c':'المواقع','x':f'الموظفين في السعودية: {total_local} ({round(total_local/n*100,1)}%). خارج السعودية: {n-total_local} ({round((n-total_local)/n*100,1)}%)'})

        # Tenure analysis
        if has(emp, 'سنوات الخدمة') and n > 0:
            avg_t = emp['سنوات الخدمة'].mean()
            new_hires = len(emp[emp['سنوات الخدمة'] < 1])
            veterans = len(emp[emp['سنوات الخدمة'] >= 5])
            ins.append({'t':'info','c':'الخدمة','x':f'متوسط الخدمة: {avg_t:.1f} سنة. جدد (<سنة): {new_hires}. خبراء (5+ سنوات): {veterans}'})
            if new_hires > n * 0.3:
                ins.append({'t':'warning','c':'الخدمة','x':f'{round(new_hires/n*100)}% من الموظفين خدمتهم أقل من سنة. نسبة عالية تحتاج خطة تأهيل واحتفاظ.'})

        # Hiring trend from dashboard
        if 'HIRING TREND BY YEAR' in dashboard_sections:
            ht = dashboard_sections['HIRING TREND BY YEAR']
            if len(ht) > 0 and 'Year' in ht.columns and 'Joiners' in ht.columns:
                last_year = ht.iloc[-1]
                prev_year = ht.iloc[-2] if len(ht) > 1 else None
                ins.append({'t':'info','c':'التوظيف','x':f'آخر سنة ({last_year["Year"]}): {last_year["Joiners"]} تعيين جديد.'})
                if prev_year is not None:
                    try:
                        growth = int(last_year["Joiners"]) - int(prev_year["Joiners"])
                        if growth > 0:
                            ins.append({'t':'success','c':'التوظيف','x':f'نمو التوظيف: +{growth} مقارنة بـ {prev_year["Year"]} ({prev_year["Joiners"]} تعيين). الشركة في توسع!'})
                    except: pass

        # Salary analysis
        if has(emp, 'الراتب الأساسي') and n > 0:
            avg_s = emp['الراتب الأساسي'].mean()
            if has(emp, 'القسم'):
                da = emp.groupby('القسم')['الراتب الأساسي'].mean()
                ins.append({'t':'info','c':'الرواتب','x':f'متوسط الراتب: {avg_s:,.0f} ريال. الأعلى: {da.idxmax()} ({da.max():,.0f}). الأقل: {da.idxmin()} ({da.min():,.0f})'})

        # Nationality/Saudization
        if has(emp, 'الجنسية') and n > 0:
            sa = emp[emp['الجنسية'].isin(['سعودي','سعودية','Saudi'])]
            p = round(len(sa)/n*100, 1)
            ins.append({'t':'success' if p>=70 else 'warning','c':'السعودة','x':f'نسبة السعودة: {p}% ({len(sa)} من {n})'})

        # Performance
        if has(emp, 'تقييم الأداء %') and n > 0:
            ap = emp['تقييم الأداء %'].mean()
            ins.append({'t':'success' if ap>=80 else 'warning','c':'الأداء','x':f'متوسط الأداء: {ap:.1f}%'})

        # Turnover
        if has(emp, 'الحالة'):
            left = emp[emp['الحالة'] != 'نشط']
            if len(left) > 0:
                rate = round(len(left)/n*100, 1)
                ins.append({'t':'danger' if rate>20 else 'warning','c':'الدوران','x':f'معدل الدوران: {rate}% ({len(left)} موظف)'})

    except: pass
    if not ins: ins.append({'t':'info','c':'عام','x':f'تم تحميل البيانات'})
    return ins


# ===== SMART QUERY ANSWERER =====
def answer_query(query, emp, dashboard_sections, all_sheets):
    """Answer any question by searching across all data"""
    answer = ""
    q = query.lower()
    n = len(emp)

    # Saudization
    if any(w in q for w in ['سعودة', 'سعودي', 'جنسية', 'nationality', 'saudi']):
        if has(emp, 'الجنسية'):
            sa = emp[emp['الجنسية'].isin(['سعودي','سعودية','Saudi'])]
            answer = f"نسبة السعودة: {round(len(sa)/n*100,1)}% ({len(sa)} سعودي من {n} موظف)"
        elif has(emp, 'الموقع'):
            sa_loc = emp[emp['الموقع'].isin(['Jeddah','Riyadh','جدة','الرياض'])]
            answer = f"لا يوجد عمود جنسية في البيانات.\n\nلكن بناءً على الموقع: {len(sa_loc)} موظف في السعودية ({round(len(sa_loc)/n*100,1)}%) و{n-len(sa_loc)} خارج السعودية ({round((n-len(sa_loc))/n*100,1)}%)\n\n"
            if has(emp, 'الموقع'):
                answer += "التوزيع الجغرافي:\n"
                for loc, cnt in emp['الموقع'].value_counts().items():
                    answer += f"  - {loc}: {cnt} ({round(cnt/n*100,1)}%)\n"
            answer += "\nملاحظة: لحساب السعودة بدقة، يحتاج الملف عمود 'الجنسية' أو 'Nationality'"
        else:
            answer = "لا يوجد بيانات جنسية أو موقع في الملف لحساب نسبة السعودة."

    # Department
    elif any(w in q for w in ['قسم', 'أقسام', 'department']):
        if has(emp, 'القسم'):
            dc = emp['القسم'].value_counts()
            answer = f"عدد الأقسام: {len(dc)}\n\nالتوزيع:\n"
            for d, c in dc.items():
                answer += f"  - {d}: {c} موظف ({round(c/n*100,1)}%)\n"

    # Location
    elif any(w in q for w in ['موقع', 'مدينة', 'مواقع', 'جغرافي', 'location', 'city']):
        if has(emp, 'الموقع'):
            lc = emp['الموقع'].value_counts()
            answer = f"التوزيع الجغرافي ({len(lc)} مواقع):\n\n"
            for l, c in lc.items():
                answer += f"  - {l}: {c} موظف ({round(c/n*100,1)}%)\n"

    # Hiring / Recruitment
    elif any(w in q for w in ['توظيف', 'تعيين', 'hiring', 'recruit', 'نمو']):
        if 'HIRING TREND BY YEAR' in dashboard_sections:
            ht = dashboard_sections['HIRING TREND BY YEAR']
            answer = "اتجاه التوظيف:\n\n"
            for _, r in ht.iterrows():
                try:
                    answer += f"  - {r['Year']}: {r['Joiners']} تعيين (إجمالي تراكمي: {r['Cumulative']})\n"
                except: pass
        else:
            if has(emp, 'تاريخ التعيين'):
                emp['سنة التعيين'] = pd.to_datetime(emp['تاريخ التعيين']).dt.year
                yc = emp['سنة التعيين'].value_counts().sort_index()
                answer = "التعيينات حسب السنة:\n\n"
                for y, c in yc.items():
                    answer += f"  - {int(y)}: {c} موظف\n"

    # Tenure / Service
    elif any(w in q for w in ['خدمة', 'خبرة', 'tenure', 'أقدمية', 'جدد']):
        if 'SERVICE TENURE DISTRIBUTION' in dashboard_sections:
            td = dashboard_sections['SERVICE TENURE DISTRIBUTION']
            answer = "توزيع سنوات الخدمة:\n\n"
            for _, r in td.iterrows():
                try:
                    answer += f"  - {r['Category']}: {r['Count']} موظف ({round(float(r['%'])*100,1)}%)\n"
                except: pass
        elif has(emp, 'سنوات الخدمة'):
            avg = emp['سنوات الخدمة'].mean()
            answer = f"متوسط سنوات الخدمة: {avg:.1f}\n\n"
            bins = [(0,1,'< سنة'),(1,2,'1-2 سنة'),(2,3,'2-3 سنوات'),(3,5,'3-5 سنوات'),(5,99,'5+ سنوات')]
            for lo,hi,label in bins:
                c = len(emp[(emp['سنوات الخدمة']>=lo)&(emp['سنوات الخدمة']<hi)])
                answer += f"  - {label}: {c} ({round(c/n*100,1)}%)\n"

    # Salary / Cost
    elif any(w in q for w in ['راتب', 'رواتب', 'تكلفة', 'salary', 'cost']):
        if has(emp, 'الراتب الأساسي'):
            answer = f"متوسط الراتب: {emp['الراتب الأساسي'].mean():,.0f} ريال\n"
            if has(emp, 'القسم'):
                answer += "\nحسب القسم:\n"
                for d, v in emp.groupby('القسم')['الراتب الأساسي'].mean().sort_values(ascending=False).items():
                    answer += f"  - {d}: {v:,.0f} ريال\n"
        else:
            answer = "لا يوجد بيانات رواتب في الملف المرفوع."

    # Performance
    elif any(w in q for w in ['أداء', 'أفضل', 'تقييم', 'performance']):
        if has(emp, 'تقييم الأداء %'):
            answer = f"متوسط الأداء: {emp['تقييم الأداء %'].mean():.1f}%\n"
            if has(emp, 'الاسم'):
                top = emp.nlargest(5, 'تقييم الأداء %')
                answer += "\nأفضل 5:\n"
                for _, r in top.iterrows():
                    answer += f"  - {r['الاسم']}: {r['تقييم الأداء %']}%\n"
        else:
            answer = "لا يوجد بيانات تقييم أداء في الملف المرفوع."

    # Turnover
    elif any(w in q for w in ['دوران', 'استقالة', 'مغادرة', 'turnover']):
        if has(emp, 'الحالة'):
            left = emp[emp['الحالة'] != 'نشط']
            answer = f"معدل الدوران: {round(len(left)/n*100,1)}%\n"
        else:
            answer = "لا يوجد بيانات حالة الموظف (نشط/مستقيل) في الملف."

    # General / default
    else:
        answer = f"ملخص البيانات المتاحة:\n\n"
        answer += f"  - إجمالي الموظفين: {n}\n"
        if has(emp, 'القسم'): answer += f"  - عدد الأقسام: {emp['القسم'].nunique()}\n"
        if has(emp, 'الموقع'): answer += f"  - عدد المواقع: {emp['الموقع'].nunique()}\n"
        if has(emp, 'سنوات الخدمة'): answer += f"  - متوسط الخدمة: {emp['سنوات الخدمة'].mean():.1f} سنة\n"
        if has(emp, 'الراتب الأساسي'): answer += f"  - متوسط الراتب: {emp['الراتب الأساسي'].mean():,.0f} ريال\n"
        if has(emp, 'تقييم الأداء %'): answer += f"  - متوسط الأداء: {emp['تقييم الأداء %'].mean():.1f}%\n"

        answer += f"\nالأعمدة المتاحة: {', '.join(emp.columns)}\n"
        answer += f"\nالأوراق المتاحة: {', '.join(dashboard_sections.keys()) if dashboard_sections else 'لا يوجد'}\n"
        answer += "\nجرب أسئلة مثل: القسم الأكبر؟ توزيع المواقع؟ اتجاه التوظيف؟ سنوات الخدمة؟"

    return answer if answer else "لم أتمكن من العثور على إجابة. جرب صياغة مختلفة."


# ===== MAIN APP =====
def main():
    with st.sidebar:
        st.markdown("<div style='text-align:center;padding:20px 0;'><div style='background:linear-gradient(135deg,#E36414,#E9C46A);width:60px;height:60px;border-radius:14px;display:flex;align-items:center;justify-content:center;margin:0 auto 12px;font-size:24px;font-weight:800;color:white;'>HR</div><h2 style='margin:0;font-size:18px;'>تحليلات الموارد البشرية</h2><p style='opacity:0.6;font-size:12px;'>رسال الود لتقنية المعلومات</p></div>", unsafe_allow_html=True)
        st.markdown("---")
        page = st.radio("📌", ["🏠 نظرة عامة","💰 الرواتب والتكاليف","🔄 الدوران الوظيفي","⚡ الأداء","👥 التوظيف","📊 الأقسام والمواقع","🤖 المحلل الذكي","📋 بيانات الموظفين","📥 تصدير"], label_visibility="collapsed")
        st.markdown("---")
        st.markdown("##### 📁 مصدر البيانات")
        file = st.file_uploader("ارفع Excel", type=["xlsx","xls","csv"], label_visibility="collapsed")
        if file: st.success("✅ تم التحميل")

    if not file:
        st.markdown("<div class='app-header'><h1>📊 منصة تحليلات الموارد البشرية الذكية</h1><p>رسال الود لتقنية المعلومات | يقرأ أي ملف Excel تلقائياً</p></div>", unsafe_allow_html=True)
        st.info("📁 ارفع أي ملف بيانات موظفين من القائمة الجانبية")
        return

    # === SMART DATA LOADING ===
    try:
        if file.name.endswith('.csv'):
            emp = pd.read_csv(file)
            emp = normalize_columns(emp)
            all_sheets = {'البيانات': emp}
            dash_sections = {}
        else:
            xl = pd.ExcelFile(file)
            all_sheets = {}
            emp = None
            dash_sections = {}

            for sheet in xl.sheet_names:
                try:
                    df_s = smart_read_sheet(xl, sheet)
                    df_s = normalize_columns(df_s)
                    all_sheets[sheet] = df_s

                    if emp is None and len(df_s) > 5:
                        # Find the main employee sheet (largest with name-like columns)
                        name_cols = [c for c in df_s.columns if any(x in str(c).lower() for x in ['name','اسم','emp','موظف'])]
                        if name_cols:
                            emp = df_s

                    if sheet.lower() == 'dashboard':
                        file.seek(0)
                        dash_sections = parse_dashboard(pd.ExcelFile(file))
                except:
                    pass

            if emp is None:
                emp = list(all_sheets.values())[0] if all_sheets else pd.DataFrame()
    except Exception as e:
        st.error(f"خطأ: {e}")
        return

    # Remove summary rows
    if '#' in emp.columns:
        emp = emp[pd.to_numeric(emp['#'], errors='coerce').notna()].reset_index(drop=True)

    n = len(emp)

    # Detect available features
    has_salary = has(emp, 'الراتب الأساسي')
    has_perf = has(emp, 'تقييم الأداء %')
    has_status = has(emp, 'الحالة')
    has_nationality = has(emp, 'الجنسية')
    has_dept = has(emp, 'القسم')
    has_location = has(emp, 'الموقع')
    has_tenure = has(emp, 'سنوات الخدمة')

    active = emp[emp['الحالة']=='نشط'] if has_status else emp
    left = emp[emp['الحالة']!='نشط'] if has_status else pd.DataFrame()

    # Filters
    with st.sidebar:
        st.markdown("##### 🔍 الفلاتر")
        af = active.copy()
        if has_dept:
            df2 = st.multiselect("القسم", list(active['القسم'].unique()), default=list(active['القسم'].unique()))
            af = af[af['القسم'].isin(df2)]
        if has_location:
            lf = st.multiselect("الموقع", list(active['الموقع'].unique()), default=list(active['الموقع'].unique()))
            af = af[af['الموقع'].isin(lf)]

    if 'ins' not in st.session_state:
        st.session_state.ins = gen_insights(emp, dash_sections, all_sheets)

    # Show detected info
    with st.sidebar:
        st.markdown("---")
        st.markdown(f"##### 📊 تم اكتشاف")
        st.markdown(f"- {n} موظف")
        st.markdown(f"- {len(all_sheets)} ورقة")
        if dash_sections:
            st.markdown(f"- {len(dash_sections)} قسم بيانات")


    # === 🏠 OVERVIEW ===
    if page == "🏠 نظرة عامة":
        st.markdown("<div class='app-header'><h1>📊 نظرة عامة</h1><p>ملخص شامل</p></div>", unsafe_allow_html=True)

        cols = st.columns(5)
        with cols[0]: st.metric("👥 الموظفين", len(af))
        with cols[1]:
            if has_dept: st.metric("🏢 الأقسام", af['القسم'].nunique())
            elif has_salary: st.metric("💰 متوسط الراتب", fmt(int(safe_mean(af,'الراتب الأساسي'))))
        with cols[2]:
            if has_location: st.metric("📍 المواقع", af['الموقع'].nunique())
            elif has_perf: st.metric("⚡ الأداء", f"{safe_mean(af,'تقييم الأداء %'):.1f}%")
        with cols[3]:
            if has_tenure: st.metric("📅 متوسط الخدمة", f"{safe_mean(af,'سنوات الخدمة'):.1f} سنة")
            elif has_status: st.metric("🔄 الدوران", f"{round(len(left)/max(n,1)*100,1)}%")
        with cols[4]:
            if has_salary: st.metric("🏦 إجمالي الرواتب", fmt(int(safe_sum(af,'الراتب الأساسي'))))
            else: st.metric("📋 الأوراق", len(all_sheets))

        st.markdown("---")
        c1, c2 = st.columns(2)

        with c1:
            if has_dept:
                dc = af['القسم'].value_counts().reset_index()
                dc.columns = ['القسم','العدد']
                fig = px.pie(dc, values='العدد', names='القسم', title='توزيع الموظفين حسب القسم', hole=0.4, color_discrete_sequence=CL['dept'])
                fig.update_layout(font=dict(family="Noto Sans Arabic"), height=400)
                st.plotly_chart(fig, use_container_width=True)

        with c2:
            if has_location:
                lc = af['الموقع'].value_counts().reset_index()
                lc.columns = ['الموقع','العدد']
                fig = px.pie(lc, values='العدد', names='الموقع', title='التوزيع الجغرافي', hole=0.4, color_discrete_sequence=px.colors.qualitative.Pastel)
                fig.update_layout(font=dict(family="Noto Sans Arabic"), height=400)
                st.plotly_chart(fig, use_container_width=True)
            elif 'HIRING TREND BY YEAR' in dash_sections:
                ht = dash_sections['HIRING TREND BY YEAR']
                try:
                    fig = go.Figure()
                    fig.add_trace(go.Bar(x=ht['Year'], y=pd.to_numeric(ht['Joiners']), name='تعيينات', marker_color=CL['primary']))
                    fig.update_layout(title='اتجاه التوظيف', font=dict(family="Noto Sans Arabic"), height=400)
                    st.plotly_chart(fig, use_container_width=True)
                except: pass

        c1, c2 = st.columns(2)
        with c1:
            if has_dept:
                dc2 = af['القسم'].value_counts().reset_index()
                dc2.columns = ['القسم','العدد']
                dc2 = dc2.sort_values('العدد', ascending=True)
                fig = px.bar(dc2, x='العدد', y='القسم', orientation='h', title='عدد الموظفين حسب القسم', color='العدد', color_continuous_scale='teal')
                fig.update_layout(font=dict(family="Noto Sans Arabic"), height=400)
                st.plotly_chart(fig, use_container_width=True)

        with c2:
            if has_tenure:
                fig = px.histogram(af, x='سنوات الخدمة', nbins=10, title='توزيع سنوات الخدمة', color_discrete_sequence=[CL['primary']])
                fig.update_layout(font=dict(family="Noto Sans Arabic"), height=400)
                st.plotly_chart(fig, use_container_width=True)
            elif 'SERVICE TENURE DISTRIBUTION' in dash_sections:
                td = dash_sections['SERVICE TENURE DISTRIBUTION']
                try:
                    fig = px.bar(td, x='Category', y=pd.to_numeric(td['Count']), title='توزيع سنوات الخدمة', color_discrete_sequence=[CL['primary']])
                    fig.update_layout(font=dict(family="Noto Sans Arabic"), height=400)
                    st.plotly_chart(fig, use_container_width=True)
                except: pass

        st.markdown("### 🤖 رؤى ذكية")
        for i in st.session_state.ins:
            insight_box(i['x'], i['t'])


    # === 💰 SALARIES ===
    elif page == "💰 الرواتب والتكاليف":
        st.markdown("<div class='app-header'><h1>💰 الرواتب والتكاليف</h1></div>", unsafe_allow_html=True)
        if has_salary:
            k1,k2,k3,k4 = st.columns(4)
            with k1: st.metric("💵 الإجمالي", fmt(int(af['الراتب الأساسي'].sum())))
            with k2: st.metric("📊 المتوسط", fmt(int(af['الراتب الأساسي'].mean())))
            with k3: st.metric("📈 الأعلى", fmt(int(af['الراتب الأساسي'].max())))
            with k4: st.metric("📉 الأقل", fmt(int(af['الراتب الأساسي'].min())))
            st.markdown("---")
            c1,c2 = st.columns(2)
            with c1:
                if has_dept:
                    ds = af.groupby('القسم')['الراتب الأساسي'].mean().reset_index().sort_values('الراتب الأساسي',ascending=True)
                    fig = px.bar(ds,x='الراتب الأساسي',y='القسم',orientation='h',title='متوسط الراتب حسب القسم',color='الراتب الأساسي',color_continuous_scale='teal')
                    fig.update_layout(font=dict(family="Noto Sans Arabic"),height=400,xaxis_tickformat=','); st.plotly_chart(fig,use_container_width=True)
            with c2:
                fig = px.histogram(af,x='الراتب الأساسي',nbins=15,title='توزيع الرواتب',color_discrete_sequence=[CL['primary']])
                fig.update_layout(font=dict(family="Noto Sans Arabic"),height=400); st.plotly_chart(fig,use_container_width=True)
        else:
            st.warning("⚠️ لا يوجد بيانات رواتب في الملف المرفوع. أضف عمود 'Salary' أو 'الراتب الأساسي' للتحليل.")


    # === 🔄 TURNOVER ===
    elif page == "🔄 الدوران الوظيفي":
        st.markdown("<div class='app-header'><h1>🔄 الدوران الوظيفي</h1></div>", unsafe_allow_html=True)
        if has_status:
            k1,k2 = st.columns(2)
            with k1: st.metric("🔄 الدوران", f"{round(len(left)/max(n,1)*100,1)}%")
            with k2: st.metric("✅ نشطين", len(active))
        else:
            st.info("لا يوجد عمود حالة الموظف. يُعرض اتجاه التوظيف بدلاً من ذلك.")
            if 'HIRING TREND BY YEAR' in dash_sections:
                ht = dash_sections['HIRING TREND BY YEAR']
                try:
                    fig = go.Figure()
                    fig.add_trace(go.Bar(x=ht['Year'], y=pd.to_numeric(ht['Joiners']), name='تعيينات جديدة', marker_color=CL['success']))
                    fig.add_trace(go.Scatter(x=ht['Year'], y=pd.to_numeric(ht['Cumulative']), name='الإجمالي التراكمي', mode='lines+markers', yaxis='y2'))
                    fig.update_layout(title='اتجاه التوظيف السنوي', font=dict(family="Noto Sans Arabic"), height=450,
                        yaxis2=dict(overlaying='y', side='right', title='التراكمي'))
                    st.plotly_chart(fig, use_container_width=True)
                except: pass
            elif has(emp, 'تاريخ التعيين'):
                emp_copy = emp.copy()
                emp_copy['سنة'] = pd.to_datetime(emp_copy['تاريخ التعيين']).dt.year
                yc = emp_copy['سنة'].value_counts().sort_index().reset_index()
                yc.columns = ['السنة','العدد']
                fig = px.bar(yc, x='السنة', y='العدد', title='التعيينات حسب السنة', color_discrete_sequence=[CL['primary']])
                fig.update_layout(font=dict(family="Noto Sans Arabic"), height=400)
                st.plotly_chart(fig, use_container_width=True)


    # === ⚡ PERFORMANCE ===
    elif page == "⚡ الأداء":
        st.markdown("<div class='app-header'><h1>⚡ الأداء</h1></div>", unsafe_allow_html=True)
        if has_perf:
            ap = af['تقييم الأداء %'].mean()
            k1,k2,k3 = st.columns(3)
            with k1: st.metric("⚡ المتوسط", f"{ap:.1f}%")
            with k2: st.metric("🌟 ممتاز", len(af[af['تقييم الأداء %']>=90]))
            with k3: st.metric("⚠️ تطوير", len(af[af['تقييم الأداء %']<70]))
        else:
            st.warning("⚠️ لا يوجد بيانات أداء. أضف عمود 'Performance %' أو 'تقييم الأداء %'")


    # === 👥 RECRUITMENT ===
    elif page == "👥 التوظيف":
        st.markdown("<div class='app-header'><h1>👥 التوظيف</h1></div>", unsafe_allow_html=True)
        if 'HIRING TREND BY YEAR' in dash_sections:
            ht = dash_sections['HIRING TREND BY YEAR']
            try:
                c1,c2 = st.columns(2)
                with c1:
                    fig = px.bar(ht, x='Year', y=pd.to_numeric(ht['Joiners']), title='التعيينات السنوية', color_discrete_sequence=[CL['success']], text=ht['Joiners'])
                    fig.update_layout(font=dict(family="Noto Sans Arabic"), height=400)
                    st.plotly_chart(fig, use_container_width=True)
                with c2:
                    fig = go.Figure()
                    fig.add_trace(go.Scatter(x=ht['Year'], y=pd.to_numeric(ht['Cumulative']), mode='lines+markers+text', text=ht['Cumulative'], textposition='top center', line=dict(color=CL['primary'], width=3), fill='tozeroy', fillcolor='rgba(15,76,92,0.1)'))
                    fig.update_layout(title='النمو التراكمي', font=dict(family="Noto Sans Arabic"), height=400)
                    st.plotly_chart(fig, use_container_width=True)
            except: pass
        elif has(emp, 'تاريخ التعيين'):
            ec = emp.copy()
            ec['سنة'] = pd.to_datetime(ec['تاريخ التعيين']).dt.year
            yc = ec['سنة'].value_counts().sort_index().reset_index()
            yc.columns = ['السنة','العدد']
            fig = px.bar(yc, x='السنة', y='العدد', title='التعيينات حسب السنة', text='العدد', color_discrete_sequence=[CL['success']])
            fig.update_layout(font=dict(family="Noto Sans Arabic"), height=400)
            st.plotly_chart(fig, use_container_width=True)

        # Department hiring
        if has_dept and has(emp, 'تاريخ التعيين'):
            ec = emp.copy()
            ec['سنة'] = pd.to_datetime(ec['تاريخ التعيين']).dt.year
            recent = ec[ec['سنة'] >= ec['سنة'].max() - 1]
            if len(recent) > 0:
                fig = px.bar(recent['القسم'].value_counts().reset_index().rename(columns={'index':'القسم','القسم':'القسم','count':'العدد'}),
                    x='القسم', y='العدد' if 'العدد' in recent['القسم'].value_counts().reset_index().columns else 'count',
                    title=f'التعيينات حسب القسم (آخر سنتين)', color_discrete_sequence=[CL['primary']])
                fig.update_layout(font=dict(family="Noto Sans Arabic"), height=400)
                st.plotly_chart(fig, use_container_width=True)


    # === 📊 DEPARTMENTS & LOCATIONS ===
    elif page == "📊 الأقسام والمواقع":
        st.markdown("<div class='app-header'><h1>📊 الأقسام والمواقع</h1></div>", unsafe_allow_html=True)

        if has_dept:
            c1,c2 = st.columns(2)
            with c1:
                dc = af['القسم'].value_counts().reset_index()
                dc.columns = ['القسم','العدد']
                fig = px.bar(dc, x='العدد', y='القسم', orientation='h', title='حجم كل قسم', color='العدد', color_continuous_scale='teal')
                fig.update_layout(font=dict(family="Noto Sans Arabic"), height=500)
                st.plotly_chart(fig, use_container_width=True)
            with c2:
                if has_tenure:
                    dt2 = af.groupby('القسم')['سنوات الخدمة'].mean().reset_index().sort_values('سنوات الخدمة', ascending=True)
                    dt2.columns = ['القسم','المتوسط']
                    fig = px.bar(dt2, x='المتوسط', y='القسم', orientation='h', title='متوسط سنوات الخدمة', color='المتوسط', color_continuous_scale='oranges')
                    fig.update_layout(font=dict(family="Noto Sans Arabic"), height=500)
                    st.plotly_chart(fig, use_container_width=True)

        if has_location:
            lc = af['الموقع'].value_counts().reset_index()
            lc.columns = ['الموقع','العدد']
            fig = px.bar(lc, x='الموقع', y='العدد', title='التوزيع الجغرافي', color='الموقع', color_discrete_sequence=CL['dept'])
            fig.update_layout(font=dict(family="Noto Sans Arabic"), height=400)
            st.plotly_chart(fig, use_container_width=True)


    # === 🤖 SMART ANALYST ===
    elif page == "🤖 المحلل الذكي":
        st.markdown("<div class='app-header'><h1>🤖 المحلل الذكي</h1><p>يجيب من كل الأوراق تلقائياً</p></div>", unsafe_allow_html=True)

        st.markdown("### 📊 رؤى تلقائية")
        for i in st.session_state.ins:
            insight_box(f"**[{i['c']}]** {i['x']}", i['t'])

        st.markdown("---")
        st.markdown("### 💬 اسأل أي سؤال")

        # Dynamic quick questions based on available data
        qq = ["كم عدد الأقسام وتوزيع الموظفين؟"]
        if has_location: qq.append("ما التوزيع الجغرافي؟")
        if has_tenure: qq.append("ما متوسط سنوات الخدمة؟")
        if has_salary: qq.append("ما القسم الأعلى تكلفة؟")
        if has_perf: qq.append("من أفضل 5 أداءً؟")
        qq.append("ما نسبة السعودة؟")
        qq.append("ما اتجاه التوظيف؟")

        sq = st.selectbox("أسئلة سريعة:", ["اختر..."] + qq)
        uq = st.text_input("أو اكتب:", placeholder="مثال: كم موظف في جدة؟")
        q = uq if uq else (sq if sq != "اختر..." else "")

        if st.button("🔍 تحليل", type="primary", use_container_width=True) and q:
            with st.spinner("جاري البحث في كل الأوراق..."):
                answer = answer_query(q, emp, dash_sections, all_sheets)
                st.markdown("### 📝 النتيجة")
                st.info(answer)


    # === 📋 DATA ===
    elif page == "📋 بيانات الموظفين":
        st.markdown("<div class='app-header'><h1>📋 البيانات</h1></div>", unsafe_allow_html=True)

        # Sheet selector
        sheet_name = st.selectbox("اختر الورقة:", list(all_sheets.keys()))
        display_df = all_sheets[sheet_name]

        st.markdown(f"**{len(display_df)}** سجل | **{len(display_df.columns)}** عمود")
        sr = st.text_input("🔍 بحث:", placeholder="اكتب للبحث...")
        if sr:
            m = display_df.apply(lambda r: r.astype(str).str.contains(sr, case=False).any(), axis=1)
            display_df = display_df[m]
            st.markdown(f"**النتائج:** {len(display_df)}")
        st.dataframe(display_df, use_container_width=True, hide_index=True, height=600)


    # === 📥 EXPORT ===
    elif page == "📥 تصدير":
        st.markdown("<div class='app-header'><h1>📥 تصدير</h1></div>", unsafe_allow_html=True)
        rp = {}
        for name, sdf in all_sheets.items():
            if st.checkbox(f"📊 {name}", value=(name == list(all_sheets.keys())[0])):
                rp[name] = sdf

        if st.checkbox("📈 ملخص"):
            rows = [['الموظفين', str(n)]]
            if has_dept: rows.append(['الأقسام', str(emp['القسم'].nunique())])
            if has_location: rows.append(['المواقع', str(emp['الموقع'].nunique())])
            if has_tenure: rows.append(['متوسط الخدمة', f"{emp['سنوات الخدمة'].mean():.1f}"])
            if has_salary: rows.append(['متوسط الراتب', f"{emp['الراتب الأساسي'].mean():,.0f}"])
            rp['ملخص'] = pd.DataFrame(rows, columns=['المؤشر','القيمة'])

        if rp:
            o = io.BytesIO()
            with pd.ExcelWriter(o, engine='xlsxwriter') as w:
                for nm, d in rp.items():
                    safe_name = nm[:31]
                    d.to_excel(w, sheet_name=safe_name, index=False)
                    w.sheets[safe_name].right_to_left()
            st.download_button("📥 تحميل", data=o.getvalue(),
                file_name=f"HR_{datetime.now().strftime('%Y%m%d')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                type="primary", use_container_width=True)
        else:
            st.warning("اختر تقرير")


if __name__ == "__main__":
    main()
