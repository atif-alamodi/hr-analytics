# ===================================================
# تطبيق تحليلات الموارد البشرية - رسال الود لتقنية المعلومات
# HR Analytics AI Platform - Complete Production App
# ===================================================
# التشغيل:
#   pip install streamlit pandas plotly openpyxl anthropic xlsxwriter
#   streamlit run hr_analytics_app.py
# ===================================================

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import io
from datetime import datetime

# ===== PAGE CONFIG =====
st.set_page_config(
    page_title="تحليلات HR | رسال الود",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ===== CUSTOM CSS =====
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Noto+Sans+Arabic:wght@300;400;500;600;700;800&display=swap');
    
    * { font-family: 'Noto Sans Arabic', sans-serif; }
    
    .main .block-container { padding-top: 1rem; max-width: 1400px; }
    
    /* Sidebar */
    [data-testid="stSidebar"] {
        background: linear-gradient(180deg, #0F4C5C 0%, #1A1A2E 100%);
    }
    [data-testid="stSidebar"] * { color: white !important; }
    [data-testid="stSidebar"] .stRadio label { 
        font-size: 15px !important; padding: 8px 12px !important; 
        border-radius: 8px; transition: background 0.2s;
    }
    [data-testid="stSidebar"] .stRadio label:hover { background: rgba(255,255,255,0.1); }
    
    /* KPI Cards */
    [data-testid="stMetric"] {
        background: white; border-radius: 12px; padding: 16px 20px;
        box-shadow: 0 1px 3px rgba(0,0,0,0.06), 0 4px 12px rgba(0,0,0,0.04);
        border: 1px solid #E2E8F0;
    }
    [data-testid="stMetric"] label { font-size: 13px !important; color: #64748B !important; }
    [data-testid="stMetric"] [data-testid="stMetricValue"] { font-size: 24px !important; font-weight: 700 !important; }
    
    /* Headers */
    h1 { color: #0F4C5C !important; font-weight: 800 !important; }
    h2, h3 { color: #1A1A2E !important; font-weight: 700 !important; }
    
    /* Tabs */
    .stTabs [data-baseweb="tab-list"] { gap: 4px; }
    .stTabs [data-baseweb="tab"] { 
        border-radius: 8px 8px 0 0; padding: 10px 20px;
        font-weight: 600; font-size: 14px;
    }
    
    /* Info boxes */
    .insight-box {
        background: #EFF6FF; border-radius: 12px; padding: 16px 20px;
        border-right: 4px solid #3B82F6; margin-bottom: 12px;
        font-size: 14px; line-height: 1.8;
    }
    .insight-warning { background: #FFF7ED; border-right-color: #F97316; }
    .insight-success { background: #F0FDF4; border-right-color: #22C55E; }
    .insight-danger { background: #FEF2F2; border-right-color: #EF4444; }
    
    /* Tables */
    .dataframe { border-radius: 8px !important; font-size: 13px !important; }
    
    /* Hide streamlit branding */
    #MainMenu { visibility: hidden; }
    footer { visibility: hidden; }
    
    .app-header {
        background: linear-gradient(135deg, #0F4C5C, #1A1A2E);
        padding: 24px 32px; border-radius: 16px; margin-bottom: 24px;
        color: white;
    }
    .app-header h1 { color: white !important; margin: 0; font-size: 28px; }
    .app-header p { color: rgba(255,255,255,0.7); margin: 4px 0 0; font-size: 14px; }
    
    div[data-testid="stExpander"] { 
        background: white; border-radius: 12px; 
        border: 1px solid #E2E8F0; 
    }
</style>
""", unsafe_allow_html=True)


# ===== HELPER FUNCTIONS =====
def insight_box(text, type="info"):
    cls = f"insight-box insight-{type}" if type != "info" else "insight-box"
    icons = {"info": "💡", "warning": "⚠️", "success": "✅", "danger": "🚨"}
    st.markdown(f'<div class="{cls}">{icons.get(type, "💡")} {text}</div>', unsafe_allow_html=True)

def format_rial(value):
    return f"{value:,.0f} ريال"

def calculate_total_cost(row):
    salary = row['الراتب الأساسي']
    housing = salary * 0.25
    transport = 700
    gosi = salary * 0.1175
    medical = 500
    return salary + housing + transport + gosi + medical

def generate_ai_insights(df, active, df_salary, df_turnover):
    """توليد رؤى ذكية تلقائية من البيانات"""
    insights = []
    
    try:
        # 1. تحليل الرواتب
        if 'القسم' in active.columns and 'الراتب الأساسي' in active.columns:
            dept_avg = active.groupby('القسم')['الراتب الأساسي'].mean()
            if len(dept_avg) > 0:
                highest_dept = dept_avg.idxmax()
                lowest_dept = dept_avg.idxmin()
                gap = dept_avg.max() - dept_avg.min()
                insights.append({
                    'type': 'info',
                    'category': 'الرواتب',
                    'text': f'فجوة الرواتب بين الأقسام: {gap:,.0f} ريال. أعلى قسم: {highest_dept} ({dept_avg.max():,.0f} ريال)، أقل قسم: {lowest_dept} ({dept_avg.min():,.0f} ريال)'
                })
        
        # 2. تحليل الدوران
        if 'الحالة' in df.columns:
            left = df[df['الحالة'] != 'نشط']
            if len(left) > 0:
                turnover_rate = round(len(left) / len(df) * 100, 1)
                if 'القسم' in left.columns:
                    dept_turnover = left['القسم'].value_counts()
                    worst_dept = dept_turnover.index[0] if len(dept_turnover) > 0 else 'غير محدد'
                    worst_count = dept_turnover.iloc[0] if len(dept_turnover) > 0 else 0
                else:
                    worst_dept = 'غير محدد'
                    worst_count = 0
                t_type = 'danger' if turnover_rate > 20 else 'warning' if turnover_rate > 10 else 'success'
                insights.append({
                    'type': t_type,
                    'category': 'الدوران',
                    'text': f'معدل الدوران الإجمالي: {turnover_rate}%. أعلى قسم دوراناً: {worst_dept} ({worst_count} موظف). {"يحتاج تدخل عاجل!" if turnover_rate > 20 else ""}'
                })
                
                if 'سبب المغادرة' in left.columns:
                    top_reason = left['سبب المغادرة'].value_counts()
                    top_reason = top_reason[top_reason.index != '']
                    if len(top_reason) > 0:
                        insights.append({
                            'type': 'warning',
                            'category': 'الدوران',
                            'text': f'أكثر سبب للمغادرة: "{top_reason.index[0]}" ({top_reason.iloc[0]} موظف). يُنصح بمعالجة هذا السبب لتقليل الدوران.'
                        })
        
        # 3. تحليل الأداء
        if 'تقييم الأداء %' in active.columns:
            avg_perf = active['تقييم الأداء %'].mean()
            low_perf = active[active['تقييم الأداء %'] < 70]
            high_perf = active[active['تقييم الأداء %'] >= 90]
            
            insights.append({
                'type': 'success' if avg_perf >= 80 else 'warning',
                'category': 'الأداء',
                'text': f'متوسط الأداء العام: {avg_perf:.1f}%. عدد الموظفين الممتازين (90%+): {len(high_perf)}. يحتاج تطوير (أقل من 70%): {len(low_perf)}.'
            })
            
            if 'الحالة' in df.columns:
                left = df[df['الحالة'] != 'نشط']
                if len(left) > 0 and 'تقييم الأداء %' in left.columns:
                    left_avg_perf = left['تقييم الأداء %'].mean()
                    if left_avg_perf < avg_perf:
                        insights.append({
                            'type': 'info',
                            'category': 'الأداء',
                            'text': f'متوسط أداء الموظفين المغادرين ({left_avg_perf:.1f}%) أقل من المتوسط العام ({avg_perf:.1f}%). هناك علاقة بين انخفاض الأداء والمغادرة.'
                        })
        
        # 4. تحليل التكلفة
        if df_salary is not None and 'إجمالي التكلفة الشهرية' in df_salary.columns:
            total_monthly = df_salary['إجمالي التكلفة الشهرية'].sum()
            avg_cost = df_salary['إجمالي التكلفة الشهرية'].mean()
            insights.append({
                'type': 'info',
                'category': 'التكلفة',
                'text': f'إجمالي التكلفة الشهرية: {total_monthly:,.0f} ريال. متوسط التكلفة لكل موظف: {avg_cost:,.0f} ريال (الراتب + 30% بدلات وتأمينات تقريباً).'
            })
        
        # 5. تحليل التوظيف
        if 'الجنسية' in active.columns and len(active) > 0:
            saudi = active[active['الجنسية'].isin(['سعودي', 'سعودية'])]
            saudization = round(len(saudi) / len(active) * 100, 1)
            s_type = 'success' if saudization >= 70 else 'warning' if saudization >= 50 else 'danger'
            insights.append({
                'type': s_type,
                'category': 'السعودة',
                'text': f'نسبة السعودة: {saudization}% ({len(saudi)} من {len(active)} موظف). {"ممتازة!" if saudization >= 70 else "تحتاج تحسين لتحقيق نسب نطاقات."}'
            })
        
        # 6. الغياب
        if 'أيام الغياب' in active.columns:
            high_absence = active[active['أيام الغياب'] > 10]
            if len(high_absence) > 0:
                insights.append({
                    'type': 'warning',
                    'category': 'الغياب',
                    'text': f'{len(high_absence)} موظف لديهم أكثر من 10 أيام غياب. متوسط الغياب: {active["أيام الغياب"].mean():.1f} يوم. يُنصح بمراجعة سياسات الحضور.'
                })
    except Exception as e:
        insights.append({
            'type': 'warning',
            'category': 'النظام',
            'text': f'تعذر توليد بعض التحليلات. تأكد من تطابق أسماء الأعمدة مع التنسيق المطلوب.'
        })
    
    if not insights:
        insights.append({
            'type': 'info',
            'category': 'عام',
            'text': f'تم تحميل البيانات بنجاح. عدد السجلات: {len(df)}.'
        })
    
    return insights


def export_to_excel(dataframes_dict):
    """تصدير البيانات إلى Excel"""
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        for sheet_name, df in dataframes_dict.items():
            df.to_excel(writer, sheet_name=sheet_name, index=False)
            worksheet = writer.sheets[sheet_name]
            worksheet.right_to_left()
            for i, col in enumerate(df.columns):
                max_len = max(df[col].astype(str).map(len).max(), len(col)) + 4
                worksheet.set_column(i, i, min(max_len, 25))
    return output.getvalue()


# ===== MAIN APP =====
def main():
    # Sidebar
    with st.sidebar:
        st.markdown("""
        <div style='text-align: center; padding: 20px 0;'>
            <div style='background: linear-gradient(135deg, #E36414, #E9C46A); width: 60px; height: 60px; 
                        border-radius: 14px; display: flex; align-items: center; justify-content: center; 
                        margin: 0 auto 12px; font-size: 24px; font-weight: 800; color: white;'>HR</div>
            <h2 style='margin: 0; font-size: 18px;'>تحليلات الموارد البشرية</h2>
            <p style='opacity: 0.6; font-size: 12px; margin-top: 4px;'>رسال الود لتقنية المعلومات</p>
        </div>
        """, unsafe_allow_html=True)
        
        st.markdown("---")
        
        page = st.radio(
            "📌 التنقل",
            ["🏠 نظرة عامة", "💰 الرواتب والتكاليف", "🔄 الدوران الوظيفي", 
             "⚡ الأداء والإنتاجية", "👥 التوظيف والاستقطاب", "🤖 المحلل الذكي",
             "📋 بيانات الموظفين", "📥 تصدير التقارير"],
            label_visibility="collapsed"
        )
        
        st.markdown("---")
        
        # File uploader
        st.markdown("##### 📁 مصدر البيانات")
        file = st.file_uploader("ارفع ملف Excel", type=["xlsx", "xls", "csv"], label_visibility="collapsed")
        
        if file:
            st.success("✅ تم تحميل البيانات")
        
        st.markdown("---")
        st.markdown("""
        <div style='text-align: center; opacity: 0.5; font-size: 11px; padding-top: 20px;'>
            الإصدار 1.0<br>
            مارس 2026
        </div>
        """, unsafe_allow_html=True)
    
    # ===== LOAD DATA =====
    if not file:
        # Header
        st.markdown("""
        <div class='app-header'>
            <h1>📊 منصة تحليلات الموارد البشرية بالذكاء الاصطناعي</h1>
            <p>رسال الود لتقنية المعلومات</p>
        </div>
        """, unsafe_allow_html=True)
        
        st.markdown("### 👋 مرحباً عاطف!")
        st.info("📁 ارفع ملف بيانات الموظفين من القائمة الجانبية للبدء في التحليل")
        
        with st.expander("📖 كيفية الاستخدام", expanded=True):
            st.markdown("""
            **1.** حمّل ملف البيانات التجريبية المرفق (أو استخدم بيانات حقيقية بنفس التنسيق)
            
            **2.** ارفع الملف من القائمة الجانبية
            
            **3.** تنقل بين الصفحات لاستكشاف التحليلات:
            - 🏠 **نظرة عامة** - ملخص شامل لجميع المؤشرات
            - 💰 **الرواتب والتكاليف** - تحليل مفصل بالريال لكل موظف
            - 🔄 **الدوران الوظيفي** - تتبع الاستقالات والتعيينات
            - ⚡ **الأداء** - تقييمات وتصنيفات الموظفين
            - 👥 **التوظيف** - قمع الاستقطاب ومصادر التوظيف
            - 🤖 **المحلل الذكي** - اسأل أي سؤال عن بياناتك
            - 📋 **بيانات الموظفين** - عرض وتصفية البيانات الخام
            - 📥 **تصدير التقارير** - حمّل التقارير بصيغة Excel
            """)
        return
    
    # Read data
    try:
        if file.name.endswith('.csv'):
            df = pd.read_csv(file)
            sheets = {'بيانات الموظفين': df}
        else:
            sheets = pd.read_excel(file, sheet_name=None)
            df = sheets.get('بيانات الموظفين', list(sheets.values())[0])
    except Exception as e:
        st.error(f"خطأ في قراءة الملف: {e}")
        return
    
    df_salary = sheets.get('الرواتب والتكاليف', None)
    df_turnover = sheets.get('الدوران الوظيفي', None)
    df_performance = sheets.get('تحليل الأداء', None)
    df_recruitment = sheets.get('التوظيف والاستقطاب', None)
    df_dept_summary = sheets.get('ملخص الأقسام', None)
    
    active = df[df['الحالة'] == 'نشط'] if 'الحالة' in df.columns else df
    left = df[df['الحالة'] != 'نشط'] if 'الحالة' in df.columns else pd.DataFrame()
    
    # Store in session
    if 'insights' not in st.session_state:
        st.session_state.insights = generate_ai_insights(df, active, df_salary, df_turnover)
    
    # ===== SIDEBAR FILTERS =====
    with st.sidebar:
        st.markdown("##### 🔍 الفلاتر")
        if 'القسم' in active.columns:
            dept_filter = st.multiselect("القسم", active['القسم'].unique(), default=active['القسم'].unique())
            active_filtered = active[active['القسم'].isin(dept_filter)]
        else:
            active_filtered = active
            dept_filter = []
        
        if 'الدرجة' in active.columns:
            grade_filter = st.multiselect("الدرجة", active['الدرجة'].unique(), default=active['الدرجة'].unique())
            active_filtered = active_filtered[active_filtered['الدرجة'].isin(grade_filter)]
    
    # ===== COLORS =====
    colors = {
        'primary': '#0F4C5C', 'secondary': '#E36414', 'accent': '#9A031E',
        'success': '#2D6A4F', 'warning': '#E9C46A', 'info': '#264653',
        'teal_scale': ['#0F4C5C', '#1A6B7C', '#2D8A9C', '#4EAABB', '#7CCBDB'],
        'dept_colors': px.colors.qualitative.Set2,
    }
    
    
    # ======================================================
    #                    🏠 نظرة عامة
    # ======================================================
    if page == "🏠 نظرة عامة":
        st.markdown("""
        <div class='app-header'>
            <h1>📊 نظرة عامة على الموارد البشرية</h1>
            <p>ملخص شامل لأهم المؤشرات والرؤى الذكية</p>
        </div>
        """, unsafe_allow_html=True)
        
        # KPIs
        k1, k2, k3, k4, k5 = st.columns(5)
        with k1:
            st.metric("👥 الموظفين النشطين", len(active_filtered))
        with k2:
            avg_sal = int(active_filtered['الراتب الأساسي'].mean()) if 'الراتب الأساسي' in active_filtered.columns else 0
            st.metric("💰 متوسط الراتب", format_rial(avg_sal))
        with k3:
            avg_perf = round(active_filtered['تقييم الأداء %'].mean(), 1) if 'تقييم الأداء %' in active_filtered.columns else 0
            st.metric("⚡ متوسط الأداء", f"{avg_perf}%")
        with k4:
            turnover_rate = round(len(left) / len(df) * 100, 1) if len(df) > 0 else 0
            st.metric("🔄 معدل الدوران", f"{turnover_rate}%")
        with k5:
            if df_salary is not None and 'إجمالي التكلفة الشهرية' in df_salary.columns:
                total_cost = int(df_salary['إجمالي التكلفة الشهرية'].sum())
            else:
                total_cost = int(active_filtered['الراتب الأساسي'].sum() * 1.3) if 'الراتب الأساسي' in active_filtered.columns else 0
            st.metric("🏦 التكلفة الشهرية", format_rial(total_cost))
        
        st.markdown("---")
        
        # Charts Row 1
        c1, c2 = st.columns(2)
        
        with c1:
            if 'القسم' in active_filtered.columns:
                dept_count = active_filtered['القسم'].value_counts().reset_index()
                dept_count.columns = ['القسم', 'العدد']
                fig = px.pie(dept_count, values='العدد', names='القسم',
                           title='📊 توزيع الموظفين حسب القسم',
                           color_discrete_sequence=colors['dept_colors'],
                           hole=0.4)
                fig.update_layout(font=dict(family="Noto Sans Arabic"), height=400)
                st.plotly_chart(fig, use_container_width=True)
        
        with c2:
            if df_turnover is not None and 'عدد الموظفين آخر الشهر' in df_turnover.columns:
                month_col = 'الشهر' if 'الشهر' in df_turnover.columns else df_turnover.columns[0]
                fig = go.Figure()
                fig.add_trace(go.Scatter(
                    x=df_turnover[month_col], y=df_turnover['عدد الموظفين آخر الشهر'],
                    mode='lines+markers', name='عدد الموظفين',
                    line=dict(color=colors['primary'], width=3),
                    marker=dict(size=8), fill='tozeroy',
                    fillcolor='rgba(15,76,92,0.1)'
                ))
                fig.update_layout(title='📈 تطور عدد الموظفين', font=dict(family="Noto Sans Arabic"), height=400)
                st.plotly_chart(fig, use_container_width=True)
        
        # Charts Row 2
        c1, c2 = st.columns(2)
        
        with c1:
            if 'القسم' in active_filtered.columns and 'الراتب الأساسي' in active_filtered.columns:
                dept_sal = active_filtered.groupby('القسم')['الراتب الأساسي'].mean().reset_index()
                dept_sal.columns = ['القسم', 'متوسط الراتب']
                dept_sal = dept_sal.sort_values('متوسط الراتب', ascending=True)
                fig = px.bar(dept_sal, x='متوسط الراتب', y='القسم', orientation='h',
                           title='💰 متوسط الراتب حسب القسم',
                           color='متوسط الراتب', color_continuous_scale='teal')
                fig.update_layout(font=dict(family="Noto Sans Arabic"), height=400, xaxis_tickformat=',')
                st.plotly_chart(fig, use_container_width=True)
        
        with c2:
            if 'تقييم الأداء %' in active_filtered.columns and 'القسم' in active_filtered.columns:
                dept_perf = active_filtered.groupby('القسم')['تقييم الأداء %'].mean().reset_index()
                dept_perf.columns = ['القسم', 'متوسط الأداء']
                fig = px.bar(dept_perf, x='القسم', y='متوسط الأداء',
                           title='⚡ متوسط الأداء حسب القسم',
                           color='متوسط الأداء', color_continuous_scale='RdYlGn',
                           range_color=[60, 100])
                fig.update_layout(font=dict(family="Noto Sans Arabic"), height=400)
                st.plotly_chart(fig, use_container_width=True)
        
        # AI Insights
        st.markdown("### 🤖 رؤى الذكاء الاصطناعي")
        for ins in st.session_state.insights:
            insight_box(ins['text'], ins['type'])
    
    
    # ======================================================
    #                 💰 الرواتب والتكاليف
    # ======================================================
    elif page == "💰 الرواتب والتكاليف":
        st.markdown("""
        <div class='app-header'>
            <h1>💰 تحليل الرواتب والتكاليف</h1>
            <p>التكلفة الكلية بالريال لكل موظف وقسم</p>
        </div>
        """, unsafe_allow_html=True)
        
        # KPIs
        k1, k2, k3, k4 = st.columns(4)
        sal_col = 'الراتب الأساسي'
        with k1:
            st.metric("💵 إجمالي الرواتب", format_rial(int(active_filtered[sal_col].sum())))
        with k2:
            st.metric("📊 متوسط الراتب", format_rial(int(active_filtered[sal_col].mean())))
        with k3:
            st.metric("📈 أعلى راتب", format_rial(int(active_filtered[sal_col].max())))
        with k4:
            st.metric("📉 أقل راتب", format_rial(int(active_filtered[sal_col].min())))
        
        st.markdown("---")
        
        c1, c2 = st.columns(2)
        
        with c1:
            # Salary by Department
            dept_sal = active_filtered.groupby('القسم')[sal_col].agg(['mean', 'min', 'max', 'sum', 'count']).reset_index()
            dept_sal.columns = ['القسم', 'المتوسط', 'الأقل', 'الأعلى', 'الإجمالي', 'العدد']
            dept_sal = dept_sal.sort_values('المتوسط', ascending=True)
            
            fig = go.Figure()
            fig.add_trace(go.Bar(x=dept_sal['المتوسط'], y=dept_sal['القسم'], orientation='h',
                                name='متوسط الراتب', marker_color=colors['primary']))
            fig.update_layout(title='متوسط الراتب حسب القسم', font=dict(family="Noto Sans Arabic"),
                            height=400, xaxis_tickformat=',')
            st.plotly_chart(fig, use_container_width=True)
        
        with c2:
            # Salary Distribution
            fig = px.histogram(active_filtered, x=sal_col, nbins=15,
                             title='توزيع الرواتب', color_discrete_sequence=[colors['primary']])
            fig.update_layout(font=dict(family="Noto Sans Arabic"), height=400,
                            xaxis_title='الراتب (ريال)', yaxis_title='عدد الموظفين',
                            xaxis_tickformat=',')
            st.plotly_chart(fig, use_container_width=True)
        
        # Cost Breakdown
        if df_salary is not None:
            st.markdown("### 📊 تفصيل التكلفة الكلية لكل موظف")
            
            cost_cols = [c for c in ['الراتب الأساسي', 'بدل السكن (25%)', 'بدل النقل', 
                                     'التأمينات الاجتماعية (11.75%)', 'التأمين الطبي', 'بدلات أخرى'] 
                        if c in df_salary.columns]
            
            if cost_cols and 'الاسم' in df_salary.columns:
                df_cost = df_salary[['الاسم'] + cost_cols].copy()
                df_melted = df_cost.melt(id_vars='الاسم', var_name='البند', value_name='المبلغ')
                
                fig = px.bar(df_melted, x='الاسم', y='المبلغ', color='البند',
                           title='تفصيل التكلفة لكل موظف', barmode='stack',
                           color_discrete_sequence=px.colors.qualitative.Set2)
                fig.update_layout(font=dict(family="Noto Sans Arabic"), height=500,
                                xaxis_tickangle=-45, yaxis_tickformat=',')
                st.plotly_chart(fig, use_container_width=True)
            
            # Cost Table
            with st.expander("📋 جدول التكاليف التفصيلي"):
                display_cols = [c for c in df_salary.columns if c not in ['index']]
                st.dataframe(df_salary[display_cols], use_container_width=True, hide_index=True)
        
        # Salary by Grade
        if 'الدرجة' in active_filtered.columns:
            st.markdown("### 📊 تحليل الرواتب حسب الدرجة الوظيفية")
            c1, c2 = st.columns(2)
            with c1:
                grade_sal = active_filtered.groupby('الدرجة')[sal_col].mean().reset_index()
                grade_sal.columns = ['الدرجة', 'متوسط الراتب']
                fig = px.bar(grade_sal, x='الدرجة', y='متوسط الراتب', 
                           color='الدرجة', title='متوسط الراتب حسب الدرجة',
                           color_discrete_sequence=[colors['success'], colors['primary'], colors['secondary']])
                fig.update_layout(font=dict(family="Noto Sans Arabic"), yaxis_tickformat=',')
                st.plotly_chart(fig, use_container_width=True)
            
            with c2:
                fig = px.box(active_filtered, x='الدرجة', y=sal_col,
                           title='توزيع الرواتب حسب الدرجة', color='الدرجة',
                           color_discrete_sequence=[colors['success'], colors['primary'], colors['secondary']])
                fig.update_layout(font=dict(family="Noto Sans Arabic"), yaxis_tickformat=',')
                st.plotly_chart(fig, use_container_width=True)
        
        # Insights
        salary_insights = [i for i in st.session_state.insights if i['category'] in ['الرواتب', 'التكلفة']]
        if salary_insights:
            st.markdown("### 💡 رؤى الرواتب")
            for ins in salary_insights:
                insight_box(ins['text'], ins['type'])
    
    
    # ======================================================
    #                 🔄 الدوران الوظيفي
    # ======================================================
    elif page == "🔄 الدوران الوظيفي":
        st.markdown("""
        <div class='app-header'>
            <h1>🔄 تحليل الدوران الوظيفي</h1>
            <p>تتبع الاستقالات والتعيينات وأسباب المغادرة</p>
        </div>
        """, unsafe_allow_html=True)
        
        # KPIs
        k1, k2, k3, k4 = st.columns(4)
        with k1:
            st.metric("🔄 معدل الدوران", f"{round(len(left)/len(df)*100, 1) if len(df) > 0 else 0}%")
        with k2:
            resigned = len(df[df['الحالة'] == 'استقالة']) if 'الحالة' in df.columns else 0
            st.metric("🚪 استقالات", f"{resigned} موظف")
        with k3:
            terminated = len(df[df['الحالة'] == 'إنهاء خدمات']) if 'الحالة' in df.columns else 0
            st.metric("📋 إنهاء خدمات", f"{terminated} موظف")
        with k4:
            st.metric("✅ موظفين نشطين", f"{len(active)} موظف")
        
        st.markdown("---")
        
        if df_turnover is not None:
            c1, c2 = st.columns(2)
            
            with c1:
                month_col = 'الشهر' if 'الشهر' in df_turnover.columns else df_turnover.columns[0]
                hire_col = 'تعيينات جديدة' if 'تعيينات جديدة' in df_turnover.columns else None
                exit_col = 'استقالات' if 'استقالات' in df_turnover.columns else None
                
                if hire_col and exit_col:
                    fig = go.Figure()
                    fig.add_trace(go.Bar(x=df_turnover[month_col], y=df_turnover[hire_col],
                                       name='تعيينات', marker_color=colors['success']))
                    fig.add_trace(go.Bar(x=df_turnover[month_col], y=df_turnover[exit_col],
                                       name='استقالات', marker_color=colors['accent']))
                    fig.update_layout(title='التعيينات مقابل الاستقالات (شهرياً)',
                                    barmode='group', font=dict(family="Noto Sans Arabic"), height=400)
                    st.plotly_chart(fig, use_container_width=True)
            
            with c2:
                if 'معدل الدوران %' in df_turnover.columns:
                    fig = go.Figure()
                    fig.add_trace(go.Scatter(
                        x=df_turnover[month_col], y=df_turnover['معدل الدوران %'],
                        mode='lines+markers', name='معدل الدوران',
                        line=dict(color=colors['accent'], width=3),
                        marker=dict(size=8)
                    ))
                    fig.add_hline(y=df_turnover['معدل الدوران %'].mean(), line_dash="dash",
                                line_color="gray", annotation_text="المتوسط")
                    fig.update_layout(title='معدل الدوران الشهري %',
                                    font=dict(family="Noto Sans Arabic"), height=400)
                    st.plotly_chart(fig, use_container_width=True)
        
        # Exit Analysis
        if len(left) > 0:
            c1, c2 = st.columns(2)
            
            with c1:
                if 'سبب المغادرة' in left.columns:
                    reasons = left['سبب المغادرة'].value_counts().reset_index()
                    reasons.columns = ['السبب', 'العدد']
                    reasons = reasons[reasons['السبب'] != '']
                    if len(reasons) > 0:
                        fig = px.pie(reasons, values='العدد', names='السبب',
                                   title='أسباب المغادرة',
                                   color_discrete_sequence=px.colors.qualitative.Pastel)
                        fig.update_layout(font=dict(family="Noto Sans Arabic"), height=400)
                        st.plotly_chart(fig, use_container_width=True)
            
            with c2:
                if 'القسم' in left.columns:
                    dept_left = left['القسم'].value_counts().reset_index()
                    dept_left.columns = ['القسم', 'عدد المغادرين']
                    fig = px.bar(dept_left, x='القسم', y='عدد المغادرين',
                               title='المغادرون حسب القسم',
                               color='عدد المغادرين', color_continuous_scale='reds')
                    fig.update_layout(font=dict(family="Noto Sans Arabic"), height=400)
                    st.plotly_chart(fig, use_container_width=True)
            
            with st.expander("📋 تفاصيل الموظفين المغادرين"):
                display_cols = [c for c in ['رقم الموظف', 'الاسم', 'القسم', 'الحالة', 'سبب المغادرة', 
                                           'تاريخ التعيين', 'تاريخ الانتهاء', 'الراتب الأساسي'] if c in left.columns]
                st.dataframe(left[display_cols], use_container_width=True, hide_index=True)
        
        # Insights
        turnover_insights = [i for i in st.session_state.insights if i['category'] == 'الدوران']
        if turnover_insights:
            st.markdown("### 💡 رؤى الدوران")
            for ins in turnover_insights:
                insight_box(ins['text'], ins['type'])
    
    
    # ======================================================
    #                ⚡ الأداء والإنتاجية
    # ======================================================
    elif page == "⚡ الأداء والإنتاجية":
        st.markdown("""
        <div class='app-header'>
            <h1>⚡ تحليل الأداء والإنتاجية</h1>
            <p>تقييمات الموظفين وتصنيفاتهم وتوصيات التطوير</p>
        </div>
        """, unsafe_allow_html=True)
        
        perf_col = 'تقييم الأداء %'
        if perf_col in active_filtered.columns:
            # KPIs
            k1, k2, k3, k4 = st.columns(4)
            with k1:
                st.metric("⚡ متوسط الأداء", f"{active_filtered[perf_col].mean():.1f}%")
            with k2:
                excellent = len(active_filtered[active_filtered[perf_col] >= 90])
                st.metric("🌟 أداء ممتاز (90%+)", f"{excellent} موظف")
            with k3:
                good = len(active_filtered[(active_filtered[perf_col] >= 70) & (active_filtered[perf_col] < 90)])
                st.metric("✅ أداء جيد (70-89%)", f"{good} موظف")
            with k4:
                needs_dev = len(active_filtered[active_filtered[perf_col] < 70])
                st.metric("⚠️ يحتاج تطوير (<70%)", f"{needs_dev} موظف")
            
            st.markdown("---")
            
            c1, c2 = st.columns(2)
            
            with c1:
                # Performance Distribution
                fig = px.histogram(active_filtered, x=perf_col, nbins=10,
                                 title='توزيع تقييمات الأداء',
                                 color_discrete_sequence=[colors['primary']])
                fig.add_vline(x=active_filtered[perf_col].mean(), line_dash="dash",
                            line_color="red", annotation_text=f"المتوسط: {active_filtered[perf_col].mean():.1f}%")
                fig.update_layout(font=dict(family="Noto Sans Arabic"), height=400)
                st.plotly_chart(fig, use_container_width=True)
            
            with c2:
                # Radar Chart by Department
                if 'القسم' in active_filtered.columns:
                    dept_metrics = active_filtered.groupby('القسم').agg({
                        perf_col: 'mean',
                        'الرضا الوظيفي %': 'mean' if 'الرضا الوظيفي %' in active_filtered.columns else 'count',
                        'أيام الغياب': lambda x: 100 - x.mean() * 5 if 'أيام الغياب' in active_filtered.columns else 0,
                        'ساعات التدريب': 'mean' if 'ساعات التدريب' in active_filtered.columns else 'count',
                    }).reset_index()
                    
                    fig = go.Figure()
                    for _, row in dept_metrics.iterrows():
                        vals = [row[perf_col]]
                        cats = ['الأداء']
                        if 'الرضا الوظيفي %' in active_filtered.columns:
                            vals.append(row['الرضا الوظيفي %'])
                            cats.append('الرضا')
                        if 'أيام الغياب' in active_filtered.columns:
                            vals.append(row['أيام الغياب'])
                            cats.append('الانضباط')
                        if 'ساعات التدريب' in active_filtered.columns:
                            vals.append(min(row['ساعات التدريب'] * 2, 100))
                            cats.append('التدريب')
                        vals.append(vals[0])
                        cats.append(cats[0])
                        
                        fig.add_trace(go.Scatterpolar(r=vals, theta=cats, name=row['القسم']))
                    
                    fig.update_layout(polar=dict(radialaxis=dict(visible=True, range=[0, 100])),
                                    title='مقارنة الأقسام', font=dict(family="Noto Sans Arabic"), height=400)
                    st.plotly_chart(fig, use_container_width=True)
            
            # Performance vs Salary
            if 'الراتب الأساسي' in active_filtered.columns:
                st.markdown("### 📊 العلاقة بين الأداء والراتب")
                fig = px.scatter(active_filtered, x='الراتب الأساسي', y=perf_col,
                               color='القسم' if 'القسم' in active_filtered.columns else None,
                               size='العمر' if 'العمر' in active_filtered.columns else None,
                               hover_data=['الاسم'] if 'الاسم' in active_filtered.columns else None,
                               title='هل هناك علاقة بين الراتب والأداء؟',
                               trendline='ols')
                fig.update_layout(font=dict(family="Noto Sans Arabic"), height=450, xaxis_tickformat=',')
                st.plotly_chart(fig, use_container_width=True)
            
            # Top & Bottom Performers
            c1, c2 = st.columns(2)
            with c1:
                st.markdown("### 🏆 أفضل 10 موظفين أداءً")
                top10 = active_filtered.nlargest(10, perf_col)
                display_cols = [c for c in ['الاسم', 'القسم', perf_col, 'الراتب الأساسي'] if c in top10.columns]
                st.dataframe(top10[display_cols], use_container_width=True, hide_index=True)
            
            with c2:
                st.markdown("### ⚠️ الموظفين الأقل أداءً")
                bottom10 = active_filtered.nsmallest(10, perf_col)
                display_cols = [c for c in ['الاسم', 'القسم', perf_col, 'الراتب الأساسي'] if c in bottom10.columns]
                st.dataframe(bottom10[display_cols], use_container_width=True, hide_index=True)
        
        # Quarterly Performance
        if df_performance is not None:
            st.markdown("### 📈 التقييمات الربع سنوية")
            q_cols = [c for c in df_performance.columns if 'تقييم Q' in c or 'Q' in c]
            if q_cols and 'الاسم' in df_performance.columns:
                perf_melted = df_performance[['الاسم'] + q_cols].melt(id_vars='الاسم', var_name='الربع', value_name='التقييم')
                fig = px.line(perf_melted, x='الربع', y='التقييم', color='الاسم',
                            title='تطور أداء الموظفين عبر الأرباع', markers=True)
                fig.update_layout(font=dict(family="Noto Sans Arabic"), height=500, showlegend=False)
                st.plotly_chart(fig, use_container_width=True)
        
        # Insights
        perf_insights = [i for i in st.session_state.insights if i['category'] == 'الأداء']
        if perf_insights:
            st.markdown("### 💡 رؤى الأداء")
            for ins in perf_insights:
                insight_box(ins['text'], ins['type'])
    
    
    # ======================================================
    #               👥 التوظيف والاستقطاب
    # ======================================================
    elif page == "👥 التوظيف والاستقطاب":
        st.markdown("""
        <div class='app-header'>
            <h1>👥 تحليل التوظيف والاستقطاب</h1>
            <p>قمع التوظيف ومصادر الاستقطاب وكفاءة العملية</p>
        </div>
        """, unsafe_allow_html=True)
        
        if df_recruitment is not None:
            # KPIs
            k1, k2, k3, k4 = st.columns(4)
            with k1:
                total_apps = int(df_recruitment['عدد المتقدمين'].sum()) if 'عدد المتقدمين' in df_recruitment.columns else 0
                st.metric("📥 طلبات مستلمة", total_apps)
            with k2:
                total_hired = int(df_recruitment['تعيينات'].sum()) if 'تعيينات' in df_recruitment.columns else 0
                st.metric("✅ تعيينات فعلية", total_hired)
            with k3:
                conv_rate = round(total_hired / total_apps * 100, 1) if total_apps > 0 else 0
                st.metric("📊 معدل التحويل", f"{conv_rate}%")
            with k4:
                avg_days = round(df_recruitment['أيام التوظيف'].mean(), 1) if 'أيام التوظيف' in df_recruitment.columns else 0
                st.metric("⏱️ متوسط أيام التوظيف", f"{avg_days} يوم")
            
            st.markdown("---")
            
            c1, c2 = st.columns(2)
            
            with c1:
                # Recruitment Funnel
                funnel_cols = {'عدد المتقدمين': 'متقدمين', 'تم الفرز': 'فرز أولي', 
                             'مقابلات': 'مقابلات', 'عروض مقدمة': 'عروض', 'تعيينات': 'تعيينات'}
                funnel_data = []
                for col, label in funnel_cols.items():
                    if col in df_recruitment.columns:
                        funnel_data.append({'المرحلة': label, 'العدد': int(df_recruitment[col].sum())})
                
                if funnel_data:
                    fig = px.funnel(pd.DataFrame(funnel_data), x='العدد', y='المرحلة',
                                  title='قمع التوظيف', color_discrete_sequence=[colors['primary']])
                    fig.update_layout(font=dict(family="Noto Sans Arabic"), height=400)
                    st.plotly_chart(fig, use_container_width=True)
            
            with c2:
                # Sources
                if 'مصدر الاستقطاب' in df_recruitment.columns:
                    sources = df_recruitment.groupby('مصدر الاستقطاب').agg({
                        'عدد المتقدمين': 'sum',
                        'تعيينات': 'sum' if 'تعيينات' in df_recruitment.columns else 'count'
                    }).reset_index()
                    sources['معدل التحويل'] = round(sources['تعيينات'] / sources['عدد المتقدمين'] * 100, 1)
                    
                    fig = px.bar(sources, x='مصدر الاستقطاب', y='عدد المتقدمين',
                               title='مصادر الاستقطاب وعدد المتقدمين',
                               color='معدل التحويل', color_continuous_scale='oranges',
                               text='عدد المتقدمين')
                    fig.update_layout(font=dict(family="Noto Sans Arabic"), height=400)
                    st.plotly_chart(fig, use_container_width=True)
            
            # Recruitment by Department
            if 'القسم' in df_recruitment.columns:
                st.markdown("### 📊 طلبات التوظيف حسب القسم")
                c1, c2 = st.columns(2)
                
                with c1:
                    dept_req = df_recruitment['القسم'].value_counts().reset_index()
                    dept_req.columns = ['القسم', 'عدد الطلبات']
                    fig = px.pie(dept_req, values='عدد الطلبات', names='القسم',
                               title='توزيع طلبات التوظيف', hole=0.3,
                               color_discrete_sequence=colors['dept_colors'])
                    fig.update_layout(font=dict(family="Noto Sans Arabic"), height=400)
                    st.plotly_chart(fig, use_container_width=True)
                
                with c2:
                    if 'أيام التوظيف' in df_recruitment.columns:
                        dept_days = df_recruitment.groupby('القسم')['أيام التوظيف'].mean().reset_index()
                        dept_days.columns = ['القسم', 'متوسط الأيام']
                        dept_days = dept_days.sort_values('متوسط الأيام', ascending=True)
                        fig = px.bar(dept_days, x='متوسط الأيام', y='القسم', orientation='h',
                                   title='متوسط أيام التوظيف حسب القسم',
                                   color='متوسط الأيام', color_continuous_scale='RdYlGn_r')
                        fig.update_layout(font=dict(family="Noto Sans Arabic"), height=400)
                        st.plotly_chart(fig, use_container_width=True)
            
            # Detailed Table
            with st.expander("📋 تفاصيل طلبات التوظيف"):
                st.dataframe(df_recruitment, use_container_width=True, hide_index=True)
        else:
            st.warning("لا توجد بيانات توظيف. تأكد من وجود ورقة 'التوظيف والاستقطاب' في ملف Excel.")
    
    
    # ======================================================
    #                  🤖 المحلل الذكي
    # ======================================================
    elif page == "🤖 المحلل الذكي":
        st.markdown("""
        <div class='app-header'>
            <h1>🤖 المحلل الذكي بالذكاء الاصطناعي</h1>
            <p>اسأل أي سؤال عن بيانات الموارد البشرية واحصل على تحليل فوري</p>
        </div>
        """, unsafe_allow_html=True)
        
        # Pre-built insights
        st.markdown("### 📊 تحليلات تلقائية")
        for ins in st.session_state.insights:
            insight_box(f"**[{ins['category']}]** {ins['text']}", ins['type'])
        
        st.markdown("---")
        
        # AI Chat
        st.markdown("### 💬 اسأل المحلل الذكي")
        
        # Quick questions
        quick_qs = [
            "ما هو القسم الأعلى تكلفة؟",
            "من هم أفضل 5 موظفين أداءً؟",
            "كيف أخفض معدل الدوران؟",
            "حلل فجوة الرواتب بين الأقسام",
            "ما نسبة السعودة وهل تحتاج تحسين؟",
            "ما توصياتك لتحسين الأداء العام؟",
        ]
        
        selected_q = st.selectbox("أسئلة سريعة:", ["اختر سؤال..."] + quick_qs)
        
        user_q = st.text_input("أو اكتب سؤالك:", placeholder="مثال: ما القسم الأكثر غياباً؟")
        
        query = user_q if user_q else (selected_q if selected_q != "اختر سؤال..." else "")
        
        if st.button("🔍 تحليل", type="primary", use_container_width=True) and query:
            with st.spinner("جاري التحليل..."):
                # Build context
                data_summary = f"""
بيانات الموظفين:
- إجمالي الموظفين: {len(df)}
- الموظفين النشطين: {len(active)}
- المغادرين: {len(left)}
- متوسط الراتب: {active['الراتب الأساسي'].mean():,.0f} ريال
- متوسط الأداء: {active[perf_col].mean():.1f}% (إن وُجد)
- الأقسام: {', '.join(active['القسم'].unique()) if 'القسم' in active.columns else 'غير محدد'}

تفاصيل الأقسام:
"""
                if 'القسم' in active.columns:
                    for dept in active['القسم'].unique():
                        d = active[active['القسم'] == dept]
                        data_summary += f"- {dept}: {len(d)} موظف، متوسط راتب {d['الراتب الأساسي'].mean():,.0f} ريال"
                        if perf_col in d.columns:
                            data_summary += f"، متوسط أداء {d[perf_col].mean():.1f}%"
                        data_summary += "\n"
                
                if len(left) > 0 and 'سبب المغادرة' in left.columns:
                    reasons = left['سبب المغادرة'].value_counts()
                    data_summary += f"\nأسباب المغادرة: {dict(reasons)}\n"
                
                # Try Claude API
                try:
                    import anthropic
                    client = anthropic.Anthropic()
                    response = client.messages.create(
                        model="claude-sonnet-4-20250514",
                        max_tokens=1000,
                        system=f"""أنت محلل موارد بشرية خبير يعمل في شركة رسال الود لتقنية المعلومات في السعودية.
تحلل البيانات التالية وتقدم رؤى عملية باللغة العربية. كن مختصراً ومفيداً.
{data_summary}""",
                        messages=[{"role": "user", "content": query}]
                    )
                    answer = response.content[0].text
                except:
                    # Fallback: local analysis
                    answer = "⚠️ لم يتم ربط Claude API بعد. إليك تحليل محلي:\n\n"
                    
                    if "تكلفة" in query or "أعلى" in query:
                        if 'القسم' in active.columns:
                            dept_cost = active.groupby('القسم')['الراتب الأساسي'].sum().sort_values(ascending=False)
                            answer += f"القسم الأعلى تكلفة: {dept_cost.index[0]} بإجمالي {dept_cost.iloc[0]:,.0f} ريال\n"
                            for dept, cost in dept_cost.items():
                                answer += f"  - {dept}: {cost:,.0f} ريال\n"
                    
                    elif "أفضل" in query or "أداء" in query:
                        if perf_col in active.columns:
                            top = active.nlargest(5, perf_col)[['الاسم', 'القسم', perf_col]]
                            answer += "أفضل 5 موظفين أداءً:\n"
                            for _, r in top.iterrows():
                                answer += f"  - {r['الاسم']} ({r['القسم']}): {r[perf_col]}%\n"
                    
                    elif "دوران" in query or "استقالة" in query:
                        answer += f"معدل الدوران: {len(left)/len(df)*100:.1f}%\n"
                        if 'سبب المغادرة' in left.columns:
                            answer += "أسباب المغادرة:\n"
                            for reason, count in left['سبب المغادرة'].value_counts().items():
                                if reason:
                                    answer += f"  - {reason}: {count} موظف\n"
                        answer += "\nتوصيات: تحسين بيئة العمل، مراجعة التعويضات، برنامج احتفاظ بالمواهب"
                    
                    elif "سعودة" in query:
                        if 'الجنسية' in active.columns:
                            saudi = len(active[active['الجنسية'].isin(['سعودي', 'سعودية'])])
                            answer += f"نسبة السعودة: {saudi/len(active)*100:.1f}% ({saudi} من {len(active)})"
                    
                    else:
                        answer += f"إجمالي الموظفين: {len(active)}\n"
                        answer += f"متوسط الراتب: {active['الراتب الأساسي'].mean():,.0f} ريال\n"
                        if perf_col in active.columns:
                            answer += f"متوسط الأداء: {active[perf_col].mean():.1f}%\n"
                        answer += f"\nلتفعيل التحليل المتقدم، ثبت مكتبة anthropic:\npip install anthropic"
                
                st.markdown("### 📝 نتيجة التحليل")
                st.info(answer)
    
    
    # ======================================================
    #                  📋 بيانات الموظفين
    # ======================================================
    elif page == "📋 بيانات الموظفين":
        st.markdown("""
        <div class='app-header'>
            <h1>📋 بيانات الموظفين</h1>
            <p>عرض وتصفية وبحث في بيانات جميع الموظفين</p>
        </div>
        """, unsafe_allow_html=True)
        
        st.markdown(f"**إجمالي السجلات:** {len(active_filtered)} موظف نشط من أصل {len(df)}")
        
        # Search
        search = st.text_input("🔍 بحث بالاسم أو رقم الموظف:", placeholder="اكتب للبحث...")
        
        display_df = active_filtered.copy()
        if search:
            mask = display_df.apply(lambda row: row.astype(str).str.contains(search, case=False).any(), axis=1)
            display_df = display_df[mask]
            st.markdown(f"**نتائج البحث:** {len(display_df)} موظف")
        
        # Display
        st.dataframe(display_df, use_container_width=True, hide_index=True, height=600)
        
        # Statistics
        with st.expander("📊 إحصائيات سريعة"):
            if 'الراتب الأساسي' in display_df.columns:
                st.write(display_df[['الراتب الأساسي', 'تقييم الأداء %', 'العمر', 'أيام الغياب', 'ساعات التدريب']].describe().round(1))
    
    
    # ======================================================
    #                  📥 تصدير التقارير
    # ======================================================
    elif page == "📥 تصدير التقارير":
        st.markdown("""
        <div class='app-header'>
            <h1>📥 تصدير التقارير</h1>
            <p>حمّل تقارير مخصصة بصيغة Excel</p>
        </div>
        """, unsafe_allow_html=True)
        
        st.markdown("### اختر التقارير المطلوبة:")
        
        reports = {}
        
        if st.checkbox("📊 تقرير الموظفين النشطين", value=True):
            reports['الموظفين النشطين'] = active_filtered
        
        if st.checkbox("💰 تقرير الرواتب والتكاليف") and df_salary is not None:
            reports['الرواتب والتكاليف'] = df_salary
        
        if st.checkbox("🔄 تقرير الدوران الوظيفي") and df_turnover is not None:
            reports['الدوران الوظيفي'] = df_turnover
        
        if st.checkbox("⚡ تقرير الأداء") and df_performance is not None:
            reports['تحليل الأداء'] = df_performance
        
        if st.checkbox("👥 تقرير التوظيف") and df_recruitment is not None:
            reports['التوظيف'] = df_recruitment
        
        if len(left) > 0 and st.checkbox("🚪 تقرير المغادرين"):
            reports['المغادرين'] = left
        
        # Summary stats
        if st.checkbox("📈 ملخص إحصائي"):
            summary_data = {
                'المؤشر': ['إجمالي الموظفين', 'الموظفين النشطين', 'المغادرين', 'معدل الدوران',
                          'متوسط الراتب', 'إجمالي الرواتب', 'متوسط الأداء', 'نسبة السعودة'],
                'القيمة': [
                    len(df), len(active), len(left), f"{len(left)/len(df)*100:.1f}%",
                    f"{active['الراتب الأساسي'].mean():,.0f}", f"{active['الراتب الأساسي'].sum():,.0f}",
                    f"{active[perf_col].mean():.1f}%" if perf_col in active.columns else 'N/A',
                    f"{len(active[active['الجنسية'].isin(['سعودي', 'سعودية'])])/len(active)*100:.1f}%" if 'الجنسية' in active.columns else 'N/A'
                ]
            }
            reports['ملخص إحصائي'] = pd.DataFrame(summary_data)
        
        if reports:
            excel_data = export_to_excel(reports)
            st.download_button(
                label="📥 تحميل التقارير (Excel)",
                data=excel_data,
                file_name=f"HR_Report_{datetime.now().strftime('%Y%m%d')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                type="primary",
                use_container_width=True
            )
            st.success(f"✅ جاهز للتحميل: {len(reports)} تقرير")
        else:
            st.warning("اختر تقرير واحد على الأقل")


if __name__ == "__main__":
    main()
