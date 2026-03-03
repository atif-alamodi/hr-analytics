# ===================================================
# تطبيق تحليلات الموارد البشرية - رسال الود لتقنية المعلومات
# HR Analytics AI Platform v2.0
# ===================================================

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import io
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

def has(df, name):
    return name in df.columns

def safe_mean(df, name):
    return df[name].mean() if has(df, name) and len(df) > 0 else 0

def safe_sum(df, name):
    return df[name].sum() if has(df, name) else 0

def insight_box(text, t="info"):
    cls = f"insight-box insight-{t}" if t != "info" else "insight-box"
    icons = {"info": "💡", "warning": "⚠️", "success": "✅", "danger": "🚨"}
    st.markdown(f'<div class="{cls}">{icons.get(t, "💡")} {text}</div>', unsafe_allow_html=True)

def fmt(v): return f"{v:,.0f} ريال"

CL = {'primary': '#0F4C5C', 'accent': '#9A031E', 'success': '#2D6A4F', 'dept': px.colors.qualitative.Set2}

def gen_insights(df, active, df_salary):
    ins = []
    try:
        if has(active,'القسم') and has(active,'الراتب الأساسي') and len(active)>0:
            da = active.groupby('القسم')['الراتب الأساسي'].mean()
            if len(da)>0:
                ins.append({'t':'info','c':'الرواتب','x':f'فجوة الرواتب: {da.max()-da.min():,.0f} ريال. أعلى: {da.idxmax()} ({da.max():,.0f}), أقل: {da.idxmin()} ({da.min():,.0f})'})
        if has(df,'الحالة'):
            lf = df[df['الحالة']!='نشط']
            if len(lf)>0:
                rate = round(len(lf)/len(df)*100,1)
                tp = 'danger' if rate>20 else 'warning' if rate>10 else 'success'
                di = ""
                if has(lf,'القسم') and len(lf['القسم'].value_counts())>0:
                    td = lf['القسم'].value_counts()
                    di = f" أعلى قسم: {td.index[0]} ({td.iloc[0]})"
                ins.append({'t':tp,'c':'الدوران','x':f'معدل الدوران: {rate}%.{di}'})
                if has(lf,'سبب المغادرة'):
                    rs = lf['سبب المغادرة'].value_counts()
                    rs = rs[rs.index!='']
                    if len(rs)>0:
                        ins.append({'t':'warning','c':'الدوران','x':f'أكثر سبب: "{rs.index[0]}" ({rs.iloc[0]} موظف)'})
        if has(active,'تقييم الأداء %') and len(active)>0:
            ap = active['تقييم الأداء %'].mean()
            ins.append({'t':'success' if ap>=80 else 'warning','c':'الأداء','x':f'متوسط الأداء: {ap:.1f}%. ممتاز: {len(active[active["تقييم الأداء %"]>=90])}. تطوير: {len(active[active["تقييم الأداء %"]<70])}'})
        if has(active,'الجنسية') and len(active)>0:
            sa = active[active['الجنسية'].isin(['سعودي','سعودية'])]
            p = round(len(sa)/len(active)*100,1)
            ins.append({'t':'success' if p>=70 else 'warning','c':'السعودة','x':f'نسبة السعودة: {p}% ({len(sa)} من {len(active)})'})
        if has(active,'أيام الغياب') and len(active)>0:
            ha = active[active['أيام الغياب']>10]
            if len(ha)>0:
                ins.append({'t':'warning','c':'الغياب','x':f'{len(ha)} موظف أكثر من 10 أيام غياب. المتوسط: {active["أيام الغياب"].mean():.1f}'})
        if df_salary is not None and has(df_salary,'إجمالي التكلفة الشهرية'):
            ins.append({'t':'info','c':'التكلفة','x':f'التكلفة الشهرية: {df_salary["إجمالي التكلفة الشهرية"].sum():,.0f} ريال'})
    except: pass
    if not ins: ins.append({'t':'info','c':'عام','x':f'تم تحميل {len(df)} سجل'})
    return ins

def main():
    with st.sidebar:
        st.markdown("<div style='text-align:center;padding:20px 0;'><div style='background:linear-gradient(135deg,#E36414,#E9C46A);width:60px;height:60px;border-radius:14px;display:flex;align-items:center;justify-content:center;margin:0 auto 12px;font-size:24px;font-weight:800;color:white;'>HR</div><h2 style='margin:0;font-size:18px;'>تحليلات الموارد البشرية</h2><p style='opacity:0.6;font-size:12px;'>رسال الود لتقنية المعلومات</p></div>", unsafe_allow_html=True)
        st.markdown("---")
        page = st.radio("📌", ["🏠 نظرة عامة","💰 الرواتب والتكاليف","🔄 الدوران الوظيفي","⚡ الأداء والإنتاجية","👥 التوظيف والاستقطاب","🤖 المحلل الذكي","📋 بيانات الموظفين","📥 تصدير التقارير"], label_visibility="collapsed")
        st.markdown("---")
        st.markdown("##### 📁 مصدر البيانات")
        file = st.file_uploader("ارفع Excel", type=["xlsx","xls","csv"], label_visibility="collapsed")
        if file: st.success("✅ تم التحميل")

    if not file:
        st.markdown("<div class='app-header'><h1>📊 منصة تحليلات الموارد البشرية</h1><p>رسال الود لتقنية المعلومات</p></div>", unsafe_allow_html=True)
        st.info("📁 ارفع ملف بيانات الموظفين من القائمة الجانبية للبدء")
        return

    try:
        if file.name.endswith('.csv'):
            df=pd.read_csv(file); sheets={'بيانات الموظفين':df}
        else:
            sheets=pd.read_excel(file,sheet_name=None); df=sheets.get('بيانات الموظفين',list(sheets.values())[0])
    except Exception as e:
        st.error(f"خطأ: {e}"); return

    ds=sheets.get('الرواتب والتكاليف'); dt=sheets.get('الدوران الوظيفي'); dp=sheets.get('تحليل الأداء'); dr=sheets.get('التوظيف والاستقطاب')
    active = df[df['الحالة']=='نشط'] if has(df,'الحالة') else df
    left = df[df['الحالة']!='نشط'] if has(df,'الحالة') else pd.DataFrame()

    with st.sidebar:
        st.markdown("##### 🔍 الفلاتر")
        af=active.copy()
        if has(active,'القسم'):
            df2=st.multiselect("القسم",list(active['القسم'].unique()),default=list(active['القسم'].unique()))
            af=af[af['القسم'].isin(df2)]
        if has(active,'الدرجة'):
            gf=st.multiselect("الدرجة",list(active['الدرجة'].unique()),default=list(active['الدرجة'].unique()))
            af=af[af['الدرجة'].isin(gf)]

    if 'ins' not in st.session_state:
        st.session_state.ins = gen_insights(df,active,ds)

    # === 🏠 نظرة عامة ===
    if page=="🏠 نظرة عامة":
        st.markdown("<div class='app-header'><h1>📊 نظرة عامة</h1><p>ملخص شامل</p></div>",unsafe_allow_html=True)
        k1,k2,k3,k4,k5=st.columns(5)
        with k1: st.metric("👥 الموظفين",len(af))
        with k2: st.metric("💰 متوسط الراتب",fmt(int(safe_mean(af,'الراتب الأساسي'))))
        with k3: st.metric("⚡ الأداء",f"{safe_mean(af,'تقييم الأداء %'):.1f}%")
        with k4: st.metric("🔄 الدوران",f"{round(len(left)/max(len(df),1)*100,1)}%")
        with k5:
            if ds is not None and has(ds,'إجمالي التكلفة الشهرية'): st.metric("🏦 التكلفة",fmt(int(ds['إجمالي التكلفة الشهرية'].sum())))
            else: st.metric("🏦 التكلفة",fmt(int(safe_sum(af,'الراتب الأساسي')*1.3)))
        st.markdown("---")
        c1,c2=st.columns(2)
        with c1:
            if has(af,'القسم'):
                d=af['القسم'].value_counts().reset_index(); d.columns=['القسم','العدد']
                fig=px.pie(d,values='العدد',names='القسم',title='توزيع الموظفين',hole=0.4,color_discrete_sequence=CL['dept'])
                fig.update_layout(font=dict(family="Noto Sans Arabic"),height=400); st.plotly_chart(fig,use_container_width=True)
        with c2:
            if dt is not None and has(dt,'عدد الموظفين آخر الشهر'):
                mc='الشهر' if has(dt,'الشهر') else dt.columns[0]
                fig=go.Figure(); fig.add_trace(go.Scatter(x=dt[mc],y=dt['عدد الموظفين آخر الشهر'],mode='lines+markers',line=dict(color=CL['primary'],width=3),fill='tozeroy',fillcolor='rgba(15,76,92,0.1)'))
                fig.update_layout(title='تطور عدد الموظفين',font=dict(family="Noto Sans Arabic"),height=400); st.plotly_chart(fig,use_container_width=True)
        c1,c2=st.columns(2)
        with c1:
            if has(af,'القسم') and has(af,'الراتب الأساسي'):
                d=af.groupby('القسم')['الراتب الأساسي'].mean().reset_index(); d.columns=['القسم','المتوسط']; d=d.sort_values('المتوسط',ascending=True)
                fig=px.bar(d,x='المتوسط',y='القسم',orientation='h',title='متوسط الراتب حسب القسم',color='المتوسط',color_continuous_scale='teal')
                fig.update_layout(font=dict(family="Noto Sans Arabic"),height=400,xaxis_tickformat=','); st.plotly_chart(fig,use_container_width=True)
        with c2:
            if has(af,'القسم') and has(af,'تقييم الأداء %'):
                d=af.groupby('القسم')['تقييم الأداء %'].mean().reset_index(); d.columns=['القسم','المتوسط']
                fig=px.bar(d,x='القسم',y='المتوسط',title='الأداء حسب القسم',color='المتوسط',color_continuous_scale='RdYlGn',range_color=[60,100])
                fig.update_layout(font=dict(family="Noto Sans Arabic"),height=400); st.plotly_chart(fig,use_container_width=True)
        st.markdown("### 🤖 رؤى ذكية")
        for i in st.session_state.ins: insight_box(i['x'],i['t'])

    # === 💰 الرواتب ===
    elif page=="💰 الرواتب والتكاليف":
        st.markdown("<div class='app-header'><h1>💰 الرواتب والتكاليف</h1><p>التكلفة بالريال</p></div>",unsafe_allow_html=True)
        if has(af,'الراتب الأساسي') and len(af)>0:
            k1,k2,k3,k4=st.columns(4)
            with k1: st.metric("💵 الإجمالي",fmt(int(af['الراتب الأساسي'].sum())))
            with k2: st.metric("📊 المتوسط",fmt(int(af['الراتب الأساسي'].mean())))
            with k3: st.metric("📈 الأعلى",fmt(int(af['الراتب الأساسي'].max())))
            with k4: st.metric("📉 الأقل",fmt(int(af['الراتب الأساسي'].min())))
            st.markdown("---")
            c1,c2=st.columns(2)
            with c1:
                if has(af,'القسم'):
                    d=af.groupby('القسم')['الراتب الأساسي'].mean().reset_index().sort_values('الراتب الأساسي',ascending=True)
                    fig=px.bar(d,x='الراتب الأساسي',y='القسم',orientation='h',title='متوسط الراتب',color='الراتب الأساسي',color_continuous_scale='teal')
                    fig.update_layout(font=dict(family="Noto Sans Arabic"),height=400,xaxis_tickformat=','); st.plotly_chart(fig,use_container_width=True)
            with c2:
                fig=px.histogram(af,x='الراتب الأساسي',nbins=15,title='توزيع الرواتب',color_discrete_sequence=[CL['primary']])
                fig.update_layout(font=dict(family="Noto Sans Arabic"),height=400,xaxis_tickformat=','); st.plotly_chart(fig,use_container_width=True)
            if ds is not None and has(ds,'الاسم'):
                cc=[c for c in ['الراتب الأساسي','بدل السكن (25%)','بدل النقل','التأمينات الاجتماعية (11.75%)','التأمين الطبي','بدلات أخرى'] if has(ds,c)]
                if cc:
                    st.markdown("### 📊 تفصيل التكلفة")
                    dm=ds[['الاسم']+cc].melt(id_vars='الاسم',var_name='البند',value_name='المبلغ')
                    fig=px.bar(dm,x='الاسم',y='المبلغ',color='البند',barmode='stack',color_discrete_sequence=px.colors.qualitative.Set2)
                    fig.update_layout(font=dict(family="Noto Sans Arabic"),height=500,xaxis_tickangle=-45,yaxis_tickformat=','); st.plotly_chart(fig,use_container_width=True)
                with st.expander("📋 الجدول"): st.dataframe(ds,use_container_width=True,hide_index=True)

    # === 🔄 الدوران ===
    elif page=="🔄 الدوران الوظيفي":
        st.markdown("<div class='app-header'><h1>🔄 الدوران الوظيفي</h1><p>الاستقالات والتعيينات</p></div>",unsafe_allow_html=True)
        k1,k2,k3,k4=st.columns(4)
        with k1: st.metric("🔄 الدوران",f"{round(len(left)/max(len(df),1)*100,1)}%")
        with k2: st.metric("🚪 استقالات",len(df[df['الحالة']=='استقالة']) if has(df,'الحالة') else 0)
        with k3: st.metric("📋 إنهاء",len(df[df['الحالة']=='إنهاء خدمات']) if has(df,'الحالة') else 0)
        with k4: st.metric("✅ نشطين",len(active))
        st.markdown("---")
        if dt is not None:
            mc='الشهر' if has(dt,'الشهر') else dt.columns[0]
            c1,c2=st.columns(2)
            with c1:
                if has(dt,'تعيينات جديدة') and has(dt,'استقالات'):
                    fig=go.Figure()
                    fig.add_trace(go.Bar(x=dt[mc],y=dt['تعيينات جديدة'],name='تعيينات',marker_color=CL['success']))
                    fig.add_trace(go.Bar(x=dt[mc],y=dt['استقالات'],name='استقالات',marker_color=CL['accent']))
                    fig.update_layout(barmode='group',title='التعيينات مقابل الاستقالات',font=dict(family="Noto Sans Arabic"),height=400); st.plotly_chart(fig,use_container_width=True)
            with c2:
                if has(dt,'معدل الدوران %'):
                    fig=go.Figure(); fig.add_trace(go.Scatter(x=dt[mc],y=dt['معدل الدوران %'],mode='lines+markers',line=dict(color=CL['accent'],width=3)))
                    fig.update_layout(title='معدل الدوران %',font=dict(family="Noto Sans Arabic"),height=400); st.plotly_chart(fig,use_container_width=True)
        if len(left)>0:
            c1,c2=st.columns(2)
            with c1:
                if has(left,'سبب المغادرة'):
                    r=left['سبب المغادرة'].value_counts().reset_index(); r.columns=['السبب','العدد']; r=r[r['السبب']!='']
                    if len(r)>0:
                        fig=px.pie(r,values='العدد',names='السبب',title='أسباب المغادرة')
                        fig.update_layout(font=dict(family="Noto Sans Arabic"),height=400); st.plotly_chart(fig,use_container_width=True)
            with c2:
                if has(left,'القسم'):
                    d=left['القسم'].value_counts().reset_index(); d.columns=['القسم','العدد']
                    fig=px.bar(d,x='القسم',y='العدد',title='المغادرون حسب القسم',color='العدد',color_continuous_scale='reds')
                    fig.update_layout(font=dict(family="Noto Sans Arabic"),height=400); st.plotly_chart(fig,use_container_width=True)

    # === ⚡ الأداء ===
    elif page=="⚡ الأداء والإنتاجية":
        st.markdown("<div class='app-header'><h1>⚡ الأداء</h1><p>التقييمات والتصنيفات</p></div>",unsafe_allow_html=True)
        if has(af,'تقييم الأداء %') and len(af)>0:
            ap=af['تقييم الأداء %'].mean()
            k1,k2,k3,k4=st.columns(4)
            with k1: st.metric("⚡ المتوسط",f"{ap:.1f}%")
            with k2: st.metric("🌟 ممتاز",len(af[af['تقييم الأداء %']>=90]))
            with k3: st.metric("✅ جيد",len(af[(af['تقييم الأداء %']>=70)&(af['تقييم الأداء %']<90)]))
            with k4: st.metric("⚠️ تطوير",len(af[af['تقييم الأداء %']<70]))
            st.markdown("---")
            c1,c2=st.columns(2)
            with c1:
                fig=px.histogram(af,x='تقييم الأداء %',nbins=10,title='توزيع الأداء',color_discrete_sequence=[CL['primary']])
                fig.add_vline(x=ap,line_dash="dash",line_color="red",annotation_text=f"المتوسط: {ap:.1f}%")
                fig.update_layout(font=dict(family="Noto Sans Arabic"),height=400); st.plotly_chart(fig,use_container_width=True)
            with c2:
                if has(af,'القسم'):
                    d=af.groupby('القسم')['تقييم الأداء %'].mean().reset_index()
                    fig=px.bar(d,x='القسم',y='تقييم الأداء %',title='الأداء حسب القسم',color='تقييم الأداء %',color_continuous_scale='RdYlGn')
                    fig.update_layout(font=dict(family="Noto Sans Arabic"),height=400); st.plotly_chart(fig,use_container_width=True)
            if has(af,'الراتب الأساسي'):
                fig=px.scatter(af,x='الراتب الأساسي',y='تقييم الأداء %',color='القسم' if has(af,'القسم') else None,hover_data=['الاسم'] if has(af,'الاسم') else None,title='الراتب مقابل الأداء',trendline='ols')
                fig.update_layout(font=dict(family="Noto Sans Arabic"),height=450,xaxis_tickformat=','); st.plotly_chart(fig,use_container_width=True)
            sc=[c for c in ['الاسم','القسم','تقييم الأداء %','الراتب الأساسي'] if has(af,c)]
            c1,c2=st.columns(2)
            with c1: st.markdown("### 🏆 أفضل 10"); st.dataframe(af.nlargest(10,'تقييم الأداء %')[sc],use_container_width=True,hide_index=True)
            with c2: st.markdown("### ⚠️ الأقل"); st.dataframe(af.nsmallest(10,'تقييم الأداء %')[sc],use_container_width=True,hide_index=True)

    # === 👥 التوظيف ===
    elif page=="👥 التوظيف والاستقطاب":
        st.markdown("<div class='app-header'><h1>👥 التوظيف</h1><p>قمع الاستقطاب</p></div>",unsafe_allow_html=True)
        if dr is not None:
            ta=int(safe_sum(dr,'عدد المتقدمين')); th=int(safe_sum(dr,'تعيينات'))
            k1,k2,k3,k4=st.columns(4)
            with k1: st.metric("📥 طلبات",ta)
            with k2: st.metric("✅ تعيينات",th)
            with k3: st.metric("📊 التحويل",f"{round(th/max(ta,1)*100,1)}%")
            with k4: st.metric("⏱️ الأيام",f"{safe_mean(dr,'أيام التوظيف'):.0f}")
            st.markdown("---")
            c1,c2=st.columns(2)
            with c1:
                fm={'عدد المتقدمين':'متقدمين','تم الفرز':'فرز','مقابلات':'مقابلات','عروض مقدمة':'عروض','تعيينات':'تعيينات'}
                fd=[{'المرحلة':v,'العدد':int(safe_sum(dr,k))} for k,v in fm.items() if has(dr,k)]
                if fd:
                    fig=px.funnel(pd.DataFrame(fd),x='العدد',y='المرحلة',title='قمع التوظيف',color_discrete_sequence=[CL['primary']])
                    fig.update_layout(font=dict(family="Noto Sans Arabic"),height=400); st.plotly_chart(fig,use_container_width=True)
            with c2:
                if has(dr,'مصدر الاستقطاب'):
                    s=dr['مصدر الاستقطاب'].value_counts().reset_index(); s.columns=['المصدر','العدد']
                    fig=px.bar(s,x='المصدر',y='العدد',title='المصادر',color='العدد',color_continuous_scale='oranges')
                    fig.update_layout(font=dict(family="Noto Sans Arabic"),height=400); st.plotly_chart(fig,use_container_width=True)
            with st.expander("📋 التفاصيل"): st.dataframe(dr,use_container_width=True,hide_index=True)
        else: st.warning("لا توجد بيانات توظيف")

    # === 🤖 المحلل ===
    elif page=="🤖 المحلل الذكي":
        st.markdown("<div class='app-header'><h1>🤖 المحلل الذكي</h1><p>اسأل عن بياناتك</p></div>",unsafe_allow_html=True)
        st.markdown("### 📊 تحليلات تلقائية")
        for i in st.session_state.ins: insight_box(f"**[{i['c']}]** {i['x']}",i['t'])
        st.markdown("---")
        st.markdown("### 💬 اسأل")
        qq=["ما القسم الأعلى تكلفة؟","من أفضل 5 أداءً؟","كيف أخفض الدوران؟","ما نسبة السعودة؟"]
        sq=st.selectbox("أسئلة سريعة:",["اختر..."]+qq)
        uq=st.text_input("أو اكتب:",placeholder="مثال: ما القسم الأكثر غياباً؟")
        q=uq if uq else (sq if sq!="اختر..." else "")
        if st.button("🔍 تحليل",type="primary",use_container_width=True) and q:
            with st.spinner("جاري..."):
                try:
                    a=""
                    if "تكلفة" in q or "أعلى" in q:
                        if has(active,'القسم') and has(active,'الراتب الأساسي'):
                            dc=active.groupby('القسم')['الراتب الأساسي'].sum().sort_values(ascending=False)
                            a+=f"الأعلى: {dc.index[0]} ({dc.iloc[0]:,.0f} ريال)\n\n"
                            for d,v in dc.items(): a+=f"  - {d}: {v:,.0f} ريال\n"
                    elif "أفضل" in q or "أداء" in q:
                        if has(active,'تقييم الأداء %') and has(active,'الاسم'):
                            t=active.nlargest(5,'تقييم الأداء %'); a+="أفضل 5:\n\n"
                            for _,r in t.iterrows():
                                dp=f" ({r['القسم']})" if has(active,'القسم') else ""
                                a+=f"  - {r['الاسم']}{dp}: {r['تقييم الأداء %']}%\n"
                    elif "دوران" in q or "استقالة" in q or "أخفض" in q:
                        a+=f"الدوران: {round(len(left)/max(len(df),1)*100,1)}%\n\n"
                        if len(left)>0 and has(left,'سبب المغادرة'):
                            for r,c in left['سبب المغادرة'].value_counts().items():
                                if r: a+=f"  - {r}: {c}\n"
                        a+="\nتوصيات: تحسين بيئة العمل، مراجعة التعويضات"
                    elif "سعودة" in q:
                        if has(active,'الجنسية') and len(active)>0:
                            s=len(active[active['الجنسية'].isin(['سعودي','سعودية'])])
                            a+=f"السعودة: {round(s/len(active)*100,1)}% ({s} من {len(active)})"
                    elif "غياب" in q:
                        if has(active,'أيام الغياب') and has(active,'القسم'):
                            da=active.groupby('القسم')['أيام الغياب'].mean().sort_values(ascending=False)
                            a+="الغياب حسب القسم:\n\n"
                            for d,v in da.items(): a+=f"  - {d}: {v:.1f} يوم\n"
                    else:
                        a+=f"الموظفين: {len(active)}\n"
                        if has(active,'الراتب الأساسي'): a+=f"متوسط الراتب: {active['الراتب الأساسي'].mean():,.0f} ريال\n"
                        if has(active,'تقييم الأداء %'): a+=f"الأداء: {active['تقييم الأداء %'].mean():.1f}%\n"
                    if not a: a="جرّب سؤال آخر"
                    st.markdown("### 📝 النتيجة"); st.info(a)
                except: st.error("حدث خطأ")

    # === 📋 البيانات ===
    elif page=="📋 بيانات الموظفين":
        st.markdown("<div class='app-header'><h1>📋 البيانات</h1><p>عرض وتصفية</p></div>",unsafe_allow_html=True)
        st.markdown(f"**{len(af)}** نشط من **{len(df)}**")
        sr=st.text_input("🔍 بحث:",placeholder="اسم أو رقم...")
        dd=af.copy()
        if sr:
            m=dd.apply(lambda r: r.astype(str).str.contains(sr,case=False).any(),axis=1); dd=dd[m]
            st.markdown(f"**النتائج:** {len(dd)}")
        st.dataframe(dd,use_container_width=True,hide_index=True,height=600)
        with st.expander("📊 إحصائيات"):
            sc=[c for c in ['الراتب الأساسي','تقييم الأداء %','العمر','أيام الغياب','ساعات التدريب'] if has(dd,c)]
            if sc: st.write(dd[sc].describe().round(1))

    # === 📥 تصدير ===
    elif page=="📥 تصدير التقارير":
        st.markdown("<div class='app-header'><h1>📥 تصدير</h1><p>تقارير Excel</p></div>",unsafe_allow_html=True)
        rp={}
        if st.checkbox("📊 النشطين",value=True): rp['النشطين']=af
        if ds is not None and st.checkbox("💰 الرواتب"): rp['الرواتب']=ds
        if dt is not None and st.checkbox("🔄 الدوران"): rp['الدوران']=dt
        if dp is not None and st.checkbox("⚡ الأداء"): rp['الأداء']=dp
        if dr is not None and st.checkbox("👥 التوظيف"): rp['التوظيف']=dr
        if len(left)>0 and st.checkbox("🚪 المغادرين"): rp['المغادرين']=left
        if st.checkbox("📈 ملخص"):
            rows=[['الإجمالي',str(len(df))],['نشط',str(len(active))],['مغادر',str(len(left))],['الدوران',f"{round(len(left)/max(len(df),1)*100,1)}%"]]
            if has(active,'الراتب الأساسي') and len(active)>0: rows+=[['متوسط الراتب',f"{active['الراتب الأساسي'].mean():,.0f}"]]
            if has(active,'تقييم الأداء %') and len(active)>0: rows+=[['الأداء',f"{active['تقييم الأداء %'].mean():.1f}%"]]
            rp['ملخص']=pd.DataFrame(rows,columns=['المؤشر','القيمة'])
        if rp:
            o=io.BytesIO()
            with pd.ExcelWriter(o,engine='xlsxwriter') as w:
                for n,d in rp.items(): d.to_excel(w,sheet_name=n,index=False); w.sheets[n].right_to_left()
            st.download_button("📥 تحميل",data=o.getvalue(),file_name=f"HR_{datetime.now().strftime('%Y%m%d')}.xlsx",mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",type="primary",use_container_width=True)
            st.success(f"✅ {len(rp)} تقرير")
        else: st.warning("اختر تقرير")

if __name__=="__main__": main()
