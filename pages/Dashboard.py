import streamlit as st
from datetime import datetime, timedelta
import pandas as pd
from models import database as db
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

DICT_COL = {'cntnt_id':'컨텐츠번호','titl':'컨텐츠명','cd_nm':'상태','ctgr_path':'경로','upd_dttm':'수정일','att_cnt':'첨부',
            'emb_cnt':'색인회수','emb_c':'색인건수','ctgr_id':'카테고리번호','cntnt_cnt':'컨텐츠건수','reg_dt':'등록일',
            'ym':'년월','site':'싸이트','inq_cnt':'조회건수','reg_cnt':'등록건수','request_date':'검색요청일',
            'vtr_db_id':'벡터','vtr_db_nm':'DB명','req_date':"검색일자",'room_key_nunique':'회수','res_time_mean':'평균응답시간',
            'request_time':'요청일시','res_time':'응답','query_sentence':'질의문','llm_response':'LLM응답',
            'ans_relevancy':'답변관련도','ans_faithfulness':'답변충실도','cntxt_precision':'문서정확도','comments':'평가의견'}   

def get_chat_list(ctgr_cd, data_type, option="전체"):
    conn_name = st.secrets["connections_dbms"]["conn_name"]
    schema = st.secrets[conn_name]["schemaname"]
    
     ## 전체는 root인 'CA'로 조회, 나머지는 카테고리ROOT코드로 조회
    temp = f" AND CTGR_ID =   '{ctgr_cd}'" if ctgr_cd !='CA' else  " AND CTGR_ID = 'CA' "
    if data_type == 'chat':
        sql =  f""" select a.room_id,a.room_seq
                    ,a.vtr_db_id
                    ,b.response_time - a.request_time  as res_time
                    ,a.request_time, b.response_time
                    ,a.query_sentence
                    ,b.llm_response
                    ,c.ans_relevancy
                    ,c.ans_faithfulness 
                    ,c.cntxt_precision
                    ,c.comments
                FROM {schema}.tbctrg02 a
               INNER JOIN {schema}.tbctrg03 b
                  ON a.room_id  = b.room_id and a.room_seq = b.room_seq 
               LEFT OUTER JOIN {schema}.tbctrg05 c
                  ON c.eval_id = (SELECT max(eval_id)
                                   FROM tbctrg05
                                  WHERE room_id = a.room_id
                                    AND  room_seq = a.room_seq)
                order by request_time desc  
                """
    if  data_type == 'docs':
        sql = f'''select a.room_id ,a.room_seq, c.doc_seq
                    ,c.cntnt_key
                    ,c.title
                    ,c.section
                    ,c.paragraph
                from {schema}.tbctrg02 a, {schema}.tbctrg04 c
                where a.room_id  = c.room_id
                and a.room_seq = c.room_seq
                order by a.room_id ,a.room_seq, c.doc_seq '''
    if  data_type == 'hasgtag':
        sql = f'''select a.room_id, a.room_seq, b.tag_name 
                from {schema}.tbctrg06 a
                inner join {schema}.tbctrg07 b
                   ON a.tag_id = b.tag_id '''   
          
    # print(sql)
    if (df := db.get_kms_datadf(sql)) is None:
        st.warning('No data found', icon="⚠️")
        st.stop()

    if data_type == 'chat':
        st.session_state["chat_df"] = df
    if  data_type == 'docs':
        st.session_state["docs_df"] = df
    if  data_type == 'hashtag':
        st.session_state["tags_df"] = df
    return df

def init_session():
    if "contents_df" not in st.session_state:
        st.session_state.contents_df = None
    if "chat_df" not in st.session_state:
        st.session_state.chat_df = None
    if "docs_df" not in st.session_state:
        st.session_state.docs_df = None
    if "tags" not in st.session_state:
        st.session_state.tags_df = None
    if "min_dt" not in st.session_state:
        st.session_state.min_dt = None
    if "max_dt" not in st.session_state:
        st.session_state.max_dt = None
    if "option_ctgr" not in st.session_state:
        st.session_state.option_ctgr = None
    if "schemaname" not in st.session_state:
        st.session_state.schemaname = None

    if st.session_state.schemaname is None:  #db 스키마를 읽어옴
        conn_name = st.secrets["connections_dbms"]["conn_name"]
        st.session_state.schemaname = st.secrets[conn_name]["schemaname"]

    DICT_DAYS = {'최근3일':3,'1주일':7,'1개월':30,'3개월':90,'직접입력':0}
    with st.sidebar:
        opt = st.radio("기간",options=DICT_DAYS.keys(), horizontal =True)
        if opt == '직접입력':
            from_dt, to_dt = datetime.now(), datetime.now()
        else:
            to_dt = datetime.now()
            i = DICT_DAYS.get(opt,0)
            from_dt = to_dt -timedelta(days= i)
        cols = st.columns(2)
        with cols[0]:
                from_dt = st.date_input('시작일',value=from_dt)
        with cols[1]:
                to_dt = st.date_input('종료일',value=to_dt)
        # st.write(from_dt)
        # st.write(to_dt)
        if "from_dt" not in st.session_state:
            st.session_state.from_dt = None
        if "to_dt" not in st.session_state:
            st.session_state.to_dt = None
        st.session_state.from_dt = from_dt
        st.session_state.to_dt = to_dt
            

def app():
    tab1, tab2, tab3 = st.tabs([" 🗄️ 검색이력", " 🗂️ 컨텐츠 Dashboard", " 💹 검색 Dashboard"])

    with tab1:
        chat_list()
    with tab2:
        contents_dashboard()
    with tab3:
        chat_dashboard()

def chat_dashboard():
    
    df_contents = st.session_state["contents_df"]
    df_chat = st.session_state.chat_df
    df_docs = st.session_state.docs_df
    df_merge = pd.merge(df_chat,df_docs, left_on=['room_id','room_seq'], right_on=['room_id','room_seq'])

    from_dt, to_dt  = st.session_state.from_dt, st.session_state.to_dt
    from_dt = datetime(from_dt.year, from_dt.month, from_dt.day,0,0,0)
    to_dt = datetime(to_dt.year, to_dt.month, to_dt.day,23,59,59)
    df_merge =  df_merge[(df_merge['request_time'] >= from_dt) & (df_merge['request_time'] <= to_dt)]


    #실시간 대시보드
    today = datetime.now().date()
    yesterday = today - timedelta(days=1)
    weeksago1 = today - timedelta(weeks=1)
    weeksago2 = today - timedelta(weeks=2)
    df_chat['request_date'] = df_chat['request_time'].dt.date
    df_chat =  df_chat[(df_chat['request_time'] >= from_dt) & (df_chat['request_time'] <= to_dt)]
    df_chat['today'] = df_chat['request_date'] == today
    df_chat['yesterday'] = df_chat['request_date'] == yesterday
    df_chat['weeksago1'] = (df_chat['request_date'] > weeksago1) & (df_chat['request_date'] <= today)
    df_chat['weeksago2'] = (df_chat['request_date'] > weeksago2) & (df_chat['request_date'] <= weeksago1)

    today_cnt = int(df_chat['today'].sum())
    yesterday_cnt = int(df_chat['yesterday'].sum())
    weeksago1_cnt = int(df_chat['weeksago1'].sum())
    weeksago2_cnt = int(df_chat['weeksago2'].sum())

    col1, col2, col3,col4 = st.columns([1,1,2,3])
    col1.metric("Today", today_cnt, today_cnt - yesterday_cnt)
    col2.metric("This week", weeksago1_cnt, weeksago1_cnt - weeksago2_cnt)
    
    with col3:
        df_sum = df_chat[df_chat['request_date']==today]
        df_sum = df_sum.groupby(['request_date','vtr_db_id']).agg({'room_id':'count'}).fillna(0).reset_index().rename(columns={'room_id':'회수'})
        fig = px.pie(df_sum, values = '회수', names = "vtr_db_id",  template = "gridon",
                         hole = 0.0, height=250, title= "벡터별 사용량(당일)")
        fig.update_layout( title_x = 0.3,margin_l=50, legend_yanchor="top",)
        fig.update_traces(textposition='outside', textinfo='label+percent+value')
        st.plotly_chart(fig, use_container_width=True)
    with col4:
        df_sum = df_chat[df_chat['request_date'] > weeksago1]
        df_sum = df_sum.groupby(['request_date','vtr_db_id']).agg({'room_id':'count'}).fillna(0).reset_index().rename(columns={'room_id':'회수'})
        fig = px.pie(df_sum, values = '회수', names = "vtr_db_id",  template = "gridon",
                         hole = 0.0, height=250, title= "벡터별 사용량(최근7일)")
        fig.update_layout( title_x = 0.3,margin_l=50, )
        fig.update_traces(textposition='outside', textinfo='label+percent+value')
        st.plotly_chart(fig, use_container_width=True)
        # y_max = df_sum['회수'].max()
        # fig = px.bar(df_sum, y = "vtr_db_id", x = '회수',
        #             template = "seaborn", color="vtr_db_id",
        #             text_auto=True, color_discrete_sequence=px.colors.qualitative.G10')
        # fig.update_xaxes(title_text=None,tickangle=-45, tickfont_family='Rockwell', tickfont_color='green', tickfont_size=12)
        # fig.update_yaxes(title_text=None,range=[0,y_max])
        # fig.update_layout(legend_title_text=None,
        #                  legend_orientation="h",
        #                  legend_yanchor="top",legend_y=0.99,
        #                  legend_xanchor="left",legend_x=0.01)
        # st.plotly_chart(fig,use_container_width=True, height = 100)

    cols = st.columns([1.5,2])
    # 질문 top10
    with cols[0]:
        st.subheader("질문 top10")
        df_sum = df_chat['query_sentence'].value_counts(ascending=False).to_frame().reset_index()
        df_sum = df_sum.iloc[:10]
        df_sum.columns = ['질문','회수']
        st.dataframe(df_sum, hide_index=True)

        # fig = px.pie(df_sum, values='빈도', names='질문')
        # fig.update_layout( 
        #             legend_title_text= "",        
        #             legend_title_font_color= 'blue',
        #             legend_title_font_size=10,
        #             legend_font_size=9,
        #            )
        # st.plotly_chart(fig, theme=None)

   # 참고문서 top10
    with cols[1]: 
        st.subheader("참고문서 top10")
        df_sum = df_merge[['cntnt_key','title','section','paragraph']].value_counts(ascending=False).to_frame().reset_index()
        df_sum = df_sum.iloc[:10]
        df_sum.columns = ['컨텐츠ID','제목','탭','소제목','회수']
        st.dataframe(df_sum, hide_index=True)

    # 일별 사용량 및 응답시간
    st.subheader("일별 사용량")
    df_merge = pd.merge(df_chat,df_docs, left_on=['room_id','room_seq'], right_on=['room_id','room_seq'])
    df_merge['req_date'] = df_merge['request_time'].dt.strftime('%Y-%m-%d')
    df_merge['room_seq'] = df_merge['room_seq'].astype(str)
    df_merge['room_key'] = df_merge['room_id'] + df_merge['room_seq']
    grp_df = df_merge.groupby(['req_date','vtr_db_id']).agg({'room_key': ['nunique'],
                                      'res_time': ['mean']})
    # to flatten the multi-level columns
    grp_df.columns = ["_".join(col).strip() for col in grp_df.columns.values]
    grp_df['res_time_mean'] = grp_df['res_time_mean'].dt.seconds
    grp_df.reset_index(inplace=True)
    disp_df(grp_df.sort_values('req_date',ascending=False), hide_idx=True)

    # 컨텐츠별 참조빈도 
    st.subheader("컨텐츠별 참조회수")   
    df_merge['cntnt_id'] = df_merge['cntnt_key'].apply(lambda x: x.split('>')[0])
    df_sum = df_merge[['cntnt_id']].value_counts().reset_index()  
    df_sum = df_sum.merge(df_contents[['cntnt_id','ctgr_path','titl']], on='cntnt_id')

    df_sum.columns = ['컨텐츠ID','회수','경로','컨텐츠']
    df_sum = df_sum[['컨텐츠ID','컨텐츠','경로','회수']]
    disp_df(df_sum, hide_idx=True)

def chat_list():
    
    init_session()
            
    df_chat =  get_chat_list('CA','chat')
    df_docs =  get_chat_list('CA','docs')
    df_tags =  get_chat_list('CA','hasgtag')

    from_dt, to_dt  = st.session_state.from_dt, st.session_state.to_dt
    from_dt = datetime(from_dt.year, from_dt.month, from_dt.day,0,0,0)
    to_dt = datetime(to_dt.year, to_dt.month, to_dt.day,23,59,59)
    df_chat = df_chat[(df_chat['request_time'] >= from_dt) & (df_chat['request_time'] <= to_dt)]

    if df_chat.dtypes['res_time'].name != "int64":
        df_chat['res_time'] = [ int(i) for i in df_chat['res_time'].dt.total_seconds()]
    # df_chat.drop(columns=['response_time','request_time','room_seq','room_id'], inplace=True)
        df_chat['ans_relevancy'] = df_chat['ans_relevancy'].fillna(0).apply(lambda x: '☆'*int(x))
        df_chat['ans_faithfulness'] = df_chat['ans_faithfulness'].fillna(0).apply(lambda x: '☆'*int(x))
        df_chat['cntxt_precision'] = df_chat['cntxt_precision'].fillna(0).apply(lambda x: '☆'*int(x))

    cols = ['vtr_db_id','request_time','res_time','query_sentence','llm_response','ans_relevancy','ans_faithfulness','cntxt_precision','comments']
    event = st.dataframe(df_chat,
                         column_config= {x: DICT_COL.get(x.lower(),x) for x in df_chat.columns},
                         hide_index=True, on_select="rerun",selection_mode="single-row",column_order=cols)

    if len(event.selection['rows']) > 0:
        id_no = event.selection['rows']
        # print(id_no)
        selected_df = df_chat.iloc[id_no]
      
        for _, row in selected_df.iterrows():
            # st.markdown(f':question: {row["query_sentence"]}')
            st.markdown(f'<p style="background-color:#0066cc;color:#33ff33;font-size:14px;border-radius:2%;">Q.{row["query_sentence"]}</p>', unsafe_allow_html=True)
            st.markdown(row["llm_response"])

        join_key = ['room_id','room_seq']
         #키워드 태그 
        df_rels = pd.merge(selected_df[['room_id','room_seq']], df_tags, left_on=join_key, right_on=join_key)
        tag_list = []
        for _, rel in df_rels.iterrows():
            tag = rel['tag_name']
            if tag:
                tag_list.append(tag)
        if tag_list:
            st.markdown(f'검색키워드: {tag_list}')
            
        # 관련문서
        df_rels = pd.merge(selected_df[['room_id','room_seq']],df_docs, left_on=join_key, right_on=join_key)
        for _, rel in df_rels.iterrows():
            key = rel['cntnt_key']
            cntnt_id, section_id, para_id = key.split('>')
            title = rel['title']
            section = rel['section']
            para = rel['paragraph']
            rel_doc = f'{key} : {title} > {section} >  {para}'
            k_url = 'https://nkms.hkpalette.com/webapps/kk/kn/KkKn003.jsp?TYPE=KN&CNTNT_ID='
            k_url = f'{k_url}{cntnt_id}&CNTNT_NO={section_id}&SUB_CNTNT_NO={para_id}&MENU_ID='
            # print(f'[{rel_doc}]({k_url})')
            st.markdown(f'[{rel_doc}]({k_url})')

       
        with st.form('evaluate',clear_on_submit =True):
            releancy_grd, faithful_grd, context_grd = '','',''
            sentiment_mapping = ["1", "2", "3", "4", "5"]
            cols = st.columns(3)
            with cols[0]:
                st.text('답변관련도,Answer Relevancy', help="답변이 얼마나 질문과 관련이 있는가?")
                feed_selected = st.feedback("stars",key='1')
                if feed_selected is not None:
                    releancy_grd = sentiment_mapping[feed_selected]
            with cols[1]:
                st.text('답변충실도,Faithfulness', help="답변이 얼마나 추출된 문서와 관련이 있는가?")
                feed_selected = st.feedback("stars",key='2')
                if feed_selected is not None:
                    faithful_grd = sentiment_mapping[feed_selected]
            with cols[2]:
                st.text('문서추출정확도,Context Precision', help="답변과 관련된 문서를 놓은 우선순위로 검색했는가")
                feed_selected = st.feedback("stars",key='3')
                if feed_selected is not None:
                    context_grd = sentiment_mapping[feed_selected]
    
            comments = st.text_input("Comments Here!!! ", placeholder="답변에 대한 평가의견을 남겨주세요")
            submitted = st.form_submit_button("Submit")
            if submitted:
                if releancy_grd=='':
                    st.error("답변관련도 미평가")
                    return False
                if faithful_grd=='':
                    st.error("답변충실도 미평가")
                    return False
                if context_grd=='':
                    st.error("문서추출정확도 미평가")
                    return False

                room_id, room_seq = selected_df[['room_id','room_seq']].iloc[0]
                df_dict = {'room_id': [room_id],'room_seq': [room_seq], 'ans_relevancy':[releancy_grd], 'ans_faithfulness':[faithful_grd],
                        'cntxt_precision':[context_grd], 'comments': [comments], 'user_id':[''], 'it_processing': [datetime.now()]}    
                df_comments = pd.DataFrame(df_dict)
                db.insert_df_to_table(df_comments, f'{st.session_state.schemaname}.tbctrg05', mode='insert', repl_cond=None)

def contents_dashboard():

    if "contents_df"  in st.session_state:
        df = st.session_state['contents_df']

    if df is None:
        print("---- retry for query --------")
        df = get_contents_list("CA", include_embded=True)
    elif st.session_state.option_ctgr != "전체":  #대시보드 이므로 전체가 필요함
        df = get_contents_list("CA", include_embded=True)

    if st.session_state['min_dt'] is None:
        sql = f""" SELECT min(his_dt) min_dt, max(his_dt) max_dt
                        FROM tbctkk04 
                    """
        if (df_date := db.get_kms_datadf_ora(sql)) is None:
            st.sidebar.warning('No data found', icon="⚠️")
            st.stop()

        min_dt, max_dt = df_date['min_dt'][0], df_date['max_dt'][0]
        min_dt, max_dt = datetime.strptime(min_dt, '%Y%m%d'), datetime.strptime(max_dt, '%Y%m%d')
        st.session_state['min_dt'] = min_dt
        st.session_state['max_dt'] = max_dt

    col1, col2 = st.columns(2)
    if 'site' not in df.columns:
        df['site']   = df['ctgr_path'].apply(lambda x: x.split('>')[1])
        df['reg_dt'] = df['reg_dttm'].apply(lambda x: x.date())
         
    with col2:
        st.subheader('색인현황')
        opt = st.multiselect("Embedding Count By Site", df['site'].unique(),default=None, placeholder="Choose a Site"
                             ,label_visibility="collapsed")
        df['emb_c'] = [ 1 if x > 0 else 0  for x in df['emb_cnt'] ]
        
        if opt:
           df1 = df[df['site'].isin(opt)]
        else:
           df1 = df

        # bar chart
        c_mode = 'px'
        bar_chart(df1, c_mode, 'emb')
            
    with col1:
        st.subheader('컨텐츠 보유현황')
        opt = st.selectbox("Contents Count By Site", df['site'].unique(),index=None, placeholder="Choose a Site",label_visibility="collapsed")
        if opt:
            df11 = df[df['site']==opt]
        else:
            df11 = df
        
        # 상태별 파이차트
        if opt:
            df11['cnt'] = 1
            # st.write(opt, ':컨텐츠상태별')
            fig = px.pie(df11, values = 'cnt', names = "cd_nm", template = "gridon",
                         hole = 0.5,height=400, width = 500)
            fig.update_traces(text = df11["cd_nm"], textposition = "outside")
            st.plotly_chart(fig, use_container_width=True)
        else:
            c_mode = 'px'
            bar_chart(df11, c_mode, 'status')
       
    with st.expander(':rainbow[View data as Table] :sunglasses: '):
       df2 = df.groupby(['site','cd_nm']).agg({'cntnt_id':'count','emb_c':'sum'}).reset_index()
       df2['%'] = round(df2['emb_c'] * 100 / df2['cntnt_id'],0).map('{:.1f}%'.format)

       df2.rename(columns={'cntnt_id':'cntnt_cnt'},inplace=True)
       disp_df(df2)

    chk1 = st.checkbox('최근 등록/조회건수 보기 :memo: ')
    if chk1:
        df = st.session_state['contents_df']
        df_a = df[df['cd_nm']!='삭제'].groupby(['reg_dt'])[['cntnt_id']].count().rename(columns={'cntnt_id':'reg_cnt'}).sort_index(ascending=False)
        
        show_cnt = 30
        # Create figure with secondary y-axis
        fig = make_subplots(specs=[[{"secondary_y": True}]])
        fig.add_trace(
            go.Scatter(x=df_a.index[:show_cnt], y=df_a['reg_cnt'][:show_cnt], name="등록건수",
                       mode="lines+markers+text",text=df_a['reg_cnt'][:show_cnt]),
            row=1, col=1, secondary_y=False, 
        )
        fig.update_xaxes(title_text=None, tickangle=-60, tickfont_family='Rockwell',
                         tickfont_color='green', tickfont_size=12, tickformat = '%y.%m.%d',ticklen=10)
        fig.update_yaxes(title_text="등록건수")
        fig.update_yaxes(title_text="조회건수",secondary_y=True)
        fig.update_layout({
            'plot_bgcolor':  'Azure',
            'paper_bgcolor': 'Ivory',
        })
 
        df_inq = get_inquiry_cnt()
        df_inq['req_dt'] = df_inq['reg_dttm'].dt.date
        df_inq = df_inq.groupby(['req_dt'])[['inq_cnt']].sum().sort_index(ascending=False)
        fig.add_trace(
            go.Scatter(x=df_inq.index[:show_cnt], y=df_inq['inq_cnt'][:show_cnt], name="조회건수",
                       mode="lines+markers+text", text=df_inq['inq_cnt'][:show_cnt]),
            row=1, col=1, secondary_y=True,
        )
        fig.update_traces(textposition='top center')

        st.plotly_chart(fig, use_container_width=True, height = 200)

        with st.expander('View data'):
            dd = df_a.merge(df_inq, how='outer', left_index=True, right_index=True).fillna(0).sort_index(ascending=False).astype(int)
            disp_df(dd, False)
            # disp_df(df_a.reset_index().sort_values(by='reg_dt',ascending=False))

    chk2 = st.checkbox('월별 조회건수 보기:thumbsup: ')
    if chk2:
        # from_dt = st.session_state['min_dt'].date().strftime('%Y%m%d')
        # to_dt = st.session_state['max_dt'].date().strftime('%Y%m%d')
        # df = get_inquiry_cnt(from_dt, to_dt)
        df = get_inquiry_cnt()

        df['ym'] = df['reg_dttm'].dt.strftime("%Y%m")
        df['site'] = df['ctgr_path'].apply(lambda x: x.split('>')[1])
        df_inq = df.groupby(['ym','site'])['inq_cnt'].sum().reset_index()
        ym = sorted(df['ym'].unique(), reverse=True)
        
        sel_ym = st.multiselect("월", ym, default=ym[0], placeholder="년월을 선택~",label_visibility="collapsed")
     
        fig = px.bar(df_inq[df_inq['ym'].isin(sel_ym)], 
                     x="inq_cnt", y="site", color='ym',
                     template = "seaborn", text_auto=True,
                     orientation="h",
                     color_discrete_sequence=px.colors.qualitative.Pastel* len(df_inq))
                    #  color_discrete_sequence=["#0083B8"] * len(df_inq))
                     
        fig.update_yaxes(title_text=None)
        fig.update_xaxes(title_text="조회건수")
        fig.update_layout(legend_title_text= "년월")
        fig.update_layout({
            'plot_bgcolor':  'Azure',
            'paper_bgcolor': 'Ivory',
        })
   
        st.plotly_chart(fig, use_container_width=True, height = 200)
        with st.expander('View data'):
            # disp_df(df_inq.sort_values(by='ym', ascending=False))
            df_inq.columns = [DICT_COL.get(x,x) for x in df_inq.columns]
            st.write(df_inq.sort_values(by='년월', ascending=False).style.background_gradient(cmap="Oranges"))

def get_contents_list(ctgr_cd, include_embded, option="전체"):

    ## 전체는 root인 'CA'로 조회, 나머지는 카테고리ROOT코드로 조회
    temp = f" AND CTGR_ID =   '{ctgr_cd}'" if ctgr_cd !='CA' else  " AND CTGR_ID = 'CA' "
    sql = f"""
        SELECT
            AA.CNTNT_ID        
            , AA.TITL 
            , (SELECT CD_NM FROM TBCTKC10 WHERE CD = AA.SAVE_STAT_CD AND GROUP_CD = 'SAVE_STAT_CD') AS CD_NM
            , AA.CTGR_ID
            , AA.CTGR_PATH
            , AA.REG_DTTM
            , AA.UPD_DTTM
            , ((SELECT COUNT(FILE_KEY) FROM TBCTKK08 WHERE CNTNT_ID = AA.CNTNT_ID AND USE_YN='Y') +
               (SELECT COUNT(FILE_KEY) FROM TBCTKK22 WHERE CNTNT_ID = AA.CNTNT_ID AND USE_YN='Y')) AS ATT_CNT
            , (SELECT count(*) FROM TBCGPT10 G WHERE G.CNTNT_ID = AA.CNTNT_ID ) AS EMB_CNT
            FROM (
            SELECT A.CNTNT_ID
                ,(CASE WHEN A.SAVE_STAT_CD = '00' THEN 
                            (SELECT 
                            (CASE WHEN COUNT(*) > 0 THEN 'Y' ELSE 'N' END) AS AUTH_YN
                            FROM TBCTKC21 A
                            INNER JOIN TBCTKC23 B
                                ON  A.ATRT_GROUP_ID = B.ATRT_GROUP_ID
                            WHERE A.ETC_INFO01 > '50') 
                    ELSE 'Y' END) AS VIEW_YN
                , A.TITL
                , A.CTGR_ID
                , A.UPD_DTTM
                , A.REG_DTTM
                , BB.CTGR_PATH
                , A.SAVE_STAT_CD
            FROM TBCTKK01 A
            INNER JOIN
                (  SELECT  CTGR_ID
                    , CTGR_NM
                    , CTGR_PATH
                    , CONNECT_BY_ISLEAF AS IS_LEAF
                    FROM TBCTKK12
                    START WITH USE_YN ='Y'
                {temp}
                CONNECT BY PRIOR CTGR_ID = HGRK_CTGR_ID AND USE_YN ='Y' ) BB
            ON  A.CTGR_ID = BB.CTGR_ID
            ) AA
        WHERE AA.VIEW_YN='Y'
        ORDER BY AA.CNTNT_ID desc
    """

    # print(sql)
    if (df := db.get_kms_datadf_ora(sql)) is None:
        st.warning('No data found', icon="⚠️")
        st.stop()
    df.insert(0,'select', False)
    st.session_state["contents_df"] = df
    
    return df

def get_inquiry_cnt(from_dt=None, to_dt=None):
     
    if from_dt is None:
        from_dt = st.session_state['min_dt'].date().strftime('%Y%m%d')
    if to_dt is None:
        to_dt = st.session_state['max_dt'].date().strftime('%Y%m%d')

    sql = f"""
    SELECT  	ROW_NUMBER() OVER (ORDER BY A.INQ_CNT DESC)  AS ROW_NUMBER
     , A.CNTNT_ID
     , A.DT
    , A.TITL
    , A.INQ_CNT
    , A.CTGR_PATH
    , A.REG_DTTM
    , DPKMAPP.FNCTK_GET_USERNAME(A.REGR_ID, A.REGR_DEPT_CD, 'DEPT') AS REGR_NM
     FROM (
    SELECT   
    		 A.CNTNT_ID
    	   , A.DT
    	   , MAX(B.TITL)							AS TITL
    	   , SUM(nvl(A.INQ_CNT, 0))					AS INQ_CNT
    	   , MAX(B.CTGR_ID)							AS CTGR_ID
    	   , MAX(C.CTGR_PATH) 						AS CTGR_PATH
    	   , MAX(B.REG_DTTM) 						AS REG_DTTM
     	   , MAX(B.REGR_ID)							AS REGR_ID
     	   , MAX(B.REGR_DEPT_CD)					AS REGR_DEPT_CD
      FROM DPKMAPP.TBCTKK03 A	   /* 컨텐츠 마스터  */
    INNER JOIN DPKMAPP.TBCTKK01 B ON (A.CNTNT_ID = B.CNTNT_ID)							
     INNER JOIN (
            SELECT CTGR_ID
                 , CTGR_NM
                 , CTGR_PATH
              FROM DPKMAPP.TBCTKK12
             START WITH 1=1
			   AND CTGR_ID      ='CA'
               CONNECT BY PRIOR CTGR_ID = HGRK_CTGR_ID AND USE_YN ='Y'
            ) C 
            ON (B.CTGR_ID = C.CTGR_ID)  /* 카테고리 */
	  WHERE 1=1
 	  AND DT BETWEEN {from_dt} AND {to_dt}							
      GROUP BY A.CNTNT_ID, A.DT
     ) A
    """
    if (df_inq := db.get_kms_datadf_ora(sql)) is None:
        st.warning('No data found', icon="⚠️")
        st.stop()
    
    return df_inq

def disp_df(edited_df, hide_idx=True):
        disp_df = edited_df.copy()
        disp_df.columns = [DICT_COL.get(x,x) for x in disp_df.columns]

        st.dataframe(disp_df.style.set_properties(**{'background-color': 'white',
                            'color': 'black',
                            'border-color': 'blue'}),
                    hide_index=hide_idx, width=1000)
        
def bar_chart(df2, mode, name):
    # plotly 이용방법
    if mode == 'px' and name == 'emb':
       df2['emb_status'] = [ '완료' if x > 0 else '미완료'   for x in df2['emb_cnt']]
       df2 = df2.groupby(by = ["site",'emb_status'], as_index = False).agg({"cntnt_id":'count'})
       y_max = df2['cntnt_id'].max()  + 50
       fig = px.bar(df2, x = "site", y = 'cntnt_id',
                    template = "seaborn", color="emb_status",
                    barmode = 'stack', text_auto=True, color_discrete_sequence=px.colors.qualitative.G10)
       fig.update_xaxes(title_text=None,tickangle=-45, tickfont_family='Rockwell', tickfont_color='green', tickfont_size=12)
       fig.update_yaxes(title_text=None,range=[0,y_max])
       fig.update_layout(legend_title_text=None,
                         legend_orientation="h",
                         legend_yanchor="top",legend_y=0.99,
                         legend_xanchor="left",legend_x=0.01)
       st.plotly_chart(fig,use_container_width=True, height = 200)
    
    if mode == 'px' and name == 'status':
        df2 = df2.groupby(by = ["site",'cd_nm'], as_index = False).agg({"cntnt_id":'count'})
        y_max = df2['cntnt_id'].max()  + 50
        fig = px.bar(df2, x = "site", y = 'cntnt_id' ,
                     template = "seaborn", color="cd_nm", 
                     barmode = 'stack', text_auto=True)
        fig.update_xaxes(title_text=None,tickangle=-45, tickfont_family='Rockwell', tickfont_color='green', tickfont_size=12)
        fig.update_yaxes(title_text=None,range=[0,y_max])
        fig.update_layout(legend_title_text=None,
                         legend_orientation="h",
                         legend_yanchor="top",legend_y=0.99,
                         legend_xanchor="left",legend_x=0.01)
        st.plotly_chart(fig,use_container_width=True, height = 200)

if __name__ == '__main__':
    app()

