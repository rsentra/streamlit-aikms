import streamlit as st
import requests 
from datetime import datetime
import pandas as pd
from models import database as db
import random

def show_history():
    print('----- show history ==', len(st.session_state['chat']))
    if bool(st.session_state['chat']):
       for i, chat in enumerate(st.session_state['chat']):
           query = chat.get('query')
           answer = chat.get('answer')
           rel_documents = chat.get('rel_docs')
           with st.expander(f'{i}_history'):
               show_message(query,answer,rel_documents)

def show_message(query=None,answer=None,hashtag=None,rel_documents=None):
    if query:
        st.markdown(query)

    with st.chat_message('ai'):
        st.markdown(answer)
    st.markdown(f'검색키워드: {hashtag}')
    # cols = st.columns(3)
    # for i, rel in enumerate(rel_documents):
    #     doc = rel.get('doc')
    #     title = rel.get('metadata').get('title')
    #     with cols[i]:
    #         with st.expander(f'{i}-{title}'):
    #             st.markdown(doc,unsafe_allow_html=True)

    for rel in rel_documents:
        key = rel.get('metadata').get('primary_key') 
        cntnt_id, section_id, para_id = key.split('>')
        title = rel.get('metadata').get('title')
        section = rel.get('metadata').get('section')
        para = rel.get('metadata').get('paragraph')
        rel_doc = f'{key} : {title} > {section} >  {para}'
         # print(rel_doc)
        k_url = 'https://nkms.hkpalette.com/webapps/kk/kn/KkKn003.jsp?TYPE=KN&CNTNT_ID='
        # https://nkms.hkpalette.com/webapps/kk/kn/KkKn003.jsp?TYPE=KN&CNTNT_ID=52164&CNTNT_NO=1&SUB_CNTNT_NO=1.18&MENU_ID=

        k_url = f'{k_url}{cntnt_id}&CNTNT_NO={section_id}&SUB_CNTNT_NO={para_id}&MENU_ID='
        with st.chat_message('ai'):
            st.markdown(f'[{rel_doc}]({k_url})')

    room_id = key + str(random.randint(1,999))
    show_after(room_id)

def insert_chat_history(code,query,answer=None, hashtag=None, rel_docs=None,res_code = None):
    req_time = st.session_state.req_time
    res_time = st.session_state.res_time
    req_time = req_time.strftime('%Y-%m-%d %H:%M:%S.%f')
    res_time = res_time.strftime('%Y-%m-%d %H:%M:%S.%f')
  
    room_seq = len(st.session_state['chat'])
    cur_time = datetime.now()
    if room_seq ==0:
        room_id = cur_time.strftime('%Y%m%d%H%M%S%f')
    else:
        room_id = st.session_state['chat'][0].get('room_id')
        
    print('insert history:',room_id,'-',room_seq)

    user_id = ''
    
    df02 = pd.DataFrame([room_id, room_seq, user_id, code, query, req_time]).transpose()
    df02.columns = ['room_id','room_seq','user_id','vtr_db_id','query_sentence','request_time']
    
    df03 = pd.DataFrame([room_id, room_seq, res_code, answer, res_time]).transpose()
    df03.columns = ['room_id','room_seq','response_cd','llm_response','response_time']

    doc_seq, cntnt_key, title, section, para = [], [], [], [],[]
    for i, rel_doc in enumerate(rel_docs):
        doc_seq.append(i)
        cntnt_key.append(rel_doc.get('metadata').get('primary_key'))
        section.append(rel_doc.get('metadata').get('section'))
        para.append(rel_doc.get('metadata').get('paragraph'))
        title.append(rel_doc.get('metadata').get('title'))
        
    df04 = pd.DataFrame([doc_seq, cntnt_key, title, section, para]).transpose()
    df04.columns=['doc_seq','cntnt_key','title','section','paragraph']
    df04.insert(0,'room_id',room_id)
    df04.insert(1,'room_seq',room_seq)
    df04.insert(len(df04.columns),'it_processing',res_time)

    db.insert_df_to_table(df02, f'{st.session_state.schemaname}.tbctrg02', mode='insert', repl_cond=None)
    db.insert_df_to_table(df03, f'{st.session_state.schemaname}.tbctrg03', mode='insert', repl_cond=None)
    db.insert_df_to_table(df04, f'{st.session_state.schemaname}.tbctrg04', mode='insert', repl_cond=None)

    # 질의에 대한 키워드 hashtag입력
    # tag_list = [ x.replace("#",'').strip()  for x in hashtag]
    tag_list = [ x.strip()  for x in hashtag]
    df07 = pd.DataFrame(tag_list, columns=['tag_name'])
    db.insert_hashtag(room_id, room_seq, df07, f'{st.session_state.schemaname}')

    return room_id, room_seq
    
def app():
    if "chat" not in st.session_state:
        st.session_state.chat = []
    if "req_time" not in st.session_state:
        st.session_state.req_time= None
    if "res_time" not in st.session_state:
        st.session_state.res_time= None
    if "schemaname" not in st.session_state:
        st.session_state.schemaname= None    
    if st.session_state.schemaname is None:  #db 스키마를 읽어옴
        conn_name = st.secrets["connections_dbms"]["conn_name"]
        st.session_state.schemaname = st.secrets[conn_name]["schemaname"]
    if "vector_stores" not in st.session_state:
        st.session_state.vector_stores= None   
    if st.session_state.vector_stores is None: 
        conn_name = st.secrets["connections_dbms"]["conn_name"]
        url = f'http://hkcloudai.com:8018/api/v1_1/vectorstore_api/vectorDB/list'
        res = requests.get(url)
        res_str = res.json()
        st.session_state.vector_stores = res_str['vector_stores']
    options = st.session_state.vector_stores

    cols = st.columns([4,1])
    with cols[0]:
        code = st.selectbox('Choose category', options=options, placeholder="Choose a Category", index=0, label_visibility="collapsed")
    with cols[1]:
        chk = st.checkbox("reset history", value=True)  
        if chk:
            st.session_state.chat = []

    query  = st.chat_input('text input...')
    if  query:
        url = f'http://hkcloudai.com:8018/api/v1_1/retrieve/retrieve/retrieval_w_hashtag_non_streaming?code={code}&query={query}'
        with st.spinner('Wait for it...'):
            st.session_state.req_time = datetime.now()
            res = requests.get(url)
            print('res cd1::', res.status_code,query)
            answer = ''
            rel_documents = ''
            st.session_state.res_time = datetime.now()

            if res.status_code == 200:
                show_history()
                res_str = res.json()  ## dictionary로 넣어줄 필요가 있음
                # print(res_str)
                answer = res_str['llm_response'][0]
                rel_documents = res_str.get('retrieval_docs')
                hashtag = res_str.get('hashtags')
                show_message(query, answer, hashtag, rel_documents)
            else:
                st.error('Error occurred while processing your request=>',res.status_code)

            room_id,room_seq = insert_chat_history(code=code, query=query, answer=answer, hashtag=hashtag, rel_docs=rel_documents, res_code=res.status_code)
            st.session_state['chat'].append({'query':query,'answer':answer,'rel_docs':rel_documents,'room_id':room_id})
            #답변 입력
            # process_comments(room_id, room_seq)   
            # show_after(room_id)

def show_after(room_id):  
    cols = st.columns([20,1,1,1,1,1])
    with cols[1]:
        st.markdown("📋", help="복사")  
        # btn_copy = st.button("📋", key=f"{room_id}_copy", help="복사")  
        # if btn_copy:
        #     st.write('copy to clipboard')
    with cols[2]:
        st.markdown("➖", help="요약") 
        # btn_summary = st.button("➖", key=f"{room_id}_summary", help="요약")  
        # if btn_summary:
        #     st.write('요약결과는')   
    with cols[3]:
        st.markdown("👍", help="평가")
        # btn_evaluate = st.button("👍", key=f"{room_id}_evaluate", help="평가")  
        # if btn_evaluate:
        #     st.write('평가')  
    with cols[4]:
        st.markdown("📝", help="내보내기")
        # btn_export = st.button("📝", key=f"{room_id}_export", help="내보내기")    
        # if btn_export:
        #     st.write('내보내기')
    with cols[5]:
        st.markdown(":blue_book:", help="최근 질문보기")
        # btn_recent = st.button(":blue_book:", key=f"{room_id}_recent", help="최근 질문보기")      
        # if btn_recent:
        #     st.write('최근질문')
        #     # show_recent_queries(room_id, room_seq) 
     

def process_comments(room_id, room_seq):
    print('hahaha..........')
    with st.form('evaluate2',clear_on_submit =True):
        releancy_grd, faithful_grd, context_grd = '','',''
        sentiment_mapping = ["1", "2", "3", "4", "5"]
        cols = st.columns(3)
        with cols[0]:
            st.text('답변관련도,Answer Releancy', help="답변이 얼마나 질문과 관련이 있는가?")
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
    
        comments = st.text_input("Comments Here!!! ", placeholder="평가에 대한 의견을 남겨주세요")
        submitted = st.form_submit_button("Submit")
        if submitted:
            if releancy_grd=='':
                st.error("답변관련도 미평가")
            if faithful_grd=='':
                st.error("답변충실도 미평가")
            if context_grd=='':
                st.error("문서추출정확도 미평가")

            df_dict = {'room_id': [room_id],'room_seq': [room_seq], 'ans_relevancy':[releancy_grd], 'ans_faithfulness':[faithful_grd],
                    'cntxt_precision':[context_grd], 'comments': [comments], 'user_id':[''], 'it_processing': [datetime.now()]}    
            df_comments = pd.DataFrame(df_dict)
            db.insert_df_to_table(df_comments, f'{st.session_state.schemaname}.tbctrg05', mode='insert', repl_cond=None)
        
if __name__ == '__main__':
    app()