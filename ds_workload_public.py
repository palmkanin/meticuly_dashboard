# --- Import
from datetime import datetime, timedelta
from operator import index, mod
from re import X
from tabnanny import check
import time
from pyparsing import col
import streamlit as st
import pandas as pd
import subprocess
import numpy as np
import os
import sys
import requests


### --- Time Track --- ###
start = time.time()
### ------------------ ###

# --- Read full_log.xlsx
url_full_log = 'https://raw.githubusercontent.com/palmkanin/meticuly_dashboard/main/full_log.csv'
# myfile_full_log = requests.get(url_full_log)

# full_log = pd.read_excel(myfile_full_log.content, index_col=0)
full_log = pd.read_csv(url_full_log)
full_log = full_log.iloc[:,1:]

# --- Read userID
url_userID = 'https://raw.githubusercontent.com/palmkanin/meticuly_dashboard/main/userID.csv'
# myfile_userID = requests.get(url_userID)

# userID = pd.read_excel(myfile_userID.content, index_col=0)
userID = pd.read_csv(url_userID)

# --- Read em_designer
url_em_designer = 'https://raw.githubusercontent.com/palmkanin/meticuly_dashboard/main/em_designer.csv'
# myfile_em_designer = requests.get(url_em_designer)

# em_designer = pd.read_excel(myfile_em_designer.content, index_col=0)
em_designer = pd.read_csv(url_em_designer)
em_designer = em_designer.iloc[:,1:-2]

em_designer['1'].replace('<NA>',np.nan, regex=True)
em_designer = em_designer.dropna()
em_designer.set_index(em_designer['1'],inplace=True)
em_designer = em_designer.iloc[:,0]
em_designer = em_designer.reset_index()
em_designer.columns = ['name','em_code']

# --- Combine userID & em_designer
tempID = userID.merge(em_designer,how='left', on='name')
em_code_list = list(tempID['em_code'])
userID['em_code'] = em_code_list
userID.pop('id')
userID.columns = ['started_user_id','em_code']
# st.dataframe(userID)

# --- Designer Cleaning
full_log_merge = pd.merge(full_log, userID,on='started_user_id', how='left')
## -- check if more than one designer
full_log_merge['assigned_designer_count'] = full_log_merge['assigned_designer'].str.count(',') + 1
full_log_merge.loc[full_log_merge['started_user_id'] == 'Product Owner Team','em_code'] = full_log_merge['assigned_designer']
full_log_merge.loc[full_log_merge.assigned_designer_count > 1,'assigned_designer'] = full_log_merge['em_code']
full_log_merge['assigned_designer'] = full_log_merge['assigned_designer'].fillna('NV')
full_log_merge['duration_hrs'] = pd.to_numeric(full_log_merge['duration_hrs'])
full_log_merge['designer_check'] = full_log_merge['assigned_designer'] == full_log_merge['em_code']

temp_col = full_log_merge.pop('em_code')

full_log_merge.insert(2, temp_col.name,temp_col)
full_log_merge.columns = full_log_merge.columns.str.replace('em_code','actual_designer')

display_log = full_log_merge.iloc[:,:-2].copy()
display_log_2 = display_log.loc[:, ~display_log.columns.isin(['start', 'end'])]


# --- Show Current Session
current_log = display_log[display_log['running'] == True]
current_log = current_log[current_log['ended_at_date'].isna()]

group_current_log = current_log.groupby(['co_num','actual_designer','process','start']).aggregate('sum')
group_current_log = group_current_log.reset_index()
group_current_log = group_current_log.iloc[:,:-2]
group_current_log = group_current_log.set_index('co_num')
today = pd.Timestamp.now() - timedelta(hours=7)
group_current_log['today'] = today
group_current_log['start'] = group_current_log['start'].str.replace(' ', 'T')
group_current_log['start'] = group_current_log['start'].astype('datetime64[ns]')

group_current_log['session_duration'] = (group_current_log['today'] - group_current_log['start'])/ np.timedelta64(1,'D')
group_current_log['session_duration'] = group_current_log['session_duration'].map('{:,.2f} Day'.format)
group_current_log = group_current_log[['actual_designer','process','session_duration']]

# --- Title -------------------------------------------------------------------------------------------------------------------------------
st.title("Design Workload Monitoring :desktop_computer:")
st.write('###')

# --- Log Display
st.subheader('Current Session')
st.dataframe(group_current_log,width=450)

with st.expander('Click to see full log.'):
    st.dataframe(display_log_2)
st.subheader('#')



# --- Designer Loads -------------------------------------------------------------------

st.subheader('Designer Loads')

## -- Date Filter
Col1, Col2 = st.columns(2)

with Col1:
    start_date = st.date_input("Start Date")
    start_date_o = start_date
    start_date = datetime.strftime(start_date,'%Y-%m-%d')
    
with Col2:
    end_date = st.date_input("End Date")
    end_date_o = end_date
    end_date = datetime.strftime(end_date,'%Y-%m-%d')

total_date_o = (end_date_o - start_date_o) + timedelta(days=1)
start_date_o = start_date_o - total_date_o
start_date_o = datetime.strftime(start_date_o,'%Y-%m-%d')

end_date_o = end_date_o - total_date_o
end_date_o = datetime.strftime(end_date_o,'%Y-%m-%d')

st.write("#")

## -- Designer Tabs

ds_list = ['AS','PH', 'KSAE', 'CB','KPO','PB','NN','KSI','KP','TC','NV']
ds_tabs = st.tabs(ds_list)



for i in range(len(ds_list)):

    with ds_tabs[i]:
        def get_o_n(full_log_merge, start_date, end_date):
            ds_condition1 = full_log_merge['actual_designer'] == ds_list[i]
            condition2 = (full_log_merge['started_at_date'] >= start_date) & (full_log_merge['ended_at_date'] <= end_date)
            ds_mask = (ds_condition1 & condition2)
            
            designer_log = full_log_merge[ds_mask]
            designer_log_ds = designer_log[designer_log['process'] == 'design']
            designer_log_rev = designer_log[designer_log['process'] == 'revision']
            designer_log_manu = designer_log[designer_log['process'] == 'manu']

            full_ds = full_log_merge[ds_condition1]
            # full_ds
            def date_group(log):
                date_group = log.groupby(['started_at_date']).sum()
                date_group = date_group['duration_hrs']
                date_group = date_group.reset_index()
                return date_group
            
            designer_log_ds = date_group(designer_log_ds)
            designer_log_rev = date_group(designer_log_rev)
            designer_log_manu = date_group(designer_log_manu)
            full_ds = date_group(full_ds)
            
            ## --- Group workload into date range
            x_axis = pd.date_range(start= start_date, end= end_date)
            x_frame = pd.DataFrame(x_axis, index=x_axis)
            x_frame = x_frame.index.format()
            x_frame = pd.DataFrame(x_frame, columns=['started_at_date'])
            

            load_frame_ds = pd.merge(x_frame, designer_log_ds, how='left')
            load_frame_rev = pd.merge(x_frame, designer_log_rev, how='left')
            load_frame_manu = pd.merge(x_frame, designer_log_manu, how='left')
            
            
            x_frame['ds_dur'] = load_frame_ds['duration_hrs']
            x_frame['rev_dur'] = load_frame_rev['duration_hrs']
            x_frame['manu_dur'] = load_frame_manu['duration_hrs']
            x_frame = x_frame.fillna(0)
            x_frame = x_frame.set_index('started_at_date')
            
            x_frame_sum = x_frame.copy()
            x_frame_sum['total_dur'] = x_frame_sum['ds_dur'] + x_frame_sum['rev_dur'] + x_frame_sum['manu_dur']
            x_frame_sum = x_frame_sum.reset_index()

            # --- Data Summary
            total_design = x_frame_sum['ds_dur'].sum()
            total_revision = x_frame_sum['rev_dur'].sum()
            total_manu = x_frame_sum['manu_dur'].sum()
            total_dur_sum = x_frame_sum['total_dur'].sum()
            
            ## -- Business Day
#             bday = pd.bdate_range(start_date, end_date) --# some error when use busniness day
            bday = pd.date_range(start_date, end_date)

            bday_count = len(bday)
            total_bday_loads = bday_count * 8
            
            ## -- Ds Loads Criteria
            ds_loadss = (total_dur_sum / total_bday_loads) * 100
            mooda = ''
            if ds_loadss <= 20:
                mooda = '😄'
            elif ds_loadss <= 40:
                mooda = '🙂'
            elif ds_loadss <= 60:
                mooda = '😐'
            elif ds_loadss <= 80:
                mooda = '🙁'
            elif ds_loadss <= 100:
                mooda = '😵'
            elif ds_loadss > 100:
                mooda = '🤢'
            else:
                mooda = '🌙'

            ## -- format date
            start_date_f = datetime.strptime(start_date, '%Y-%m-%d')
            start_date_f = start_date_f.strftime('%d-%b-%Y')
            end_date_f = datetime.strptime(end_date, '%Y-%m-%d')
            end_date_f = end_date_f.strftime('%d-%b-%Y')

            # -- Result Check
            date_co_num_group =  designer_log.copy()
            date_co_num_group = date_co_num_group.loc[date_co_num_group['process'] != 'review']
            date_co_num_group = date_co_num_group[['co_num','assigned_designer','process','started_at_date','running','duration_hrs']]
            result_co_num = date_co_num_group.groupby(['co_num']).aggregate('sum')
            result_co_num = result_co_num.reset_index()
            result = date_co_num_group.groupby(['started_at_date','co_num','assigned_designer','process'])['duration_hrs'].aggregate('sum')
            result = result.reset_index()
            result = result.set_index('started_at_date')
            
            row = (result.shape)[0]
            case_assist = result_co_num['co_num'].count()
            return result, mooda, case_assist, total_design, total_revision, total_manu, ds_loadss,start_date_f,end_date_f, row, x_frame


        result, mood, case_assist, total_design, total_revision, total_manu, ds_loads,start_date_f,end_date_f, row, x_frame = get_o_n(full_log_merge, start_date, end_date) # ---- Current Date Range
        result_o, mood_o, case_assist_o, total_design_o, total_revision_o, total_manu_o, ds_loads_o,start_date_f_o,end_date_f_o, row_o, x_frame_o = get_o_n(full_log_merge, start_date_o, end_date_o) # ---- Previous Date Range
        
        total_dur = total_design + total_revision + total_manu
        total_dur_o = total_design_o + total_revision_o + total_manu_o

        # --- Display -----------------------------------------------------------------------
#         st.write('###')
#         st.caption(f'from {start_date_f} - {end_date_f}')
        st.write('###')
        
        block1, block2 = st.columns(2, gap="small")
        with block1:
            st.subheader(f'{ds_list[i]} Workload Summary ')
            st.caption(f'from {start_date_f} - {end_date_f}')
        with block2:
            st.subheader('status: '+ mood)
            
            mood_info = {'mood': ['🌙', '😄','🙂','😐','🙁','😵','🤢'], 'details': ['0 %', '< 20 %','20 - 40 %','40 - 60 %','60 - 80 %','80 - 100 %','> 100 %']}
            mood_table = pd.DataFrame(data=mood_info)
            with st.expander('status info'):
                st.table(mood_table)
        
        st.write('###')

        # # --- Total Cases
        case_delta =  ((case_assist-case_assist_o)/case_assist_o)*100
        # case_delta = delta_display(case_delta)

        # # --- Total Cases
        total_dur_delta =  ((total_dur-total_dur_o)/total_dur_o)*100
        
        # case_delta = delta_display(case_delta)
        # # --- Total Design
        design_delta = ((total_design-total_design_o)/total_design_o)*100
        # design_delta = delta_display(design_delta)
        
        # # --- Total Revision
        rev_delta = ((total_revision-total_revision_o)/total_revision_o)*100
        # rev_delta = delta_display(rev_delta)

        # # --- Total Manu
        manu_delta = ((total_manu-total_manu_o)/total_manu_o)*100
        # manu_delta = delta_display(manu_delta)
        
        # # --- Total Loads
        load_delta = ((ds_loads-ds_loads_o)/ds_loads_o)*100
        # load_delta = delta_display(load_delta)

        
        st.markdown('**Total**')
        
        col11, col22, col33, col44 = st.columns(4)

        with col11:
            st.metric('Case Assisted', f'{case_assist} case',f'{case_delta:,.2f} %')
        with col22:
            st.metric('Design Duration',f'{total_dur:,.2f} hrs',f'{total_dur_delta:,.2f} %')
        with col33:
            st.metric(f'Workloads',f'{ds_loads:,.2f} %',f'{load_delta:,.2f} %')

        st.markdown('###')
        st.markdown('**Details**')

        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.metric('Design 💻',f'{total_design:,.2f} hrs')
        with col2:
            st.metric('Revision 🔨',f'{total_revision:,.2f} hrs')
        with col3:
            st.metric('Manu Prep 🏭',f'{total_manu:,.2f} hrs')    
        
        st.write("###")
        
        
        
        if row == 0:
            st.warning('no data found.')
        else:    
            # -- Workload Chart
            st.markdown(f'Total designing hours / day')
            st.bar_chart(x_frame)

            ## -- Expander
            with st.expander("Click to see more details"):
                result.index.set_names = [['Date','co_num','assigned_designer','process']]
                st.dataframe(result) # --- expander data


st.markdown("""---""")
##### Refresh -----------------------------------------
# path = os.getcwd()+'/monday_fetch.py'

# if st.button('Fetch Data'):
#     with st.spinner('Fetching Monday Data...'):
#         if subprocess.run([sys.executable, 'monday_fetch.py']):
#             st.success('Success')
#             st.experimental_rerun()
#         else:
#             st.error('Error to fetch data')

#### --------------------------------------------------

### --- Time Track --- ###
end = time.time()
total_run = (end - start)
st.markdown(f'Total running time: {total_run:.3f} seconds with {full_log_merge.shape[0]} results.')
### ------------------ ###







