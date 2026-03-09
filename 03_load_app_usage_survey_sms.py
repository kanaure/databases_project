# 08_load_app_usage (Egor)

# 09_load_survey (Egor)

# 10_load_sms (Egor)

import os
import pandas as pd
import os
import re
from config import engine

#paths
data_folder_path = os.path.join(os.path.dirname(__file__), 'data','dataset')

app_folder_path = os.path.join(data_folder_path, 'app_usage')
survey_folder_path = os.path.join(data_folder_path, 'survey')
sms_folder_path = os.path.join(data_folder_path, 'sms')


# Helper function to extract uid from filename
def extract_uid(filename):
    """Extract uid from filenames like 'activity_u01.csv' -> 'u01'"""
    match = re.search(r'u\d+', filename)
    return match.group() if match else None

    




#app usage population    
# # App Usage
# class AppUsage(Base):
#     __tablename__ = "app_usage"
#     usage_id = Column(Integer, primary_key=True, autoincrement=True)
#     uid = Column(String, ForeignKey("student.uid"), nullable=False)
#     record_id = Column(String)
#     device = Column(String)
#     timestamp = Column(BigInteger)
#     running_tasks_base_activity_mclass = Column(String)
#     running_tasks_base_activity_mpackage = Column(String)
#     running_tasks_id = Column(Integer)
#     running_tasks_num_activities = Column(Integer)
#     running_tasks_num_running = Column(Integer)
#     running_tasks_top_activity_mclass = Column(String)
#     running_tasks_top_activity_mpackage = Column(String)

for filename in os.listdir(app_folder_path):        
        filepath = os.path.join(app_folder_path, filename)
        df = pd.read_csv(filepath)
        df['uid'] = extract_uid(filename)
        df.columns = df.columns.str.strip()
        print(df.head())
        break