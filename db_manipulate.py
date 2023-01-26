import pandas as pd
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy import  create_engine

try:
    # when using crud for other modules, dataModel needs to be imported there
    import dbModel#chcwd
    from dbModel import engine, Base
except:
    pass

from typing import List
from pathlib import Path
import os

from typing import List, Dict, Union, Tuple
from datetime import datetime

from dbModel import Education, Person


sqlite_file = r"C:\Users\thuc-nhat-truong.huy\Desktop\DataOpsEngineerCaseStudy\personDB.db"
sqlite_url = f"sqlite:///{sqlite_file}"

engine = create_engine(sqlite_url)
db_session = scoped_session(
    sessionmaker(autocommit=False, autoflush=False, bind=engine)
)

def process_person(file_path):
    person_df = pd.read_csv(file_path)
    person_df.drop(columns=['education'], inplace=True)
    person_df.rename(columns={
            "educational-num":"educational_number",
            "marital-status":"marital_status",
            "relationship":"relation_ship",
            "capital-gain":"capital_gain",
            "capital-loss":"capital_loss",
            "hours-per-week":"hours_per_week",
            "native-country":"native_country"
        },inplace=True)
    return person_df

def process_education(file_path):
    edu_df = pd.read_csv(file_path)
    edu_df = edu_df.drop_duplicates(subset='educational-num', keep="first", ignore_index=True)
    edu_df.drop(
        columns=['age','workclass','fnlwgt', 'marital-status','relationship','race','gender','capital-gain','capital-loss','hours-per-week','native-country'],
        inplace=True)
    edu_df.rename(columns={"education":"name"},inplace=True)
    return edu_df

def insert_to_db(person_df, edu_df):
    
    person_dict = person_df.to_dict(orient="records")
    edu_dict = edu_df.to_dict(orient="records")

    db_session.bulk_insert_mappings(Person, person_dict)
    db_session.bulk_insert_mappings(Education, edu_dict)

    # # Commit the changes
    db_session.commit()
    # # Close the session
    db_session.close()


def main():
    edu_df = process_education(r'C:\Users\thuc-nhat-truong.huy\Desktop\DataOpsEngineerCaseStudy\cleaned_Data.csv')
    person_df = process_person(r'C:\Users\thuc-nhat-truong.huy\Desktop\DataOpsEngineerCaseStudy\cleaned_Data.csv')
    # print(person_df)

    insert_to_db(person_df, edu_df)

main()