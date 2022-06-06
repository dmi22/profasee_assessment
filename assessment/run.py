import pandas as pd
import json
import sqlalchemy
from fastapi import FastAPI

CONFIG_FILE_NAME = "config.json"


def run():
    print("Hello, Profasee!")


def get_config():
    with open(CONFIG_FILE_NAME) as json_file:
        return json.load(json_file)


def unify_phone_separator(df):
    df["PhoneNumber"].replace(regex="\.", value="-", inplace=True)


def strip_interests(df):
    for c in df.columns:
        if "Interest" in c:
            df[c] = df[c].str.strip()


def filter_no_interests(df):
    df.dropna(how='all', subset=[c for c in df.columns if "Interest" in c], inplace=True)


def open_sql_conn(sql_db_url):
    engine = sqlalchemy.create_engine(sql_db_url)
    return engine.connect()


def load_to_db(df, con, target_table_name):
    df.to_sql(target_table_name, con, if_exists='replace', index=False)


def get_age_stat(con):
    return dict(con.execute(sqlalchemy.text(
        "select min(p.Age) as min_age, max(p.Age) as max_age, avg(p.Age) as avg_age from people p"
    )).fetchone())


def get_the_biggest_city(con):
    return dict(con.execute(sqlalchemy.text(
        "select p.City as city_most_people from people p group by p.City order by count(*) desc limit 1"
    )).fetchone())


def get_top_5_interests(con):
    cur = con.execute(sqlalchemy.text("select u.interest from "
                                      "(select p.Interest1 as interest from people p union all "
                                      "select p.Interest2 from people p union all "
                                      "select p.Interest3 from people p union all "
                                      "select p.Interest4 from people p) u "
                                      "where u.interest is not null "
                                      "group by u.interest "
                                      "order by count(*) desc "
                                      "limit 5"))
    i = 1
    res = dict()
    for r in cur:
        res[str(i) + " of top interests"] = r[0]
        i += 1
    return res


def get_data_stats(con):
    return get_age_stat(con) | get_the_biggest_city(con) | get_top_5_interests(con)


config = get_config()

if __name__ == "__main__":
    run()

    # Task 1
    df = pd.read_csv(config["src_url"])
    df.to_csv(config["raw_data_file_path"], index=False)
    df.to_json(config["converted_data_file_path"], orient="records")

    # Task 2
    unify_phone_separator(df)
    strip_interests(df)
    filter_no_interests(df)

    # Task 3
    con = open_sql_conn(config["sql_db_url"])
    load_to_db(pd.read_csv(config["src_url"]), con, config["target_table_name"])
    con.close()

    # Task 4
    con = open_sql_conn(config["sql_db_url"])
    print(get_data_stats(con))
    con.close()

# Task 5
app = FastAPI()


@app.get("/get-age-stat")
def get_age_stat_api():
    con = open_sql_conn(config["sql_db_url"])
    res = get_age_stat(con)
    con.close()
    return res


@app.get("/get-the-biggest-city")
def get_the_biggest_city_api():
    con = open_sql_conn(config["sql_db_url"])
    res = get_the_biggest_city(con)
    con.close()
    return res


@app.get("/get-top-5-interests")
def get_top_5_interests_api():
    con = open_sql_conn(config["sql_db_url"])
    res = get_top_5_interests(con)
    con.close()
    return res
