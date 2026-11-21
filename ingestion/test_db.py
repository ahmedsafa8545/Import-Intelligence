import os
import sqlalchemy as s

def check_db():
    # Airflow uses SQLAlchemy 1.4, so engine.begin() + text() works there
    engine = s.create_engine(os.environ["SQLALCHEMY_DB_URL"])
    with engine.begin() as conn:
        result = conn.execute(s.text("select 1")).scalar()
    print(f"DB check result: {result}")   # should print 1
    return result

if __name__ == "__main__":
    check_db()