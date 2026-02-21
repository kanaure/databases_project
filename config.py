from sqlalchemy import create_engine

# PostgreSQL connection string for db_group_D
# Replace 'your_username' and 'your_password' with your actual PostgreSQL credentials
DATABASE_URL = "postgresql://group_d:WhereAreYou#@sqlcourse.servers.ceu.edu:5432/db_group_d"

# Create the engine
engine = create_engine(DATABASE_URL)

# Test the connection
def test_connection():
    try:
        with engine.connect() as conn:
            print("✓ Database connection successful!")
            return True
    except Exception as e:
        print(f"✗ Connection failed: {e}")
        return False
