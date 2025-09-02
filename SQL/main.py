from pathlib import Path
import sqlite3

def create_database():
    
    db_path = Path(__file__).parent / "drivers.db"

    if db_path.exists():
        db_path.unlink()
    
    conn = sqlite3.connect("drivers.db")
    cursor = conn.cursor()

    return conn, cursor

def create_tables(cursor):
    cursor.execute("""
        CREATE TABLE Drivers (
            id INTEGER PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            age INTEGER NOT NULL,
            WDC INTEGER
        )
    """)

    cursor.execute("""
        CREATE TABLE Teams (
            id INTEGER PRIMARY KEY,
            name VARCHAR(255) NOT NULL UNIQUE,
            CURR_POS INTEGER
        )
    """)

def main():
    conn, cursor = create_database()
    
    try:
        create_tables(cursor)
        conn.commit()
        print("Tables are created!")
    except sqlite3.Error as e:
        print(f"An error occurred: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    main()