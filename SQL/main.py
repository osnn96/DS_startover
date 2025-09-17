from pathlib import Path
import sqlite3

def create_database():
    
    db_path = Path(__file__).parent / "drivers.db"

    if db_path.exists():
        db_path.unlink()
    
    conn = sqlite3.connect(str(db_path))
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

def insert_sample_data(cursor):

    drivers = [
        (1, 'Lewis Hamilton', 40, 7),
        (2, 'Max Verstappen', 31, 4),
        (3, 'Sebastian Vettel', 38, 4)
    ]
    
    teams = [
        (1, 'Mercedes', 1),
        (2, 'Red Bull Racing', 2),
        (3, 'Ferrari', 3)
    ]
    
    cursor.executemany("INSERT INTO Drivers VALUES (?, ?, ?, ?)", drivers)
    cursor.executemany("INSERT INTO Teams VALUES (?, ?, ?)", teams)

    print("Sample data inserted successfully!\n")

def query_data(cursor):

    # basic select query
    print("--------All Drivers:--------")
    cursor.execute("SELECT * FROM Drivers")
    rows = cursor.fetchall()
    for row in rows:
        print(row, end="\n")

    # where clause
    print("--------Selected Drivers:--------")
    cursor.execute("SELECT * FROM Drivers WHERE WDC > 4")
    rows = cursor.fetchall()
    for row in rows:
        print(row, end="\n")

    # order by clause
    print("--------Drivers Ordered by Age:--------")
    cursor.execute("SELECT * FROM Teams ORDER BY CURR_POS")
    rows = cursor.fetchall()   
    for row in rows:
        print(row, end="\n")

def sql_update_delete_insert_operations(conn, cursor):

    # insert operation
    cursor.execute("INSERT INTO Drivers (id, name, age, WDC) VALUES (?, ?, ?, ?)", 
                   (4, 'Charles Leclerc', 30, 0))
    print(f"Rows inserted: {cursor.rowcount}")
    conn.commit()

    # update operation
    cursor.execute("UPDATE Drivers SET age = age + 1 WHERE name = 'Lewis Hamilton'")
    print(f"Rows updated: {cursor.rowcount}")
    conn.commit()

    # delete operation
    cursor.execute("DELETE FROM Drivers WHERE id = 3")
    cursor.execute("UPDATE Drivers SET id = 3 WHERE name = 'Charles Leclerc'")
    print(f"Rows deleted: {cursor.rowcount}")
    conn.commit()

    # extra data entry
    cursor.execute("INSERT INTO Drivers (id, name, age, WDC) VALUES (?, ?, ?, ?)", 
                   (4, 'Lando Norris', 25, 0))
    print(f"Rows inserted: {cursor.rowcount}")
    conn.commit()


def perform_aggregation_operations(cursor):
    print("\n--------Aggregation Operations--------")
    
    # COUNT operation
    print("\n1. Count of all drivers:")
    cursor.execute("SELECT COUNT(*) as total_drivers FROM Drivers")
    result = cursor.fetchone()
    print(f"Total drivers: {result[0]}")
    
    # AVG (MEAN) operation
    print("\n2. Average age of drivers:")
    cursor.execute("SELECT AVG(age) as avg_age FROM Drivers")
    result = cursor.fetchone()
    print(f"Average age: {result[0]:.1f}")
    
    # MIN and MAX operations
    print("\n3. Min and Max ages:")
    cursor.execute("SELECT MIN(age) as min_age, MAX(age) as max_age FROM Drivers")
    result = cursor.fetchone()
    print(f"Youngest driver: {result[0]} years old")
    print(f"Oldest driver: {result[1]} years old")
    
    # SUM operation
    print("\n4. Total World Driver Championships:")
    cursor.execute("SELECT SUM(WDC) as total_championships FROM Drivers")
    result = cursor.fetchone()
    print(f"Total WDCs: {result[0]}")
    
    # GROUP BY operation
    print("\n5. Count of drivers by WDC count:")
    cursor.execute("""
        SELECT WDC, COUNT(*) as driver_count 
        FROM Drivers 
        GROUP BY WDC
        ORDER BY WDC DESC
    """)
    results = cursor.fetchall()
    for wdc, count in results:
        print(f"Drivers with {wdc} championships: {count}")


def main():
    conn, cursor = create_database()
    
    try:
        create_tables(cursor)
        insert_sample_data(cursor)
        query_data(cursor)
        sql_update_delete_insert_operations(conn, cursor)
        perform_aggregation_operations(cursor)
        conn.commit()
    
    except sqlite3.Error as e:
        print(f"An error occurred: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    main()