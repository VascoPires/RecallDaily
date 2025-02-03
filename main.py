import logging
import logging.handlers
import sqlite3


#######

# Define the path to the SQLite database file
dbfile = 'KoboReader.sqlite'

#######

# Define simple functions
def isBlank (myString):
    return not (myString and myString.strip())


def isNotBlank (myString):
    return bool(myString and myString.strip())


## Setup logger. 
# Credit: https://github.com/patrickloeber/python-github-action-template/tree/main

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger_file_handler = logging.handlers.RotatingFileHandler(
    "status.log",
    maxBytes=1024 * 1024,
    backupCount=1,
    encoding="utf8",
)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger_file_handler.setFormatter(formatter)
logger.addHandler(logger_file_handler)


if __name__ == "__main__":

    # Create a SQL connection to the SQLite database
    con = sqlite3.connect(dbfile)
    cur = con.cursor()

    # Get the structure of the Bookmark table
    cur.execute("PRAGMA table_info('Bookmark');")
    columns = cur.fetchall()

    # Dynamically find the indexes of the 'Text' and 'Annotation' columns
    column_names = [col[1] for col in columns]
    text_index = column_names.index('Text')
    annotation_index = column_names.index('Annotation')

    # Fetch all rows and extract only the 'Text' and 'Annotation' columns
    cur.execute("SELECT * FROM Bookmark;")
    rows = cur.fetchall()

    # Create a new SQLite database to store the extracted highlights
    new_dbfile = 'extracted_highlights.db'
    new_con = sqlite3.connect(new_dbfile)
    new_cur = new_con.cursor()

    # Create a new table for storing the highlights
    new_cur.execute('''
    CREATE TABLE IF NOT EXISTS Highlights (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        Text TEXT,
        Annotation TEXT
    )''')

    # Write the relevant data to a file for further analysis
    output_file = 'extracted_highlights.txt'
    with open(output_file, 'w', encoding='utf-8') as file:
        for row in rows:
            text = row[text_index]
            annotation = row[annotation_index]

            # Checks for empty quotes:
            if isNotBlank(text):
                if isBlank(annotation):
                    file.write(f"Text: {text}\n\n")
                else:
                    file.write(f"Text: {text}\nAnnotation: {annotation}\n\n")

            # Insert the relevant data into the new table
            new_cur.execute(
                "INSERT INTO Highlights (Text, Annotation) VALUES (?, ?)",
                (text, annotation if isNotBlank(annotation) else None)
            )

    # Commit the changes and close the new database connection
    new_con.commit()
    new_con.close()

    logger.info(f"Extracted 'Text' and 'Annotation' written to new database '{new_dbfile}' and text file '{output_file}'")

    # Close the original connection
    con.close()
