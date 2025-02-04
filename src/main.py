# Required Libs
import logging
import logging.handlers
import sqlite3
import os
import re
import smtplib
import ssl
import random
import csv
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.header import Header
from datetime import datetime 

from config import config

MAIL_USER = os.environ["MAIL_USER"]
MAIL_PASSWORD = os.environ["MAIL_PASSWORD"]
DAILY_REPORT_FROM = os.environ["DAILY_REPORT_FROM"]
DAILY_REPORT_TO = os.environ["DAILY_REPORT_TO"]

# Define simple functions
def isBlank(myString):
    return not (myString and myString.strip())

def isNotBlank(myString):
    return bool(myString and myString.strip())

def clean_text(text):
    if text is None:
        return None
    # Remove leading/trailing whitespace and unnecessary punctuation
    text = text.strip()
    # Remove leading numbers, dots, and spaces
    text = re.sub(r'^[\d\.\s]+', '', text)
    # Remove 'dogear' at the end if it exists
    text = re.sub(r'dogear$', '', text).strip()
    # Remove any remaining whitespace-only strings
    if isBlank(text):
        return None
    return text

def format_source(source):
    if source is None:
        return None
    # Remove the file path prefix and file extensions
    source = re.sub(r'^file:///mnt/onboard/', '', source)
    source = re.sub(r'\.kepub\.epub$|\.epub$', '', source)
    # Remove underscores and split into author and title
    source = source.replace('_', ' ')
    parts = source.split('/')
    if len(parts) > 1:
        author = parts[0]
        title = parts[1]
        source = f"{title}"
    return source

# Setup logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger_file_handler = logging.handlers.RotatingFileHandler(
    "status.log",
    maxBytes=1024 * 1024,
    backupCount=1,
    encoding="utf8",
)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger_file_handler.setFormatter(formatter)
logger.addHandler(logger_file_handler)

def extract_highlights(dbfile, new_dbfile, output_file):
    # Create a SQL connection to the SQLite database
    con = sqlite3.connect(dbfile)
    cur = con.cursor()

    # Get the structure of the Bookmark table
    cur.execute("PRAGMA table_info('Bookmark');")
    columns = cur.fetchall()

    # Dynamically find the indexes of the 'Text', 'Annotation', and 'VolumeID' columns
    column_names = [col[1] for col in columns]
    text_index = column_names.index('Text')
    annotation_index = column_names.index('Annotation')
    source_index = column_names.index('VolumeID')

    logger.debug(f"Column names: {column_names}")
    logger.debug(f"Text index: {text_index}, Annotation index: {annotation_index}, Source index: {source_index}")

    # Fetch all rows and extract the relevant columns
    cur.execute("SELECT * FROM Bookmark;")
    rows = cur.fetchall()

    if not rows:
        logger.warning("No rows found in the Bookmark table.")

    # Create a new SQLite database to store the extracted highlights
    new_con = sqlite3.connect(new_dbfile)
    new_cur = new_con.cursor()

    # Create a new table for storing the highlights if it doesn't exist
    new_cur.execute('''
    CREATE TABLE IF NOT EXISTS Highlights (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        Text TEXT,
        Annotation TEXT,
        Source TEXT
    )''')

    # Fetch existing highlights to avoid duplicates
    new_cur.execute("SELECT Text, Annotation, Source FROM Highlights;")
    existing_highlights = new_cur.fetchall()
    existing_highlights_set = set(existing_highlights)

    # Write the relevant data to a file for further analysis
    with open(output_file, 'w', encoding='utf-8') as file:
        for row in rows:
            text = row[text_index]
            annotation = row[annotation_index]
            source = row[source_index]

            if text is not None:
                text = clean_text(text)
            if annotation is not None:
                annotation = clean_text(annotation)
            if source is not None:
                source = format_source(source)

            if text is None:
                logger.warning(f"Skipping row with empty text: {row}")
                continue

            highlight_tuple = (text, annotation if isNotBlank(annotation) else None, source)
            if highlight_tuple in existing_highlights_set:
                logger.info(f"Skipping duplicate highlight: {highlight_tuple}")
                continue

            # Checks for empty quotes:
            if isNotBlank(text):
                if isBlank(annotation):
                    file.write(f"Text: {text}\nSource: {source}\n\n")
                else:
                    file.write(f"Text: {text}\nAnnotation: {annotation}\nSource: {source}\n\n")

            # Insert the relevant data into the new table
            new_cur.execute(
                "INSERT INTO Highlights (Text, Annotation, Source) VALUES (?, ?, ?)",
                highlight_tuple
            )

    # Commit the changes and close the new database connection
    new_con.commit()
    new_con.close()

    logger.info(f"Extracted 'Text', 'Annotation', and 'Source' written to new database '{new_dbfile}' and text file '{output_file}'")

    # Close the original connection
    con.close()

def extract_manual_quotes(file_path, new_db_file):
    # Create a new SQLite database connection to store the manual highlights
    new_con = sqlite3.connect(new_db_file)
    new_cur = new_con.cursor()

    # Create a new table for storing the manual highlights if it doesn't exist
    new_cur.execute('''
    CREATE TABLE IF NOT EXISTS Highlights (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        Text TEXT,
        Annotation TEXT,
        Source TEXT
    )''')

    # Fetch existing highlights to avoid duplicates
    new_cur.execute("SELECT Text, Annotation, Source FROM Highlights;")
    existing_highlights = new_cur.fetchall()
    existing_highlights_set = set(existing_highlights)

    with open(file_path, 'r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        for row in reader:
            text = clean_text(row['Text'])
            annotation = clean_text(row['Annotation']) if 'Annotation' in row else None
            source = clean_text(row['Source'])

            if text is not None and source is not None:
                highlight_tuple = (text, annotation if isNotBlank(annotation) else None, source)

                if highlight_tuple not in existing_highlights_set:
                    new_cur.execute(
                        "INSERT INTO Highlights (Text, Annotation, Source) VALUES (?, ?, ?)",
                        highlight_tuple
                    )

    # Commit the changes and close the new database connection
    new_con.commit()
    new_con.close()

    logger.info(f"Manual quotes from '{file_path}' added to the database '{new_db_file}'")

def merge_databases(kobo_dbfile, manual_dbfile, merged_dbfile):
    if os.path.exists(merged_dbfile):
        logger.info(f"The merged database '{merged_dbfile}' already exists. Skipping merge.")
        return

    # Create a new SQLite database connection for the merged database
    merged_con = sqlite3.connect(merged_dbfile)
    merged_cur = merged_con.cursor()

    # Create a new table for storing the merged highlights if it doesn't exist
    merged_cur.execute('''
    CREATE TABLE IF NOT EXISTS Highlights (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        Text TEXT,
        Annotation TEXT,
        Source TEXT
    )''')

    # Merge Kobo highlights
    kobo_con = sqlite3.connect(kobo_dbfile)
    kobo_cur = kobo_con.cursor()
    kobo_cur.execute("SELECT Text, Annotation, Source FROM Highlights;")
    kobo_highlights = kobo_cur.fetchall()
    for highlight in kobo_highlights:
        merged_cur.execute(
            "INSERT INTO Highlights (Text, Annotation, Source) VALUES (?, ?, ?)",
            highlight
        )
    kobo_con.close()

    # Merge manual highlights
    manual_con = sqlite3.connect(manual_dbfile)
    manual_cur = manual_con.cursor()
    manual_cur.execute("SELECT Text, Annotation, Source FROM Highlights;")
    manual_highlights = manual_cur.fetchall()
    for highlight in manual_highlights:
        merged_cur.execute(
            "INSERT INTO Highlights (Text, Annotation, Source) VALUES (?, ?, ?)",
            highlight
        )
    manual_con.close()

    # Commit the changes and close the merged database connection
    merged_con.commit()
    merged_con.close()

    logger.info(f"Merged highlights written to new database '{merged_dbfile}'")

def format_quote_html(text, annotation, source):
    if annotation:
        annotation_html = f"<p class='annotation'>Your notes: {annotation}</p>"
    else:
        annotation_html = ""
    
    return f"""
    <blockquote class="callout">
        <p>{text}</p>
        {annotation_html}
        <footer><strong>{source}</strong></footer>
    </blockquote>
    """

def select_random_quotes(dbfile, n):
    con = sqlite3.connect(dbfile)
    cur = con.cursor()
    cur.execute("SELECT Text, Annotation, Source FROM Highlights;")
    highlights = cur.fetchall()
    con.close()

    if len(highlights) < n:
        n = len(highlights)

    selected_quotes = random.sample(highlights, n)
    return selected_quotes

def load_html_template(template_path):
    with open(template_path, 'r', encoding='utf-8') as file:
        return file.read()

def send_email(subject, body):
    msg = MIMEMultipart()
    msg['From'] = DAILY_REPORT_FROM
    msg['To'] = DAILY_REPORT_TO
    msg['Subject'] = Header(subject, 'utf-8').encode()

    msg_content = MIMEText(body, 'html')
    msg.attach(msg_content)

    # Create secure connection with server and send email
    context = ssl.create_default_context()
    with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
        server.login(MAIL_USER, MAIL_PASSWORD)
        server.sendmail(DAILY_REPORT_FROM, DAILY_REPORT_TO, msg.as_string())

if __name__ == "__main__":
    if config["use_kobo_db"]:
        if os.path.exists(config["extracted_dbfile"]):
            logger.info(f"The database '{config['extracted_dbfile']}' already exists. Skipping extraction.")
        else:
            extract_highlights(config["kobo_dbfile"], config["extracted_dbfile"], 'extracted_highlights.txt')

    
    if config["use_manual_quotes"]:
        if os.path.exists(config["manual_dbfile"]):
            logger.info(f"The database '{config['manual_dbfile']}' already exists. Skipping manual quotes extraction.")
        else:
            extract_manual_quotes(config["manual_quotes_file"], config["manual_dbfile"])

    # Merge databases if both are in use, otherwise use the available one
    if config["use_kobo_db"] and config["use_manual_quotes"]:
        merge_databases(config["extracted_dbfile"], config["manual_dbfile"], config["merged_dbfile"])
        dbfile_to_use = config["merged_dbfile"]
    elif config["use_kobo_db"]:
        dbfile_to_use = config["extracted_dbfile"]
    elif config["use_manual_quotes"]:
        dbfile_to_use = config["manual_dbfile"]
    else:
        logger.error("No database is configured to be used.")
        raise ValueError("No database is configured to be used.")

    # Select random quotes
    selected_quotes = select_random_quotes(dbfile_to_use, config["num_quotes"])
    quotes_body = "".join([format_quote_html(quote[0], quote[1], quote[2]) for quote in selected_quotes])

    # Load HTML template
    html_template = load_html_template('html/email_template.html')

    # Insert dynamic content into the template
    email_body = html_template.replace("<!-- MAIN CONTENT -->", f"""
        <div class="header">
            <h1>Recall Daily</h1>
            <h2 class="date">{datetime.now().strftime('%d.%m.%Y')}</h2>
        </div>
        <div class="quotes">
            {quotes_body}
        </div>
    """)

    # Send daily report email
    email_subject = f"Your Recall Daily Report - {datetime.now().strftime('%d.%m.%Y')}"
    send_email(email_subject, email_body)

