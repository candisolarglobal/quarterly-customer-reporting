import boto3
import openpyxl
import io
import json
from datetime import datetime, timezone
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

ses = boto3.client('ses')

def get_current_quarter_and_year():
    """Derives the current quarter and year from today's date."""
    now = datetime.now(timezone.utc)
    year = now.year
    month = now.month
    quarter = f"Q{(month - 1) // 3 + 1}"
    return quarter, year

def get_gdrive_service(service_account_cred_string, scopes=['https://www.googleapis.com/auth/drive.readonly']):
    """Authenticates using a service account JSON string."""
    info = json.loads(service_account_cred_string)
    creds = service_account.Credentials.from_service_account_info(info, scopes=scopes)
    return build('drive', 'v3', credentials=creds)

def get_gdrive_folder_id_by_path(service, path, parent_id):
    """Finds the ID of the last folder in a path string."""
    parts = [p for p in path.split('/') if p]
    for part in parts:
        query = (
            f"name = '{part}' and "
            f"mimeType = 'application/vnd.google-apps.folder' and "
            f"'{parent_id}' in parents and "
            f"trashed = false"
        )
        results = service.files().list(
            q=query,
            spaces='drive',
            fields="files(id, name)",
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
            corpora='allDrives'
        ).execute()

        items = results.get('files', [])
        if not items:
            raise Exception(f"Folder '{part}' not found under parent ID '{parent_id}'")
        parent_id = items[0]['id']
    return parent_id

def list_gdrive_files_in_folder(service, folder_id):
    """Returns a list of file metadata dicts (id, name) for all files in a folder."""
    query = (
        f"'{folder_id}' in parents and "
        f"mimeType != 'application/vnd.google-apps.folder' and "
        f"trashed = false"
    )
    results = service.files().list(
        q=query,
        fields="files(id, name)",
        supportsAllDrives=True,
        includeItemsFromAllDrives=True,
        corpora='allDrives'
    ).execute()
    return results.get('files', [])

def download_file_content(service, file_id):
    """Downloads a file from Google Drive into memory and returns bytes."""
    request = service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    fh.seek(0)
    return fh.read()

def get_ssm_param(ssm_param_name, region_name="eu-central-1", WithDecryption=True):
    ssm_client = boto3.client("ssm", region_name=region_name)
    response = ssm_client.get_parameter(
        Name=ssm_param_name,
        WithDecryption=WithDecryption
    )
    return response["Parameter"]["Value"]

def send_report_email(to_address, from_address, filename, file_bytes, customer_name):
    """Sends an email via SES with the professional Asset Management body."""
    import email.mime.multipart as mp
    import email.mime.text as mt
    import email.mime.application as ma

    msg = mp.MIMEMultipart()
    msg['Subject'] = f"Your Quarterly Solar Performance & Environmental Impact Report"
    msg['From'] = from_address
    msg['To'] = to_address

    # This section was previously un-indented, causing the error
    body = f"""
    <html>
    <body>
        <p>Dear <b>{customer_name}</b> team,</p>
        
        <p>We are pleased to share the latest quarterly update regarding the <b>solar performance</b> 
        and <b>environmental impact</b> of the <b>{customer_name} project.</b></p>
        
        <p>The attached report comprehensively breaks down your system's performance for this quarter. 
        It highlights the significant strides we have made together in generating clean electricity, 
        reducing carbon emissions, and achieving measurable utility savings.</p>
        
        <p><b>What’s included in your report:</b></p>
        <ul>
            <li>Detailed solar generation and efficiency metrics.</li>
            <li>Environmental impact summaries (Carbon offset and tree-planting equivalents).</li>
            <li>A summary of ongoing operations and maintenance (O&M) activities.</li>
        </ul>
        
        <p>Please find the full document, <i>{filename}</i>, attached for your review.</p>
        
        <p>Thank you for your continued partnership and shared dedication to a sustainable future.</p>
        
        <p>Best regards,<br>
        <b>Commercial Asset Management Team</b><br>
       
        
        <br>
        <img src="https://images.squarespace-cdn.com/content/v1/609e468f7c3af8779451f4da/1622470879366-1L7GB9068KZ4XDMTVFFO/candi_B.png" alt="candi solar logo" width="200">
    </body>
    </html>
    """
    
    # CRITICAL: Changed 'plain' to 'html' so your formatting works
    msg.attach(mt.MIMEText(body, 'html'))
    
    attachment = ma.MIMEApplication(file_bytes, _subtype='pdf')
    attachment.add_header('Content-Disposition', 'attachment', filename=filename)
    msg.attach(attachment)

    ses.send_raw_email(
        Source=from_address,
        Destinations=[to_address],
        RawMessage={'Data': msg.as_bytes()}
    )

def send_summary_report(to_address, from_address, successes, fails, no_matches, folder_name):
    """Sends an internal execution summary to the sender email."""
    import email.mime.multipart as mp
    import email.mime.text as mt

    msg = mp.MIMEMultipart()
    msg['Subject'] = f"INTERNAL: Quarterly Dispatch Summary - {folder_name}"
    msg['From'] = from_address
    msg['To'] = to_address

    summary_body = f"Quarterly Report Dispatch Execution Summary\n"
    summary_body += f"Folder: {folder_name}\n"
    summary_body += "="*50 + "\n\n"

    summary_body += f"✅ SUCCESSFUL SENDS ({len(successes)}):\n"
    summary_body += ("\n".join(successes) if successes else "None") + "\n\n"

    summary_body += f"❌ FAILED SENDS ({len(fails)}):\n"
    summary_body += ("\n".join(fails) if fails else "None") + "\n\n"

    summary_body += f"⚠️ NO EXCEL MATCHES ({len(no_matches)}):\n"
    summary_body += ("\n".join(no_matches) if no_matches else "None") + "\n\n"

    msg.attach(mt.MIMEText(summary_body, 'plain'))
    
    ses.send_raw_email(
        Source=from_address,
        Destinations=[to_address],
        RawMessage={'Data': msg.as_bytes()}
    )

def lambda_handler(event, context):
    # --- 1. CONFIGURATION ---
    SENDER_EMAIL = "cam-in@candi.solar"
    EXCEL_FILENAME = "Customer IDS"

    GDRIVE_SERVICE_ACCOUNT_KEY = get_ssm_param("/general/AMGDriveAccountKey")
    gdrive_service = get_gdrive_service(GDRIVE_SERVICE_ACCOUNT_KEY)
    FOLDER_ID_CUST_REPORTS = "1h4N3hiPy9gKEv2fYYveaKbMrhzSv8oXo" 

    # --- 2. AUTO-DETECT QUARTER AND YEAR ---
    quarter, year = get_current_quarter_and_year()
    quarter_report_folder_name = f"{quarter}Report{year}"
    print(f"--- Starting Dispatch for: {quarter_report_folder_name} ---")

    # --- 3. RESOLVE QUARTER FOLDER ID ---
    try:
        folder_id_quarter = get_gdrive_folder_id_by_path(
            gdrive_service, quarter_report_folder_name, FOLDER_ID_CUST_REPORTS
        )
    except Exception as e:
        print(f"CRITICAL ERROR: Folder '{quarter_report_folder_name}' not found: {e}")
        return

    # --- 4. LIST FILES ---
    main_folder_files = list_gdrive_files_in_folder(gdrive_service, FOLDER_ID_CUST_REPORTS)
    quarter_folder_files = list_gdrive_files_in_folder(gdrive_service, folder_id_quarter)
    
    # --- 5. IDENTIFY EXCEL FILE ---
    excel_file_meta = next((f for f in main_folder_files if f['name'].lower() in [EXCEL_FILENAME.lower(), f"{EXCEL_FILENAME.lower()}.xlsx"]), None)

    if not excel_file_meta:
        print(f"ERROR: Could not find '{EXCEL_FILENAME}' in the MAIN folder.")
        return

    # --- 6. IDENTIFY PDF FILES ---
    pdf_files = [f for f in quarter_folder_files if f['name'].lower().endswith('.pdf')]
    print(f"Found {len(pdf_files)} PDF reports.")

    # --- 7. DOWNLOAD AND PARSE THE EXCEL FILE ---
    try:
        excel_bytes = download_file_content(gdrive_service, excel_file_meta['id'])
        wb = openpyxl.load_workbook(io.BytesIO(excel_bytes), data_only=True)
        sheet = wb["Data"]

        email_map = {}
        for row in sheet.iter_rows(min_row=2, values_only=True):
            # Column A (0): Project/Name | Column B (1): Email | Column C (2): Verified
            if row[0] and row[1]:
                proj_name = str(row[0]).strip()
                email_addr = str(row[1]).strip()
                
                # --- STRICT VERIFICATION CHECK ---
                # Checks for 'y', 'Y', 'yes', 'Yes', 'YES' etc.
                raw_verified = str(row[2]).strip().lower() if row[2] else ""
                is_verified = raw_verified in ['y', 'yes']
                
                email_map[proj_name.lower()] = {
                    "original_name": proj_name,
                    "email": email_addr,
                    "verified": is_verified
                }
        print(f"Loaded {len(email_map)} mapping entries from Excel.")
    except Exception as e:
        print(f"ERROR: Failed to process Excel: {e}")
        return

    # --- 8. MATCH EACH PDF AND TRACK RESULTS ---
    successful_sends = []
    failed_sends = []
    no_match_found = []
    skipped_unverified = []

    for pdf_file in pdf_files:
        pdf_name = pdf_file['name']
        pdf_stem = pdf_name.rsplit('.', 1)[0]
        # Split by underscore and take the first part as the customer name
        customer_name_from_pdf = pdf_stem.split('_')[0].strip().lower()

        if customer_name_from_pdf in email_map:
            customer_data = email_map[customer_name_from_pdf]
            
            # --- THE GUARD: ONLY SEND IF VERIFIED ---
            if not customer_data["verified"]:
                log_entry = f"SKIPPED (Unverified): '{pdf_name}' for customer '{customer_data['original_name']}'"
                skipped_unverified.append(f"- {log_entry}")
                print(log_entry)
                continue

            # Verified! Proceed to download and send
            try:
                recipient_email = customer_data["email"]
                pdf_bytes = download_file_content(gdrive_service, pdf_file['id'])
                
                send_report_email(
                    to_address=recipient_email, 
                    from_address=SENDER_EMAIL, 
                    filename=pdf_name, 
                    file_bytes=pdf_bytes, 
                    customer_name=customer_data["original_name"]
                )
                
                log_entry = f"SUCCESS: '{pdf_name}' sent to {recipient_email}"
                successful_sends.append(f"- {log_entry}")
                print(log_entry)
            except Exception as e:
                log_entry = f"FAILED: '{pdf_name}' | Error: {e}"
                failed_sends.append(f"- {log_entry}")
                print(log_entry)
        else:
            log_entry = f"NO MATCH: '{customer_name_from_pdf}' (from file {pdf_name}) not found in Excel."
            no_match_found.append(f"- {log_entry}")
            print(log_entry)

    # --- 9. FINAL SUMMARY ---
    print("\n" + "="*50)
    print("JOB COMPLETED")
    print("="*50)

    try:
        # We combine 'unverified' and 'no match' into the summary report's "no_matches" section
        # or you can modify send_summary_report to have a 4th category.
        total_issues = no_match_found + skipped_unverified
        
        send_summary_report(
            to_address=SENDER_EMAIL,
            from_address=SENDER_EMAIL,
            successes=successful_sends,
            fails=failed_sends,
            no_matches=total_issues,
            folder_name=quarter_report_folder_name
        )
        print("Summary email sent.")
    except Exception as e:
        print(f"ERROR: Could not send summary email: {e}")

    return {
        'statusCode': 200,
        'body': json.dumps('Process Finished')
    }
