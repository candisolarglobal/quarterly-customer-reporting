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
