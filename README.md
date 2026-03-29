# India Customer Report Dispatcher
Python code for automating the process of sending quarterly reports to clients.

 ---
**Background**

Reports are first generated on Google LookerStudio. Via a Python function, they are auto-downloaded into the folder Client Communication/Customer Reports.
Every quarter, they are to be sent to the customer via mail. This repo aims to provide a means to automate that by leveraging AWS functionality.

---
**AWS Functionality**

1.  ***Lambda*** - Used for implementing Python codes - for matching customer_name in PDFs to customer_name in Excel Database. Also included GDrive Authentication code for providing access to stored PDFs.
2.  ***S3*** - Think of it like a storage bucket. Stores the Excel Database and the PDFs to be sent.
3.  ***SES*** - Simple Email Service - The function used to send the emails
4.  ***EventBridge*** - Used to automate the sending process. It is like a cron job and can be pre-set.
5.  ***IAM*** - Identity and Access Management - Used for authentication purposes.

---
**General Process:**

1. Automatic download of customer reports from Google LookerStudio by using Python script.
2. The reports are downloaded into the folder with the naming standard: {QuarterReportYear} for example: Q1Report2026.
3. The Commercial team will update the email Database with verified (Y/N).
4. Post verification of the reports by the Commercial team, the reports are sent to the customers on a pre-set date and time.
5. The dispatch summary is visible to the Commercial team through an email sent to the Commercial team's inbox.
