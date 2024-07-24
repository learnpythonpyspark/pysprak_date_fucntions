import ast
import pandas as pd
from datetime import datetime
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

def process_dataframe(df):
    for column in df.columns:
        if df[column].dtype == 'object':
            if all(isinstance(item, dict) for item in df[column] if item is not None):
                expanded = pd.json_normalize(df[column].dropna(), errors='ignore')
                expanded.columns = [f"{column}.{col}" for col in expanded.columns]
                df = pd.concat([df.drop(column, axis=1), expanded], axis=1)
    return df

def generate_html_tables(data):
    html_content = ""
    for key, value in data.items():
        if isinstance(value, list) and len(value) > 0:
            df = pd.DataFrame(value)
            df = process_dataframe(df)
            html_content += f"<h3>{key} Table:</h3>"
            html_content += df.to_html(index=False, escape=False, na_rep='N/A')
            html_content += "<br><br>"
    return html_content

def send_email_report(fName, alert_data, env, smtp_config):
    load_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    data = ast.literal_eval(alert_data)
    
    tables_html = generate_html_tables(data)
    
    htmlBody = f"""
    <html>
    <head>
        <style>
            table {{
                border-collapse: collapse;
                width: 100%;
                margin-bottom: 20px;
            }}
            th, td {{
                border: 1px solid #ddd;
                padding: 8px;
                text-align: left;
            }}
            th {{
                background-color: #f2f2f2;
            }}
            h3 {{
                color: #333;
            }}
        </style>
    </head>
    <body>
        <p>Hi {fName},</p>
        <marquee><p>Alert: BASC (report)</p></marquee>
        <p style="color:red">
            Report Date: {load_date}<br>
            Environment: {env.upper()}
        </p>
        <div class="tables-container">{tables_html}</div>
        <p>
        This is an Automated mail sent by <strong>ABC OK  .   </strong>.
        </p>
    </body>
    </html>
    """
    
    disclaimer = """
    <p style="font-size: 12px; color: #666;">Disclaimer: This is an automated report. Please verify all information before taking any action.</p>
    """
    htmlBody += disclaimer

    msg = MIMEMultipart()
    msg['Subject'] = f"({env.upper()}) TG Health Monitor Report"
    msg['From'] = smtp_config['sender']
    msg['To'] = smtp_config['sender']

    msg.attach(MIMEText(htmlBody, 'html'))

    with smtplib.SMTP(smtp_config['smtp_server'], smtp_config['smtp_port']) as smtp:
        smtp.starttls()
        smtp.login(smtp_config['smtp_username'], smtp_config['smtp_password'])
        smtp.sendmail(
            smtp_config['sender'], 
            smtp_config['recipients'] + [smtp_config['additional_email']], 
            msg.as_string()
        )
    
    print('Email sent successfully!')

if __name__ == "__main__":
    fName = "asdfsadf"
    env = "asdf"
   abvfdf
    smtp_config = {
        'smtp_server': 'smtp.abc.com',
        'smtp_port': 1234,
        'smtp_username': 'zx@asdf.com',
        'smtp_password': 'asdf',
        'sender': 'zx@asdf.com',
        'recipients': ['zx@asdf.com'],
        'additional_email': '56@asdf.com'
    }

    send_email_report(fName, alert_data, env, smtp_config)
