import logging
from flask import Flask, render_template, request, jsonify, make_response, send_file
import pymssql
import sqlalchemy as sa
import ast
import pandas as pd
from flask_cors import CORS
import vertexai
from vertexai.preview.language_models import TextGenerationModel
from vertexai.preview.generative_models import GenerativeModel, Part
import os
import warnings
warnings.filterwarnings('ignore')

# Configure logging
# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logging.basicConfig(
    filename='app.log',  # Specify the file name
    level=logging.INFO,   # Set the log level
    format='%(asctime)s - %(levelname)s - %(message)s',  # Set the log format
    filemode='a'  # 'a' for append mode; 'w' for write mode to overwrite
)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app, origins="*", allowed_headers=["Content-Type", "Authorization"], methods=["GET", "POST", "OPTIONS"])


def claim_notes_fun(claim_no):
    logger.info("Fetching claim notes for claim_no: %s", claim_no)
    try:
        con = pymssql.connect(user = 'user' , password = 'pass', host = 'ip', database = 'mis_db', appname = 'app', autocommit = True)
        cur = con.cursor()
        
        query = f"""
            """
        query2 = f"""

            """
        
        notes = pd.read_sql(query, con)
        era = pd.read_sql(query2, con)
        era['Combined_Column'] = 'ERA POSTED INDICATING ' + era['Combined_Column']

        notes_query = f"""

            """
        
        summary_notes = pd.read_sql(notes_query, con)
        cur.close()
        con.close()

        fina_notes = pd.concat([notes, era])
        fina_notes_fin = fina_notes.fillna('')
        fina_not = fina_notes_fin.sort_values(by='Date', ascending=False).reset_index(drop=True)
        fina_not['Date'] = fina_not['Date'].apply(lambda x: x.strftime('%Y-%m-%d'))
        fina_not['Combined_Column'] = fina_not['Date'] + ' : ' + fina_not['Combined_Column']
        fina_not.drop(columns=['Date'], inplace=True)

        claim_notes = ' '.join(fina_not['Combined_Column'])
        notes['Date'] = pd.to_datetime(notes['Date'])
        max_date = notes['Date'].max()

        logger.info("Claim notes fetched successfully for claim_no: %s", claim_no)
        return claim_notes, max_date, summary_notes

    except Exception as e:
        logger.error("Error fetching claim notes for claim_no %s: %s", claim_no, e)
        raise


def note_summary(claim_notes):
    logger.info("Sending Request to Gemini Model for Summary")
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = ""
    vertexai.init(project="", location="")
    gemini_model = GenerativeModel("gemini-1.5-pro-001")

    prompt = f"""
       
        You are an experienced and detail-oriented summarizer of lengthy texts. You will receive detailed notes about a
            claim's history, which include information on the work done to secure payment from the insurance company. 
            Various users have been tracking the claim over time and adding comments about its status. Your task is to create
            a concise summary of all these comments, outlining the work completed and the current status of the claim.
            Create summary in points ranging between 1-15 and make sure that points should not be more than 15. e.g.
            - first comment here
            - Secod comment here
            - so on...............
            -
            -
            -
            -
            - last comment here
            and don't include ** in the summary

           
        Input: {claim_notes}
                Please generate a concise summary of the following claim notes, including the payment amount, adjustment amount, 
                approved amount, and any fax or appeal status along with the date. Also, include any copay, coinsurance, or patient responsibility 
                amounts if applicable.

                Note that the term "Amt. Adjusted" refers to an adjustment, not a payment. If the claim is sent in 837 format, 
                indicate that it was submitted electronically, avoiding the mention of both terms.

                The date format should be 'mm/dd/yyyy'.

                
                Please avoid using ** in your response, and include dates without placing them at the beginning. One who is calling to Insurance
                or following up on claim is an Agent and Provider is a Doctor.
                Add multiple comments in one row that are similar and should be chronological i.e. earlier dates first then the latest dates
                if there are more than one dates in a single point e.g An appeal was faxed to the insurance on 04/28/2023,
                for the late filing and appeal response was received on 05/11/2023 and agent contacted insurance and received response as no claim was on file on 06/12/2023.

                Indicate the full payment only if paid amount is equal to approved amount and if there is 
                "Status: Paidup to Billed" and "Blank to Paidup" in the comments it doesn't mean that claim is paid it mean's it is again billed to Insurance.
                Mention claim was paid or payment received only if there is some paid amount mentioned in the comments. Also if it is mentioned that ERA POSTED
                then it is not necessary that claim is paid, it can b a denial as well.
                
                After generating above summary generate another concise summary in two or three sentences which includes overall summary including important dates,
                denials, paymetns, adjustemntsa information and include this summary in Claim Status. 
                If the latest status is ERA POSTED with some reason then mention the reason along with date in Claim Status.
                Make sure to follow chronological order in a single point as well like earlier dates first then later dates for example instead of writing 
                '''An appeal was submitted on 07/20/2023. The agent called on 07/17/2023 to follow up on the appeal''', it should be like this 
                '''The agent called on 07/17/2023 to follow up on the appeal and then An appeal was submitted on 07/20/2023.'''.
                Then give me final output like this:

                Claim Status:
                    Includes summary of above summary and it should be in one or two sentence and not more than that..... 
                AI Generated Claim Notes:
<<<<<<< HEAD
                    includes initial claims summary....

                Note: Notes can be very lengthy and short as weel you have to generate summary and 
                Only give me output in above format that i mentioned and don't include any additional comments. 

=======
                    includes initial claims summary.... 
                    
                Note: Claim Notes can be very lengthy and short as well you have to generate summary in each scenario and 
                Only give me output in above format that i mentioned and don't include any additional comments.
                
>>>>>>> Logs implemented
        Output:  
    """

    try:
<<<<<<< HEAD
        engine = sa.create_engine('mssql+pymssql://user:pass@ip/db_name')
=======
        response = gemini_model.generate_content(
            prompt,
            generation_config={
                "max_output_tokens": 8192,
                "temperature": 0.2,
                "top_p": 0.95,
            },
        )
        res = response.text
        logger.info("Response Received from Gemini Model")
        return res

>>>>>>> Logs implemented
    except Exception as e:
        logger.error("Error generating summary: %s", e)
        raise


def db_dump(fin_df):
    logger.info("Dumping data to database")
    try:
        engine = sa.create_engine('mssql+pymssql://user:pass@ip/db_name')
        with engine.begin() as conn:
            fin_df.to_sql('table_name', conn, if_exists='append', index=False)
            logger.info("Total Data Dumped: %d", fin_df.shape[0])

    except Exception as e:
        logger.error("Error in data insertion: %s", e)
        raise


# Function to read log file
def read_log_file(log_file_path):
    """Read the contents of the log file and return it as a string."""
    try:
        with open(log_file_path, 'r') as file:
            log_contents = file.readlines()
        return ''.join(log_contents)
    except Exception as e:
        logger.error("Error reading log file: %s", e)
        return None

# Endpoint to get logs
@app.route('/logs', methods=['GET'])
def get_logs():
    log_file_path = os.path.join(os.getcwd(), 'app.log') 
    log_contents = read_log_file(log_file_path)
    
    if log_contents is not None:
        return jsonify({"status": "success", "logs": log_contents}), 200
    else:
        return jsonify({"status": "error", "message": "Could not read log file."}), 500

@app.route('/download_logs', methods=['GET'])
def download_log():
    log_file_path = os.path.join(os.getcwd(), 'app.log') 
    try:
        return send_file(log_file_path, as_attachment=True)
    except Exception as e:
        logger.error("Error sending log file: %s", e)
        return jsonify({"status": "error", "message": "Could not send log file."}), 500

@app.route('/note_summary', methods=['POST'])
def get_key_route():
    claim_no = request.json.get('claim_no')
    logger.info("Received request for claim_no: %s", claim_no)


    else:
        claim_summary = note_summary(claim_notes)
        summary_list = [line.strip() for line in claim_summary.splitlines()]


        try:
            # Find the index for 'AI Generated Claim Notes:'
            start_index = summary_list.index('AI Generated Claim Notes:') + 1
            claim_status_text = summary_list[1].strip()  # Strip extra spaces
            ai_generated_notes = summary_list[start_index:]

            # Reverse the AI Generated Claim Notes
            reversed_notes = ai_generated_notes[::-1]

            # Strip whitespace and filter out empty strings
            cleaned_notes = [note.strip() for note in reversed_notes if note.strip()]

            # Create the final internal dictionary
            final_claim_summary = {
                'Claim Status': claim_status_text,
                'AI Generated Claim Notes': cleaned_notes
            }

        except ValueError:
            # Handle the case where 'AI Generated Claim Notes:' is not found
            final_claim_summary = {
                'Claim Status': summary_list[1].strip() if len(summary_list) > 1 else '',
                'AI Generated Claim Notes': []
            }

            
        fin_dict = {"CLAIM_NO":[claim_no], "CLAIM_NOTES_SUMMARY":[final_claim_summary]} 
        fin_df = pd.DataFrame(fin_dict)

        fin_df['CLAIM_NO'] = fin_df['CLAIM_NO'].astype(int)
        fin_df['CLAIM_NOTES_SUMMARY'] = fin_df['CLAIM_NOTES_SUMMARY'].astype(str)
        if (fin_df['CLAIM_NOTES_SUMMARY'] != "{'Claim Status': '', 'AI Generated Claim Notes': []}").any():
            print("calling data dumping function")
            db_dump(fin_df)
            return jsonify(final_claim_summary)
        else:
            return jsonify(final_claim_summary)

=======
    try:
        claim_notes, max_date, summary_notes = claim_notes_fun(claim_no)
        notes_max_date = max_date.date()
        
        if summary_notes.shape[0] > 0:
            summary_max_date = summary_notes.MAX_SUMMARY_DATE[0]

        if summary_notes.shape[0] > 0 and summary_max_date >= notes_max_date:
            fin_claim_summary = summary_notes['CLAIM_NOTES_SUMMARY'][0]
            final_claim_summary = ast.literal_eval(fin_claim_summary)
            logger.info("Returning existing summary from DB for claim_no: %s", claim_no)
            return jsonify(final_claim_summary)

        else:
            claim_summary = note_summary(claim_notes)
            summary_list = [line.strip() for line in claim_summary.splitlines()]

            try:
                start_index = summary_list.index('AI Generated Claim Notes:') + 1
                claim_status_text = summary_list[1].strip()
                ai_generated_notes = summary_list[start_index:]

                reversed_notes = ai_generated_notes[::-1]
                cleaned_notes = [note.strip() for note in reversed_notes if note.strip()]

                final_claim_summary = {
                    'Claim Status': claim_status_text,
                    'AI Generated Claim Notes': cleaned_notes
                }
                logger.info("Summary generation successful for claim no: %s", claim_no)

            except Exception as e:
                final_claim_summary = {
                    'Claim Status': summary_list[1].strip() if len(summary_list) > 1 else '',
                    'AI Generated Claim Notes': []
                }

                logger.error("Summary not generated correctly: %s", e)
                logger.info("Response from Model is: %s", claim_summary)

            fin_dict = {"CLAIM_NO": [claim_no], "CLAIM_NOTES_SUMMARY": [final_claim_summary]}
            fin_df = pd.DataFrame(fin_dict)
            # fin_df['CLAIM_NOTES_SUMMARY'] = fin_df['CLAIM_NOTES_SUMMARY'].apply(json.dumps)
            
            empty_summary_str = "{'Claim Status': '', 'AI Generated Claim Notes': []}"
            fin_df['CLAIM_NOTES_SUMMARY'] = fin_df['CLAIM_NOTES_SUMMARY'].astype(str)
            fin_df['CLAIM_NO'] = fin_df['CLAIM_NO'].astype(int)
            # Convert the CLAIM_NOTES_SUMMARY column to JSON strings
            # fin_df['CLAIM_NOTES_SUMMARY'] = fin_df['CLAIM_NOTES_SUMMARY'].apply(json.dumps)

            # Use the serialized version in the if condition
            if (fin_df['CLAIM_NOTES_SUMMARY'] != empty_summary_str).any():
            # if (fin_df['CLAIM_NOTES_SUMMARY'] != "{'Claim Status': '', 'AI Generated Claim Notes': []}").any():
            # if not fin_df['CLAIM_NOTES_SUMMARY'].apply(lambda x: x == {'Claim Status': '', 'AI Generated Claim Notes': []}).all():
                logger.info("Data dump initiated for claim_no: %s", claim_no)
                db_dump(fin_df)
                logger.info("Data dump complete for claim_no: %s", claim_no)
                return jsonify(final_claim_summary)
            else:
                return jsonify(final_claim_summary)

    except Exception as e:
        logger.error("Error processing request for claim_no %s: %s", claim_no, e)
        return jsonify({"status": "Failure", "message": str(e)}), 500
>>>>>>> Logs implemented


@app.route('/', methods=['GET'])
def service_status():
    try:
        logger.info("Service status check")
        response = jsonify({"status": "Success", "message": "Claim Notes Summary Service is working fine"})
        return make_response(response, 200)
    except Exception as e:
        logger.error("Service status check failed: %s", e)
        response = jsonify({"status": "Failure", "message": str(e)})
        return make_response(response, 400)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=9470, debug=True)
