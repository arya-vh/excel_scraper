My scraper is designed to automatically download, extract, validate, and process employee data from a ZIP file.

First, it downloads the ZIP file from a given URL. I ensure that the file is actually a valid ZIP and retry the download if there are network issues.

Once downloaded, the scraper extracts the ZIP file and checks if it contains the correct data file, like an Excel file. If there are multiple files, it intelligently selects the relevant one.

Then, it validates the file format to make sure it's not corrupted and is in a supported format like Excel. After validation, the scraper reads the file and extracts important employee details such as Employee ID, Name, Email, Job Title, Phone Number, and Hire Date.

I have also implemented proper error handling. If anything fails — like download issues, invalid ZIP files, or unsupported formats — the system logs the error and handles it gracefully instead of crashing.

Finally, I verify the entire process using multiple test cases to ensure reliability, including checking download success, extraction, file format validation, and data integrity.

First, I built the core logic in a file called pipeline_monitor.py.

This file acts as the engine of the system. It reads a cleaned employee CSV file and generates two outputs. The first output is a set of business metrics, such as total employee count, recent hiring activity, top job roles, and average employee tenure. These metrics give a quick, executive-friendly snapshot of the workforce and are stored in JSON format so they can be reused.

The second output is a data quality report. This checks whether important fields like email and phone number are present, identifies duplicate employee records, and validates email formats. If any columns are missing, the pipeline does not fail. Instead, it reports them as missing and still produces a quality score. This makes the pipeline resilient and suitable for real-world data.

After building the pipeline monitor, I added app.py, which serves as the API layer. The app file does not perform data processing itself. Instead, it imports the pipeline monitor and exposes its functionality through a FastAPI service. One endpoint returns the employee metrics, and another endpoint returns the data quality report. This allows the same logic to be accessed through HTTP requests instead of running Python scripts manually.

Commands to run:
python scraper.py         
python test.py 
python pipeline_monitor.py 
python api.py       
python -m uvicorn api:app --host 127.0.0.1 --port 8000   
Invoke-RestMethod -Uri http://127.0.0.1:8000/metrics      
>> Invoke-RestMethod -Uri http://127.0.0.1:8000/quality
http://127.0.0.1:8000/docs — Interactive Swagger UI
