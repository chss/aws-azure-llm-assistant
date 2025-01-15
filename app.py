from fastapi import FastAPI, HTTPException, Request, Depends
from fastapi.responses import FileResponse
import psycopg2
import os
import tempfile
import logging

# Configure logging
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

app = FastAPI()

# Database connection details for AWS (example)
#aws_database_name = "nemsis"
#aws_host = "database-4.cfi2gcmskckg.us-east-2.rds.amazonaws.com
#aws_user = "postgres"
#aws_password = "nemsisdbpwd"

aws_database_name = "dvdrental"
aws_host = "database-1.cfi2gcmskckg.us-east-2.rds.amazonaws.com"
aws_user = "postgres"
aws_password = "postgresdbadmin"

def create_aws_connection():
    try:
        connection = psycopg2.connect(
            host=aws_host,
            user=aws_user,
            password=aws_password,
            database=aws_database_name
        )
        return connection
    except (Exception, psycopg2.Error) as error:
        logger.error(f"Error while connecting to PostgreSQL: {error}")
        raise HTTPException(status_code=500, detail="Error connecting to database")

def get_connection():
    return create_aws_connection()

@app.get("/sqlquery/")
async def sqlquery(sqlquery: str, request: Request):
    logger.debug(f"Received API call: {request.url}")
    try:
        connection = get_connection()
        with connection.cursor() as cursor:
            cursor.execute(sqlquery)
            
            # Handle queries that return results
            if cursor.description is not None:
                headers = [i[0] for i in cursor.description]
                results = cursor.fetchall()

                # Create a temporary file
                with tempfile.NamedTemporaryFile(delete=False, mode='w', suffix=".txt") as temp_file:
                    temp_file.write(" | ".join(headers) + "\n")
                    for row in results:
                        temp_file.write(" | ".join(str(item) for item in row) + "\n")
                    temp_file_path = temp_file.name

                logger.debug(f"Query executed successfully, results written to {temp_file_path}")
                # Return the file response
                response = FileResponse(path=temp_file_path, filename="output.txt", media_type='text/plain')
                return response
            
            # Handle non-SELECT queries
            else:
                connection.commit()
                logger.debug("Non-SELECT query executed successfully")
                return {"status": "Query executed successfully"}

    except (Exception, psycopg2.Error) as error:
        logger.error(f"Error executing query: {error}")
        raise HTTPException(status_code=500, detail="Error executing SQL query")

    finally:
        if connection:
            connection.close()
            logger.debug("Database connection closed")

@app.middleware("http")
async def remove_temp_file(request, call_next):
    logger.debug(f"Processing request: {request.url}")
    response = await call_next(request)
    if isinstance(response, FileResponse) and os.path.exists(response.path):
        try:
            os.remove(response.path)
            logger.debug(f"Temporary file {response.path} removed successfully")
        except Exception as e:
            logger.error(f"Error removing temp file: {e}")
    return response

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, timeout=120)
