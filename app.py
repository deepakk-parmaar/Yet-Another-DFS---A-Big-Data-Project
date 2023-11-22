from flask import Flask, render_template, request
import threading
import logging
from ns import NameNode
from ns import DataNode
from client import Client

app = Flask(__name__)

name_node = NameNode()
data_node = DataNode()
client = Client()

@app.route('/')
def index():
    try:
        # Fetch the list of files from the NameNode
        file_list = name_node.list_files()

        # Authenticate the client
        authentication_response = client.authenticate()

        return render_template('index.html', file_list=file_list, authentication_response=authentication_response)
    except Exception as e:
        logging.error(f"Error in index route: {str(e)}")
        return render_template('error.html', error_message="Internal Server Error")

@app.route('/upload', methods=['POST'])
def upload():
    try:
        uploaded_file = request.files['file']
        if uploaded_file:
            filename = uploaded_file.filename
            logging.info(f"Received file: {filename}")

            # Simulate sending a command to the NameNode to upload the file
            response = name_node.upload_file(filename)

            # Simulate sending a command to the DataNode to upload the file content
            with uploaded_file as file_content:
                response_data_node = data_node.upload_content(file_content.read(), filename)

        return render_template('index.html', message=response, message_data_node=response_data_node)
    except Exception as e:
        logging.error(f"Error in upload route: {str(e)}")
        return render_template('error.html', error_message="Internal Server Error")

@app.route('/download/<filename>')
def download(filename):
    try:
        # Implement file download logic here
        # You may need to retrieve the file from your distributed file system

        # Simulate sending a command to the NameNode to get file information
        file_info = name_node.get_file_info(filename)

        # Simulate sending a command to the DataNode to get the file content
        file_content = data_node.download_content(filename)

        # Return the file content for download
        return file_content
    except Exception as e:
        logging.error(f"Error in download route: {str(e)}")
        return render_template('error.html', error_message="Internal Server Error")

if __name__ == '__main__':
    # Configure logging
    logging.basicConfig(level=logging.INFO)

    try:
        # Start the Flask app
        threading.Thread(target=app.run, kwargs={'host': 'localhost', 'port': 5000}).start()

        # Start other components in separate threads
        threading.Thread(target=name_node.run).start()
        threading.Thread(target=data_node.run).start()
        threading.Thread(target=client.run).start()

        # Keep the main thread running
        while True:
            threading.Event().wait(1)
    except KeyboardInterrupt:
        logging.info("Exiting...")
