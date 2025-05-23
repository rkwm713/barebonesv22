import os
import datetime
from flask import Flask, request, render_template, send_from_directory, jsonify, url_for
from werkzeug.utils import secure_filename
import barebones  # Your existing script

app = Flask(__name__)

# Configuration
UPLOAD_FOLDER = 'uploads'
# DOWNLOAD_FOLDER is now determined by barebones.FileProcessor instance
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

# Ensure upload directory exists
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
# The barebones.FileProcessor will create its own downloads_path (e.g., /tmp or tmp_output)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/process', methods=['POST'])
def process_file():
    if 'file' not in request.files:
        return jsonify({'error': 'No file part'}), 400
    
    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': 'No selected file'}), 400

    if file and file.filename.endswith('.json'):
        filename = secure_filename(file.filename)
        job_json_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        file.save(job_json_path)

        processor = barebones.FileProcessor()
        
        # Modify barebones.py's download path to be relative to our app for serving files
        # This is a bit of a hack; ideally, barebones.py would allow configuring output dir
        # For now, we'll assume it outputs to its default Downloads folder,
        # and we'll construct paths based on that.
        
        # The process_files method in barebones.py generates versioned filenames
        # We need to capture these names.
        # Let's modify FileProcessor slightly or capture its output.
        # For now, let's assume process_files returns the paths or we can deduce them.

        # We need to know the exact output filenames.
        # barebones.py's process_files method creates versioned files.
        # Let's try to predict or capture them.
        json_base = os.path.splitext(filename)[0]
        
        # --- Simulating filename generation from barebones.py ---
        # This part is tricky because barebones.py handles its own versioning.
        # A robust solution would involve modifying barebones.py to return the generated paths.
        # For now, we'll try to find the latest generated files. # This comment is now outdated.
        
        # process_files now returns (success_bool, excel_path, log_path)
        processing_successful, excel_full_path, log_full_path = processor.process_files(job_json_path)

        if not processing_successful:
            error_response = {'error': 'Processing failed.'}
            if log_full_path and os.path.exists(log_full_path):
                log_filename = os.path.basename(log_full_path)
                log_url = url_for('download_file', filename=log_filename)
                error_response['message'] = 'Processing failed. Check log.'
                error_response['log_file'] = log_filename
                error_response['log_url'] = log_url
            elif os.path.exists(job_json_path): # Clean up uploaded JSON if processing fails early
                 try:
                    os.remove(job_json_path)
                 except OSError as e:
                    print(f"Error removing uploaded file after failure: {e}")
            return jsonify(error_response), 500

        if not excel_full_path or not os.path.exists(excel_full_path) or \
           not log_full_path or not os.path.exists(log_full_path):
            if os.path.exists(job_json_path): # Clean up
                 try:
                    os.remove(job_json_path)
                 except OSError as e:
                    print(f"Error removing uploaded file after partial success: {e}")
            return jsonify({'error': 'Output files not found after processing despite success signal.'}), 500
            
        excel_filename = os.path.basename(excel_full_path)
        log_filename = os.path.basename(log_full_path)

        excel_url = url_for('download_file', filename=excel_filename)
        log_url = url_for('download_file', filename=log_filename)
        
        # Clean up the uploaded JSON file after successful processing
        try:
            os.remove(job_json_path)
            print(f"Successfully removed uploaded file: {job_json_path}")
        except OSError as e:
            print(f"Error removing uploaded file: {e}")


        return jsonify({
            'message': 'File processed successfully',
            'excel_file': excel_filename,
            'log_file': log_filename,
            'excel_url': excel_url,
            'log_url': log_url
        })
    else:
        return jsonify({'error': 'Invalid file type. Please upload a JSON file.'}), 400

@app.route('/download/<path:filename>') # path: allows for slashes if filename includes subdirs (though not expected here)
def download_file(filename):
    # barebones.py now manages its own downloads_path (e.g., /tmp on Heroku or ./tmp_output locally)
    # We need an instance of FileProcessor to know this path.
    processor = barebones.FileProcessor()
    download_dir = processor.downloads_path 
    
    file_path = os.path.join(download_dir, filename)
    
    if not os.path.exists(file_path):
        # This could happen if the file was cleaned up or if the dyno restarted on Heroku
        print(f"Error: File not found for download: {file_path}")
        return jsonify({'error': f'File not found or no longer available: {filename}. Please try processing again.'}), 404
        
    # For Heroku /tmp, files might be cleaned up. Consider adding a cleanup task here too after send_file.
    # However, send_from_directory doesn't have an easy post-send hook.
    # A better approach for cleanup is a separate scheduled task or relying on Heroku's ephemeral nature.
    try:
        response = send_from_directory(download_dir, filename, as_attachment=True)
        # Potential cleanup after sending the file, though tricky with send_from_directory
        # For example, using @after_this_request (Flask feature)
        # @after_this_request
        # def remove_file(response):
        #     try:
        #         if os.path.exists(file_path):
        #             os.remove(file_path)
        #             print(f"Cleaned up {file_path}")
        #     except Exception as e:
        #         print(f"Error removing file {file_path}: {e}")
        #     return response
        return response
    except Exception as e:
        print(f"Error sending file {filename}: {e}")
        return jsonify({'error': f'Could not send file: {filename}'}), 500


if __name__ == '__main__':
    app.run(debug=True)
