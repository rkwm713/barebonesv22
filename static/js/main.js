document.addEventListener('DOMContentLoaded', () => {
    const uploadForm = document.getElementById('upload-form');
    const fileInput = document.getElementById('file-input');
    const fileDropArea = document.querySelector('.file-drop-area');
    const fileMsg = document.querySelector('.file-msg');
    const processButton = document.getElementById('process-button');
    const resultsSection = document.getElementById('results-section');
    const statusMessage = document.getElementById('status-message');
    const downloadLinks = document.getElementById('download-links');
    const loader = document.getElementById('loader');

    // Drag and Drop functionality
    fileDropArea.addEventListener('dragover', (event) => {
        event.preventDefault();
        fileDropArea.classList.add('dragover');
    });

    fileDropArea.addEventListener('dragleave', () => {
        fileDropArea.classList.remove('dragover');
    });

    fileDropArea.addEventListener('drop', (event) => {
        event.preventDefault();
        fileDropArea.classList.remove('dragover');
        const files = event.dataTransfer.files;
        if (files.length > 0) {
            fileInput.files = files;
            updateFileMsg(files[0].name);
        }
    });

    fileInput.addEventListener('change', () => {
        if (fileInput.files.length > 0) {
            updateFileMsg(fileInput.files[0].name);
        } else {
            updateFileMsg('Drag & drop your JSON file here, or click to select.');
        }
    });

    function updateFileMsg(message) {
        fileMsg.textContent = message;
    }

    // Form submission
    uploadForm.addEventListener('submit', async (event) => {
        event.preventDefault();

        if (!fileInput.files || fileInput.files.length === 0) {
            displayMessage('Please select a JSON file to process.', 'error');
            return;
        }

        const formData = new FormData();
        formData.append('file', fileInput.files[0]);

        showLoader(true);
        processButton.disabled = true;
        resultsSection.style.display = 'none';
        statusMessage.textContent = '';
        statusMessage.className = '';
        downloadLinks.innerHTML = '';

        try {
            const response = await fetch('/process', {
                method: 'POST',
                body: formData,
            });

            const result = await response.json();

            if (response.ok) {
                displayMessage(result.message || 'File processed successfully!', 'success');
                if (result.excel_url && result.excel_file) {
                    addDownloadLink(result.excel_file, result.excel_url, 'Excel Report');
                }
                if (result.log_url && result.log_file) {
                    addDownloadLink(result.log_file, result.log_url, 'Processing Log', true);
                }
                resultsSection.style.display = 'block';
            } else {
                let errorMsg = result.error || 'An unknown error occurred.';
                if (result.log_url && result.log_file) {
                     errorMsg += ` <a href="${result.log_url}" download="${result.log_file}" class="error-log-link">Download Log</a>`;
                }
                displayMessage(errorMsg, 'error', true); // Allow HTML in error message
                resultsSection.style.display = 'block';
            }
        } catch (error) {
            console.error('Error during processing:', error);
            displayMessage('An error occurred while communicating with the server.', 'error');
            resultsSection.style.display = 'block';
        } finally {
            showLoader(false);
            processButton.disabled = false;
            // Clear the file input for the next upload
            uploadForm.reset(); 
            updateFileMsg('Drag & drop your JSON file here, or click to select.');
        }
    });

    function displayMessage(message, type, allowHtml = false) {
        if(allowHtml) {
            statusMessage.innerHTML = message;
        } else {
            statusMessage.textContent = message;
        }
        statusMessage.className = type; // 'success' or 'error'
    }

    function addDownloadLink(filename, url, linkText, isLog = false) {
        const link = document.createElement('a');
        link.href = url;
        link.textContent = `Download ${linkText}`;
        link.download = filename; // Suggests filename to browser
        if(isLog) {
            link.classList.add('log-link');
        }
        downloadLinks.appendChild(link);
    }

    function showLoader(show) {
        loader.style.display = show ? 'flex' : 'none';
    }
});
