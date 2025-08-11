// Enhanced JavaScript for Local Database Administration (LDA) application

// Global variables
let currentDatabase = null;
let uploadProgress = {};
let worksheetMappings = {};
let currentGlobalDatabase = null;
let databaseLoadPromise = null;

// Initialize application when DOM is loaded
document.addEventListener('DOMContentLoaded', function() {
    // Initialize core functionality
    initializeTooltips();
    initializeFileUpload();
    initializeSearch();
    initializeTheme();
    initializeDrawer(); // Initialize drawer navigation
    
    // Initialize enhanced functionality
    initializeEnhancedFeatures();
    initializeProgressTracking();
    initializeTableManagement();
    
    // Load page-specific functionality
    const currentPage = getCurrentPage();
    loadPageSpecificFeatures(currentPage);
    
    // Load global databases only if selector exists (on certain pages)
    if (document.getElementById('globalDatabaseSelect')) {
        loadGlobalDatabases();
    }
    
    // Add staggered animation to cards
    const cards = document.querySelectorAll('.card');
    cards.forEach((card, index) => {
        card.style.animationDelay = `${index * 0.1}s`;
        card.classList.add('animate-fadeInUp');
    });
});

// Initialize enhanced functionality
function initializeEnhancedFeatures() {
    // Initialize enhanced drag and drop
    initializeEnhancedDragDrop();
    
    // Initialize progress indicators
    initializeProgressIndicators();
    
    // Initialize advanced notifications
    initializeAdvancedNotifications();
    
    // Initialize keyboard shortcuts
    initializeKeyboardShortcuts();
}

// Initialize progress tracking
function initializeProgressTracking() {
    // Set up progress tracking for uploads
    const uploadForms = document.querySelectorAll('form[enctype="multipart/form-data"]');
    uploadForms.forEach(form => {
        form.addEventListener('submit', handleFormSubmissionWithProgress);
    });
}

// Initialize table management features
function initializeTableManagement() {
    // Initialize table-specific functionality
    initializeTableActions();
    initializeRecordManagement();
    initializeBulkOperations();
}

// Get current page identifier
function getCurrentPage() {
    const path = window.location.pathname;
    if (path.includes('enhanced_upload')) return 'enhanced_upload';
    if (path.includes('view_table')) return 'view_table';
    if (path.includes('tables')) return 'tables';
    if (path.includes('data_analysis')) return 'data_analysis';
    return 'default';
}

// Load page-specific features
function loadPageSpecificFeatures(page) {
    switch(page) {
        case 'enhanced_upload':
            loadEnhancedUploadFeatures();
            break;
        case 'view_table':
            loadTableViewFeatures();
            break;
        case 'tables':
            loadTablesPageFeatures();
            break;
        case 'data_analysis':
            loadDataAnalysisFeatures();
            break;
    }
}

// Initialize Bootstrap tooltips
function initializeTooltips() {
    const tooltipTriggerList = [].slice.call(document.querySelectorAll('[data-bs-toggle="tooltip"]'));
    const tooltipList = tooltipTriggerList.map(function (tooltipTriggerEl) {
        return new bootstrap.Tooltip(tooltipTriggerEl);
    });
}

// Enhanced Upload Page Functions
function loadEnhancedUploadFeatures() {
    // Load existing tables on page load
    if (typeof loadExistingTables === 'function') {
        loadExistingTables();
    }
    
    // Initialize file processing
    initializeFileProcessing();
    
    // Initialize worksheet mapping
    initializeWorksheetMapping();
}

function loadExistingTables() {
    const container = document.getElementById('existingTablesListContainer');
    if (!container) return;

    fetch('/api/tables')
        .then(response => response.json())
        .then(data => {
            if (data.success && data.tables.length > 0) {
                let tablesList = '<div class="row">';
                data.tables.forEach(table => {
                    tablesList += `
                        <div class="col-md-6 col-lg-4 mb-3">
                            <div class="card border-0 shadow-sm h-100">
                                <div class="card-body">
                                    <h6 class="card-title text-primary mb-1">
                                        <i class="bi bi-table"></i> ${table.name}
                                    </h6>
                                    <p class="card-text small text-muted mb-2">
                                        ${formatNumber(table.row_count)} records
                                    </p>
                                    <div class="d-flex gap-1">
                                        <a href="/view_table/${table.name}" class="btn btn-outline-primary btn-sm">
                                            <i class="bi bi-eye"></i> View
                                        </a>
                                        <button class="btn btn-outline-secondary btn-sm" 
                                                onclick="showTableInfo('${table.name}')">
                                            <i class="bi bi-info-circle"></i> Info
                                        </button>
                                    </div>
                                </div>
                            </div>
                        </div>
                    `;
                });
                tablesList += '</div>';
                container.innerHTML = tablesList;
            } else {
                container.innerHTML = `
                    <div class="text-center text-muted">
                        <i class="bi bi-database-x fs-1"></i>
                        <p class="mt-2">No tables found in the current database</p>
                        <p class="small">Upload an Excel file to create your first table</p>
                    </div>
                `;
            }
        })
        .catch(error => {
            console.error('Error loading tables:', error);
            container.innerHTML = `
                <div class="alert alert-warning">
                    <i class="bi bi-exclamation-triangle"></i>
                    Unable to load existing tables. Please refresh the page.
                </div>
            `;
        });
}

function setTargetTable(sheetName, tableName) {
    if (!worksheetMappings[sheetName]) {
        worksheetMappings[sheetName] = {};
    }
    worksheetMappings[sheetName].targetTable = tableName;
    
    // Update UI to show selection
    const targetElement = document.querySelector(`#target_mapping_${sheetName.replace(/[^a-zA-Z0-9]/g, '_')} select`);
    if (targetElement) {
        targetElement.value = tableName;
    }
    
    // Show confirmation
    showNotification(`Target table set to "${tableName}" for worksheet "${sheetName}"`, 'success', 3000);
}

// Table View Page Functions
function loadTableViewFeatures() {
    // Initialize table management
    initializeTableSelections();
    initializeRecordActions();
}

function toggleAllCheckboxes() {
    const selectAllCheckbox = document.getElementById('selectAll');
    const rowCheckboxes = document.querySelectorAll('input[name="record_ids"]');
    
    if (selectAllCheckbox && rowCheckboxes.length > 0) {
        const isChecked = selectAllCheckbox.checked;
        rowCheckboxes.forEach(checkbox => {
            checkbox.checked = isChecked;
        });
        updateDeleteButton();
    }
}

function updateDeleteButton() {
    const checkedBoxes = document.querySelectorAll('input[name="record_ids"]:checked');
    const deleteBtn = document.getElementById('deleteSelectedBtn');
    
    if (deleteBtn) {
        deleteBtn.disabled = checkedBoxes.length === 0;
        deleteBtn.textContent = checkedBoxes.length > 0 
            ? `Delete Selected (${checkedBoxes.length})` 
            : 'Delete Selected';
    }
}

function deleteSelectedRecords() {
    const checkedBoxes = document.querySelectorAll('input[name="record_ids"]:checked');
    if (checkedBoxes.length === 0) return;
    
    const recordIds = Array.from(checkedBoxes).map(cb => cb.value);
    const tableName = new URLSearchParams(window.location.search).get('table') || 
                     window.location.pathname.split('/').pop();
    
    if (confirm(`Are you sure you want to delete ${recordIds.length} selected records?`)) {
        showLoading('deleteSelectedBtn', 'Deleting...');
        
        fetch(`/api/table/${tableName}/records/bulk_delete`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({ record_ids: recordIds })
        })
        .then(response => response.json())
        .then(data => {
            if (data.success) {
                showNotification(`Successfully deleted ${recordIds.length} records`, 'success');
                location.reload();
            } else {
                showNotification('Error deleting records: ' + (data.error || 'Unknown error'), 'danger');
            }
        })
        .catch(error => {
            console.error('Error:', error);
            showNotification('Error deleting records', 'danger');
        });
    }
}

function editRecord(recordId) {
    const tableName = new URLSearchParams(window.location.search).get('table') || 
                     window.location.pathname.split('/').pop();
    
    // Show loading in modal
    const modal = new bootstrap.Modal(document.getElementById('editModal'));
    const modalBody = document.querySelector('#editModal .modal-body');
    
    modalBody.innerHTML = `
        <div class="text-center">
            <div class="spinner-border" role="status">
                <span class="visually-hidden">Loading...</span>
            </div>
            <p class="mt-2">Loading record data...</p>
        </div>
    `;
    
    modal.show();
    
    // Fetch record data
    fetch(`/api/table/${tableName}/record/${recordId}`)
        .then(response => response.json())
        .then(data => {
            if (data.success) {
                let formHTML = '<form id="editForm">';
                
                Object.entries(data.record).forEach(([key, value]) => {
                    if (key !== 'id') {
                        formHTML += `
                            <div class="mb-3">
                                <label for="field_${key}" class="form-label">${key}</label>
                                <input type="text" class="form-control" id="field_${key}" 
                                       name="${key}" value="${value || ''}">
                            </div>
                        `;
                    }
                });
                
                formHTML += `
                    </form>
                    <div class="modal-footer">
                        <button type="button" class="btn btn-secondary" data-bs-dismiss="modal">Cancel</button>
                        <button type="button" class="btn btn-primary" onclick="saveRecord(${recordId})">Save Changes</button>
                    </div>
                `;
                
                modalBody.innerHTML = formHTML;
            } else {
                modalBody.innerHTML = `
                    <div class="alert alert-danger">
                        Error loading record: ${data.error || 'Unknown error'}
                    </div>
                `;
            }
        })
        .catch(error => {
            console.error('Error:', error);
            modalBody.innerHTML = `
                <div class="alert alert-danger">
                    Error loading record data
                </div>
            `;
        });
}

function saveRecord(recordId) {
    const form = document.getElementById('editForm');
    const formData = new FormData(form);
    const data = Object.fromEntries(formData.entries());
    
    const tableName = new URLSearchParams(window.location.search).get('table') || 
                     window.location.pathname.split('/').pop();
    
    fetch(`/api/table/${tableName}/record/${recordId}`, {
        method: 'PUT',
        headers: {
            'Content-Type': 'application/json',
        },
        body: JSON.stringify(data)
    })
    .then(response => response.json())
    .then(result => {
        if (result.success) {
            showNotification('Record updated successfully', 'success');
            location.reload();
        } else {
            showNotification('Error updating record: ' + (result.error || 'Unknown error'), 'danger');
        }
    })
    .catch(error => {
        console.error('Error:', error);
        showNotification('Error updating record', 'danger');
    });
}

function deleteRecord(recordId) {
    const tableName = new URLSearchParams(window.location.search).get('table') || 
                     window.location.pathname.split('/').pop();
    
    if (confirm('Are you sure you want to delete this record?')) {
        fetch(`/api/table/${tableName}/record/${recordId}`, {
            method: 'DELETE'
        })
        .then(response => response.json())
        .then(data => {
            if (data.success) {
                showNotification('Record deleted successfully', 'success');
                location.reload();
            } else {
                showNotification('Error deleting record: ' + (data.error || 'Unknown error'), 'danger');
            }
        })
        .catch(error => {
            console.error('Error:', error);
            showNotification('Error deleting record', 'danger');
        });
    }
}
// Initialize file upload functionality
function initializeFileUpload() {
    const fileInput = document.getElementById('file');
    const uploadForm = document.getElementById('uploadForm') || document.getElementById('enhancedUploadForm');
    
    if (!fileInput || !uploadForm) return;
    
    // Add enhanced drag and drop functionality
    initializeEnhancedDragDrop(uploadForm, fileInput);
    
    // Update file input label
    fileInput.addEventListener('change', function() {
        if (this.files.length > 0) {
            updateFileInputLabel(this.files[0].name);
            
            // Auto-preview if on enhanced upload page
            if (document.getElementById('previewCard')) {
                previewFile(this.files[0]);
            }
        }
    });
}

// Enhanced drag and drop functionality
function initializeEnhancedDragDrop(uploadForm, fileInput) {
    if (!uploadForm) return;
    
    const dropZone = uploadForm;
    
    ['dragenter', 'dragover', 'dragleave', 'drop'].forEach(eventName => {
        dropZone.addEventListener(eventName, preventDefaults, false);
        document.body.addEventListener(eventName, preventDefaults, false);
    });
    
    function preventDefaults(e) {
        e.preventDefault();
        e.stopPropagation();
    }
    
    ['dragenter', 'dragover'].forEach(eventName => {
        dropZone.addEventListener(eventName, highlight, false);
    });
    
    ['dragleave', 'drop'].forEach(eventName => {
        dropZone.addEventListener(eventName, unhighlight, false);
    });
    
    function highlight(e) {
        dropZone.classList.add('border-primary', 'border-3', 'bg-light');
        dropZone.style.borderStyle = 'dashed';
    }
    
    function unhighlight(e) {
        dropZone.classList.remove('border-primary', 'border-3', 'bg-light');
        dropZone.style.borderStyle = 'solid';
    }
    
    dropZone.addEventListener('drop', handleDrop, false);
    
    function handleDrop(e) {
        const dt = e.dataTransfer;
        const files = dt.files;
        
        if (files.length > 0) {
            const file = files[0];
            
            // Validate file type
            const allowedTypes = ['.xlsx', '.xls', '.xlsm'];
            const fileExtension = '.' + file.name.split('.').pop().toLowerCase();
            
            if (!allowedTypes.includes(fileExtension)) {
                showNotification('Please select a valid Excel file (.xlsx, .xls, .xlsm)', 'danger');
                return;
            }
            
            // Validate file size (256MB limit)
            const maxSize = 256 * 1024 * 1024; // 256MB in bytes
            if (file.size > maxSize) {
                showNotification('File size exceeds 256MB limit', 'danger');
                return;
            }
            
            fileInput.files = files;
            updateFileInputLabel(file.name);
            
            // Show file info
            showFileInfo(file);
            
            // Auto-preview if on enhanced upload page
            if (document.getElementById('previewCard')) {
                previewFile(file);
            }
        }
    }
}

function updateFileInputLabel(fileName) {
    const label = document.querySelector('label[for="file"]');
    if (label) {
        label.innerHTML = `<i class="bi bi-file-earmark-excel"></i> Selected: ${fileName}`;
        label.classList.add('text-success');
    }
}

function showFileInfo(file) {
    const container = document.getElementById('fileInfoContainer');
    if (container) {
        container.innerHTML = `
            <div class="alert alert-info">
                <h6><i class="bi bi-info-circle"></i> File Information</h6>
                <ul class="mb-0">
                    <li><strong>Name:</strong> ${file.name}</li>
                    <li><strong>Size:</strong> ${formatFileSize(file.size)}</li>
                    <li><strong>Type:</strong> ${file.type || 'Excel file'}</li>
                    <li><strong>Last Modified:</strong> ${new Date(file.lastModified).toLocaleString()}</li>
                </ul>
            </div>
        `;
    }
}

// File preview functionality
function previewFile(file) {
    const previewCard = document.getElementById('previewCard');
    const previewContainer = document.getElementById('previewContainer');
    
    if (!previewCard || !previewContainer) return;
    
    previewCard.style.display = 'block';
    previewContainer.innerHTML = `
        <div class="text-center">
            <div class="spinner-border" role="status">
                <span class="visually-hidden">Analyzing file...</span>
            </div>
            <p class="mt-2">Analyzing Excel file structure...</p>
        </div>
    `;
    
    // Create FormData for file upload
    const formData = new FormData();
    formData.append('file', file);
    
    fetch('/api/preview_file', {
        method: 'POST',
        body: formData
    })
    .then(response => response.json())
    .then(data => {
        if (data.success) {
            displayFilePreview(data);
        } else {
            previewContainer.innerHTML = `
                <div class="alert alert-danger">
                    <i class="bi bi-exclamation-triangle"></i>
                    Error analyzing file: ${data.error || 'Unknown error'}
                </div>
            `;
        }
    })
    .catch(error => {
        console.error('Error:', error);
        previewContainer.innerHTML = `
            <div class="alert alert-danger">
                <i class="bi bi-exclamation-triangle"></i>
                Error analyzing file. Please try again.
            </div>
        `;
    });
}

function displayFilePreview(data) {
    const previewContainer = document.getElementById('previewContainer');
    if (!previewContainer) return;
    
    let previewHTML = `
        <div class="row">
            <div class="col-md-6">
                <h6><i class="bi bi-file-earmark-spreadsheet"></i> File Summary</h6>
                <ul class="list-unstyled">
                    <li><strong>Worksheets:</strong> ${data.worksheets ? data.worksheets.length : 0}</li>
                    <li><strong>Total Rows:</strong> ${formatNumber(data.total_rows || 0)}</li>
                    <li><strong>Estimated Size:</strong> ${formatFileSize(data.estimated_size || 0)}</li>
                </ul>
            </div>
            <div class="col-md-6">
                <h6><i class="bi bi-list-task"></i> Processing Options</h6>
                <div class="form-check">
                    <input class="form-check-input" type="checkbox" id="skipEmptyRows" checked>
                    <label class="form-check-label" for="skipEmptyRows">
                        Skip empty rows
                    </label>
                </div>
                <div class="form-check">
                    <input class="form-check-input" type="checkbox" id="inferDataTypes" checked>
                    <label class="form-check-label" for="inferDataTypes">
                        Auto-detect data types
                    </label>
                </div>
            </div>
        </div>
    `;
    
    if (data.worksheets && data.worksheets.length > 0) {
        previewHTML += `
            <div class="mt-4">
                <h6><i class="bi bi-collection"></i> Worksheets Preview</h6>
                <div class="accordion" id="worksheetAccordion">
        `;
        
        data.worksheets.forEach((worksheet, index) => {
            previewHTML += `
                <div class="accordion-item">
                    <h2 class="accordion-header" id="heading${index}">
                        <button class="accordion-button ${index > 0 ? 'collapsed' : ''}" 
                                type="button" data-bs-toggle="collapse" 
                                data-bs-target="#collapse${index}">
                            ${worksheet.name} 
                            <span class="badge bg-secondary ms-2">${formatNumber(worksheet.rows)} rows</span>
                        </button>
                    </h2>
                    <div id="collapse${index}" 
                         class="accordion-collapse collapse ${index === 0 ? 'show' : ''}" 
                         data-bs-parent="#worksheetAccordion">
                        <div class="accordion-body">
                            <div class="table-responsive">
                                <table class="table table-sm table-striped">
                                    <thead>
                                        <tr>
                                            ${worksheet.columns.map(col => `<th class="small">${col}</th>`).join('')}
                                        </tr>
                                    </thead>
                                    <tbody>
                                        ${worksheet.preview.map(row => 
                                            `<tr>${row.map(cell => `<td class="small">${cell || ''}</td>`).join('')}</tr>`
                                        ).join('')}
                                    </tbody>
                                </table>
                            </div>
                            <div class="mt-2">
                                <small class="text-muted">
                                    Showing first ${worksheet.preview.length} rows of ${formatNumber(worksheet.rows)} total
                                </small>
                            </div>
                        </div>
                    </div>
                </div>
            `;
        });
        
        previewHTML += `
                </div>
            </div>
        `;
    }
    
    previewContainer.innerHTML = previewHTML;
    
    // Show configuration section
    const configCard = document.getElementById('configCard');
    if (configCard) {
        configCard.style.display = 'block';
        showWorksheetConfig(data.worksheets);
    }
}

// Worksheet configuration
function showWorksheetConfig(worksheets) {
    const configContainer = document.getElementById('worksheetConfigContainer');
    if (!configContainer || !worksheets) return;
    
    let configHTML = '';
    
    worksheets.forEach(worksheet => {
        const sheetId = worksheet.name.replace(/[^a-zA-Z0-9]/g, '_');
        configHTML += `
            <div class="card mb-3" id="config_${sheetId}">
                <div class="card-header">
                    <div class="row align-items-center">
                        <div class="col">
                            <h6 class="mb-0">
                                <i class="bi bi-file-earmark-spreadsheet"></i> ${worksheet.name}
                                <span class="badge bg-info ms-2">${formatNumber(worksheet.rows)} rows</span>
                            </h6>
                        </div>
                        <div class="col-auto">
                            <div class="form-check form-switch">
                                <input class="form-check-input" type="checkbox" 
                                       id="include_${sheetId}" checked
                                       onchange="toggleWorksheet('${worksheet.name}', this.checked)">
                                <label class="form-check-label" for="include_${sheetId}">
                                    Include in import
                                </label>
                            </div>
                        </div>
                    </div>
                </div>
                <div class="card-body">
                    <div class="row">
                        <div class="col-md-6">
                            <label class="form-label small fw-bold">Import Mode</label>
                            <div class="btn-group w-100" role="group">
                                <input type="radio" class="btn-check" name="mode_${sheetId}" 
                                       id="new_${sheetId}" value="new" checked
                                       onchange="setImportMode('${worksheet.name}', 'new')">
                                <label class="btn btn-outline-success btn-sm" for="new_${sheetId}">
                                    <i class="bi bi-plus-circle"></i> New Table
                                </label>
                                
                                <input type="radio" class="btn-check" name="mode_${sheetId}" 
                                       id="append_${sheetId}" value="append"
                                       onchange="setImportMode('${worksheet.name}', 'append')">
                                <label class="btn btn-outline-primary btn-sm" for="append_${sheetId}">
                                    <i class="bi bi-plus-square"></i> Append
                                </label>
                                
                                <input type="radio" class="btn-check" name="mode_${sheetId}" 
                                       id="replace_${sheetId}" value="replace"
                                       onchange="setImportMode('${worksheet.name}', 'replace')">
                                <label class="btn btn-outline-warning btn-sm" for="replace_${sheetId}">
                                    <i class="bi bi-arrow-repeat"></i> Replace
                                </label>
                            </div>
                        </div>
                        <div class="col-md-6">
                            <label class="form-label small fw-bold">Target</label>
                            <div id="target_${sheetId}">
                                <div class="input-group input-group-sm">
                                    <span class="input-group-text"><i class="bi bi-table"></i></span>
                                    <input type="text" class="form-control" 
                                           placeholder="Table name" 
                                           value="${worksheet.name.replace(/[^a-zA-Z0-9]/g, '_')}"
                                           onchange="setTargetTable('${worksheet.name}', this.value)">
                                </div>
                                <div class="form-text small text-success">✓ Will create new table</div>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        `;
        
        // Initialize worksheet mapping
        worksheetMappings[worksheet.name] = {
            included: true,
            mode: 'new',
            targetTable: worksheet.name.replace(/[^a-zA-Z0-9]/g, '_'),
            columns: worksheet.columns
        };
    });
    
    configContainer.innerHTML = configHTML;
}

// Worksheet management functions
function toggleWorksheet(sheetName, included) {
    if (worksheetMappings[sheetName]) {
        worksheetMappings[sheetName].included = included;
    }
    
    const sheetId = sheetName.replace(/[^a-zA-Z0-9]/g, '_');
    const configCard = document.getElementById(`config_${sheetId}`);
    if (configCard) {
        configCard.style.opacity = included ? '1' : '0.5';
    }
}

function setImportMode(sheetName, mode) {
    if (!worksheetMappings[sheetName]) {
        worksheetMappings[sheetName] = {};
    }
    worksheetMappings[sheetName].mode = mode;
    
    const sheetId = sheetName.replace(/[^a-zA-Z0-9]/g, '_');
    const targetContainer = document.getElementById(`target_${sheetId}`);
    
    if (mode === 'new') {
        targetContainer.innerHTML = `
            <div class="input-group input-group-sm">
                <span class="input-group-text"><i class="bi bi-table"></i></span>
                <input type="text" class="form-control" 
                       placeholder="New table name" 
                       value="${sheetName.replace(/[^a-zA-Z0-9]/g, '_')}"
                       onchange="setTargetTable('${sheetName}', this.value)">
            </div>
            <div class="form-text small text-success">✓ Will create new table</div>
        `;
    } else if (mode === 'append' || mode === 'replace') {
        // Load existing tables for selection
        loadTablesForMode(sheetName, mode);
    }
}

function loadTablesForMode(sheetName, mode) {
    fetch('/api/tables')
        .then(response => response.json())
        .then(data => {
            const sheetId = sheetName.replace(/[^a-zA-Z0-9]/g, '_');
            const targetContainer = document.getElementById(`target_${sheetId}`);
            
            if (data.success && data.tables.length > 0) {
                let tableOptions = '<option value="">Choose table...</option>';
                data.tables.forEach(table => {
                    tableOptions += `<option value="${table.name}">${table.name} (${formatNumber(table.row_count)} records)</option>`;
                });
                
                const modeIcon = mode === 'append' ? 'plus-square' : 'arrow-repeat';
                const modeText = mode === 'append' ? 'Add data to selected table' : 'Replace data in selected table';
                const modeClass = mode === 'append' ? 'primary' : 'warning';
                
                targetContainer.innerHTML = `
                    <div class="input-group input-group-sm">
                        <span class="input-group-text"><i class="bi bi-${modeIcon}"></i></span>
                        <select class="form-select" onchange="setTargetTable('${sheetName}', this.value)">
                            ${tableOptions}
                        </select>
                    </div>
                    <div class="form-text small text-${modeClass}">
                        ${mode === 'append' ? '✓' : '⚠️'} ${modeText}
                    </div>
                `;
            } else {
                targetContainer.innerHTML = `
                    <div class="alert alert-warning alert-sm">
                        No existing tables found. Please create a new table instead.
                    </div>
                `;
                
                // Reset to new mode
                const newRadio = document.getElementById(`new_${sheetId}`);
                if (newRadio) {
                    newRadio.checked = true;
                    setImportMode(sheetName, 'new');
                }
            }
        })
        .catch(error => {
            console.error('Error loading tables:', error);
            showNotification('Error loading existing tables', 'danger');
        });
}

// Initialize search functionality
function initializeSearch() {
    const searchForm = document.getElementById('searchForm');
    const searchInput = document.getElementById('search');
    
    if (!searchForm || !searchInput) return;
    
    // Add real-time search with debounce
    let searchTimeout;
    searchInput.addEventListener('input', function() {
        clearTimeout(searchTimeout);
        searchTimeout = setTimeout(() => {
            if (this.value.length >= 3 || this.value.length === 0) {
                searchForm.submit();
            }
        }, 500);
    });
    
    // Clear search functionality
    const clearSearchBtn = document.querySelector('[data-action="clear-search"]');
    if (clearSearchBtn) {
        clearSearchBtn.addEventListener('click', function() {
            searchInput.value = '';
            searchForm.submit();
        });
    }
    
    // Enhanced search with filters
    initializeAdvancedSearch();
}

// Advanced search functionality
function initializeAdvancedSearch() {
    const searchForm = document.getElementById('searchForm');
    if (!searchForm) return;
    
    // Add search filters if they exist
    const filterInputs = searchForm.querySelectorAll('select, input[type="checkbox"]');
    filterInputs.forEach(input => {
        input.addEventListener('change', debounce(() => {
            searchForm.submit();
        }, 300));
    });
}

// Initialize theme functionality
function initializeTheme() {
    // Load saved theme
    const savedTheme = localStorage.getItem('selectedTheme') || 'light';
    setTheme(savedTheme);
    
    // Add event listeners to theme options
    document.querySelectorAll('.theme-option, [onclick*="setTheme"]').forEach(option => {
        const themeMatch = option.getAttribute('onclick')?.match(/setTheme\('([^']+)'\)/);
        if (themeMatch) {
            const theme = themeMatch[1];
            option.addEventListener('click', function(e) {
                e.preventDefault();
                setTheme(theme);
                localStorage.setItem('selectedTheme', theme);
            });
        }
    });
}

function setTheme(theme) {
    // Set theme on body
    document.body.setAttribute('data-theme', theme);
    
    // Update active theme indicator
    document.querySelectorAll('.theme-option').forEach(option => {
        option.classList.remove('active');
    });
    
    const activeTheme = document.querySelector(`[data-theme="${theme}"]`);
    if (activeTheme) {
        activeTheme.classList.add('active');
    }
    
    // Update navbar classes for better theme compatibility
    const navbar = document.querySelector('.navbar');
    if (navbar) {
        if (theme === 'dark') {
            navbar.classList.remove('navbar-light');
            navbar.classList.add('navbar-dark');
        } else {
            navbar.classList.remove('navbar-dark');
            navbar.classList.add('navbar-light');
        }
    }
    
    // Update theme toggle display
    updateThemeToggle(theme);
    
    // Save theme preference
    localStorage.setItem('selectedTheme', theme);
    
    // Update meta theme-color for mobile browsers
    const colors = {
        light: '#2563eb',
        dark: '#1e293b',
        blue: '#0284c7',
        green: '#16a34a',
        purple: '#7c3aed',
        orange: '#ea580c',
        rose: '#e11d48'
    };
    
    const metaThemeColor = document.querySelector('meta[name="theme-color"]');
    if (metaThemeColor) {
        metaThemeColor.content = colors[theme] || colors.light;
    }
}

function updateThemeToggle(activeTheme) {
    const themeToggle = document.querySelector('.theme-toggle');
    if (themeToggle) {
        const themeIcon = themeToggle.querySelector('i');
        const themeText = themeToggle.querySelector('.theme-text');
        
        const themeIcons = {
            'light': 'bi-sun',
            'dark': 'bi-moon', 
            'blue': 'bi-droplet',
            'green': 'bi-tree',
            'purple': 'bi-heart-pulse',
            'orange': 'bi-fire',
            'rose': 'bi-flower1'
        };
        
        if (themeIcon) {
            themeIcon.className = `bi ${themeIcons[activeTheme] || 'bi-palette'}`;
        }
        
        if (themeText) {
            themeText.textContent = activeTheme.charAt(0).toUpperCase() + activeTheme.slice(1);
        }
    }
    
    // Update theme options
    const themeOptions = document.querySelectorAll('.theme-option');
    themeOptions.forEach(option => {
        option.classList.remove('active');
        if (option.dataset.theme === activeTheme) {
            option.classList.add('active');
        }
    });
}

// Global Database Management Functions
async function loadGlobalDatabases() {
    if (databaseLoadPromise) return databaseLoadPromise;
    
    databaseLoadPromise = (async () => {
        try {
            const response = await fetch('/api/database/selection-info');
            const data = await response.json();
            
            const select = document.getElementById('globalDatabaseSelect');
            if (!select) return;
            
            select.innerHTML = '';
            
            if (data.success && data.databases && data.databases.length > 0) {
                // Add default option
                const defaultOption = document.createElement('option');
                defaultOption.value = '';
                defaultOption.textContent = 'Select Database';
                select.appendChild(defaultOption);
                
                // Add database options
                data.databases.forEach(db => {
                    const option = document.createElement('option');
                    option.value = db.path || db.name;
                    option.textContent = db.display_name || db.name || 'Unnamed Database';
                    if (db.current) {
                        option.selected = true;
                        currentGlobalDatabase = db.path || db.name;
                    }
                    select.appendChild(option);
                });
                
                // Add event listener for database changes
                select.addEventListener('change', async function() {
                    const selectedDb = this.value;
                    if (selectedDb && selectedDb !== currentGlobalDatabase) {
                        await switchGlobalDatabase(selectedDb);
                    }
                });
            } else {
                const option = document.createElement('option');
                option.value = '';
                option.textContent = 'No databases found';
                option.disabled = true;
                select.appendChild(option);
            }
        } catch (error) {
            console.error('Error loading databases:', error);
            const select = document.getElementById('globalDatabaseSelect');
            if (select) {
                select.innerHTML = '<option value="" disabled>Error loading</option>';
            }
        }
    })();
    
    return databaseLoadPromise;
}

async function switchGlobalDatabase(databasePath) {
    try {
        const response = await fetch('/api/database/switch', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({ database_path: databasePath })
        });
        
        const result = await response.json();
        
        if (result.success) {
            currentGlobalDatabase = databasePath;
            
            // Show success message
            showNotification('Database switched successfully!', 'success');
            
            // Refresh page to update data
            setTimeout(() => {
                window.location.reload();
            }, 1000);
        } else {
            showNotification(result.message || 'Failed to switch database', 'danger');
            // Reset select to previous value
            const select = document.getElementById('globalDatabaseSelect');
            if (select) {
                select.value = currentGlobalDatabase || '';
            }
        }
    } catch (error) {
        console.error('Error switching database:', error);
        showNotification('Error switching database', 'danger');
    }
}

// Additional page-specific initializations
function loadTablesPageFeatures() {
    // Initialize table management features
    initializeTableActions();
}

function loadDataAnalysisFeatures() {
    // Initialize data analysis features
    initializeCharts();
    initializeAnalysisTools();
}

// Table actions initialization
function initializeTableActions() {
    // Add event listeners for table management buttons
    document.querySelectorAll('[data-action="delete-table"]').forEach(btn => {
        btn.addEventListener('click', function() {
            const tableName = this.getAttribute('data-table');
            deleteTable(tableName);
        });
    });
    
    document.querySelectorAll('[data-action="export-table"]').forEach(btn => {
        btn.addEventListener('click', function() {
            const tableName = this.getAttribute('data-table');
            exportTable(tableName);
        });
    });
}

function deleteTable(tableName) {
    if (confirm(`Are you sure you want to delete the table "${tableName}"? This action cannot be undone.`)) {
        showLoading(`deleteBtn_${tableName}`, 'Deleting...');
        
        fetch(`/api/table/${tableName}`, {
            method: 'DELETE'
        })
        .then(response => response.json())
        .then(data => {
            if (data.success) {
                showNotification(`Table "${tableName}" deleted successfully`, 'success');
                location.reload();
            } else {
                showNotification('Error deleting table: ' + (data.error || 'Unknown error'), 'danger');
            }
        })
        .catch(error => {
            console.error('Error:', error);
            showNotification('Error deleting table', 'danger');
        });
    }
}

function exportTable(tableName) {
    showNotification('Starting export...', 'info', 2000);
    window.open(`/api/table/${tableName}/export`, '_blank');
}

// Record management initialization
function initializeRecordManagement() {
    // Initialize bulk selection
    const selectAllCheckbox = document.getElementById('selectAll');
    if (selectAllCheckbox) {
        selectAllCheckbox.addEventListener('change', toggleAllCheckboxes);
    }
    
    // Initialize individual record checkboxes
    document.querySelectorAll('input[name="record_ids"]').forEach(checkbox => {
        checkbox.addEventListener('change', updateDeleteButton);
    });
}

// Bulk operations initialization
function initializeBulkOperations() {
    const bulkDeleteBtn = document.getElementById('deleteSelectedBtn');
    if (bulkDeleteBtn) {
        bulkDeleteBtn.addEventListener('click', deleteSelectedRecords);
    }
}

// Table selections initialization
function initializeTableSelections() {
    updateDeleteButton();
}

// Record actions initialization  
function initializeRecordActions() {
    // Already handled by individual function definitions
}

// File processing initialization
function initializeFileProcessing() {
    const uploadForm = document.getElementById('enhancedUploadForm');
    if (uploadForm) {
        uploadForm.addEventListener('submit', handleEnhancedUpload);
    }
}

function handleEnhancedUpload(e) {
    e.preventDefault();
    
    const formData = new FormData(e.target);
    const fileInput = document.getElementById('file');
    
    if (!fileInput.files.length) {
        showNotification('Please select a file to upload', 'warning');
        return;
    }
    
    // Add worksheet mappings to form data
    formData.append('worksheet_mappings', JSON.stringify(worksheetMappings));
    
    // Show progress
    showUploadProgress();
    
    // Submit form
    fetch('/enhanced_upload', {
        method: 'POST',
        body: formData
    })
    .then(response => response.json())
    .then(data => {
        hideUploadProgress();
        
        if (data.success) {
            showNotification('File uploaded and processed successfully!', 'success');
            
            // Show results
            displayProcessingResults(data);
            
            // Refresh tables list
            setTimeout(() => {
                loadExistingTables();
            }, 1000);
        } else {
            showNotification('Error processing file: ' + (data.error || 'Unknown error'), 'danger');
        }
    })
    .catch(error => {
        hideUploadProgress();
        console.error('Error:', error);
        showNotification('Error uploading file', 'danger');
    });
}

function showUploadProgress() {
    const processingCard = document.getElementById('processingCard');
    if (processingCard) {
        processingCard.style.display = 'block';
        processingCard.innerHTML = `
            <div class="card-header">
                <h5 class="card-title mb-0">
                    <span class="badge bg-info me-2">Processing</span>
                    Uploading and Processing File
                </h5>
            </div>
            <div class="card-body">
                <div class="progress mb-3">
                    <div class="progress-bar progress-bar-striped progress-bar-animated" 
                         role="progressbar" style="width: 100%"></div>
                </div>
                <p class="text-center mb-0">Please wait while we process your Excel file...</p>
            </div>
        `;
    }
}

function hideUploadProgress() {
    const processingCard = document.getElementById('processingCard');
    if (processingCard) {
        processingCard.style.display = 'none';
    }
}

function displayProcessingResults(data) {
    const resultsCard = document.getElementById('resultsCard');
    if (!resultsCard) return;
    
    resultsCard.style.display = 'block';
    
    let resultsHTML = `
        <div class="card-header">
            <h5 class="card-title mb-0">
                <span class="badge bg-success me-2">✓</span>
                Processing Complete
            </h5>
        </div>
        <div class="card-body">
    `;
    
    if (data.results && data.results.length > 0) {
        resultsHTML += '<div class="row">';
        
        data.results.forEach(result => {
            const statusClass = result.success ? 'success' : 'danger';
            const statusIcon = result.success ? 'check-circle' : 'x-circle';
            
            resultsHTML += `
                <div class="col-md-6 mb-3">
                    <div class="card border-${statusClass}">
                        <div class="card-body">
                            <h6 class="card-title">
                                <i class="bi bi-${statusIcon} text-${statusClass}"></i>
                                ${result.worksheet}
                            </h6>
                            <p class="card-text small">
                                ${result.success 
                                    ? `✓ Successfully imported ${formatNumber(result.rows_imported)} rows into "${result.table_name}"`
                                    : `✗ Error: ${result.error}`
                                }
                            </p>
                            ${result.success ? `
                                <a href="/view_table/${result.table_name}" 
                                   class="btn btn-outline-${statusClass} btn-sm">
                                    <i class="bi bi-eye"></i> View Table
                                </a>
                            ` : ''}
                        </div>
                    </div>
                </div>
            `;
        });
        
        resultsHTML += '</div>';
    }
    
    resultsHTML += '</div>';
    resultsCard.innerHTML = resultsHTML;
}

// Worksheet mapping initialization
function initializeWorksheetMapping() {
    worksheetMappings = {};
}

// Enhanced drag and drop initialization
function initializeEnhancedDragDrop() {
    // Already handled in initializeFileUpload
}

// Progress indicators initialization
function initializeProgressIndicators() {
    // Set up global progress tracking
    const progressContainer = document.getElementById('globalProgressContainer');
    if (progressContainer) {
        // Initialize progress tracking UI
    }
}

// Advanced notifications initialization
function initializeAdvancedNotifications() {
    // Set up notification container if it doesn't exist
    if (!document.getElementById('notificationContainer')) {
        const container = document.createElement('div');
        container.id = 'notificationContainer';
        container.className = 'position-fixed top-0 end-0 p-3';
        container.style.zIndex = '9999';
        document.body.appendChild(container);
    }
}

// Keyboard shortcuts initialization
function initializeKeyboardShortcuts() {
    document.addEventListener('keydown', function(e) {
        // Ctrl+/ for search focus
        if (e.ctrlKey && e.key === '/') {
            e.preventDefault();
            const searchInput = document.getElementById('search');
            if (searchInput) {
                searchInput.focus();
            }
        }
        
        // Escape to close modals
        if (e.key === 'Escape') {
            const modals = document.querySelectorAll('.modal.show');
            modals.forEach(modal => {
                const modalInstance = bootstrap.Modal.getInstance(modal);
                if (modalInstance) {
                    modalInstance.hide();
                }
            });
        }
    });
}

// Form submission with progress handling
function handleFormSubmissionWithProgress(e) {
    const form = e.target;
    const submitBtn = form.querySelector('button[type="submit"]');
    
    if (submitBtn) {
        showLoading(submitBtn, 'Processing...');
    }
}

// Global form loading states
document.addEventListener('submit', function(e) {
    const form = e.target;
    if (form.tagName === 'FORM') {
        const submitBtn = form.querySelector('button[type="submit"], input[type="submit"]');
        if (submitBtn && !submitBtn.hasAttribute('data-no-loading')) {
            submitBtn.innerHTML = '<span class="spinner-border spinner-border-sm me-2"></span>Processing...';
            submitBtn.disabled = true;
        }
    }
});

// Charts initialization
function initializeCharts() {
    // Initialize Chart.js if available
    if (typeof Chart !== 'undefined') {
        // Chart initialization code here
    }
}

// Analysis tools initialization  
function initializeAnalysisTools() {
    // Initialize data analysis specific tools
    const analysisTools = document.querySelectorAll('[data-analysis-tool]');
    analysisTools.forEach(tool => {
        // Add analysis tool functionality
    });
}

// Show table info function
function showTableInfo(tableName) {
    fetch(`/api/table/${tableName}/info`)
        .then(response => response.json())
        .then(data => {
            if (data.success) {
                const modal = new bootstrap.Modal(document.getElementById('tableInfoModal') || createTableInfoModal());
                updateTableInfoModal(data.table_info);
                modal.show();
            } else {
                showNotification('Error loading table info: ' + (data.error || 'Unknown error'), 'danger');
            }
        })
        .catch(error => {
            console.error('Error:', error);
            showNotification('Error loading table information', 'danger');
        });
}

function createTableInfoModal() {
    const modal = document.createElement('div');
    modal.className = 'modal fade';
    modal.id = 'tableInfoModal';
    modal.innerHTML = `
        <div class="modal-dialog modal-lg">
            <div class="modal-content">
                <div class="modal-header">
                    <h5 class="modal-title">Table Information</h5>
                    <button type="button" class="btn-close" data-bs-dismiss="modal"></button>
                </div>
                <div class="modal-body" id="tableInfoModalBody">
                    <!-- Content will be populated -->
                </div>
                <div class="modal-footer">
                    <button type="button" class="btn btn-secondary" data-bs-dismiss="modal">Close</button>
                </div>
            </div>
        </div>
    `;
    document.body.appendChild(modal);
    return modal;
}

function updateTableInfoModal(tableInfo) {
    const modalBody = document.getElementById('tableInfoModalBody');
    if (!modalBody || !tableInfo) return;
    
    modalBody.innerHTML = `
        <div class="row">
            <div class="col-md-6">
                <h6>General Information</h6>
                <ul class="list-unstyled">
                    <li><strong>Table Name:</strong> ${tableInfo.name}</li>
                    <li><strong>Total Records:</strong> ${formatNumber(tableInfo.row_count)}</li>
                    <li><strong>Columns:</strong> ${tableInfo.columns ? tableInfo.columns.length : 0}</li>
                    <li><strong>Created:</strong> ${tableInfo.created_date || 'Unknown'}</li>
                </ul>
            </div>
            <div class="col-md-6">
                <h6>Storage Information</h6>
                <ul class="list-unstyled">
                    <li><strong>Estimated Size:</strong> ${formatFileSize(tableInfo.estimated_size || 0)}</li>
                    <li><strong>Database:</strong> ${tableInfo.database_path || 'Current'}</li>
                </ul>
            </div>
        </div>
        
        ${tableInfo.columns ? `
            <div class="mt-4">
                <h6>Column Information</h6>
                <div class="table-responsive">
                    <table class="table table-sm table-striped">
                        <thead>
                            <tr>
                                <th>Column Name</th>
                                <th>Data Type</th>
                                <th>Nullable</th>
                                <th>Default Value</th>
                            </tr>
                        </thead>
                        <tbody>
                            ${tableInfo.columns.map(col => `
                                <tr>
                                    <td>${col.name}</td>
                                    <td><span class="badge bg-secondary">${col.type}</span></td>
                                    <td>${col.nullable ? '✓' : '✗'}</td>
                                    <td>${col.default || '-'}</td>
                                </tr>
                            `).join('')}
                        </tbody>
                    </table>
                </div>
            </div>
        ` : ''}
    `;
}

// Utility functions

// Show loading state
function showLoading(element, text = 'Loading...') {
    if (typeof element === 'string') {
        element = document.getElementById(element);
    }
    
    if (element) {
        // Store original content
        if (!element.hasAttribute('data-original-content')) {
            element.setAttribute('data-original-content', element.innerHTML);
        }
        
        element.innerHTML = `
            <span class="spinner-border spinner-border-sm me-2" role="status">
                <span class="visually-hidden">${text}</span>
            </span>
            ${text}
        `;
        element.disabled = true;
    }
}

// Hide loading state
function hideLoading(element, originalContent = '') {
    if (typeof element === 'string') {
        element = document.getElementById(element);
    }
    
    if (element) {
        const storedContent = element.getAttribute('data-original-content');
        element.innerHTML = originalContent || storedContent || element.innerHTML.replace(/<span class="spinner-border.*?<\/span>\s*/, '');
        element.disabled = false;
        element.removeAttribute('data-original-content');
    }
}

// Enhanced notification system
function showNotification(message, type = 'info', duration = 5000) {
    const notificationContainer = document.getElementById('notificationContainer') || createNotificationContainer();
    
    const notificationId = 'notification_' + Date.now();
    const alertDiv = document.createElement('div');
    alertDiv.id = notificationId;
    alertDiv.className = `alert alert-${type} alert-dismissible fade show shadow-sm`;
    alertDiv.style.minWidth = '300px';
    alertDiv.innerHTML = `
        <div class="d-flex align-items-center">
            <i class="bi bi-${getNotificationIcon(type)} me-2"></i>
            <div class="flex-grow-1">${message}</div>
            <button type="button" class="btn-close" data-bs-dismiss="alert"></button>
        </div>
    `;
    
    notificationContainer.appendChild(alertDiv);
    
    // Auto-hide after duration
    if (duration > 0) {
        setTimeout(() => {
            if (alertDiv.parentNode) {
                alertDiv.classList.remove('show');
                setTimeout(() => {
                    if (alertDiv.parentNode) {
                        alertDiv.remove();
                    }
                }, 150);
            }
        }, duration);
    }
    
    return notificationId;
}

function createNotificationContainer() {
    const container = document.createElement('div');
    container.id = 'notificationContainer';
    container.className = 'position-fixed top-0 end-0 p-3';
    container.style.zIndex = '9999';
    document.body.appendChild(container);
    return container;
}

function getNotificationIcon(type) {
    const icons = {
        'success': 'check-circle',
        'danger': 'exclamation-triangle',
        'warning': 'exclamation-circle',
        'info': 'info-circle',
        'primary': 'info-circle',
        'secondary': 'info-circle'
    };
    return icons[type] || 'info-circle';
}

// Format file size
function formatFileSize(bytes) {
    if (bytes === 0) return '0 Bytes';
    
    const k = 1024;
    const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
}

// Format numbers with commas
function formatNumber(num) {
    if (num === null || num === undefined) return '0';
    return num.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ',');
}

// Debounce function
function debounce(func, wait, immediate) {
    let timeout;
    return function executedFunction() {
        const context = this;
        const args = arguments;
        
        const later = function() {
            timeout = null;
            if (!immediate) func.apply(context, args);
        };
        
        const callNow = immediate && !timeout;
        clearTimeout(timeout);
        timeout = setTimeout(later, wait);
        
        if (callNow) func.apply(context, args);
    };
}

// Throttle function
function throttle(func, limit) {
    let inThrottle;
    return function() {
        const args = arguments;
        const context = this;
        if (!inThrottle) {
            func.apply(context, args);
            inThrottle = true;
            setTimeout(() => inThrottle = false, limit);
        }
    };
}

// Enhanced API helper functions
const API = {
    // Generic API call with better error handling
    call: async function(endpoint, options = {}) {
        const defaultOptions = {
            headers: {
                'Content-Type': 'application/json',
            },
        };
        
        const mergedOptions = { ...defaultOptions, ...options };
        
        try {
            const response = await fetch(endpoint, mergedOptions);
            
            // Handle different content types
            let data;
            const contentType = response.headers.get('content-type');
            if (contentType && contentType.includes('application/json')) {
                data = await response.json();
            } else {
                data = await response.text();
            }
            
            if (!response.ok) {
                throw new Error(data.error || data || `HTTP error! status: ${response.status}`);
            }
            
            return data;
        } catch (error) {
            console.error('API Error:', error);
            throw error;
        }
    },
    
    // Get table info
    getTableInfo: function(tableName) {
        return this.call(`/api/table/${encodeURIComponent(tableName)}/info`);
    },
    
    // Delete table
    deleteTable: function(tableName) {
        return this.call(`/api/table/${encodeURIComponent(tableName)}`, { method: 'DELETE' });
    },
    
    // Get tables list
    getTables: function() {
        return this.call('/api/tables');
    },
    
    // Get record
    getRecord: function(tableName, recordId) {
        return this.call(`/api/table/${encodeURIComponent(tableName)}/record/${encodeURIComponent(recordId)}`);
    },
    
    // Update record
    updateRecord: function(tableName, recordId, data) {
        return this.call(`/api/table/${encodeURIComponent(tableName)}/record/${encodeURIComponent(recordId)}`, {
            method: 'PUT',
            body: JSON.stringify(data)
        });
    },
    
    // Delete record
    deleteRecord: function(tableName, recordId) {
        return this.call(`/api/table/${encodeURIComponent(tableName)}/record/${encodeURIComponent(recordId)}`, { 
            method: 'DELETE' 
        });
    },
    
    // Bulk delete records
    bulkDeleteRecords: function(tableName, recordIds) {
        return this.call(`/api/table/${encodeURIComponent(tableName)}/records/bulk_delete`, {
            method: 'POST',
            body: JSON.stringify({ record_ids: recordIds })
        });
    },
    
    // Export table
    exportTable: function(tableName, format = 'xlsx') {
        return `/api/table/${encodeURIComponent(tableName)}/export?format=${format}`;
    },
    
    // Preview file
    previewFile: function(file) {
        const formData = new FormData();
        formData.append('file', file);
        
        return fetch('/api/preview_file', {
            method: 'POST',
            body: formData
        }).then(response => response.json());
    },
    
    // Upload file with progress
    uploadFile: function(formData, onProgress) {
        return new Promise((resolve, reject) => {
            const xhr = new XMLHttpRequest();
            
            if (onProgress) {
                xhr.upload.addEventListener('progress', (e) => {
                    if (e.lengthComputable) {
                        const percentComplete = (e.loaded / e.total) * 100;
                        onProgress(percentComplete);
                    }
                });
            }
            
            xhr.addEventListener('load', () => {
                if (xhr.status >= 200 && xhr.status < 300) {
                    try {
                        const response = JSON.parse(xhr.responseText);
                        resolve(response);
                    } catch (e) {
                        resolve(xhr.responseText);
                    }
                } else {
                    reject(new Error(`HTTP Error: ${xhr.status}`));
                }
            });
            
            xhr.addEventListener('error', () => {
                reject(new Error('Network Error'));
            });
            
            xhr.open('POST', '/enhanced_upload');
            xhr.send(formData);
        });
    }
};

// DOM utilities
const DOM = {
    // Safely get element
    get: function(selector) {
        return typeof selector === 'string' ? document.querySelector(selector) : selector;
    },
    
    // Get all elements
    getAll: function(selector) {
        return document.querySelectorAll(selector);
    },
    
    // Create element with attributes and content
    create: function(tag, attributes = {}, content = '') {
        const element = document.createElement(tag);
        
        Object.entries(attributes).forEach(([key, value]) => {
            if (key === 'className') {
                element.className = value;
            } else if (key === 'innerHTML') {
                element.innerHTML = value;
            } else {
                element.setAttribute(key, value);
            }
        });
        
        if (content) {
            element.innerHTML = content;
        }
        
        return element;
    },
    
    // Show element
    show: function(element) {
        const el = this.get(element);
        if (el) el.style.display = '';
    },
    
    // Hide element
    hide: function(element) {
        const el = this.get(element);
        if (el) el.style.display = 'none';
    },
    
    // Toggle element visibility
    toggle: function(element) {
        const el = this.get(element);
        if (el) {
            el.style.display = el.style.display === 'none' ? '' : 'none';
        }
    }
};

// Storage utilities
const Storage = {
    // Set item with JSON support
    set: function(key, value) {
        try {
            const serializedValue = typeof value === 'object' ? JSON.stringify(value) : value;
            localStorage.setItem(key, serializedValue);
        } catch (error) {
            console.error('Storage set error:', error);
        }
    },
    
    // Get item with JSON support
    get: function(key, defaultValue = null) {
        try {
            const value = localStorage.getItem(key);
            if (value === null) return defaultValue;
            
            try {
                return JSON.parse(value);
            } catch {
                return value;
            }
        } catch (error) {
            console.error('Storage get error:', error);
            return defaultValue;
        }
    },
    
    // Remove item
    remove: function(key) {
        try {
            localStorage.removeItem(key);
        } catch (error) {
            console.error('Storage remove error:', error);
        }
    },
    
    // Clear all items
    clear: function() {
        try {
            localStorage.clear();
        } catch (error) {
            console.error('Storage clear error:', error);
        }
    }
};

// Validation utilities
const Validator = {
    // Validate email
    email: function(email) {
        const re = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
        return re.test(email);
    },
    
    // Validate file type
    fileType: function(file, allowedTypes) {
        const fileExtension = '.' + file.name.split('.').pop().toLowerCase();
        return allowedTypes.includes(fileExtension);
    },
    
    // Validate file size
    fileSize: function(file, maxSizeBytes) {
        return file.size <= maxSizeBytes;
    },
    
    // Validate required fields
    required: function(value) {
        return value !== null && value !== undefined && value !== '';
    }
};

// Global error handler
window.addEventListener('error', function(e) {
    console.error('Global error:', e.error);
    
    // Show user-friendly error message for critical errors
    if (e.error && e.error.message) {
        showNotification('An unexpected error occurred. Please refresh the page.', 'danger', 10000);
    }
});

// Global unhandled promise rejection handler
window.addEventListener('unhandledrejection', function(e) {
    console.error('Unhandled promise rejection:', e.reason);
    
    // Show user-friendly error message
    showNotification('A network or processing error occurred. Please try again.', 'warning', 5000);
});

// Export for use in other scripts
if (typeof module !== 'undefined' && module.exports) {
    module.exports = { 
        API, 
        DOM, 
        Storage, 
        Validator,
        showNotification, 
        formatFileSize, 
        formatNumber, 
        debounce, 
        throttle,
        showLoading,
        hideLoading
    };
}

// Global functions for backward compatibility
window.API = API;
window.DOM = DOM;
window.Storage = Storage;
window.Validator = Validator;
window.showNotification = showNotification;
window.formatFileSize = formatFileSize;
window.formatNumber = formatNumber;
window.debounce = debounce;
window.throttle = throttle;
window.showLoading = showLoading;
window.hideLoading = hideLoading;

// Drawer navigation functions
function toggleDrawer() {
    const drawer = document.getElementById('mainDrawer');
    const overlay = document.getElementById('drawerOverlay');
    const drawerToggle = document.querySelector('.drawer-toggle');
    
    if (drawer && overlay) {
        const isOpen = drawer.classList.contains('show');
        
        if (isOpen) {
            drawer.classList.remove('show');
            overlay.classList.remove('show');
            document.body.classList.remove('drawer-open');
            if (drawerToggle) drawerToggle.setAttribute('aria-expanded', 'false');
        } else {
            drawer.classList.add('show');
            overlay.classList.add('show');
            document.body.classList.add('drawer-open');
            if (drawerToggle) drawerToggle.setAttribute('aria-expanded', 'true');
        }
    }
}

function closeDrawer() {
    const drawer = document.getElementById('mainDrawer');
    const overlay = document.getElementById('drawerOverlay');
    const drawerToggle = document.querySelector('.drawer-toggle');
    
    if (drawer && overlay) {
        drawer.classList.remove('show');
        overlay.classList.remove('show');
        document.body.classList.remove('drawer-open');
        if (drawerToggle) drawerToggle.setAttribute('aria-expanded', 'false');
    }
}

function toggleSubmenu(submenuIdOrElement) {
    let element, submenuId;
    
    // Handle both string ID and element parameters
    if (typeof submenuIdOrElement === 'string') {
        submenuId = submenuIdOrElement;
        // Find the element that triggers this submenu
        element = document.querySelector(`[onclick*="toggleSubmenu('${submenuId}')"]`);
    } else {
        element = submenuIdOrElement;
        // Extract submenu ID from onclick attribute
        const onclickAttr = element.getAttribute('onclick');
        const match = onclickAttr ? onclickAttr.match(/toggleSubmenu\('([^']+)'\)/) : null;
        submenuId = match ? match[1] : null;
    }
    
    if (!element || !submenuId) return;
    
    // Find the submenu element by ID
    const submenu = document.getElementById(submenuId);
    if (!submenu) return;
    
    // Find the icon element
    const icon = element.querySelector('.bi-chevron-down, .bi-chevron-right, .submenu-icon');
    
    // Toggle submenu visibility
    const isExpanded = submenu.classList.contains('show');
    
    if (isExpanded) {
        submenu.classList.remove('show');
        submenu.style.maxHeight = '0';
        if (icon) {
            icon.classList.remove('bi-chevron-down');
            icon.classList.add('bi-chevron-right');
        }
        element.setAttribute('aria-expanded', 'false');
    } else {
        submenu.classList.add('show');
        submenu.style.maxHeight = submenu.scrollHeight + 'px';
        if (icon) {
            icon.classList.remove('bi-chevron-right');
            icon.classList.add('bi-chevron-down');
        }
        element.setAttribute('aria-expanded', 'true');
    }
}

// Initialize drawer functionality
function initializeDrawer() {
    // Handle drawer toggle button
    const drawerToggle = document.querySelector('.drawer-toggle');
    if (drawerToggle) {
        drawerToggle.addEventListener('click', function(e) {
            e.preventDefault();
            toggleDrawer();
        });
    }
    
    // Handle drawer close button
    const drawerClose = document.getElementById('drawerClose');
    if (drawerClose) {
        drawerClose.addEventListener('click', function(e) {
            e.preventDefault();
            closeDrawer();
        });
    }
    
    // Handle drawer overlay click
    const drawerOverlay = document.getElementById('drawerOverlay');
    if (drawerOverlay) {
        drawerOverlay.addEventListener('click', closeDrawer);
    }
    
    // Handle submenu toggles - keep onclick attributes functional
    const submenuToggles = document.querySelectorAll('button[onclick*="toggleSubmenu"]');
    submenuToggles.forEach(toggle => {
        // Add additional event listener but keep onclick
        toggle.addEventListener('click', function(e) {
            e.preventDefault();
            // Extract submenu ID from onclick attribute
            const onclickAttr = this.getAttribute('onclick');
            const match = onclickAttr ? onclickAttr.match(/toggleSubmenu\('([^']+)'\)/) : null;
            if (match) {
                toggleSubmenu(match[1]);
            }
        });
    });
    
    // Handle escape key to close drawer
    document.addEventListener('keydown', function(e) {
        if (e.key === 'Escape') {
            const drawer = document.getElementById('mainDrawer');
            if (drawer && drawer.classList.contains('show')) {
                closeDrawer();
            }
        }
    });
    
    // Close drawer on window resize for mobile
    let resizeTimeout;
    window.addEventListener('resize', function() {
        clearTimeout(resizeTimeout);
        resizeTimeout = setTimeout(function() {
            if (window.innerWidth > 768) {
                closeDrawer();
            }
        }, 250);
    });
}

// Make main functions globally available for onclick handlers
window.setTheme = setTheme;
window.loadExistingTables = loadExistingTables;
window.setTargetTable = setTargetTable;
window.toggleAllCheckboxes = toggleAllCheckboxes;
window.updateDeleteButton = updateDeleteButton;
window.deleteSelectedRecords = deleteSelectedRecords;
window.editRecord = editRecord;
window.saveRecord = saveRecord;
window.deleteRecord = deleteRecord;
window.deleteTable = deleteTable;
window.exportTable = exportTable;
window.showTableInfo = showTableInfo;
window.toggleWorksheet = toggleWorksheet;
window.setImportMode = setImportMode;
window.previewFile = previewFile;

// ==============================================================================
// UNIVERSAL MENU HELPERS
// ==============================================================================

/**
 * Create a consistent dropdown menu using the universal styling
 * @param {Array} items - Array of menu items
 * @param {Object} options - Menu configuration options
 * @returns {HTMLElement} - The created menu element
 */
function createUniversalDropdown(items, options = {}) {
    const {
        triggerElement,
        position = 'bottom-start',
        className = '',
        compact = false,
        searchable = false
    } = options;

    // Create dropdown container
    const dropdown = document.createElement('div');
    dropdown.className = `dropdown ${className}`;

    // Create dropdown menu
    const menu = document.createElement('div');
    menu.className = `dropdown-menu ${compact ? 'compact' : ''}`;
    menu.setAttribute('role', 'menu');

    // Add search if requested
    if (searchable) {
        const searchInput = document.createElement('input');
        searchInput.type = 'text';
        searchInput.className = 'menu-search form-control form-control-sm';
        searchInput.placeholder = 'Search menu...';
        searchInput.addEventListener('input', (e) => {
            filterMenuItems(menu, e.target.value);
        });
        menu.appendChild(searchInput);
    }

    // Add items
    items.forEach(itemData => {
        const item = createUniversalMenuItem(itemData);
        menu.appendChild(item);
    });

    dropdown.appendChild(menu);

    // Position dropdown relative to trigger element
    if (triggerElement) {
        positionDropdown(dropdown, triggerElement, position);
    }

    return dropdown;
}

/**
 * Create a consistent menu item using the universal styling
 * @param {Object} itemData - Menu item configuration
 * @returns {HTMLElement} - The created menu item element
 */
function createUniversalMenuItem(itemData) {
    const {
        text,
        subtitle,
        icon,
        action,
        href,
        disabled = false,
        separator = false,
        header = false,
        checked = null,
        badge = null,
        className = ''
    } = itemData;

    // Create separator
    if (separator) {
        const sep = document.createElement('div');
        sep.className = 'dropdown-divider menu-separator';
        return sep;
    }

    // Create header
    if (header) {
        const headerEl = document.createElement('div');
        headerEl.className = 'dropdown-header menu-header';
        headerEl.textContent = text;
        return headerEl;
    }

    // Create menu item
    const item = document.createElement(href ? 'a' : 'button');
    item.className = `dropdown-item ${className}`;
    item.setAttribute('role', 'menuitem');
    item.setAttribute('tabindex', '0');

    if (href) {
        item.href = href;
    } else {
        item.type = 'button';
    }

    if (disabled) {
        item.classList.add('disabled');
        item.setAttribute('aria-disabled', 'true');
        item.disabled = true;
    }

    if (checked !== null) {
        item.classList.add('checkable');
        if (checked) {
            item.classList.add('checked');
        }
    }

    // Build item content
    let innerHTML = '';

    if (icon) {
        innerHTML += `<i class="${icon}"></i>`;
    }

    innerHTML += '<div class="dropdown-item-content">';
    innerHTML += text;

    if (subtitle) {
        innerHTML += `<small>${subtitle}</small>`;
    }

    innerHTML += '</div>';

    if (badge) {
        innerHTML += `<span class="badge bg-secondary">${badge}</span>`;
    }

    item.innerHTML = innerHTML;

    // Add action handler
    if (action && typeof action === 'function') {
        item.addEventListener('click', (e) => {
            e.preventDefault();
            action(e, item);
        });
    }

    return item;
}

/**
 * Show a context menu at the specified position
 * @param {Event} event - The triggering event (usually right-click)
 * @param {Array} items - Array of menu items
 * @param {Object} options - Additional options
 */
function showUniversalContextMenu(event, items, options = {}) {
    event.preventDefault();

    // Remove existing context menus
    document.querySelectorAll('.context-menu').forEach(menu => menu.remove());

    // Create context menu
    const menu = document.createElement('div');
    menu.className = 'dropdown-menu context-menu show';
    menu.setAttribute('role', 'menu');
    menu.style.position = 'fixed';
    menu.style.left = `${event.clientX}px`;
    menu.style.top = `${event.clientY}px`;
    menu.style.zIndex = '2000';

    // Add items
    items.forEach(itemData => {
        const item = createUniversalMenuItem(itemData);
        menu.appendChild(item);
    });

    document.body.appendChild(menu);

    // Adjust position if menu goes off screen
    const rect = menu.getBoundingClientRect();
    if (rect.right > window.innerWidth) {
        menu.style.left = `${event.clientX - rect.width}px`;
    }
    if (rect.bottom > window.innerHeight) {
        menu.style.top = `${event.clientY - rect.height}px`;
    }

    // Close on click outside
    const closeHandler = (e) => {
        if (!menu.contains(e.target)) {
            menu.remove();
            document.removeEventListener('click', closeHandler);
            document.removeEventListener('contextmenu', closeHandler);
        }
    };

    setTimeout(() => {
        document.addEventListener('click', closeHandler);
        document.addEventListener('contextmenu', closeHandler);
    }, 100);

    return menu;
}

/**
 * Add universal styling to existing dropdowns and menus
 */
function enhanceExistingMenus() {
    // Find all existing dropdown menus and enhance them
    document.querySelectorAll('.dropdown-menu').forEach(menu => {
        if (!menu.classList.contains('enhanced')) {
            menu.classList.add('enhanced');
            
            // Enhance menu items
            menu.querySelectorAll('.dropdown-item').forEach(item => {
                if (!item.querySelector('.dropdown-item-content')) {
                    enhanceMenuItem(item);
                }
            });
        }
    });
}

/**
 * Enhance an existing menu item to follow universal styling
 * @param {HTMLElement} item - The menu item to enhance
 */
function enhanceMenuItem(item) {
    const text = item.textContent.trim();
    const icon = item.querySelector('i');
    
    if (!text) return;

    // Extract icon if present
    if (icon) {
        icon.remove();
    }

    // Create content structure
    const contentDiv = document.createElement('div');
    contentDiv.className = 'dropdown-item-content';
    contentDiv.textContent = text;

    // Clear and rebuild item
    item.innerHTML = '';
    
    if (icon) {
        item.appendChild(icon);
    }
    
    item.appendChild(contentDiv);

    // Ensure proper attributes
    if (!item.hasAttribute('role')) {
        item.setAttribute('role', 'menuitem');
    }
    if (!item.hasAttribute('tabindex')) {
        item.setAttribute('tabindex', '0');
    }
}

/**
 * Filter menu items based on search term
 * @param {HTMLElement} menu - The menu container
 * @param {string} searchTerm - The search term
 */
function filterMenuItems(menu, searchTerm) {
    const items = menu.querySelectorAll('.dropdown-item:not(.menu-search)');
    const term = searchTerm.toLowerCase();

    items.forEach(item => {
        const text = item.textContent.toLowerCase();
        const shouldShow = text.includes(term) || term === '';
        item.style.display = shouldShow ? '' : 'none';
    });
}

/**
 * Position a dropdown relative to a trigger element
 * @param {HTMLElement} dropdown - The dropdown element
 * @param {HTMLElement} trigger - The trigger element
 * @param {string} position - Position preference (bottom-start, bottom-end, etc.)
 */
function positionDropdown(dropdown, trigger, position = 'bottom-start') {
    const triggerRect = trigger.getBoundingClientRect();
    const menu = dropdown.querySelector('.dropdown-menu');
    
    // Show temporarily to get dimensions
    menu.style.visibility = 'hidden';
    menu.style.display = 'block';
    document.body.appendChild(dropdown);
    
    const menuRect = menu.getBoundingClientRect();
    
    let left, top;
    
    switch (position) {
        case 'bottom-start':
            left = triggerRect.left;
            top = triggerRect.bottom + 5;
            break;
        case 'bottom-end':
            left = triggerRect.right - menuRect.width;
            top = triggerRect.bottom + 5;
            break;
        case 'top-start':
            left = triggerRect.left;
            top = triggerRect.top - menuRect.height - 5;
            break;
        case 'top-end':
            left = triggerRect.right - menuRect.width;
            top = triggerRect.top - menuRect.height - 5;
            break;
        default:
            left = triggerRect.left;
            top = triggerRect.bottom + 5;
    }
    
    // Adjust if menu goes off screen
    if (left + menuRect.width > window.innerWidth) {
        left = window.innerWidth - menuRect.width - 10;
    }
    if (left < 10) {
        left = 10;
    }
    if (top + menuRect.height > window.innerHeight) {
        top = triggerRect.top - menuRect.height - 5;
    }
    if (top < 10) {
        top = triggerRect.bottom + 5;
    }
    
    menu.style.position = 'fixed';
    menu.style.left = `${left}px`;
    menu.style.top = `${top}px`;
    menu.style.visibility = 'visible';
}

// Initialize menu enhancements when the page loads
document.addEventListener('DOMContentLoaded', function() {
    // Enhance existing menus after a short delay to ensure they're rendered
    setTimeout(enhanceExistingMenus, 500);
    
    // Set up observer for dynamically added menus
    const observer = new MutationObserver((mutations) => {
        mutations.forEach((mutation) => {
            mutation.addedNodes.forEach((node) => {
                if (node.nodeType === Node.ELEMENT_NODE) {
                    if (node.classList?.contains('dropdown-menu') || 
                        node.querySelector?.('.dropdown-menu')) {
                        setTimeout(enhanceExistingMenus, 50);
                    }
                }
            });
        });
    });
    
    observer.observe(document.body, {
        childList: true,
        subtree: true
    });
});

// Make menu functions globally available
window.createUniversalDropdown = createUniversalDropdown;
window.createUniversalMenuItem = createUniversalMenuItem;
window.showUniversalContextMenu = showUniversalContextMenu;
window.enhanceExistingMenus = enhanceExistingMenus;
window.loadGlobalDatabases = loadGlobalDatabases;
window.switchGlobalDatabase = switchGlobalDatabase;

// Make drawer functions globally available
window.toggleDrawer = toggleDrawer;
window.closeDrawer = closeDrawer;
window.toggleSubmenu = toggleSubmenu;
