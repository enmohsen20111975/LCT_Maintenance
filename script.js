// Declare currentData at the top level
let currentData = null;

document.addEventListener('DOMContentLoaded', function() {
    // Theme handling
    const themeSelector = document.getElementById('themeSelector');
    const themeToggle = document.getElementById('themeToggle');
    
    // Add helper functions first
    function parseFrenchDate(dateStr) {
        if (!dateStr) return null;
        try {
            // Handle Excel numeric dates
            if (typeof dateStr === 'number') {
                const excelEpoch = new Date(1899, 11, 30); // Excel's epoch is December 30, 1899
                const msPerDay = 24 * 60 * 60 * 1000;
                const date = new Date(excelEpoch.getTime() + (dateStr * msPerDay));
                return date;
            }

            // Handle ISO format YYYY-MM-DD
            if (typeof dateStr === 'string' && dateStr.match(/^\d{4}-\d{2}-\d{2}/)) {
                const date = new Date(dateStr);
                if (!isNaN(date.getTime())) {
                    return date;
                }
            }

            // Handle French format DD/MM/YYYY or DD-MM-YYYY
            const parts = String(dateStr).split(/[\/\-\s]/);
            if (parts.length >= 3) {
                const day = parseInt(parts[0], 10);
                const month = parseInt(parts[1], 10) - 1;
                const year = parseInt(parts[2], 10);
                if (!isNaN(day) && !isNaN(month) && !isNaN(year)) {
                    const date = new Date(year, month, day);
                    if (date && !isNaN(date.getTime())) {
                        return date;
                    }
                }
            }

            // Try standard date parsing as fallback
            const date = new Date(dateStr);
            return isNaN(date.getTime()) ? null : date;
        } catch (e) {
            console.warn('Error parsing date:', e, 'for input:', dateStr);
            return null;
        }
    }

    function formatToFrenchDate(date) {
        if (!date || !(date instanceof Date) || isNaN(date)) return null;
        return `${date.getDate().toString().padStart(2, '0')}/${(date.getMonth() + 1).toString().padStart(2, '0')}/${date.getFullYear()}`;
    }

    function getWeekNumber(date) {
        const d = new Date(Date.UTC(date.getFullYear(), date.getMonth(), date.getDate()));
        d.setUTCDate(d.getUTCDate() + 4 - (d.getUTCDay() || 7));
        const yearStart = new Date(Date.UTC(d.getUTCFullYear(), 0, 1));
        return Math.ceil(((d - yearStart) / 86400000 + 1) / 7);
    }

    function formatDateForChart(dateStr) {
        const date = parseFrenchDate(dateStr);
        if (!date) return 'Unknown';
        const year = date.getFullYear();
        const week = getWeekNumber(date);
        return `${year}-W${week.toString().padStart(2, '0')}`;
    }

    function getJobTypeFullName(acronym) {
        const jobTypeMap = {
            'C': 'Corrective',
            'P': 'Planning',
            'CM': 'Corrective Maintenance',
            'PM': 'Preventive Maintenance',
            'OPS': 'Operation',
            'INSP': 'Inspection',
            'BDN': 'Breakdown',
            'SOP': 'Standard Operating Procedure',
            'KPI': 'Key Performance Indicator',
            'Eng': 'Engineer',
            'Sup': 'Supervisor',
            'Tech': 'Technician',
            'S': 'Safety'
        };
        return jobTypeMap[acronym] || acronym;
    }

    function formatTimeDuration(days) {
        if (days < 1) {
            const hours = Math.floor(days * 24);
            const minutes = Math.floor((days * 24 * 60) % 60);
            return {
                display: `${hours}h ${minutes}m`,
                unit: ''
            };
        } else {
            return {
                display: days.toFixed(1),
                unit: ' days'
            };
        }
    }

    // Theme management functions
    function setTheme(themeName) {
        // Remove all existing theme classes
        document.documentElement.classList.forEach(className => {
            if (className.startsWith('theme-')) {
                document.documentElement.classList.remove(className);
            }
        });
        
        // Add new theme class if not default
        if (themeName !== 'default') {
            document.documentElement.classList.add(`theme-${themeName}`);
            // Add dark variant if dark mode is enabled
            if (document.body.classList.contains('dark')) {
                document.documentElement.classList.add(`theme-${themeName}-dark`);
            }
        }
        
        // Save theme preference
        localStorage.setItem('theme', themeName);

        // Update UI elements
        updateUIForTheme(themeName);

        // Update charts if they exist
        if (currentData) {
            updateCharts(currentData.data);
        }
    }

    function updateUIForTheme(themeName) {
        // Get theme colors
        const style = getComputedStyle(document.documentElement);
        const primaryColor = style.getPropertyValue('--primary-color').trim();
        const secondaryColor = style.getPropertyValue('--secondary-color').trim();
        const accentColor = style.getPropertyValue('--accent-color').trim();
        
        // Update buttons
        document.querySelectorAll('.btn-primary').forEach(btn => {
            btn.style.backgroundColor = primaryColor;
        });
        
        document.querySelectorAll('.btn-secondary').forEach(btn => {
            btn.style.backgroundColor = secondaryColor;
        });
        
        // Update KPI cards
        document.querySelectorAll('.kpi-card').forEach(card => {
            card.style.borderLeft = `4px solid ${primaryColor}`;
        });
        
        // Update icons
        document.querySelectorAll('.fas').forEach(icon => {
            icon.style.color = accentColor;
        });
    }

    function initializeTheme() {
        const savedTheme = localStorage.getItem('theme') || 'default';
        const isDark = localStorage.getItem('darkMode') === 'true';

        // Set the theme selector value
        themeSelector.value = savedTheme;

        // Apply the theme
        setTheme(savedTheme);

        // Apply dark mode if needed
        if (isDark) {
            document.body.classList.add('dark');
            themeToggle.innerHTML = '<i class="fas fa-sun"></i>';
        }
    }

    // Event listeners for theme changes
    themeSelector.addEventListener('change', (e) => {
        setTheme(e.target.value);
    });

    themeToggle.addEventListener('click', function() {
        document.body.classList.toggle('dark');
        const isDark = document.body.classList.contains('dark');
        localStorage.setItem('darkMode', isDark);

        // Update icon
        themeToggle.innerHTML = isDark ?
            '<i class="fas fa-sun"></i>' :
            '<i class="fas fa-moon"></i>';

        // Update charts if they exist
        if (currentData) {
            updateCharts(currentData.data);
        }
    });

    // Initialize theme on page load
    initializeTheme();

    // Auto-load WO.xlsx on startup after a brief delay
    setTimeout(async function() {
        try {
            // Create a new input element
            const input = document.createElement('input');
            input.type = 'file';
            
            // Try to load WO.xlsx from the current directory
            const filePath = 'WO.xlsx';
            
            // Use FileReader to read the local file
            const reader = new FileReader();
            
            reader.onload = function(e) {
                try {
                    const data = new Uint8Array(e.target.result);
                    const workbook = XLSX.read(data, { type: 'array' });
                    
                    // Process the Excel data
                    const result = processExcelWorkbook(workbook);
                    
                    if (!result || !result.data || !result.charts) {
                        throw new Error('Invalid data structure returned from processExcelWorkbook');
                    }

                    // Store data and update UI
                    currentData = result;
                    dashboard.classList.remove('hidden');
                    
                    if (result.kpis) {
                        renderKPIs(result.kpis);
                    }
                    if (result.charts) {
                        renderCharts(result.charts);
                        if (result.charts.top_faults) {
                            renderTopFaults(result.charts.top_faults);
                        }
                    }
                    if (result.data) {
                        populateDropdowns(result.data);
                    }

                    // Show success notification
                    const notification = document.createElement('div');
                    notification.className = 'fixed top-4 right-4 bg-green-500 text-white px-6 py-3 rounded shadow-lg z-50';
                    notification.textContent = 'WO.xlsx loaded successfully';
                    document.body.appendChild(notification);
                    setTimeout(() => notification.remove(), 3000);

                } catch (error) {
                    console.error('Error processing Excel data:', error);
                    showErrorAndOpenModal();
                }
            };

            reader.onerror = function() {
                console.error('Error reading file');
                showErrorAndOpenModal();
            };

            // Check if file exists using XMLHttpRequest (works for local files)
            const xhr = new XMLHttpRequest();
            xhr.open('GET', filePath, true);
            xhr.onload = function() {
                if (xhr.status === 200) {
                    // File exists, read it
                    const blob = new Blob([xhr.response], { type: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' });
                    reader.readAsArrayBuffer(blob);
                } else {
                    showErrorAndOpenModal();
                }
            };
            xhr.onerror = function() {
                showErrorAndOpenModal();
            };
            xhr.responseType = 'arraybuffer';
            xhr.send();

        } catch (error) {
            console.error('Error auto-loading WO.xlsx:', error);
            showErrorAndOpenModal();
        }
    }, 1000); // 1 second delay

    // Helper function to show error and open modal
    function showErrorAndOpenModal() {
        // Show error notification
        const notification = document.createElement('div');
        notification.className = 'fixed top-4 right-4 bg-red-500 text-white px-6 py-3 rounded shadow-lg z-50';
        notification.textContent = 'Could not auto-load WO.xlsx. Please select the file manually.';
        document.body.appendChild(notification);
        setTimeout(() => notification.remove(), 5000);
        
        // Show the upload modal for manual file selection
        const uploadModal = document.getElementById('uploadModal');
        if (uploadModal) {
            uploadModal.classList.remove('hidden');
        }
    }

    // Sidebar toggle
    const sidebar = document.getElementById('sidebar');
    const sidebarToggle = document.getElementById('sidebarToggle');
    const sidebarCloseBtn = document.getElementById('sidebarCloseBtn');
    const mainContent = document.querySelector('main');

    // Event listeners for sidebar toggle
    sidebarToggle.addEventListener('click', () => {
        const isCollapsed = sidebar.classList.contains('collapsed');
        toggleSidebar(isCollapsed);
    });

    sidebarCloseBtn.addEventListener('click', () => {
        toggleSidebar(false);
    });

    function toggleSidebar(show) {
        if (show) {
            sidebar.classList.remove('collapsed');
            mainContent.classList.remove('sidebar-collapsed');
        } else {
            sidebar.classList.add('collapsed');
            mainContent.classList.add('sidebar-collapsed');
        }
    }

    // Modal handling
    const uploadModal = document.getElementById('uploadModal');
    const openUploadBtn = document.getElementById('openUpload');
    const closeUploadBtn = document.getElementById('closeUpload');
    const uploadForm = document.getElementById('uploadForm');

    // Show modal
    openUploadBtn.addEventListener('click', function() {
        uploadModal.classList.remove('hidden');
    });

    // Hide modal
    closeUploadBtn.addEventListener('click', function() {
        uploadModal.classList.add('hidden');
    });

    // ODC Modal handling
    const odcModal = document.getElementById('odcModal');
    const openODCBtn = document.getElementById('openODC');
    const closeODCBtn = document.getElementById('closeODC');
    const odcForm = document.getElementById('odcForm');

    // Show ODC modal
    openODCBtn.addEventListener('click', function() {
        odcModal.classList.remove('hidden');
    });

    // Hide ODC modal
    closeODCBtn.addEventListener('click', function() {
        odcModal.classList.add('hidden');
    });

    // Close modal when clicking outside
    window.addEventListener('click', function(event) {
        if (event.target === uploadModal) {
            uploadModal.classList.add('hidden');
        }
        if (event.target === odcModal) {
            odcModal.classList.add('hidden');
        }
    });

    const excelFileInput = document.getElementById('excelFile');
    const dashboard = document.getElementById('dashboard');
    const exportBtn = document.getElementById('exportBtn');
    const refreshBtn = document.getElementById('refreshBtn');

    // Setup drag and drop
    const dropZone = document.querySelector('.border-dashed');
    
    // Prevent default drag behaviors
    ['dragenter', 'dragover', 'dragleave', 'drop'].forEach(eventName => {
        dropZone.addEventListener(eventName, preventDefaults, false);
        document.body.addEventListener(eventName, preventDefaults, false);
    });

    // Highlight drop zone when file is dragged over it
    ['dragenter', 'dragover'].forEach(eventName => {
        dropZone.addEventListener(eventName, highlight, false);
    });

    ['dragleave', 'drop'].forEach(eventName => {
        dropZone.addEventListener(eventName, unhighlight, false);
    });

    // Handle dropped files
    dropZone.addEventListener('drop', handleDrop, false);

    function preventDefaults (e) {
        e.preventDefault();
        e.stopPropagation();
    }

    function highlight(e) {
        dropZone.classList.add('border-blue-500', 'bg-blue-50', 'dark:bg-blue-900/20');
    }

    function unhighlight(e) {
        dropZone.classList.remove('border-blue-500', 'bg-blue-50', 'dark:bg-blue-900/20');
    }

    function handleDrop(e) {
        const dt = e.dataTransfer;
        const file = dt.files[0];
        
        if (file && (file.name.endsWith('.xlsx') || file.name.endsWith('.xls'))) {
            excelFileInput.files = dt.files;
            // Trigger form submission
            uploadForm.dispatchEvent(new Event('submit'));
        } else {
            alert('Please upload an Excel file (.xlsx or .xls)');
        }
    }

    // Show selected filename
    excelFileInput.addEventListener('change', function(e) {
        const fileName = e.target.files[0]?.name;
        if (fileName) {
            const fileNameDisplay = dropZone.querySelector('p.text-xs');
            fileNameDisplay.textContent = `Selected file: ${fileName}`;
        }
    });

    // Handle file selection and auto-process sheets
    uploadForm.addEventListener('submit', function(e) {
        e.preventDefault();
        const file = excelFileInput.files[0];
        if (!file) {
            alert('Please select a file first');
            return;
        }

        const loadingSpinner = document.getElementById('loadingSpinner');
        const statusMessage = document.getElementById('statusMessage');

        try {
            loadingSpinner.classList.remove('hidden');
            statusMessage.textContent = 'Reading Excel file...';

            const reader = new FileReader();

            reader.onload = function(e) {
                try {
                    statusMessage.textContent = 'Processing Excel data...';
                    const data = new Uint8Array(e.target.result);
                    const workbook = XLSX.read(data, { type: 'array' });
                    
                    // Process the Excel data
                    const result = processExcelWorkbook(workbook);
                    
                    if (!result || !result.data || !result.charts) {
                        throw new Error('Invalid data structure returned from processExcelWorkbook');
                    }

                    // Store data and update UI
                    currentData = result;
                    dashboard.classList.remove('hidden');
                    uploadModal.classList.add('hidden');
                    
                    if (result.kpis) {
                        renderKPIs(result.kpis);
                    }
                    if (result.charts) {
                        renderCharts(result.charts);
                        if (result.charts.top_faults) {
                            renderTopFaults(result.charts.top_faults);
                        }
                    }
                    if (result.data) {
                        populateDropdowns(result.data);
                    }

                    loadingSpinner.classList.add('hidden');
                    
                    // Show success notification
                    const notification = document.createElement('div');
                    notification.className = 'fixed top-4 right-4 bg-green-500 text-white px-6 py-3 rounded shadow-lg z-50';
                    notification.textContent = 'File processed successfully';
                    document.body.appendChild(notification);
                    setTimeout(() => notification.remove(), 3000);

                } catch (error) {
                    console.error('Error processing Excel data:', error);
                    loadingSpinner.classList.add('hidden');
                    statusMessage.textContent = 'Error: ' + error.message;
                    alert('Error processing Excel data: ' + error.message);
                }
            };

            reader.onerror = function() {
                console.error('Error reading file');
                loadingSpinner.classList.add('hidden');
                alert('Error reading file');
            };

            reader.readAsArrayBuffer(file);
        } catch (error) {
            console.error('Error:', error);
            loadingSpinner.classList.add('hidden');
            alert('Error processing file');
        }
    });

    // Handle ODC form submission
    odcForm.addEventListener('submit', async function(e) {
        e.preventDefault();

        const loadingSpinner = document.getElementById('loadingSpinner');
        const statusMessage = document.getElementById('statusMessage');

        try {
            loadingSpinner.classList.remove('hidden');
            statusMessage.textContent = 'Connecting to ODC...';

            // Simulate ODC connection with sample data
            setTimeout(() => {
                try {
                    const result = generateSampleData();
                    
                    statusMessage.textContent = 'Generating visualizations...';
                    odcModal.classList.add('hidden');
                    dashboard.classList.remove('hidden');
                    
                    // Store data and update UI
                    currentData = result;
                    
                    if (result.kpis) {
                        renderKPIs(result.kpis);
                    }
                    if (result.charts) {
                        renderCharts(result.charts);
                        if (result.charts.top_faults) {
                            renderTopFaults(result.charts.top_faults);
                        }
                    }
                    if (result.data) {
                        populateDropdowns(result.data);
                    }

                    loadingSpinner.classList.add('hidden');

                    // Show success notification
                    const notification = document.createElement('div');
                    notification.className = 'fixed top-4 right-4 bg-green-500 text-white px-6 py-3 rounded shadow-lg z-50';
                    notification.textContent = 'Connected to ODC successfully';
                    document.body.appendChild(notification);
                    setTimeout(() => notification.remove(), 3000);

                } catch (error) {
                    console.error('Error processing ODC data:', error);
                    loadingSpinner.classList.add('hidden');
                    alert('Error processing ODC data: ' + error.message);
                }
            }, 1500);
        } catch (error) {
            console.error('ODC Connection Error:', error);
            loadingSpinner.classList.add('hidden');
            alert('Error connecting to ODC: ' + error.message);
        }
    });

    // Handle export
    exportBtn.addEventListener('click', function() {
        if (!currentData || !currentData.data || currentData.data.length === 0) {
            alert('No data available to export. Please load data first.');
            return;
        }

        try {
            // Create a new workbook
            const wb = XLSX.utils.book_new();
            
            // Convert data to worksheet
            const ws = XLSX.utils.json_to_sheet(currentData.data);
            
            // Add the worksheet to the workbook
            XLSX.utils.book_append_sheet(wb, ws, "Work Orders");

            // Generate Excel file and trigger download
            XLSX.writeFile(wb, "work_orders_export.xlsx");

            // Show success notification
            const notification = document.createElement('div');
            notification.className = 'fixed top-4 right-4 bg-green-500 text-white px-6 py-3 rounded shadow-lg z-50';
            notification.textContent = 'Data exported successfully';
            document.body.appendChild(notification);
            setTimeout(() => notification.remove(), 3000);

        } catch (error) {
            console.error('Export Error:', error);
            alert('Error exporting data: ' + error.message);
        }
    });

    // Handle refresh
    refreshBtn.addEventListener('click', function() {
        if (!currentData) {
            alert('No data to refresh. Please load data first.');
            return;
        }

        const loadingSpinner = document.getElementById('loadingSpinner');
        const statusMessage = document.getElementById('statusMessage');

        loadingSpinner.classList.remove('hidden');
        statusMessage.textContent = 'Refreshing data...';

        // Simulate refresh delay
        setTimeout(() => {
            loadingSpinner.classList.add('hidden');
            alert('Data refreshed successfully');
        }, 1500);
    });

    // Modify the filters section of the HTML
    const filtersContent = document.querySelector('.filters-content form');
    if (filtersContent) {
        // Add job status filter
        const jobStatusFilter = document.createElement('div');
        jobStatusFilter.innerHTML = `
            <div>
                <label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Job Status</label>
                <div class="flex space-x-2 mb-1">
                    <button type="button" class="text-xs bg-gray-200 hover:bg-gray-300 dark:bg-gray-600 dark:hover:bg-gray-500 px-2 py-1 rounded select-all-btn" data-target="jobStatusSelect">Select All</button>
                    <button type="button" class="text-xs bg-gray-200 hover:bg-gray-300 dark:bg-gray-600 dark:hover:bg-gray-500 px-2 py-1 rounded clear-all-btn" data-target="jobStatusSelect">Clear All</button>
                </div>
                <select class="w-full border rounded-lg px-3 py-2 dark:bg-gray-600 dark:border-gray-500" id="jobStatusSelect" multiple size="5"></select>
            </div>
        `;
        filtersContent.insertBefore(jobStatusFilter, filtersContent.firstChild);
    }

    function populateDropdowns(data) {
        // Get unique values for each field
        const jobTypes = [...new Set(data.map(item => item.Job_type).filter(Boolean))];
        const jobStatuses = [...new Set(data.map(item => item.JobstatusObs).filter(Boolean))];
        const costPurposes = [...new Set(data.map(item => item.Cost_purpose_key).filter(Boolean))];
        const failures = [...new Set(data.map(item => item.failure).filter(Boolean))];
        const equipmentTypes = [...new Set(data.map(item => item.EQ_type).filter(Boolean))];
        const faultLocations = [...new Set(data.map(item => item.faultlocation).filter(Boolean))];

        // Populate Equipment Type dropdown
        const eqTypeSelect = document.getElementById('eqTypeSelect');
        if (eqTypeSelect) {
            eqTypeSelect.innerHTML = '';
            equipmentTypes.forEach(type => {
                eqTypeSelect.innerHTML += `<option value="${type}" selected>${type}</option>`;
            });
        }

        // Populate Fault Location dropdown
        const faultLocationSelect = document.getElementById('faultLocationSelect');
        if (faultLocationSelect) {
            faultLocationSelect.innerHTML = '';
            faultLocations.forEach(location => {
                faultLocationSelect.innerHTML += `<option value="${location}" selected>${location}</option>`;
            });
        }

        // Populate Job Status dropdown
        const jobStatusSelect = document.getElementById('jobStatusSelect');
        if (jobStatusSelect) {
            jobStatusSelect.innerHTML = '';
            jobStatuses.forEach(status => {
                const statusInfo = getJobStatusFullName(status);
                jobStatusSelect.innerHTML += `<option value="${status}" selected>${statusInfo.description} (${status})</option>`;
            });
        }

        // Populate Job Type dropdown with full names
        const jobTypeSelect = document.getElementById('jobTypeSelect');
        if (jobTypeSelect) {
            jobTypeSelect.innerHTML = '';
            jobTypes.forEach(type => {
                const fullName = getJobTypeFullName(type);
                jobTypeSelect.innerHTML += `<option value="${type}" selected>${fullName} (${type})</option>`;
            });
        }

        // Populate Cost Purpose dropdown
        const costPurposeSelect = document.getElementById('costPurposeSelect');
        if (costPurposeSelect) {
            costPurposeSelect.innerHTML = '';
            costPurposes.forEach(purpose => {
                costPurposeSelect.innerHTML += `<option value="${purpose}" selected>${purpose}</option>`;
            });
        }

        // Populate Failure dropdown
        const failureSelect = document.getElementById('failureSelect');
        if (failureSelect) {
            failureSelect.innerHTML = '';
            failures.forEach(failure => {
                failureSelect.innerHTML += `<option value="${failure}" selected>${failure}</option>`;
            });
        }
    }

    // Handle Select All/Clear All buttons
    document.addEventListener('click', function(e) {
        if (e.target.classList.contains('select-all-btn')) {
            const targetId = e.target.getAttribute('data-target');
            const select = document.getElementById(targetId);
            Array.from(select.options).forEach(option => {
                option.selected = true;
            });
            applyFilters(); // Trigger immediate update
        } else if (e.target.classList.contains('clear-all-btn')) {
            const targetId = e.target.getAttribute('data-target');
            const select = document.getElementById(targetId);
            Array.from(select.options).forEach(option => {
                option.selected = false;
            });
            applyFilters(); // Trigger immediate update
        }
    });

    function renderTopFaults(faults) {
        const tableBody = document.getElementById('faultsTableBody');
        tableBody.innerHTML = '';

        const total = Object.values(faults).reduce((sum, count) => sum + count, 0);

        Object.entries(faults).forEach(([fault, count]) => {
            const percentage = ((count / total) * 100).toFixed(1);
            const trend = Math.random() > 0.5 ? 'up' : 'down';
            const trendColor = trend === 'up' ? 'text-red-500' : 'text-green-500';

            const row = document.createElement('tr');
            row.innerHTML = `
                <td class="px-6 py-4 whitespace-nowrap text-sm font-medium text-gray-800 dark:text-gray-200">${fault}</td>
                <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-500 dark:text-gray-300">${count}</td>
                <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-500 dark:text-gray-300">${percentage}%</td>
                <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-500 dark:text-gray-300">
                    <span class="${trendColor}">
                        <i class="fas fa-arrow-${trend} mr-1"></i>
                        ${(Math.random() * 10).toFixed(1)}%
                    </span>
                </td>
            `;
            tableBody.appendChild(row);
        });
    }

    // Add job type mapping function
    function getJobTypeFullName(acronym) {
        const jobTypeMap = {
            'C': 'Corrective',
            'P': 'Planning',
            'CM': 'Corrective Maintenance',
            'PM': 'Preventive Maintenance',
            'OPS': 'Operation',
            'INSP': 'Inspection',
            'BDN': 'Breakdown',
            'SOP': 'Standard Operating Procedure',
            'KPI': 'Key Performance Indicator',
            'Eng': 'Engineer',
            'Sup': 'Supervisor',
            'Tech': 'Technician',
            'S': 'Safety',
            'I': 'Inspection',
            'O': 'Accedint',
            'U':'Unplanned'
        };
        return jobTypeMap[acronym] || acronym;
    }

    // Add job status mapping function
    function getJobStatusFullName(code) {
        const statusMap = {
            0: { obs: 'TEM', description: 'Template' },
            1: { obs: 'PAS', description: 'Passive' },
            2: { obs: 'ORD', description: 'Job ordered' },
            3: { obs: 'INI', description: 'Job Initial status - Default' },
            4: { obs: 'LAT', description: 'Looked at' },
            5: { obs: 'RDY', description: 'Job ready for execution' },
            6: { obs: 'IPG', description: 'Job in progress' },
            7: { obs: 'STP', description: 'Job stopped' },
            8: { obs: 'WSP', description: 'Waiting for spare parts' },
            9: { obs: 'RNF', description: 'Registration not finished' },
            10: { obs: 'FIN', description: 'Technically finished' },
            50: { obs: 'WPP', description: 'Pending spare parts PSTL' },
            55: { obs: 'WSL', description: 'Pending spare parts LCT' },
            60: { obs: 'ODE', description: 'Ops Delay' },
            65: { obs: 'WFT', description: 'Tools/Service equipment' },
            70: { obs: 'ITS', description: 'IT Support' }
        };
        
        // If code is a number and exists in the map, return both obs and description
        if (!isNaN(code) && statusMap[code]) {
            return statusMap[code];
        }
        // If code is a string (obs code), try to find matching status
        const numericKey = Object.keys(statusMap).find(key => statusMap[key].obs === code);
        if (numericKey) {
            return statusMap[numericKey];
        }
        // Return default if no match found
        return { obs: code, description: code };
    }

    // Add equipment selection handler
    const equipmentSelect = document.getElementById('equipmentSelect');
    if (equipmentSelect) {
        equipmentSelect.addEventListener('change', function() {
            if (!currentData) {
                alert('Please load data first before filtering.');
                return;
            }

            const selectedEquipment = equipmentSelect.value;
            let filteredData = currentData.data;

            if (selectedEquipment !== 'all') {
                filteredData = currentData.data.filter(item => {
                    if (selectedEquipment === 'STS') {
                        return item.MO_key && item.MO_key.toUpperCase().includes('STS');
                    } else if (selectedEquipment === 'SPR') {
                        const moKey = item.MO_key && item.MO_key.toUpperCase();
                        return moKey && moKey.includes('SPR') && isValidSpreaderNumber(moKey);
                    }
                    return true;
                });
            }

            updateCharts(filteredData);
        });
    }

    function getStatusCounts(data) {
        const finishedStatuses = ['FIN']; // Completed work orders
        const waitingStatuses = ['ORD', 'INI', 'LAT', 'RDY']; // Waiting work orders

        return {
            finished: data.filter(item => finishedStatuses.includes(item.JobstatusObs)).length,
            waiting: data.filter(item => waitingStatuses.includes(item.JobstatusObs)).length,
            total: data.length
        };
    }

    // Add helper functions for determining equipment type and fault location
    function getEquipmentType(moKey) {
        if (!moKey) return 'Other';
        
        moKey = moKey.toUpperCase();
        if (moKey.includes('STS')) {
            return 'STS Crane';
        } else if (moKey.includes('SPS') || moKey.includes('SPR')) {
            return 'Spreader';
        } else {
            return 'Other';
        }
    }

    function getFaultLocation(moKey) {
        if (!moKey) return 'Other';
        
        moKey = moKey.toUpperCase();
        if (moKey.includes('MNH')) {
            return 'HOIST';
        } else if (moKey.includes('BMH')) {
            return 'BOOM';
        } else if (moKey.includes('HDB')) {
            return 'SPREADER';
        } else if (moKey.includes('GAN')) {
            return 'GANTRY';
        } else if (moKey.includes('ELE')) {
            return 'ELECTRICAL';
        } else if (moKey.includes('TRL')) {
            return 'TROLLEY';
        } else if (moKey.includes('LIG')) {
            return 'LIGHTING';
        } else if (moKey.includes('CAB')) {
            return 'OPERATOR CABIN';
        } else if (moKey.includes('HYD')) {
            return 'HYDRAULIC';
        } else if (moKey.includes('FES')) {
            return 'FESTOON';
        } else if (moKey.includes('ELV')) {
            return 'ELEVATOR';
        } else if (moKey.includes('TRM') || moKey.includes('TLS')) {
            return 'TLS';
        } else if (moKey.includes('SLE')) {
            return 'Machine room';
        } else {
            return 'Other';
        }
    }

    // Add helper functions for determining failure type and snag location
    function determineFailure(cause) {
        if (!cause) return null;
        const d = String(cause).trim().toUpperCase();
        
        if (d.includes("TWIN")) return "twin";
        if (d.includes("TELESC")) return "Telescopy";
        if (d.includes("NOISE") || d.includes("DAMAGE") || d.includes("BRUIT") || 
            d.includes("ENDOMMA") || d.includes("VIBRE")) return "Mech fail";
        if (d.includes("AC FAULT")) return "A/C";
        if (d.includes("SIÈGE") || d.includes("SEAT")) return "operator seat";
        if (d.includes("CRANE OF")) return "Crane off";
        if (d.includes("DRIVE OF") || d.includes("CONTROL OF") || 
            d.includes("CONTROLE OF") || d.includes("ALM")) return "Drive Off";
        if (d.includes("POWER")) return "Power cut off";
        if (d.includes("ROOF") || d.includes("TTDS") || d.includes("SPREADER READY")) return "spreader ready intrlck";
        if (d.includes("DOMMAGE")) return "Incident_Dommage";
        if (d.includes("HOIST BRAKE") || d.includes("HOIST SERVICE BRAKE")) return "Hoist service brake";
        if (d.includes("HOIST EMERG")) return "Hoist Emergency brake";
        if (d.includes("HDB") || d.includes("HEADBLOCK")) return "Headblock";
        if (d.includes("FESTOON")) return "Festoon";
        if (d.includes("HOIST SLOW")) return "Hoist slowdown";
        if (d.includes("AFFICHEUR")) return "Display";
        if (d.includes("GANTRY DRIVE")) return "Gantry drive";
        if (d.includes("GANTRY POSITION")) return "Gantry position";
        if (d.includes("GANTRY WHEEL")) return "Gantry wheel brake";
        if (d.includes("GANTRY BRAKE")) return "Gantry brake";
        if (d.includes("GANTRY ENCODER")) return "Gantry encoder";
        if (d.includes("GANTRY MOTOR")) return "Gantry motor";
        if (d.includes("TROLLEY DRIVE")) return "Trolley drive";
        if (d.includes("TROLLEY POSITION")) return "Trolley position";
        if (d.includes("TROLLEY BRAKE")) return "Trolley brake";
        if (d.includes("TROLLEY GATE")) return "Trolley gate";
        if (d.includes("TROLLEY ROPE")) return "Trolley rope tension";
        if (d.includes("HOIST DRIVE")) return "hoist drive";
        if (d.includes("HOIST POSITION")) return "hoist position";
        if (d.includes("HOIST WIRE")) return "hoist wire rope";
        if (d.includes("HOIST BRAKE")) return "Hoist brake";
        if (d.includes("HOIST ENCODER")) return "Hoist encoder";
        if (d.includes("HOIST MOTOR")) return "Hoist motor";
        if (d.includes("GCR")) return "GCR";
        if (d.includes("SCR") || d.includes("SPREADER CABLE REEL")) return "SCR";
        if (d.includes("BAD STACK") || d.includes("COINC") || d.includes("SPREADER BLOQUÉ") || 
            d.includes("SPREADER ACCROCHÉ") || d.includes("STUCK")) return "Stuck";
        if (d.includes("BLINK FAULT") || d.includes("COMMUNICA") || 
            d.includes("COMUNICA") || d.includes("LIGHT BLINK")) return "Communication";
        if (d.includes("BOOM ISSUE") || d.includes("BOOM FAULT") || d.includes("BOOM INV")) return "Boom Drive";
        if (d.includes("BOOM LEVEL") || d.includes("BOOM DOWN") || 
            d.includes("BOOM UP") || d.includes("NO BOOM")) return "Boom position";
        if (d.includes("TLS")) return "TLS fault";
        if (d.includes("CHANGE") || d.includes("CHANGEMENT")) return "spreader change";
        if (d.includes("JOYSTICK") || d.includes("JOYSTI")) return "Joystick fault";
        if (d.includes("CONNECTOR") || d.includes("PLUG")) return "spreader plug";
        if (d.includes("FUITE D'HUILE") || d.includes("OIL LEAK")) return "oil leakage";
        if (d.includes("DÉVÉRROU") || d.includes("VÉRROU") || d.includes("LOCK FAULT") || 
            d.includes("UNLOCK") || d.includes("UNLOPK") || d.includes("LOCKING FAULT")) return "Lock/unlock";
        if (d.includes("FLIPPER")) return "Flipper";
        if (d.includes("TELESCOP") || d.includes("TELECO") || d.includes("TELSCO")) return "Telescopic";
        if (d.includes("LIGHTS") || d.includes("LIGHT") || d.includes("LIGHT FAULT") || 
            d.includes("LIGHT ISSUE") || d.includes("LIGHT OFF") || 
            d.includes("LAMPE") || d.includes("FLOODLIGHT")) return "Light";
        if (d.includes("POMPE SPREADER") || d.includes("SPREADER PUMP") || d.includes("PUMP")) return "spreader pump";
        return null;
    }

    function determineSnagLocation(fault) {
        if (!fault) return "";
        const s = String(fault).toUpperCase();
        if (!s.includes("SNAG")) return "";
        
        if (s.includes("FAULT #0")) return "loadCell";
        if (s.includes("FAULT #1") && s.includes("FAULT #2") && 
            s.includes("FAULT #3") && s.includes("FAULT #4")) return "Cylinder 1234";
        if (s.includes("FAULT #1") && s.includes("FAULT #2") && s.includes("FAULT #3")) return "Cylinder 123";
        if (s.includes("FAULT #1") && s.includes("FAULT #2") && s.includes("FAULT #4")) return "Cylinder 124";
        if (s.includes("FAULT #2") && s.includes("FAULT #3") && s.includes("FAULT #4")) return "Cylinder 234";
        if (s.includes("FAULT #1") && s.includes("FAULT #2")) return "Cylinder 12";
        if (s.includes("FAULT #1") && s.includes("FAULT #3")) return "Cylinder 13";
        if (s.includes("FAULT #1") && s.includes("FAULT #4")) return "Cylinder 14";
        if (s.includes("FAULT #2") && s.includes("FAULT #3")) return "Cylinder 23";
        if (s.includes("FAULT #2") && s.includes("FAULT #4")) return "Cylinder 24";
        if (s.includes("FAULT #3") && s.includes("FAULT #4")) return "Cylinder 34";
        if (s.includes("FAULT #1")) return "Cylinder 1";
        if (s.includes("FAULT #2")) return "Cylinder 2";
        if (s.includes("FAULT #3")) return "Cylinder 3";
        if (s.includes("FAULT #4")) return "Cylinder 4";
        return "";
    }

    function isValidSpreaderNumber(spreaderName) {
        // Extract the number part from spreader name (e.g., "SPR102" -> 102)
        const match = spreaderName.match(/SPR(\d+)/i);
        if (!match) return false;
        
        const number = parseInt(match[1], 10);
        // Include spreaders with numbers < 100 or > 200
        return number < 100 || number > 200;
    }

    // Process Excel workbook and extract data
    function processExcelWorkbook(workbook) {
        if (!workbook || !workbook.Sheets) {
            throw new Error('Invalid workbook format');
        }

        // Initialize empty result structure
        let result = {
            data: [],
            kpis: {
                total_wo: 0,
                avg_processing_time: 0,
                closed_count: 0,
                waiting_count: 0,
                top_job_type: { obs: 'None', description: 'None', count: 0 },
                top_status: { obs: 'None', description: 'None', count: 0 }
            },
            charts: {
                wo_created: {},
                wo_closed: {},
                eq_type_dist: {},
                wo_per_location: {},
                wo_per_cause: {},
                status_dist: {},
                job_type_dist: {},
                top_faults: {}
            }
        };

        let combinedData = [];

        try {
            // Try to read from specific sheets first
            if (workbook.Sheets['WO active']) {
                const activeData = XLSX.utils.sheet_to_json(workbook.Sheets['WO active']) || [];
                combinedData = [...combinedData, ...activeData.map(row => ({ ...row, Jobstatus: 'active' }))];
            }

            if (workbook.Sheets['WO history']) {
                const historyData = XLSX.utils.sheet_to_json(workbook.Sheets['WO history']) || [];
                combinedData = [...combinedData, ...historyData.map(row => ({ ...row, Jobstatus: 'history' }))];
            }

            // If no data found in specific sheets, try the first available sheet
            if (combinedData.length === 0 && workbook.SheetNames.length > 0) {
                const firstSheet = workbook.SheetNames[0];
                const sheetData = XLSX.utils.sheet_to_json(workbook.Sheets[firstSheet]) || [];
                combinedData = sheetData.map(row => ({ ...row, Jobstatus: 'active' }));
            }

            if (combinedData.length === 0) {
                throw new Error('No data found in the Excel file');
            }

            // Initialize the result structure first
            let result = {
                data: [],
                kpis: {
                    total_wo: 0,
                    avg_processing_time: 0,
                    closed_count: 0,
                    waiting_count: 0,
                    top_job_type: { obs: 'None', description: 'None', count: 0 },
                    top_status: { obs: 'None', description: 'None', count: 0 }
                },
                charts: {
                    wo_created: {},
                    wo_closed: {},
                    eq_type_dist: {},
                    wo_per_location: {},
                    wo_per_cause: {},
                    status_dist: {},
                    job_type_dist: {},
                    top_faults: {}
                }
            };

            // Map and clean the data
            const processedData = combinedData.map((row, index) => {
                // Try to parse dates with better error handling
                let orderDate = null;
                let execDate = null;

                try {
                    if (row.Order_date !== undefined && row.Order_date !== null) {
                        if (typeof row.Order_date === 'number') {
                            // Handle Excel numeric dates
                            orderDate = parseFrenchDate(row.Order_date);
                        } else {
                            // Handle string dates
                            orderDate = parseFrenchDate(row.Order_date);
                        }
                    }

                    if (row.Jobexec_dt !== undefined && row.Jobexec_dt !== null) {
                        if (typeof row.Jobexec_dt === 'number') {
                            // Handle Excel numeric dates
                            execDate = parseFrenchDate(row.Jobexec_dt);
                        } else {
                            // Handle string dates
                            execDate = parseFrenchDate(row.Jobexec_dt);
                        }
                    }
                } catch (e) {
                    console.warn('Date parsing error:', e, 'for row:', row);
                }

                // Extract equipment name based on MO_key
                let EQ_name = '';
                if (row.MO_key) {
                    const moKey = row.MO_key.toUpperCase();
                    if (moKey.includes('STS')) {
                        EQ_name = moKey.slice(0, 5); // First 5 characters for STS
                    } else if (moKey.includes('SPR') || moKey.includes('SPS')) {
                        // Only include spreaders with valid numbers (< 100 or > 200)
                        if (isValidSpreaderNumber(moKey)) {
                            EQ_name = moKey.slice(0, 6); // First 6 characters for Spreader
                        }
                    }
                }

                if (!EQ_name) return null; // Skip invalid equipment

                // Determine equipment type and fault location
                const equipmentType = getEquipmentType(row.MO_key);
                const faultLocation = getFaultLocation(row.MO_key);
                
                return {
                    ...row,
                    EQ_name,
                    WO_key: row.WO_key || row.wo_key || `WO${280000 + index}`,
                    WO_name: row.WO_name || row.wo_name || `Work Order ${280000 + index}`,
                    Description: row.Description || row.description || `Description for WO ${280000 + index}`,
                    MO_key: row.MO_key || row.mo_key || `MO${Math.floor(Math.random() * 10000)}`,
                    Jobstatus: row.Jobstatus || 'active',
                    EQ_type: equipmentType,
                    faultlocation: faultLocation,
                    Job_type: row.Job_type || row.job_type || row.type || 'Repair',
                    Cost_purpose_key: row.Cost_purpose_key || row.cost_purpose || row.purpose || 'Corrective',
                    Order_date: orderDate ? formatToFrenchDate(orderDate) : null,
                    Jobexec_dt: execDate ? formatToFrenchDate(execDate) : null
                };
            }).filter(Boolean); // Remove null entries (invalid spreaders)

            if (processedData.length === 0) {
                throw new Error('No valid data rows found after processing');
            }

            // Process the data and return the result
            const resultData = processData(processedData);
            
            // Update time period information
            if (resultData.data) {
                updateTimePeriodInfo(resultData.data);
            }

            // Return the complete result
            return resultData;

        } catch (error) {
            console.error('Error processing Excel workbook:', error);
            throw new Error(`Error processing Excel data: ${error.message}`);
        }
    }

    function renderKPIs(kpis) {
        const kpiSection = document.getElementById('kpiSection');
        if (!kpiSection || !kpis) return;
        
        kpiSection.innerHTML = `
            <div class="kpi-card bg-white dark:bg-gray-700 rounded-lg shadow-sm p-4">
                <div class="flex items-center justify-between">
                    <div>
                        <p class="text-sm font-medium text-gray-500 dark:text-gray-400">Total Work Orders</p>
                        <p class="text-2xl font-semibold text-blue-600 dark:text-blue-400">${kpis.total_wo}</p>
                    </div>
                    <div class="p-3 rounded-full bg-blue-100 dark:bg-blue-900 text-blue-600 dark:text-blue-400">
                        <i class="fas fa-clipboard-list"></i>
                    </div>
                </div>
                <div class="mt-4 flex items-center justify-between text-xs text-gray-500 dark:text-gray-400">
                    <span>${kpis.waiting_count} waiting</span>
                    <span class="text-green-500 dark:text-green-400">${kpis.closed_count} closed</span>
                </div>
            </div>

            <div class="kpi-card bg-white dark:bg-gray-700 rounded-lg shadow-sm p-4">
                <div class="flex items-center justify-between">
                    <div>
                        <p class="text-sm font-medium text-gray-500 dark:text-gray-400">Average Processing Days</p>
                        <p class="text-2xl font-semibold text-green-600 dark:text-green-400">${kpis.avg_processing_time.toFixed(1)}</p>
                    </div>
                    <div class="p-3 rounded-full bg-green-100 dark:bg-green-900 text-green-600 dark:text-green-400">
                        <i class="fas fa-clock"></i>
                    </div>
                </div>
                <div class="mt-4 flex items-center justify-between text-xs text-gray-500 dark:text-gray-400">
                    <span>Target: 5 days</span>
                </div>
            </div>

            <div class="kpi-card bg-white dark:bg-gray-700 rounded-lg shadow-sm p-4">
                <div class="flex items-center justify-between">
                    <div>
                        <p class="text-sm font-medium text-gray-500 dark:text-gray-400">Top Job Type</p>
                        <p class="text-2xl font-semibold text-purple-600 dark:text-purple-400">${kpis.top_job_type.obs}</p>
                    </div>
                    <div class="p-3 rounded-full bg-purple-100 dark:bg-purple-900 text-purple-600 dark:text-purple-400">
                        <i class="fas fa-tasks"></i>
                    </div>
                </div>
                <div class="mt-4 flex items-center justify-between text-xs text-gray-500 dark:text-gray-400">
                    <span>${kpis.top_job_type.description}</span>
                    <span>${kpis.top_job_type.count} WOs</span>
                </div>
            </div>

            <div class="kpi-card bg-white dark:bg-gray-700 rounded-lg shadow-sm p-4">
                <div class="flex items-center justify-between">
                    <div>
                        <p class="text-sm font-medium text-gray-500 dark:text-gray-400">Top Status</p>
                        <p class="text-2xl font-semibold text-orange-600 dark:text-orange-400">${kpis.top_status.obs}</p>
                    </div>
                    <div class="p-3 rounded-full bg-orange-100 dark:bg-orange-900 text-orange-600 dark:text-orange-400">
                        <i class="fas fa-chart-pie"></i>
                    </div>
                </div>
                <div class="mt-4 flex items-center justify-between text-xs text-gray-500 dark:text-gray-400">
                    <span>${kpis.top_status.description}</span>
                    <span>${kpis.top_status.count} WOs</span>
                </div>
            </div>
        `;
    }

    // Process data to calculate KPIs and chart data
    function processData(data) {
        // Validate input data
        if (!data) {
            throw new Error('No data provided to process');
        }

        if (!Array.isArray(data)) {
            throw new Error('Data must be an array');
        }

        if (data.length === 0) {
            throw new Error('Data array is empty');
        }

        try {
            // Ensure each item in data array is an object with required properties
            data = data.filter(item => {
                if (!item || typeof item !== 'object') return false;
                return true;
            });

            if (data.length === 0) {
                throw new Error('No valid data objects found in array');
            }

            // Map numeric status codes to their descriptions with error handling
            data = data.map(item => {
                try {
                    // Process job status
                    if (item.Jobstatus) {
                        const status = getJobStatusFullName(item.Jobstatus);
                        item = {
                            ...item,
                            JobstatusCode: item.Jobstatus,
                            JobstatusObs: status.obs || 'Unknown',
                            JobstatusDescription: status.description || 'Unknown'
                        };
                    }

                    // Extract equipment name based on MO_key
                    let EQ_name = 'Unknown';
                    if (item.MO_key) {
                        const moKey = item.MO_key.toUpperCase();
                        if (moKey.includes('STS')) {
                            EQ_name = moKey.substring(0, 5);  // Get first 5 characters for STS
                        } else if (moKey.includes('SPR')) {
                            EQ_name = moKey.substring(0, 6);  // Get first 6 characters for Spreader
                        }
                    }
                    item.EQ_name = EQ_name;
                    
                    // Extract failure from WO_name and Description
                    const combinedText = `${item.WO_name || ''} ${item.Description || ''}`;
                    item.failure = determineFailure(combinedText) || 'Unknown';
                    
                    return item;
                } catch (e) {
                    console.warn('Error processing item:', e);
                    return {
                        ...item,
                        JobstatusCode: item.Jobstatus,
                        JobstatusObs: 'Unknown',
                        JobstatusDescription: 'Unknown',
                        failure: 'Unknown',
                        EQ_name: 'Unknown'
                    };
                }
            });

            // Initialize all required objects with default values
            const result = {
                data: data,
                kpis: {
                    total_wo: 0,
                    avg_processing_time: 0,
                    closed_count: 0,
                    waiting_count: 0,
                    top_job_type: { obs: 'None', description: 'None', count: 0 },
                    top_status: { obs: 'None', description: 'None', count: 0 }
                },
                charts: {
                    wo_created: {},
                    wo_closed: {},
                    eq_type_dist: {},
                    wo_per_location: {},
                    wo_per_cause: {},
                    status_dist: {},
                    job_type_dist: {},
                    top_faults: {}
                }
            };

            // Safely filter closed and waiting WOs
            const closed_wo = data.filter(item => item && item.Jobexec_dt);
            const waiting_wo = data.filter(item => item && !item.Jobexec_dt);

            // Calculate KPIs with validation
            result.kpis.total_wo = data.length;
            result.kpis.closed_count = closed_wo.length;
            result.kpis.waiting_count = waiting_wo.length;

            // Calculate average processing time safely
            if (closed_wo.length > 0) {
                const validProcessingTimes = closed_wo
                    .map(item => {
                        try {
                            const execDate = parseFrenchDate(item.Jobexec_dt);
                            const orderDate = parseFrenchDate(item.Order_date);
                            if (!isNaN(execDate.getTime()) && !isNaN(orderDate.getTime())) {
                                return (execDate - orderDate) / (1000 * 60 * 60 * 24);
                            }
                            return null;
                        } catch (e) {
                            console.warn('Error calculating processing time:', e);
                            return null;
                        }
                    })
                    .filter(time => time !== null);

                if (validProcessingTimes.length > 0) {
                    result.kpis.avg_processing_time = validProcessingTimes.reduce((a, b) => a + b, 0) / validProcessingTimes.length;
                }
            }

            // Calculate distributions with error handling
            const distributions = {
                jobType: {},
                status: {},
                equipment: {},
                location: {},
                cause: {}
            };

            data.forEach(item => {
                try {
                    // Job Type Distribution
                    const jobType = item.Job_type || 'Unknown';
                    distributions.jobType[jobType] = (distributions.jobType[jobType] || 0) + 1;

                    // Status Distribution
                    const status = item.JobstatusObs || 'Unknown';
                    distributions.status[status] = (distributions.status[status] || 0) + 1;

                    // Equipment Type Distribution
                    const eqType = item.EQ_type || 'Other';
                    distributions.equipment[eqType] = (distributions.equipment[eqType] || 0) + 1;

                    // Location Distribution
                    const location = item.faultlocation || 'Other';
                    distributions.location[location] = (distributions.location[location] || 0) + 1;

                    // Cause Distribution
                    const cause = item.failure || 'Unknown';
                    distributions.cause[cause] = (distributions.cause[cause] || 0) + 1;

                    // WO Created Distribution
                    if (item.Order_date) {
                        const createdDate = formatDateForChart(item.Order_date);
                        result.charts.wo_created[createdDate] = (result.charts.wo_created[createdDate] || 0) + 1;
                    }

                    // WO Closed Distribution
                    if (item.Jobexec_dt) {
                        const closedDate = formatDateForChart(item.Jobexec_dt);
                        result.charts.wo_closed[closedDate] = (result.charts.wo_closed[closedDate] || 0) + 1;
                    }
                } catch (e) {
                    console.warn('Error processing distribution data:', e);
                }
            });

            // Find top job type and status
            const topJobType = Object.entries(distributions.jobType)
                .sort((a, b) => b[1] - a[1])[0] || ['None', 0];
            const topStatus = Object.entries(distributions.status)
                .sort((a, b) => b[1] - a[1])[0] || ['None', 0];

            // Update KPIs with top distributions
            result.kpis.top_job_type = {
                obs: topJobType[0],
                description: getJobTypeFullName(topJobType[0]),
                count: topJobType[1]
            };

            result.kpis.top_status = {
                obs: topStatus[0],
                description: getJobStatusFullName(topStatus[0]).description,
                count: topStatus[1]
            };

            // Update charts data
            result.charts.eq_type_dist = distributions.equipment;
            result.charts.wo_per_location = distributions.location;
            result.charts.wo_per_cause = distributions.cause;
            result.charts.status_dist = Object.entries(distributions.status)
                .sort((a, b) => b[1] - a[1])
                .reduce((obj, [key, val]) => ({...obj, [key]: val}), {});
            result.charts.job_type_dist = Object.entries(distributions.jobType)
                .sort((a, b) => b[1] - a[1])
                .reduce((obj, [key, val]) => ({...obj, [key]: val}), {});
            result.charts.top_faults = Object.entries(distributions.cause)
                .sort((a, b) => b[1] - a[1])
                .slice(0, 10)
                .reduce((obj, [key, val]) => ({...obj, [key]: val}), {});

            return result;
        } catch (error) {
            console.error('Error in processData:', error);
            throw new Error(`Failed to process data: ${error.message}`);
        }
    }

    // Initialize sidebar sections and analysis buttons
    const sidebarSections = document.querySelectorAll('.sidebar-section');
    sidebarSections.forEach((section, index) => {
        const button = section.querySelector('button');
        const content = section.querySelector('.sidebar-section-content');
        const icon = button.querySelector('i');

        // Set up click handler
        button.addEventListener('click', () => {
            // Toggle content visibility
            content.classList.toggle('hidden');
            // Rotate icon
            icon.style.transform = content.classList.contains('hidden') ? 'rotate(0deg)' : 'rotate(180deg)';
        });

        // Show Corrective section by default (first section)
        if (index === 0) {
            content.classList.remove('hidden');
            icon.style.transform = 'rotate(180deg)';
        }
    });

    // Initialize analysis buttons
    const generalAnalysisBtn = document.getElementById('generalAnalysisBtn');
    const correctiveAnalysisBtn = document.getElementById('correctiveAnalysisBtn');
    const breakdownAnalysisBtn = document.getElementById('breakdownAnalysisBtn');

    let currentView = 'general'; // Track current view type

    if (generalAnalysisBtn) {
        generalAnalysisBtn.addEventListener('click', function() {
            if (!currentData) {
                alert('Please load data first before running analysis.');
                return;
            }

            const loadingSpinner = document.getElementById('loadingSpinner');
            const statusMessage = document.getElementById('statusMessage');
            
            loadingSpinner.classList.remove('hidden');
            statusMessage.textContent = 'Loading General Overview...';

            currentView = 'general';
            renderGeneralKPIs(currentData.data);
            renderViewSpecificCharts(currentData.data, currentView);
            loadingSpinner.classList.add('hidden');
        });
    }

    if (correctiveAnalysisBtn) {
        correctiveAnalysisBtn.addEventListener('click', function() {
            if (!currentData) {
                alert('Please load data first before running analysis.');
                return;
            }

            const loadingSpinner = document.getElementById('loadingSpinner');
            const statusMessage = document.getElementById('statusMessage');
            
            loadingSpinner.classList.remove('hidden');
            statusMessage.textContent = 'Running Corrective Analysis...';

            // Filter for corrective maintenance data - Updated filter logic
            const correctiveData = currentData.data.filter(item => 
                item.Job_type === 'C' || item.Job_type === 'CM' || item.Job_type === 'INSP'
            );

            currentView = 'corrective';
            renderCorrectiveKPIs(correctiveData);
            renderViewSpecificCharts(correctiveData, currentView);
            loadingSpinner.classList.add('hidden');
        });
    }

    if (breakdownAnalysisBtn) {
        breakdownAnalysisBtn.addEventListener('click', function() {
            if (!currentData) {
                alert('Please load data first before running analysis.');
                return;
            }

            const loadingSpinner = document.getElementById('loadingSpinner');
            const statusMessage = document.getElementById('statusMessage');
            
            loadingSpinner.classList.remove('hidden');
            statusMessage.textContent = 'Running Breakdown Analysis...';

            // Filter for breakdown data - Updated filter logic to include "U" for unplanned
            const breakdownData = currentData.data.filter(item => 
                item.Job_type === 'BDN' || item.Job_type === 'U'
            );

            currentView = 'breakdown';
            renderBreakdownKPIs(breakdownData);
            renderViewSpecificCharts(breakdownData, currentView);
            loadingSpinner.classList.add('hidden');
        });
    }

    function renderViewSpecificCharts(data, viewType) {
        const chartSection = document.getElementById('chartSection');
        chartSection.innerHTML = '';

        // Calculate time-based metrics
        const timeMetrics = data.reduce((acc, wo) => {
            if (wo.Order_date && wo.Jobexec_dt) {
                const startDate = parseFrenchDate(wo.Order_date);
                const endDate = parseFrenchDate(wo.Jobexec_dt);
                if (startDate && endDate) {
                    const timeDiff = (endDate - startDate) / (1000 * 60 * 60); // Convert to hours
                    acc.durations.push(timeDiff);
                }
            }
            return acc;
        }, { durations: [] });

        // Format the average time
        let avgTimeDisplay = 'N/A';
        let timeUnit = '';
        if (timeMetrics.durations.length > 0) {
            const avgHours = timeMetrics.durations.reduce((a, b) => a + b, 0) / timeMetrics.durations.length;
            if (avgHours < 24) {
                const hours = Math.floor(avgHours);
                const minutes = Math.floor((avgHours % 1) * 60);
                avgTimeDisplay = `${hours}h ${minutes}m`;
            } else {
                const days = (avgHours / 24).toFixed(1);
                avgTimeDisplay = days;
                timeUnit = ' days';
            }
        }

        // Common data preparation
        const chartData = prepareChartData(data);

        // Add average time to chart data
        chartData.avgTimeDisplay = avgTimeDisplay;
        chartData.timeUnit = timeUnit;

        // Render different charts based on view type
        switch (viewType) {
            case 'general':
                renderGeneralCharts(chartData);
                break;
            case 'corrective':
                renderCorrectiveCharts(chartData);
                break;
            case 'breakdown':
                renderBreakdownCharts(chartData);
                break;
        }
    }

    function prepareChartData(data) {
        const wo_created = {};
        const wo_closed = {};
        const eq_type_dist = {};
        const wo_per_location = {};
        const wo_per_cause = {};
        const status_dist = {};

        data.forEach(item => {
            // WO created date with week format
            if (item.Order_date) {
                const formattedDate = formatDateForChart(item.Order_date);
                wo_created[formattedDate] = (wo_created[formattedDate] || 0) + 1;
            }

            // WO closed date with week format
            if (item.Jobexec_dt) {
                const formattedDate = formatDateForChart(item.Jobexec_dt);
                wo_closed[formattedDate] = (wo_closed[formattedDate] || 0) + 1;
            }

            // Equipment type distribution
            const type = item.EQ_type || 'Other';
            eq_type_dist[type] = (eq_type_dist[type] || 0) + 1;

            // Location distribution
            const loc = item.faultlocation || 'Other';
            wo_per_location[loc] = (wo_per_location[loc] || 0) + 1;

            // Cause distribution
            const cause = item.failure || 'Unknown';
            wo_per_cause[cause] = (wo_per_cause[cause] || 0) + 1;

            // Status distribution
            const status = item.JobstatusObs || 'Unknown';
            status_dist[status] = (status_dist[status] || 0) + 1;
        });

        return {
            wo_created,
            wo_closed,
            eq_type_dist,
            wo_per_location,
            wo_per_cause,
            status_dist
        };
    }

    function renderGeneralCharts(chartData) {
        // Render WO Trend
        renderWOTrendChart(chartData.wo_created, chartData.wo_closed);
        // Render Equipment Distribution
        renderPieChart('eq_type_dist', chartData.eq_type_dist, 'Equipment Type Distribution');
        // Render Location Distribution
        renderBarChart('wo_per_location', chartData.wo_per_location, 'Work Orders per Location');
        // Render Status Distribution
        renderPieChart('status_dist', chartData.status_dist, 'Status Distribution');
    }

    function renderCorrectiveCharts(chartData) {
        // Render WO Trend
        renderWOTrendChart(chartData.wo_created, chartData.wo_closed);
        // Render Fault Location Distribution
        renderBarChart('wo_per_location', chartData.wo_per_location, 'Fault Locations');
        // Render Cause Distribution
        renderBarChart('wo_per_cause', chartData.wo_per_cause, 'Failure Causes');
        // Render Equipment Type Impact
        renderPieChart('eq_type_dist', chartData.eq_type_dist, 'Equipment Impact Distribution');
    }

    function renderBreakdownCharts(chartData) {
        // Render Critical Events Timeline
        renderWOTrendChart(chartData.wo_created, chartData.wo_closed);
        // Render Impact by Equipment
        renderPieChart('eq_type_dist', chartData.eq_type_dist, 'Equipment Breakdown Distribution');
        // Render Failure Analysis
        renderBarChart('wo_per_cause', chartData.wo_per_cause, 'Breakdown Causes');
        // Render Response Time Analysis
        renderTimelineChart('response_time', chartData.wo_created, 'Response Time Analysis');
    }

    // Chart rendering helper functions
    function renderWOTrendChart(created, closed) {
        const dates = [...new Set([...Object.keys(created), ...Object.keys(closed)])].sort();
        const container = createChartContainer();

        Plotly.newPlot(container, [
            {
                x: dates,
                y: dates.map(date => created[date] || 0),
                name: 'Created',
                type: 'scatter',
                mode: 'lines+markers',
                line: { color: '#3B82F6' }
            },
            {
                x: dates,
                y: dates.map(date => closed[date] || 0),
                name: 'Closed',
                type: 'scatter',
                mode: 'lines+markers',
                line: { color: '#10B981' }
            }
        ], {
            title: 'Work Orders Trend',
            xaxis: { 
                title: 'Year-Month-Week',
                tickangle: -45
            },
            yaxis: { title: 'Count' },
            paper_bgcolor: 'rgba(0,0,0,0)',
            plot_bgcolor: 'rgba(0,0,0,0)',
            showlegend: true,
            legend: { orientation: 'h', y: -0.2 },
            margin: { b: 100 }
        });
    }

    function renderPieChart(id, data, title) {
        const container = createChartContainer();
        
        Plotly.newPlot(container, [{
            values: Object.values(data),
            labels: Object.keys(data),
            type: 'pie',
            hole: 0.4,
            marker: {
                colors: ['#3B82F6', '#10B981', '#F59E0B', '#EF4444', '#8B5CF6', 
                        '#EC4899', '#14B8A6', '#6366F1', '#D946EF', '#F97316']
            }
        }], {
            title: title,
            paper_bgcolor: 'rgba(0,0,0,0)',
            plot_bgcolor: 'rgba(0,0,0,0)',
            showlegend: true,
            legend: { orientation: 'h', y: -0.2 }
        });
    }

    function renderBarChart(id, data, title) {
        const container = createChartContainer();
        const sortedData = Object.entries(data)
            .sort((a, b) => b[1] - a[1])
            .slice(0, 10);

        Plotly.newPlot(container, [{
            x: sortedData.map(([label]) => label),
            y: sortedData.map(([, value]) => value),
            type: 'bar',
            marker: {
                color: '#3B82F6'
            }
        }], {
            title: title,
            xaxis: { title: 'Category', tickangle: -45 },
            yaxis: { title: 'Count' },
            paper_bgcolor: 'rgba(0,0,0,0)',
            plot_bgcolor: 'rgba(0,0,0,0)',
            margin: { b: 100 }
        });
    }

    function renderTimelineChart(id, data, title) {
        const container = createChartContainer();
        const dates = Object.keys(data).sort();
        const values = dates.map(date => data[date]);

        // Calculate cumulative response times
        const cumulativeData = values.reduce((acc, curr, idx) => {
            const prev = idx > 0 ? acc[idx - 1] : 0;
            acc.push(prev + curr);
            return acc;
        }, []);

        Plotly.newPlot(container, [
            {
                x: dates,
                y: cumulativeData,
                type: 'scatter',
                mode: 'lines+markers',
                name: 'Cumulative',
                line: { shape: 'spline', smoothing: 1.3, color: '#8B5CF6' }
            },
            {
                x: dates,
                y: values,
                type: 'bar',
                name: 'Per Period',
                marker: { color: '#3B82F6' }
            }
        ], {
            title: title,
            xaxis: { 
                title: 'Time Period',
                tickangle: -45
            },
            yaxis: { title: 'Count' },
            paper_bgcolor: 'rgba(0,0,0,0)',
            plot_bgcolor: 'rgba(0,0,0,0)',
            showlegend: true,
            legend: { orientation: 'h', y: -0.2 },
            margin: { b: 100 },
            barmode: 'group'
        });
    }

    function createChartContainer() {
        const container = document.createElement('div');
        container.className = 'chart-container';
        container.style.minHeight = '400px';
        container.style.width = '100%';
        document.getElementById('chartSection').appendChild(container);
        return container;
    }

    // General charts renderer for backward compatibility
    function renderCharts(charts) {
        const chartSection = document.getElementById('chartSection');
        chartSection.innerHTML = '';

        // Create WO Created vs Closed chart
        renderWOTrendChart(charts.wo_created, charts.wo_closed);

        // Equipment Type Distribution chart
        renderPieChart('eq_type_dist', charts.eq_type_dist, 'Equipment Type Distribution');

        // Work Orders per Location chart
        renderBarChart('wo_per_location', charts.wo_per_location, 'Work Orders per Location');

        // Status Distribution chart
        renderPieChart('status_dist', charts.status_dist, 'Status Distribution');

        // Add dark mode event listener to update chart themes
        const darkModeObserver = new MutationObserver(mutations => {
            mutations.forEach(mutation => {
                if (mutation.type === 'attributes' && mutation.attributeName === 'class') {
                    const isDark = document.body.classList.contains('dark');
                    const chartUpdate = {
                        'paper_bgcolor': 'rgba(0,0,0,0)',
                        'plot_bgcolor': 'rgba(0,0,0,0)',
                        'font.color': isDark ? '#F3F4F6' : '#111827',
                        'xaxis.gridcolor': isDark ? '#4B5563' : '#E5E7EB',
                        'yaxis.gridcolor': isDark ? '#4B5563' : '#E5E7EB'
                    };
                    
                    const charts = document.querySelectorAll('.chart-container');
                    charts.forEach(chart => {
                        Plotly.relayout(chart, chartUpdate);
                    });
                }
            });
        });

        darkModeObserver.observe(document.body, { attributes: true });
    }

    // Add filter event handler
    const filtersForm = document.getElementById('filtersForm');
    if (filtersForm) {
        // Setup date range pickers with dynamic min/max dates
        function setupDateRanges(data) {
            // Get all dates from the data
            const dates = data
                .map(item => item.Order_date)
                .filter(Boolean)
                .map(dateStr => parseFrenchDate(dateStr))
                .filter(date => date && !isNaN(date.getTime()));

            if (dates.length > 0) {
                const minDate = new Date(Math.min(...dates));
                const maxDate = new Date(Math.max(...dates));

                // Set min/max for order date inputs
                const orderStartDate = document.getElementById('orderStartDate');
                const orderEndDate = document.getElementById('orderEndDate');
                if (orderStartDate && orderEndDate) {
                    const minWeek = getWeekNumber(minDate);
                    const maxWeek = getWeekNumber(maxDate);
                    orderStartDate.min = `${minDate.getFullYear()}-W${String(minWeek).padStart(2, '0')}`;
                    orderEndDate.max = `${maxDate.getFullYear()}-W${String(maxWeek).padStart(2, '0')}`;
                    
                    // Set default values
                    orderStartDate.value = orderStartDate.min;
                    orderEndDate.value = orderEndDate.max;
                }

                // Set min/max for execution date inputs
                const execStartDate = document.getElementById('execStartDate');
                const execEndDate = document.getElementById('execEndDate');
                if (execStartDate && execEndDate) {
                    const minWeek = getWeekNumber(minDate);
                    const maxWeek = getWeekNumber(maxDate);
                    execStartDate.min = `${minDate.getFullYear()}-W${String(minWeek).padStart(2, '0')}`;
                    execEndDate.max = `${maxDate.getFullYear()}-W${String(maxWeek).padStart(2, '0')}`;
                    
                    // Set default values
                    execStartDate.value = execStartDate.min;
                    execEndDate.value = execEndDate.max;
                }
            }
        }

        filtersForm.addEventListener('submit', function(e) {
            e.preventDefault();
            if (!currentData) {
                alert('Please load data first before applying filters.');
                return;
            }

            // Get filter values with defensive checks
            const orderStartDate = document.getElementById('orderStartDate')?.value || '';
            const orderEndDate = document.getElementById('orderEndDate')?.value || '';
            const execStartDate = document.getElementById('execStartDate')?.value || '';
            const execEndDate = document.getElementById('execEndDate')?.value || '';
            
            // Get selected options with defensive checks
            const jobTypeSelect = document.getElementById('jobTypeSelect');
            const jobStatusSelect = document.getElementById('jobStatusSelect');
            const faultLocationSelect = document.getElementById('faultLocationSelect');
            const eqTypeSelect = document.getElementById('eqTypeSelect');
            const costPurposeSelect = document.getElementById('costPurposeSelect');
            const failureSelect = document.getElementById('failureSelect');

            const selectedJobTypes = jobTypeSelect ? Array.from(jobTypeSelect.selectedOptions).map(opt => opt.value) : [];
            const selectedStatuses = jobStatusSelect ? Array.from(jobStatusSelect.selectedOptions).map(opt => opt.value) : [];
            const selectedLocations = faultLocationSelect ? Array.from(faultLocationSelect.selectedOptions).map(opt => opt.value) : [];
            const selectedEquipment = eqTypeSelect ? Array.from(eqTypeSelect.selectedOptions).map(opt => opt.value) : [];
            const selectedPurposes = costPurposeSelect ? Array.from(costPurposeSelect.selectedOptions).map(opt => opt.value) : [];
            const selectedFailures = failureSelect ? Array.from(failureSelect.selectedOptions).map(opt => opt.value) : [];

            // Apply filters
            let filteredData = currentData.data.filter(item => {
                // Date range filters
                const itemOrderDate = parseFrenchDate(item.Order_date);
                const itemExecDate = parseFrenchDate(item.Jobexec_dt);
                const orderStartDateTime = orderStartDate ? getDateFromISOWeek(orderStartDate) : null;
                const orderEndDateTime = orderEndDate ? getDateFromISOWeek(orderEndDate) : null;
                const execStartDateTime = execStartDate ? getDateFromISOWeek(execStartDate) : null;
                const execEndDateTime = execEndDate ? getDateFromISOWeek(execEndDate) : null;

                // Check order date range
                if (orderStartDateTime && itemOrderDate && itemOrderDate < orderStartDateTime) return false;
                if (orderEndDateTime && itemOrderDate && itemOrderDate > orderEndDateTime) return false;

                // Check execution date range
                if (execStartDateTime && itemExecDate && itemExecDate < execStartDateTime) return false;
                if (execEndDateTime && itemExecDate && itemExecDate > execEndDateTime) return false;

                // Other filters
                if (selectedJobTypes.length && !selectedJobTypes.includes(item.Job_type)) return false;
                if (selectedStatuses.length && !selectedStatuses.includes(item.JobstatusObs)) return false;
                if (selectedLocations.length && !selectedLocations.includes(item.faultlocation)) return false;
                if (selectedEquipment.length && !selectedEquipment.includes(item.EQ_type)) return false;
                if (selectedPurposes.length && !selectedPurposes.includes(item.Cost_purpose_key)) return false;
                if (selectedFailures.length && !selectedFailures.includes(item.failure)) return false;

                return true;
            });

            // Update charts with filtered data
            updateCharts(filteredData);
        });

        // Helper function to get week number from ISO week string (YYYY-Www)
        function getWeekFromISOString(isoWeek) {
            const [year, week] = isoWeek.split('-W');
            return parseInt(week, 10);
        }
    }

    // Call setupDateRanges when data is loaded
    const originalPopulateDropdowns = populateDropdowns;
    populateDropdowns = function(data) {
        originalPopulateDropdowns(data);
        setupDateRanges(data);
    };

    // Helper function to convert ISO week to Date
    function getDateFromISOWeek(isoWeek) {
        if (!isoWeek) return null;
        try {
            const [year, week] = isoWeek.split('-W');
            const simple = new Date(year, 0, 1 + (week - 1) * 7);
            const day = simple.getDay();
            const diff = simple.getDate() - day + (day === 0 ? -6 : 1); // Adjust when day is Sunday
            return new Date(simple.setDate(diff));
        } catch (e) {
            console.warn('Error parsing ISO week:', e);
            return null;
        }
    }

    function applyFilters() {
        if (!currentData) {
            alert('Please load data first before applying filters.');
            return;
        }

        // Get filter values
        const orderStartDate = document.getElementById('orderStartDate')?.value;
        const orderEndDate = document.getElementById('orderEndDate')?.value;
        const execStartDate = document.getElementById('execStartDate')?.value;
        const execEndDate = document.getElementById('execEndDate')?.value;
        
        // Convert ISO week values to dates
        const orderStartDateTime = orderStartDate ? getDateFromISOWeek(orderStartDate) : null;
        const orderEndDateTime = orderEndDate ? getDateFromISOWeek(orderEndDate) : null;
        const execStartDateTime = execStartDate ? getDateFromISOWeek(execStartDate) : null;
        const execEndDateTime = execEndDate ? getDateFromISOWeek(execEndDate) : null;

        // Get selected options
        const jobTypeSelect = document.getElementById('jobTypeSelect');
        const jobStatusSelect = document.getElementById('jobStatusSelect');
        const faultLocationSelect = document.getElementById('faultLocationSelect');
        const eqTypeSelect = document.getElementById('eqTypeSelect');
        const costPurposeSelect = document.getElementById('costPurposeSelect');
        const failureSelect = document.getElementById('failureSelect');

        const selectedJobTypes = jobTypeSelect ? Array.from(jobTypeSelect.selectedOptions).map(opt => opt.value) : [];
        const selectedStatuses = jobStatusSelect ? Array.from(jobStatusSelect.selectedOptions).map(opt => opt.value) : [];
        const selectedLocations = faultLocationSelect ? Array.from(faultLocationSelect.selectedOptions).map(opt => opt.value) : [];
        const selectedEquipment = eqTypeSelect ? Array.from(eqTypeSelect.selectedOptions).map(opt => opt.value) : [];
        const selectedPurposes = costPurposeSelect ? Array.from(costPurposeSelect.selectedOptions).map(opt => opt.value) : [];
        const selectedFailures = failureSelect ? Array.from(failureSelect.selectedOptions).map(opt => opt.value) : [];

        // Apply filters
        let filteredData = currentData.data.filter(item => {
            // Date range filters
            const itemOrderDate = parseFrenchDate(item.Order_date);
            const itemExecDate = parseFrenchDate(item.Jobexec_dt);

            // Check order date range
            if (orderStartDateTime && itemOrderDate && itemOrderDate < orderStartDateTime) return false;
            if (orderEndDateTime && itemOrderDate && itemOrderDate > orderEndDateTime) return false;

            // Check execution date range
            if (execStartDateTime && itemExecDate && itemExecDate < execStartDateTime) return false;
            if (execEndDateTime && itemExecDate && itemExecDate > execEndDateTime) return false;

            // Multi-select filters - if nothing is selected, don't filter
            if (selectedJobTypes.length > 0 && !selectedJobTypes.includes(item.Job_type)) return false;
            if (selectedStatuses.length > 0 && !selectedStatuses.includes(item.JobstatusObs)) return false;
            if (selectedLocations.length > 0 && !selectedLocations.includes(item.faultlocation)) return false;
            if (selectedEquipment.length > 0 && !selectedEquipment.includes(item.EQ_type)) return false;
            if (selectedPurposes.length > 0 && !selectedPurposes.includes(item.Cost_purpose_key)) return false;
            if (selectedFailures.length > 0 && !selectedFailures.includes(item.failure)) return false;

            return true;
        });

        // Update charts and KPIs with filtered data
        updateCharts(filteredData);
        if (currentView === 'general') {
            renderGeneralKPIs(filteredData);
        } else if (currentView === 'corrective') {
            renderCorrectiveKPIs(filteredData);
        } else if (currentView === 'breakdown') {
            renderBreakdownKPIs(filteredData);
        }
    }

    function renderGeneralKPIs(data) {
        const kpiSection = document.getElementById('kpiSection');
        if (!kpiSection || !data) return;

        // Calculate overall statistics
        const total = data.length;
        const closed = data.filter(wo => wo.Jobexec_dt).length;
        const waiting = total - closed;
        
        // Calculate average processing time for all WOs
        let avgProcessingTime = 0;
        let timeUnit = 'days';
        let processingTimeDisplay = '';
        const closedWOs = data.filter(wo => wo.Jobexec_dt);
        if (closedWOs.length > 0) {
            const processingTimes = closedWOs.map(wo => {
                const startDate = parseFrenchDate(wo.Order_date);
                const endDate = parseFrenchDate(wo.Jobexec_dt);
                return startDate && endDate ? (endDate - startDate) / (1000 * 60 * 60 * 24) : 0;
            }).filter(time => time > 0);
            
            if (processingTimes.length > 0) {
                avgProcessingTime = processingTimes.reduce((a, b) => a + b, 0) / processingTimes.length;
                
                const formattedTime = formatTimeDuration(avgProcessingTime);
                processingTimeDisplay = formattedTime.display;
                timeUnit = formattedTime.unit;
            }
        }

        // Get most common job type and status
        const jobTypeCounts = {};
        const statusCounts = {};
        data.forEach(wo => {
            if (wo.Job_type) {
                jobTypeCounts[wo.Job_type] = (jobTypeCounts[wo.Job_type] || 0) + 1;
            }
            if (wo.JobstatusObs) {
                statusCounts[wo.JobstatusObs] = (statusCounts[wo.JobstatusObs] || 0) + 1;
            }
        });

        const topJobType = Object.entries(jobTypeCounts)
            .sort((a, b) => b[1] - a[1])[0] || ['Unknown', 0];
        const topStatus = Object.entries(statusCounts)
            .sort((a, b) => b[1] - a[1])[0] || ['Unknown', 0];

        kpiSection.innerHTML = `
            <div class="kpi-card bg-white dark:bg-gray-700 rounded-lg shadow-sm p-4">
                <div class="flex items-center justify-between">
                    <div>
                        <p class="text-sm font-medium text-gray-500 dark:text-gray-400">Total Work Orders</p>
                        <p class="text-2xl font-semibold text-blue-600 dark:text-blue-400">${total}</p>
                    </div>
                    <div class="p-3 rounded-full bg-blue-100 dark:bg-blue-900 text-blue-600 dark:text-blue-400">
                        <i class="fas fa-clipboard-list"></i>
                    </div>
                </div>
                <div class="mt-4 flex items-center justify-between text-xs text-gray-500 dark:text-gray-400">
                    <span>${waiting} waiting</span>
                    <span class="text-green-500 dark:text-green-400">${closed} closed</span>
                </div>
            </div>

            <div class="kpi-card bg-white dark:bg-gray-700 rounded-lg shadow-sm p-4">
                <div class="flex items-center justify-between">
                    <div>
                        <p class="text-sm font-medium text-gray-500 dark:text-gray-400">Average Processing Time</p>
                        <p class="text-2xl font-semibold text-green-600 dark:text-green-400">${processingTimeDisplay}${timeUnit}</p>
                    </div>
                    <div class="p-3 rounded-full bg-green-100 dark:bg-green-900 text-green-600 dark:text-green-400">
                        <i class="fas fa-clock"></i>
                    </div>
                </div>
                <div class="mt-4 flex items-center justify-between text-xs text-gray-500 dark:text-gray-400">
                    <span>Target: 5 days</span>
                </div>
            </div>

            <div class="kpi-card bg-white dark:bg-gray-700 rounded-lg shadow-sm p-4">
                <div class="flex items-center justify-between">
                    <div>
                        <p class="text-sm font-medium text-gray-500 dark:text-gray-400">Top Job Type</p>
                        <p class="text-2xl font-semibold text-purple-600 dark:text-purple-400">${topJobType[0]}</p>
                    </div>
                    <div class="p-3 rounded-full bg-purple-100 dark:bg-purple-900 text-purple-600 dark:text-purple-400">
                        <i class="fas fa-tasks"></i>
                    </div>
                </div>
                <div class="mt-4 flex items-center justify-between text-xs text-gray-500 dark:text-gray-400">
                    <span>${getJobTypeFullName(topJobType[0])}</span>
                    <span>${topJobType[1]} WOs</span>
                </div>
            </div>

            <div class="kpi-card bg-white dark:bg-gray-700 rounded-lg shadow-sm p-4">
                <div class="flex items-center justify-between">
                    <div>
                        <p class="text-sm font-medium text-gray-500 dark:text-gray-400">Top Status</p>
                        <p class="text-2xl font-semibold text-orange-600 dark:text-orange-400">${topStatus[0]}</p>
                    </div>
                    <div class="p-3 rounded-full bg-orange-100 dark:bg-orange-900 text-orange-600 dark:text-orange-400">
                        <i class="fas fa-chart-pie"></i>
                    </div>
                </div>
                <div class="mt-4 flex items-center justify-between text-xs text-gray-500 dark:text-gray-400">
                    <span>${getJobStatusFullName(topStatus[0]).description}</span>
                    <span>${topStatus[1]} WOs</span>
                </div>
            </div>
        `;
    }

    function renderCorrectiveKPIs(data) {
        const kpiSection = document.getElementById('kpiSection');
        if (!kpiSection || !data) return;

        // Calculate corrective maintenance statistics
        const total = data.length;
        const closed = data.filter(wo => wo.Jobexec_dt).length;
        const waiting = total - closed;

        // Calculate MTTR (Mean Time To Repair)
        let mttr = 0;
        let mttrDisplay = '';
        let timeUnit = '';
        const closedWOs = data.filter(wo => wo.Jobexec_dt);
        if (closedWOs.length > 0) {
            const repairTimes = closedWOs.map(wo => {
                const startDate = parseFrenchDate(wo.Order_date);
                const endDate = parseFrenchDate(wo.Jobexec_dt);
                return startDate && endDate ? (endDate - startDate) / (1000 * 60 * 60 * 24) : 0;
            }).filter(time => time > 0);
            
            if (repairTimes.length > 0) {
                mttr = repairTimes.reduce((a, b) => a + b, 0) / repairTimes.length;
                
                const formattedTime = formatTimeDuration(mttr);
                mttrDisplay = formattedTime.display;
                timeUnit = formattedTime.unit;
            }
        }

        // Get most common failure type and location
        const failureCounts = {};
        const locationCounts = {};
        data.forEach(wo => {
            if (wo.failure) {
                failureCounts[wo.failure] = (failureCounts[wo.failure] || 0) + 1;
            }
            if (wo.faultlocation) {
                locationCounts[wo.faultlocation] = (locationCounts[wo.faultlocation] || 0) + 1;
            }
        });

        const topFailure = Object.entries(failureCounts)
            .sort((a, b) => b[1] - a[1])[0] || ['Unknown', 0];
        const topLocation = Object.entries(locationCounts)
            .sort((a, b) => b[1] - a[1])[0] || ['Unknown', 0];

        kpiSection.innerHTML = `
            <div class="kpi-card bg-white dark:bg-gray-700 rounded-lg shadow-sm p-4">
                <div class="flex items-center justify-between">
                    <div>
                        <p class="text-sm font-medium text-gray-500 dark:text-gray-400">Corrective WOs</p>
                        <p class="text-2xl font-semibold text-blue-600 dark:text-blue-400">${total}</p>
                    </div>
                    <div class="p-3 rounded-full bg-blue-100 dark:bg-blue-900 text-blue-600 dark:text-blue-400">
                        <i class="fas fa-wrench"></i>
                    </div>
                </div>
                <div class="mt-4 flex items-center justify-between text-xs text-gray-500 dark:text-gray-400">
                    <span>${waiting} pending</span>
                    <span class="text-green-500 dark:text-green-400">${closed} completed</span>
                </div>
            </div>

            <div class="kpi-card bg-white dark:bg-gray-700 rounded-lg shadow-sm p-4">
                <div class="flex items-center justify-between">
                    <div>
                        <p class="text-sm font-medium text-gray-500 dark:text-gray-400">Mean Time To Repair</p>
                        <p class="text-2xl font-semibold text-green-600 dark:text-green-400">${mttrDisplay}${timeUnit}</p>
                    </div>
                    <div class="p-3 rounded-full bg-green-100 dark:bg-green-900 text-green-600 dark:text-green-400">
                        <i class="fas fa-clock"></i>
                    </div>
                </div>
                <div class="mt-4 flex items-center justify-between text-xs text-gray-500 dark:text-gray-400">
                    <span>Target: 3 days</span>
                </div>
            </div>

            <div class="kpi-card bg-white dark:bg-gray-700 rounded-lg shadow-sm p-4">
                <div class="flex items-center justify-between">
                    <div>
                        <p class="text-sm font-medium text-gray-500 dark:text-gray-400">Top Failure Type</p>
                        <p class="text-2xl font-semibold text-purple-600 dark:text-purple-400">${topFailure[0]}</p>
                    </div>
                    <div class="p-3 rounded-full bg-purple-100 dark:bg-purple-900 text-purple-600 dark:text-purple-400">
                        <i class="fas fa-exclamation-triangle"></i>
                    </div>
                </div>
                <div class="mt-4 flex items-center justify-between text-xs text-gray-500 dark:text-gray-400">
                    <span>${topFailure[1]} occurrences</span>
                </div>
            </div>

            <div class="kpi-card bg-white dark:bg-gray-700 rounded-lg shadow-sm p-4">
                <div class="flex items-center justify-between">
                    <div>
                        <p class="text-sm font-medium text-gray-500 dark:text-gray-400">Most Affected Location</p>
                        <p class="text-2xl font-semibold text-orange-600 dark:text-orange-400">${topLocation[0]}</p>
                    </div>
                    <div class="p-3 rounded-full bg-orange-100 dark:bg-orange-900 text-orange-600 dark:text-orange-400">
                        <i class="fas fa-map-marker-alt"></i>
                    </div>
                </div>
                <div class="mt-4 flex items-center justify-between text-xs text-gray-500 dark:text-gray-400">
                    <span>${topLocation[1]} issues</span>
                </div>
            </div>
        `;
    }

    function renderBreakdownKPIs(data) {
        const kpiSection = document.getElementById('kpiSection');
        if (!kpiSection || !data) return;

        // Calculate breakdown statistics based on execution date only
        const total = data.length;
        const resolved = data.filter(wo => wo.Jobexec_dt).length;
        const pending = total - resolved;

        // Calculate MTTR using more precise calculation
        let mttr = 0;
        let mttrDisplay = '';
        let timeUnit = '';
        const resolvedWOs = data.filter(wo => wo.Jobexec_dt);
        if (resolvedWOs.length > 0) {
            const repairTimes = resolvedWOs.map(wo => {
                const startDate = parseFrenchDate(wo.Order_date);
                const endDate = parseFrenchDate(wo.Jobexec_dt);
                return startDate && endDate ? (endDate - startDate) / (1000 * 60 * 60 * 24) : 0;
            }).filter(time => time > 0);

            if (repairTimes.length > 0) {
                mttr = repairTimes.reduce((a, b) => a + b, 0) / repairTimes.length;
                
                const formattedTime = formatTimeDuration(mttr);
                mttrDisplay = formattedTime.display;
                timeUnit = formattedTime.unit;
            }
        }

        // Get most affected equipment and critical failures
        const equipmentCounts = {};
        const criticalFailures = {};
        data.forEach(wo => {
            if (wo.EQ_type) {
                equipmentCounts[wo.EQ_type] = (equipmentCounts[wo.EQ_type] || 0) + 1;
            }
            if (wo.failure) {
                criticalFailures[wo.failure] = (criticalFailures[wo.failure] || 0) + 1;
            }
        });

        const mostAffectedEq = Object.entries(equipmentCounts)
            .sort((a, b) => b[1] - a[1])[0] || ['Unknown', 0];
        const topCriticalFailure = Object.entries(criticalFailures)
            .sort((a, b) => b[1] - a[1])[0] || ['Unknown', 0];

        kpiSection.innerHTML = `
            <div class="kpi-card bg-white dark:bg-gray-700 rounded-lg shadow-sm p-4">
                <div class="flex items-center justify-between">
                    <div>
                        <p class="text-sm font-medium text-gray-500 dark:text-gray-400">Total Breakdowns</p>
                        <p class="text-2xl font-semibold text-red-600 dark:text-red-400">${total}</p>
                    </div>
                    <div class="p-3 rounded-full bg-red-100 dark:bg-red-900 text-red-600 dark:text-red-400">
                        <i class="fas fa-exclamation-circle"></i>
                    </div>
                </div>
                <div class="mt-4 flex items-center justify-between text-xs text-gray-500 dark:text-gray-400">
                    <span>${pending} active</span>
                    <span class="text-green-500 dark:text-green-400">${resolved} resolved</span>
                </div>
            </div>

            <div class="kpi-card bg-white dark:bg-gray-700 rounded-lg shadow-sm p-4">
                <div class="flex items-center justify-between">
                    <div>
                        <p class="text-sm font-medium text-gray-500 dark:text-gray-400">Average Resolution Time</p>
                        <p class="text-2xl font-semibold text-orange-600 dark:text-orange-400">${mttrDisplay}${timeUnit}</p>
                    </div>
                    <div class="p-3 rounded-full bg-orange-100 dark:bg-orange-900 text-orange-600 dark:text-orange-400">
                        <i class="fas fa-stopwatch"></i>
                    </div>
                </div>
                <div class="mt-4 flex items-center justify-between text-xs text-gray-500 dark:text-gray-400">
                    <span>Target: 1 day</span>
                </div>
            </div>

            <div class="kpi-card bg-white dark:bg-gray-700 rounded-lg shadow-sm p-4">
                <div class="flex items-center justify-between">
                    <div>
                        <p class="text-sm font-medium text-gray-500 dark:text-gray-400">Most Affected Equipment</p>
                        <p class="text-2xl font-semibold text-purple-600 dark:text-purple-400">${mostAffectedEq[0]}</p>
                    </div>
                    <div class="p-3 rounded-full bg-purple-100 dark:bg-purple-900 text-purple-600 dark:text-purple-400">
                        <i class="fas fa-tools"></i>
                    </div>
                </div>
                <div class="mt-4 flex items-center justify-between text-xs text-gray-500 dark:text-gray-400">
                    <span>${mostAffectedEq[1]} breakdowns</span>
                </div>
            </div>

            <div class="kpi-card bg-white dark:bg-gray-700 rounded-lg shadow-sm p-4">
                <div class="flex items-center justify-between">
                    <div>
                        <p class="text-sm font-medium text-gray-500 dark:text-gray-400">Top Critical Failure</p>
                        <p class="text-2xl font-semibold text-yellow-600 dark:text-yellow-400">${topCriticalFailure[0]}</p>
                    </div>
                    <div class="p-3 rounded-full bg-yellow-100 dark:bg-yellow-900 text-yellow-600 dark:text-yellow-400">
                        <i class="fas fa-bolt"></i>
                    </div>
                </div>
                <div class="mt-4 flex items-center justify-between text-xs text-gray-500 dark:text-gray-400">
                    <span>${topCriticalFailure[1]} occurrences</span>
                </div>
            </div>
        `;
    }

    function setupFilterListeners() {
        const filterInputs = [
            'orderStartDate', 'orderEndDate', 'execStartDate', 'execEndDate',
            'jobTypeSelect', 'jobStatusSelect', 'faultLocationSelect',
            'eqTypeSelect', 'costPurposeSelect', 'failureSelect'
        ];

        filterInputs.forEach(id => {
            const element = document.getElementById(id);
            if (element) {
                element.addEventListener('change', applyFilters);
            }
        });

        // Prevent form submission since we're handling changes immediately
        const filtersForm = document.getElementById('filtersForm');
        if (filtersForm) {
            filtersForm.addEventListener('submit', (e) => {
                e.preventDefault();
                applyFilters();
            });
        }
    }

    function updateCharts(filteredData) {
        // Update time period information
        updateTimePeriodInfo(filteredData);

        // Clear existing chart section before redrawing
        const chartSection = document.getElementById('chartSection');
        if (chartSection) {
            chartSection.innerHTML = '';
        }

        // Add equipment analysis with refreshed data
        renderEquipmentAnalysis(filteredData);

        // Process the filtered data for charts
        const chartData = prepareChartData(filteredData);

        // Update charts based on current view
        switch (currentView) {
            case 'general':
                renderGeneralKPIs(filteredData);
                renderGeneralCharts(chartData);
                break;
            case 'corrective':
                renderCorrectiveKPIs(filteredData);
                renderCorrectiveCharts(chartData);
                break;
            case 'breakdown':
                renderBreakdownKPIs(filteredData);
                renderBreakdownCharts(chartData);
                break;
            default:
                // Default to general view
                renderGeneralKPIs(filteredData);
                renderGeneralCharts(chartData);
        }

        // Update top faults table with fresh data
        if (filteredData.length > 0) {
            const faultCounts = {};
            filteredData.forEach(item => {
                if (item.failure) {
                    faultCounts[item.failure] = (faultCounts[item.failure] || 0) + 1;
                }
            });
            
            // Sort and get top 10 faults
            const topFaults = Object.entries(faultCounts)
                .sort((a, b) => b[1] - a[1])
                .slice(0, 10)
                .reduce((obj, [key, val]) => ({...obj, [key]: val}), {});
                
            renderTopFaults(topFaults);
        }

        // Update date-based dropdowns and filters
        updateDateFilters(filteredData);
    }

    function updateTimePeriodInfo(data) {
        const orderDates = data
            .map(item => parseFrenchDate(item.Order_date))
            .filter(date => date && !isNaN(date.getTime()));
        
        const execDates = data
            .map(item => parseFrenchDate(item.Jobexec_dt))
            .filter(date => date && !isNaN(date.getTime()));

        if (orderDates.length > 0) {
            const minOrderDate = new Date(Math.min(...orderDates));
            const maxOrderDate = new Date(Math.max(...orderDates));
            document.getElementById('orderPeriod').textContent = 
                `${formatToFrenchDate(minOrderDate)} - ${formatToFrenchDate(maxOrderDate)}`;
        } else {
            document.getElementById('orderPeriod').textContent = 'No data';
        }

        if (execDates.length > 0) {
            const minExecDate = new Date(Math.min(...execDates));
            const maxExecDate = new Date(Math.max(...execDates));
            document.getElementById('execPeriod').textContent = 
                `${formatToFrenchDate(minExecDate)} - ${formatToFrenchDate(maxExecDate)}`;
        } else {
            document.getElementById('execPeriod').textContent = 'No data';
        }
    }

    function getEquipmentDetails(data) {
        // Group work orders by equipment name
        const equipmentStats = {};
        
        data.forEach(wo => {
            if (wo.EQ_name) {
                if (!equipmentStats[wo.EQ_name]) {
                    equipmentStats[wo.EQ_name] = {
                        totalWOs: 0,
                        completedWOs: 0,
                        avgRepairTime: 0,
                        failures: {},
                        type: wo.EQ_type || 'Unknown'
                    };
                }
                
                equipmentStats[wo.EQ_name].totalWOs++;
                
                if (wo.Jobexec_dt) {
                    equipmentStats[wo.EQ_name].completedWOs++;
                    const startDate = parseFrenchDate(wo.Order_date);
                    const endDate = parseFrenchDate(wo.Jobexec_dt);
                    if (startDate && endDate) {
                        const repairTime = (endDate - startDate) / (1000 * 60 * 60 * 24); // Convert to days
                        if (repairTime > 0) {
                            const currentTotal = equipmentStats[wo.EQ_name].avgRepairTime * (equipmentStats[wo.EQ_name].completedWOs - 1);
                            equipmentStats[wo.EQ_name].avgRepairTime = (currentTotal + repairTime) / equipmentStats[wo.EQ_name].completedWOs;
                        }
                    }
                }
                
                if (wo.failure) {
                    equipmentStats[wo.EQ_name].failures[wo.failure] = (equipmentStats[wo.EQ_name].failures[wo.failure] || 0) + 1;
                }
            }
        });

        return equipmentStats;
    }

    window.toggleEquipmentSection = function(index) {
        const content = document.getElementById('equipment-content-' + index);
        const chevron = document.getElementById('chevron-' + index);
        if (content && chevron) {
            content.classList.toggle('hidden');
            chevron.style.transform = content.classList.contains('hidden') ? 'rotate(0deg)' : 'rotate(180deg)';
        }
    };

    function renderEquipmentAnalysis(data) {
        const equipmentStats = getEquipmentDetails(data);
        
        // Group equipment by type
        const equipmentByType = {};
        Object.entries(equipmentStats).forEach(([eqName, stats]) => {
            if (!equipmentByType[stats.type]) {
                equipmentByType[stats.type] = {};
            }
            equipmentByType[stats.type][eqName] = stats;
        });

        // Create equipment analysis section if it doesn't exist
        let equipmentSection = document.getElementById('equipmentAnalysis');
        if (!equipmentSection) {
            equipmentSection = document.createElement('div');
            equipmentSection.id = 'equipmentAnalysis';
            equipmentSection.className = 'mb-6 space-y-4';
            document.getElementById('dashboard').insertBefore(
                equipmentSection,
                document.getElementById('chartSection')
            );
        }

        // Render equipment analysis with collapsible sections by type
        const equipmentHTML = [`
            <div class="bg-white dark:bg-gray-700 rounded-lg shadow-sm p-4">
                <h3 class="text-lg font-semibold text-gray-800 dark:text-white mb-4">Equipment Performance Analysis</h3>
                <div class="space-y-4">
        `];

        // Sort equipment types to ensure consistent order
        const sortedTypes = Object.keys(equipmentByType).sort();

        sortedTypes.forEach((type, typeIndex) => {
            const equipmentOfType = equipmentByType[type];
            const equipmentNames = Object.keys(equipmentOfType).sort();
            
            equipmentHTML.push(`
                <div class="equipment-type-section border dark:border-gray-600 rounded-lg">
                    <button class="w-full px-4 py-3 flex items-center justify-between text-left text-gray-800 dark:text-gray-200 hover:bg-gray-50 dark:hover:bg-gray-600 rounded-lg focus:outline-none" 
                            onclick="toggleEquipmentSection(${typeIndex})">
                        <span class="font-medium">${type}</span>
                        <i class="fas fa-chevron-down transform transition-transform duration-200" id="chevron-${typeIndex}"></i>
                    </button>
                    <div class="equipment-content hidden overflow-x-auto" id="equipment-content-${typeIndex}">
                        <table class="min-w-full divide-y divide-gray-200 dark:divide-gray-600">
                            <thead class="bg-gray-50 dark:bg-gray-800">
                                <tr>
                                    <th scope="col" class="px-6 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">Equipment</th>
                                    <th scope="col" class="px-6 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">Total WOs</th>
                                    <th scope="col" class="px-6 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">Completed</th>
                                    <th scope="col" class="px-6 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">Completion Rate</th>
                                    <th scope="col" class="px-6 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">Avg Repair Time</th>
                                    <th scope="col" class="px-6 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">Top Issue</th>
                                </tr>
                            </thead>
                            <tbody class="bg-white dark:bg-gray-700 divide-y divide-gray-200 dark:divide-gray-600">
            `);

            equipmentNames.forEach(eqName => {
                const stats = equipmentOfType[eqName];
                const completionRate = ((stats.completedWOs / stats.totalWOs) * 100).toFixed(1);
                
                // Format repair time using the formatTimeDuration helper
                const formattedTime = formatTimeDuration(stats.avgRepairTime);
                const repairTimeDisplay = formattedTime.display;
                const timeUnit = formattedTime.unit;

                const topFailure = Object.entries(stats.failures)
                    .sort((a, b) => b[1] - a[1])[0] || ['None', 0];

                equipmentHTML.push(`
                    <tr>
                        <td class="px-6 py-4 whitespace-nowrap text-sm font-medium text-gray-800 dark:text-gray-200">${eqName}</td>
                        <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-500 dark:text-gray-300">${stats.totalWOs}</td>
                        <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-500 dark:text-gray-300">${stats.completedWOs}</td>
                        <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-500 dark:text-gray-300">${completionRate}%</td>
                        <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-500 dark:text-gray-300">${repairTimeDisplay}${timeUnit}</td>
                        <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-500 dark:text-gray-300">${topFailure[0]} (${topFailure[1]})</td>
                    </tr>
                `);
            });

            equipmentHTML.push(`
                            </tbody>
                        </table>
                    </div>
                </div>
            `);
        });

        equipmentHTML.push(`
                </div>
            </div>
        `);

        equipmentSection.innerHTML = equipmentHTML.join('');

        // Show first equipment type section by default
        if (sortedTypes.length > 0) {
            setTimeout(() => toggleEquipmentSection(0), 100);
        }
    }

    function updateDateFilters(data) {
        // Get all dates from the data
        const orderDates = data
            .map(item => parseFrenchDate(item.Order_date))
            .filter(date => date && !isNaN(date.getTime()));
            
        const execDates = data
            .map(item => parseFrenchDate(item.Jobexec_dt))
            .filter(date => date && !isNaN(date.getTime()));

        if (orderDates.length > 0) {
            const minOrderDate = new Date(Math.min(...orderDates));
            const maxOrderDate = new Date(Math.max(...orderDates));

            // Get week numbers for order dates
            const minOrderWeek = getWeekNumber(minOrderDate);
            const maxOrderWeek = getWeekNumber(maxOrderDate);

            // Update order date inputs
            const orderStartDate = document.getElementById('orderStartDate');
            const orderEndDate = document.getElementById('orderEndDate');
            
            if (orderStartDate && orderEndDate) {
                orderStartDate.min = `${minOrderDate.getFullYear()}-W${String(minOrderWeek).padStart(2, '0')}`;
                orderEndDate.max = `${maxOrderDate.getFullYear()}-W${String(maxOrderWeek).padStart(2, '0')}`;
                
                // Set default values if not already set
                if (!orderStartDate.value) {
                    orderStartDate.value = orderStartDate.min;
                }
                if (!orderEndDate.value) {
                    orderEndDate.value = orderEndDate.max;
                }
            }
        }

        if (execDates.length > 0) {
            const minExecDate = new Date(Math.min(...execDates));
            const maxExecDate = new Date(Math.max(...execDates));

            // Get week numbers for execution dates
            const minExecWeek = getWeekNumber(minExecDate);
            const maxExecWeek = getWeekNumber(maxExecDate);

            // Update execution date inputs
            const execStartDate = document.getElementById('execStartDate');
            const execEndDate = document.getElementById('execEndDate');
            
            if (execStartDate && execEndDate) {
                execStartDate.min = `${minExecDate.getFullYear()}-W${String(minExecWeek).padStart(2, '0')}`;
                execEndDate.max = `${maxExecDate.getFullYear()}-W${String(maxExecWeek).padStart(2, '0')}`;
                
                // Set default values if not already set
                if (!execStartDate.value) {
                    execStartDate.value = execStartDate.min;
                }
                if (!execEndDate.value) {
                    execEndDate.value = execEndDate.max;
                }
            }
        }
    }
});
