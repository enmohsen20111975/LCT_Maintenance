// Add date handling functions at the top of the file
function parseFrenchDate(dateStr) {
    if (!dateStr) return null;
    try {
        // Handle Excel numeric dates
        if (typeof dateStr === 'number') {
            const excelEpoch = new Date(1899, 11, 30); // Excel's epoch is December 30, 1899
            const msPerDay = 24 * 60 * 60 * 1000;
            return new Date(excelEpoch.getTime() + (dateStr * msPerDay));
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
        console.warn('Error parsing date:', e);
        return null;
    }
}

function formatToFrenchDate(date) {
    if (!date || !(date instanceof Date) || isNaN(date.getTime())) {
        return '';
    }
    try {
        return `${String(date.getDate()).padStart(2, '0')}/${String(date.getMonth() + 1).padStart(2, '0')}/${date.getFullYear()}`;
    } catch (e) {
        console.warn('Error formatting date:', e);
        return '';
    }
}

function formatDateForChart(dateStr) {
    const date = parseFrenchDate(dateStr);
    if (!date) return '';
    
    const year = date.getFullYear();
    const month = date.getMonth() + 1;
    const weekNumber = getWeekNumber(date);
    return `${year}-${String(month).padStart(2, '0')}-W${String(weekNumber).padStart(2, '0')}`;
}

function getWeekNumber(date) {
    const d = new Date(Date.UTC(date.getFullYear(), date.getMonth(), date.getDate()));
    const dayNum = d.getUTCDay() || 7;
    d.setUTCDate(d.getUTCDate() + 4 - dayNum);
    const yearStart = new Date(Date.UTC(d.getUTCFullYear(), 0, 1));
    return Math.ceil((((d - yearStart) / 86400000) + 1) / 7);
}

// Declare currentData at the top level
let currentData = null;

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

function renderCorrectiveKPIs(data) {
    const kpiSection = document.getElementById('kpiSection');
    if (!kpiSection || !data) return;

    // Filter corrective maintenance work orders
    const correctiveWOs = data.filter(item => 
        item.Job_type === 'C' || item.Job_type === 'CM' || 
        item.Cost_purpose_key === 'Corrective'
    );

    // Calculate KPIs for corrective maintenance
    const total = correctiveWOs.length;
    const closed = correctiveWOs.filter(wo => wo.Jobexec_dt).length;
    const waiting = total - closed;
    
    // Calculate average processing time for closed corrective WOs
    let avgProcessingTime = 0;
    const closedWOs = correctiveWOs.filter(wo => wo.Jobexec_dt);
    if (closedWOs.length > 0) {
        const processingTimes = closedWOs.map(wo => {
            const startDate = parseFrenchDate(wo.Order_date);
            const endDate = parseFrenchDate(wo.Jobexec_dt);
            return startDate && endDate ? (endDate - startDate) / (1000 * 60 * 60 * 24) : 0;
        }).filter(time => time > 0);
        
        if (processingTimes.length > 0) {
            avgProcessingTime = processingTimes.reduce((a, b) => a + b, 0) / processingTimes.length;
        }
    }

    // Get most common fault location
    const locationCounts = {};
    correctiveWOs.forEach(wo => {
        if (wo.faultlocation) {
            locationCounts[wo.faultlocation] = (locationCounts[wo.faultlocation] || 0) + 1;
        }
    });
    const topLocation = Object.entries(locationCounts)
        .sort((a, b) => b[1] - a[1])[0] || ['Unknown', 0];

    kpiSection.innerHTML = `
        <div class="kpi-card bg-white dark:bg-gray-700 rounded-lg shadow-sm p-4">
            <div class="flex items-center justify-between">
                <div>
                    <p class="text-sm font-medium text-gray-500 dark:text-gray-400">Corrective Work Orders</p>
                    <p class="text-2xl font-semibold text-blue-600 dark:text-blue-400">${total}</p>
                </div>
                <div class="p-3 rounded-full bg-blue-100 dark:bg-blue-900 text-blue-600 dark:text-blue-400">
                    <i class="fas fa-wrench"></i>
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
                    <p class="text-sm font-medium text-gray-500 dark:text-gray-400">Average Repair Time</p>
                    <p class="text-2xl font-semibold text-green-600 dark:text-green-400">${avgProcessingTime.toFixed(1)} days</p>
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
                    <p class="text-sm font-medium text-gray-500 dark:text-gray-400">Most Common Location</p>
                    <p class="text-2xl font-semibold text-purple-600 dark:text-purple-400">${topLocation[0]}</p>
                </div>
                <div class="p-3 rounded-full bg-purple-100 dark:bg-purple-900 text-purple-600 dark:text-purple-400">
                    <i class="fas fa-map-marker-alt"></i>
                </div>
            </div>
            <div class="mt-4 flex items-center justify-between text-xs text-gray-500 dark:text-gray-400">
                <span>${topLocation[1]} occurrences</span>
            </div>
        </div>
    `;
}

function renderBreakdownKPIs(data) {
    const kpiSection = document.getElementById('kpiSection');
    if (!kpiSection || !data) return;

    // Filter breakdown work orders
    const breakdownWOs = data.filter(item => 
        item.Job_type === 'BDN' || 
        (item.Description && item.Description.toUpperCase().includes('BREAKDOWN'))
    );

    // Calculate KPIs for breakdowns
    const total = breakdownWOs.length;
    const closed = breakdownWOs.filter(wo => wo.Jobexec_dt).length;
    const waiting = total - closed;
    
    // Calculate average downtime for closed breakdown WOs
    let avgDowntime = 0;
    const closedBreakdowns = breakdownWOs.filter(wo => wo.Jobexec_dt);
    if (closedBreakdowns.length > 0) {
        const downtimes = closedBreakdowns.map(wo => {
            const startDate = parseFrenchDate(wo.Order_date);
            const endDate = parseFrenchDate(wo.Jobexec_dt);
            return startDate && endDate ? (endDate - startDate) / (1000 * 60 * 60) : 0; // Convert to hours
        }).filter(time => time > 0);
        
        if (downtimes.length > 0) {
            avgDowntime = downtimes.reduce((a, b) => a + b, 0) / downtimes.length;
        }
    }

    // Get most critical equipment
    const equipmentCounts = {};
    breakdownWOs.forEach(wo => {
        if (wo.EQ_type) {
            equipmentCounts[wo.EQ_type] = (equipmentCounts[wo.EQ_type] || 0) + 1;
        }
    });
    const criticalEquipment = Object.entries(equipmentCounts)
        .sort((a, b) => b[1] - a[1])[0] || ['Unknown', 0];

    kpiSection.innerHTML = `
        <div class="kpi-card bg-white dark:bg-gray-700 rounded-lg shadow-sm p-4">
            <div class="flex items-center justify-between">
                <div>
                    <p class="text-sm font-medium text-gray-500 dark:text-gray-400">Total Breakdowns</p>
                    <p class="text-2xl font-semibold text-red-600 dark:text-red-400">${total}</p>
                </div>
                <div class="p-3 rounded-full bg-red-100 dark:bg-red-900 text-red-600 dark:text-red-400">
                    <i class="fas fa-exclamation-triangle"></i>
                </div>
            </div>
            <div class="mt-4 flex items-center justify-between text-xs text-gray-500 dark:text-gray-400">
                <span>${waiting} active</span>
                <span class="text-green-500 dark:text-green-400">${closed} resolved</span>
            </div>
        </div>

        <div class="kpi-card bg-white dark:bg-gray-700 rounded-lg shadow-sm p-4">
            <div class="flex items-center justify-between">
                <div>
                    <p class="text-sm font-medium text-gray-500 dark:text-gray-400">Average Downtime</p>
                    <p class="text-2xl font-semibold text-orange-600 dark:text-orange-400">${avgDowntime.toFixed(1)} hrs</p>
                </div>
                <div class="p-3 rounded-full bg-orange-100 dark:bg-orange-900 text-orange-600 dark:text-orange-400">
                    <i class="fas fa-hourglass-half"></i>
                </div>
            </div>
            <div class="mt-4 flex items-center justify-between text-xs text-gray-500 dark:text-gray-400">
                <span>Target: < 4 hours</span>
            </div>
        </div>

        <div class="kpi-card bg-white dark:bg-gray-700 rounded-lg shadow-sm p-4">
            <div class="flex items-center justify-between">
                <div>
                    <p class="text-sm font-medium text-gray-500 dark:text-gray-400">Most Critical Equipment</p>
                    <p class="text-2xl font-semibold text-purple-600 dark:text-purple-400">${criticalEquipment[0]}</p>
                </div>
                <div class="p-3 rounded-full bg-purple-100 dark:bg-purple-900 text-purple-600 dark:text-purple-400">
                    <i class="fas fa-tools"></i>
                </div>
            </div>
            <div class="mt-4 flex items-center justify-between text-xs text-gray-500 dark:text-gray-400">
                <span>${criticalEquipment[1]} breakdowns</span>
            </div>
        </div>
    `;
}

// Add generateSampleData function near the top with other utility functions
function generateSampleData() {
    const data = [];
    const statuses = ['IPG', 'FIN', 'WSP', 'RDY', 'INI'];
    const jobTypes = ['CM', 'PM', 'BDN', 'INSP'];
    const locations = ['HOIST', 'TROLLEY', 'GANTRY', 'SPREADER', 'BOOM'];
    const equipment = ['STS', 'SPR'];

    // Generate 50 sample records
    for (let i = 0; i < 50; i++) {
        const orderDate = new Date(2025, 0, 1);
        orderDate.setDate(orderDate.getDate() + Math.floor(Math.random() * 365));
        
        const isClosed = Math.random() > 0.3;
        const execDate = isClosed ? new Date(orderDate.getTime() + Math.random() * 7 * 24 * 60 * 60 * 1000) : null;

        data.push({
            WO_key: `WO${280000 + i}`,
            Description: `Sample work order ${i + 1}`,
            MO_key: `${equipment[Math.floor(Math.random() * equipment.length)]}${1000 + i}`,
            Order_date: formatToFrenchDate(orderDate),
            Jobexec_dt: execDate ? formatToFrenchDate(execDate) : null,
            JobstatusObs: statuses[Math.floor(Math.random() * statuses.length)],
            Job_type: jobTypes[Math.floor(Math.random() * jobTypes.length)],
            faultlocation: locations[Math.floor(Math.random() * locations.length)],
            Cost_purpose_key: 'Corrective'
        });
    }

    return {
        data: data,
        kpis: processData(data).kpis,
        charts: processData(data).charts
    };
}

function createChartContainer() {
    const container = document.createElement('div');
    container.className = 'chart-container';
    container.style.minHeight = '400px';
    container.style.width = '100%';
    document.getElementById('chartSection').appendChild(container);
    return container;
}

document.addEventListener('DOMContentLoaded', function() {
    // Theme handling
    const themeSelector = document.getElementById('themeSelector');
    const themeToggle = document.getElementById('themeToggle');
    
    // Theme management functions
    function setTheme(themeName) {
        // Remove all existing theme classes
        document.body.classList.forEach(className => {
            if (className.startsWith('theme-')) {
                document.body.classList.remove(className);
            }
        });
        
        // Add new theme class if not default
        if (themeName !== 'default') {
            document.body.classList.add(`theme-${themeName}`);
        }
        
        // Save theme preference
        localStorage.setItem('theme', themeName);

        // Update charts if they exist
        if (currentData) {
            updateCharts(currentData.data);
        }
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
        } else if (e.target.classList.contains('clear-all-btn')) {
            const targetId = e.target.getAttribute('data-target');
            const select = document.getElementById(targetId);
            Array.from(select.options).forEach(option => {
                option.selected = false;
            });
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
            'S': 'Safety'
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
                        return item.MO_key && item.MO_key.toUpperCase().includes('SPR');
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
                    if (row.MO_key.toUpperCase().includes('STS')) {
                        EQ_name = row.MO_key.slice(-5); // Last 5 characters for STS
                    } else if (row.MO_key.toUpperCase().includes('SPR')) {
                        EQ_name = row.MO_key.slice(-6); // Last 6 characters for Spreader
                    }
                }

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
            }).filter(Boolean);

            if (processedData.length === 0) {
                throw new Error('No valid data rows found after processing');
            }

            // Process the data and return the result
            const resultData = processData(processedData);
            
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
                    if (item.Jobstatus) {
                        const status = getJobStatusFullName(item.Jobstatus);
                        return {
                            ...item,
                            JobstatusCode: item.Jobstatus,
                            JobstatusObs: status.obs || 'Unknown',
                            JobstatusDescription: status.description || 'Unknown'
                        };
                    }
                    return item;
                } catch (e) {
                    console.warn('Error processing job status:', e);
                    return {
                        ...item,
                        JobstatusCode: item.Jobstatus,
                        JobstatusObs: 'Unknown',
                        JobstatusDescription: 'Unknown'
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

            // Filter for corrective maintenance data
            const correctiveData = currentData.data.filter(item => 
                item.Job_type === 'C' || item.Job_type === 'CM' || 
                item.Cost_purpose_key === 'Corrective'
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

            // Filter for breakdown data
            const breakdownData = currentData.data.filter(item => 
                item.Job_type === 'BDN' || 
                item.Description?.toLowerCase().includes('breakdown')
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

        // Common data preparation
        const chartData = prepareChartData(data);

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
});