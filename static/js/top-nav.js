// Top Navigation JavaScript
document.addEventListener('DOMContentLoaded', function() {
    console.log('Top navigation script loaded');
    
    // Initialize top navigation functionality
    initializeTopNavigation();
    setActiveNavigation();
    initializeTheme();
    initializeSearch();
});

function initializeTopNavigation() {
    // No specific initialization needed for Bootstrap dropdowns
    // Bootstrap handles dropdown functionality automatically
    
    // Handle navbar collapse on mobile
    const navLinks = document.querySelectorAll('.navbar-nav .nav-link:not(.dropdown-toggle)');
    const navbarCollapse = document.querySelector('.navbar-collapse');
    
    navLinks.forEach(link => {
        link.addEventListener('click', () => {
            // Close mobile menu when a link is clicked
            if (window.innerWidth < 992 && navbarCollapse.classList.contains('show')) {
                const bsCollapse = new bootstrap.Collapse(navbarCollapse);
                bsCollapse.hide();
            }
        });
    });
}

function setActiveNavigation() {
    const currentPath = window.location.pathname;
    const allLinks = document.querySelectorAll('.nav-link, .dropdown-item');
    
    // Remove all active classes first
    allLinks.forEach(link => link.classList.remove('active'));
    
    allLinks.forEach(function(link) {
        if (link.href) {
            const linkPath = new URL(link.href, window.location.origin).pathname;
            
            if (linkPath === currentPath || (currentPath !== '/' && linkPath !== '/' && currentPath.startsWith(linkPath))) {
                link.classList.add('active');
                
                // If it's a dropdown item, also mark the parent dropdown as active
                if (link.classList.contains('dropdown-item')) {
                    const parentDropdown = link.closest('.dropdown');
                    if (parentDropdown) {
                        const parentLink = parentDropdown.querySelector('.dropdown-toggle');
                        if (parentLink) {
                            parentLink.classList.add('active');
                        }
                    }
                }
            }
        }
    });
}

function initializeTheme() {
    // Load saved theme
    const savedTheme = localStorage.getItem('theme') || 'light';
    setTheme(savedTheme);
    
    // Theme option click handlers
    document.querySelectorAll('.theme-option').forEach(function(option) {
        option.addEventListener('click', function() {
            const theme = this.getAttribute('data-theme');
            setTheme(theme);
        });
    });
}

function setTheme(theme) {
    document.body.setAttribute('data-theme', theme);
    localStorage.setItem('theme', theme);
    
    // Update active theme indicator
    document.querySelectorAll('.theme-option').forEach(function(option) {
        option.classList.remove('active');
    });
    
    const activeOption = document.querySelector('.theme-' + theme);
    if (activeOption) {
        activeOption.classList.add('active');
    }
}

function initializeSearch() {
    const searchInputs = document.querySelectorAll('#topNavSearch, #mobileNavSearch');
    
    searchInputs.forEach(searchInput => {
        if (searchInput) {
            searchInput.addEventListener('input', function() {
                const searchTerm = this.value.toLowerCase();
                filterNavigation(searchTerm);
            });
        }
    });
}

function filterNavigation(searchTerm) {
    const dropdownItems = document.querySelectorAll('.dropdown-item');
    const dropdowns = document.querySelectorAll('.nav-item.dropdown');
    
    if (!searchTerm) {
        // Show all items
        dropdownItems.forEach(item => {
            item.style.display = '';
        });
        dropdowns.forEach(dropdown => {
            dropdown.style.display = '';
        });
        return;
    }
    
    // Filter dropdown items
    dropdownItems.forEach(item => {
        const text = item.textContent.toLowerCase();
        const matches = text.includes(searchTerm);
        item.style.display = matches ? '' : 'none';
    });
    
    // Hide dropdowns that have no visible items
    dropdowns.forEach(dropdown => {
        const visibleItems = dropdown.querySelectorAll('.dropdown-item:not([style*="display: none"])');
        dropdown.style.display = visibleItems.length > 0 ? '' : 'none';
    });
}

// Global function for theme switching (called from dropdown)
window.setTheme = setTheme;
