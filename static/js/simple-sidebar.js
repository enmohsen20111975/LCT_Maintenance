// Simple and Direct Sidebar Navigation
document.addEventListener('DOMContentLoaded', function() {
    console.log('Sidebar script loaded');
    
    // Initialize sidebar functionality
    initializeSidebar();
    initializeSubmenus();
    setActiveNavigation();
    initializeTheme();
});

function initializeSidebar() {
    const sidebarToggle = document.getElementById('sidebarToggle');
    const sidebar = document.getElementById('sidebar');
    const sidebarOverlay = document.getElementById('sidebarOverlay');
    const mainContent = document.querySelector('.main-content');
    
    if (!sidebarToggle || !sidebar) {
        console.error('Sidebar elements not found');
        return;
    }
    
    // Toggle sidebar
    sidebarToggle.addEventListener('click', function() {
        if (window.innerWidth <= 768) {
            // Mobile
            sidebar.classList.toggle('mobile-open');
            if (sidebarOverlay) {
                sidebarOverlay.classList.toggle('active');
            }
        } else {
            // Desktop
            sidebar.classList.toggle('collapsed');
            if (mainContent) {
                mainContent.classList.toggle('sidebar-collapsed');
            }
        }
    });
    
    // Close on overlay click (mobile)
    if (sidebarOverlay) {
        sidebarOverlay.addEventListener('click', function() {
            sidebar.classList.remove('mobile-open');
            sidebarOverlay.classList.remove('active');
        });
    }
    
    // Handle window resize
    window.addEventListener('resize', function() {
        if (window.innerWidth > 768) {
            sidebar.classList.remove('mobile-open');
            if (sidebarOverlay) {
                sidebarOverlay.classList.remove('active');
            }
        }
    });
}

function initializeSubmenus() {
    const submenuLinks = document.querySelectorAll('[data-toggle="submenu"]');
    console.log('Found submenu links:', submenuLinks.length);
    
    submenuLinks.forEach(function(link, index) {
        console.log('Setting up submenu', index + 1, ':', link.textContent.trim());
        
        link.addEventListener('click', function(e) {
            e.preventDefault();
            console.log('Submenu clicked:', this.textContent.trim());
            
            // Find parent nav item
            const navItem = this.closest('.nav-item.has-submenu');
            const submenu = navItem ? navItem.querySelector('.nav-submenu') : null;
            
            if (navItem && submenu) {
                console.log('Toggling submenu...');
                
                // Toggle classes
                navItem.classList.toggle('expanded');
                submenu.classList.toggle('expanded');
                
                // Log state
                const isExpanded = navItem.classList.contains('expanded');
                console.log('Submenu is now:', isExpanded ? 'expanded' : 'collapsed');
                
                // Save state
                const submenuId = this.textContent.trim().replace(/\s+/g, '_');
                localStorage.setItem('submenu_' + submenuId, isExpanded);
            } else {
                console.error('Could not find nav item or submenu', {navItem, submenu});
            }
        });
    });
    
    // Restore saved states
    restoreSubmenuStates();
}

function restoreSubmenuStates() {
    const submenuLinks = document.querySelectorAll('[data-toggle="submenu"]');
    
    submenuLinks.forEach(function(link) {
        const submenuId = link.textContent.trim().replace(/\s+/g, '_');
        const savedState = localStorage.getItem('submenu_' + submenuId);
        
        if (savedState === 'true') {
            const navItem = link.closest('.nav-item.has-submenu');
            const submenu = navItem ? navItem.querySelector('.nav-submenu') : null;
            
            if (navItem && submenu) {
                navItem.classList.add('expanded');
                submenu.classList.add('expanded');
            }
        }
    });
}

function setActiveNavigation() {
    const currentPath = window.location.pathname;
    const allLinks = document.querySelectorAll('.nav-link, .submenu-item');
    
    allLinks.forEach(function(link) {
        const linkPath = new URL(link.href, window.location.origin).pathname;
        
        if (linkPath === currentPath || (currentPath !== '/' && linkPath !== '/' && currentPath.startsWith(linkPath))) {
            link.classList.add('active');
            
            // If it's a submenu item, expand parent
            if (link.classList.contains('submenu-item')) {
                const parentNavItem = link.closest('.nav-item.has-submenu');
                const parentSubmenu = link.closest('.nav-submenu');
                
                if (parentNavItem && parentSubmenu) {
                    parentNavItem.classList.add('expanded');
                    parentSubmenu.classList.add('expanded');
                }
            }
        } else {
            link.classList.remove('active');
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
