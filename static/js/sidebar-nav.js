/**
 * Sidebar Navigation JavaScript
 * Provides functionality for the sidebar navigation menu
 */

class SidebarNavigation {
    constructor() {
        this.sidebar = document.getElementById('sidebar');
        this.sidebarToggle = document.getElementById('sidebarToggle');
        this.sidebarOverlay = document.getElementById('sidebarOverlay');
        this.sidebarSearch = document.getElementById('sidebarSearch');
        this.mainContent = document.querySelector('.main-content');
        this.navItems = document.querySelectorAll('.nav-item');
        this.submenuItems = document.querySelectorAll('.submenu-item');
        
        this.init();
    }
    
    init() {
        this.bindEvents();
        this.setActiveNavItem();
        this.initializeSearch();
        this.initializeKeyboardSupport();
        this.loadUserPreferences();
    }
    
    bindEvents() {
        console.log('Binding sidebar events...');
        
        // Sidebar toggle functionality
        if (this.sidebarToggle) {
            this.sidebarToggle.addEventListener('click', () => this.toggleSidebar());
            console.log('Sidebar toggle bound');
        }
        
        // Overlay click to close mobile sidebar
        if (this.sidebarOverlay) {
            this.sidebarOverlay.addEventListener('click', () => this.closeMobileSidebar());
        }
        
        // Submenu toggle functionality
        const submenuToggles = document.querySelectorAll('[data-toggle="submenu"]');
        console.log('Found submenu toggles:', submenuToggles.length);
        
        submenuToggles.forEach((link, index) => {
            console.log(`Binding submenu ${index}:`, link.textContent.trim());
            link.addEventListener('click', (e) => {
                console.log('Submenu clicked:', link.textContent.trim());
                this.toggleSubmenu(e);
            });
        });
        
        // Window resize handler
        window.addEventListener('resize', () => this.handleResize());
        
        // Auto-close mobile sidebar when clicking nav links
        document.querySelectorAll('.nav-link, .submenu-item').forEach(link => {
            link.addEventListener('click', (e) => this.handleNavClick(e));
        });
    }
    
    toggleSidebar() {
        if (window.innerWidth <= 768) {
            // Mobile behavior
            this.sidebar.classList.toggle('mobile-open');
            this.sidebarOverlay.classList.toggle('active');
        } else {
            // Desktop behavior
            this.sidebar.classList.toggle('collapsed');
            this.mainContent.classList.toggle('sidebar-collapsed');
            
            // Save preference
            const isCollapsed = this.sidebar.classList.contains('collapsed');
            localStorage.setItem('sidebarCollapsed', isCollapsed);
        }
    }
    
    closeMobileSidebar() {
        this.sidebar.classList.remove('mobile-open');
        this.sidebarOverlay.classList.remove('active');
    }
    
    toggleSubmenu(e) {
        e.preventDefault();
        console.log('toggleSubmenu called');
        
        const navItem = e.target.closest('.nav-item');
        const submenu = navItem.querySelector('.nav-submenu');
        
        console.log('navItem:', navItem);
        console.log('submenu:', submenu);
        
        if (!navItem || !submenu) {
            console.error('Missing navItem or submenu');
            return;
        }
        
        // Toggle expanded state
        const wasExpanded = navItem.classList.contains('expanded');
        navItem.classList.toggle('expanded');
        submenu.classList.toggle('expanded');
        
        console.log('Submenu toggled. Was expanded:', wasExpanded, 'Now expanded:', navItem.classList.contains('expanded'));
        
        // Save expanded state
        const submenuId = navItem.querySelector('.nav-link').textContent.trim();
        const isExpanded = navItem.classList.contains('expanded');
        this.saveSubmenuState(submenuId, isExpanded);
    }
    
    handleResize() {
        if (window.innerWidth > 768) {
            // Reset mobile states on desktop
            this.sidebar.classList.remove('mobile-open');
            this.sidebarOverlay.classList.remove('active');
        }
    }
    
    handleNavClick(e) {
        if (window.innerWidth <= 768 && !e.target.closest('[data-toggle="submenu"]')) {
            this.closeMobileSidebar();
        }
    }
    
    setActiveNavItem() {
        const currentPath = window.location.pathname;
        const allLinks = document.querySelectorAll('.nav-link, .submenu-item');
        
        allLinks.forEach(link => {
            const linkPath = new URL(link.href, window.location.origin).pathname;
            
            if (linkPath === currentPath || (currentPath !== '/' && currentPath.startsWith(linkPath))) {
                link.classList.add('active');
                
                // If it's a submenu item, expand the parent
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
    
    initializeSearch() {
        if (!this.sidebarSearch) return;
        
        this.sidebarSearch.addEventListener('input', (e) => {
            const searchTerm = e.target.value.toLowerCase();
            this.filterNavItems(searchTerm);
        });
        
        // Clear search on escape
        this.sidebarSearch.addEventListener('keydown', (e) => {
            if (e.key === 'Escape') {
                this.sidebarSearch.value = '';
                this.filterNavItems('');
            }
        });
    }
    
    filterNavItems(searchTerm) {
        const navItems = document.querySelectorAll('.nav-item');
        const submenuItems = document.querySelectorAll('.submenu-item');
        
        if (searchTerm.length > 0) {
            navItems.forEach(item => {
                const text = item.textContent.toLowerCase();
                const hasSubmenu = item.classList.contains('has-submenu');
                
                if (text.includes(searchTerm)) {
                    item.style.display = 'block';
                    if (hasSubmenu) {
                        // Expand submenu if parent matches
                        item.classList.add('expanded');
                        const submenu = item.querySelector('.nav-submenu');
                        if (submenu) {
                            submenu.classList.add('expanded');
                        }
                    }
                } else {
                    // Check if any submenu items match
                    if (hasSubmenu) {
                        const submenuItems = item.querySelectorAll('.submenu-item');
                        let hasMatchingSubmenu = false;
                        
                        submenuItems.forEach(subItem => {
                            const subText = subItem.textContent.toLowerCase();
                            if (subText.includes(searchTerm)) {
                                hasMatchingSubmenu = true;
                                subItem.style.display = 'flex';
                            } else {
                                subItem.style.display = 'none';
                            }
                        });
                        
                        if (hasMatchingSubmenu) {
                            item.style.display = 'block';
                            item.classList.add('expanded');
                            const submenu = item.querySelector('.nav-submenu');
                            if (submenu) {
                                submenu.classList.add('expanded');
                            }
                        } else {
                            item.style.display = 'none';
                        }
                    } else {
                        item.style.display = 'none';
                    }
                }
            });
        } else {
            // Reset all items
            navItems.forEach(item => {
                item.style.display = 'block';
                // Don't reset expanded state here - restore saved state instead
                this.restoreSubmenuStates();
            });
            
            submenuItems.forEach(item => {
                item.style.display = 'flex';
            });
            
            // Restore active states
            this.setActiveNavItem();
        }
    }
    
    initializeKeyboardSupport() {
        document.addEventListener('keydown', (e) => {
            // Ctrl/Cmd + Shift + S to toggle sidebar
            if ((e.ctrlKey || e.metaKey) && e.shiftKey && e.key === 'S') {
                e.preventDefault();
                this.toggleSidebar();
            }
            
            // Escape to close mobile sidebar
            if (e.key === 'Escape' && window.innerWidth <= 768) {
                this.closeMobileSidebar();
            }
            
            // Arrow key navigation
            if (e.target.closest('.sidebar')) {
                this.handleArrowKeys(e);
            }
        });
    }
    
    handleArrowKeys(e) {
        const focusableElements = this.sidebar.querySelectorAll('.nav-link, .submenu-item');
        const currentIndex = Array.from(focusableElements).indexOf(document.activeElement);
        
        switch (e.key) {
            case 'ArrowDown':
                e.preventDefault();
                if (currentIndex < focusableElements.length - 1) {
                    focusableElements[currentIndex + 1].focus();
                }
                break;
            case 'ArrowUp':
                e.preventDefault();
                if (currentIndex > 0) {
                    focusableElements[currentIndex - 1].focus();
                }
                break;
            case 'Enter':
            case ' ':
                if (document.activeElement.hasAttribute('data-toggle')) {
                    e.preventDefault();
                    document.activeElement.click();
                }
                break;
        }
    }
    
    loadUserPreferences() {
        // Load sidebar collapsed state
        const isCollapsed = localStorage.getItem('sidebarCollapsed') === 'true';
        if (isCollapsed && window.innerWidth > 768) {
            this.sidebar.classList.add('collapsed');
            this.mainContent.classList.add('sidebar-collapsed');
        }
        
        // Load submenu states
        this.restoreSubmenuStates();
    }
    
    saveSubmenuState(submenuId, isExpanded) {
        const submenuStates = JSON.parse(localStorage.getItem('submenuStates') || '{}');
        submenuStates[submenuId] = isExpanded;
        localStorage.setItem('submenuStates', JSON.stringify(submenuStates));
    }
    
    restoreSubmenuStates() {
        const submenuStates = JSON.parse(localStorage.getItem('submenuStates') || '{}');
        
        Object.entries(submenuStates).forEach(([submenuId, isExpanded]) => {
            const navItems = document.querySelectorAll('.nav-item.has-submenu');
            navItems.forEach(item => {
                const linkText = item.querySelector('.nav-link').textContent.trim();
                if (linkText === submenuId && isExpanded) {
                    item.classList.add('expanded');
                    const submenu = item.querySelector('.nav-submenu');
                    if (submenu) {
                        submenu.classList.add('expanded');
                    }
                }
            });
        });
    }
    
    // Public API methods
    expand() {
        if (window.innerWidth > 768) {
            this.sidebar.classList.remove('collapsed');
            this.mainContent.classList.remove('sidebar-collapsed');
            localStorage.setItem('sidebarCollapsed', 'false');
        } else {
            this.sidebar.classList.add('mobile-open');
            this.sidebarOverlay.classList.add('active');
        }
    }
    
    collapse() {
        if (window.innerWidth > 768) {
            this.sidebar.classList.add('collapsed');
            this.mainContent.classList.add('sidebar-collapsed');
            localStorage.setItem('sidebarCollapsed', 'true');
        } else {
            this.closeMobileSidebar();
        }
    }
    
    isCollapsed() {
        return this.sidebar.classList.contains('collapsed') || 
               (window.innerWidth <= 768 && !this.sidebar.classList.contains('mobile-open'));
    }
    
    addNavigationItem(config) {
        // Method to dynamically add navigation items
        const { label, icon, href, submenu, parent } = config;
        const navItem = document.createElement('div');
        navItem.className = submenu ? 'nav-item has-submenu' : 'nav-item';
        
        const navLink = document.createElement('a');
        navLink.href = href || '#';
        navLink.className = 'nav-link';
        if (submenu) {
            navLink.setAttribute('data-toggle', 'submenu');
        }
        
        navLink.innerHTML = `
            <i class="bi ${icon}"></i>
            ${label}
        `;
        
        navItem.appendChild(navLink);
        
        if (submenu && Array.isArray(submenu)) {
            const submenuDiv = document.createElement('div');
            submenuDiv.className = 'nav-submenu';
            
            submenu.forEach(subItem => {
                const submenuItem = document.createElement('a');
                submenuItem.href = subItem.href;
                submenuItem.className = 'submenu-item';
                submenuItem.innerHTML = `
                    <i class="bi ${subItem.icon}"></i>
                    <div>
                        ${subItem.label}
                        ${subItem.description ? `<small>${subItem.description}</small>` : ''}
                    </div>
                `;
                submenuDiv.appendChild(submenuItem);
            });
            
            navItem.appendChild(submenuDiv);
        }
        
        // Insert the item
        const navItems = document.querySelector('.nav-items');
        if (parent) {
            const parentItem = Array.from(navItems.children).find(item => 
                item.querySelector('.nav-link').textContent.trim() === parent
            );
            if (parentItem) {
                navItems.insertBefore(navItem, parentItem.nextSibling);
            } else {
                navItems.appendChild(navItem);
            }
        } else {
            navItems.appendChild(navItem);
        }
        
        // Rebind events for the new item
        if (submenu) {
            navLink.addEventListener('click', (e) => this.toggleSubmenu(e));
        }
        
        if (navItem.querySelector('.submenu-item')) {
            navItem.querySelectorAll('.submenu-item').forEach(link => {
                link.addEventListener('click', (e) => this.handleNavClick(e));
            });
        }
    }
}

// Initialize the sidebar navigation when DOM is loaded
document.addEventListener('DOMContentLoaded', function() {
    window.sidebarNav = new SidebarNavigation();
    
    // Theme management
    const savedTheme = localStorage.getItem('theme') || 'light';
    setTheme(savedTheme);
    
    // Theme option click handlers
    document.querySelectorAll('.theme-option').forEach(option => {
        option.addEventListener('click', function() {
            const theme = this.getAttribute('data-theme');
            setTheme(theme);
        });
    });
});

// Theme management function
function setTheme(theme) {
    document.body.setAttribute('data-theme', theme);
    localStorage.setItem('theme', theme);
    
    // Update active theme indicator
    document.querySelectorAll('.theme-option').forEach(option => {
        option.classList.remove('active');
    });
    
    const activeOption = document.querySelector(`.theme-${theme}`);
    if (activeOption) {
        activeOption.classList.add('active');
    }
}

// Export for use in other scripts
if (typeof module !== 'undefined' && module.exports) {
    module.exports = SidebarNavigation;
}
