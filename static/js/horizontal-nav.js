/**
 * Enhanced Horizontal Navigation JavaScript
 * Provides advanced functionality for the horizontal sliding navigation
 */

class HorizontalNavigation {
    constructor() {
        this.navContainer = document.getElementById('navContainer');
        this.scrollLeft = document.getElementById('scrollLeft');
        this.scrollRight = document.getElementById('scrollRight');
        this.navSearch = document.getElementById('navSearch');
        this.navItems = document.querySelectorAll('.nav-item');
        this.dropdowns = document.querySelectorAll('.nav-dropdown');
        
        this.init();
    }
    
    init() {
        this.bindEvents();
        this.checkScroll();
        this.setActiveNavItem();
        this.initializeSearch();
        this.initializeTouchSupport();
        this.initializeKeyboardSupport();
        this.loadUserPreferences();
    }
    
    bindEvents() {
        // Scroll button events
        this.scrollLeft?.addEventListener('click', () => this.scrollNavigation('left'));
        this.scrollRight?.addEventListener('click', () => this.scrollNavigation('right'));
        
        // Container events
        this.navContainer?.addEventListener('scroll', () => this.checkScroll());
        window.addEventListener('resize', () => this.checkScroll());
        
        // Click outside to close dropdowns
        document.addEventListener('click', (e) => this.handleOutsideClick(e));
        
        // Dropdown hover effects
        this.navItems.forEach(item => {
            const dropdown = item.querySelector('.nav-dropdown');
            if (dropdown) {
                item.addEventListener('mouseenter', () => this.showDropdown(dropdown));
                item.addEventListener('mouseleave', () => this.hideDropdown(dropdown));
            }
        });
    }
    
    scrollNavigation(direction) {
        const scrollAmount = 200;
        const scrollValue = direction === 'left' ? -scrollAmount : scrollAmount;
        
        this.navContainer.scrollBy({
            left: scrollValue,
            behavior: 'smooth'
        });
    }
    
    checkScroll() {
        if (!this.navContainer) return;
        
        const isScrollable = this.navContainer.scrollWidth > this.navContainer.clientWidth;
        const isAtStart = this.navContainer.scrollLeft <= 0;
        const isAtEnd = this.navContainer.scrollLeft >= this.navContainer.scrollWidth - this.navContainer.clientWidth;
        
        this.updateScrollIndicators(isScrollable, isAtStart, isAtEnd);
    }
    
    updateScrollIndicators(isScrollable, isAtStart, isAtEnd) {
        if (this.scrollLeft) {
            this.scrollLeft.style.display = isScrollable && !isAtStart ? 'block' : 'none';
        }
        if (this.scrollRight) {
            this.scrollRight.style.display = isScrollable && !isAtEnd ? 'block' : 'none';
        }
    }
    
    setActiveNavItem() {
        const currentPath = window.location.pathname;
        const navLinks = document.querySelectorAll('.nav-link');
        
        navLinks.forEach(link => {
            const linkPath = new URL(link.href).pathname;
            if (this.isActivePath(linkPath, currentPath)) {
                link.classList.add('active');
                this.scrollToActiveItem(link);
            } else {
                link.classList.remove('active');
            }
        });
    }
    
    isActivePath(linkPath, currentPath) {
        return linkPath === currentPath || 
               (currentPath !== '/' && currentPath.startsWith(linkPath) && linkPath !== '/');
    }
    
    scrollToActiveItem(activeLink) {
        const activeItem = activeLink.closest('.nav-item');
        if (activeItem && this.navContainer) {
            const itemRect = activeItem.getBoundingClientRect();
            const containerRect = this.navContainer.getBoundingClientRect();
            
            if (itemRect.left < containerRect.left || itemRect.right > containerRect.right) {
                activeItem.scrollIntoView({
                    behavior: 'smooth',
                    block: 'nearest',
                    inline: 'center'
                });
            }
        }
    }
    
    initializeSearch() {
        if (!this.navSearch) return;
        
        this.navSearch.addEventListener('input', (e) => this.handleSearch(e));
        this.navSearch.addEventListener('focus', () => this.navSearch.setAttribute('placeholder', 'Search navigation...'));
        this.navSearch.addEventListener('blur', () => this.navSearch.setAttribute('placeholder', 'Search...'));
    }
    
    handleSearch(e) {
        const searchTerm = e.target.value.toLowerCase().trim();
        
        if (searchTerm.length >= 2) {
            this.performSearch(searchTerm);
        } else {
            this.resetSearch();
        }
    }
    
    performSearch(searchTerm) {
        const allDropdownItems = document.querySelectorAll('.dropdown-item');
        let hasResults = false;
        
        // Hide all dropdowns first
        this.dropdowns.forEach(dropdown => this.hideDropdown(dropdown));
        
        allDropdownItems.forEach(item => {
            const text = item.textContent.toLowerCase();
            const parent = item.closest('.nav-dropdown');
            
            if (text.includes(searchTerm)) {
                item.style.display = 'flex';
                this.showDropdown(parent);
                hasResults = true;
                
                // Highlight search term
                this.highlightSearchTerm(item, searchTerm);
            } else {
                item.style.display = 'none';
            }
        });
        
        // Show "no results" message if needed
        if (!hasResults) {
            this.showNoResultsMessage();
        }
    }
    
    highlightSearchTerm(item, searchTerm) {
        const text = item.innerHTML;
        const regex = new RegExp(`(${searchTerm})`, 'gi');
        item.innerHTML = text.replace(regex, '<mark>$1</mark>');
    }
    
    resetSearch() {
        // Reset all dropdowns
        this.dropdowns.forEach(dropdown => {
            dropdown.style.opacity = '';
            dropdown.style.visibility = '';
            dropdown.style.transform = '';
        });
        
        // Reset all dropdown items
        document.querySelectorAll('.dropdown-item').forEach(item => {
            item.style.display = '';
            // Remove highlights
            item.innerHTML = item.innerHTML.replace(/<mark>(.*?)<\/mark>/gi, '$1');
        });
        
        this.hideNoResultsMessage();
    }
    
    showNoResultsMessage() {
        let noResultsDiv = document.getElementById('noSearchResults');
        if (!noResultsDiv) {
            noResultsDiv = document.createElement('div');
            noResultsDiv.id = 'noSearchResults';
            noResultsDiv.className = 'nav-dropdown active';
            noResultsDiv.innerHTML = `
                <div class="dropdown-header">
                    <h6><i class="bi bi-search me-2"></i>No Results Found</h6>
                </div>
                <div class="dropdown-item">
                    <i class="bi bi-info-circle"></i>
                    Try a different search term
                </div>
            `;
            noResultsDiv.style.position = 'fixed';
            noResultsDiv.style.top = '150px';
            noResultsDiv.style.left = '50%';
            noResultsDiv.style.transform = 'translateX(-50%)';
            noResultsDiv.style.zIndex = '1060';
            document.body.appendChild(noResultsDiv);
        }
        noResultsDiv.style.display = 'block';
    }
    
    hideNoResultsMessage() {
        const noResultsDiv = document.getElementById('noSearchResults');
        if (noResultsDiv) {
            noResultsDiv.style.display = 'none';
        }
    }
    
    showDropdown(dropdown) {
        if (dropdown) {
            dropdown.style.opacity = '1';
            dropdown.style.visibility = 'visible';
            dropdown.style.transform = 'translateY(0)';
        }
    }
    
    hideDropdown(dropdown) {
        if (dropdown) {
            dropdown.style.opacity = '';
            dropdown.style.visibility = '';
            dropdown.style.transform = '';
        }
    }
    
    initializeTouchSupport() {
        if (!this.navContainer) return;
        
        let startX = 0;
        let scrollStart = 0;
        let isScrolling = false;
        
        this.navContainer.addEventListener('touchstart', (e) => {
            startX = e.touches[0].clientX;
            scrollStart = this.navContainer.scrollLeft;
            isScrolling = true;
        }, { passive: true });
        
        this.navContainer.addEventListener('touchmove', (e) => {
            if (!isScrolling) return;
            
            const currentX = e.touches[0].clientX;
            const diff = startX - currentX;
            this.navContainer.scrollLeft = scrollStart + diff;
        }, { passive: true });
        
        this.navContainer.addEventListener('touchend', () => {
            isScrolling = false;
            this.checkScroll();
        }, { passive: true });
    }
    
    initializeKeyboardSupport() {
        document.addEventListener('keydown', (e) => {
            if (e.target === this.navSearch) return;
            
            if (e.ctrlKey || e.metaKey) {
                switch (e.key) {
                    case 'ArrowLeft':
                        e.preventDefault();
                        this.scrollNavigation('left');
                        break;
                    case 'ArrowRight':
                        e.preventDefault();
                        this.scrollNavigation('right');
                        break;
                    case 'f':
                    case 'F':
                        e.preventDefault();
                        this.focusSearch();
                        break;
                }
            }
            
            if (e.key === 'Escape') {
                this.resetSearch();
                if (this.navSearch) {
                    this.navSearch.value = '';
                    this.navSearch.blur();
                }
            }
        });
    }
    
    focusSearch() {
        if (this.navSearch) {
            this.navSearch.focus();
            this.navSearch.select();
        }
    }
    
    handleOutsideClick(e) {
        if (!e.target.closest('.nav-search') && !e.target.closest('.nav-dropdown')) {
            this.resetSearch();
            if (this.navSearch) {
                this.navSearch.value = '';
            }
        }
    }
    
    loadUserPreferences() {
        // Load saved navigation preferences
        const savedScrollPosition = localStorage.getItem('navScrollPosition');
        if (savedScrollPosition && this.navContainer) {
            this.navContainer.scrollLeft = parseInt(savedScrollPosition);
        }
        
        // Save scroll position on unload
        window.addEventListener('beforeunload', () => {
            if (this.navContainer) {
                localStorage.setItem('navScrollPosition', this.navContainer.scrollLeft);
            }
        });
    }
    
    // Public methods for external use
    scrollToItem(itemSelector) {
        const item = document.querySelector(itemSelector);
        if (item) {
            item.scrollIntoView({
                behavior: 'smooth',
                block: 'nearest',
                inline: 'center'
            });
        }
    }
    
    addNavigationItem(config) {
        // Method to dynamically add navigation items
        const { label, icon, href, dropdown } = config;
        const navItem = document.createElement('div');
        navItem.className = 'nav-item';
        
        navItem.innerHTML = `
            <a href="${href}" class="nav-link">
                <i class="bi ${icon}"></i>
                ${label}
            </a>
        `;
        
        if (dropdown) {
            const dropdownDiv = document.createElement('div');
            dropdownDiv.className = 'nav-dropdown';
            dropdownDiv.innerHTML = dropdown;
            navItem.appendChild(dropdownDiv);
        }
        
        this.navContainer.insertBefore(navItem, this.navContainer.querySelector('.nav-search'));
        this.checkScroll();
    }
}

// Initialize the navigation when DOM is loaded
document.addEventListener('DOMContentLoaded', function() {
    window.horizontalNav = new HorizontalNavigation();
});

// Export for use in other scripts
if (typeof module !== 'undefined' && module.exports) {
    module.exports = HorizontalNavigation;
}
