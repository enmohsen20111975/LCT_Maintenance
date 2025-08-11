/**
 * Universal Menu System - JavaScript Helper
 * Ensures all dynamically created menus follow the consistent styling
 */

class UniversalMenu {
    constructor() {
        this.initializeMenuSystem();
    }

    initializeMenuSystem() {
        // Apply universal styling to existing menus
        this.applyUniversalStyling();
        
        // Set up observers for dynamic content
        this.setupMutationObserver();
        
        // Initialize keyboard navigation
        this.setupKeyboardNavigation();
        
        // Initialize context menu support
        this.setupContextMenus();
    }

    applyUniversalStyling() {
        // Find all menu containers and apply consistent classes
        const menuSelectors = [
            '.dropdown-menu',
            '.context-menu',
            '.nav-menu',
            '.submenu',
            '.menu-container'
        ];

        menuSelectors.forEach(selector => {
            document.querySelectorAll(selector).forEach(menu => {
                this.styleMenuContainer(menu);
            });
        });

        // Find all menu items and apply consistent classes
        const itemSelectors = [
            '.dropdown-item',
            '.menu-item',
            '.nav-item > a',
            '.context-menu-item',
            '.submenu-item'
        ];

        itemSelectors.forEach(selector => {
            document.querySelectorAll(selector).forEach(item => {
                this.styleMenuItem(item);
            });
        });
    }

    styleMenuContainer(menu) {
        // Add universal menu container class if not present
        if (!menu.classList.contains('universal-menu')) {
            menu.classList.add('universal-menu');
        }

        // Ensure proper ARIA attributes
        if (!menu.hasAttribute('role')) {
            menu.setAttribute('role', 'menu');
        }

        // Add animation class when shown
        if (menu.classList.contains('show')) {
            menu.style.animation = 'menuSlideIn 0.2s ease-out';
        }
    }

    styleMenuItem(item) {
        // Add universal menu item class if not present
        if (!item.classList.contains('universal-menu-item')) {
            item.classList.add('universal-menu-item');
        }

        // Ensure proper ARIA attributes
        if (!item.hasAttribute('role')) {
            item.setAttribute('role', 'menuitem');
        }

        // Add keyboard accessibility
        if (!item.hasAttribute('tabindex')) {
            item.setAttribute('tabindex', '0');
        }

        // Process icon and content structure
        this.processMenuItemContent(item);
    }

    processMenuItemContent(item) {
        // Check if item already has proper structure
        if (item.querySelector('.dropdown-item-content') || item.querySelector('.menu-item-content')) {
            return;
        }

        const icon = item.querySelector('i');
        const text = item.textContent || item.innerText;
        
        // Skip if item is empty or only contains icon
        if (!text.trim() || text.trim() === '') {
            return;
        }

        // Create structured content
        const contentDiv = document.createElement('div');
        contentDiv.className = 'dropdown-item-content';
        
        // Extract main text and subtitle if separated by newlines or specific patterns
        const lines = text.split('\n').map(line => line.trim()).filter(line => line);
        
        if (lines.length > 1) {
            const mainText = lines[0];
            const subtitle = lines.slice(1).join(' ');
            
            contentDiv.innerHTML = `
                ${mainText}
                <small>${subtitle}</small>
            `;
        } else {
            contentDiv.textContent = text.trim();
        }

        // Clear item content and rebuild structure
        item.innerHTML = '';
        
        if (icon) {
            item.appendChild(icon);
        }
        
        item.appendChild(contentDiv);
    }

    createMenu(items, options = {}) {
        const {
            type = 'dropdown',
            className = '',
            position = { x: 0, y: 0 },
            target = null,
            compact = false,
            searchable = false
        } = options;

        const menu = document.createElement('div');
        menu.className = `menu-container ${type === 'context' ? 'context-menu' : 'dropdown-menu'} ${compact ? 'compact' : ''} ${className}`;
        menu.setAttribute('role', 'menu');
        menu.style.position = type === 'context' ? 'fixed' : 'absolute';
        
        if (type === 'context') {
            menu.style.left = `${position.x}px`;
            menu.style.top = `${position.y}px`;
            menu.style.zIndex = '2000';
        }

        // Add search if requested
        if (searchable) {
            const searchInput = document.createElement('input');
            searchInput.type = 'text';
            searchInput.className = 'menu-search';
            searchInput.placeholder = 'Search...';
            searchInput.addEventListener('input', (e) => {
                this.filterMenuItems(menu, e.target.value);
            });
            menu.appendChild(searchInput);
        }

        // Add items
        items.forEach(item => {
            const menuItem = this.createMenuItem(item);
            menu.appendChild(menuItem);
        });

        // Style the menu
        this.styleMenuContainer(menu);

        return menu;
    }

    createMenuItem(itemData) {
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
            submenu = null
        } = itemData;

        if (separator) {
            const separator = document.createElement('div');
            separator.className = 'dropdown-divider menu-separator';
            return separator;
        }

        if (header) {
            const header = document.createElement('div');
            header.className = 'dropdown-header menu-header';
            header.textContent = text;
            return header;
        }

        const item = document.createElement(href ? 'a' : 'div');
        item.className = 'dropdown-item menu-item';
        
        if (href) {
            item.href = href;
        }
        
        if (disabled) {
            item.classList.add('disabled');
            item.setAttribute('aria-disabled', 'true');
        }

        if (checked !== null) {
            item.classList.add('checkable');
            if (checked) {
                item.classList.add('checked');
            }
        }

        // Build content structure
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
            innerHTML += `<span class="badge">${badge}</span>`;
        }

        item.innerHTML = innerHTML;

        // Add action handler
        if (action && typeof action === 'function') {
            item.addEventListener('click', (e) => {
                e.preventDefault();
                action(e, item);
            });
        }

        // Add submenu if provided
        if (submenu) {
            const submenuElement = this.createMenu(submenu, { type: 'submenu' });
            submenuElement.className += ' submenu';
            item.appendChild(submenuElement);
        }

        // Style the item
        this.styleMenuItem(item);

        return item;
    }

    showContextMenu(event, items, options = {}) {
        event.preventDefault();
        
        // Remove existing context menus
        document.querySelectorAll('.context-menu').forEach(menu => menu.remove());

        const menu = this.createMenu(items, {
            type: 'context',
            position: { x: event.clientX, y: event.clientY },
            ...options
        });

        document.body.appendChild(menu);
        menu.classList.add('show');

        // Close on click outside
        const closeHandler = (e) => {
            if (!menu.contains(e.target)) {
                menu.remove();
                document.removeEventListener('click', closeHandler);
            }
        };

        setTimeout(() => {
            document.addEventListener('click', closeHandler);
        }, 100);

        return menu;
    }

    setupKeyboardNavigation() {
        document.addEventListener('keydown', (e) => {
            const activeMenu = document.querySelector('.dropdown-menu.show, .context-menu.show');
            if (!activeMenu) return;

            const menuItems = activeMenu.querySelectorAll('.dropdown-item:not(.disabled)');
            const currentIndex = Array.from(menuItems).findIndex(item => item === document.activeElement);

            switch (e.key) {
                case 'ArrowDown':
                    e.preventDefault();
                    const nextIndex = (currentIndex + 1) % menuItems.length;
                    menuItems[nextIndex]?.focus();
                    break;
                    
                case 'ArrowUp':
                    e.preventDefault();
                    const prevIndex = currentIndex <= 0 ? menuItems.length - 1 : currentIndex - 1;
                    menuItems[prevIndex]?.focus();
                    break;
                    
                case 'Enter':
                case ' ':
                    if (document.activeElement.classList.contains('dropdown-item')) {
                        e.preventDefault();
                        document.activeElement.click();
                    }
                    break;
                    
                case 'Escape':
                    activeMenu.classList.remove('show');
                    break;
            }
        });
    }

    setupContextMenus() {
        // Prevent default context menu on elements with data-context-menu
        document.addEventListener('contextmenu', (e) => {
            const element = e.target.closest('[data-context-menu]');
            if (element) {
                e.preventDefault();
                
                const menuType = element.getAttribute('data-context-menu');
                const menuItems = this.getContextMenuItems(menuType, element);
                
                if (menuItems.length > 0) {
                    this.showContextMenu(e, menuItems);
                }
            }
        });
    }

    getContextMenuItems(menuType, element) {
        // Define different context menu types
        const contextMenus = {
            'table-row': [
                { icon: 'bi bi-eye', text: 'View Details', action: () => this.viewDetails(element) },
                { icon: 'bi bi-pencil', text: 'Edit', action: () => this.editItem(element) },
                { separator: true },
                { icon: 'bi bi-copy', text: 'Copy', action: () => this.copyItem(element) },
                { icon: 'bi bi-download', text: 'Export', action: () => this.exportItem(element) },
                { separator: true },
                { icon: 'bi bi-trash', text: 'Delete', action: () => this.deleteItem(element) }
            ],
            'chart': [
                { icon: 'bi bi-fullscreen', text: 'Full Screen', action: () => this.fullscreenChart(element) },
                { icon: 'bi bi-download', text: 'Download PNG', action: () => this.downloadChart(element, 'png') },
                { icon: 'bi bi-file-pdf', text: 'Download PDF', action: () => this.downloadChart(element, 'pdf') },
                { separator: true },
                { icon: 'bi bi-gear', text: 'Chart Settings', action: () => this.chartSettings(element) }
            ],
            'data-grid': [
                { icon: 'bi bi-plus', text: 'Add Row', action: () => this.addRow(element) },
                { icon: 'bi bi-filter', text: 'Apply Filter', action: () => this.applyFilter(element) },
                { icon: 'bi bi-sort-down', text: 'Sort Column', action: () => this.sortColumn(element) },
                { separator: true },
                { icon: 'bi bi-columns', text: 'Show/Hide Columns', action: () => this.toggleColumns(element) }
            ]
        };

        return contextMenus[menuType] || [];
    }

    setupMutationObserver() {
        const observer = new MutationObserver((mutations) => {
            mutations.forEach((mutation) => {
                mutation.addedNodes.forEach((node) => {
                    if (node.nodeType === Node.ELEMENT_NODE) {
                        // Check if the added node is a menu or contains menus
                        if (node.classList?.contains('dropdown-menu') || 
                            node.classList?.contains('context-menu') ||
                            node.querySelector('.dropdown-menu, .context-menu')) {
                            
                            setTimeout(() => this.applyUniversalStyling(), 50);
                        }
                    }
                });
            });
        });

        observer.observe(document.body, {
            childList: true,
            subtree: true
        });
    }

    filterMenuItems(menu, searchTerm) {
        const items = menu.querySelectorAll('.dropdown-item');
        const term = searchTerm.toLowerCase();

        items.forEach(item => {
            const text = item.textContent.toLowerCase();
            const shouldShow = text.includes(term);
            item.style.display = shouldShow ? '' : 'none';
        });
    }

    // Utility methods for context menu actions
    viewDetails(element) { console.log('View details for:', element); }
    editItem(element) { console.log('Edit item:', element); }
    copyItem(element) { console.log('Copy item:', element); }
    exportItem(element) { console.log('Export item:', element); }
    deleteItem(element) { console.log('Delete item:', element); }
    fullscreenChart(element) { console.log('Fullscreen chart:', element); }
    downloadChart(element, format) { console.log('Download chart as', format, ':', element); }
    chartSettings(element) { console.log('Chart settings for:', element); }
    addRow(element) { console.log('Add row to:', element); }
    applyFilter(element) { console.log('Apply filter to:', element); }
    sortColumn(element) { console.log('Sort column in:', element); }
    toggleColumns(element) { console.log('Toggle columns in:', element); }
}

// Initialize the universal menu system when DOM is ready
document.addEventListener('DOMContentLoaded', () => {
    window.universalMenu = new UniversalMenu();
});

// Export for use in other scripts
if (typeof module !== 'undefined' && module.exports) {
    module.exports = UniversalMenu;
}
